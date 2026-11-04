/**
 * Session resume: dial + resume frame, returning plain data
 * ({@link SessionOpened}) — never a ShamirClient.
 *
 * PLATFORM-AGNOSTIC.
 */

import * as ed from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha512';
import type { Platform } from '../../support/platform.js';
import { WsFramer, encode, decode } from '../framing.js';
import {
  asBytes,
  RESUME_OK_SESSION_ID,
  RESUME_OK_EXPIRES_AT_NS,
  RESUME_OK_RESUMPTION_TICKET,
  RESUME_OK_RESUMPTION_EXPIRES_AT_NS,
  RESUME_OK_SERVER_QUERY_VERSION,
} from '../protocol.js';
import { BINDING_MODE_TLS_EXPORTER } from '../scram.js';
import { DEFAULT_CONNECT_TIMEOUT_MS } from '../../types/connection.js';
import type { ResumeOptions } from '../../types/connection.js';
import { dialEndpoint, endpointFromOptions } from './open.js';
import type { SessionOpened } from './opened.js';

/** Domain tag of the resume identity proof; must equal RESUME_PROOF_V1. */
const RESUME_PROOF_V1 = new TextEncoder().encode('SHAMIR-RESUME-PROOF-v1');

/**
 * Verify the server's identity proof on a tls_exporter resume link, BEFORE
 * the (bearer) ticket is sent. Mirrors the Rust client's
 * `resume_wire_exchange`: sends `[client_nonce]` as a 1-field positional
 * array, expects `[server_pub_key(32), signature(64)]`, and checks the key
 * against the pinned one and the Ed25519 signature over
 * `RESUME_PROOF_V1 || client_nonce || exporter`.
 */
export async function verifyResumeIdentityProof(
  platform: Platform,
  framer: WsFramer,
  clientNonce: Uint8Array,
  exporter: Uint8Array,
  pinnedServerPubKey: Uint8Array,
): Promise<void> {
  framer.send(encode([clientNonce]));
  const resp = decode(await framer.recv());
  if (!Array.isArray(resp)) {
    throw new Error('resume proof: malformed response');
  }
  let serverPubKey: Uint8Array;
  let signature: Uint8Array;
  try {
    serverPubKey = asBytes(resp[0], 'resume proof: server_pub_key');
    signature = asBytes(resp[1], 'resume proof: signature');
  } catch {
    throw new Error('resume proof: malformed response');
  }
  if (serverPubKey.length !== 32 || signature.length !== 64) {
    throw new Error('resume proof: malformed response');
  }
  if (!platform.timingSafeEqual(serverPubKey, pinnedServerPubKey)) {
    throw new Error('resume: server identity does not match pinned key');
  }
  const msg = new Uint8Array(
    RESUME_PROOF_V1.length + clientNonce.length + exporter.length,
  );
  msg.set(RESUME_PROOF_V1, 0);
  msg.set(clientNonce, RESUME_PROOF_V1.length);
  msg.set(exporter, RESUME_PROOF_V1.length + clientNonce.length);
  if (ed.etc.sha512Sync === undefined) {
    ed.etc.sha512Sync = (...m) => sha512(ed.etc.concatBytes(...m));
  }
  let valid = false;
  try {
    valid = ed.verify(signature, msg, serverPubKey, { zip215: false });
  } catch {
    valid = false;
  }
  if (!valid) {
    throw new Error('resume: server identity proof invalid');
  }
}

/**
 * Fast reconnection using a resumption ticket from a previous session.
 *
 * Opens a new connection (transport per `opts.transport`) and sends a resume
 * frame (ticket + fresh nonce) instead of running the full SCRAM handshake.
 * On a tls_exporter link the server first proves its identity. The server
 * validates the ticket and issues a new session.
 *
 * Throws (and closes the socket) if the server rejects the ticket or the
 * response is malformed.
 */
export async function resumeSession(
  platform: Platform,
  opts: ResumeOptions,
): Promise<SessionOpened> {
  const endpoint = endpointFromOptions(opts);
  const link = await dialEndpoint(
    platform,
    endpoint,
    opts.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
  );
  const framer = new WsFramer(link.socket);
  const binding = link.binding;

  try {
    const clientNonce = platform.randomBytes(32);

    if (binding.bindingMode === BINDING_MODE_TLS_EXPORTER) {
      await verifyResumeIdentityProof(
        platform,
        framer,
        clientNonce,
        binding.exporter,
        opts.serverPubKey,
      );
    }

    // Wire: { ticket, client_nonce, binding_mode }
    // (binding_mode is the session link's: 0x02 browser profile, 0x01 tls_exporter)
    framer.send(
      encode({
        ticket: opts.ticket,
        client_nonce: clientNonce,
        binding_mode: binding.bindingMode,
      }),
    );

    // Server responds with `ResumeOkWire` (crates/shamir-server/src/
    // connection/wire.rs:80-92): a plain struct with NO `#[serde(rename_all)]`,
    // emitted via `rmp_serde::to_vec` (not `to_vec_named`) → a POSITIONAL
    // msgpack ARRAY in declaration order:
    //   [0] session_id (bytes, 32)
    //   [1] expires_at_ns (u64)
    //   [2] resumption_ticket (bytes; empty Vec when no ticket)
    //   [3] resumption_expires_at_ns (u64; 0 when no ticket)
    //   [4] server_query_version (u8; 0 when absent)
    // This mirrors how `runHandshake` decodes `auth_ok` positionally.
    //
    // On resume REJECTION the server actually shuts the connection down
    // (handshake.rs:602-606), so the client observes a socket close rather
    // than an error frame. The array-vs-error-map guard below is kept
    // defensive (and mirrors runHandshake's auth_ok error-map check): a
    // map with an `error` string is still surfaced as a thrown rejection.
    const rawBytes = await framer.recv();
    const resp = decode(rawBytes) as unknown[] | { error?: string };

    if (!Array.isArray(resp)) {
      const errMap = resp as { error?: string };
      if (typeof errMap.error === 'string') {
        throw new Error(`resume rejected: ${errMap.error}`);
      }
      throw new Error('resume_ok: unexpected non-array response');
    }

    const sessionId = asBytes(
      resp[RESUME_OK_SESSION_ID],
      'resume response: session_id',
    );
    if (sessionId.length !== 32) {
      throw new Error('resume response: session_id must be 32 bytes');
    }

    const expiresAtNs = BigInt(
      resp[RESUME_OK_EXPIRES_AT_NS] as number | bigint,
    );

    // Optional trailing fields. The server always emits indices 2 and 3
    // (even when no ticket is issued: empty Vec and 0u64 respectively), but
    // a pre-v2 server that omits them yields a shorter array — read them
    // defensively, matching runHandshake's handling of the analogous
    // auth_ok trailing fields.
    // An empty/absent resumption_ticket means "no ticket" → undefined.
    const ticketRaw = resp[RESUME_OK_RESUMPTION_TICKET];
    const resumptionTicket =
      ticketRaw instanceof Uint8Array && ticketRaw.length > 0
        ? ticketRaw
        : undefined;
    const resumptionExpiresRaw = resp[RESUME_OK_RESUMPTION_EXPIRES_AT_NS];
    const resumptionExpiresAtNs =
      resumptionTicket !== undefined &&
      resumptionExpiresRaw !== undefined &&
      resumptionExpiresRaw !== null
        ? BigInt(resumptionExpiresRaw as number | bigint)
        : undefined;

    // Max query-language version the server supports (u8, default 0).
    // Absent/malformed → 0 (a pre-v2 server omitting the field is a valid
    // legacy case), mirroring runHandshake's handling of the analogous
    // `auth_ok.server_query_version` positional field (index 7 there).
    const serverQueryVersion =
      typeof resp[RESUME_OK_SERVER_QUERY_VERSION] === 'number'
        ? resp[RESUME_OK_SERVER_QUERY_VERSION]
        : 0;

    return {
      framer,
      sessionId,
      serverPubKey: opts.serverPubKey,
      expiresAtNs,
      resumptionTicket,
      resumptionExpiresAtNs,
      serverQueryVersion,
      normalizedUser: undefined,
      binding,
    };
  } catch (e) {
    await framer.close();
    throw e;
  }
}
