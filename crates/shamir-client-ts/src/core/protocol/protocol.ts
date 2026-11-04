/**
 * SCRAM handshake state machine (auth_init → challenge → client_proof → auth_ok).
 *
 * Operates over a WsFramer + Platform. No platform-specific imports.
 *
 * PLATFORM-AGNOSTIC.
 */

import type { Platform } from '../support/platform.js';
import { WsFramer, encode, decode } from './framing.js';
import {
  buildAuthMessage,
  computeClientProof,
  validateKdfParams,
  verifyServerSignature,
  BINDING_MODE_TLS_EXPORTER,
  BINDING_MODE_TLS_NO_EXPORT,
  SUPPORTED_VERSION,
  type KdfParams,
} from './scram.js';
import { browserWsBinding } from './session/binding.js';
import type { ChannelBinding } from './session/binding.js';

/**
 * rmp_serde (the Rust msgpack library) serialises structs as **arrays**
 * (sequences) by default, not maps. The wire format for all handshake
 * messages is therefore positional:
 *
 *   Challenge = [salt, memory_kb, time, parallelism, argon2_version, server_nonce]
 *   AuthOk    = [server_signature, server_pub_key, identity_sig, session_id,
 *                expires_at_ns, resumption_ticket?, resumption_expires_at_ns?,
 *                server_query_version?]
 *   ResumeOk  = [session_id, expires_at_ns, resumption_ticket?,
 *                resumption_expires_at_ns?, server_query_version?]
 */

// Positional indices for Challenge (rmp_serde array layout).
const CH_SALT = 0;
const CH_MEMORY_KB = 1;
const CH_TIME = 2;
const CH_PARALLELISM = 3;
const CH_ARGON2_VERSION = 4;
const CH_SERVER_NONCE = 5;

// Positional indices for AuthOk (rmp_serde array layout).
const OK_SERVER_SIG = 0;
const OK_SERVER_PUB = 1;
const OK_IDENTITY_SIG = 2;
const OK_SESSION_ID = 3;
const OK_EXPIRES_AT_NS = 4;

/**
 * Positional indices for ResumeOk (rmp_serde array layout). Matches the field
 * order of `ResumeOkWire` in `crates/shamir-server/src/connection/wire.rs`
 * (and `WireResumeOk` in `crates/shamir-client/src/wire_frames.rs`). Trailing
 * fields are `#[serde(default)]` on the server side, so a server that omits
 * them produces a shorter array — callers read indices 2..4 defensively.
 */
export const RESUME_OK_SESSION_ID = 0;
export const RESUME_OK_EXPIRES_AT_NS = 1;
export const RESUME_OK_RESUMPTION_TICKET = 2;
export const RESUME_OK_RESUMPTION_EXPIRES_AT_NS = 3;
export const RESUME_OK_SERVER_QUERY_VERSION = 4;

/**
 * Coerce a msgpack-decoded value to `Uint8Array`, else throw with a label.
 * Shared by the `auth_ok` and `resume_ok` decoders so both enforce the same
 * bytes-or-throw invariant on binary fields.
 */
export function asBytes(v: unknown, what: string): Uint8Array {
  if (v instanceof Uint8Array) return v;
  throw new Error(`${what}: expected binary, got ${typeof v}`);
}

/** Result of a successful SCRAM handshake. */
export interface HandshakeResult {
  sessionId: Uint8Array;
  serverPubKey: Uint8Array;
  expiresAtNs: bigint;
  /** Optional resumption ticket for fast reconnection (if server issued one). */
  resumptionTicket?: Uint8Array;
  resumptionExpiresAtNs?: bigint;
  /**
   * Max query-language version this server supports. `0` means the server
   * predates query-lang negotiation (pre-v2). Positional index 7 in the
   * AuthOk array.
   */
  serverQueryVersion: number;
}

/**
 * Run the 4-message SCRAM-Argon2id handshake over `framer`.
 * Throws on any validation or crypto failure.
 *
 * `binding` defaults to the WebSocket/browser profile
 * (`browserWsBinding()`: `TRANSPORT_KIND_WS`/`BINDING_MODE_TLS_NO_EXPORT`).
 * A legacy numeric form is also accepted: a bare `transportKind` number plus
 * an optional `legacyBindingModeIfNumber` (default
 * `BINDING_MODE_TLS_NO_EXPORT`) — kept so existing callers (the
 * `protocol.test.ts` guard tests) need no change. In the numeric form the
 * exporter is zero-filled. The binding's exporter is part of the SCRAM
 * `auth_message`: all-zero for non-exporter profiles (browser/IPC/legacy),
 * the real TLS exporter for binding mode 0x01.
 */
export async function runHandshake(
  platform: Platform,
  framer: WsFramer,
  username: string,
  password: string,
  binding: ChannelBinding | number = browserWsBinding(),
  legacyBindingModeIfNumber?: number,
): Promise<HandshakeResult> {
  const resolved: ChannelBinding = typeof binding === 'number'
    ? { transportKind: binding as ChannelBinding['transportKind'],
        bindingMode: (legacyBindingModeIfNumber ?? BINDING_MODE_TLS_NO_EXPORT) as ChannelBinding['bindingMode'],
        exporter: new Uint8Array(32) }
    : binding;
  // Server normalises the username (PRECIS UsernameCaseMapped + NFC).
  // We send the NFC form so the two byte strings agree for ASCII usernames.
  const normalizedUser = username.normalize('NFC');

  // --- msg1: auth_init ---
  const clientNonce = platform.randomBytes(32);
  framer.send(
    encode({
      user: normalizedUser,
      client_nonce: clientNonce,
      binding_mode: resolved.bindingMode,
      version: SUPPORTED_VERSION,
    }),
  );

  // --- msg2: challenge ---
  // rmp_serde encodes structs as arrays: [salt, memory_kb, time, parallelism,
  // argon2_version, server_nonce]
  const chArr = decode(await framer.recv()) as unknown[];
  const salt = asBytes(chArr[CH_SALT], 'challenge.salt');
  const serverNonce = asBytes(chArr[CH_SERVER_NONCE], 'challenge.server_nonce');
  const kdf: KdfParams = {
    memoryKb: chArr[CH_MEMORY_KB] as number,
    time: chArr[CH_TIME] as number,
    parallelism: chArr[CH_PARALLELISM] as number,
    argon2Version: chArr[CH_ARGON2_VERSION] as number,
  };
  validateKdfParams(kdf);
  if (salt.length !== 16) throw new Error('challenge salt must be 16 bytes');
  if (serverNonce.length !== 32) {
    throw new Error('challenge server_nonce must be 32 bytes');
  }

  // --- msg3: client_proof ---
  // Send as array too: [client_proof_bytes]
  const authMessage = buildAuthMessage(normalizedUser, clientNonce, {
    serverNonce,
    salt,
    kdf,
    transportKind: resolved.transportKind,
    bindingMode: resolved.bindingMode,
    // The server uses the exporter only for mode 0x01; any other mode hashes zeros.
    tlsExporterOrZeros:
      resolved.bindingMode === BINDING_MODE_TLS_EXPORTER ? resolved.exporter : new Uint8Array(32),
  });
  const { clientProof, serverKey } = await computeClientProof(
    platform,
    password,
    salt,
    kdf,
    authMessage,
  );
  // Send as a named map — rmp_serde on the server can decode either
  // array or named-map format for struct deserialization.
  framer.send(encode({ client_proof: clientProof }));

  // --- msg4: auth_ok | error ---
  // rmp_serde AuthOk array: [server_signature, server_pub_key, identity_sig,
  //                           session_id, expires_at_ns, ...]
  const okRaw = decode(await framer.recv()) as unknown[] | { error?: string };
  if (!Array.isArray(okRaw)) {
    const errMap = okRaw as { error?: string };
    if (typeof errMap.error === 'string') {
      throw new Error(`authentication failed: ${errMap.error}`);
    }
    throw new Error(`auth_ok: unexpected non-array response`);
  }
  const serverSignature = asBytes(okRaw[OK_SERVER_SIG], 'auth_ok.server_signature');
  const sessionId = asBytes(okRaw[OK_SESSION_ID], 'auth_ok.session_id');
  const serverPubKey = asBytes(okRaw[OK_SERVER_PUB], 'auth_ok.server_pub_key');

  if (!verifyServerSignature(platform, serverKey, authMessage, serverSignature)) {
    throw new Error('server signature verification failed (MITM?)');
  }
  if (sessionId.length !== 32) {
    throw new Error('auth_ok.session_id must be 32 bytes');
  }

  const expiresAtNs = BigInt(okRaw[OK_EXPIRES_AT_NS] as number | bigint);

  // Optional trailing fields: resumption_ticket, resumption_expires_at_ns
  const resumptionTicket =
    okRaw[5] instanceof Uint8Array ? okRaw[5] : undefined;
  const resumptionExpiresAtNs =
    okRaw[6] !== undefined && okRaw[6] !== null
      ? BigInt(okRaw[6] as number | bigint)
      : undefined;

  // Positional index 7: server_query_version (u8, default 0).
  const serverQueryVersion =
    typeof okRaw[7] === 'number' ? okRaw[7] : 0;

  return { sessionId, serverPubKey, expiresAtNs, resumptionTicket, resumptionExpiresAtNs, serverQueryVersion };
}
