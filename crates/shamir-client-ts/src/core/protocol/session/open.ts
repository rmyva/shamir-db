/**
 * Session open: dial + SCRAM handshake, returning plain data
 * ({@link SessionOpened}) — never a ShamirClient.
 *
 * PLATFORM-AGNOSTIC.
 */

import type { Platform } from '../../support/platform.js';
import * as ed from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha512';
import { WsFramer } from '../framing.js';
import { runHandshake } from '../protocol.js';
import { withConnectTimeout } from '../../client/client-internals.js';
import { DEFAULT_CONNECT_TIMEOUT_MS } from '../../types/connection.js';
import type {
  ConnectOptions as ScramConnectOptions,
  ConnectLocalOptions,
  SessionTransport,
} from '../../types/connection.js';
import { browserWsBinding, ipcBinding } from './binding.js';
import type { ChannelBinding } from './binding.js';
import { BINDING_MODE_TLS_EXPORTER } from '../scram.js';
import { ServerIdentityChangedError } from '../../support/errors.js';
import type {
  SessionEndpoint,
  SessionLink,
  SessionTlsOptions,
} from './endpoint.js';
import type { SessionOpened } from './opened.js';

/** 18-byte ASCII header — must equal IDENTITY_V1 in domain_tags.rs. */
export const IDENTITY_V1 = new TextEncoder().encode('SHAMIR-IDENTITY-v1');

/** Inputs of {@link buildIdentityInput} — the byte string the server signs. */
export interface IdentityInput {
  serverPubKey: Uint8Array;
  transportKind: number;
  bindingMode: number;
  tlsExporterOrZeros: Uint8Array;
  authMessage: Uint8Array;
  sessionId: Uint8Array;
  expiresAtNs: bigint;
}

function u64be(n: bigint): Uint8Array {
  const out = new Uint8Array(8);
  for (let i = 7; i >= 0; i -= 1) {
    out[i] = Number(n & 0xffn);
    n >>= 8n;
  }
  return out;
}

/**
 * Build the canonical `identity_input` the server Ed25519-signs into
 * `auth_ok.identity_sig`. Byte-for-byte mirror of
 * `crates/shamir-connect/src/common/user/identity.rs::build_identity_input`:
 *   "SHAMIR-IDENTITY-v1" || SHA256(server_pub_key) || u8(transport_kind)
 *   || u8(binding_mode) || tls_exporter_or_zeros(32) || auth_message
 *   || session_id(32) || u64_be(expires_at_ns)
 *
 * Takes the platform so the SHA-256 of the server public key uses the
 * platform's crypto implementation.
 */
export function buildIdentityInput(
  platform: Platform,
  inputs: IdentityInput,
): Uint8Array {
  const hashedPub = platform.sha256(inputs.serverPubKey);
  const parts: Uint8Array[] = [
    IDENTITY_V1,
    hashedPub,
    Uint8Array.of(inputs.transportKind & 0xff),
    Uint8Array.of(inputs.bindingMode & 0xff),
    inputs.tlsExporterOrZeros,
    inputs.authMessage,
    inputs.sessionId,
    u64be(inputs.expiresAtNs),
  ];
  let len = 0;
  for (const p of parts) len += p.length;
  const out = new Uint8Array(len);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

/**
 * Resolve the caller's pin policy BEFORE any network I/O (mirrors the Rust
 * reference's `HandshakeBuilder::build`, which refuses a connect that has
 * neither a pin nor trust-on-first-use). `acceptNewHost` defaults to `true`,
 * which keeps the historical behaviour: the first server is accepted and its
 * pin captured for persistence. Throws when `trustedPin` is unset and
 * `acceptNewHost` is explicitly `false`.
 */
export function resolveTrustedPin(opts: {
  trustedPin?: Uint8Array;
  acceptNewHost?: boolean;
}): { trustedPin: Uint8Array | undefined } {
  const trustedPin = opts.trustedPin;
  const acceptNewHost = opts.acceptNewHost ?? true;
  if (trustedPin === undefined && !acceptNewHost) {
    throw new Error(
      'no trustedPin AND acceptNewHost not set: refusing first contact with an unpinned server',
    );
  }
  return { trustedPin };
}

/** Everything needed to verify the server's auth_ok identity. Steps 2-4 of
 * the Rust reference's `process_auth_ok`; step 1 (the SCRAM server_signature)
 * is already checked inside `runHandshake`. */
export interface AuthIdentityInputs {
  serverPubKey: Uint8Array;
  identitySig: Uint8Array;
  sessionId: Uint8Array;
  expiresAtNs: bigint;
  authMessage: Uint8Array;
  binding: ChannelBinding;
  /** Resolved pin (`resolveTrustedPin`); `undefined` = trust-on-first-use. */
  trustedPin?: Uint8Array;
}

/**
 * Verify the server's Ed25519 identity from `auth_ok`, mirroring the ORDER of
 * checks of the Rust client (`crates/shamir-connect/src/client/handshake.rs::
 * process_auth_ok`, steps 2-4):
 *   (2) pin check — SHA256(server_pub_key) equals `trustedPin`, constant
 *       time; a mismatch throws ServerIdentityChangedError('pin_mismatch');
 *   (3) Ed25519 strict verify of `identity_sig` over `buildIdentityInput`
 *       with the server's public key; a failure throws
 *       ServerIdentityChangedError('identity_sig_invalid');
 *   (4) trust-on-first-use — with no pin, all checks passing, the captured
 *       pin SHA256(server_pub_key) is returned for persistence.
 * The exporter enters the identity input as tls_exporter_or_zeros: the real
 * exporter for binding mode 0x01, zeros for every other profile (the same
 * rule the auth_message uses).
 * Returns the captured pin (exposed via `ShamirClient.trustedPin()`).
 */
export function verifyAuthOkIdentity(
  platform: Platform,
  inputs: AuthIdentityInputs,
): Uint8Array {
  const { serverPubKey, identitySig, sessionId, authMessage, binding } = inputs;
  if (serverPubKey.length !== 32) {
    throw new Error('auth_ok.server_pub_key must be 32 bytes');
  }
  if (identitySig.length !== 64) {
    throw new Error('auth_ok.identity_sig must be 64 bytes');
  }
  if (sessionId.length !== 32) {
    throw new Error('auth_ok.session_id must be 32 bytes');
  }
  // (2) Pin check — mismatch is fatal.
  const receivedPin = platform.sha256(serverPubKey);
  if (
    inputs.trustedPin !== undefined &&
    !platform.timingSafeEqual(inputs.trustedPin, receivedPin)
  ) {
    throw new ServerIdentityChangedError('pin_mismatch');
  }
  // (3) Ed25519 strict verify of identity_sig.
  const identityInput = buildIdentityInput(platform, {
    serverPubKey,
    transportKind: binding.transportKind,
    bindingMode: binding.bindingMode,
    tlsExporterOrZeros:
      binding.bindingMode === BINDING_MODE_TLS_EXPORTER
        ? binding.exporter
        : new Uint8Array(32),
    authMessage,
    sessionId,
    expiresAtNs: inputs.expiresAtNs,
  });
  if (ed.etc.sha512Sync === undefined) {
    ed.etc.sha512Sync = (...m) => sha512(ed.etc.concatBytes(...m));
  }
  let valid = false;
  try {
    valid = ed.verify(identitySig, identityInput, serverPubKey, { zip215: false });
  } catch {
    valid = false;
  }
  if (!valid) {
    throw new ServerIdentityChangedError('identity_sig_invalid');
  }
  // (4) First contact (TOFU): all checks passed — offer the pin.
  return receivedPin;
}

/**
 * Run the SCRAM handshake over an already-open link, verify the server's
 * identity (pin + `identity_sig`), and return the plain result. Closes the
 * framer and rethrows on any failure.
 */
export async function openSession(
  platform: Platform,
  link: SessionLink,
  username: string,
  password: string,
  policy: { trustedPin?: Uint8Array } = {},
): Promise<SessionOpened> {
  const framer = new WsFramer(link.socket);

  try {
    const {
      sessionId,
      serverPubKey,
      identitySig,
      expiresAtNs,
      resumptionTicket,
      resumptionExpiresAtNs,
      serverQueryVersion,
      authMessage,
    } = await runHandshake(platform, framer, username, password, link.binding);
    const trustedPin = verifyAuthOkIdentity(platform, {
      serverPubKey,
      identitySig,
      sessionId,
      expiresAtNs,
      authMessage,
      binding: link.binding,
      trustedPin: policy.trustedPin,
    });
    return {
      framer,
      sessionId,
      serverPubKey,
      expiresAtNs,
      resumptionTicket,
      resumptionExpiresAtNs,
      serverQueryVersion,
      normalizedUser: username.normalize('NFC'),
      binding: link.binding,
      trustedPin,
    };
  } catch (e) {
    await framer.close();
    throw e;
  }
}

export interface EndpointOptions {
  host: string;
  port: number;
  transport?: SessionTransport;
  tls?: { rejectUnauthorized?: boolean; ca?: string | Uint8Array };
  origin?: string;
}

function hostForUrl(host: string): string {
  return host.includes(':') && !host.startsWith('[') ? `[${host}]` : host;
}

/**
 * True when `host` is an explicit loopback endpoint: `localhost`, an IPv4
 * literal inside 127.0.0.0/8, `::1`, or an IPv4-mapped IPv6 loopback
 * (`::ffff:127.x.x.x`). Plain TCP is refused for anything else BEFORE a
 * socket is opened — the same client-side rule as the Rust `connect_plain`.
 * A DNS name that would resolve to loopback is NOT accepted (the client
 * refuses it without resolving).
 */
export function isLoopbackHost(host: string): boolean {
  let h = host.toLowerCase();
  if (h.startsWith('[') && h.endsWith(']')) h = h.slice(1, -1);
  if (h === 'localhost') return true;
  const v4 = parseIpv4(h);
  if (v4 !== null) return v4[0] === 127;
  const v6 = parseIpv6(h);
  if (v6 === null) return false;
  const loopback = v6.slice(0, 7).every((g) => g === 0) && v6[7] === 1;
  const mappedLoopback =
    v6.slice(0, 5).every((g) => g === 0) && v6[5] === 0xffff && (v6[6] >> 8) === 127;
  return loopback || mappedLoopback;
}

/** Parse a dotted IPv4 literal (four decimal octets, each 0-255) or null. */
function parseIpv4(s: string): number[] | null {
  const m = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(s);
  if (m === null) return null;
  const octets = [m[1], m[2], m[3], m[4]].map((o) => Number(o));
  return octets.every((o) => o >= 0 && o <= 255) ? octets : null;
}

/** Parse an IPv6 literal (with `::` and an optional trailing IPv4 part) into 8 groups, or null. */
function parseIpv6(s: string): number[] | null {
  if (!s.includes(':')) return null;
  let tail: number[] = [];
  let body = s;
  const lastColon = body.lastIndexOf(':');
  if (body.slice(lastColon + 1).includes('.')) {
    const v4 = parseIpv4(body.slice(lastColon + 1));
    if (v4 === null) return null;
    tail = [(v4[0] << 8) | v4[1], (v4[2] << 8) | v4[3]];
    body = body.slice(0, lastColon + 1);
    if (body.endsWith(':') && !body.endsWith('::')) body = body.slice(0, -1);
  }
  const halves = body.split('::');
  if (halves.length > 2) return null;
  const parse = (part: string): number[] | null => {
    if (part === '') return [];
    const groups = part.split(':');
    const out: number[] = [];
    for (const g of groups) {
      if (!/^[0-9a-f]{1,4}$/.test(g)) return null;
      out.push(parseInt(g, 16));
    }
    return out;
  };
  const head = parse(halves[0] ?? '');
  if (head === null) return null;
  if (halves.length === 1) {
    const all = [...head, ...tail];
    return all.length === 8 ? all : null;
  }
  const rest = parse(halves[1] ?? '');
  if (rest === null) return null;
  const fill = 8 - head.length - rest.length - tail.length;
  if (fill < 1) return null;
  return [...head, ...new Array<number>(fill).fill(0), ...rest, ...tail];
}

/** Map connect/resume options to a transport endpoint. */
export function endpointFromOptions(opts: EndpointOptions): SessionEndpoint {
  const transport = opts.transport;
  if (transport === undefined || transport === 'ws-browser') {
    if (opts.tls?.ca !== undefined) {
      throw new Error("tls.ca is only supported with transport 'tcp' or 'ws-native'");
    }
    return {
      kind: 'ws-browser',
      url: `wss://${opts.host}:${opts.port}/shamir/v1/browser`,
      origin: opts.origin ?? `https://${opts.host}`,
      tls: { rejectUnauthorized: opts.tls?.rejectUnauthorized ?? true },
    };
  }
  if (transport === 'tcp-plain') {
    if (opts.tls !== undefined) {
      throw new Error("tls options are not valid with transport 'tcp-plain' (plain TCP has no TLS)");
    }
    if (!isLoopbackHost(opts.host)) {
      throw new Error(
        `plain TCP is loopback-only: refusing host '${opts.host}' before any connection attempt`,
      );
    }
    return { kind: 'tcp-plain', host: opts.host, port: opts.port };
  }
  if (transport === 'tcp' || transport === 'ws-native') {
    const tls: SessionTlsOptions = {
      rejectUnauthorized: opts.tls?.rejectUnauthorized ?? true,
      ...(opts.tls?.ca === undefined ? {} : { ca: opts.tls.ca }),
    };
    if (transport === 'tcp') {
      return { kind: 'tcp', host: opts.host, port: opts.port, tls };
    }
    return {
      kind: 'ws-native',
      url: `wss://${hostForUrl(opts.host)}:${opts.port}/shamir/v1`,
      tls,
    };
  }
  throw new Error(`unknown transport: ${String(transport)}`);
}

/** Open the transport link for `endpoint` (no handshake). */
export async function dialEndpoint(
  platform: Platform,
  endpoint: SessionEndpoint,
  connectTimeoutMs: number,
): Promise<SessionLink> {
  switch (endpoint.kind) {
    case 'ws-browser': {
      const socket = await withConnectTimeout(
        platform.openSocket(endpoint.url, {
          rejectUnauthorized: endpoint.tls?.rejectUnauthorized ?? true,
          origin: endpoint.origin,
        }),
        connectTimeoutMs,
      );
      return { socket, binding: browserWsBinding() };
    }
    case 'tcp':
    case 'tcp-plain':
    case 'ws-native':
      if (!platform.openNativeLink) {
        throw new Error(
          `transport '${endpoint.kind}': this Platform does not implement openNativeLink (TCP, plain TCP and native WebSocket with the TLS exporter are Node only)`,
        );
      }
      return platform.openNativeLink(endpoint, { connectTimeoutMs });
    case 'ipc':
      throw new Error('dialEndpoint: ipc endpoints are opened by openLocalSession');
  }
}

/**
 * Dial the selected transport and run the SCRAM handshake. Platform provides
 * crypto + socket.
 */
export async function openScramSession(
  platform: Platform,
  opts: ScramConnectOptions,
): Promise<SessionOpened> {
  const policy = resolveTrustedPin(opts);
  const link = await dialEndpoint(
    platform,
    endpointFromOptions(opts),
    opts.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
  );
  return openSession(platform, link, opts.username, opts.password, policy);
}

/**
 * Open a local-IPC connection (Unix domain socket / Windows Named Pipe,
 * spec TRANSPORT_UNIX.md) and run the SCRAM handshake. Mirrors
 * {@link openScramSession} exactly except: no TLS setup, and the channel
 * binding is `ipcBinding()` (`TRANSPORT_KIND_UNIX`/`BINDING_MODE_NONE`)
 * instead of the WS/browser profile.
 *
 * Node-only — `platform.openIpcSocket` is optional on {@link Platform}
 * precisely because there is no browser implementation; this throws
 * immediately (before touching the network) if the platform lacks it.
 */
export async function openLocalSession(
  platform: Platform,
  opts: ConnectLocalOptions,
): Promise<SessionOpened> {
  const policy = resolveTrustedPin(opts);
  if (!platform.openIpcSocket) {
    throw new Error(
      'connectLocal: this Platform does not implement openIpcSocket ' +
        '(local IPC has no browser equivalent — Node only)',
    );
  }
  const socket = await withConnectTimeout(
    platform.openIpcSocket(opts.addr),
    opts.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
  );
  return openSession(
    platform,
    { socket, binding: ipcBinding() },
    opts.username,
    opts.password,
    policy,
  );
}
