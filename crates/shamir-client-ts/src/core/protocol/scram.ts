/**
 * SCRAM-Argon2id crypto — PLATFORM-AGNOSTIC.
 *
 * Byte-for-byte mirror of the Rust reference implementation:
 *   - canonical `auth_message`  → crates/shamir-connect/src/common/auth_message.rs
 *   - SCRAM key schedule        → crates/shamir-connect/src/common/crypto.rs
 *
 * All crypto is delegated to the injected `Platform`; no `node:crypto` or
 * WebCrypto imports appear here.
 */

import type { Platform } from '../support/platform.js';

/** 14-byte ASCII header — must equal `AUTH_V1` in domain_tags.rs. */
const AUTH_V1 = new TextEncoder().encode('SHAMIR-AUTH-v1');

/** 15-byte ASCII header — must equal `CHGPW_V1` in domain_tags.rs. */
const CHGPW_V1 = new TextEncoder().encode('SHAMIR-CHGPW-v1');

/** Argon2 version 1.3 (0x13 == 19). */
export const ARGON2_VERSION_13 = 0x13;
/** Transport tag for the TCP path (TransportKind::Tcp). */
export const TRANSPORT_KIND_TCP = 0x01;
/** Binding mode for the tls_exporter listener profile (BindingMode::TlsExporter). */
export const BINDING_MODE_TLS_EXPORTER = 0x01;
/** Transport tag for the WebSocket path (TransportKind::Ws). */
export const TRANSPORT_KIND_WS = 0x02;
/** Transport tag for the local-IPC path (TransportKind::Unix — spec
 * TRANSPORT_UNIX.md; one wire value covers both Unix domain socket and
 * Windows Named Pipe). */
export const TRANSPORT_KIND_UNIX = 0x03;
/** Binding mode for the browser path (BindingMode::TlsNoExport). */
export const BINDING_MODE_TLS_NO_EXPORT = 0x02;
/** Binding mode for local IPC — no TLS at all (BindingMode::None). Same
 * value the native `tcp+plain` profile uses; `tls_exporter_or_zeros` is
 * always 32 zero bytes for this mode. */
export const BINDING_MODE_NONE = 0x00;
/** Supported protocol version (ProtocolVersion::V1). */
export const SUPPORTED_VERSION = 0x01;

/**
 * Query-language wire version the client emits in every
 * `query_version`-carrying DbRequest envelope (`execute`, `tx_begin`,
 * `tx_execute`, `tx_commit`). MUST track `CURRENT_QUERY_LANG_VERSION` in
 * `crates/shamir-query-types/src/wire/db_message.rs` — v4 opts into required
 * committed visibility metadata and adds typed transaction options.
 */
export const CURRENT_QUERY_LANG_VERSION = 4;

/** KDF parameters as carried in the server `challenge`. */
export interface KdfParams {
  memoryKb: number;
  time: number;
  parallelism: number;
  argon2Version: number;
}

export function validateKdfParams(kdf: KdfParams): void {
  if (
    !Number.isInteger(kdf.memoryKb) || kdf.memoryKb < 19456 || kdf.memoryKb > 262144 ||
    !Number.isInteger(kdf.time) || kdf.time < 2 || kdf.time > 8 ||
    !Number.isInteger(kdf.parallelism) || kdf.parallelism < 1 || kdf.parallelism > 8 ||
    !Number.isInteger(kdf.argon2Version) || kdf.argon2Version !== ARGON2_VERSION_13
  ) {
    throw new Error('kdf_params_rejected');
  }
}

/** Inputs needed to rebuild the canonical `auth_message`. */
export interface AuthMessageChallenge {
  serverNonce: Uint8Array;
  salt: Uint8Array;
  kdf: KdfParams;
  /** 32-byte TLS exporter, or 32 zero bytes for the browser path. */
  tlsExporterOrZeros?: Uint8Array;
  transportKind?: number;
  bindingMode?: number;
  supportedVersion?: number;
}

function u16be(n: number): Uint8Array {
  const b = new Uint8Array(2);
  b[0] = (n >>> 8) & 0xff;
  b[1] = n & 0xff;
  return b;
}

function u32be(n: number): Uint8Array {
  const b = new Uint8Array(4);
  b[0] = (n >>> 24) & 0xff;
  b[1] = (n >>> 16) & 0xff;
  b[2] = (n >>> 8) & 0xff;
  b[3] = n & 0xff;
  return b;
}

function concat(parts: Uint8Array[]): Uint8Array {
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

function xor(a: Uint8Array, b: Uint8Array): Uint8Array {
  if (a.length !== b.length) throw new Error('xor length mismatch');
  const out = new Uint8Array(a.length);
  for (let i = 0; i < a.length; i++) out[i] = a[i] ^ b[i];
  return out;
}

/**
 * Build the canonical `auth_message` byte string (spec §4.1).
 *
 * Layout (mirrors auth_message.rs::build):
 *   "SHAMIR-AUTH-v1"                       (14 bytes)
 *   u16_be(byte_len(username_nfc)) || username_nfc
 *   client_nonce(32) || server_nonce(32) || salt(16)
 *   u32_be(memory_kb) || u32_be(time) || u32_be(parallelism)
 *   u8(argon2_version) || u8(transport_kind) || u8(binding_mode)
 *   tls_exporter_or_zeros(32) || u8(supported_version)
 *
 * No platform dependency — pure byte arithmetic.
 */
export function buildAuthMessage(
  username: string,
  clientNonce: Uint8Array,
  challenge: AuthMessageChallenge,
): Uint8Array {
  const userBytes = new TextEncoder().encode(username);
  if (userBytes.length > 255) {
    throw new Error('username > 255 bytes after UTF-8 encoding');
  }
  if (clientNonce.length !== 32) {
    throw new Error(`client_nonce must be 32 bytes, got ${clientNonce.length}`);
  }
  if (challenge.serverNonce.length !== 32) {
    throw new Error(
      `server_nonce must be 32 bytes, got ${challenge.serverNonce.length}`,
    );
  }
  if (challenge.salt.length !== 16) {
    throw new Error(`salt must be 16 bytes, got ${challenge.salt.length}`);
  }
  const exporter = challenge.tlsExporterOrZeros ?? new Uint8Array(32);
  if (exporter.length !== 32) {
    throw new Error('tls_exporter_or_zeros must be 32 bytes');
  }

  return concat([
    AUTH_V1,
    u16be(userBytes.length),
    userBytes,
    clientNonce,
    challenge.serverNonce,
    challenge.salt,
    u32be(challenge.kdf.memoryKb),
    u32be(challenge.kdf.time),
    u32be(challenge.kdf.parallelism),
    Uint8Array.of(challenge.kdf.argon2Version & 0xff),
    Uint8Array.of((challenge.transportKind ?? TRANSPORT_KIND_WS) & 0xff),
    Uint8Array.of((challenge.bindingMode ?? BINDING_MODE_TLS_NO_EXPORT) & 0xff),
    exporter,
    Uint8Array.of((challenge.supportedVersion ?? SUPPORTED_VERSION) & 0xff),
  ]);
}

/**
 * Inputs needed to build the changePassword `auth_message_cp` (spec §12.5).
 * Mirrors `ChangePwAuthMessageInputs` in changepw.rs.
 */
export interface CpAuthMessageInputs {
  /** Already-normalized (NFC) username. */
  username: string;
  /** Active 32-byte session id. */
  sessionId: Uint8Array;
  /** Client-supplied per-request 32-byte CSPRNG nonce. */
  clientNonceCp: Uint8Array;
  /** Server-issued per-request 32-byte CSPRNG nonce. */
  serverNonceCp: Uint8Array;
  /** User's current 16-byte Argon2id salt. */
  salt: Uint8Array;
  /** User's current KDF parameters. */
  kdf: KdfParams;
  /** Transport tag snapshotted at session creation. */
  transportKind: number;
  /** Binding mode snapshotted at session creation. */
  bindingMode: number;
  /** 32-byte `channel_binding_at_auth` from the active session (zeros for WS path). */
  channelBindingAtAuth: Uint8Array;
}

/**
 * Build the canonical `auth_message_cp` byte string for the changePassword
 * flow (spec §12.5). Mirrors changepw.rs::build_auth_message_cp — different
 * domain tag and inputs from login's `buildAuthMessage` (notably includes
 * the live `session_id` and omits `supported_version`).
 *
 * Layout (mirrors changepw.rs::build_auth_message_cp):
 *   "SHAMIR-CHGPW-v1"                        (15 bytes)
 *   u16_be(byte_len(username_nfc)) || username_nfc
 *   session_id(32) || client_nonce_cp(32) || server_nonce_cp(32) || salt(16)
 *   u32_be(memory_kb) || u32_be(time) || u32_be(parallelism)
 *   u8(argon2_version) || u8(transport_kind) || u8(binding_mode)
 *   channel_binding_at_auth(32)
 *
 * No platform dependency — pure byte arithmetic.
 */
export function buildAuthMessageCp(inputs: CpAuthMessageInputs): Uint8Array {
  const userBytes = new TextEncoder().encode(inputs.username);
  if (userBytes.length > 255) {
    throw new Error('username > 255 bytes after UTF-8 encoding');
  }
  if (inputs.sessionId.length !== 32) {
    throw new Error(
      `session_id must be 32 bytes, got ${inputs.sessionId.length}`,
    );
  }
  if (inputs.clientNonceCp.length !== 32) {
    throw new Error(
      `client_nonce_cp must be 32 bytes, got ${inputs.clientNonceCp.length}`,
    );
  }
  if (inputs.serverNonceCp.length !== 32) {
    throw new Error(
      `server_nonce_cp must be 32 bytes, got ${inputs.serverNonceCp.length}`,
    );
  }
  if (inputs.salt.length !== 16) {
    throw new Error(`salt must be 16 bytes, got ${inputs.salt.length}`);
  }
  if (inputs.channelBindingAtAuth.length !== 32) {
    throw new Error(
      `channel_binding_at_auth must be 32 bytes, got ${inputs.channelBindingAtAuth.length}`,
    );
  }

  return concat([
    CHGPW_V1,
    u16be(userBytes.length),
    userBytes,
    inputs.sessionId,
    inputs.clientNonceCp,
    inputs.serverNonceCp,
    inputs.salt,
    u32be(inputs.kdf.memoryKb),
    u32be(inputs.kdf.time),
    u32be(inputs.kdf.parallelism),
    Uint8Array.of(inputs.kdf.argon2Version & 0xff),
    Uint8Array.of(inputs.transportKind & 0xff),
    Uint8Array.of(inputs.bindingMode & 0xff),
    inputs.channelBindingAtAuth,
  ]);
}

/** Result of `computeClientProof`. */
export interface ClientProofResult {
  clientProof: Uint8Array;
  serverKey: Uint8Array;
  /** SCRAM `stored_key` = SHA256(client_key) — useful for invariant tests. */
  storedKey: Uint8Array;
  /** Raw `client_key` — useful for invariant tests. */
  clientKey: Uint8Array;
}

/**
 * Derive the SCRAM proof + server key (spec §5.1.3 / crypto.rs):
 *   salted_password  = argon2id(password, salt, kdf)
 *   client_key       = HMAC-SHA256(salted_password, "Client Key")
 *   server_key       = HMAC-SHA256(salted_password, "Server Key")
 *   stored_key       = SHA256(client_key)
 *   client_signature = HMAC-SHA256(stored_key, auth_message)
 *   client_proof     = client_key XOR client_signature
 *
 * All HMAC/SHA256/argon2id calls delegated to `platform`.
 */
export async function computeClientProof(
  platform: Platform,
  password: string,
  salt: Uint8Array,
  kdfParams: KdfParams,
  authMessage: Uint8Array,
): Promise<ClientProofResult> {
  validateKdfParams(kdfParams);
  const enc = new TextEncoder();
  const passwordBytes = enc.encode(password);
  const saltedPassword = await platform.argon2id(passwordBytes, salt, {
    memoryKb: kdfParams.memoryKb,
    time: kdfParams.time,
    parallelism: kdfParams.parallelism,
  });
  const clientKey = platform.hmacSha256(saltedPassword, enc.encode('Client Key'));
  const serverKey = platform.hmacSha256(saltedPassword, enc.encode('Server Key'));
  const storedKey = platform.sha256(clientKey);
  const clientSignature = platform.hmacSha256(storedKey, authMessage);
  const clientProof = xor(clientKey, clientSignature);
  return { clientProof, serverKey, storedKey, clientKey };
}

/**
 * Verify the server signature: HMAC-SHA256(server_key, auth_message) must
 * equal the `server_signature` returned in `auth_ok`. Constant-time via
 * `platform.timingSafeEqual`.
 */
export function verifyServerSignature(
  platform: Platform,
  serverKey: Uint8Array,
  authMessage: Uint8Array,
  receivedSig: Uint8Array,
): boolean {
  const expected = platform.hmacSha256(serverKey, authMessage);
  if (expected.length !== receivedSig.length) return false;
  return platform.timingSafeEqual(expected, receivedSig);
}
