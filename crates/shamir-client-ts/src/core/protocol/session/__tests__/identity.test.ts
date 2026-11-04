/**
 * Unit tests for the server-identity pin of the SCRAM handshake
 * (openScramSession): the auth_ok `identity_sig` must be a valid Ed25519
 * signature over the canonical identity input, and the server pub key must
 * match a pinned SHA-256 pin when one is provided.
 *
 * The oracle is INDEPENDENT of production code: real SHA-256 + real Ed25519,
 * and the signed transcript is built BY HAND from the Rust reference
 * (crates/shamir-connect/src/common/user/identity.rs::build_identity_input,
 * tag from common/primitives/domain_tags.rs::IDENTITY_V1) — production
 * `verifyAuthOkIdentity` can only accept the fixture if its own
 * `buildIdentityInput` produces byte-identical output. Fake socket + fake
 * platform (SCRAM key schedule only), no network.
 */

import { describe, it, expect } from 'vitest';
import * as ed from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha512';
import { sha256 } from '@noble/hashes/sha256';
import { openScramSession, buildIdentityInput, IDENTITY_V1 } from '../open.js';
import { buildAuthMessage, ARGON2_VERSION_13 } from '../../scram.js';
import { encode, decode } from '../../framing.js';
import { ServerIdentityChangedError } from '../../../support/errors.js';
import type { Socket, Platform, Argon2Params } from '../../../support/platform.js';

if (ed.etc.sha512Sync === undefined) {
  ed.etc.sha512Sync = (...m) => sha512(ed.etc.concatBytes(...m));
}

// ─── constants ──────────────────────────────────────────────────────────────

const PRIV = new Uint8Array(32).fill(5);
const PUB = ed.getPublicKey(PRIV);
const SESSION_ID = new Uint8Array(32).fill(0x55);
const SALT = new Uint8Array(16).fill(0xa5);
const SERVER_NONCE = new Uint8Array(32).fill(0x5a);
const KDF = {
  memoryKb: 65536,
  time: 3,
  parallelism: 1,
  argon2Version: ARGON2_VERSION_13,
};
const EXPIRES = BigInt('1830000000000000000');
const USERNAME = 'alice';
const PASSWORD = 'correct horse battery staple';

/**
 * Hard-coded bytes of the Rust reference's `IDENTITY_V1` domain tag
 * (crates/shamir-connect/src/common/primitives/domain_tags.rs, ASCII
 * "SHAMIR-IDENTITY-v1", spec §5.2.4). The production constant is asserted
 * AGAINST this, not trusted.
 */
const IDENTITY_V1_BYTES = new Uint8Array([
  0x53, 0x48, 0x41, 0x4d, 0x49, 0x52, 0x2d, 0x49, // "SHAMIR-I"
  0x44, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x2d, // "DENTITY-"
  0x76, 0x31, // "v1"
]);

/**
 * Hard-coded SHA-256("") — the standard empty-input vector. Pins the fake
 * platform's `sha256` (the pin/identity oracle) to the REAL digest: the
 * FNV stand-in used for the SCRAM HMAC cannot produce it.
 */
const SHA256_EMPTY = new Uint8Array([
  0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
  0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
  0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
  0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
]);

// ─── FakeSocket (deferred delivery — mirrors connect-local.test.ts) ─────────

/**
 * Frames pushed before the handshake starts are QUEUED until a message
 * handler is installed (`WsFramer` registers one in its constructor) and
 * delivered then — never dropped.
 */
class FakeSocket implements Socket {
  sent: Uint8Array[] = [];
  closeHandler: (_err?: Error) => void = () => {};
  closed = false;
  private messageHandler: (_data: Uint8Array) => void = () => {};
  private pending: Uint8Array[] = [];
  private handlerRegistered = false;

  send(data: Uint8Array): void {
    if (this.closed) throw new Error('connection closed');
    this.sent.push(data);
  }
  onMessage(h: (data: Uint8Array) => void): void {
    this.messageHandler = h;
    this.handlerRegistered = true;
    const queued = this.pending;
    this.pending = [];
    for (const f of queued) this.messageHandler(f);
  }
  onClose(h: (err?: Error) => void): void {
    this.closeHandler = h;
  }
  close(): Promise<void> {
    this.closed = true;
    this.closeHandler();
    return Promise.resolve();
  }

  /** Queue a length-prefixed frame as if received from the server. */
  pushFrame(body: Uint8Array): void {
    const buf = new Uint8Array(4 + body.length);
    const len = body.length >>> 0;
    buf[0] = (len >>> 24) & 0xff;
    buf[1] = (len >>> 16) & 0xff;
    buf[2] = (len >>> 8) & 0xff;
    buf[3] = len & 0xff;
    buf.set(body, 4);
    if (this.handlerRegistered) this.messageHandler(buf);
    else this.pending.push(buf);
  }
}

// ─── fake SCRAM crypto (mirrors protocol.test.ts) ───────────────────────────

/** FNV-1a folded into u32 — deterministic stand-in for HMAC-SHA256 only. */
function fnv1a32(bytes: Uint8Array): number {
  let h = 0x811c9dc5 >>> 0;
  for (let i = 0; i < bytes.length; i++) {
    h = Math.imul(h ^ bytes[i], 0x01000193) >>> 0;
  }
  return h >>> 0;
}

function wrU32(b: Uint8Array, off: number, v: number): void {
  b[off] = (v >>> 24) & 0xff;
  b[off + 1] = (v >>> 16) & 0xff;
  b[off + 2] = (v >>> 8) & 0xff;
  b[off + 3] = v & 0xff;
}

/** Deterministic HMAC-SHA256 stand-in: stable 32-byte digest of key||data. */
function fakeHmac(key: Uint8Array, data: Uint8Array): Uint8Array {
  const cat = new Uint8Array(key.length + data.length);
  cat.set(key, 0);
  cat.set(data, key.length);
  let s0 = fnv1a32(cat);
  let s1 = fnv1a32(cat);
  const cat2 = new Uint8Array(cat.length + 1);
  cat2.set(cat, 0);
  cat2.set(new Uint8Array([0x5a]), cat.length);
  s1 = fnv1a32(cat2);
  const out = new Uint8Array(32);
  for (let i = 0; i < 8; i++) {
    s0 = (Math.imul(s0, 0x01000193) ^ (i + 0x70)) >>> 0;
    s1 = (Math.imul(s1, 0x01000193) ^ (i + 0xe0)) >>> 0;
    wrU32(out, i * 4, (s0 ^ s1) >>> 0);
  }
  return out;
}

/** Deterministic "salted password" stand-in for argon2id. */
function fakeArgon2(password: Uint8Array, salt: Uint8Array, p: Argon2Params): Uint8Array {
  const tag = new Uint8Array([
    p.memoryKb & 0xff,
    p.time & 0xff,
    p.parallelism & 0xff,
  ]);
  const cat = new Uint8Array(password.length + salt.length + tag.length);
  cat.set(password, 0);
  cat.set(salt, password.length);
  cat.set(tag, password.length + salt.length);
  return fakeHmac(new TextEncoder().encode('argon2'), cat);
}

/**
 * Fake platform: REAL SHA-256 (the pin/identity oracle must hash exactly
 * like the server does), deterministic stand-ins only for the SCRAM key
 * schedule (HMAC-SHA256/argon2id — fixture and client compute them
 * identically over the same platform). `openSocket` returns the given
 * FakeSocket itself (a real `Socket`, not a native-link wrapper) and
 * counts dials so tests can assert refuse-before-dial ordering.
 */
function makeFakePlatform(socket: FakeSocket): { platform: Platform; dialCount: () => number } {
  let dials = 0;
  const platform: Platform = {
    hmacSha256: (k, d) => fakeHmac(k, d),
    sha256: (d) => sha256(d),
    randomBytes: (n) => {
      const out = new Uint8Array(n);
      for (let i = 0; i < n; i++) out[i] = (i * 31 + 7) & 0xff;
      return out;
    },
    timingSafeEqual: (a, b) => {
      if (a.length !== b.length) return false;
      let diff = 0;
      for (let i = 0; i < a.length; i++) diff |= a[i] ^ b[i];
      return diff === 0;
    },
    argon2id: async (pw, salt, p) => fakeArgon2(pw, salt, p),
    openSocket: async () => {
      dials += 1;
      return socket;
    },
  };
  return { platform, dialCount: () => dials };
}

// ─── wire-frame builders ────────────────────────────────────────────────────

/** Build a valid Challenge frame body (msgpack array). */
function challengeFrame(): Uint8Array {
  return encode([
    SALT,
    KDF.memoryKb,
    KDF.time,
    KDF.parallelism,
    KDF.argon2Version,
    SERVER_NONCE,
  ]);
}

/** The deterministic client_nonce the fake platform's randomBytes emits. */
function clientNonce(): Uint8Array {
  return new Uint8Array(32).map((_, i) => (i * 31 + 7) & 0xff);
}

/** Rebuild the auth_message the client builds internally. */
function rebuildAuthMessage(): Uint8Array {
  return buildAuthMessage(USERNAME.normalize('NFC'), clientNonce(), {
    serverNonce: SERVER_NONCE,
    salt: SALT,
    kdf: KDF,
    transportKind: 0x02,
    bindingMode: 0x02,
    tlsExporterOrZeros: new Uint8Array(32),
  });
}

function concatBytes(parts: Uint8Array[]): Uint8Array {
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

function u64be(n: bigint): Uint8Array {
  const out = new Uint8Array(8);
  let v = n;
  for (let i = 7; i >= 0; i -= 1) {
    out[i] = Number(v & 0xffn);
    v >>= 8n;
  }
  return out;
}

/**
 * The identity transcript built BY HAND from the Rust reference
 * (identity.rs::build_identity_input):
 *   IDENTITY_V1 || SHA256(pub) || u8(kind) || u8(mode) || exporter(32)
 *   || auth_message || session_id(32) || u64_be(expires)
 * Browser profile: TransportKind::Ws (0x02), BindingMode::TlsNoExport
 * (0x02), 32 zero bytes as tls_exporter_or_zeros.
 */
function handBuiltIdentityInput(): Uint8Array {
  return concatBytes([
    IDENTITY_V1_BYTES,
    sha256(PUB),
    Uint8Array.of(0x02),
    Uint8Array.of(0x02),
    new Uint8Array(32),
    rebuildAuthMessage(),
    SESSION_ID,
    u64be(EXPIRES),
  ]);
}

/** Valid identity_sig for PUB: real Ed25519 over the hand-built transcript. */
function validIdentitySig(): Uint8Array {
  return ed.sign(handBuiltIdentityInput(), PRIV);
}

/**
 * Assemble the auth_ok body whose server_signature mirrors the fake
 * platform's key schedule (argon2 → "Server Key" HMAC → HMAC over the
 * auth_message) — the same computation protocol.test.ts performs.
 */
function authOkBody(identitySig: Uint8Array): Uint8Array {
  const authMessage = rebuildAuthMessage();
  const salted = fakeArgon2(
    new TextEncoder().encode(PASSWORD),
    SALT,
    { memoryKb: KDF.memoryKb, time: KDF.time, parallelism: KDF.parallelism },
  );
  const serverKey = fakeHmac(salted, new TextEncoder().encode('Server Key'));
  const serverSignature = fakeHmac(serverKey, authMessage);
  return encode([
    serverSignature,
    PUB,
    identitySig,
    SESSION_ID,
    EXPIRES,
    new Uint8Array(0),
    0,
    2,
  ]);
}

/** Queue both server frames (challenge + auth_ok) before dialing. */
function setup(socket: FakeSocket, authOk: Uint8Array): void {
  socket.pushFrame(challengeFrame());
  socket.pushFrame(authOk);
}

// ─── tests ──────────────────────────────────────────────────────────────────

describe('server identity pin (auth_ok identity_sig)', () => {
  const HOST = { host: 'h', port: 1, username: USERNAME, password: PASSWORD };

  it('the identity oracle is real SHA-256 and the production tag is the Rust IDENTITY_V1', () => {
    const { platform } = makeFakePlatform(new FakeSocket());
    // Hard-coded standard vector: the platform sha256 must be the REAL
    // digest (the SCRAM stand-in cannot produce it) — this is what makes
    // every pin assertion below non-circular.
    expect(platform.sha256(new Uint8Array(0))).toEqual(SHA256_EMPTY);
    expect(sha256(new Uint8Array(0))).toEqual(SHA256_EMPTY);
    // Byte-identical to domain_tags.rs::IDENTITY_V1.
    expect(IDENTITY_V1).toEqual(IDENTITY_V1_BYTES);
    expect(IDENTITY_V1.length).toBe(18);
  });

  it('buildIdentityInput matches the hand-built Rust transcript layout', () => {
    const { platform } = makeFakePlatform(new FakeSocket());
    const built = buildIdentityInput(platform, {
      serverPubKey: PUB,
      transportKind: 0x02,
      bindingMode: 0x02,
      tlsExporterOrZeros: new Uint8Array(32),
      authMessage: rebuildAuthMessage(),
      sessionId: SESSION_ID,
      expiresAtNs: EXPIRES,
    });
    // 18 + 32 + 1 + 1 + 32 + auth_message + 32 + 8 — the identity.rs sizes.
    expect(built.length).toBe(
      18 + 32 + 1 + 1 + 32 + rebuildAuthMessage().length + 32 + 8,
    );
    expect(built).toEqual(handBuiltIdentityInput());
  });

  it('accepts a correct signature with a matching pin and exposes the captured pin', async () => {
    const socket = new FakeSocket();
    const { platform } = makeFakePlatform(socket);
    const expectedPin = sha256(PUB);
    setup(socket, authOkBody(validIdentitySig()));

    const opened = await openScramSession(platform, {
      ...HOST,
      trustedPin: expectedPin,
    });

    expect(new Uint8Array(opened.trustedPin)).toEqual(expectedPin);
    expect(opened.binding.bindingMode).toBe(2);
  });

  it('accepts first contact without a pin (trust-on-first-use default) and captures the pin', async () => {
    const socket = new FakeSocket();
    const { platform } = makeFakePlatform(socket);
    setup(socket, authOkBody(validIdentitySig()));

    const opened = await openScramSession(platform, { ...HOST });

    expect(new Uint8Array(opened.trustedPin)).toEqual(sha256(PUB));
  });

  it('captured pin equals SHA256(pub_key) on first contact', async () => {
    const socket = new FakeSocket();
    const { platform } = makeFakePlatform(socket);
    setup(socket, authOkBody(validIdentitySig()));

    const opened = await openScramSession(platform, { ...HOST });

    expect(new Uint8Array(opened.trustedPin)).toEqual(sha256(PUB));
    // The transcript was signed over the hand-built bytes and production
    // verified them — the server pub key round-trips unmodified.
    expect(opened.serverPubKey).toEqual(PUB);
  });

  it('refuses a flipped signature byte with the typed error', async () => {
    const socket = new FakeSocket();
    const { platform } = makeFakePlatform(socket);
    const sig = validIdentitySig();
    sig[0] ^= 0x01;
    setup(socket, authOkBody(sig));

    let caught: unknown;
    try {
      await openScramSession(platform, { ...HOST });
    } catch (e) {
      caught = e;
    }

    expect(caught).toBeInstanceOf(ServerIdentityChangedError);
    expect((caught as ServerIdentityChangedError).reason).toBe('identity_sig_invalid');
    expect((caught as ServerIdentityChangedError).code).toBe('server_identity_changed');
    expect(socket.closed).toBe(true);
  });

  it('refuses a wrong pin with the typed error and sends nothing after the server hello', async () => {
    const socket = new FakeSocket();
    const { platform } = makeFakePlatform(socket);
    setup(socket, authOkBody(validIdentitySig()));

    let caught: unknown;
    try {
      await openScramSession(platform, {
        ...HOST,
        trustedPin: new Uint8Array(32).fill(0x99),
      });
    } catch (e) {
      caught = e;
    }

    expect(caught).toBeInstanceOf(ServerIdentityChangedError);
    expect((caught as ServerIdentityChangedError).reason).toBe('pin_mismatch');
    // auth_init + client_proof — nothing sent after the server's frames.
    expect(socket.sent.length).toBe(2);
    expect(socket.closed).toBe(true);

    const init = decode(socket.sent[0].subarray(4)) as {
      user: string;
      client_nonce: Uint8Array;
      binding_mode: number;
    };
    expect(init.user).toBe(USERNAME);
    expect(new Uint8Array(init.client_nonce).length).toBe(32);
    expect(init.binding_mode).toBe(2);

    const proof = decode(socket.sent[1].subarray(4)) as { client_proof: Uint8Array };
    expect(proof.client_proof).toBeDefined();
  });

  it('refuses first contact without acceptNewHost before opening a socket', async () => {
    const socket = new FakeSocket();
    const { platform, dialCount } = makeFakePlatform(socket);

    await expect(
      openScramSession(platform, {
        host: 'h',
        port: 1,
        username: USERNAME,
        password: PASSWORD,
        acceptNewHost: false,
      }),
    ).rejects.toThrow(/refusing first contact/);

    expect(dialCount()).toBe(0);
  });

  it('acceptNewHost:false with a trustedPin still connects', async () => {
    const socket = new FakeSocket();
    const { platform } = makeFakePlatform(socket);
    setup(socket, authOkBody(validIdentitySig()));

    const opened = await openScramSession(platform, {
      ...HOST,
      trustedPin: sha256(PUB),
      acceptNewHost: false,
    });

    expect(new Uint8Array(opened.trustedPin)).toEqual(sha256(PUB));
  });
});
