/**
 * Unit tests for the server-identity pin of the SCRAM handshake
 * (openScramSession): the auth_ok `identity_sig` must be a valid Ed25519
 * signature over the canonical identity input, and the server pub key must
 * match a pinned SHA-256 pin when one is provided. Real Ed25519, fake
 * socket + fake platform, no network.
 */

import { describe, it, expect } from 'vitest';
import * as ed from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha512';
import { openScramSession, buildIdentityInput, IDENTITY_V1 } from '../open.js';
import { browserWsBinding } from '../binding.js';
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

// The tag is exported mainly for reference; keep it referenced so the import
// stays meaningful without asserting on its bytes.
void IDENTITY_V1;

// ─── FakeSocket (mirrors protocol.test.ts) ──────────────────────────────────

class FakeSocket implements Socket {
  sent: Uint8Array[] = [];
  messageHandler: (_data: Uint8Array) => void = () => {};
  closeHandler: (_err?: Error) => void = () => {};
  closed = false;

  send(data: Uint8Array): void {
    if (this.closed) throw new Error('connection closed');
    this.sent.push(data);
  }
  onMessage(h: (data: Uint8Array) => void): void {
    this.messageHandler = h;
  }
  onClose(h: (err?: Error) => void): void {
    this.closeHandler = h;
  }
  close(): Promise<void> {
    this.closed = true;
    this.closeHandler();
    return Promise.resolve();
  }

  /** Push a length-prefixed frame as if received from the server. */
  pushFrame(body: Uint8Array): void {
    const buf = new Uint8Array(4 + body.length);
    const len = body.length >>> 0;
    buf[0] = (len >>> 24) & 0xff;
    buf[1] = (len >>> 16) & 0xff;
    buf[2] = (len >>> 8) & 0xff;
    buf[3] = len & 0xff;
    buf.set(body, 4);
    this.messageHandler(buf);
  }
}

// ─── FakePlatform (mirrors protocol.test.ts) ────────────────────────────────

/**
 * FNV-1a hash folded into a u32 (deterministic, non-crypto).
 */
function fnv1a32(bytes: Uint8Array): number {
  let h = 0x811c9dc5 >>> 0;
  for (let i = 0; i < bytes.length; i++) {
    h = Math.imul(h ^ bytes[i], 0x01000193) >>> 0;
  }
  return h >>> 0;
}

/** Write big-endian u32 into a Uint8Array at offset. */
function wrU32(b: Uint8Array, off: number, v: number): void {
  b[off] = (v >>> 24) & 0xff;
  b[off + 1] = (v >>> 16) & 0xff;
  b[off + 2] = (v >>> 8) & 0xff;
  b[off + 3] = v & 0xff;
}

/** Deterministic "HMAC-SHA256" stand-in: stable 32-byte digest of key||data. */
function fakeHmac(key: Uint8Array, data: Uint8Array): Uint8Array {
  const cat = new Uint8Array(key.length + data.length);
  cat.set(key, 0);
  cat.set(data, key.length);

  let s0 = fnv1a32(cat);
  let s1 = fnv1a32(cat);
  const tag = new Uint8Array(1);
  tag[0] = 0x5a;
  const cat2 = new Uint8Array(cat.length + 1);
  cat2.set(cat, 0);
  cat2.set(tag, cat.length);
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
  const tag = new Uint8Array(3);
  tag[0] = p.memoryKb & 0xff;
  tag[1] = p.time & 0xff;
  tag[2] = p.parallelism & 0xff;
  const cat = new Uint8Array(password.length + salt.length + tag.length);
  cat.set(password, 0);
  cat.set(salt, password.length);
  cat.set(tag, password.length + salt.length);
  return fakeHmac(new TextEncoder().encode('argon2'), cat);
}

/**
 * Fake platform with a dialable socket: `openSocket` returns a session link
 * over the given FakeSocket with the browser WS binding. Counts invocations
 * so tests can assert refuse-before-dial ordering.
 */
function makeFakePlatform(socket: FakeSocket): { platform: Platform; dialCount: () => number } {
  let dials = 0;
  const link = { socket, binding: browserWsBinding() };
  const platform = {
    hmacSha256: (k: Uint8Array, d: Uint8Array) => fakeHmac(k, d),
    sha256: (d: Uint8Array) => fakeHmac(new Uint8Array(0), d),
    randomBytes: (n: number) => {
      const out = new Uint8Array(n);
      for (let i = 0; i < n; i++) out[i] = (i * 31 + 7) & 0xff;
      return out;
    },
    timingSafeEqual: (a: Uint8Array, b: Uint8Array) => {
      if (a.length !== b.length) return false;
      let diff = 0;
      for (let i = 0; i < a.length; i++) diff |= a[i] ^ b[i];
      return diff === 0;
    },
    argon2id: ((pw: Uint8Array, salt: Uint8Array, p: Argon2Params) =>
      fakeArgon2(pw, salt, p)) as unknown as Platform['argon2id'],
    openSocket: async () => {
      dials += 1;
      return link;
    },
  } as unknown as Platform;
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

/**
 * The deterministic client_nonce the protocol will emit for a fresh fake
 * platform (randomBytes is a pure function of its length).
 */
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

/**
 * Valid identity_sig for PUB: Ed25519 over buildIdentityInput with the
 * browser profile binding (transportKind 0x02, bindingMode 0x02, zero
 * exporter).
 */
function validIdentitySig(platform: Platform): Uint8Array {
  return ed.sign(
    buildIdentityInput(platform, {
      serverPubKey: PUB,
      transportKind: 0x02,
      bindingMode: 0x02,
      tlsExporterOrZeros: new Uint8Array(32),
      authMessage: rebuildAuthMessage(),
      sessionId: SESSION_ID,
      expiresAtNs: EXPIRES,
    }),
    PRIV,
  );
}

/**
 * Assemble the auth_ok body whose server_signature mirrors the fake
 * platform's key schedule (argon2 → "Server Key" HMAC → HMAC over the
 * auth_message) — the same computation protocol.test.ts performs.
 */
function authOkBody(identitySig: Uint8Array): Uint8Array {
  const platform = makeFakePlatform(new FakeSocket()).platform;
  const authMessage = rebuildAuthMessage();
  // The fake argon2id is sync under the hood; call it through a sync cast.
  const syncArgon = platform.argon2id as unknown as (
    pw: Uint8Array, s: Uint8Array, p: Argon2Params,
  ) => Uint8Array;
  const salted = syncArgon(
    new TextEncoder().encode(PASSWORD),
    SALT,
    { memoryKb: KDF.memoryKb, time: KDF.time, parallelism: KDF.parallelism },
  );
  const serverKey = platform.hmacSha256(salted, new TextEncoder().encode('Server Key'));
  const serverSignature = platform.hmacSha256(serverKey, authMessage);
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

  it('accepts a correct signature with a matching pin and exposes the captured pin', async () => {
    const socket = new FakeSocket();
    const { platform } = makeFakePlatform(socket);
    const expectedPin = platform.sha256(PUB);
    setup(socket, authOkBody(validIdentitySig(platform)));

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
    setup(socket, authOkBody(validIdentitySig(platform)));

    const opened = await openScramSession(platform, { ...HOST });

    expect(new Uint8Array(opened.trustedPin)).toEqual(platform.sha256(PUB));
  });

  it('captured pin equals SHA256(pub_key) on first contact', async () => {
    const socket = new FakeSocket();
    const { platform } = makeFakePlatform(socket);
    setup(socket, authOkBody(validIdentitySig(platform)));

    const opened = await openScramSession(platform, { ...HOST });

    expect(new Uint8Array(opened.trustedPin)).toEqual(platform.sha256(PUB));
  });

  it('refuses a flipped signature byte with the typed error', async () => {
    const socket = new FakeSocket();
    const { platform } = makeFakePlatform(socket);
    const sig = validIdentitySig(platform);
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
    setup(socket, authOkBody(validIdentitySig(platform)));

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
    setup(socket, authOkBody(validIdentitySig(platform)));

    const opened = await openScramSession(platform, {
      ...HOST,
      trustedPin: platform.sha256(PUB),
      acceptNewHost: false,
    });

    expect(new Uint8Array(opened.trustedPin)).toEqual(platform.sha256(PUB));
  });
});
