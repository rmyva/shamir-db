/**
 * Unit tests for `ShamirClient.connectLocal` — the local-IPC (Unix domain
 * socket / Windows Named Pipe, spec TRANSPORT_UNIX.md) connect path.
 *
 * No live server, no real crypto, no real `net.Socket` — drives a real
 * `ShamirClient.connectLocal` against a `FakePlatform` whose
 * `openIpcSocket` resolves to a `FakeSocket` (mirrors the deterministic
 * crypto harness in `protocol.test.ts`, duplicated here rather than
 * shared — matches this codebase's existing per-file FakeSocket
 * convention, see `client.test.ts`).
 */

import { describe, it, expect, afterEach } from 'vitest';
import type { Socket, Platform, Argon2Params } from '../../support/platform.js';
import { encode, decode } from '../../protocol/framing.js';
import { ShamirClient } from '../client.js';
import type { ConnectLocalOptions } from '../../types/index.js';
import { ARGON2_VERSION_13, TRANSPORT_KIND_UNIX, BINDING_MODE_NONE } from '../../protocol/scram.js';
import { buildAuthMessage } from '../../protocol/scram.js';
import { openIpcSocket } from '../../../platform/node.js';
import * as ed from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha512';
import { sha256 } from '@noble/hashes/sha256';
import { buildIdentityInput } from '../../protocol/session/open.js';

if (ed.etc.sha512Sync === undefined) {
  ed.etc.sha512Sync = (...m) => sha512(ed.etc.concatBytes(...m));
}

const SERVER_PRIV = new Uint8Array(32).fill(7);
const SERVER_PUB = ed.getPublicKey(SERVER_PRIV);

const openedClients: ShamirClient[] = [];

describe('openIpcSocket endpoint validation', () => {
  it('rejects invalid platform addresses before attempting I/O', async () => {
    const addresses = process.platform === 'win32'
      ? ['shamir-db', '\\\\.\\pipe\\nested\\component', '\\\\.\\pipe\\bad\0name']
      : ['relative.sock', '/tmp/with\0nul'];
    for (const address of addresses) {
      await expect(openIpcSocket(address)).rejects.toThrow('Invalid IPC endpoint');
    }
  });
});


afterEach(async () => {
  while (openedClients.length > 0) {
    const c = openedClients.pop()!;
    try {
      await c.close();
    } catch {
      /* socket may already be closed */
    }
  }
});

// ─── FakeSocket (deferred delivery — mirrors client.test.ts) ───────────────

class FakeSocket implements Socket {
  sent: Uint8Array[] = [];
  private messageHandler: (_data: Uint8Array) => void = () => {};
  closeHandler: (_err?: Error) => void = () => {};
  private pending: Uint8Array[] = [];
  private _handlerRegistered = false;
  private _closed = false;

  send(data: Uint8Array): void {
    if (this._closed) throw new Error('connection closed');
    this.sent.push(data);
  }
  onMessage(h: (data: Uint8Array) => void): void {
    this.messageHandler = h;
    this._handlerRegistered = true;
    const queued = this.pending;
    this.pending = [];
    for (const f of queued) this.messageHandler(f);
  }
  onClose(h: (err?: Error) => void): void {
    this.closeHandler = h;
  }
  close(): Promise<void> {
    this._closed = true;
    this.closeHandler();
    return Promise.resolve();
  }
  pushFrame(body: Uint8Array): void {
    const buf = new Uint8Array(4 + body.length);
    const len = body.length >>> 0;
    buf[0] = (len >>> 24) & 0xff;
    buf[1] = (len >>> 16) & 0xff;
    buf[2] = (len >>> 8) & 0xff;
    buf[3] = len & 0xff;
    buf.set(body, 4);
    if (this._handlerRegistered) {
      this.messageHandler(buf);
    } else {
      this.pending.push(buf);
    }
  }
}

// ─── deterministic fake crypto (mirrors protocol.test.ts) ──────────────────

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
function fakeArgon2(password: Uint8Array, salt: Uint8Array, p: Argon2Params): Uint8Array {
  const tag = new Uint8Array([p.memoryKb & 0xff, p.time & 0xff, p.parallelism & 0xff]);
  const cat = new Uint8Array(password.length + salt.length + tag.length);
  cat.set(password, 0);
  cat.set(salt, password.length);
  cat.set(tag, password.length + salt.length);
  return fakeHmac(new TextEncoder().encode('argon2'), cat);
}

const SALT = new Uint8Array(16).fill(0xa5);
const SERVER_NONCE = new Uint8Array(32).fill(0x5a);
const KDF = { memoryKb: 65536, time: 3, parallelism: 1, argon2Version: ARGON2_VERSION_13 };
const USERNAME = 'admin';
const PASSWORD = 'correct horse battery staple';

function challengeFrame(): Uint8Array {
  return encode([SALT, KDF.memoryKb, KDF.time, KDF.parallelism, KDF.argon2Version, SERVER_NONCE]);
}

/** Builds a fake `Platform` whose `openIpcSocket` resolves to `socket`. */
function makeFakePlatform(socket: FakeSocket): Platform {
  return {
    hmacSha256: (k, d) => fakeHmac(k, d),
    // REAL SHA-256: auth_ok.identity_sig is Ed25519 over an input that
    // hashes the server pub key — a stand-in digest would make the fixture
    // signature unverifiable by the client's recomputation.
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
    argon2id: ((pw: Uint8Array, salt: Uint8Array, p: Argon2Params) =>
      fakeArgon2(pw, salt, p)) as unknown as Platform['argon2id'],
    openSocket: async () => {
      throw new Error('not used by connectLocal');
    },
    openIpcSocket: async () => socket,
  };
}

function computeServerSignature(platform: Platform, clientNonce: Uint8Array): Uint8Array {
  const authMessage = buildAuthMessage(USERNAME.normalize('NFC'), clientNonce, {
    serverNonce: SERVER_NONCE,
    salt: SALT,
    kdf: KDF,
    transportKind: TRANSPORT_KIND_UNIX,
    bindingMode: BINDING_MODE_NONE,
  });
  const syncArgon = platform.argon2id as unknown as (
    pw: Uint8Array,
    s: Uint8Array,
    p: Argon2Params,
  ) => Uint8Array;
  const salted = syncArgon(new TextEncoder().encode(PASSWORD), SALT, KDF);
  const serverKey = platform.hmacSha256(salted, new TextEncoder().encode('Server Key'));
  return platform.hmacSha256(serverKey, authMessage);
}

// ─── tests ──────────────────────────────────────────────────────────────────

describe('ShamirClient.connectLocal', () => {
  const opts: ConnectLocalOptions = {
    addr: '/run/shamir/shamir-db.sock',
    username: USERNAME,
    password: PASSWORD,
  };

  it('throws immediately when the Platform has no openIpcSocket (e.g. BrowserPlatform)', async () => {
    const platformWithoutIpc: Platform = {
      hmacSha256: (k, d) => fakeHmac(k, d),
      sha256: (d) => fakeHmac(new Uint8Array(0), d),
      randomBytes: (n) => new Uint8Array(n),
      timingSafeEqual: (a, b) => a.length === b.length,
      argon2id: async () => new Uint8Array(32),
      openSocket: async () => {
        throw new Error('not used');
      },
      // openIpcSocket intentionally omitted.
    };

    await expect(ShamirClient.connectLocal(platformWithoutIpc, opts)).rejects.toThrow(
      /does not implement openIpcSocket/,
    );
  });

  it('happy path: authenticates over the fake IPC socket and returns a ready client', async () => {
    const socket = new FakeSocket();
    const platform = makeFakePlatform(socket);

    const clientNonce = platform.randomBytes(32);
    const serverSig = computeServerSignature(platform, clientNonce);
    const sessionId = new Uint8Array(32).fill(0x11);
    const serverPubKey = SERVER_PUB;
    const expiresAtNs = BigInt('1830000000000000000');
    // Real Ed25519 identity_sig over the identity input the client
    // recomputes in verifyAuthOkIdentity (same auth_message bytes: IPC
    // binding = TRANSPORT_KIND_UNIX/BINDING_MODE_NONE, zero exporter, the
    // deterministic client_nonce). An all-zero sig fails the now-mandatory
    // verification, so the fixture must sign for real.
    const identitySig = ed.sign(
      buildIdentityInput(platform, {
        serverPubKey,
        transportKind: TRANSPORT_KIND_UNIX,
        bindingMode: BINDING_MODE_NONE,
        tlsExporterOrZeros: new Uint8Array(32),
        authMessage: buildAuthMessage(USERNAME.normalize('NFC'), clientNonce, {
          serverNonce: SERVER_NONCE,
          salt: SALT,
          kdf: KDF,
          transportKind: TRANSPORT_KIND_UNIX,
          bindingMode: BINDING_MODE_NONE,
        }),
        sessionId,
        expiresAtNs,
      }),
      SERVER_PRIV,
    );

    socket.pushFrame(challengeFrame());
    socket.pushFrame(
      encode([serverSig, serverPubKey, identitySig, sessionId, expiresAtNs]),
    );

    const client = await ShamirClient.connectLocal(platform, opts);
    openedClients.push(client);

    expect(client.sessionId()).toEqual(sessionId);
    expect(client.serverPubKeyPin()).toEqual(serverPubKey);

    // The auth_init frame sent over the fake socket must carry
    // binding_mode = BINDING_MODE_NONE (0), not the WS default.
    const initPayload = socket.sent[0].subarray(4);
    const initDecoded = decode(initPayload) as { binding_mode: number };
    expect(initDecoded.binding_mode).toBe(BINDING_MODE_NONE);
  });
});
