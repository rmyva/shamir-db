/**
 * Platform interface — the ONLY abstraction over Node vs Browser differences.
 *
 * The core library (scram.ts, framing.ts, protocol.ts, client.ts) depends
 * ONLY on this interface. No `require('crypto')`, `ws`, or WebCrypto live
 * anywhere in core/. Those live in platform/node.ts and platform/browser.ts.
 */

import type {
  NativeSessionEndpoint,
  SessionLink,
} from '../protocol/session/endpoint.js';

/**
 * A binary WebSocket abstraction. The platform adapter wraps `ws` (Node) or
 * native `WebSocket` (browser) behind this thin interface so the core never
 * imports either.
 */
export interface Socket {
  /** Send a binary frame. Throws if the socket is closed. */
  send(data: Uint8Array): void;
  /** Register a handler for the next (or any future) inbound binary message. */
  onMessage(handler: (data: Uint8Array) => void): void;
  /** Register a one-shot close/error handler. */
  onClose(handler: (err?: Error) => void): void;
  /** Close the socket gracefully. */
  close(): Promise<void>;
}

/** Argon2id cost parameters as received in a SCRAM challenge. */
export interface Argon2Params {
  memoryKb: number;
  time: number;
  parallelism: number;
}

/**
 * Platform capabilities injected into the core at construction time.
 * All methods are synchronous except `argon2id` and `openSocket`.
 */
export interface Platform {
  /** HMAC-SHA256(key, data) → 32 bytes. */
  hmacSha256(key: Uint8Array, data: Uint8Array): Uint8Array;
  /** SHA-256(data) → 32 bytes. */
  sha256(data: Uint8Array): Uint8Array;
  /** Cryptographically-secure random bytes. */
  randomBytes(n: number): Uint8Array;
  /**
   * Constant-time equality check. MUST NOT short-circuit on first differing
   * byte to avoid timing side channels (§B24 of rust-intel — same principle
   * applies to TS).
   */
  timingSafeEqual(a: Uint8Array, b: Uint8Array): boolean;
  /**
   * Argon2id KDF → 32-byte salted password.
   * Version is always 0x13 (validated before this is called).
   */
  argon2id(
    password: Uint8Array,
    salt: Uint8Array,
    p: Argon2Params,
  ): Promise<Uint8Array>;
  /**
   * Open a binary WebSocket to `url`. Resolves once the socket is connected
   * and ready to send/receive.
   */
  openSocket(
    url: string,
    opts: { rejectUnauthorized?: boolean; origin?: string },
  ): Promise<Socket>;
  /**
   * Open a local-IPC connection (Unix domain socket / Windows Named Pipe,
   * spec TRANSPORT_UNIX.md) to `path`. OPTIONAL — there is no browser
   * equivalent (a browser sandbox has no filesystem/pipe access), so
   * `BrowserPlatform` does not implement this. `ShamirClient.connectLocal`
   * throws a clear error if called against a `Platform` lacking it, rather
   * than the core module importing `node:net` itself (which would break
   * the "no Node/browser-specific imports in core/" rule this file's own
   * doc comment states).
   */
  openIpcSocket?(path: string): Promise<Socket>;
  /**
   * Open a native session link: TCP+TLS or native WSS, with the real TLS
   * exporter as channel binding. OPTIONAL — Node only; `BrowserPlatform`
   * does not implement it. `connectTimeoutMs <= 0` disables the bound. The
   * implementation must destroy the socket on timeout/failure.
   */
  openNativeLink?(
    endpoint: NativeSessionEndpoint,
    opts: { connectTimeoutMs: number },
  ): Promise<SessionLink>;
}
