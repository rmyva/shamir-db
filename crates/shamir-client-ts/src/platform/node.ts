/**
 * NodePlatform — thin Node.js adapter (~70 lines).
 *
 * Wraps:
 *   - node:crypto   → hmacSha256 / sha256 / randomBytes / timingSafeEqual
 *   - argon2-browser → argon2id (works in Node via WASM)
 *   - ws            → openSocket
 *
 * `openIpcSocket` (below `NodePlatform`) is a Node-only ADDITION to this
 * file, not part of the `Platform` interface — local IPC (Unix domain
 * socket / Windows Named Pipe) has no browser equivalent at all, so it
 * can't be a `Platform` method the way `openSocket` is. It is consumed
 * directly by `core/client.ts`'s `connectLocal`, and only ever imported
 * from `src/index.ts` (the Node entry point) — never from `src/browser.ts`.
 */

import { createHash, createHmac, timingSafeEqual, randomBytes } from 'node:crypto';
import { createConnection, type Socket as NetSocket } from 'node:net';
import WebSocket from 'ws';
import argon2 from 'argon2-browser';
import type { Platform, Socket, Argon2Params } from '../core/support/platform.js';
import { ARGON2_VERSION_13 } from '../core/protocol/scram.js';
import { createNodeTokenPlatform as createTokenPlatform } from './token/http-node.js';
import type { NodeTokenHttpConnectOptions, NodeTokenWsConnectOptions } from '../core/types/connection.js';
import type { TokenPlatform } from './token/types.js';

export function createNodeTokenPlatform(options: NodeTokenHttpConnectOptions | NodeTokenWsConnectOptions): TokenPlatform {
  return createTokenPlatform(options);
}

export class NodeSocket implements Socket {
  private readonly ws: WebSocket;
  private readonly messageHandlers: Array<(data: Uint8Array) => void> = [];
  private closeHandlers: Array<(err?: Error) => void> = [];
  private closeFired = false;

  constructor(ws: WebSocket) {
    this.ws = ws;
    ws.binaryType = 'nodebuffer';
    ws.on('message', (data: Buffer, isBinary: boolean) => {
      if (!isBinary) return;
      const bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
      for (const h of this.messageHandlers) h(bytes);
    });
    ws.on('close', () => {
      if (this.closeFired) return;
      this.closeFired = true;
      for (const h of this.closeHandlers) h(undefined);
    });
    ws.on('error', (err: Error) => {
      if (this.closeFired) return;
      this.closeFired = true;
      for (const h of this.closeHandlers) h(err);
    });
  }

  send(data: Uint8Array): void {
    this.ws.send(data, { binary: true });
  }

  onMessage(handler: (data: Uint8Array) => void): void {
    this.messageHandlers.push(handler);
  }

  onClose(handler: (err?: Error) => void): void {
    this.closeHandlers.push(handler);
  }

  close(): Promise<void> {
    return new Promise((resolve) => {
      if (
        this.ws.readyState === WebSocket.CLOSED ||
        this.ws.readyState === WebSocket.CLOSING
      ) {
        resolve();
        return;
      }
      this.ws.once('close', () => resolve());
      this.ws.close(1000);
    });
  }
}

export const NodePlatform: Platform = {
  hmacSha256(key: Uint8Array, data: Uint8Array): Uint8Array {
    return new Uint8Array(createHmac('sha256', key).update(data).digest());
  },

  sha256(data: Uint8Array): Uint8Array {
    return new Uint8Array(createHash('sha256').update(data).digest());
  },

  randomBytes(n: number): Uint8Array {
    return new Uint8Array(randomBytes(n));
  },

  timingSafeEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;
    return timingSafeEqual(a, b);
  },

  async argon2id(
    password: Uint8Array,
    salt: Uint8Array,
    p: Argon2Params,
  ): Promise<Uint8Array> {
    // argon2-browser types omit `version` but the runtime accepts it.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const result = await (argon2.hash as any)({
      pass: password,
      salt,
      time: p.time,
      mem: p.memoryKb,
      parallelism: p.parallelism,
      hashLen: 32,
      type: argon2.ArgonType.Argon2id,
      version: ARGON2_VERSION_13,
    });
    return new Uint8Array(result.hash as Uint8Array);
  },

  async openSocket(
    url: string,
    opts: { rejectUnauthorized?: boolean; origin?: string },
  ): Promise<Socket> {
    return new Promise((resolve, reject) => {
      // `ws` takes subprotocols as the 2nd argument (not an options field).
      const ws = new WebSocket(url, ['shamir-v1'], {
        rejectUnauthorized: opts.rejectUnauthorized ?? true,
        origin: opts.origin,
      });
      const onOpen = () => {
        ws.off('error', onError);
        resolve(new NodeSocket(ws));
      };
      const onError = (err: Error) => {
        ws.off('open', onOpen);
        reject(err);
      };
      ws.once('open', onOpen);
      ws.once('error', onError);
    });
  },

  openIpcSocket,
};

/**
 * `Socket` adapter over a raw `node:net` connection (Unix domain socket on
 * POSIX, Windows Named Pipe when `path` is a `\\.\pipe\...` name — Node's
 * `net` module dispatches to the right OS primitive itself; no separate
 * package or branch needed here).
 *
 * Unlike WebSocket (message-framed — one `onMessage` call already equals
 * one complete application message), a raw `net.Socket` is an unbounded
 * byte stream: `'data'` events can deliver a partial frame, multiple
 * frames, or a frame split across two events. This class buffers and
 * re-assembles `[u32_be length][payload]` records itself and delivers the
 * FULL record (length prefix included) per `onMessage` call — matching
 * exactly what `core/framing.ts`'s `WsFramer.onBinary` already expects
 * (it re-parses/validates that same 4-byte prefix). `WsFramer` itself
 * needs zero changes to work over this socket.
 */
export class NodeIpcSocket implements Socket {
  private readonly socket: NetSocket;
  private readonly messageHandlers: Array<(data: Uint8Array) => void> = [];
  private closeHandlers: Array<(err?: Error) => void> = [];
  private closeFired = false;
  private buffer: Buffer = Buffer.alloc(0);

  constructor(socket: NetSocket) {
    this.socket = socket;
    socket.on('data', (chunk: Buffer) => {
      this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk]);
      this.drainFrames();
    });
    socket.on('close', () => {
      if (this.closeFired) return;
      this.closeFired = true;
      for (const h of this.closeHandlers) h(undefined);
    });
    socket.on('error', (err: Error) => {
      if (this.closeFired) return;
      this.closeFired = true;
      for (const h of this.closeHandlers) h(err);
    });
  }

  /** Extract every complete `[u32_be length][payload]` record currently
   * buffered, delivering each (length prefix included) to the message
   * handlers, and leave any trailing partial record in `this.buffer` for
   * the next `'data'` event. */
  private drainFrames(): void {
    for (;;) {
      if (this.buffer.length < 4) return;
      const declared = this.buffer.readUInt32BE(0);
      const total = 4 + declared;
      if (this.buffer.length < total) return;
      // `new Uint8Array(typedArray)` copies — the slice below aliases
      // `this.buffer`'s backing store, which is about to be reassigned.
      const frame = new Uint8Array(this.buffer.subarray(0, total));
      this.buffer = this.buffer.subarray(total);
      for (const h of this.messageHandlers) h(frame);
    }
  }

  send(data: Uint8Array): void {
    this.socket.write(data);
  }

  onMessage(handler: (data: Uint8Array) => void): void {
    this.messageHandlers.push(handler);
  }

  onClose(handler: (err?: Error) => void): void {
    this.closeHandlers.push(handler);
  }

  close(): Promise<void> {
    return new Promise((resolve) => {
      if (this.socket.destroyed) {
        resolve();
        return;
      }
      this.socket.once('close', () => resolve());
      this.socket.end();
    });
  }
}

/**
 * Open a local-IPC connection: `path` is a filesystem path on POSIX or a
 * `\\.\pipe\name` Named Pipe name on Windows — same call, no branching by
 * the caller. Node-only; there is no browser equivalent (see the module
 * doc comment above).
 */
export async function openIpcSocket(path: string): Promise<Socket> {
  const invalid = (rule: string): Error => new Error(`Invalid IPC endpoint: ${rule}`);
  if (process.platform === 'win32') {
    const prefix = '\\\\.\\pipe\\';
    const root = path.slice(0, prefix.length);
    if (root.toLowerCase() !== prefix.toLowerCase()) {
      throw invalid('Windows address must start with \\\\\\\\.\\\\pipe\\\\');
    }
    const rest = path.slice(prefix.length);
    if (!rest || rest.includes('\\')) throw invalid('pipe name must be a non-empty single component');
    if (path.includes('\0')) throw invalid('address must not contain NUL');
    if ([...path].reduce((units, ch) => units + (ch.codePointAt(0)! > 0xffff ? 2 : 1), 0) > 256) {
      throw invalid('address must not exceed 256 UTF-16 code units');
    }
  } else {
    if (!path.startsWith('/')) throw invalid('Unix path must be absolute');
    if (path.split('/').includes('..')) throw invalid('Unix path must not contain a parent-directory component');
    if (path.includes('\0')) throw invalid('address must not contain NUL');
    const limit = ['darwin', 'freebsd', 'openbsd', 'netbsd', 'dragonfly', 'ios'].includes(process.platform) ? 104 : 108;
    if (Buffer.byteLength(path) >= limit) throw invalid(`Unix path must be shorter than SUN_PATH_LEN (${limit} bytes)`);
  }
  return new Promise((resolve, reject) => {
    const socket = createConnection({ path }, () => {
      socket.off('error', onError);
      resolve(new NodeIpcSocket(socket));
    });
    const onError = (err: Error) => {
      reject(err);
    };
    socket.once('error', onError);
  });
}
