import type { Socket as NetSocket } from 'node:net';
import { MAX_FRAME_SIZE_DEFAULT } from '../../core/protocol/framing.js';
import type { Socket } from '../../core/support/platform.js';

const CLOSE_GRACE_MS = 2000;
const PREFIX_LEN = 4;

/**
 * `[u32_be length][payload]` frames over a PLAIN TCP socket (`kind: tcp,
 * profile: plain`; loopback only, binding mode 0x00, no TLS). Each delivered
 * message keeps its length prefix. Same rules as the TLS twin: this is a
 * network path, so a declared length above the frame ceiling destroys the
 * socket, a zero length is the peer's close marker (as in the Rust
 * transport), and buffering is linear in the bytes received (no per-chunk
 * re-concatenation).
 */
export class PlainFrameSocket implements Socket {
  private readonly socket: NetSocket;
  private readonly messageHandlers: Array<(data: Uint8Array) => void> = [];
  private readonly closeHandlers: Array<(err?: Error) => void> = [];
  private closeFired = false;
  private chunks: Buffer[] = [];
  private buffered = 0;

  constructor(socket: NetSocket) {
    this.socket = socket;
    socket.on('data', (chunk: Buffer) => {
      this.chunks.push(chunk);
      this.buffered += chunk.length;
      this.drain();
    });
    socket.on('close', () => this.fireClose(undefined));
    socket.on('error', (err: Error) => this.fireClose(err));
  }

  private fireClose(err: Error | undefined): void {
    if (this.closeFired) return;
    this.closeFired = true;
    for (const h of this.closeHandlers) h(err);
  }

  private drain(): void {
    while (this.buffered >= PREFIX_LEN) {
      const head = this.chunks[0].length >= PREFIX_LEN ? this.chunks[0] : Buffer.concat(this.chunks, PREFIX_LEN);
      const declared = head.readUInt32BE(0);
      if (declared === 0) {
        this.socket.destroy();
        return;
      }
      if (declared > MAX_FRAME_SIZE_DEFAULT) {
        this.socket.destroy(new Error(`frame too large: ${declared} bytes`));
        return;
      }
      const total = PREFIX_LEN + declared;
      if (this.buffered < total) return;
      const all = this.chunks.length === 1 ? this.chunks[0] : Buffer.concat(this.chunks, this.buffered);
      // `new Uint8Array(typedArray)` copies: `all` may alias a chunk we keep.
      const frame = new Uint8Array(all.subarray(0, total));
      const rest = all.subarray(total);
      this.chunks = rest.length > 0 ? [rest] : [];
      this.buffered = rest.length;
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
      const timer: ReturnType<typeof setTimeout> = setTimeout(() => this.socket.destroy(), CLOSE_GRACE_MS);
      if (typeof timer.unref === 'function') timer.unref();
      this.socket.once('close', () => {
        clearTimeout(timer);
        resolve();
      });
      this.socket.end();
    });
  }
}
