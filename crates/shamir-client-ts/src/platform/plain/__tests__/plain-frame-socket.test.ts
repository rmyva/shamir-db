/**
 * Unit tests for PlainFrameSocket: `[u32_be length][payload]` framing over a
 * plain node:net socket, same rules as TlsFrameSocket. Fake sockets only.
 */

import { EventEmitter } from 'node:events';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Socket as NetSocket } from 'node:net';
import { MAX_FRAME_SIZE_DEFAULT } from '../../../core/protocol/framing.js';
import { PlainFrameSocket } from '../plain-frame-socket.js';

class FakeNet extends EventEmitter {
  destroyed = false;
  destroy = vi.fn((err?: Error) => {
    this.destroyed = true;
    if (err) this.emit('error', err);
    this.emit('close');
  });
  write = vi.fn();
  end = vi.fn();
}

afterEach(() => {
  vi.useRealTimers();
});

function frame(payload: Uint8Array): Buffer {
  const out = Buffer.alloc(4 + payload.length);
  out.writeUInt32BE(payload.length, 0);
  out.set(payload, 4);
  return out;
}

function setup() {
  const fake = new FakeNet();
  const socket = new PlainFrameSocket(fake as unknown as NetSocket);
  const messages: Uint8Array[] = [];
  const closes: Array<Error | undefined> = [];
  socket.onMessage((data) => messages.push(data));
  socket.onClose((err) => closes.push(err));
  return { fake, socket, messages, closes };
}

describe('PlainFrameSocket', () => {
  it('delivers a frame split over three chunks, prefix included', () => {
    const { fake, messages } = setup();
    const whole = frame(new Uint8Array([1, 2, 3, 4, 5]));
    fake.emit('data', whole.subarray(0, 2));
    fake.emit('data', whole.subarray(2, 6));
    expect(messages.length).toBe(0);
    fake.emit('data', whole.subarray(6));
    expect(messages.length).toBe(1);
    expect(Array.from(messages[0]!)).toEqual(Array.from(whole));
  });

  it('splits two frames that arrive in one chunk', () => {
    const { fake, messages } = setup();
    const a = frame(new Uint8Array([9]));
    const b = frame(new Uint8Array([8, 7]));
    fake.emit('data', Buffer.concat([a, b]));
    expect(messages.map((m) => Array.from(m))).toEqual([
      Array.from(a),
      Array.from(b),
    ]);
  });

  it('destroys the socket with an Error when the declared length exceeds the ceiling', () => {
    const { fake, messages, closes } = setup();
    const head = Buffer.alloc(4);
    head.writeUInt32BE(MAX_FRAME_SIZE_DEFAULT + 1, 0);
    fake.emit('data', head);
    expect(messages.length).toBe(0);
    expect(fake.destroy).toHaveBeenCalledTimes(1);
    expect(closes.length).toBe(1);
    expect(closes[0]).toBeInstanceOf(Error);
  });

  it('destroys the socket silently on a zero-length frame', () => {
    const { fake, messages, closes } = setup();
    fake.emit('data', Buffer.alloc(4));
    expect(messages.length).toBe(0);
    expect(fake.destroy).toHaveBeenCalledTimes(1);
    expect(closes).toEqual([undefined]);
  });

  it('reports close once on error + close', () => {
    const { fake, closes } = setup();
    fake.emit('error', new Error('boom'));
    fake.emit('close');
    expect(closes.length).toBe(1);
  });

  it('close() resolves after close and end() was called', async () => {
    const { fake, socket } = setup();
    const p = socket.close();
    expect(fake.end).toHaveBeenCalledTimes(1);
    fake.emit('close');
    await expect(p).resolves.toBeUndefined();
  });

  it('close() resolves via the grace timer when the peer never closes', async () => {
    vi.useFakeTimers();
    const { fake, socket } = setup();
    const p = socket.close();
    expect(fake.end).toHaveBeenCalledTimes(1);
    expect(fake.destroy).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(2000);
    await expect(p).resolves.toBeUndefined();
    expect(fake.destroy).toHaveBeenCalledTimes(1);
  });
});
