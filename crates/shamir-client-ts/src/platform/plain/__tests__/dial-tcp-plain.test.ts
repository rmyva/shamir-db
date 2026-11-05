/**
 * Unit tests for dialTcpPlain: the plain TCP dialer. node:net is mocked;
 * no network.
 */

import { EventEmitter } from 'node:events';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { PlainFrameSocket } from '../plain-frame-socket.js';
import { TRANSPORT_KIND_TCP, BINDING_MODE_NONE } from '../../../core/protocol/scram.js';

class FakeNetSocket extends EventEmitter {
  destroyed = false;
  destroy = vi.fn(() => {
    this.destroyed = true;
  });
  write = vi.fn();
  end = vi.fn();
}

type DialFn = (
  endpoint: unknown,
  connectTimeoutMs: number,
) => Promise<{ socket: PlainFrameSocket; binding: { transportKind: number; bindingMode: number; exporter: Uint8Array } }>;

async function setup() {
  const fake = new FakeNetSocket();
  const connectMock = vi.fn((_opts: Record<string, unknown>) => fake);
  vi.doMock('node:net', () => ({ connect: connectMock, default: { connect: connectMock } }));
  const { dialTcpPlain } = await import('../dial-tcp-plain.js');
  // Same module registry as dial-tcp-plain.js (resetModules makes a static import a different class).
  const { ShamirTimeoutError } = await import('../../../core/support/errors.js');
  // Same module registry as the dialer: a static import would be a different class after resetModules.
  const { PlainFrameSocket: PlainFrameSocketClass } = await import('../plain-frame-socket.js');
  return {
    fake,
    connectMock,
    dialTcpPlain: dialTcpPlain as unknown as DialFn,
    ShamirTimeoutError,
    PlainFrameSocketClass,
  };
}

afterEach(() => {
  vi.resetModules();
  vi.doUnmock('node:net');
});

describe('dialTcpPlain', () => {
  it('refuses non-loopback hosts BEFORE creating any socket', async () => {
    const { connectMock, dialTcpPlain } = await setup();
    for (const host of ['10.0.0.1', 'example.com']) {
      await expect(
        dialTcpPlain({ host, port: 1 }, 0),
      ).rejects.toThrow(/loopback-only/);
    }
    expect(connectMock).not.toHaveBeenCalled();
  });

  it('resolves a PlainFrameSocket link with the TCP / no-binding profile on connect', async () => {
    const { fake, dialTcpPlain, PlainFrameSocketClass } = await setup();
    const p = dialTcpPlain({ host: '127.0.0.1', port: 1 }, 0);
    fake.emit('connect');
    const link = await p;
    expect(link.socket).toBeInstanceOf(PlainFrameSocketClass);
    expect(link.binding.transportKind).toBe(TRANSPORT_KIND_TCP);
    expect(link.binding.transportKind).toBe(0x01);
    expect(link.binding.bindingMode).toBe(BINDING_MODE_NONE);
    expect(link.binding.bindingMode).toBe(0x00);
    expect(Array.from(link.binding.exporter)).toEqual(new Array(32).fill(0));

    // The link socket reassembles frames exactly like the TLS one.
    const got: number[][] = [];
    link.socket.onMessage((d) => got.push(Array.from(d)));
    const rec = [0, 0, 0, 3, 9, 8, 7];
    fake.emit('data', Buffer.from(rec));
    expect(got).toEqual([rec]);
  });

  it('times out and destroys the socket when connect never fires', async () => {
    const { fake, dialTcpPlain, ShamirTimeoutError } = await setup();
    await expect(dialTcpPlain({ host: '127.0.0.1', port: 1 }, 20)).rejects.toBeInstanceOf(
      ShamirTimeoutError,
    );
    expect(fake.destroy).toHaveBeenCalled();
  });

  it('rejects on an early close before connect', async () => {
    const { fake, dialTcpPlain } = await setup();
    const p = dialTcpPlain({ host: '127.0.0.1', port: 1 }, 0);
    fake.emit('close');
    await expect(p).rejects.toThrow(/closed/);
  });
});
