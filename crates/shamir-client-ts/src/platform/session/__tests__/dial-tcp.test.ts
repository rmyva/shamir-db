import { EventEmitter } from 'node:events';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ShamirTimeoutError } from '../../../core/support/errors.js';

class FakeTlsSocket extends EventEmitter {
  exportKeyingMaterial = vi.fn((_len: number, _label: string, _ctx: Buffer) => Buffer.alloc(32, 7));
  destroyed = false;
  destroy = vi.fn(() => {
    this.destroyed = true;
  });
  write = vi.fn();
  end = vi.fn();
}

async function setup() {
  const fake = new FakeTlsSocket();
  const connectMock = vi.fn((_opts: Record<string, unknown>) => fake);
  vi.doMock('node:tls', () => ({ connect: connectMock, default: { connect: connectMock } }));
  const { dialTcp } = await import('../dial-tcp.js');
  return { fake, connectMock, dialTcp };
}

afterEach(() => {
  vi.resetModules();
  vi.doUnmock('node:tls');
});

describe('dialTcp', () => {
  it('passes secure TLS options with SNI for a DNS name and without ALPN', async () => {
    const { connectMock, dialTcp } = await setup();
    void dialTcp({ kind: 'tcp', host: 'db.example', port: 4433 }, 0).catch(() => undefined);
    const opts = connectMock.mock.calls[0]![0];
    expect(opts.host).toBe('db.example');
    expect(opts.port).toBe(4433);
    expect(opts.minVersion).toBe('TLSv1.3');
    expect(opts.rejectUnauthorized).toBe(true);
    expect('ALPNProtocols' in opts).toBe(false);
    expect(opts.servername).toBe('db.example');
    expect('ca' in opts).toBe(false);
  });

  it('honours rejectUnauthorized=false and ca variants', async () => {
    const a = await setup();
    void a.dialTcp({ kind: 'tcp', host: 'h', port: 1, tls: { rejectUnauthorized: false, ca: new Uint8Array([1, 2]) } }, 0).catch(
      () => undefined,
    );
    const optsA = a.connectMock.mock.calls[0]![0];
    expect(optsA.rejectUnauthorized).toBe(false);
    expect(Buffer.isBuffer(optsA.ca)).toBe(true);

    vi.resetModules();
    const b = await setup();
    void b.dialTcp({ kind: 'tcp', host: 'h', port: 1, tls: { ca: 'PEM' } }, 0).catch(() => undefined);
    expect(b.connectMock.mock.calls[0]![0].ca).toBe('PEM');
  });

  it('resolves with the exporter binding and reassembles frames', async () => {
    const { fake, dialTcp } = await setup();
    const p = dialTcp({ kind: 'tcp', host: 'h', port: 1 }, 0);
    fake.emit('secureConnect');
    const link = await p;
    expect(link.binding.transportKind).toBe(1);
    expect(link.binding.bindingMode).toBe(1);
    expect(Array.from(link.binding.exporter)).toEqual(new Array(32).fill(7));
    expect(fake.exportKeyingMaterial).toHaveBeenCalledWith(32, 'EXPORTER-ShamirDB-AUTH-v1', Buffer.alloc(0));

    const got: number[][] = [];
    link.socket.onMessage((d) => got.push(Array.from(d)));
    const rec1 = [0, 0, 0, 3, 9, 8, 7];
    const rec2 = [0, 0, 0, 1, 5];
    const all = Buffer.from([...rec1, ...rec2]);
    fake.emit('data', all.subarray(0, 3));
    expect(got).toHaveLength(0);
    fake.emit('data', all.subarray(3));
    expect(got).toEqual([rec1, rec2]);
  });

  it('rejects and destroys on an error before secureConnect', async () => {
    const { fake, dialTcp } = await setup();
    const p = dialTcp({ kind: 'tcp', host: 'h', port: 1 }, 0);
    fake.emit('error', new Error('boom'));
    await expect(p).rejects.toThrow(/boom/);
    expect(fake.destroy).toHaveBeenCalled();
  });

  it('rejects when the exporter throws', async () => {
    const { fake, dialTcp } = await setup();
    fake.exportKeyingMaterial.mockImplementation(() => {
      throw new Error('nope');
    });
    const p = dialTcp({ kind: 'tcp', host: 'h', port: 1 }, 0);
    fake.emit('secureConnect');
    await expect(p).rejects.toThrow(/TLS exporter unavailable/);
    expect(fake.destroy).toHaveBeenCalled();
  });

  it('times out and destroys the socket', async () => {
    const { fake, dialTcp } = await setup();
    await expect(dialTcp({ kind: 'tcp', host: 'h', port: 1 }, 20)).rejects.toBeInstanceOf(ShamirTimeoutError);
    expect(fake.destroy).toHaveBeenCalled();
  });
});
