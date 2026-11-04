import { EventEmitter } from 'node:events';
import { afterEach, describe, expect, it, vi } from 'vitest';

class FakeTlsSocket extends EventEmitter {
  exportKeyingMaterial = vi.fn((_len: number, _label: string, _ctx: Buffer) => Buffer.alloc(32, 7));
}

class FakeWs extends EventEmitter {
  static instances: FakeWs[] = [];
  static CLOSED = 3;
  static CLOSING = 2;
  protocol = 'shamir-v1';
  readyState = 0;
  binaryType = '';
  send = vi.fn();
  close = vi.fn();
  terminate = vi.fn();
  args: unknown[];
  constructor(...args: unknown[]) {
    super();
    this.args = args;
    FakeWs.instances.push(this);
  }
}

async function setup() {
  FakeWs.instances = [];
  vi.doMock('ws', () => ({ default: FakeWs }));
  return await import('../dial-ws-native.js');
}

afterEach(() => {
  vi.resetModules();
  vi.doUnmock('ws');
});

describe('dialWsNative', () => {
  it('constructs ws with secure options and resolves with the exporter', async () => {
    const { dialWsNative } = await setup();
    const p = dialWsNative({ kind: 'ws-native', url: 'wss://h/shamir/v1' }, 0);
    const ws = FakeWs.instances[0]!;
    const [url, protocols, options] = ws.args as [string, string[], Record<string, unknown>];
    expect(url).toBe('wss://h/shamir/v1');
    expect(protocols).toEqual(['shamir-v1']);
    expect('origin' in options).toBe(false);
    expect('headers' in options).toBe(false);
    expect(options.perMessageDeflate).toBe(false);
    expect(options.minVersion).toBe('TLSv1.3');
    expect(options.rejectUnauthorized).toBe(true);

    const tls = new FakeTlsSocket();
    ws.emit('upgrade', { socket: tls });
    ws.emit('open');
    const link = await p;
    expect(link.binding.transportKind).toBe(2);
    expect(link.binding.bindingMode).toBe(1);
    expect(Array.from(link.binding.exporter)).toEqual(new Array(32).fill(7));

    const got: number[][] = [];
    link.socket.onMessage((d) => got.push(Array.from(d)));
    ws.emit('message', Buffer.from([0, 0, 0, 2, 1, 2]), true);
    expect(got).toEqual([[0, 0, 0, 2, 1, 2]]);
  });

  it('rejects and terminates on a wrong subprotocol', async () => {
    const { dialWsNative } = await setup();
    const p = dialWsNative({ kind: 'ws-native', url: 'wss://h/x' }, 0);
    const ws = FakeWs.instances[0]!;
    ws.protocol = 'other';
    ws.emit('upgrade', { socket: new FakeTlsSocket() });
    ws.emit('open');
    await expect(p).rejects.toThrow(/shamir-v1/);
    expect(ws.terminate).toHaveBeenCalled();
  });

  it('rejects ws:// URLs without constructing a WebSocket', async () => {
    const { dialWsNative } = await setup();
    await expect(dialWsNative({ kind: 'ws-native', url: 'ws://h/x' }, 0)).rejects.toThrow(/wss/);
    expect(FakeWs.instances).toHaveLength(0);
  });

  it('rejects on an error before open', async () => {
    const { dialWsNative } = await setup();
    const p = dialWsNative({ kind: 'ws-native', url: 'wss://h/x' }, 0);
    FakeWs.instances[0]!.emit('error', new Error('boom'));
    await expect(p).rejects.toThrow(/boom/);
    expect(FakeWs.instances[0]!.terminate).toHaveBeenCalled();
  });
});
