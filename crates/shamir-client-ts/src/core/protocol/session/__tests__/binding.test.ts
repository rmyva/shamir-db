/**
 * Unit tests for the channel-binding profiles and their effect on the
 * `auth_init` frame sent by runHandshake(). No network, no real crypto: the
 * handshake only needs `randomBytes` before `auth_init` is sent.
 */

import { describe, it, expect } from 'vitest';
import type { Socket, Platform } from '../../../support/platform.js';
import { WsFramer, decode } from '../../framing.js';
import { runHandshake } from '../../protocol.js';
import {
  TRANSPORT_KIND_WS,
  TRANSPORT_KIND_TCP,
  TRANSPORT_KIND_UNIX,
  BINDING_MODE_TLS_NO_EXPORT,
  BINDING_MODE_NONE,
  SUPPORTED_VERSION,
} from '../../scram.js';
import { browserWsBinding, ipcBinding, tcpPlainBinding } from '../binding.js';

class FakeSocket implements Socket {
  sent: Uint8Array[] = [];
  messageHandler: (_data: Uint8Array) => void = () => {};
  closeHandler: (_err?: Error) => void = () => {};
  private _closed = false;

  send(data: Uint8Array): void {
    if (this._closed) throw new Error('connection closed');
    this.sent.push(data);
  }
  onMessage(h: (data: Uint8Array) => void): void {
    this.messageHandler = h;
  }
  onClose(h: (err?: Error) => void): void {
    this.closeHandler = h;
  }
  close(): Promise<void> {
    this._closed = true;
    this.closeHandler();
    return Promise.resolve();
  }

  simulateClose(err?: Error): void {
    this._closed = true;
    this.closeHandler(err);
  }
}

const platform = {
  randomBytes: (n: number) => new Uint8Array(n).fill(7),
} as unknown as Platform;

interface AuthInit {
  user: string;
  client_nonce: Uint8Array;
  binding_mode: number;
  version: number;
}

/** Decode the first sent frame (length prefix stripped) as auth_init. */
function readAuthInit(socket: FakeSocket): AuthInit {
  const frame = socket.sent[0];
  return decode(frame.subarray(4)) as AuthInit;
}

/** Run a handshake just far enough to capture auth_init, then abort it. */
async function captureAuthInit(
  start: (framer: WsFramer) => Promise<unknown>,
): Promise<AuthInit> {
  const socket = new FakeSocket();
  const framer = new WsFramer(socket);
  const handshake = start(framer);
  await new Promise((r) => setTimeout(r, 0));
  const init = readAuthInit(socket);
  socket.simulateClose(new Error('stop'));
  await expect(handshake).rejects.toThrow();
  return init;
}

describe('channel binding profiles', () => {
  it('browserWsBinding() is the WS / TLS-no-export profile', () => {
    const b = browserWsBinding();
    expect(b.transportKind).toBe(TRANSPORT_KIND_WS);
    expect(b.bindingMode).toBe(BINDING_MODE_TLS_NO_EXPORT);
    expect(b.exporter).toEqual(new Uint8Array(32));
  });

  it('ipcBinding() is the UNIX / no-binding profile', () => {
    const b = ipcBinding();
    expect(b.transportKind).toBe(TRANSPORT_KIND_UNIX);
    expect(b.bindingMode).toBe(BINDING_MODE_NONE);
    expect(b.exporter).toEqual(new Uint8Array(32));
  });

  it('returns a fresh exporter array on every call', () => {
    const a = browserWsBinding();
    const b = browserWsBinding();
    a.exporter[0] = 0xff;
    expect(b.exporter[0]).toBe(0);

    const c = ipcBinding();
    const d = ipcBinding();
    c.exporter[0] = 0xff;
    expect(d.exporter[0]).toBe(0);
  });
});

describe('tcpPlainBinding()', () => {
  it('is the TCP / no-binding profile', () => {
    const b = tcpPlainBinding();
    expect(b.transportKind).toBe(TRANSPORT_KIND_TCP);
    expect(b.transportKind).toBe(0x01);
    expect(b.bindingMode).toBe(BINDING_MODE_NONE);
    expect(b.bindingMode).toBe(0x00);
    expect(b.exporter).toEqual(new Uint8Array(32));
  });

  it('returns a fresh exporter array on every call', () => {
    const a = tcpPlainBinding();
    const b = tcpPlainBinding();
    a.exporter[0] = 0xff;
    expect(b.exporter[0]).toBe(0);
  });
});

describe('runHandshake auth_init per binding profile', () => {
  it('browser profile sends binding_mode 0x02', async () => {
    const init = await captureAuthInit((framer) =>
      runHandshake(platform, framer, 'alice', 'pw', browserWsBinding()),
    );
    expect(init.binding_mode).toBe(BINDING_MODE_TLS_NO_EXPORT);
    expect(init.binding_mode).toBe(0x02);
    expect(init.user).toBe('alice');
    expect(init.version).toBe(SUPPORTED_VERSION);
    expect(init.client_nonce.length).toBe(32);
  });

  it('ipc profile sends binding_mode 0x00', async () => {
    const init = await captureAuthInit((framer) =>
      runHandshake(platform, framer, 'alice', 'pw', ipcBinding()),
    );
    expect(init.binding_mode).toBe(BINDING_MODE_NONE);
    expect(init.binding_mode).toBe(0x00);
    expect(init.user).toBe('alice');
    expect(init.version).toBe(SUPPORTED_VERSION);
    expect(init.client_nonce.length).toBe(32);
  });

  it('tcp-plain profile sends binding_mode 0x00', async () => {
    const init = await captureAuthInit((framer) =>
      runHandshake(platform, framer, 'alice', 'pw', tcpPlainBinding()),
    );
    expect(init.binding_mode).toBe(BINDING_MODE_NONE);
    expect(init.binding_mode).toBe(0x00);
    expect(init.user).toBe('alice');
    expect(init.version).toBe(SUPPORTED_VERSION);
    expect(init.client_nonce.length).toBe(32);
  });

  it('legacy numeric form (UNIX, NONE) sends binding_mode 0x00', async () => {
    const init = await captureAuthInit((framer) =>
      runHandshake(
        platform,
        framer,
        'alice',
        'pw',
        TRANSPORT_KIND_UNIX,
        BINDING_MODE_NONE,
      ),
    );
    expect(init.binding_mode).toBe(0);
    expect(init.user).toBe('alice');
    expect(init.version).toBe(SUPPORTED_VERSION);
    expect(init.client_nonce.length).toBe(32);
  });
});
