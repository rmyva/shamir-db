/**
 * Unit tests for resumeLocalSession: resuming over a local IPC transport
 * (binding 0x00 — NO identity-proof exchange; the resume frame is the FIRST
 * frame sent). Fakes only: no network, no real sockets.
 */

import { describe, it, expect, vi } from 'vitest';
import type { Socket, Platform } from '../../../support/platform.js';
import { WsFramer, decode, encode } from '../../framing.js';
import { resumeLocalSession } from '../resume.js';

const TICKET = new Uint8Array(40).fill(0x7a);
const SESSION_ID = new Uint8Array(32).fill(0x55);
const SERVER_PUB_KEY = new Uint8Array(32).fill(0xaa);

function frame(value: unknown): Uint8Array {
  const payload = encode(value);
  const out = new Uint8Array(4 + payload.length);
  new DataView(out.buffer).setUint32(0, payload.length, false);
  out.set(payload, 4);
  return out;
}

class ScriptedSocket implements Socket {
  sent: Uint8Array[] = [];
  closed = false;
  messageHandler: (_data: Uint8Array) => void = () => {};
  closeHandler: (_err?: Error) => void = () => {};
  private readonly replies: unknown[];

  constructor(replies: unknown[]) {
    this.replies = [...replies];
  }

  send(data: Uint8Array): void {
    if (this.closed) throw new Error('connection closed');
    this.sent.push(data);
    if (this.replies.length > 0) {
      const reply = frame(this.replies.shift());
      queueMicrotask(() => this.messageHandler(reply));
    }
  }
  onMessage(h: (data: Uint8Array) => void): void {
    this.messageHandler = h;
  }
  onClose(h: (_err?: Error) => void): void {
    this.closeHandler = h;
  }
  close(): Promise<void> {
    this.closed = true;
    this.closeHandler();
    return Promise.resolve();
  }
}

function platformFor(socket: Socket): Platform {
  return {
    openIpcSocket: async (_addr: string) => socket,
    randomBytes: (n: number) => new Uint8Array(n).fill(9),
    sha256: (_data: Uint8Array) => new Uint8Array(32).fill(7),
  } as unknown as Platform;
}

function decodeSent(socket: ScriptedSocket, i: number): unknown {
  return decode(socket.sent[i].subarray(4));
}

describe('resumeLocalSession', () => {
  it('rejects before any socket when the Platform has no openIpcSocket', async () => {
    const openSocket = vi.fn(() => {
      throw new Error('must never be called');
    });
    const platform = { openSocket } as unknown as Platform;
    await expect(
      resumeLocalSession(platform, {
        addr: '/tmp/s.sock',
        ticket: TICKET,
        serverPubKey: SERVER_PUB_KEY,
      }),
    ).rejects.toThrow(/openIpcSocket/);
    expect(platform.openSocket).not.toHaveBeenCalled();
  });

  it('sends the resume frame FIRST (no proof exchange) and decodes resume_ok', async () => {
    const socket = new ScriptedSocket([
      [SESSION_ID, 123n, new Uint8Array([5, 6]), 456n, 2],
    ]);
    const opened = await resumeLocalSession(platformFor(socket), {
      addr: '/tmp/s.sock',
      ticket: TICKET,
      serverPubKey: SERVER_PUB_KEY,
    });

    // Exactly ONE frame sent: no proof request/response.
    expect(socket.sent.length).toBe(1);
    const sent = decodeSent(socket, 0) as {
      ticket: Uint8Array;
      client_nonce: Uint8Array;
      binding_mode: number;
    };
    expect(Array.isArray(sent)).toBe(false);
    expect(sent.binding_mode).toBe(0);
    expect(new Uint8Array(sent.ticket)).toEqual(TICKET);
    expect(sent.client_nonce).toBeInstanceOf(Uint8Array);
    expect(sent.client_nonce.length).toBe(32);
    expect(sent.client_nonce.every((v) => v === 9)).toBe(true);

    expect(opened.sessionId).toBeInstanceOf(Uint8Array);
    expect(opened.sessionId.length).toBe(32);
    expect(new Uint8Array(opened.sessionId)).toEqual(SESSION_ID);
    expect(opened.serverPubKey).toBe(SERVER_PUB_KEY);
    expect(opened.expiresAtNs).toBe(123n);
    expect(new Uint8Array(opened.resumptionTicket as Uint8Array)).toEqual(
      new Uint8Array([5, 6]),
    );
    expect(opened.resumptionExpiresAtNs).toBe(456n);
    expect(opened.serverQueryVersion).toBe(2);
    expect(opened.normalizedUser).toBeUndefined();
    expect(opened.binding.transportKind).toBe(0x03);
    expect(opened.binding.bindingMode).toBe(0x00);
    expect(opened.binding.exporter).toEqual(new Uint8Array(32));
    expect(opened.framer).toBeInstanceOf(WsFramer);
  });

  it('maps an empty ticket and an absent server_query_version to defaults', async () => {
    const socket = new ScriptedSocket([
      [SESSION_ID, 123n, new Uint8Array(0), 0n],
    ]);
    const opened = await resumeLocalSession(platformFor(socket), {
      addr: '/tmp/s.sock',
      ticket: TICKET,
      serverPubKey: SERVER_PUB_KEY,
    });
    expect(opened.resumptionTicket).toBeUndefined();
    expect(opened.resumptionExpiresAtNs).toBeUndefined();
    expect(opened.serverQueryVersion).toBe(0);
  });

  it('rejects and closes the framer on a short session_id', async () => {
    const socket = new ScriptedSocket([
      [new Uint8Array(16), 123n, new Uint8Array(0), 0n, 0],
    ]);
    await expect(
      resumeLocalSession(platformFor(socket), {
        addr: '/tmp/s.sock',
        ticket: TICKET,
        serverPubKey: SERVER_PUB_KEY,
      }),
    ).rejects.toThrow(/session_id/);
    expect(socket.closed).toBe(true);
  });
});
