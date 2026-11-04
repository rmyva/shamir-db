/**
 * Unit tests for the resume server-identity proof and its ordering in
 * resumeSession (the bearer ticket must only be sent after the proof).
 * Real Ed25519, fake sockets, no network.
 */

import { describe, it, expect } from 'vitest';
import * as ed from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha512';
import type { Socket, Platform } from '../../../support/platform.js';
import { WsFramer, decode, encode } from '../../framing.js';
import { resumeSession, verifyResumeIdentityProof } from '../resume.js';
import { tlsExporterBinding } from '../binding.js';
import type { NativeSessionEndpoint, SessionLink } from '../endpoint.js';

if (ed.etc.sha512Sync === undefined) {
  ed.etc.sha512Sync = (...m) => sha512(ed.etc.concatBytes(...m));
}

const TAG = new TextEncoder().encode('SHAMIR-RESUME-PROOF-v1');
const NONCE = new Uint8Array(32).fill(0x11);
const EXPORTER = new Uint8Array(32).fill(9);
const OTHER_EXPORTER = new Uint8Array(32).fill(8);
const TICKET = new Uint8Array(40).fill(0x7a);
const SESSION_ID = new Uint8Array(32).fill(0x55);

const PRIV = new Uint8Array(32).fill(5);
const PUB = ed.getPublicKey(PRIV);
const OTHER_PRIV = new Uint8Array(32).fill(6);
const OTHER_PUB = ed.getPublicKey(OTHER_PRIV);

function proofMessage(exporter: Uint8Array): Uint8Array {
  const msg = new Uint8Array(TAG.length + NONCE.length + exporter.length);
  msg.set(TAG, 0);
  msg.set(NONCE, TAG.length);
  msg.set(exporter, TAG.length + NONCE.length);
  return msg;
}

function sign(exporter: Uint8Array): Uint8Array {
  return ed.sign(proofMessage(exporter), PRIV);
}

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
  onClose(h: (err?: Error) => void): void {
    this.closeHandler = h;
  }
  close(): Promise<void> {
    this.closed = true;
    this.closeHandler();
    return Promise.resolve();
  }
}

const platform = {
  randomBytes: (n: number) => new Uint8Array(n).fill(0x11),
  timingSafeEqual: (a: Uint8Array, b: Uint8Array) =>
    a.length === b.length && a.every((v, i) => v === b[i]),
} as unknown as Platform;

function decodeSent(socket: ScriptedSocket, i: number): unknown {
  return decode(socket.sent[i].subarray(4));
}

describe('verifyResumeIdentityProof', () => {
  it('accepts a valid proof and sends one 1-field array frame', async () => {
    const socket = new ScriptedSocket([[PUB, sign(EXPORTER)]]);
    const framer = new WsFramer(socket);
    await verifyResumeIdentityProof(platform, framer, NONCE, EXPORTER, PUB);
    expect(socket.sent.length).toBe(1);
    const sent = decodeSent(socket, 0);
    expect(Array.isArray(sent)).toBe(true);
    const arr = sent as unknown[];
    expect(arr.length).toBe(1);
    expect(new Uint8Array(arr[0] as Uint8Array)).toEqual(NONCE);
  });

  it('rejects a signature made over another exporter', async () => {
    const socket = new ScriptedSocket([[PUB, sign(OTHER_EXPORTER)]]);
    const framer = new WsFramer(socket);
    await expect(
      verifyResumeIdentityProof(platform, framer, NONCE, EXPORTER, PUB),
    ).rejects.toThrow(/proof invalid/);
  });

  it('rejects a server key that is not the pinned key', async () => {
    const socket = new ScriptedSocket([
      [OTHER_PUB, ed.sign(proofMessage(EXPORTER), OTHER_PRIV)],
    ]);
    const framer = new WsFramer(socket);
    await expect(
      verifyResumeIdentityProof(platform, framer, NONCE, EXPORTER, PUB),
    ).rejects.toThrow(/does not match pinned/);
  });

  it('rejects a non-array response', async () => {
    const socket = new ScriptedSocket([{ error: 'nope' }]);
    const framer = new WsFramer(socket);
    await expect(
      verifyResumeIdentityProof(platform, framer, NONCE, EXPORTER, PUB),
    ).rejects.toThrow(/malformed/);
  });
});

describe('resumeSession over a tls_exporter link', () => {
  const opts = {
    host: 'h',
    port: 1,
    transport: 'tcp' as const,
    ticket: TICKET,
    serverPubKey: PUB,
  };

  function platformFor(socket: ScriptedSocket): Platform {
    const link: SessionLink = {
      socket,
      binding: tlsExporterBinding(0x01, EXPORTER),
    };
    return {
      ...platform,
      openNativeLink: async (
        _endpoint: NativeSessionEndpoint,
        _o: { connectTimeoutMs: number },
      ) => link,
    } as unknown as Platform;
  }

  it('never sends the ticket when the proof is invalid', async () => {
    const socket = new ScriptedSocket([[PUB, sign(OTHER_EXPORTER)]]);
    await expect(resumeSession(platformFor(socket), opts)).rejects.toThrow(
      /proof invalid/,
    );
    expect(socket.sent.length).toBe(1);
    expect(Array.isArray(decodeSent(socket, 0))).toBe(true);
    expect(socket.closed).toBe(true);
  });

  it('sends the ticket after a valid proof and resumes', async () => {
    const socket = new ScriptedSocket([
      [PUB, sign(EXPORTER)],
      [SESSION_ID, 123, new Uint8Array(0), 0, 2],
    ]);
    const opened = await resumeSession(platformFor(socket), opts);
    expect(socket.sent.length).toBe(2);

    const first = decodeSent(socket, 0) as unknown[];
    expect(Array.isArray(first)).toBe(true);
    expect(first.length).toBe(1);

    const second = decodeSent(socket, 1) as {
      ticket: Uint8Array;
      client_nonce: Uint8Array;
      binding_mode: number;
    };
    expect(Array.isArray(second)).toBe(false);
    expect(second.binding_mode).toBe(1);
    expect(new Uint8Array(second.ticket)).toEqual(TICKET);

    expect(opened.binding.bindingMode).toBe(1);
    expect(new Uint8Array(opened.sessionId)).toEqual(SESSION_ID);
    expect(opened.serverQueryVersion).toBe(2);
  });
});
