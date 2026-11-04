/**
 * Unit tests for the tls_exporter channel binding and its effect on the
 * handshake (`auth_init` binding_mode and the exporter inside auth_message).
 * Fakes only: no network.
 */

import { describe, it, expect } from 'vitest';
import { createHash, createHmac } from 'node:crypto';
import type { Socket, Platform } from '../../../support/platform.js';
import { WsFramer, decode, encode } from '../../framing.js';
import { runHandshake } from '../../protocol.js';
import {
  TRANSPORT_KIND_TCP,
  TRANSPORT_KIND_WS,
  BINDING_MODE_TLS_EXPORTER,
  buildAuthMessage,
  computeClientProof,
} from '../../scram.js';
import type { KdfParams } from '../../scram.js';
import { browserWsBinding, tlsExporterBinding } from '../binding.js';

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

function frame(value: unknown): Uint8Array {
  const payload = encode(value);
  const out = new Uint8Array(4 + payload.length);
  new DataView(out.buffer).setUint32(0, payload.length, false);
  out.set(payload, 4);
  return out;
}

const USER = 'alice';
const PASSWORD = 'pw';
const CLIENT_NONCE = new Uint8Array(32).fill(0x11);
const SERVER_NONCE = new Uint8Array(32).fill(0x22);
const SALT = new Uint8Array(16).fill(0x33);
const KDF: KdfParams = {
  memoryKb: 19456,
  time: 2,
  parallelism: 1,
  argon2Version: 19,
};

const platform = {
  randomBytes: (n: number) => new Uint8Array(n).fill(0x11),
  hmacSha256: (key: Uint8Array, data: Uint8Array) =>
    new Uint8Array(createHmac('sha256', key).update(data).digest()),
  sha256: (data: Uint8Array) =>
    new Uint8Array(createHash('sha256').update(data).digest()),
  argon2id: async (password: Uint8Array, salt: Uint8Array) =>
    new Uint8Array(createHash('sha256').update(password).update(salt).digest()),
  timingSafeEqual: (a: Uint8Array, b: Uint8Array) =>
    a.length === b.length && a.every((v, i) => v === b[i]),
} as unknown as Platform;

describe('tlsExporterBinding', () => {
  it('builds the tls_exporter profile for TCP and WS', () => {
    const e = new Uint8Array(32).fill(9);
    const tcp = tlsExporterBinding(0x01, e);
    expect(tcp.transportKind).toBe(TRANSPORT_KIND_TCP);
    expect(tcp.bindingMode).toBe(0x01);
    expect(tcp.bindingMode).toBe(BINDING_MODE_TLS_EXPORTER);
    expect(tcp.exporter).toEqual(e);

    const ws = tlsExporterBinding(0x02, e);
    expect(ws.transportKind).toBe(TRANSPORT_KIND_WS);
    expect(ws.bindingMode).toBe(BINDING_MODE_TLS_EXPORTER);
    expect(ws.exporter).toEqual(e);
  });

  it('returns a copy of the exporter', () => {
    const e = new Uint8Array(32).fill(9);
    const a = tlsExporterBinding(0x01, e);
    const b = tlsExporterBinding(0x01, e);
    expect(a.exporter).not.toBe(b.exporter);
    expect(a.exporter).not.toBe(e);

    e[0] = 0xff;
    expect(a.exporter[0]).toBe(9);

    a.exporter[1] = 0xee;
    expect(e[1]).toBe(9);
    expect(b.exporter[1]).toBe(9);
  });

  it('rejects exporters that are not 32 bytes', () => {
    expect(() => tlsExporterBinding(0x01, new Uint8Array(31))).toThrow(RangeError);
    expect(() => tlsExporterBinding(0x01, new Uint8Array(33))).toThrow(
      'TLS exporter must be 32 bytes',
    );
  });
});

describe('runHandshake with the tls_exporter binding', () => {
  it('sends binding_mode 0x01 in auth_init', async () => {
    const socket = new FakeSocket();
    const framer = new WsFramer(socket);
    const handshake = runHandshake(
      platform,
      framer,
      USER,
      PASSWORD,
      tlsExporterBinding(0x01, new Uint8Array(32).fill(9)),
    );
    await new Promise((r) => setTimeout(r, 0));
    const init = decode(socket.sent[0].subarray(4)) as { binding_mode: number };
    expect(init.binding_mode).toBe(0x01);
    socket.simulateClose(new Error('stop'));
    await expect(handshake).rejects.toThrow();
  });

  it('puts the exporter into the auth_message behind client_proof', async () => {
    const exporter = new Uint8Array(32).fill(9);
    const socket = new FakeSocket();
    const framer = new WsFramer(socket);
    socket.messageHandler(
      frame([
        SALT,
        KDF.memoryKb,
        KDF.time,
        KDF.parallelism,
        KDF.argon2Version,
        SERVER_NONCE,
      ]),
    );
    const handshake = runHandshake(
      platform,
      framer,
      USER,
      PASSWORD,
      tlsExporterBinding(0x01, exporter),
    );
    await new Promise((r) => setTimeout(r, 20));
    expect(socket.sent.length).toBe(2);
    const sent = decode(socket.sent[1].subarray(4)) as {
      client_proof: Uint8Array;
    };
    socket.simulateClose(new Error('stop'));
    await expect(handshake).rejects.toThrow();

    const proofFor = async (tlsExporterOrZeros: Uint8Array) => {
      const authMessage = buildAuthMessage(USER, CLIENT_NONCE, {
        serverNonce: SERVER_NONCE,
        salt: SALT,
        kdf: KDF,
        transportKind: 0x01,
        bindingMode: 0x01,
        tlsExporterOrZeros,
      });
      const { clientProof } = await computeClientProof(
        platform,
        PASSWORD,
        SALT,
        KDF,
        authMessage,
      );
      return clientProof;
    };

    expect(new Uint8Array(sent.client_proof)).toEqual(await proofFor(exporter));
    expect(new Uint8Array(sent.client_proof)).not.toEqual(
      await proofFor(new Uint8Array(32)),
    );
  });

  it('hashes zeros for a non-exporter binding even if the exporter field is set', async () => {
    const socket = new FakeSocket();
    const framer = new WsFramer(socket);
    socket.messageHandler(
      frame([
        SALT,
        KDF.memoryKb,
        KDF.time,
        KDF.parallelism,
        KDF.argon2Version,
        SERVER_NONCE,
      ]),
    );
    const binding = browserWsBinding();
    binding.exporter.fill(0x7f);
    const handshake = runHandshake(platform, framer, USER, PASSWORD, binding);
    await new Promise((r) => setTimeout(r, 20));
    expect(socket.sent.length).toBe(2);
    const sent = decode(socket.sent[1].subarray(4)) as {
      client_proof: Uint8Array;
    };
    socket.simulateClose(new Error('stop'));
    await expect(handshake).rejects.toThrow();

    // The pre-exporter form: no exporter in the auth_message at all.
    const authMessage = buildAuthMessage(USER, CLIENT_NONCE, {
      serverNonce: SERVER_NONCE,
      salt: SALT,
      kdf: KDF,
      transportKind: TRANSPORT_KIND_WS,
      bindingMode: 0x02,
    });
    const { clientProof } = await computeClientProof(
      platform,
      PASSWORD,
      SALT,
      KDF,
      authMessage,
    );
    expect(new Uint8Array(sent.client_proof)).toEqual(clientProof);
  });
});
