/**
 * End-to-end tests for plain loopback TCP and local IPC transports
 * (T9-8c, matrix rows 1b, 6d, 6e): `profile: plain` listeners —
 * `kind: tcp` on loopback and `kind: unix` (socket path / named pipe).
 *
 * Covers sessions, small query round-trips, resumption tickets crossing
 * listeners (plain TCP -> TLS TCP allowed, TLS TCP -> plain TCP refused as a downgrade), resume over IPC, and rejection paths
 * (bogus ticket, non-loopback host refused before connecting).
 *
 * The whole file is skipped when no release server binary exists
 * (`describe.skipIf(!SERVER_AVAILABLE)`).
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';

import type { ShamirClient } from '../../../index.js';
import { connect, connectLocal, resume, resumeLocal, Query, Batch } from '../../../index.js';
import {
  SERVER_AVAILABLE,
  HOST,
  ADMIN_USER,
  ADMIN_PASSWORD,
  startMultiListenerServer,
  connectNativeAs,
  br,
  setupDb,
  seed,
} from '../../e2e-harness.js';
import type { MultiServerHandle } from '../../e2e-harness.js';

describe.skipIf(!SERVER_AVAILABLE)(
  'e2e plain TCP + IPC session and resume (requires release binary)',
  () => {
    let server: MultiServerHandle | null = null;
    let admin: ShamirClient | null = null;

    beforeAll(async () => {
      server = await startMultiListenerServer();
      try {
        admin = await connect({
          host: HOST,
          port: server.tcpPlainPort,
          username: ADMIN_USER,
          password: ADMIN_PASSWORD,
          transport: 'tcp-plain',
        });
      } catch (e) {
        console.error('[e2e-plain-ipc] connection failed. Server logs:\n' + server.logs());
        throw e;
      }
    }, 60_000);

    afterAll(async () => {
      if (admin) {
        try { await admin.close(); } catch { /* ok */ }
        admin = null;
      }
      if (server) {
        try { await server.stop(); } catch { /* ok */ }
        server = null;
      }
    }, 15_000);

    it('session over plain loopback TCP: ping and a small batch', async () => {
      const pong = await admin!.ping();
      expect(pong).toBeDefined();
      expect(admin!.sessionId().length).toBe(32);

      const db = await setupDb(admin!, 'plain_tcp');
      await seed(admin!, db, 'items', [
        { id: 'n1', name: 'one' },
        { id: 'n2', name: 'two' },
      ]);
      const resp = br(await Batch.create('plain-tcp-select')
        .add('r', Query.from('items'))
        .execute(admin!, db));
      const ids = resp.results.r.records.map((r) => String(r.id)).sort();
      expect(ids).toEqual(['n1', 'n2']);
    }, 30_000);

    it('resume over plain loopback TCP', async () => {
      const ticket = admin!.resumptionTicket();
      expect(ticket).toBeInstanceOf(Uint8Array);
      expect(ticket!.length).toBeGreaterThan(0);

      const resumed = await resume({
        host: HOST,
        port: server!.tcpPlainPort,
        ticket: ticket!,
        serverPubKey: admin!.serverPubKeyPin(),
        transport: 'tcp-plain',
      });
      try {
        expect(await resumed.ping()).toBeDefined();
        expect(resumed.sessionId().length).toBe(32);
        expect(Buffer.from(resumed.sessionId()).equals(Buffer.from(admin!.sessionId()))).toBe(false);
      } finally {
        try { await resumed.close(); } catch { /* ok */ }
      }
    }, 30_000);

    // The server refuses a resume whose channel binding is WEAKER than the one the ticket was
    // issued under (anti-downgrade: plain 0x00 < browser 0x02 < TLS exporter 0x01 in strength).
    it('ticket crossing: plain ticket resumes over TLS TCP; TLS ticket is refused over plain TCP', async () => {
      const plainClient = await connect({
        host: HOST,
        port: server!.tcpPlainPort,
        username: ADMIN_USER,
        password: ADMIN_PASSWORD,
        transport: 'tcp-plain',
      });
      try {
        // Plain ticket -> TLS TCP (an upgrade: allowed).
        const plainTicket = plainClient.resumptionTicket();
        expect(plainTicket!.length).toBeGreaterThan(0);
        const overTls = await resume({
          host: HOST,
          port: server!.tcpPort,
          ticket: plainTicket!,
          serverPubKey: plainClient.serverPubKeyPin(),
          transport: 'tcp',
          tls: { ca: server!.caPem },
        });
        try {
          expect(await overTls.ping()).toBeDefined();
        } finally {
          try { await overTls.close(); } catch { /* ok */ }
        }
      } finally {
        try { await plainClient.close(); } catch { /* ok */ }
      }

      const tlsClient = await connectNativeAs(
        'tcp',
        HOST,
        server!.tcpPort,
        server!.caPem,
        ADMIN_USER,
        ADMIN_PASSWORD,
      );
      try {
        const tlsTicket = tlsClient.resumptionTicket();
        expect(tlsTicket!.length).toBeGreaterThan(0);
        // TLS-exporter ticket -> plain TCP (a downgrade: refused).
        await expect(
          resume({
            host: HOST,
            port: server!.tcpPlainPort,
            ticket: tlsTicket!,
            serverPubKey: tlsClient.serverPubKeyPin(),
            transport: 'tcp-plain',
          }),
        ).rejects.toThrow();
      } finally {
        try { await tlsClient.close(); } catch { /* ok */ }
      }
    }, 30_000);

    it('IPC session then resume over IPC', async () => {
      const first = await connectLocal({
        addr: server!.ipcAddr,
        username: ADMIN_USER,
        password: ADMIN_PASSWORD,
      });
      try {
        const ticket = first.resumptionTicket();
        expect(ticket).toBeInstanceOf(Uint8Array);
        expect(ticket!.length).toBeGreaterThan(0);

        const resumed = await resumeLocal({
          addr: server!.ipcAddr,
          ticket: ticket!,
          serverPubKey: first.serverPubKeyPin(),
        });
        try {
          expect(await resumed.ping()).toBeDefined();
          expect(resumed.sessionId().length).toBe(32);
          expect(Buffer.from(resumed.sessionId()).equals(Buffer.from(first.sessionId()))).toBe(false);
        } finally {
          try { await resumed.close(); } catch { /* ok */ }
        }
      } finally {
        try { await first.close(); } catch { /* ok */ }
      }
    }, 30_000);

    it('resume over plain TCP rejects a wrong ticket', async () => {
      const ticket = new Uint8Array(96);
      for (let i = 0; i < ticket.length; i += 1) ticket[i] = 0xde;
      await expect(
        resume({
          host: HOST,
          port: server!.tcpPlainPort,
          ticket,
          serverPubKey: admin!.serverPubKeyPin(),
          transport: 'tcp-plain',
        }),
      ).rejects.toThrow();
    }, 30_000);

    it('non-loopback host is refused before connecting', async () => {
      await expect(
        connect({
          host: '10.0.0.1',
          port: server!.tcpPlainPort,
          username: ADMIN_USER,
          password: ADMIN_PASSWORD,
          transport: 'tcp-plain',
        }),
      ).rejects.toThrow(/loopback/);
      await expect(
        connect({
          host: 'example.invalid',
          port: server!.tcpPlainPort,
          username: ADMIN_USER,
          password: ADMIN_PASSWORD,
          transport: 'tcp-plain',
        }),
      ).rejects.toThrow(/loopback/);
    }, 10_000);
  },
);
