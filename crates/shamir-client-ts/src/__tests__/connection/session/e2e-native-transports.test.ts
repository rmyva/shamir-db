/**
 * End-to-end tests for the native Node session transports (T9-6, matrix rows
 * 1, 2, 6a, 6b for TS Node): TCP+TLS and native WebSocket (`wss://`) against
 * `tls_exporter` listeners, plus resume over both and the default browser
 * transport.
 *
 * The whole file is skipped when no release server binary exists
 * (`describe.skipIf(!SERVER_AVAILABLE)`).
 *
 * A successful SCRAM handshake over a `tls_exporter` listener already proves
 * client and server agree on the TLS exporter (it is part of auth_message), so
 * there is no separate exporter-equality assertion.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';

import type { ShamirClient } from '../../../index.js';
import { connect, resume, Query, Batch } from '../../../index.js';
import {
  SERVER_AVAILABLE,
  HOST,
  ADMIN_USER,
  ADMIN_PASSWORD,
  startMultiListenerServer,
  connectNativeAs,
  connectAdmin,
  readSelfSignedCaPem,
  br,
  setupDb,
  seed,
} from '../../e2e-harness.js';
import type { MultiServerHandle } from '../../e2e-harness.js';

describe.skipIf(!SERVER_AVAILABLE)(
  'e2e native transports (requires release binary)',
  () => {
    let server: MultiServerHandle | null = null;
    let admin: ShamirClient | null = null;

    beforeAll(async () => {
      server = await startMultiListenerServer();
      try {
        admin = await connectNativeAs(
          'tcp',
          HOST,
          server.tcpPort,
          server.caPem,
          ADMIN_USER,
          ADMIN_PASSWORD,
        );
      } catch (e) {
        console.error('[e2e-native-transports] connection failed. Server logs:\n' + server.logs());
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

    async function smallQuery(client: ShamirClient, label: string): Promise<void> {
      const pong = await client.ping();
      expect(pong).toBeDefined();
      expect(client.sessionId().length).toBe(32);

      const db = await setupDb(client, label);
      await seed(client, db, 'items', [
        { id: 'n1', name: 'one' },
        { id: 'n2', name: 'two' },
      ]);
      const resp = br(await Batch.create('native-select')
        .add('r', Query.from('items'))
        .execute(client, db));
      const ids = resp.results.r.records.map((r) => String(r.id)).sort();
      expect(ids).toEqual(['n1', 'n2']);
    }

    it('session over TCP+TLS: ping and a small query', async () => {
      await smallQuery(admin!, 'native_tcp');
    }, 30_000);

    it('session over native WebSocket: ping and a small query', async () => {
      const client = await connectNativeAs(
        'ws-native',
        HOST,
        server!.wsNativePort,
        server!.caPem,
        ADMIN_USER,
        ADMIN_PASSWORD,
      );
      try {
        await smallQuery(client, 'native_ws');
      } finally {
        try { await client.close(); } catch { /* ok */ }
      }
    }, 30_000);

    it('default transport still uses the browser profile listener', async () => {
      const client = await connectAdmin(HOST, server!.browserPort);
      try {
        expect(await client.ping()).toBeDefined();
      } finally {
        try { await client.close(); } catch { /* ok */ }
      }
    }, 30_000);

    it('resume over TCP+TLS', async () => {
      const ticket = admin!.resumptionTicket();
      expect(ticket).toBeInstanceOf(Uint8Array);
      expect(ticket!.length).toBeGreaterThan(0);

      const resumed = await resume({
        host: HOST,
        port: server!.tcpPort,
        ticket: ticket!,
        serverPubKey: admin!.serverPubKeyPin(),
        transport: 'tcp',
        tls: { ca: server!.caPem },
      });
      try {
        expect(await resumed.ping()).toBeDefined();
        expect(resumed.sessionId().length).toBe(32);
        expect(Buffer.from(resumed.sessionId()).equals(Buffer.from(admin!.sessionId()))).toBe(false);
      } finally {
        try { await resumed.close(); } catch { /* ok */ }
      }
    }, 30_000);

    it('resume over native WebSocket', async () => {
      const first = await connectNativeAs(
        'ws-native',
        HOST,
        server!.wsNativePort,
        server!.caPem,
        ADMIN_USER,
        ADMIN_PASSWORD,
      );
      try {
        const ticket = first.resumptionTicket();
        expect(ticket).toBeInstanceOf(Uint8Array);
        expect(ticket!.length).toBeGreaterThan(0);

        const resumed = await resume({
          host: HOST,
          port: server!.wsNativePort,
          ticket: ticket!,
          serverPubKey: first.serverPubKeyPin(),
          transport: 'ws-native',
          tls: { ca: server!.caPem },
        });
        try {
          expect(await resumed.ping()).toBeDefined();
        } finally {
          try { await resumed.close(); } catch { /* ok */ }
        }
      } finally {
        try { await first.close(); } catch { /* ok */ }
      }
    }, 30_000);

    it('resume rejects a bogus ticket over TCP+TLS', async () => {
      const ticket = new Uint8Array(100);
      for (let i = 0; i < ticket.length; i += 1) ticket[i] = Math.floor(Math.random() * 256);
      await expect(
        resume({
          host: HOST,
          port: server!.tcpPort,
          ticket,
          serverPubKey: admin!.serverPubKeyPin(),
          transport: 'tcp',
          tls: { ca: server!.caPem },
        }),
      ).rejects.toThrow();
    }, 30_000);

    it('changePassword over TCP+TLS uses the real exporter', async () => {
      const uname = `cpw_tcp_${process.pid}_${Date.now()}`;
      const oldPw = 'old correct horse battery staple';
      const newPw = 'new correct horse battery staple';

      await admin!.createScramUser(uname, oldPw, []);

      const user = await connectNativeAs('tcp', HOST, server!.tcpPort, server!.caPem, uname, oldPw);
      try {
        await user.changePassword(oldPw, newPw);
      } finally {
        try { await user.close(); } catch { /* ok */ }
      }

      await expect(
        connectNativeAs('tcp', HOST, server!.tcpPort, server!.caPem, uname, oldPw),
      ).rejects.toThrow();

      const reconnected = await connectNativeAs(
        'tcp',
        HOST,
        server!.tcpPort,
        server!.caPem,
        uname,
        newPw,
      );
      await reconnected.close();
    }, 30_000);

    it('wrong CA is rejected at connect (TCP+TLS)', async () => {
      const otherCa = readSelfSignedCaPem();
      if (otherCa === null) {
        console.warn('[e2e-native-transports] openssl unavailable; skipping wrong-CA assertions');
        return;
      }
      await expect(
        connect({
          host: HOST,
          port: server!.tcpPort,
          username: ADMIN_USER,
          password: ADMIN_PASSWORD,
          transport: 'tcp',
          tls: { ca: otherCa },
          connectTimeoutMs: 5000,
        }),
      ).rejects.toThrow();
      await expect(
        connect({
          host: HOST,
          port: server!.wsNativePort,
          username: ADMIN_USER,
          password: ADMIN_PASSWORD,
          transport: 'ws-native',
          tls: { ca: otherCa },
          connectTimeoutMs: 5000,
        }),
      ).rejects.toThrow();
    }, 30_000);

    it('no CA configured is rejected by default (rejectUnauthorized true)', async () => {
      await expect(
        connect({
          host: HOST,
          port: server!.tcpPort,
          username: ADMIN_USER,
          password: ADMIN_PASSWORD,
          transport: 'tcp',
          connectTimeoutMs: 5000,
        }),
      ).rejects.toThrow();

      const client = await connect({
        host: HOST,
        port: server!.tcpPort,
        username: ADMIN_USER,
        password: ADMIN_PASSWORD,
        transport: 'tcp',
        tls: { rejectUnauthorized: false },
      });
      try {
        expect(await client.ping()).toBeDefined();
      } finally {
        try { await client.close(); } catch { /* ok */ }
      }
    }, 30_000);
  },
);
