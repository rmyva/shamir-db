/**
 * End-to-end tests for identity pinning and browser-entry resume (T9-8d):
 * (a) connect with the right pin succeeds on the browser WS listener and on
 *     native TCP+TLS; (b) a wrong pin is refused with the typed error;
 * (c) resume from the BROWSER entry point over the browser WS listener after
 *     a browser-profile session.
 *
 * The whole file is skipped when no release server binary exists
 * (`describe.skipIf(!SERVER_AVAILABLE)`).
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';

import type { ShamirClient } from '../../../index.js';
import { connect } from '../../../index.js';
import { connect as connectBrowser, resume as resumeBrowser, ServerIdentityChangedError as BrowserServerIdentityChangedError } from '../../../browser.js';
import {
  SERVER_AVAILABLE,
  HOST,
  ORIGIN,
  ADMIN_USER,
  ADMIN_PASSWORD,
  startMultiListenerServer,
} from '../../e2e-harness.js';
import { installBrowserSocket } from '../token/support/browser-socket.js';
import type { BrowserSocketObserver } from '../token/support/browser-socket.js';
import type { MultiServerHandle } from '../../e2e-harness.js';

describe.skipIf(!SERVER_AVAILABLE)(
  'e2e browser resume + identity pin (requires release binary)',
  () => {
    let server: MultiServerHandle | null = null;
    let first: ShamirClient | null = null;
    let firstPin: Uint8Array | null = null;
    let firstTicket: Uint8Array | undefined;
    let browserSocket: BrowserSocketObserver | null = null;

    beforeAll(async () => {
      server = await startMultiListenerServer();
      // Browser entry points on Node: install the ws-based WebSocket
      // adapter that sends the allow-listed Origin and trusts the harness
      // CA (same approach as e2e-browser.test.ts). Left installed for the
      // whole suite — `resumeBrowser` needs it too — and restored in afterAll.
      browserSocket = installBrowserSocket(ORIGIN, server.caPem);
      try {
        first = await connectBrowser({
          host: HOST,
          port: server.browserPort,
          username: ADMIN_USER,
          password: ADMIN_PASSWORD,
        });
        firstPin = first.trustedPin();
        firstTicket = first.resumptionTicket();
        expect(firstTicket).toBeInstanceOf(Uint8Array);
        expect(firstTicket!.length).toBeGreaterThan(0);
      } catch (e) {
        console.error('[e2e-browser-resume] connection failed. Server logs:\n' + server.logs());
        throw e;
      }
    }, 60_000);

    afterAll(async () => {
      if (first) {
        try { await first.close(); } catch { /* ok */ }
        first = null;
      }
      if (browserSocket) {
        browserSocket.restore();
        browserSocket = null;
      }
      if (server) {
        try { await server.stop(); } catch { /* ok */ }
        server = null;
      }
    }, 15_000);

    it('connect with the right pin succeeds on the browser WS listener', async () => {
      const client = await connectBrowser({
        host: HOST,
        port: server!.browserPort,
        username: ADMIN_USER,
        password: ADMIN_PASSWORD,
        trustedPin: firstPin!,
      });
      try {
        expect(await client.ping()).toBeDefined();
        expect(Buffer.from(client.trustedPin()).equals(Buffer.from(firstPin!))).toBe(true);
        expect(client.sessionId().length).toBe(32);
      } finally {
        try { await client.close(); } catch { /* ok */ }
      }
    }, 30_000);

    it('connect with the right pin succeeds on native TCP+TLS', async () => {
      const client = await connect({
        host: HOST,
        port: server!.tcpPort,
        username: ADMIN_USER,
        password: ADMIN_PASSWORD,
        transport: 'tcp',
        tls: { ca: server!.caPem },
        trustedPin: firstPin!,
      });
      try {
        expect(await client.ping()).toBeDefined();
      } finally {
        try { await client.close(); } catch { /* ok */ }
      }
    }, 30_000);

    it('wrong pin is refused with the typed error', async () => {
      const wrongPin = new Uint8Array(32);
      wrongPin.fill(0x99);
      const attempt = connectBrowser({
        host: HOST,
        port: server!.browserPort,
        username: ADMIN_USER,
        password: ADMIN_PASSWORD,
        trustedPin: wrongPin,
      });
      await expect(attempt).rejects.toBeInstanceOf(BrowserServerIdentityChangedError);
      await expect(attempt).rejects.toThrow();
    }, 30_000);

    it('resume from the browser entry point over the browser WS listener', async () => {
      const resumed = await resumeBrowser({
        host: HOST,
        port: server!.browserPort,
        ticket: firstTicket!,
        serverPubKey: first!.serverPubKeyPin(),
      });
      try {
        expect(await resumed.ping()).toBeDefined();
        expect(resumed.sessionId().length).toBe(32);
        expect(Buffer.from(resumed.trustedPin()).equals(Buffer.from(firstPin!))).toBe(true);
      } finally {
        try { await resumed.close(); } catch { /* ok */ }
      }
    }, 30_000);
  },
);
