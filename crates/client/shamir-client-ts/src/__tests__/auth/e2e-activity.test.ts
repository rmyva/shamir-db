import { describe, expect, it } from 'vitest';
import { listAllActivity, ShamirDbError } from '../../index.js';
import type { ActivityPage, ShamirClient } from '../../index.js';
import { ADMIN_USER, connectAdmin, connectAs, HOST, SERVER_AVAILABLE, startServer } from '../e2e-harness.js';

function onlySessionHandle(page: ActivityPage, username: string): bigint {
  expect(page.rows.length).toBe(1);
  const [row] = page.rows;
  if (row?.kind !== 'session' || row.user !== username || typeof row.session_handle !== 'bigint') {
    throw new Error('Own activity did not return the expected session');
  }
  return row.session_handle;
}

describe.skipIf(!SERVER_AVAILABLE)('e2e activity', () => {
  it('isolates Own sessions and lets an admin page through All sessions', async () => {
    const server = await startServer();
    let admin: ShamirClient | undefined;
    let ordinary: ShamirClient | undefined;
    try {
      const db = 'default';
      const username = 'activity_reader';
      const password = 'test-only activity password';
      admin = await connectAdmin(HOST, server.port);
      await admin.createScramUser(username, password, []);
      ordinary = await connectAs(HOST, server.port, username, password);

      const adminHandle = onlySessionHandle(
        await admin.listActivity(db, { scope: 'own', kind: 'sessions' }), ADMIN_USER,
      );
      const ordinaryHandle = onlySessionHandle(
        await ordinary.listActivity(db, { scope: 'own', kind: 'sessions' }), username,
      );
      expect(adminHandle !== ordinaryHandle, 'distinct session handles').toBe(true);

      const denied = await ordinary.listActivity(db, { scope: 'all', kind: 'sessions' }).then(
        () => false,
        (error: unknown) => error instanceof ShamirDbError && error.code === 'permission_denied',
      );
      expect(denied, 'ordinary All must throw ShamirDbError: permission_denied').toBe(true);

      const seen = new Set<string>();
      let pages = 0;
      for await (const page of listAllActivity(admin, db, { scope: 'all', kind: 'sessions', limit: 1 }, 3)) {
        pages += 1;
        expect(page.rows.length).toBe(1);
        const [row] = page.rows;
        if (row?.kind !== 'session' || !(
          (row.user === ADMIN_USER && row.session_handle === adminHandle) ||
          (row.user === username && row.session_handle === ordinaryHandle)
        )) {
          throw new Error('All activity returned an unexpected session');
        }
        seen.add(row.user);
        if (pages === 1) {
          expect(typeof page.continuation === 'string' && page.continuation.length > 0).toBe(true);
        } else {
          expect(page.continuation === undefined).toBe(true);
        }
      }
      expect(pages).toBe(2);
      expect(seen.size).toBe(2);
    } finally {
      try {
        await ordinary?.close();
      } finally {
        try {
          await admin?.close();
        } finally {
          await server.stop();
        }
      }
    }
  });
});
