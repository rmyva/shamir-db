import { describe, expect, it } from 'vitest';
import { ActivityProtocolError, Batch, listActivity, listAllActivity, ShamirDbError } from '../../../../../browser.js';
import type { ActivityPage, ListActivityRequest, ShamirClient } from '../../../../../browser.js';
import { CURRENT_QUERY_LANG_VERSION } from '../../../../protocol/scram.js';
import { fixture } from '../../../../visibility/__tests__/fixture.js';

const db = 'activity_db';
const request: ListActivityRequest = {
  scope: 'all', kind: 'sessions', limit: 7, continuation: 'start:+/=%2F',
};

async function roundTrip(
  response: Record<string, unknown>,
  inspect?: (sent: Record<string, unknown>, requests: Record<string, unknown>[]) => void,
): Promise<ActivityPage> {
  const f = await fixture();
  try {
    const pending = f.client.listActivity(db, request);
    const sent = await Promise.race([
      f.nextRequest(),
      pending.then(() => { throw new Error('listActivity completed before the scripted reply'); }),
    ]);
    f.respond(response);
    const page = await pending;
    inspect?.(sent, f.requests);
    return page;
  } finally {
    await f.client.close();
  }
}

describe('ShamirClient.listActivity', () => {
  it('sends one standalone builder batch and decodes the top-level activity page', async () => {
    const handle = 9_007_199_254_740_993n;
    const row = {
      kind: 'session', session_handle: handle, user: 'operator', transport: 'ws',
      created_at_ns: handle + 2n, idle_ns: 3, open_transactions: 0, open_cursors: 1,
    };
    const page = await roundTrip(
      { kind: 'activity_page', page: { rows: [row], continuation: 'next:+/=%2F' } },
      (sent, requests) => {
        expect(requests).toHaveLength(1);
        expect(sent).toStrictEqual({
          op: 'execute',
          query_version: CURRENT_QUERY_LANG_VERSION,
          db,
          batch: Batch.create().add('list_activity', listActivity(request)).build(),
        });
      },
    );
    expect(page).toStrictEqual({ rows: [{ ...row, idle_ns: 3n }], continuation: 'next:+/=%2F' });
  });

  it.each([
    { code: 'permission_denied', detail: 'Listing all activity requires permission' },
    { code: 'invalid_request', detail: 'Unknown operation type' },
    { code: 'token_request_denied', detail: 'Listing activity requires a SCRAM session' },
  ])('preserves the server error $code and its detail', async ({ code, detail }) => {
    const pending = roundTrip({ kind: 'error', code, message: detail });
    await expect(pending).rejects.toBeInstanceOf(ShamirDbError);
    await expect(pending).rejects.toMatchObject({ code, detail });
  });

  it.each([
    {
      response: { kind: 'activity_page', page: { rows: 'not an array' } },
      code: 'shape_mismatch',
    },
    {
      response: {
        kind: 'batch', response: { id: 1, results: {}, execution_plan: [], execution_time_us: 0 },
      },
      code: 'unexpected_response',
    },
    { response: { kind: 'activity_page' }, code: 'shape_mismatch' },
    { response: { kind: 'pong' }, code: 'unexpected_response' },
    {
      response: {
        kind: 'as_of_visibility_timeout', code: 'as_of_visibility_timeout',
        message: 'Visibility wait timed out', requested: 12n, visible_through: 9n, blocking_version: 10n,
      },
      code: 'unexpected_response',
    },
  ])('rejects $response.kind as $code', async ({ response, code }) => {
    const pending = roundTrip(response);
    await expect(pending).rejects.toBeInstanceOf(ActivityProtocolError);
    await expect(pending).rejects.toMatchObject({ code });
  });

  it('rejects a MessagePack null as unexpected_response', async () => {
    const pending = roundTrip(null as unknown as Record<string, unknown>);
    await expect(pending).rejects.toBeInstanceOf(ActivityProtocolError);
    await expect(pending).rejects.toMatchObject({ code: 'unexpected_response' });
  });
});

describe('listAllActivity', () => {
  const opaque = '  next:+/=%2F  ';
  const first: ActivityPage = { rows: [], continuation: opaque };
  const start: ListActivityRequest = { ...request, continuation: 'resume:+/=%2F' };

  it.each([
    { reason: 'the cap', maxPages: 2, last: { rows: [], continuation: 'more' } },
    { reason: 'an absent continuation', maxPages: 1000, last: { rows: [] } },
    { reason: 'an empty continuation', maxPages: 3, last: { rows: [], continuation: '' } },
  ])('stops at $reason without an extra call', async ({ maxPages, last }) => {
    const pages: ActivityPage[] = [first, last];
    const calls: Array<[string, ListActivityRequest]> = [];
    const client: Pick<ShamirClient, 'listActivity'> = {
      async listActivity(database, options) {
        calls.push([database, { ...options }]);
        const page = pages[calls.length - 1];
        if (page === undefined) throw new Error('unexpected additional listActivity call');
        return page;
      },
    };
    const yielded: ActivityPage[] = [];
    for await (const page of listAllActivity(client, db, start, maxPages)) yielded.push(page);
    expect(yielded).toStrictEqual(pages);
    expect(calls).toStrictEqual([[db, start], [db, { ...start, continuation: opaque }]]);
  });

  it('finishes without requesting a page for an empty starting continuation', async () => {
    const client: Pick<ShamirClient, 'listActivity'> = {
      async listActivity() { throw new Error('unexpected listActivity call'); },
    };
    const pages = listAllActivity(client, db, { ...start, continuation: '' }, 1);
    expect((await pages.next()).done).toBe(true);
  });

  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, 1001])(
    'rejects maxPages=%s before requesting a page',
    async (maxPages) => {
      let calls = 0;
      const client: Pick<ShamirClient, 'listActivity'> = {
        async listActivity() {
          calls++;
          return { rows: [] };
        },
      };
      await expect((async () => listAllActivity(client, db, start, maxPages).next())())
        .rejects.toBeInstanceOf(RangeError);
      expect(calls).toBe(0);
    },
  );
});
