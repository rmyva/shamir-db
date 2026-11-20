import type { ShamirClient } from '../../../client/client.js';
import type { ActivityPage, ListActivityRequest } from './model.js';

export function listAllActivity(
  client: Pick<ShamirClient, 'listActivity'>,
  db: string,
  request: ListActivityRequest,
  maxPages: number,
): AsyncGenerator<ActivityPage> {
  if (!Number.isSafeInteger(maxPages) || maxPages < 1 || maxPages > 1000) {
    throw new RangeError('maxPages must be an integer from 1 to 1000');
  }
  const { scope, kind, limit, continuation: start } = request;

  return (async function* () {
    let continuation = start;
    for (let i = 0; i < maxPages && continuation !== ''; i++) {
      const page = await client.listActivity(db, { scope, kind, limit, continuation });
      continuation = page.continuation;
      yield page;
      if (continuation === undefined) return;
    }
  })();
}
