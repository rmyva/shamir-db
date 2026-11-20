import { CURRENT_QUERY_LANG_VERSION } from '../../../protocol/scram.js';
import type { BatchRequest } from '../../../types/write/batch.js';
import { Batch } from '../../write/batch.js';
import type { ListActivityOp, ListActivityRequest } from './model.js';

export function listActivity(request: ListActivityRequest): ListActivityOp {
  if (request === null || typeof request !== 'object' || Array.isArray(request)) {
    throw new TypeError('expected a list activity request object');
  }
  const { scope, kind, limit, continuation } = request;
  if (scope !== 'all' && scope !== 'own') throw new TypeError('invalid activity scope');
  if (kind !== 'sessions' && kind !== 'requests' && kind !== 'transactions' && kind !== 'cursors' && kind !== 'all') {
    throw new TypeError('invalid activity kind');
  }

  const op: ListActivityOp = { list_activity: true, scope, kind };
  if (limit !== undefined) {
    if (typeof limit !== 'number') throw new TypeError('activity limit must be a number');
    if (!Number.isInteger(limit) || limit < 0 || limit > 0xffffffff) {
      throw new RangeError('activity limit must be a u32');
    }
    op.limit = limit;
  }
  if (continuation !== undefined) {
    if (typeof continuation !== 'string') throw new TypeError('activity continuation must be a string');
    op.continuation = continuation;
  }
  return op;
}

export function activityRequest(db: string, request: ListActivityRequest): {
  op: 'execute';
  query_version: number;
  db: string;
  batch: BatchRequest;
} {
  return {
    op: 'execute',
    query_version: CURRENT_QUERY_LANG_VERSION,
    db,
    batch: Batch.create().add('list_activity', listActivity(request)).build(),
  };
}
