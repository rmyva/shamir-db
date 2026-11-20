import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

import { Batch } from '../../../write/batch.js';
import type { ListActivityRequest } from '../model.js';
import { listActivity } from '../wire.js';

const vectors = JSON.parse(readFileSync(new URL(
  '../../../../../../../../query/shamir-query-builder/src/wire/tests/fixtures/list_activity.json',
  import.meta.url,
), 'utf8')) as Record<'populated' | 'omitted', unknown>;

const requests = {
  populated: { scope: 'all', kind: 'requests', limit: 50, continuation: 'tok' },
  omitted: { scope: 'own', kind: 'all' },
} satisfies Record<keyof typeof vectors, ListActivityRequest>;

function build(request: ListActivityRequest) {
  return Batch.create().add('activity', listActivity(request)).build();
}

describe('ListActivity wire', () => {
  it.each(['populated', 'omitted'] as const)('flattens the %s Rust golden map into the batch entry', (name) => {
    expect(build(requests[name]).queries).toStrictEqual({ activity: vectors[name] });
  });

  it.each([0, 0xffffffff])('accepts limit %s', (limit) => {
    expect(build({ ...requests.omitted, limit }).queries.activity).toHaveProperty('limit', limit);
  });

  it.each([-1, 0.5, 0x100000000, Number.MAX_SAFE_INTEGER + 1, NaN, Infinity, -Infinity])(
    'rejects numeric limit %s outside u32', (limit) => {
      expect(() => build({ ...requests.omitted, limit })).toThrow(RangeError);
    },
  );

  it.each([
    { field: 'scope', value: 'other' }, { field: 'scope', value: 1 }, { field: 'scope', value: null },
    { field: 'scope', value: undefined },
    { field: 'kind', value: 'request' }, { field: 'kind', value: false }, { field: 'kind', value: null },
    { field: 'kind', value: undefined },
    { field: 'limit', value: '50' }, { field: 'limit', value: 50n }, { field: 'limit', value: null },
    { field: 'continuation', value: 1 }, { field: 'continuation', value: null },
  ])('rejects $field = $value', ({ field, value }) => {
    const request = { ...requests.omitted, [field]: value } as ListActivityRequest;
    expect(() => build(request)).toThrow(TypeError);
  });

  it.each([{ value: null }, { value: undefined }, { value: false }, { value: [] }])(
    'rejects non-map request $value', ({ value }) => {
      expect(() => build(value as unknown as ListActivityRequest)).toThrow(TypeError);
    },
  );
});
