import { describe, expect, it } from 'vitest';

import { decodeActivityPage } from '../decode.js';
import { ActivityProtocolError } from '../error.js';
import type { ActivityRow } from '../model.js';

const aboveSafe = 9007199254740993n;
const maxU64 = 18446744073709551615n;

const session = {
  kind: 'session', session_handle: aboveSafe, user: 'alice', transport: 'tcp',
  created_at_ns: maxU64, idle_ns: 0, open_transactions: 1, open_cursors: 2n,
};
const request = {
  kind: 'request', request_exec_id: maxU64, user: 'alice', db: 'demo',
  op_kind: 'query', state: 'running', elapsed_ns: 3,
};
const transaction = {
  kind: 'transaction', tx_handle: 4, session_handle: aboveSafe, user: 'alice',
  state: 'awaiting_commit', idle_ns: 5,
};
const cursor = {
  kind: 'cursor', cursor_id: 6, session_handle: aboveSafe, user: 'alice',
  table: 'items', pinned_version: maxU64, open_ns: 7,
};

const expectedRows = [
  {
    kind: 'session', session_handle: aboveSafe, user: 'alice', transport: 'tcp',
    created_at_ns: maxU64, idle_ns: 0n, open_transactions: 1, open_cursors: 2,
  },
  {
    kind: 'request', request_exec_id: maxU64, user: 'alice', db: 'demo',
    op_kind: 'query', state: 'running', elapsed_ns: 3n,
  },
  {
    kind: 'transaction', tx_handle: 4n, session_handle: aboveSafe, user: 'alice',
    state: 'awaiting_commit', idle_ns: 5n,
  },
  {
    kind: 'cursor', cursor_id: 6n, session_handle: aboveSafe, user: 'alice',
    table: 'items', pinned_version: maxU64, open_ns: 7n,
  },
] satisfies ActivityRow[];

const requiredFields = [session, request, transaction, cursor].flatMap((source) =>
  Object.keys(source).map((field) => ({ kind: source.kind, field, source })),
);

function expectShapeMismatch(page: unknown) {
  const decode = () => decodeActivityPage(page);
  expect(decode).toThrow(ActivityProtocolError);
  expect(decode).toThrow(expect.objectContaining({ code: 'shape_mismatch' }));
}

describe('decodeActivityPage', () => {
  it('decodes all four variants exactly with absent optional fields omitted', () => {
    expect(decodeActivityPage({ rows: [session, request, transaction, cursor] }))
      .toStrictEqual({ rows: expectedRows });
  });

  it('decodes present optionals and projects away extra fields on pages and rows', () => {
    const rows = [session, { ...request, session_handle: 8, state: 'done' }, transaction, cursor]
      .map((row) => ({ ...row, unexpected: 'drop' }));
    expect(decodeActivityPage({ rows, continuation: 'next', unexpected: 'drop' })).toStrictEqual({
      rows: [expectedRows[0], { ...expectedRows[1], session_handle: 8n, state: 'done' }, expectedRows[2], expectedRows[3]],
      continuation: 'next',
    });
  });

  it('accepts an empty page', () => {
    expect(decodeActivityPage({ rows: [] })).toStrictEqual({ rows: [] });
  });

  it.each(['active', 'expired_pending_reap'] as const)('accepts transaction state %s', (state) => {
    expect(decodeActivityPage({ rows: [{ ...transaction, state }] }))
      .toStrictEqual({ rows: [{ ...expectedRows[2], state }] });
  });

  it.each(requiredFields)('rejects missing $kind.$field', ({ source, field }) => {
    const row: Record<string, unknown> = { ...source };
    delete row[field];
    expectShapeMismatch({ rows: [row] });
  });

  it.each(requiredFields)('rejects mistyped $kind.$field', ({ source, field }) => {
    expectShapeMismatch({ rows: [{ ...source, [field]: false }] });
  });

  it.each([
    { row: { ...session, kind: 'sessions' } },
    { row: { ...request, state: 'active' } },
    { row: { ...transaction, state: 'running' } },
  ])('rejects unknown row kind or state: $row', ({ row }) => {
    expectShapeMismatch({ rows: [row] });
  });

  it.each([
    { value: '1' }, { value: -1 }, { value: -1n }, { value: 0.5 },
    { value: Number.MAX_SAFE_INTEGER + 1 }, { value: NaN }, { value: Infinity },
    { value: maxU64 + 1n },
  ])('rejects invalid u64 $value', ({ value }) => {
    expectShapeMismatch({ rows: [{ ...cursor, pinned_version: value }] });
  });

  it.each([
    { value: '1' }, { value: -1 }, { value: -1n }, { value: 0.5 }, { value: NaN },
    { value: Number.MAX_SAFE_INTEGER + 1 }, { value: BigInt(Number.MAX_SAFE_INTEGER) + 1n },
  ])('rejects invalid counts $value', ({ value }) => {
    expectShapeMismatch({ rows: [{ ...session, open_transactions: value }] });
    expectShapeMismatch({ rows: [{ ...session, open_cursors: value }] });
  });

  it.each([{ value: null }, { value: undefined }, { value: false }])(
    'rejects present invalid continuation or request session handle: $value', ({ value }) => {
      expectShapeMismatch({ rows: [], continuation: value });
      expectShapeMismatch({ rows: [{ ...request, session_handle: value }] });
    },
  );

  it.each([
    { name: 'null page', page: null },
    { name: 'undefined page', page: undefined },
    { name: 'array page', page: [] },
    { name: 'non-record page', page: new Map([['rows', []]]) },
    { name: 'missing rows', page: {} },
    { name: 'non-array rows', page: { rows: { 0: session, length: 1 } } },
    { name: 'null rows', page: { rows: null } },
    { name: 'sparse rows', page: { rows: new Array<ActivityRow>(1) } },
    { name: 'non-record row', page: { rows: [session, new Map(Object.entries(request))] } },
    { name: 'null row', page: { rows: [null] } },
    { name: 'array row', page: { rows: [[]] } },
  ])('rejects $name', ({ page }) => {
    expectShapeMismatch(page);
  });
});
