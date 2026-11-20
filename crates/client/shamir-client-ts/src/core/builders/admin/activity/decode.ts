import { ActivityProtocolError } from './error.js';
import type { ActivityPage, ActivityRow } from './model.js';

function mismatch(field: string): never {
  throw new ActivityProtocolError('shape_mismatch', `invalid activity ${field}`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false;
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function record(value: unknown, field: string): Record<string, unknown> {
  if (!isRecord(value)) mismatch(field);
  return value;
}

function own(record: Record<string, unknown>, field: string): unknown {
  return Object.hasOwn(record, field) ? record[field] : undefined;
}

function text(value: unknown, field: string): string {
  if (typeof value !== 'string') mismatch(field);
  return value;
}

function u64(value: unknown, field: string): bigint {
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return BigInt(value);
  if (typeof value === 'bigint' && value >= 0n && value <= 0xffffffffffffffffn) return value;
  return mismatch(field);
}

function count(value: unknown, field: string): number {
  const integer = u64(value, field);
  if (integer > BigInt(Number.MAX_SAFE_INTEGER)) mismatch(field);
  return Number(integer);
}

function decodeRow(value: unknown): ActivityRow {
  const row = record(value, 'row');
  const kind = own(row, 'kind');
  switch (kind) {
    case 'session':
      return {
        kind,
        session_handle: u64(own(row, 'session_handle'), 'session_handle'),
        user: text(own(row, 'user'), 'user'),
        transport: text(own(row, 'transport'), 'transport'),
        created_at_ns: u64(own(row, 'created_at_ns'), 'created_at_ns'),
        idle_ns: u64(own(row, 'idle_ns'), 'idle_ns'),
        open_transactions: count(own(row, 'open_transactions'), 'open_transactions'),
        open_cursors: count(own(row, 'open_cursors'), 'open_cursors'),
      };
    case 'request': {
      const state = own(row, 'state');
      if (state !== 'running' && state !== 'done') mismatch('request state');
      const result: ActivityRow = {
        kind,
        request_exec_id: u64(own(row, 'request_exec_id'), 'request_exec_id'),
        user: text(own(row, 'user'), 'user'),
        db: text(own(row, 'db'), 'db'),
        op_kind: text(own(row, 'op_kind'), 'op_kind'),
        state,
        elapsed_ns: u64(own(row, 'elapsed_ns'), 'elapsed_ns'),
      };
      if (Object.hasOwn(row, 'session_handle')) result.session_handle = u64(row.session_handle, 'session_handle');
      return result;
    }
    case 'transaction': {
      const state = own(row, 'state');
      if (state !== 'active' && state !== 'awaiting_commit' && state !== 'expired_pending_reap') mismatch('transaction state');
      return {
        kind,
        tx_handle: u64(own(row, 'tx_handle'), 'tx_handle'),
        session_handle: u64(own(row, 'session_handle'), 'session_handle'),
        user: text(own(row, 'user'), 'user'),
        state,
        idle_ns: u64(own(row, 'idle_ns'), 'idle_ns'),
      };
    }
    case 'cursor':
      return {
        kind,
        cursor_id: u64(own(row, 'cursor_id'), 'cursor_id'),
        session_handle: u64(own(row, 'session_handle'), 'session_handle'),
        user: text(own(row, 'user'), 'user'),
        table: text(own(row, 'table'), 'table'),
        pinned_version: u64(own(row, 'pinned_version'), 'pinned_version'),
        open_ns: u64(own(row, 'open_ns'), 'open_ns'),
      };
    default:
      return mismatch('row kind');
  }
}

export function decodeActivityPage(page: unknown): ActivityPage {
  const source = record(page, 'page');
  const rows = own(source, 'rows');
  if (!Array.isArray(rows)) mismatch('rows');
  const result: ActivityPage = { rows: [] };
  for (const row of rows) result.rows.push(decodeRow(row));
  if (Object.hasOwn(source, 'continuation')) result.continuation = text(source.continuation, 'continuation');
  return result;
}
