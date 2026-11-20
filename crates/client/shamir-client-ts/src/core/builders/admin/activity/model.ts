export interface ListActivityRequest {
  scope: 'all' | 'own';
  kind: 'sessions' | 'requests' | 'transactions' | 'cursors' | 'all';
  limit?: number;
  continuation?: string;
}

export type ListActivityOp = ListActivityRequest & { list_activity: true };

export type ActivityRow =
  | {
      kind: 'session';
      session_handle: bigint;
      user: string;
      transport: string;
      created_at_ns: bigint;
      idle_ns: bigint;
      open_transactions: number;
      open_cursors: number;
    }
  | {
      kind: 'request';
      request_exec_id: bigint;
      session_handle?: bigint;
      user: string;
      db: string;
      op_kind: string;
      state: 'running' | 'done';
      elapsed_ns: bigint;
    }
  | {
      kind: 'transaction';
      tx_handle: bigint;
      session_handle: bigint;
      user: string;
      state: 'active' | 'awaiting_commit' | 'expired_pending_reap';
      idle_ns: bigint;
    }
  | {
      kind: 'cursor';
      cursor_id: bigint;
      session_handle: bigint;
      user: string;
      table: string;
      pinned_version: bigint;
      open_ns: bigint;
    };

export interface ActivityPage {
  rows: ActivityRow[];
  continuation?: string;
}
