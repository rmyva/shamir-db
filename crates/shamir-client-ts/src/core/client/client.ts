/**
 * ShamirClient — platform-agnostic core.
 *
 * Constructed with a `Platform`; all platform specifics (crypto, sockets)
 * are delegated there. No `node:crypto`, `ws`, or WebCrypto imports here.
 *
 * PLATFORM-AGNOSTIC.
 */

import type { VisibilityWait, VisibilityState } from '../visibility/types.js';
import { validateTransactionVisibility, normalizeWait } from '../visibility/positions.js';
import { commitTransaction } from '../visibility/commit.js';
import { waitForVisibility } from '../visibility/wait.js';
import { executeWithReceipts, requestChangesSince, type AliasReceipt, type ChangesSinceResult } from '../receipts/index.js';
import { responseError } from '../visibility/errors.js';
import type { Platform } from '../support/platform.js';
import type { ConnectOptions as ScramConnectOptions, ConnectLocalOptions, ResumeOptions, ResumeLocalOptions } from '../types/connection.js';
import { DEFAULT_REQUEST_TIMEOUT_MS } from '../types/connection.js';
import type { BatchResponse, TransactionInfo, IsolationLevel } from '../types/write/batch.js';
import type { ReadQuery } from '../types/read/query.js';
import type {
  CreateCursorRequest,
  FetchNextRequest,
  CancelCursorRequest,
} from '../types/read/cursor.js';
import { ShamirTimeoutError } from '../support/errors.js';
import { CursorIterator } from './cursor-iterator.js';
import type { QueryBuildable } from './cursor-iterator.js';
import { WsFramer, encode, decode } from '../protocol/framing.js';
import { CURRENT_QUERY_LANG_VERSION } from '../protocol/scram.js';
import { signCanonical } from '../protocol/hmac.js';
import { Db } from './db.js';
import { SubscriptionRouter } from './subscription-router.js';
import type { PushEnvelope } from '../types/stream/subscribe.js';
import { InternerCacheRegistry } from '../support/field-map.js';
import { apiTokenAdmin, getDdlOpStatus } from './client-internals.js';
import { openScramSession, openLocalSession, resumeSession, resumeLocalSession } from '../protocol/session/index.js';
import type { SessionOpened, ChannelBinding } from '../protocol/session/index.js';
import type { ApiTokenAdmin } from '../types/access/token-admin.js';
import {
  createScramUser,
  setSuperuser,
  setReplicator,
  changePassword,
  resolvePrincipal,
  mergeInternerDelta,
  touchFields,
  executeWithTouch,
  type TxOpened,
  type ScramUserCreated,
  type SuperuserSet,
  type ReplicatorSet,
} from './client-internals.js';

export type { TxOpened, ScramUserCreated, SuperuserSet, ReplicatorSet } from './client-internals.js';

/** Pending request slot awaiting a server response. */
interface PendingRequest {
  resolve: (v: Record<string, unknown>) => void;
  reject: (e: Error) => void;
  /**
   * Per-request timeout handle (Finding 2.2). Cleared when the response
   * arrives / the request is rejected. `undefined` when timeouts are disabled
   * (`requestTimeoutMs === 0`).
   */
  timer?: ReturnType<typeof setTimeout>;
}

export class ShamirClient {
  private readonly platform: Platform;
  private readonly framer: WsFramer;
  private readonly _sessionId: Uint8Array;
  private readonly _serverPubKey: Uint8Array;
  private readonly _expiresAtNs: bigint;
  private readonly _resumptionTicket: Uint8Array | undefined;
  private readonly _resumptionExpiresAtNs: bigint | undefined;
  private readonly subscriptionRouter = new SubscriptionRouter();
  /** Per-`(db, repo)` field-name ↔ id cache (Stage 5-wire interner). */
  private readonly _internerCache = new InternerCacheRegistry();
  /**
   * Max query-language version advertised by the server in auth_ok / resume_ok.
   * 0 = pre-v2 server (no id-keyed write path). Set once at connect/resume.
   */
  private readonly _serverQueryVersion: number;
  /**
   * Per-request deadline in ms (Finding 2.2); `0` disables it. A pending
   * request with no response within this budget rejects with a
   * {@link ShamirTimeoutError}.
   */
  private readonly _requestTimeoutMs: number;
  /**
   * NFC-normalized username from the original SCRAM handshake, needed by
   * `changePassword` to build `auth_message_cp`. `undefined` on resumed
   * sessions (which lack the username).
   */
  private readonly _normalizedUser: string | undefined;
  /** Channel binding this session was opened with (S1); may be `undefined`. */
  private readonly _channelBinding: ChannelBinding | undefined;
  /** Pin of the server identity this session is bound to: SHA256(serverPubKey). */
  private readonly _trustedPin: Uint8Array;
  private nextRequestId = 1;

  /**
   * In-flight requests keyed by `rid`. Concurrent callers each get their own
   * slot; the readLoop resolves/rejects them in completion order (matching the
   * server's multiplexed response order — not send order).
   */
  private readonly pending = new Map<number, PendingRequest>();

  /**
   * Optional hook called for frames that carry neither a known `rid` nor a
   * fatal (rid-less) error. Designed as the entry point for future server-push
   * / subscription frames. By default such frames are silently dropped.
   */
  onUnroutedFrame?: (frame: Record<string, unknown>) => void;

  private constructor(
    platform: Platform,
    framer: WsFramer,
    sessionId: Uint8Array,
    serverPubKey: Uint8Array,
    expiresAtNs: bigint,
    resumptionTicket?: Uint8Array,
    resumptionExpiresAtNs?: bigint,
    serverQueryVersion?: number,
    requestTimeoutMs?: number,
    normalizedUser?: string,
    channelBinding?: ChannelBinding,
    trustedPin: Uint8Array = new Uint8Array(32),
  ) {
    this.platform = platform;
    this.framer = framer;
    this._sessionId = sessionId;
    this._serverPubKey = serverPubKey;
    this._expiresAtNs = expiresAtNs;
    this._resumptionTicket = resumptionTicket;
    this._resumptionExpiresAtNs = resumptionExpiresAtNs;
    this._serverQueryVersion = serverQueryVersion ?? 0;
    this._requestTimeoutMs = requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    this._normalizedUser = normalizedUser;
    this._channelBinding = channelBinding;
    this._trustedPin = trustedPin;

    // Start the persistent read loop immediately. The loop is the sole consumer
    // of framer.recv(); it routes each incoming frame to the matching pending
    // slot by rid.
    void this.readLoop();
  }

  /** Open a WS connection, run SCRAM, and return an authenticated client. */
  static async connect(platform: Platform, opts: ScramConnectOptions): Promise<ShamirClient> {
    return ShamirClient.fromOpened(platform, await openScramSession(platform, opts),
      opts.requestTimeoutMs);
  }

  /** Local IPC (Unix socket / Named Pipe, TRANSPORT_UNIX.md); Node-only, see `openLocalSession`. */
  static async connectLocal(platform: Platform, opts: ConnectLocalOptions): Promise<ShamirClient> {
    return ShamirClient.fromOpened(platform, await openLocalSession(platform, opts),
      opts.requestTimeoutMs);
  }

  /**
   * Fast reconnection with a resumption ticket instead of full SCRAM.
   * Throws (and closes the socket) if rejected or the response is malformed.
   */
  static async resume(platform: Platform, opts: ResumeOptions): Promise<ShamirClient> {
    return ShamirClient.fromOpened(platform, await resumeSession(platform, opts),
      opts.requestTimeoutMs);
  }

  /**
   * Fast reconnection over local IPC (Unix socket / Named Pipe) with a
   * resumption ticket. Node-only, see `resumeLocalSession`; throws (and
   * closes the socket) if rejected or the response is malformed.
   */
  static async resumeLocal(platform: Platform, opts: ResumeLocalOptions): Promise<ShamirClient> {
    return ShamirClient.fromOpened(platform, await resumeLocalSession(platform, opts),
      opts.requestTimeoutMs);
  }

  private static fromOpened(platform: Platform, o: SessionOpened, requestTimeoutMs: number | undefined): ShamirClient {
    return new ShamirClient(
      platform, o.framer, o.sessionId, o.serverPubKey, o.expiresAtNs,
      o.resumptionTicket, o.resumptionExpiresAtNs, o.serverQueryVersion,
      requestTimeoutMs, o.normalizedUser, o.binding, o.trustedPin,
    );
  }

  /** 32-byte session id assigned by the server. */
  sessionId(): Uint8Array {
    return this._sessionId;
  }

  /** Raw 32-byte Ed25519 server public key from `auth_ok`. */
  serverPubKeyPin(): Uint8Array {
    return this._serverPubKey;
  }

  /**
   * `SHA256(server_pub_key)` (32 bytes) — the pin of the server identity this
   * session is bound to. Verified against `ConnectOptions.trustedPin` when one
   * was supplied; on first contact (trust-on-first-use) this is the captured
   * pin. Persist it and pass it back as `trustedPin` on later connections
   * (recommended hardening). Companion to {@link serverPubKeyPin}, which
   * returns the RAW 32-byte public key (what `resume` needs).
   */
  trustedPin(): Uint8Array {
    return this._trustedPin;
  }

  /** Absolute session expiry (unix nanoseconds). */
  expiresAtNs(): bigint {
    return this._expiresAtNs;
  }

  /**
   * Resumption ticket issued by the server, if any. Present only when the
   * server returned one (either at `connect()` or `resume()` time).
   * Pass to {@link ShamirClient.resume} for fast reconnection.
   */
  resumptionTicket(): Uint8Array | undefined {
    return this._resumptionTicket;
  }

  /**
   * Absolute expiry of the resumption ticket (unix nanoseconds), or
   * `undefined` if no ticket was issued.
   */
  resumptionExpiresAtNs(): bigint | undefined {
    return this._resumptionExpiresAtNs;
  }

  /**
   * Hex HMAC-SHA256 tag over `canonical`, keyed by this session's derived
   * HMAC key (`SHA256("shamir-db hmac key v1\0" || session_id)`). The
   * "did-you-mean-it" intent tag the server requires on destructive
   * `drop_*` / migration ops. Pair with the `hmac.canonical*` builders.
   */
  hmacTagHex(canonical: Uint8Array): string {
    return signCanonical(this.platform, this._sessionId, canonical);
  }

  /**
   * Persistent read loop — the sole consumer of `framer.recv()`.
   *
   * Routing rules for each decoded envelope:
   *   • `error` field present AND `rid` present → reject that pending request.
   *   • `error` field present AND `rid` absent → fatal protocol error; reject
   *     all pending requests and stop the loop.
   *   • `res` bytes present AND `rid` present → resolve that pending request.
   *   • Neither routable → call `onUnroutedFrame` (server-push / subscription
   *     frames); default is silent drop.
   *
   * On socket close (`framer.recv()` rejects): every pending request is
   * rejected with the close error and the loop terminates.
   */
  private async readLoop(): Promise<void> {
    for (;;) {
      let frameBytes: Uint8Array;
      try {
        frameBytes = await this.framer.recv();
      } catch (err) {
        // Socket closed or errored — reject all pending and stop.
        const closeErr =
          err instanceof Error ? err : new Error(String(err));
        this.rejectAll(closeErr);
        return;
      }

      let frame: Record<string, unknown>;
      try {
        frame = decode(frameBytes) as Record<string, unknown>;
      } catch (err) {
        // Malformed frame — treat as fatal.
        const parseErr =
          err instanceof Error ? err : new Error(String(err));
        this.rejectAll(parseErr);
        return;
      }

      const rid = typeof frame.rid === 'number' ? frame.rid : undefined;
      const errorMsg =
        typeof frame.error === 'string' ? frame.error : undefined;

      if (errorMsg !== undefined) {
        if (rid !== undefined) {
          // Request-scoped protocol error — reject only this request.
          const waiter = this.takePending(rid);
          if (waiter) {
            waiter.reject(new Error(`protocol error: ${errorMsg}`));
          }
          // If rid is unknown, the server sent an unsolicited error frame —
          // fall through to onUnroutedFrame so it isn't silently lost.
          else {
            this.onUnroutedFrame?.(frame);
          }
        } else {
          // Fatal protocol error — no rid to route to; tear down everything.
          this.rejectAll(new Error(`protocol error: ${errorMsg}`));
          return;
        }
        continue;
      }

      if (rid !== undefined) {
        const waiter = this.takePending(rid);
        if (waiter) {
          if (frame.res instanceof Uint8Array) {
            let dbResponse: Record<string, unknown>;
            try {
              dbResponse = decode(frame.res) as Record<string, unknown>;
            } catch (err) {
              waiter.reject(
                err instanceof Error ? err : new Error(String(err)),
              );
              continue;
            }
            if (dbResponse.kind === 'error' || dbResponse.kind === 'as_of_visibility_timeout') {
              // Finding 2.1: surface a typed ShamirDbError carrying the
              // server's `code` + a `retryable` classification, instead of an
              // opaque interpolated-string Error the caller has to regex.
              waiter.reject(
                responseError(dbResponse),
              );
            } else {
              waiter.resolve(dbResponse);
            }
          } else {
            waiter.reject(
              new Error('response envelope missing `res` bytes'),
            );
          }
          continue;
        }
        // rid present but not in pending — unrouted (e.g. late/stale push).
      }

      // No rid, no error — server-push or subscription frame.
      if (typeof frame.push === 'string') {
        this.subscriptionRouter.route(frame as unknown as PushEnvelope);
        continue;
      }
      this.onUnroutedFrame?.(frame);
    }
  }

  /**
   * Remove and return the pending slot for `rid`, clearing its timeout timer
   * (Finding 2.2) so a settled request never fires a spurious timeout.
   */
  private takePending(rid: number): PendingRequest | undefined {
    const waiter = this.pending.get(rid);
    if (waiter) {
      this.pending.delete(rid);
      if (waiter.timer !== undefined) clearTimeout(waiter.timer);
    }
    return waiter;
  }

  /** Reject every pending request with `err`, clearing timers and the map. */
  private rejectAll(err: Error): void {
    for (const waiter of this.pending.values()) {
      if (waiter.timer !== undefined) clearTimeout(waiter.timer);
      waiter.reject(err);
    }
    this.pending.clear();
  }

  /**
   * Round-trip one `DbRequest` and return the decoded `DbResponse` object.
   *
   * Calls are CONCURRENT: each request is assigned a unique `rid` and
   * registered in the pending map before the frame is sent (eliminating the
   * race between a fast server response and registration). The persistent
   * `readLoop` demultiplexes responses by `rid`, so multiple overlapping
   * round-trips (`Promise.all([db.run(a), db.run(b)])`) proceed in parallel
   * and resolve in server-completion order — not send order.
   *
   * On send failure the pending slot is removed immediately and the returned
   * promise rejects with the send error.
   */
  private sendDbRequest(req: object): Promise<Record<string, unknown>> {
    const rid = this.nextRequestId++;

    return new Promise<Record<string, unknown>>((resolve, reject) => {
      // Register BEFORE sending to avoid missing a fast response.
      const slot: PendingRequest = { resolve, reject };
      this.pending.set(rid, slot);

      // Finding 2.2: arm a per-request deadline. Without this a Ping /
      // TxCommit / CreateScramUser (none bounded by the server's
      // `max_execution_time_secs`) or a lost response id would hang the caller
      // forever. On fire, reject with a typed, retryable timeout and drop the
      // slot; a late response then routes to nothing (harmless).
      if (this._requestTimeoutMs > 0) {
        slot.timer = setTimeout(() => {
          if (this.pending.get(rid) === slot) {
            this.pending.delete(rid);
            reject(new ShamirTimeoutError('request', this._requestTimeoutMs));
          }
        }, this._requestTimeoutMs);
        // Do not keep the Node event loop alive solely for this timer.
        (slot.timer as { unref?: () => void }).unref?.();
      }

      let envelope: Uint8Array;
      try {
        // Outer request envelope. `req` is opaque msgpack bytes (serde_bytes)
        // carrying the internally-tagged DbRequest (tag = "op").
        envelope = encode({ sid: this._sessionId, rid, req: encode(req) });
      } catch (err) {
        this.takePending(rid);
        reject(err instanceof Error ? err : new Error(String(err)));
        return;
      }

      try {
        this.framer.send(envelope);
      } catch (err) {
        // Socket already closed — remove the slot and reject immediately.
        this.takePending(rid);
        reject(err instanceof Error ? err : new Error(String(err)));
      }
    });
  }

  /**
   * Health-check ping — zero DB cost. Returns the decoded `DbResponse`
   * (`{ kind: "pong" }`). Uses `DbRequest::Ping`.
   */
  async ping(): Promise<object> {
    return this.sendDbRequest({ op: 'ping' });
  }

  /**
   * Execute a BatchRequest against `db`. Returns the unwrapped
   * {@link BatchResponse} (the server's `DbResponse::Batch.response`), so
   * callers read `.results` / `.execution_plan` / `.transaction` directly —
   * matching the napi binding's ergonomics. Throws on transport, protocol,
   * or DB-layer (`kind:"error"`) failures.
   *
   * Stage 5-wire interner integration (ambient sync):
   * - Attaches `interner_epochs` from the client's cached epochs for `db`,
   *   so the server knows which entries to include in its delta.
   * - After receiving the response, merges any `interner_delta` entries into
   *   the per-repo FieldMap caches.
   */
  async execute(db: string, batch: object, visibilityWait?: VisibilityWait): Promise<BatchResponse> {
    // Attach ambient interner epochs so the server returns only the delta.
    const epochs = this._internerCache.allEpochs(db);
    const enrichedBatch: Record<string, unknown> =
      typeof batch === 'object' && batch !== null
        ? { ...(batch as Record<string, unknown>) }
        : { ...(batch as object) };
    if (Object.keys(epochs).length > 0) {
      // Convert bigint epochs to numbers for msgpack serialisation. The
      // epoch values are gap-free high-water marks and will fit in a JS
      // safe integer for any realistic number of fields (2^53 fields would
      // require a universe-sized interner). The framing encoder
      // (@msgpack/msgpack) does NOT encode BigInt by default — it throws
      // "Unrecognized object: [object BigInt]" — so we narrow to Number
      // here; the server reads u64 and accepts the msgpack uint just fine.
      const wireEpochs: Record<string, number> = {};
      for (const [repo, e] of Object.entries(epochs)) {
        wireEpochs[repo] = Number(e);
      }
      // Merge with any epochs already present (e.g. from executeWithTouch
      // which may include epoch-0 cold repos for delta bootstrapping).
      const existing = enrichedBatch['interner_epochs'] as
        | Record<string, number>
        | undefined;
      enrichedBatch['interner_epochs'] = existing
        ? { ...existing, ...wireEpochs }
        : wireEpochs;
    }

    if (visibilityWait !== undefined) enrichedBatch.visibility_wait = normalizeWait(visibilityWait);
    const r = await this.sendDbRequest({
      op: 'execute',
      query_version: CURRENT_QUERY_LANG_VERSION,
      db,
      batch: enrichedBatch,
    });
    // DbResponse::Batch is `{ kind: "batch", response: BatchResponse }` —
    // unwrap the envelope so callers get the BatchResponse directly.
    if (r.kind === 'batch' && r.response !== undefined) {
      const response = r.response as BatchResponse;
      if (response.transaction != null) validateTransactionVisibility(response.transaction);
      // Merge any interner_delta from the response into the local cache.
      this.mergeInternerDelta(db, response);
      return response;
    }
    throw new Error(
      `unexpected DbResponse kind for execute: ${(r.kind as string) ?? 'missing'}`,
    );
  }

  /**
   * Merge `response.interner_delta` into the per-repo FieldMap caches for
   * `db`. Called after every successful `execute` response.
   *
   * Monotonic: the FieldMap's `applyDelta` rejects stale epochs, so
   * out-of-order or duplicate deltas are harmless.
   */
  private mergeInternerDelta(db: string, response: BatchResponse): void {
    mergeInternerDelta(this._internerCache, db, response);
  }

  /**
   * Register field `names` against the server's interner for `(db, repo)`
   * and merge the returned `(name, id)` mappings into the local cache.
   *
   * Returns the resolved `{ name → id }` map in input order. Idempotent:
   * the server returns existing ids for already-interned names.
   *
   * Short-circuits when every name is already cached (no roundtrip).
   */
  async touchFields(
    db: string,
    repo: string,
    names: string[],
  ): Promise<Map<string, bigint>> {
    return touchFields(this._internerCache, (execDb, execBatch) => this.execute(execDb, execBatch), db, repo, names);
  }

  /**
   * Access the interner cache registry. Allows callers to read cached
   * field name ↔ id mappings without a roundtrip (cache-hit path only).
   */
  get internerCache(): InternerCacheRegistry {
    return this._internerCache;
  }

  /**
   * Max query-language version advertised by the server. `0` means pre-v2.
   */
  serverQueryVersion(): number {
    return this._serverQueryVersion;
  }

  /**
   * Execute a batch with automatic client-side field interning (Stage 5-wire).
   *
   * 1. Collects field names from INSERT/SET/UPDATE ops, grouped by repo.
   * 2. Touches unknown field names per repo (populates the FieldMap cache).
   * 3. On v2+ servers: encodes fully-literal INSERT records into id-keyed
   *    msgpack (`records_idmsgpack`); removes them from `values`. Records
   *    with `$fn` markers stay on `values` (server-side eval). Sets
   *    `result_encoding = "id"` so the server returns id-keyed rows.
   * 4. Calls `execute()`.
   * 5. De-interns any id-keyed result rows back to name-keyed objects.
   *
   * Mirrors the Rust `Client::execute_with_touch`.
   */
  async executeWithTouch(db: string, batch: object): Promise<BatchResponse> {
    return executeWithTouch(
      this._internerCache,
      this._serverQueryVersion,
      (execDb, execBatch) => this.execute(execDb, execBatch),
      (touchDb, touchRepo, touchNames) => this.touchFields(touchDb, touchRepo, touchNames),
      db,
      batch,
    );
  }

  // ── Interactive (multi-call) transactions ─────────────────────────

  /**
   * Open an interactive transaction scoped to a single `repo`. Returns the
   * minted handle + the snapshot version it reads at. `isolation` is
   * `"snapshot"` (default) | `"serializable"`.
   */
  async txBegin(
    db: string,
    repo: string,
    isolation?: IsolationLevel,
  ): Promise<TxOpened> {
    const r = await this.sendDbRequest({
      op: 'tx_begin',
      query_version: CURRENT_QUERY_LANG_VERSION,
      db,
      repo,
      ...(isolation !== undefined ? { isolation } : {}),
    });
    if (r.kind !== 'tx_opened') {
      throw new Error(`unexpected DbResponse kind for tx_begin: ${r.kind}`);
    }
    return {
      tx_handle: r.tx_handle as number,
      snapshot_version: r.snapshot_version as number | bigint,
      isolation: r.isolation as string,
    };
  }

  /**
   * Run a batch inside an open interactive transaction. State accumulates in
   * the parked transaction (no commit). Returns the {@link BatchResponse}
   * (its `.transaction` stays null — there is no per-call commit outcome yet).
   */
  async txExecute(
    db: string,
    txHandle: number,
    batch: object,
  ): Promise<BatchResponse> {
    const r = await this.sendDbRequest({
      op: 'tx_execute',
      query_version: CURRENT_QUERY_LANG_VERSION,
      db,
      tx_handle: txHandle,
      batch,
    });
    if (r.kind === 'tx_batch' && r.response !== undefined) {
      return r.response as BatchResponse;
    }
    throw new Error(`unexpected DbResponse kind for tx_execute: ${r.kind}`);
  }

  /** Settlement observation; open a fresh snapshot afterwards. */
  waitForVisibility(db: string, repo: string, version: bigint, wait: VisibilityWait): Promise<VisibilityState> {
    return waitForVisibility((req) => this.sendDbRequest(req), db, repo, version, wait);
  }

  async txCommit(db: string, txHandle: number, visibilityWait?: VisibilityWait): Promise<TransactionInfo> {
    return commitTransaction((req) => this.sendDbRequest(req), db, txHandle, visibilityWait);
  }

  executeWithReceipts(db: string, batch: object, visibilityWait?: VisibilityWait): Promise<{ response: BatchResponse; receipts: AliasReceipt[] }> { return executeWithReceipts(this, db, batch, visibilityWait); }
  changesSince(db: string, repo: string, cursor: number | bigint, limit?: number): Promise<ChangesSinceResult> { return requestChangesSince(this, db, repo, cursor, limit); }

  /** Roll back (abort) an open interactive transaction. */
  async txRollback(db: string, txHandle: number): Promise<void> {
    const r = await this.sendDbRequest({
      op: 'tx_rollback',
      db,
      tx_handle: txHandle,
    });
    if (r.kind !== 'tx_rolled_back') {
      throw new Error(`unexpected DbResponse kind for tx_rollback: ${r.kind}`);
    }
  }

  // ── SCRAM user provisioning ───────────────────────────────────────

  /**
   * Create a SCRAM-authenticatable user (one that can log in over the wire).
   * Requires a superuser session AND an HMAC confirmation tag — the tag is
   * unconditional (every call signs it), mirroring `setSuperuser`'s gate
   * (task #604). The server runs Argon2id with its KDF defaults and writes
   * the durable user record. `roles: ["superuser"]` grants admin powers;
   * other strings are app-defined.
   */
  async createScramUser(
    name: string,
    password: string,
    roles: string[] = [],
  ): Promise<ScramUserCreated> {
    return createScramUser(name, password, roles, (req) => this.sendDbRequest(req), (canonical) => this.hmacTagHex(canonical));
  }

  /**
   * Grant or revoke superuser status on an existing SCRAM-directory user
   * (top-level `DbRequest::SetSuperuser`, NOT a `BatchOp`). Requires an
   * already-superuser session AND an HMAC confirmation tag — the tag is
   * unconditional (every call signs it via this session's HMAC key).
   * Mirrors `createScramUser`'s top-level shape; the signed wire op is
   * built by the `setSuperuser` admin builder.
   */
  async setSuperuser(user: string, on: boolean): Promise<SuperuserSet> {
    return setSuperuser(user, on, (req) => this.sendDbRequest(req), (canonical) => this.hmacTagHex(canonical));
  }

  /**
   * Grant or revoke replicator status on an existing SCRAM-directory user
   * (top-level `DbRequest::SetReplicator`, NOT a `BatchOp`, task #621).
   * Requires an already-superuser session AND an HMAC confirmation tag —
   * the tag is unconditional (every call signs it via this session's HMAC
   * key). Mirrors `setSuperuser`'s top-level shape and gate exactly; the
   * signed wire op is built by the `setReplicator` admin builder.
   */
  async setReplicator(user: string, on: boolean): Promise<ReplicatorSet> {
    return setReplicator(user, on, (req) => this.sendDbRequest(req), (canonical) => this.hmacTagHex(canonical));
  }

  /**
   * Change the password for the currently-authenticated user (spec §12.5).
   * Two-step flow over the active session: challenge (server issues fresh
   * nonce + echoes current salt/KDF), then verify (client submits SCRAM
   * proof-of-old-password + pre-derived new credentials). On success every
   * other live session for this user is killed server-side.
   *
   * NOT available on resumed sessions — the client needs the normalized
   * username from the original SCRAM handshake to build `auth_message_cp`.
   */
  async changePassword(
    oldPassword: string,
    newPassword: string,
  ): Promise<void> {
    return changePassword(oldPassword, newPassword, this.platform, this._sessionId, this._normalizedUser, this._channelBinding, (req) => this.sendDbRequest(req));
  }

  /**
   * Resolve a username to its real server-assigned `principal64` — a stable
   * 64-bit projection of the random 128-bit `user_id` the directory mints at
   * user creation (task #548). Backed by `ListOp::Users` against the durable
   * principal directory; this is a real network round-trip, NOT a
   * client-side hash.
   *
   * Required by callers of `chown` / `addGroupMember` / `removeGroupMember`,
   * whose wire parameter is a `u64` principal id (never a username string).
   * Throws `Error("no such user: <username>")` when the name is unknown.
   *
   * Uses the `"default"` db context like other Root-scoped admin convenience
   * calls on this client — `ListOp::Users` is authorised at
   * `ResourcePath::Root` (admin_list.rs:77-80) and is NOT database-scoped
   * server-side, so any db context the caller can traverse works; `"default"`
   * matches the established pattern in `createUser`/`grantRole` callers.
   */
  async resolvePrincipal(username: string): Promise<bigint> {
    return resolvePrincipal(username, (execDb, execBatch) => this.execute(execDb, execBatch));
  }

  /**
   * Open a server-side cursor over `query` on database `db` and return an
   * idiomatic {@link CursorIterator} — usable via
   * `for await (const record of client.streamCursor(...)) { ... }`.
   * `pageSize` bounds the first (and, by default, every subsequent) page.
   *
   * `sendDbRequest` is `private`, so `CursorIterator` (a separate module)
   * cannot call it directly — this method injects a bound closure instead
   * (dependency injection over visibility widening), the same pattern
   * `execute` follows for `sendDbRequest` itself.
   */
  streamCursor(
    db: string,
    query: ReadQuery | QueryBuildable,
    pageSize: number,
  ): CursorIterator {
    return new CursorIterator((req) => this.sendDbRequest(req), db, query, pageSize);
  }

  /**
   * Send a raw cursor-lifecycle request (`create_cursor` / `fetch_next` /
   * `cancel_cursor`, as built by `builders/cursor.ts`) and return the
   * decoded `DbResponse`. Deliberately narrow: unlike a general
   * `sendDbRequest` widening, this only accepts the cursor op shapes —
   * exists so tests can drive a follow-up request against a cursor id
   * obtained from a live {@link CursorIterator} (via its `cursorId`
   * property) to prove server-side state (e.g. that `cancel_cursor` / a
   * drained cursor actually removed the registry entry), mirroring FG-5c's
   * Rust test's use of the crate-internal `Client::roundtrip`.
   */
  async probeCursorOp(
    req: CreateCursorRequest | FetchNextRequest | CancelCursorRequest,
  ): Promise<Record<string, unknown>> {
    return this.sendDbRequest(req);
  }

  /**
   * Poll a DDL operation's status by its operation ID.
   *
   * Returns the full `DdlOpStatus` if found, `null` if the operation is unknown
   * (GC'd, never existed, or a pre-RFC op that had no op_id).
   *
   * This is the mechanism that allows a client to learn whether a crashed-and-recovered
   * DDL op actually completed after a server restart.
   *
   * @throws {ShamirDbError} If the server returns an error (including `not_supported`
   *   for old servers that don't understand this request).
   */
  async getDdlOpStatus(
    db: string,
    repo: string,
    table: string,
    opId: string
  ): Promise<import('../types/write/batch.js').DdlOpStatus | null> {
    return getDdlOpStatus(db, repo, table, opId, (req) => this.sendDbRequest(req));
  }

  /** API-token administration for this SCRAM session (TOKEN_API §12). */
  get apiTokens(): ApiTokenAdmin {
    return apiTokenAdmin((req) => this.sendDbRequest(req), (canonical) => this.hmacTagHex(canonical));
  }

  /**
   * Return a bound {@link Db} handle for `name`. Subsequent calls via
   * the handle (`db.run(...)`, `db.query(...)`, `db.batch(...)`, etc.)
   * automatically thread the client and database name — no re-threading.
   */
  db(name: string): Db {
    return new Db(this, name);
  }

  /** Access the subscription router for registering/unregistering push handlers. */
  get subscriptions(): SubscriptionRouter {
    return this.subscriptionRouter;
  }

  /** Close the WS (normal closure). Idempotent. */
  async close(): Promise<void> {
    await this.framer.close();
  }
}

