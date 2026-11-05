import { ShamirClient } from './core/client/client.js';
import { loginNode } from './login/node/node.js';
import type { LoginOptions, LoginResult } from './login/types.js';
export function login(options: LoginOptions): Promise<LoginResult> {
  return loginNode(options);
}
export { LoginError } from './login/errors.js';
export type { LoginErrorCode } from './login/errors.js';
export type { LoginOptions, LoginResult, LoginMetadata, LoginScope } from './login/types.js';

import { NodePlatform } from './platform/node.js';
import { NodeSessionPlatform } from './platform/session/native-platform.js';
import type { ConnectOptions, ConnectLocalOptions, ResumeOptions, ResumeLocalOptions, NodeConnectOptions, NodeTokenConnectOptions } from './core/types/connection.js';
import { connectTokenNode } from './platform/connect/node-connect.js';
import type { TokenHttpClient } from './entry/token-client.js';
import type { TokenWsClient, PublicTokenClient } from './entry/ws/token-client.js';
import { TokenTransportError } from './core/protocol/token/errors.js';

/**
 * Open an authenticated ShamirDB connection from Node.js.
 * Uses `ws` for the socket and `node:crypto` for HMAC/SHA256.
 */
export function connect(opts: ConnectOptions): Promise<ShamirClient>;
export function connect(opts: NodeTokenConnectOptions & ({ channel: 'http' } | { url: `http://${string}` | `https://${string}` })): Promise<TokenHttpClient>;
export function connect(opts: NodeTokenConnectOptions & ({ channel: 'ws' } | { url: `ws://${string}` | `wss://${string}` })): Promise<TokenWsClient>;
export function connect(opts: NodeTokenConnectOptions): Promise<PublicTokenClient>;
export function connect(opts: NodeConnectOptions): Promise<ShamirClient | PublicTokenClient>;
export async function connect(opts: NodeConnectOptions): Promise<ShamirClient | PublicTokenClient> {
  if (!opts || typeof opts !== 'object') throw new TokenTransportError('invalid_request', 'Invalid connection options');
  if ('auth' in opts) {
    if (opts.auth !== 'token' || !('token' in opts) || !('url' in opts)) throw new TokenTransportError('invalid_request', 'Invalid token connection options');
    return connectTokenNode(opts);
  }
  if ('token' in opts) throw new TokenTransportError('invalid_request', 'Token authentication must be explicit');
  return ShamirClient.connect(NodeSessionPlatform, opts);
}

/**
 * Open an authenticated ShamirDB connection over local IPC (Unix domain
 * socket / Windows Named Pipe, spec TRANSPORT_UNIX.md) from Node.js. No
 * TLS — the OS-level access boundary (file permissions / DACL, enforced
 * server-side) stands in for transport encryption. Node-only: NOT exported
 * from `browser.ts` — a browser sandbox has no filesystem/pipe access.
 */
export async function connectLocal(opts: ConnectLocalOptions): Promise<ShamirClient> {
  return ShamirClient.connectLocal(NodePlatform, opts);
}

/**
 * Fast reconnection from Node.js using a resumption ticket from a previous
 * session. Skips the full SCRAM handshake.
 */
export async function resume(opts: ResumeOptions): Promise<ShamirClient> {
  return ShamirClient.resume(NodeSessionPlatform, opts);
}

/**
 * Fast reconnection over local IPC (Unix domain socket / Windows Named Pipe)
 * from Node.js using a resumption ticket from a previous session. Skips the
 * full SCRAM handshake; no server identity proof is exchanged (binding 0x00).
 * Node-only: NOT exported from `browser.ts`.
 */
export async function resumeLocal(opts: ResumeLocalOptions): Promise<ShamirClient> {
  return ShamirClient.resumeLocal(NodePlatform, opts);
}

export { ApiToken } from './entry/api-token.js';
export { TokenTransportError } from './core/protocol/token/errors.js';
export type { TokenTransportErrorCode, TokenTransportErrorOptions } from './core/protocol/token/errors.js';
export type { TokenConnectOptions, NodeTokenConnectOptions, NodeConnectOptions, NodeTokenHttpConnectOptions, NodeTokenWsConnectOptions, TokenTlsOptions } from './core/types/connection.js';
export type { TokenWsClient, PublicTokenClient } from './entry/ws/token-client.js';
export type { TokenHttpClient } from './entry/token-client.js';
export type { TokenCursor } from './entry/ws/token-cursor.js';
export type { TokenHttpDb, TokenLiveResult, TokenLiveSubscription, TokenTransactionContext } from './entry/token-db.js';
export type { TokenWsDb } from './entry/ws/token-client.js';
export { VisibilityError } from './core/visibility/errors.js';
export type { TransactionResult, TransactionOptions } from './core/visibility/types.js';
export {
  extractReceipts,
  reconcileReceipts,
  executeWithReceipts,
  decodeChangesSince,
} from './core/receipts/index.js';
export type {
  AliasReceipt,
  ChangesSinceEvent,
  ChangesSinceRecordChange,
  ChangesSinceResult,
  RepoObservation,
} from './core/receipts/index.js';
export { ShamirClient };
export {
  ShamirDbError,
  ShamirTimeoutError,
  ServerIdentityChangedError,
  isRetryableCode,
  isVersionConflict,
  RETRYABLE_ERROR_CODES,
  TOKEN_ADMIN_ERROR_CODES,
  isTokenAdminError,
} from './core/support/errors.js';
export type { TokenAdminErrorCode } from './core/support/errors.js';
export type {
  TxOpened,
  ScramUserCreated,
  SuperuserSet,
  ReplicatorSet,
} from './core/client/client.js';
export { SubscriptionRouter } from './core/client/subscription-router.js';
export type { SubscriptionEvent } from './core/client/subscription-router.js';
export { SubscriptionHandle } from './core/client/subscription-handle.js';
export { CursorIterator } from './core/client/cursor-iterator.js';
export type { ResumeOptions, ResumeLocalOptions, ConnectLocalOptions };
export type { Db, Tx } from './core/client/db.js';
export * from './core/builders/index.js';
export type * from './core/types/index.js';
