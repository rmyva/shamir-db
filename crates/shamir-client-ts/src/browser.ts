import { loginBrowser } from './login/browser/browser.js';
import type { LoginOptions, LoginResult } from './login/types.js';
export function login(options: LoginOptions): Promise<LoginResult> {
  return loginBrowser(options);
}
export { LoginError } from './login/errors.js';
export type { LoginErrorCode } from './login/errors.js';
export type { LoginOptions, LoginResult, LoginMetadata, LoginScope } from './login/types.js';

import { ShamirClient } from './core/client/client.js';
import { BrowserPlatform } from './platform/browser.js';
import type { ConnectOptions, BrowserConnectOptions, BrowserTokenConnectOptions } from './core/types/connection.js';
import { connectTokenBrowser } from './platform/connect/browser-connect.js';
import type { TokenHttpClient } from './entry/token-client.js';
import type { TokenWsClient, PublicTokenClient } from './entry/ws/token-client.js';
import { TokenTransportError } from './core/protocol/token/errors.js';

/**
 * Open an authenticated ShamirDB connection from a browser.
 * Uses native WebSocket and WebCrypto / argon2-browser.
 */
export function connect(opts: ConnectOptions): Promise<ShamirClient>;
export function connect(opts: BrowserTokenConnectOptions & ({ channel: 'http' } | { url: `http://${string}` | `https://${string}` })): Promise<TokenHttpClient>;
export function connect(opts: BrowserTokenConnectOptions & ({ channel: 'ws' } | { url: `ws://${string}` | `wss://${string}` })): Promise<TokenWsClient>;
export function connect(opts: BrowserTokenConnectOptions): Promise<PublicTokenClient>;
export function connect(opts: BrowserConnectOptions): Promise<ShamirClient | PublicTokenClient>;
export async function connect(opts: BrowserConnectOptions): Promise<ShamirClient | PublicTokenClient> {
  if (!opts || typeof opts !== 'object') throw new TokenTransportError('invalid_request', 'Invalid connection options');
  if ('auth' in opts) {
    if (opts.auth !== 'token' || !('token' in opts) || !('url' in opts)) throw new TokenTransportError('invalid_request', 'Invalid token connection options');
    return connectTokenBrowser(opts);
  }
  if ('token' in opts) throw new TokenTransportError('invalid_request', 'Token authentication must be explicit');
  return ShamirClient.connect(BrowserPlatform, opts);
}

export { ApiToken } from './entry/api-token.js';
export { TokenTransportError } from './core/protocol/token/errors.js';
export type { TokenTransportErrorCode, TokenTransportErrorOptions } from './core/protocol/token/errors.js';
export type { TokenConnectOptions, BrowserTokenConnectOptions, BrowserConnectOptions, BrowserTokenHttpConnectOptions, BrowserTokenWsConnectOptions } from './core/types/connection.js';
export type { PublicTokenClient, TokenWsClient } from './entry/ws/token-client.js';
export type { TokenHttpClient } from './entry/token-client.js';
export type { TokenCursor } from './entry/ws/token-cursor.js';
export type { TokenHttpDb, TokenLiveResult, TokenLiveSubscription, TokenTransactionContext } from './entry/token-db.js';
export type { SubscriptionEvent } from './core/client/subscription-router.js';
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
  isRetryableCode,
  isVersionConflict,
  RETRYABLE_ERROR_CODES,
  TOKEN_ADMIN_ERROR_CODES,
  isTokenAdminError,
} from './core/support/errors.js';
export type { TokenAdminErrorCode } from './core/support/errors.js';
export type { TxOpened, ScramUserCreated } from './core/client/client.js';
export type { Db, Tx } from './core/client/db.js';
export * from './core/builders/index.js';
export type * from './core/types/index.js';
