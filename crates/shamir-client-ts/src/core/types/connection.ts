import type { ApiToken } from '../../entry/api-token.js';

/** Internal transport contracts; token is always an opaque validated handle. */
export interface TokenHttpConnectOptions {
  auth: 'token'; channel: 'http'; url: string; token: ApiToken;
  requestTimeoutMs?: number; connectTimeoutMs?: number; signal?: AbortSignal;
}
export type NodeTokenHttpConnectOptions = TokenHttpConnectOptions & { tls?: TokenTlsOptions; origin?: string };
export type BrowserTokenHttpConnectOptions = TokenHttpConnectOptions & { tls?: never; origin?: never };
export interface NodeTokenWsConnectOptions extends Omit<TokenHttpConnectOptions, 'channel'> { channel: 'ws'; tls?: TokenTlsOptions; origin?: string; }
export type BrowserTokenWsConnectOptions = Omit<NodeTokenWsConnectOptions, 'tls' | 'origin'> & { tls?: never; origin?: never };
export type TokenWsConnectOptions = NodeTokenWsConnectOptions | BrowserTokenWsConnectOptions;

export type TokenConnectOptions = {
  auth: 'token'; channel?: 'http' | 'ws'; url: string; token: ApiToken | string;
  requestTimeoutMs?: number; connectTimeoutMs?: number; signal?: AbortSignal;
};
export type NodeTokenConnectOptions = TokenConnectOptions & { tls?: TokenTlsOptions; origin?: string; username?: never; password?: never; host?: never; port?: never };
export type BrowserTokenConnectOptions = TokenConnectOptions & { tls?: never; origin?: never; username?: never; password?: never; host?: never; port?: never };
export type NodeConnectOptions = ScramConnectOptions | NodeTokenConnectOptions;
export type BrowserConnectOptions = ScramConnectOptions | BrowserTokenConnectOptions;

export interface TokenTlsOptions {
  ca?: string | Uint8Array;
  allowInsecureTlsForDevelopment?: true;
}


/**
 * Connection wire types.
 *
 * Pure type declarations; the connecting code lives in `../client.ts` and
 * the platform entry points (`../../index.ts`, `../../browser.ts`).
 *
 * PLATFORM-AGNOSTIC.
 */

/**
 * Session transport. Default 'ws-browser' = browser-profile WebSocket
 * `/shamir/v1/browser`, unchanged; 'tcp' = TCP+TLS1.3 listener
 * `tcp/tls_exporter`; 'ws-native' = `wss://host:port/shamir/v1`, listener
 * `ws/tls_exporter`, no Origin. 'tcp' and 'ws-native' are Node only and use
 * the real TLS exporter; `origin` is ignored for them.
 */
export type SessionTransport = 'ws-browser' | 'tcp' | 'ws-native';

/**
 * Options for {@link ShamirClient.resume} — fast reconnection using a
 * resumption ticket obtained from a previous session.
 */
export interface ResumeOptions {
  host: string;
  port: number;
  /** Resumption ticket bytes obtained from a previous session. */
  ticket: Uint8Array;
  /** Server public key pinned from the first connection. */
  serverPubKey: Uint8Array;
  /** Transport; see {@link SessionTransport}. */
  transport?: SessionTransport;
  /**
   * TLS options. `ca`: PEM CA certificate(s) trusted for the server
   * certificate, used by 'tcp' and 'ws-native' only (setting it with the
   * browser-profile transport throws). Trust = `rejectUnauthorized`
   * (default true) plus `ca`; no `identity_sig` pin check in TS (task #622).
   */
  tls?: { rejectUnauthorized?: boolean; ca?: string | Uint8Array };
  /**
   * Origin header sent on the WS upgrade. Defaults to `https://${host}`.
   */
  origin?: string;
  /**
   * Per-request deadline in ms (Finding 2.2). A pending request that gets no
   * server response within this budget rejects with a {@link ShamirTimeoutError}
   * instead of hanging forever. Defaults to {@link DEFAULT_REQUEST_TIMEOUT_MS}.
   * Pass `0` to disable.
   */
  requestTimeoutMs?: number;
  /**
   * Deadline in ms for the initial connection attempt (Finding 2.2). Defaults
   * to {@link DEFAULT_CONNECT_TIMEOUT_MS}. Pass `0` to disable.
   */
  connectTimeoutMs?: number;
}

/**
 * Connection parameters (mirrors the napi `ConnectOptions`).
 *
 * Unlike the napi/Node client, this TS client does NOT verify the server's
 * `identity_sig` / do TOFU pinning of its Ed25519 identity (task #622) — a
 * prior `acceptNewHost`/`trustedPin` pair was declared but never consulted
 * anywhere, so it was removed rather than left dead. Real pinning needs a
 * browser storage backend and an `acceptNewHost` UX design pass; until
 * then this client is not MITM-resistant on the initial connection.
 */
export interface ConnectOptions {
  host: string;
  port: number;
  username: string;
  password: string;
  /** Transport; see {@link SessionTransport}. */
  transport?: SessionTransport;
  /**
   * TLS options. `ca`: PEM CA certificate(s) trusted for the server
   * certificate, used by 'tcp' and 'ws-native' only (setting it with the
   * browser-profile transport throws). Trust = `rejectUnauthorized`
   * (default true) plus `ca`; no `identity_sig` pin check in TS (task #622).
   */
  tls?: { rejectUnauthorized?: boolean; ca?: string | Uint8Array };
  /**
   * Origin header sent on the WS upgrade. The server's browser endpoint
   * REQUIRES an Origin in its allowlist (browser.rs §9). Defaults to
   * `https://${host}`.
   */
  origin?: string;
  /**
   * Per-request deadline in ms (Finding 2.2). A pending request that gets no
   * server response within this budget rejects with a {@link ShamirTimeoutError}
   * instead of hanging forever. Defaults to {@link DEFAULT_REQUEST_TIMEOUT_MS}.
   * Pass `0` to disable.
   */
  requestTimeoutMs?: number;
  /**
   * Deadline in ms for the initial connection attempt (Finding 2.2). Defaults
   * to {@link DEFAULT_CONNECT_TIMEOUT_MS}. Pass `0` to disable.
   */
  connectTimeoutMs?: number;
}
export type ScramConnectOptions = ConnectOptions;

/**
 * Connection parameters for {@link ShamirClient.connectLocal} — local IPC
 * transport (Unix domain socket / Windows Named Pipe), spec
 * TRANSPORT_UNIX.md. No TLS, no `host`/`port`/`tls`/`origin` — the OS-level
 * access boundary (file permissions / DACL, enforced server-side) replaces
 * transport encryption. Node-only: there is no browser equivalent (a
 * browser sandbox has no filesystem/pipe access).
 */
export interface ConnectLocalOptions {
  /**
   * Raw platform endpoint: an absolute Unix socket path or a `\\.\pipe\name`
   * Windows pipe. Validity is platform-specific; no logical-name resolution
   * or prefixing occurs.
   */
  addr: string;
  username: string;
  password: string;
  /**
   * Per-request deadline in ms — see {@link ConnectOptions.requestTimeoutMs}.
   */
  requestTimeoutMs?: number;
  /**
   * Deadline in ms for the initial connection attempt — see
   * {@link ConnectOptions.connectTimeoutMs}.
   */
  connectTimeoutMs?: number;
}

/**
 * Default per-request deadline. Chosen comfortably above the server's default
 * `max_execution_time_secs` (30 s) so a legitimately slow `Execute` is bounded
 * by the SERVER, and this client timer only fires for genuinely stuck ops
 * (`Ping`, `TxCommit`, a lost response id) — see Finding 2.2.
 */
export const DEFAULT_REQUEST_TIMEOUT_MS = 35_000;

/** Default deadline for the initial connection attempt. */
export const DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
