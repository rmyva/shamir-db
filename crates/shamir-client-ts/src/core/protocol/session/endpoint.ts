/**
 * Transport-neutral endpoint/link descriptions for the session layer.
 * Types only. `ws-native` and `tcp` are Node-only endpoints; their dialers
 * live in `platform/session` (Node-only).
 *
 * PLATFORM-AGNOSTIC.
 */

import type { ChannelBinding } from './binding.js';
import type { Socket } from '../../support/platform.js';

/**
 * TLS trust options. Trust is `rejectUnauthorized` (default true) plus `ca`
 * (PEM CA certificate(s) trusted for the server certificate). There is no
 * `identity_sig` pin check in TS (task #622).
 */
export interface SessionTlsOptions {
  rejectUnauthorized?: boolean;
  ca?: string | Uint8Array;
}

export type SessionEndpoint =
  | { kind: 'ws-browser'; url: string; origin: string; tls?: SessionTlsOptions }
  | { kind: 'ws-native'; url: string; tls?: SessionTlsOptions }
  | { kind: 'tcp'; host: string; port: number; tls?: SessionTlsOptions }
  | { kind: 'ipc'; addr: string };

export type TcpSessionEndpoint = Extract<SessionEndpoint, { kind: 'tcp' }>;
export type WsNativeSessionEndpoint = Extract<SessionEndpoint, { kind: 'ws-native' }>;
export type NativeSessionEndpoint = TcpSessionEndpoint | WsNativeSessionEndpoint;

export interface SessionLink {
  socket: Socket;
  binding: ChannelBinding;
}
