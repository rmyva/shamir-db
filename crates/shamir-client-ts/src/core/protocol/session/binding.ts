/**
 * Channel-binding description for the SCRAM handshake session layer.
 *
 * PLATFORM-AGNOSTIC.
 */

import {
  TRANSPORT_KIND_WS,
  TRANSPORT_KIND_UNIX,
  BINDING_MODE_TLS_NO_EXPORT,
  BINDING_MODE_TLS_EXPORTER,
  BINDING_MODE_NONE,
  TRANSPORT_KIND_TCP,
} from '../scram.js';

/** Channel-binding description handed to the handshake step (design 2.2). */
export interface ChannelBinding {
  transportKind: 0x01 | 0x02 | 0x03;
  bindingMode: 0x00 | 0x01 | 0x02;
  /** 32-byte TLS exporter, or 32 zero bytes when the transport has none. */
  exporter: Uint8Array;
}

function zeroExporter(): Uint8Array {
  return new Uint8Array(32);
}

/**
 * Binding for the browser WebSocket profile (`wss://.../shamir/v1/browser`,
 * TLS terminated upstream without exporter — `BINDING_MODE_TLS_NO_EXPORT`).
 * Matches the wire today. Returns a FRESH object with a fresh exporter array
 * on every call (never share the mutable `Uint8Array`). The exporter is the
 * 32 zero bytes the `auth_message` carries for this profile; it is also the
 * `channel_binding_at_auth` field of `auth_message_cp` (password change).
 */
export function browserWsBinding(): ChannelBinding {
  return {
    transportKind: TRANSPORT_KIND_WS,
    bindingMode: BINDING_MODE_TLS_NO_EXPORT,
    exporter: zeroExporter(),
  };
}

/**
 * Binding for the local-IPC profile (Unix domain socket / Windows Named Pipe,
 * plain transport, no TLS — `BINDING_MODE_NONE`). Matches the wire today.
 * Returns a FRESH object with a fresh exporter array on every call. The
 * exporter is the 32 zero bytes the `auth_message` carries for this profile;
 * it is also the `channel_binding_at_auth` field of `auth_message_cp`
 * (password change).
 */
export function ipcBinding(): ChannelBinding {
  return {
    transportKind: TRANSPORT_KIND_UNIX,
    bindingMode: BINDING_MODE_NONE,
    exporter: zeroExporter(),
  };
}

/**
 * Binding for the plain-TCP profile (`kind: tcp, profile: plain`, loopback
 * only — no TLS, `BINDING_MODE_NONE`). Matches the Rust client's
 * `connect_plain`. Returns a FRESH object with a fresh exporter array on
 * every call (never share the mutable `Uint8Array`). The exporter is the 32
 * zero bytes the `auth_message` carries for this profile; it is also the
 * `channel_binding_at_auth` field of `auth_message_cp` (password change).
 */
export function tcpPlainBinding(): ChannelBinding {
  return {
    transportKind: TRANSPORT_KIND_TCP,
    bindingMode: BINDING_MODE_NONE,
    exporter: zeroExporter(),
  };
}

/**
 * Binding for the `tls_exporter` listener profile: TCP+TLS1.3
 * (`TRANSPORT_KIND_TCP`, 0x01) or native WSS (`TRANSPORT_KIND_WS`, 0x02)
 * with the real 32-byte TLS exporter. Returns a COPY of `exporter`.
 */
export function tlsExporterBinding(
  transportKind: 0x01 | 0x02,
  exporter: Uint8Array,
): ChannelBinding {
  if (exporter.length !== 32) {
    throw new RangeError('TLS exporter must be 32 bytes');
  }
  return {
    transportKind,
    bindingMode: BINDING_MODE_TLS_EXPORTER,
    exporter: new Uint8Array(exporter),
  };
}
