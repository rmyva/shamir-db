/**
 * Session open: dial + SCRAM handshake, returning plain data
 * ({@link SessionOpened}) — never a ShamirClient.
 *
 * PLATFORM-AGNOSTIC.
 */

import type { Platform } from '../../support/platform.js';
import { WsFramer } from '../framing.js';
import { runHandshake } from '../protocol.js';
import { withConnectTimeout } from '../../client/client-internals.js';
import { DEFAULT_CONNECT_TIMEOUT_MS } from '../../types/connection.js';
import type {
  ConnectOptions as ScramConnectOptions,
  ConnectLocalOptions,
  SessionTransport,
} from '../../types/connection.js';
import { browserWsBinding, ipcBinding } from './binding.js';
import type {
  SessionEndpoint,
  SessionLink,
  SessionTlsOptions,
} from './endpoint.js';
import type { SessionOpened } from './opened.js';

/**
 * Run the SCRAM handshake over an already-open link. Closes the framer and
 * rethrows on any failure.
 */
export async function openSession(
  platform: Platform,
  link: SessionLink,
  username: string,
  password: string,
): Promise<SessionOpened> {
  const framer = new WsFramer(link.socket);

  try {
    const {
      sessionId,
      serverPubKey,
      expiresAtNs,
      resumptionTicket,
      resumptionExpiresAtNs,
      serverQueryVersion,
    } = await runHandshake(platform, framer, username, password, link.binding);
    return {
      framer,
      sessionId,
      serverPubKey,
      expiresAtNs,
      resumptionTicket,
      resumptionExpiresAtNs,
      serverQueryVersion,
      normalizedUser: username.normalize('NFC'),
      binding: link.binding,
    };
  } catch (e) {
    await framer.close();
    throw e;
  }
}

export interface EndpointOptions {
  host: string;
  port: number;
  transport?: SessionTransport;
  tls?: { rejectUnauthorized?: boolean; ca?: string | Uint8Array };
  origin?: string;
}

function hostForUrl(host: string): string {
  return host.includes(':') && !host.startsWith('[') ? `[${host}]` : host;
}

/** Map connect/resume options to a transport endpoint. */
export function endpointFromOptions(opts: EndpointOptions): SessionEndpoint {
  const transport = opts.transport;
  if (transport === undefined || transport === 'ws-browser') {
    if (opts.tls?.ca !== undefined) {
      throw new Error("tls.ca is only supported with transport 'tcp' or 'ws-native'");
    }
    return {
      kind: 'ws-browser',
      url: `wss://${opts.host}:${opts.port}/shamir/v1/browser`,
      origin: opts.origin ?? `https://${opts.host}`,
      tls: { rejectUnauthorized: opts.tls?.rejectUnauthorized ?? true },
    };
  }
  if (transport === 'tcp' || transport === 'ws-native') {
    const tls: SessionTlsOptions = {
      rejectUnauthorized: opts.tls?.rejectUnauthorized ?? true,
      ...(opts.tls?.ca === undefined ? {} : { ca: opts.tls.ca }),
    };
    if (transport === 'tcp') {
      return { kind: 'tcp', host: opts.host, port: opts.port, tls };
    }
    return {
      kind: 'ws-native',
      url: `wss://${hostForUrl(opts.host)}:${opts.port}/shamir/v1`,
      tls,
    };
  }
  throw new Error(`unknown transport: ${String(transport)}`);
}

/** Open the transport link for `endpoint` (no handshake). */
export async function dialEndpoint(
  platform: Platform,
  endpoint: SessionEndpoint,
  connectTimeoutMs: number,
): Promise<SessionLink> {
  switch (endpoint.kind) {
    case 'ws-browser': {
      const socket = await withConnectTimeout(
        platform.openSocket(endpoint.url, {
          rejectUnauthorized: endpoint.tls?.rejectUnauthorized ?? true,
          origin: endpoint.origin,
        }),
        connectTimeoutMs,
      );
      return { socket, binding: browserWsBinding() };
    }
    case 'tcp':
    case 'ws-native':
      if (!platform.openNativeLink) {
        throw new Error(
          `transport '${endpoint.kind === 'tcp' ? 'tcp' : 'ws-native'}': this Platform does not implement openNativeLink (TCP and native WebSocket with the TLS exporter are Node only)`,
        );
      }
      return platform.openNativeLink(endpoint, { connectTimeoutMs });
    case 'ipc':
      throw new Error('dialEndpoint: ipc endpoints are opened by openLocalSession');
  }
}

/**
 * Dial the selected transport and run the SCRAM handshake. Platform provides
 * crypto + socket.
 */
export async function openScramSession(
  platform: Platform,
  opts: ScramConnectOptions,
): Promise<SessionOpened> {
  const link = await dialEndpoint(
    platform,
    endpointFromOptions(opts),
    opts.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
  );
  return openSession(platform, link, opts.username, opts.password);
}

/**
 * Open a local-IPC connection (Unix domain socket / Windows Named Pipe,
 * spec TRANSPORT_UNIX.md) and run the SCRAM handshake. Mirrors
 * {@link openScramSession} exactly except: no TLS setup, and the channel
 * binding is `ipcBinding()` (`TRANSPORT_KIND_UNIX`/`BINDING_MODE_NONE`)
 * instead of the WS/browser profile.
 *
 * Node-only — `platform.openIpcSocket` is optional on {@link Platform}
 * precisely because there is no browser implementation; this throws
 * immediately (before touching the network) if the platform lacks it.
 */
export async function openLocalSession(
  platform: Platform,
  opts: ConnectLocalOptions,
): Promise<SessionOpened> {
  if (!platform.openIpcSocket) {
    throw new Error(
      'connectLocal: this Platform does not implement openIpcSocket ' +
        '(local IPC has no browser equivalent — Node only)',
    );
  }
  const socket = await withConnectTimeout(
    platform.openIpcSocket(opts.addr),
    opts.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
  );
  return openSession(
    platform,
    { socket, binding: ipcBinding() },
    opts.username,
    opts.password,
  );
}
