import type { IncomingMessage } from 'node:http';
import type { TLSSocket } from 'node:tls';
import WebSocket from 'ws';
import { TRANSPORT_KIND_WS } from '../../core/protocol/scram.js';
import { tlsExporterBinding } from '../../core/protocol/session/binding.js';
import type { SessionLink, WsNativeSessionEndpoint } from '../../core/protocol/session/endpoint.js';
import { ShamirTimeoutError } from '../../core/support/errors.js';
import { NodeSocket } from '../node.js';
import { exportTlsBinding } from './tls-exporter.js';
import { nativeTlsOptions } from './tls-options.js';

const MAX_PAYLOAD = 16 * 1024 * 1024 + 4;

export function dialWsNative(endpoint: WsNativeSessionEndpoint, connectTimeoutMs: number): Promise<SessionLink> {
  let protocol: string;
  try {
    protocol = new URL(endpoint.url).protocol;
  } catch {
    return Promise.reject(new Error('invalid WebSocket URL'));
  }
  if (protocol !== 'wss:') {
    return Promise.reject(new Error('native WebSocket session requires a wss:// URL (TLS exporter)'));
  }

  return new Promise<SessionLink>((resolve, reject) => {
    const ws = new WebSocket(endpoint.url, ['shamir-v1'], {
      ...nativeTlsOptions(endpoint.tls),
      perMessageDeflate: false,
      maxPayload: MAX_PAYLOAD,
    });
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let tlsSocket: TLSSocket | undefined;

    const fail = (err: Error): void => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      try {
        ws.terminate();
      } catch {
        // already closed
      }
      reject(err);
    };
    const onEarlyClose = (): void => {
      fail(new Error('connection closed before the WebSocket handshake completed'));
    };

    ws.once('upgrade', (res: IncomingMessage) => {
      tlsSocket = res.socket as TLSSocket;
    });
    ws.on('error', fail);
    ws.once('close', onEarlyClose);
    ws.once('open', () => {
      if (settled) return;
      if (ws.protocol !== 'shamir-v1') {
        fail(new Error('server did not negotiate the shamir-v1 subprotocol'));
        return;
      }
      if (tlsSocket === undefined || typeof tlsSocket.exportKeyingMaterial !== 'function') {
        fail(new Error('TLS exporter unavailable'));
        return;
      }
      let exporter: Uint8Array;
      try {
        exporter = exportTlsBinding(tlsSocket);
      } catch (err) {
        fail(err instanceof Error ? err : new Error('TLS exporter unavailable'));
        return;
      }
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      ws.removeListener('close', onEarlyClose);
      resolve({
        socket: new NodeSocket(ws),
        binding: tlsExporterBinding(TRANSPORT_KIND_WS, exporter),
      });
    });

    if (connectTimeoutMs > 0) {
      timer = setTimeout(() => fail(new ShamirTimeoutError('connect', connectTimeoutMs)), connectTimeoutMs);
    }
  });
}
