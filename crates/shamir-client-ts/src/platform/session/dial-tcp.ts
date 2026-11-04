import { isIP } from 'node:net';
import { connect as tlsConnect } from 'node:tls';
import { TRANSPORT_KIND_TCP } from '../../core/protocol/scram.js';
import { tlsExporterBinding } from '../../core/protocol/session/binding.js';
import type { SessionLink, TcpSessionEndpoint } from '../../core/protocol/session/endpoint.js';
import { ShamirTimeoutError } from '../../core/support/errors.js';
import { exportTlsBinding } from './tls-exporter.js';
import { TlsFrameSocket } from './tls-frame-socket.js';
import { nativeTlsOptions } from './tls-options.js';

export function dialTcp(endpoint: TcpSessionEndpoint, connectTimeoutMs: number): Promise<SessionLink> {
  return new Promise<SessionLink>((resolve, reject) => {
    const tlsSocket = tlsConnect({
      host: endpoint.host,
      port: endpoint.port,
      // SNI for DNS names only (an IP literal is not a valid server name).
      ...(isIP(endpoint.host) === 0 ? { servername: endpoint.host } : {}),
      ...nativeTlsOptions(endpoint.tls),
    });
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const fail = (err: Error): void => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      tlsSocket.destroy();
      reject(err);
    };
    const onEarlyClose = (): void => {
      fail(new Error('connection closed before the TLS handshake completed'));
    };

    tlsSocket.on('error', fail);
    tlsSocket.once('close', onEarlyClose);
    tlsSocket.once('secureConnect', () => {
      if (settled) return;
      let exporter: Uint8Array;
      try {
        exporter = exportTlsBinding(tlsSocket);
      } catch (err) {
        fail(err instanceof Error ? err : new Error('TLS exporter unavailable'));
        return;
      }
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      tlsSocket.removeListener('close', onEarlyClose);
      resolve({
        socket: new TlsFrameSocket(tlsSocket),
        binding: tlsExporterBinding(TRANSPORT_KIND_TCP, exporter),
      });
    });

    if (connectTimeoutMs > 0) {
      timer = setTimeout(() => fail(new ShamirTimeoutError('connect', connectTimeoutMs)), connectTimeoutMs);
    }
  });
}
