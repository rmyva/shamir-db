import { connect as netConnect, type Socket as NetSocket } from 'node:net';
import { tcpPlainBinding } from '../../core/protocol/session/binding.js';
import type { SessionLink, TcpPlainSessionEndpoint } from '../../core/protocol/session/endpoint.js';
import { isLoopbackHost } from '../../core/protocol/session/open.js';
import { ShamirTimeoutError } from '../../core/support/errors.js';
import { PlainFrameSocket } from './plain-frame-socket.js';

/**
 * Dial a plain loopback TCP session link (`kind: tcp, profile: plain`;
 * channel binding `{ TCP, 0x00 }` with the zero exporter). Node-only. The
 * loopback refusal happens before any socket is opened, the same rule the
 * Rust `connect_plain` applies (the server additionally refuses non-loopback
 * plain binds itself).
 */
export function dialTcpPlain(endpoint: TcpPlainSessionEndpoint, connectTimeoutMs: number): Promise<SessionLink> {
  if (!isLoopbackHost(endpoint.host)) {
    return Promise.reject(new Error('plain TCP is loopback-only'));
  }
  return new Promise<SessionLink>((resolve, reject) => {
    const socket: NetSocket = netConnect({ host: endpoint.host, port: endpoint.port });
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const fail = (err: Error): void => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      socket.destroy();
      reject(err);
    };
    const onEarlyClose = (): void => {
      fail(new Error('connection closed before the plain TCP session could start'));
    };

    socket.on('error', fail);
    socket.once('close', onEarlyClose);
    socket.once('connect', () => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      socket.removeListener('close', onEarlyClose);
      resolve({
        socket: new PlainFrameSocket(socket),
        binding: tcpPlainBinding(),
      });
    });

    if (connectTimeoutMs > 0) {
      timer = setTimeout(() => fail(new ShamirTimeoutError('connect', connectTimeoutMs)), connectTimeoutMs);
    }
  });
}
