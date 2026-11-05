import type { Platform } from '../../core/support/platform.js';
import { NodePlatform } from '../node.js';
import { dialTcpPlain } from '../plain/dial-tcp-plain.js';
import { dialTcp } from './dial-tcp.js';
import { dialWsNative } from './dial-ws-native.js';

/**
 * Node platform plus the native-link dialers; used by the Node entry for
 * `connect()`/`resume()`; never imported from the browser entry. The dialers
 * cover TCP+TLS, plain loopback TCP and native WSS.
 */
export const NodeSessionPlatform: Platform = {
  ...NodePlatform,
  openNativeLink(endpoint, opts) {
    if (endpoint.kind === 'tcp') return dialTcp(endpoint, opts.connectTimeoutMs);
    if (endpoint.kind === 'tcp-plain') return dialTcpPlain(endpoint, opts.connectTimeoutMs);
    return dialWsNative(endpoint, opts.connectTimeoutMs);
  },
};
