import type { Platform } from '../../core/support/platform.js';
import { NodePlatform } from '../node.js';
import { dialTcp } from './dial-tcp.js';
import { dialWsNative } from './dial-ws-native.js';

/**
 * Node platform plus the native-link dialers; used by the Node entry for
 * `connect()`/`resume()`; never imported from the browser entry.
 */
export const NodeSessionPlatform: Platform = {
  ...NodePlatform,
  openNativeLink(endpoint, opts) {
    return endpoint.kind === 'tcp'
      ? dialTcp(endpoint, opts.connectTimeoutMs)
      : dialWsNative(endpoint, opts.connectTimeoutMs);
  },
};
