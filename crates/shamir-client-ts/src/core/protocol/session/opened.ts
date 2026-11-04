/**
 * Plain-data result of opening/resuming a session.
 *
 * PLATFORM-AGNOSTIC.
 */

import type { WsFramer } from '../framing.js';
import type { ChannelBinding } from './binding.js';

/** Plain data produced by a successful session open/resume; the ShamirClient constructor consumes it. */
export interface SessionOpened {
  framer: WsFramer;
  sessionId: Uint8Array;
  serverPubKey: Uint8Array;
  expiresAtNs: bigint;
  resumptionTicket: Uint8Array | undefined;
  resumptionExpiresAtNs: bigint | undefined;
  serverQueryVersion: number;
  /** NFC-normalised username; undefined on resumed sessions. */
  normalizedUser: string | undefined;
  binding: ChannelBinding;
  /**
   * Pin of this session's server identity: SHA256(serverPubKey). Persist it
   * and pass it back as `trustedPin` on later connections.
   */
  trustedPin: Uint8Array;
}
