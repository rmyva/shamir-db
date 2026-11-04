import type { SecureVersion } from 'node:tls';
import type { SessionTlsOptions } from '../../core/protocol/session/endpoint.js';

export function nativeTlsOptions(tls: SessionTlsOptions | undefined): {
  minVersion: SecureVersion;
  rejectUnauthorized: boolean;
  ca?: string | Buffer;
} {
  const out: { minVersion: SecureVersion; rejectUnauthorized: boolean; ca?: string | Buffer } = {
    minVersion: 'TLSv1.3',
    rejectUnauthorized: tls?.rejectUnauthorized ?? true,
  };
  if (tls?.ca !== undefined) {
    out.ca = typeof tls.ca === 'string' ? tls.ca : Buffer.from(tls.ca);
  }
  return out;
}
