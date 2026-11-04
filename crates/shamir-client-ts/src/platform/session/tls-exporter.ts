import type { TLSSocket } from 'node:tls';

export const TLS_EXPORTER_LABEL = 'EXPORTER-ShamirDB-AUTH-v1';

export function exportTlsBinding(socket: TLSSocket): Uint8Array {
  let material: Buffer;
  try {
    material = socket.exportKeyingMaterial(32, TLS_EXPORTER_LABEL, Buffer.alloc(0));
  } catch {
    throw new Error('TLS exporter unavailable');
  }
  if (material.length !== 32) {
    throw new Error('TLS exporter unavailable');
  }
  return new Uint8Array(material);
}
