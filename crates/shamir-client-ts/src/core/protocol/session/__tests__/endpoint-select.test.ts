/**
 * Unit tests for endpoint selection and dialing. Fakes only: no network.
 */

import { describe, it, expect, vi } from 'vitest';
import type { Socket, Platform } from '../../../support/platform.js';
import {
  dialEndpoint,
  endpointFromOptions,
  isLoopbackHost,
} from '../open.js';
import { tlsExporterBinding } from '../binding.js';
import type {
  NativeSessionEndpoint,
  SessionEndpoint,
  SessionLink,
} from '../endpoint.js';

const fakeSocket = {} as unknown as Socket;

describe('endpointFromOptions', () => {
  it('defaults to the browser profile', () => {
    expect(endpointFromOptions({ host: 'db.example', port: 7333 })).toEqual({
      kind: 'ws-browser',
      url: 'wss://db.example:7333/shamir/v1/browser',
      origin: 'https://db.example',
      tls: { rejectUnauthorized: true },
    });
  });

  it('honours an explicit origin and ws-browser transport', () => {
    expect(
      endpointFromOptions({
        host: 'db.example',
        port: 7333,
        transport: 'ws-browser',
        origin: 'https://app.example',
        tls: { rejectUnauthorized: false },
      }),
    ).toEqual({
      kind: 'ws-browser',
      url: 'wss://db.example:7333/shamir/v1/browser',
      origin: 'https://app.example',
      tls: { rejectUnauthorized: false },
    });
  });

  it('selects tcp', () => {
    expect(
      endpointFromOptions({ host: '127.0.0.1', port: 7334, transport: 'tcp' }),
    ).toEqual({
      kind: 'tcp',
      host: '127.0.0.1',
      port: 7334,
      tls: { rejectUnauthorized: true },
    });
    expect(
      endpointFromOptions({
        host: '127.0.0.1',
        port: 7334,
        transport: 'tcp',
        tls: { rejectUnauthorized: false },
      }),
    ).toEqual({
      kind: 'tcp',
      host: '127.0.0.1',
      port: 7334,
      tls: { rejectUnauthorized: false },
    });
    expect(
      endpointFromOptions({
        host: '127.0.0.1',
        port: 7334,
        transport: 'tcp',
        tls: { ca: 'PEM' },
      }),
    ).toEqual({
      kind: 'tcp',
      host: '127.0.0.1',
      port: 7334,
      tls: { rejectUnauthorized: true, ca: 'PEM' },
    });
  });

  it('selects ws-native and brackets IPv6 hosts', () => {
    expect(
      endpointFromOptions({ host: '127.0.0.1', port: 7333, transport: 'ws-native' }),
    ).toEqual({
      kind: 'ws-native',
      url: 'wss://127.0.0.1:7333/shamir/v1',
      tls: { rejectUnauthorized: true },
    });
    expect(
      endpointFromOptions({ host: '::1', port: 7333, transport: 'ws-native' }),
    ).toEqual({
      kind: 'ws-native',
      url: 'wss://[::1]:7333/shamir/v1',
      tls: { rejectUnauthorized: true },
    });
  });

  it('rejects tls.ca with the browser profile', () => {
    expect(() =>
      endpointFromOptions({ host: 'h', port: 1, tls: { ca: 'PEM' } }),
    ).toThrow(/tls\.ca is only supported/);
  });

  it('rejects an unknown transport', () => {
    expect(() =>
      endpointFromOptions({
        host: 'h',
        port: 1,
        transport: 'carrier-pigeon' as unknown as 'tcp',
      }),
    ).toThrow(/unknown transport: carrier-pigeon/);
  });
});

describe('tcp-plain transport', () => {
  it('produces a plain tcp-plain endpoint without a tls field', () => {
    expect(
      endpointFromOptions({ host: '127.0.0.1', port: 1, transport: 'tcp-plain' }),
    ).toEqual({ kind: 'tcp-plain', host: '127.0.0.1', port: 1 });
  });

  it('refuses tls options — plain TCP has no TLS', () => {
    expect(() =>
      endpointFromOptions({
        host: '127.0.0.1',
        port: 1,
        transport: 'tcp-plain',
        tls: { rejectUnauthorized: false },
      }),
    ).toThrow(/no TLS/);
    expect(() =>
      endpointFromOptions({
        host: '127.0.0.1',
        port: 1,
        transport: 'tcp-plain',
        tls: { ca: 'PEM' },
      }),
    ).toThrow(/no TLS/);
  });

  it('refuses non-loopback hosts synchronously, before any socket', () => {
    for (const host of ['10.0.0.1', 'example.com', '192.168.1.5']) {
      expect(() =>
        endpointFromOptions({ host, port: 1, transport: 'tcp-plain' }),
      ).toThrow(/loopback/);
    }
  });

  it('accepts loopback hosts', () => {
    for (const host of ['127.0.0.1', 'localhost', '::1']) {
      expect(
        endpointFromOptions({ host, port: 1, transport: 'tcp-plain' }),
      ).toEqual({ kind: 'tcp-plain', host, port: 1 });
    }
  });
});

describe('isLoopbackHost', () => {
  it('recognises loopback forms', () => {
    for (const host of [
      'localhost',
      '127.0.0.1',
      '127.9.9.9',
      '::1',
      '[::1]',
      '0:0:0:0:0:0:0:1',
      '0000:0000:0000:0000:0000:0000:0000:0001',
      '::ffff:127.0.0.1',
      '::ffff:7f00:1',
    ]) {
      expect(isLoopbackHost(host)).toBe(true);
    }
  });

  it('rejects non-loopback forms', () => {
    for (const host of [
      '10.0.0.1',
      'example.com',
      '192.168.1.5',
      '',
      '127.999.0.1',
      '127.0.0.256',
      '127.0.0',
      '::2',
      '0:0:0:0:0:0:0:2',
      '1::1',
      '::ffff:10.0.0.1',
      '::ffff:127.0.0.999',
    ]) {
      expect(isLoopbackHost(host)).toBe(false);
    }
  });
});

describe('dialEndpoint', () => {
  const tcp: NativeSessionEndpoint = {
    kind: 'tcp',
    host: 'h',
    port: 1,
    tls: { rejectUnauthorized: true },
  };
  const wsNative: NativeSessionEndpoint = {
    kind: 'ws-native',
    url: 'wss://h:1/shamir/v1',
    tls: { rejectUnauthorized: true },
  };

  it('fails before touching the network without openNativeLink', async () => {
    const openSocket = vi.fn();
    const platform = { openSocket } as unknown as Platform;
    await expect(dialEndpoint(platform, tcp, 1000)).rejects.toThrow(/openNativeLink/);
    await expect(dialEndpoint(platform, wsNative, 1000)).rejects.toThrow(
      /openNativeLink/,
    );
    expect(openSocket).not.toHaveBeenCalled();
  });

  it('delegates native endpoints to openNativeLink', async () => {
    const link: SessionLink = {
      socket: fakeSocket,
      binding: tlsExporterBinding(0x01, new Uint8Array(32)),
    };
    const openNativeLink = vi.fn(
      async (_e: NativeSessionEndpoint, _o: { connectTimeoutMs: number }) => link,
    );
    const platform = { openNativeLink } as unknown as Platform;
    const got = await dialEndpoint(platform, tcp, 1234);
    expect(got).toBe(link);
    expect(openNativeLink).toHaveBeenCalledWith(tcp, { connectTimeoutMs: 1234 });
  });

  it('opens the browser profile through openSocket', async () => {
    const openSocket = vi.fn(
      async (_u: string, _o: { rejectUnauthorized?: boolean; origin?: string }) =>
        fakeSocket,
    );
    const platform = { openSocket } as unknown as Platform;
    const endpoint: SessionEndpoint = {
      kind: 'ws-browser',
      url: 'wss://h:1/shamir/v1/browser',
      origin: 'https://h',
    };
    const link = await dialEndpoint(platform, endpoint, 1000);
    expect(openSocket).toHaveBeenCalledWith('wss://h:1/shamir/v1/browser', {
      rejectUnauthorized: true,
      origin: 'https://h',
    });
    expect(link.socket).toBe(fakeSocket);
    expect(link.binding.bindingMode).toBe(0x02);
  });

  it('rejects ipc endpoints', async () => {
    const platform = {} as unknown as Platform;
    await expect(
      dialEndpoint(platform, { kind: 'ipc', addr: 'x' }, 1000),
    ).rejects.toThrow(/openLocalSession/);
  });
});
