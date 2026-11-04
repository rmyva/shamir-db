import { describe, expect, it, vi, afterEach } from 'vitest';
import { inspect } from 'node:util';
import { connect as connectNode, ApiToken as NodeApiToken } from '../../../index.js';
import { connect as connectBrowser, ApiToken as BrowserApiToken } from '../../../browser.js';
import { TokenTransportError } from '../../../core/protocol/token/errors.js';
import { ShamirClient } from '../../../core/client/client.js';
import { Batch } from '../../../core/builders/write/batch.js';
import { Query } from '../../../core/builders/read/query.js';
import { subscribe } from '../../../core/builders/stream/subscribe.js';
import type { ConnectOptions, NodeConnectOptions, BrowserConnectOptions } from '../../../core/types/connection.js';
import type { TokenHttpClient } from '../../../entry/token-client.js';
import type { TokenWsClient, PublicTokenClient } from '../../../entry/ws/token-client.js';
import type { TokenCursor as NodeTokenCursor } from '../../../index.js';
import type { TokenCursor as BrowserTokenCursor, SubscriptionEvent as BrowserSubscriptionEvent } from '../../../browser.js';
import { decodeTokenFrame, decodeTokenPayload, encodeTokenPayload } from '../../../core/protocol/token/ws.js';
import * as NodePlatformModule from '../../../platform/node.js';
import * as BrowserPlatformModule from '../../../platform/browser.js';
import { NodeSessionPlatform } from '../../../platform/session/native-platform.js';
import type { TokenPlatform, TokenSocket, TokenSocketCloseEvent } from '../../../platform/token/types.js';

const TOKEN_VALUE = `shk1.${'A'.repeat(22)}.${'A'.repeat(43)}`;
const token = NodeApiToken.from(TOKEN_VALUE);
afterEach(() => { vi.restoreAllMocks(); });

class FakeSocket implements TokenSocket {
  readonly sent: Uint8Array[] = [];
  private message: ((bytes: Uint8Array) => void) | undefined;
  private readonly early: Uint8Array[] = [];
  private nextSubId = 7;
  send(bytes: Uint8Array): void {
    this.sent.push(bytes);
    const envelope = decodeTokenFrame(bytes);
    const request = decodeTokenPayload(envelope.req as Uint8Array) as { op?: string; batch?: { queries?: Record<string, Record<string, unknown>> } };
    if (request.op === 'ping') { this.deliver({ rid: envelope.rid, res: encodeTokenPayload({ kind: 'pong' }) }); return; }
    if (request.op !== 'execute') return;
    const queries = request.batch?.queries ?? {};
    const results = Object.fromEntries(Object.entries(queries).map(([alias, query]) => {
      if ('subscribe' in query) return [alias, { value: { sub: this.nextSubId++ }, records: [] }];
      if ('unsubscribe' in query) return [alias, { records: [] }];
      return [alias, { records: [{ name: 'ok' }] }];
    }));
    this.deliver({ rid: envelope.rid, res: encodeTokenPayload({ kind: 'batch', response: { id: 1, results, execution_plan: [], execution_time_us: 1 } }) });
  }
  onMessage(handler: (bytes: Uint8Array) => void): void { this.message = handler; for (const frame of this.early.splice(0)) handler(frame); }
  onClose(_handler: (event: TokenSocketCloseEvent) => void): void {}
  close(): Promise<void> { return Promise.resolve(); }
  deliver(value: unknown): void { const frame = encodeTokenPayload(value); if (this.message) this.message(frame); else this.early.push(frame); }
}
function fakePlatform(runtime: 'node' | 'browser', socket: FakeSocket): TokenPlatform & { requests: URL[] } {
  const requests: URL[] = [];
  return {
    runtime, requests,
    async requestHttp(url, _token, init) {
      requests.push(url);
      const request = init.body ? decodeTokenPayload(init.body) as { batch?: { queries?: Record<string, unknown> } } : undefined;
      const results = Object.fromEntries(Object.keys(request?.batch?.queries ?? {}).map((alias) => [alias, { records: [{ name: 'ok' }] }]));
      const body = init.method === 'GET' ? encodeTokenPayload({ kind: 'pong' }) : encodeTokenPayload({ kind: 'batch', response: { id: 1, results, execution_plan: [], execution_time_us: 1 } });
      return { status: 200, headers: { get: (name: string) => name.toLowerCase() === 'content-type' ? 'application/msgpack' : null }, body };
    },
    async openTokenSocket() { socket.deliver({ ok: true, sid: new Uint8Array(32).fill(1), expires_at_ns: 99n }); return socket; },
    async close() {},
  };
}
function expectRestDenial(action: () => unknown): void {
  try { action(); } catch (error) { expect(error).toBeInstanceOf(TokenTransportError); expect(error).toMatchObject({ code: 'not_supported_over_rest' }); return; }
  throw new Error('expected REST capability denial');
}

describe('public connect contract', () => {
  it('accepts Node remote plaintext HTTP and WebSocket URLs', async () => {
    const httpPlatform = fakePlatform('node', new FakeSocket());
    vi.spyOn(NodePlatformModule, 'createNodeTokenPlatform').mockReturnValue(httpPlatform);
    const http = await connectNode({ auth: 'token', url: 'http://127.attacker.example', token });
    expect(http.channel).toBe('http'); await http.close();
    const wsPlatform = fakePlatform('node', new FakeSocket());
    vi.spyOn(NodePlatformModule, 'createNodeTokenPlatform').mockReturnValue(wsPlatform);
    const ws = await connectNode({ auth: 'token', url: 'ws://db.remote.example', token });
    expect(ws.channel).toBe('ws'); await ws.close();
  });
  it('rejects invalid Node and browser options before platform construction and preserves SCRAM', async () => {
    const nodeFactory = vi.spyOn(NodePlatformModule, 'createNodeTokenPlatform');
    const browserFactory = vi.spyOn(BrowserPlatformModule, 'createBrowserTokenPlatform');
    const controller = new AbortController(); controller.abort();
    const invalidNode: Array<[unknown, 'invalid_request' | 'client_aborted']> = [
      [{ auth: 'token', url: 'https://db.example/path', token }, 'invalid_request'],
      [{ auth: 'token', url: 'https://db.example?query=x', token }, 'invalid_request'],
      [{ auth: 'token', url: 'https://user:pass@db.example', token }, 'invalid_request'],
      [{ auth: 'token', url: 'https://db.example', token, tls: [] }, 'invalid_request'],
      [{ auth: 'token', url: 'https://db.example', token, signal: controller.signal }, 'client_aborted'],
      [{ auth: 'token', url: 'https://db.example', token, username: 'alice' }, 'invalid_request'],
      [{ auth: 'token', url: 'https://db.example', token, username: 'alice', password: 'secret' }, 'invalid_request'],
      [{ auth: 'token', url: 'https://db.example', token: Object.create(NodeApiToken.prototype) }, 'invalid_request'],
      [{ url: 'https://db.example', token }, 'invalid_request'],
      [{ auth: 'unknown', url: 'https://db.example', token }, 'invalid_request'],
      [{ auth: 'token', url: 'http://db.example', token, tls: {} }, 'invalid_request'],
      [{ auth: 'token', url: 'ws://db.example', token, tls: {} }, 'invalid_request'],
      [{ auth: 'token', url: 'https://db.example', token, tls: { allowInsecureTlsForDevelopment: false } }, 'invalid_request'],
      [{ auth: 'token', url: 'https://db.example', token, connectTimeoutMs: 1.5 }, 'invalid_request'],
      [{ auth: 'token', channel: 'ws', url: 'https://db.example', token }, 'invalid_request'],
      [{ auth: 'token', channel: 'http', url: 'wss://db.example', token }, 'invalid_request'],
      [{ auth: 'token', channel: 'http', url: 'ws://db.example', token }, 'invalid_request'],
    ];
    for (const [options, code] of invalidNode) await expect(connectNode(options as never)).rejects.toMatchObject({ code });
    const invalidBrowser: Array<[unknown, 'invalid_request']> = [
      [{ auth: 'token', url: 'https://db.example/path', token }, 'invalid_request'],
      [{ auth: 'token', url: 'https://db.example', token, tls: {} }, 'invalid_request'],
      [{ auth: 'token', url: 'https://db.example', token, origin: 'https://other.example' }, 'invalid_request'],
      [{ auth: 'token', url: 'https://db.example', token, requestTimeoutMs: -1 }, 'invalid_request'],
      [{ auth: 'token', url: 'https://db.example', token, tls: { allowInsecureTlsForDevelopment: false } }, 'invalid_request'],
      [{ auth: 'token', url: 'ws://db.remote.example', token }, 'invalid_request'],
      [{ auth: 'token', url: 'https://db.example', token, allow_self_signed: true }, 'invalid_request'],
      [{ auth: 'token', url: 'https://db.example', token, username: 'alice' }, 'invalid_request'],
      [{ url: 'https://db.example', token }, 'invalid_request'],
      [{ auth: 'unknown', url: 'https://db.example', token }, 'invalid_request'],
      [{ auth: 'token', channel: 'http', url: 'wss://db.example', token }, 'invalid_request'],
    ];
    for (const [options, code] of invalidBrowser) await expect(connectBrowser(options as never)).rejects.toMatchObject({ code });
    expect(nodeFactory).not.toHaveBeenCalled(); expect(browserFactory).not.toHaveBeenCalled();
    const client = Object.create(ShamirClient.prototype) as ShamirClient;
    const connectSpy = vi.spyOn(ShamirClient, 'connect').mockResolvedValue(client);
    const options: ConnectOptions = { host: 'localhost', port: 7000, username: 'alice', password: 'safe' };
    const nodeResult: Promise<ShamirClient> = connectNode(options);
    const browserResult: Promise<ShamirClient> = connectBrowser(options);
    await expect(nodeResult).resolves.toBe(client); await expect(browserResult).resolves.toBe(client);
    expect(connectSpy).toHaveBeenNthCalledWith(1, NodeSessionPlatform, options);
    expect(connectSpy).toHaveBeenNthCalledWith(2, BrowserPlatformModule.BrowserPlatform, options);
  });
  it('selects Node HTTP from scheme and runs ping/query requests through REST', async () => {
    const socket = new FakeSocket(); const platform = fakePlatform('node', socket);
    const factory = vi.spyOn(NodePlatformModule, 'createNodeTokenPlatform').mockReturnValue(platform);
    const client = await connectNode({ auth: 'token', url: 'https://db.example', token: TOKEN_VALUE });
    await expect(client.ping()).resolves.toEqual({ kind: 'pong' });
    await expect(client.db('db').run(Query.from('items').select(['name']))).resolves.toMatchObject({ records: [{ name: 'ok' }] });
    expect(factory).toHaveBeenCalledWith(expect.objectContaining({ channel: 'http', token: expect.any(NodeApiToken), url: 'https://db.example/' }));
    expect(socket.sent).toHaveLength(0); expect(platform.requests.map((url) => url.pathname)).toEqual(['/shamir/v1/ping', '/shamir/v1/request']); await client.close();
  });
  it('uses explicit Node WS and never sends HTTP requests', async () => {
    const socket = new FakeSocket(); const platform = fakePlatform('node', socket);
    const factory = vi.spyOn(NodePlatformModule, 'createNodeTokenPlatform').mockReturnValue(platform);
    const client = await connectNode({ auth: 'token', channel: 'ws', url: 'wss://db.example', token: TOKEN_VALUE });
    await expect(client.ping()).resolves.toEqual({ kind: 'pong' });
    await expect(client.db('db').run(Query.from('items').select(['name']))).resolves.toMatchObject({ records: [{ name: 'ok' }] });
    expect(factory).toHaveBeenCalledWith(expect.objectContaining({ channel: 'ws', token: expect.any(NodeApiToken), url: 'wss://db.example' }));
    expect(platform.requests).toHaveLength(0); expect(socket.sent).toHaveLength(2); await client.close();
  });
  it('connects in browser over HTTP and WS using normalized string tokens', async () => {
    const httpSocket = new FakeSocket(); const httpPlatform = fakePlatform('browser', httpSocket);
    const httpFactory = vi.spyOn(BrowserPlatformModule, 'createBrowserTokenPlatform').mockReturnValue(httpPlatform);
    const http = await connectBrowser({ auth: 'token', url: 'https://db.example', token: TOKEN_VALUE });
    expect(http.channel).toBe('http'); expect(httpPlatform.requests).toHaveLength(0);
    expect(httpFactory).toHaveBeenCalledWith(expect.objectContaining({ channel: 'http', token: expect.any(BrowserApiToken), url: 'https://db.example/' }));
    await expect(http.ping()).resolves.toEqual({ kind: 'pong' });
    await expect(http.db('db').run(Query.from('items').select(['name']))).resolves.toMatchObject({ records: [{ name: 'ok' }] });
    expect(httpPlatform.requests.map((url) => url.pathname)).toEqual(['/shamir/v1/ping', '/shamir/v1/request']); await http.close();
    const wsSocket = new FakeSocket(); const wsPlatform = fakePlatform('browser', wsSocket);
    const wsFactory = vi.spyOn(BrowserPlatformModule, 'createBrowserTokenPlatform').mockReturnValue(wsPlatform);
    const ws = await connectBrowser({ auth: 'token', channel: 'ws', url: 'wss://db.example', token: TOKEN_VALUE });
    expect(ws.channel).toBe('ws');
    expect(wsFactory).toHaveBeenCalledWith(expect.objectContaining({ channel: 'ws', token: expect.any(BrowserApiToken), url: 'wss://db.example' }));
    await expect(ws.ping()).resolves.toEqual({ kind: 'pong' });
    await expect(ws.db('db').run(Query.from('items').select(['name']))).resolves.toMatchObject({ records: [{ name: 'ok' }] });
    expect(wsPlatform.requests).toHaveLength(0); expect(wsSocket.sent).toHaveLength(2); await ws.close();
  });
  it('denies REST-only operations before sending through both public entries', async () => {
    const nodePlatform = fakePlatform('node', new FakeSocket());
    vi.spyOn(NodePlatformModule, 'createNodeTokenPlatform').mockReturnValue(nodePlatform);
    const node = await connectNode({ auth: 'token', url: 'https://db.example', token });
    const nodeCallback = vi.fn(async () => 'x');
    expectRestDenial(() => node.db('db').cursor(Query.from('items'), 10));
    await expect(node.db('db').tx(nodeCallback)).rejects.toMatchObject({ code: 'not_supported_over_rest' });
    await expect(node.db('db').runLive(Batch.create().add('read', Query.from('items')))).rejects.toMatchObject({ code: 'not_supported_over_rest' });
    await expect(node.execute('db', Batch.create().add('sub', subscribe({ store: 'main', table: 'items' })).build())).rejects.toMatchObject({ code: 'not_supported_over_rest' });
    expect(nodeCallback).not.toHaveBeenCalled(); expect(nodePlatform.requests).toHaveLength(0); await node.close();
    const browserPlatform = fakePlatform('browser', new FakeSocket());
    vi.spyOn(BrowserPlatformModule, 'createBrowserTokenPlatform').mockReturnValue(browserPlatform);
    const browser = await connectBrowser({ auth: 'token', url: 'https://db.example', token });
    const browserCallback = vi.fn(async () => 'x');
    expectRestDenial(() => browser.db('db').cursor(Query.from('items'), 10));
    await expect(browser.db('db').tx(browserCallback)).rejects.toMatchObject({ code: 'not_supported_over_rest' });
    await expect(browser.db('db').runLive(Batch.create().add('read', Query.from('items')))).rejects.toMatchObject({ code: 'not_supported_over_rest' });
    await expect(browser.execute('db', Batch.create().add('sub', subscribe({ store: 'main', table: 'items' })).build())).rejects.toMatchObject({ code: 'not_supported_over_rest' });
    expect(browserCallback).not.toHaveBeenCalled(); expect(browserPlatform.requests).toHaveLength(0); await browser.close();
  });
  it('delivers pushes through both public entries and unsubscribes', async () => {
    const nodeSocket = new FakeSocket(); const nodePlatform = fakePlatform('node', nodeSocket);
    vi.spyOn(NodePlatformModule, 'createNodeTokenPlatform').mockReturnValue(nodePlatform);
    const node = await connectNode({ auth: 'token', url: 'wss://db.example', token });
    const nodeLive = await node.db('db').runLive(Batch.create().add('sub', subscribe({ store: 'main', table: 'items' })));
    nodeSocket.deliver({ push: 'event', sub: 7, seq: 0, data: Uint8Array.of(4, 2) });
    expect(await nodeLive.subs.sub!.next()).toMatchObject({ kind: 'event', seq: 0, data: Uint8Array.of(4, 2) });
    await nodeLive.subs.sub!.unsubscribe(); expect(nodeSocket.sent).toHaveLength(2);
    const nodeUnsub = decodeTokenPayload(decodeTokenFrame(nodeSocket.sent[1]!).req as Uint8Array) as { batch?: { queries?: Record<string, unknown> } };
    expect(nodeUnsub.batch?.queries?._unsub).toEqual({ unsubscribe: 7 }); expect(nodePlatform.requests).toHaveLength(0); await node.close();
    const browserSocket = new FakeSocket(); const browserPlatform = fakePlatform('browser', browserSocket);
    vi.spyOn(BrowserPlatformModule, 'createBrowserTokenPlatform').mockReturnValue(browserPlatform);
    const browser = await connectBrowser({ auth: 'token', url: 'wss://db.example', token });
    const browserLive = await browser.db('db').runLive(Batch.create().add('sub', subscribe({ store: 'main', table: 'items' })));
    browserSocket.deliver({ push: 'event', sub: 7, seq: 1, data: Uint8Array.of(8, 5) });
    expect(await browserLive.subs.sub!.next()).toMatchObject({ kind: 'event', seq: 1, data: Uint8Array.of(8, 5) });
    await browserLive.subs.sub!.unsubscribe(); expect(browserSocket.sent).toHaveLength(2);
    const browserUnsub = decodeTokenPayload(decodeTokenFrame(browserSocket.sent[1]!).req as Uint8Array) as { batch?: { queries?: Record<string, unknown> } };
    expect(browserUnsub.batch?.queries?._unsub).toEqual({ unsubscribe: 7 }); expect(browserPlatform.requests).toHaveLength(0); await browser.close();
  });
  it('routes two subscriptions and makes repeated unsubscribe idempotent', async () => {
    const socket = new FakeSocket();
    vi.spyOn(NodePlatformModule, 'createNodeTokenPlatform').mockReturnValue(fakePlatform('node', socket));
    const client = await connectNode({ auth: 'token', url: 'wss://db.example', token });
    const live = await client.db('db').runLive(Batch.create().add('first', subscribe({ store: 'main', table: 'items' })).add('second', subscribe({ store: 'main', table: 'other' })));
    await live.subs.first!.unsubscribe(); await live.subs.first!.unsubscribe(); await live.subs.second!.unsubscribe();
    expect(socket.sent).toHaveLength(3);
    expect(socket.sent.slice(1).map((frame) => {
      const request = decodeTokenPayload(decodeTokenFrame(frame).req as Uint8Array) as { batch?: { queries?: Record<string, { unsubscribe?: number }> } };
      return Object.values(request.batch?.queries ?? {})[0]?.unsubscribe;
    })).toEqual([7, 8]);
    await client.close();
  });
  it('derives or honors matching channel choices across Node and browser schemes', async () => {
    const cases = [
      { runtime: 'node' as const, url: 'https://db.example', channel: 'http' as const },
      { runtime: 'node' as const, url: 'http://db.example', channel: 'http' as const },
      { runtime: 'node' as const, url: 'wss://db.example', channel: 'ws' as const },
      { runtime: 'node' as const, url: 'ws://db.example', channel: 'ws' as const },
      { runtime: 'browser' as const, url: 'https://db.example', channel: 'http' as const },
      { runtime: 'browser' as const, url: 'http://db.example', channel: 'http' as const },
      { runtime: 'browser' as const, url: 'wss://db.example', channel: 'ws' as const },
      { runtime: 'browser' as const, url: 'ws://127.0.0.1', channel: 'ws' as const },
    ];
    for (const entry of cases) for (const explicit of [false, true]) {
      const socket = new FakeSocket(); const platform = fakePlatform(entry.runtime, socket);
      const nodeFactory = entry.runtime === 'node' ? vi.spyOn(NodePlatformModule, 'createNodeTokenPlatform').mockReturnValue(platform) : undefined;
      const browserFactory = entry.runtime === 'browser' ? vi.spyOn(BrowserPlatformModule, 'createBrowserTokenPlatform').mockReturnValue(platform) : undefined;
      const options: { auth: 'token'; url: string; token: NodeApiToken | string; channel?: 'http' | 'ws' } = { auth: 'token', url: entry.url, token, ...(explicit ? { channel: entry.channel } : {}) };
      const client = await (entry.runtime === 'node' ? connectNode(options) : connectBrowser(options));
      expect(client.channel).toBe(entry.channel);
      expect(nodeFactory ?? browserFactory).toHaveBeenCalledWith(expect.objectContaining({ channel: entry.channel }));
      await client.close(); vi.restoreAllMocks();
    }
  });
  it('checks public overload returns without invoking connect', () => {
    type Assert<T extends true> = T;
    type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;
    type NodeParams = Assert<Equal<Parameters<typeof connectNode>, [NodeConnectOptions]>>;
    type BrowserParams = Assert<Equal<Parameters<typeof connectBrowser>, [BrowserConnectOptions]>>;
    type NodeReturn = Assert<Equal<ReturnType<typeof connectNode>, Promise<ShamirClient | PublicTokenClient>>>;
    type BrowserReturn = Assert<Equal<ReturnType<typeof connectBrowser>, Promise<ShamirClient | PublicTokenClient>>>;
    const probe = (): void => {
      const nodeSchemeHttp: Promise<TokenHttpClient> = connectNode({ auth: 'token', url: 'https://db.example', token });
      const nodeExplicitHttp: Promise<TokenHttpClient> = connectNode({ auth: 'token', channel: 'http', url: 'https://db.example', token });
      const nodeWs: Promise<TokenWsClient> = connectNode({ auth: 'token', url: 'wss://db.example', token });
      const nodeGeneric: Promise<PublicTokenClient> = connectNode({ auth: 'token', url: String('https://db.example'), token });
      const browserSchemeHttp: Promise<TokenHttpClient> = connectBrowser({ auth: 'token', url: 'https://db.example', token });
      const browserExplicitHttp: Promise<TokenHttpClient> = connectBrowser({ auth: 'token', channel: 'http', url: 'https://db.example', token });
      const browserWs: Promise<TokenWsClient> = connectBrowser({ auth: 'token', url: 'wss://db.example', token });
      const browserGeneric: Promise<PublicTokenClient> = connectBrowser({ auth: 'token', url: String('https://db.example'), token });
      void [nodeSchemeHttp, nodeExplicitHttp, nodeWs, nodeGeneric, browserSchemeHttp, browserExplicitHttp, browserWs, browserGeneric];
    };
    void probe;
    const typeExports: [NodeTokenCursor, BrowserTokenCursor, BrowserSubscriptionEvent] | undefined = undefined; void typeExports;
    type CursorExports = Assert<Equal<NodeTokenCursor, BrowserTokenCursor>>;
    type Checks = [NodeParams, BrowserParams, NodeReturn, BrowserReturn, CursorExports]; void (0 as unknown as Checks);
  });
  it('exports no platform internals and redacts public token values', async () => {
    const node = await import('../../../index.js'); const browser = await import('../../../browser.js');
    expect(Object.keys(node)).not.toContain('TokenWsClient'); expect(Object.keys(node)).not.toContain('TokenHttpClient');
    expect(Object.keys(browser)).not.toContain('TokenWsClient'); expect(Object.keys(browser)).not.toContain('TokenHttpClient');
    expect(Object.keys(node)).not.toContain('connectTokenNode'); expect(Object.keys(browser)).not.toContain('connectTokenBrowser');
    expect(Object.keys(browser)).not.toContain('createNodeTokenPlatform'); expect(Object.keys(browser)).not.toContain('createBrowserTokenPlatform');
    let error: unknown;
    try { await connectNode({ auth: 'token', url: `https://db.example/${TOKEN_VALUE}`, token }); } catch (caught) { error = caught; }
    expect(`${String(error)} ${String((error as Error).stack)} ${inspect(error)}`).not.toContain(TOKEN_VALUE);
    expect(String(token)).toBe('[REDACTED]'); expect(JSON.stringify(token)).toBe('"[REDACTED]"');
    expect(inspect(token)).toBe('ApiToken [REDACTED]'); expect(Reflect.ownKeys(token)).toEqual([]); expect(structuredClone(token)).toEqual({});
  });
});
