'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const Module = require('node:module');
const codec = require('@msgpack/msgpack');
const { encode } = codec;
let decodedInputs = [];

// These are codec-boundary responses, not examples of query construction.
let response;
let requestBytes;
let nativeCalls = [];
class NativeClient {
  static async connectWs(o) { nativeCalls.push(['connectWs', o]); return new NativeClient(); }
  static async connectPlain(o) { nativeCalls.push(['connectPlain', o]); return new NativeClient(); }
  static async connectLocal(o) { nativeCalls.push(['connectLocal', o]); return new NativeClient(); }
  static async resume(o) { nativeCalls.push(['resume', o]); return new NativeClient(); }
  static async resumeWs(o) { nativeCalls.push(['resumeWs', o]); return new NativeClient(); }
  static async connectWsBrowser(o) { nativeCalls.push(['connectWsBrowser', o]); return new NativeClient(); }
  static async resumeWsBrowser(o) { nativeCalls.push(['resumeWsBrowser', o]); return new NativeClient(); }
  static async resumePlain(o) { nativeCalls.push(['resumePlain', o]); return new NativeClient(); }
  static async resumeLocal(o) { nativeCalls.push(['resumeLocal', o]); return new NativeClient(); }
  static async connectTokenWs(o) { nativeCalls.push(['connectTokenWs', o]); return new NativeClient(); }
  authKind() { return 'token'; }
  async createApiToken(o) { nativeCalls.push(['createApiToken', o]); return response; }
  async createScramUser() { return response; }
  async repl() { return response; }
  async setReplicator() { return response; }
  async execute(db, bytes) { requestBytes = bytes; return response; }
}

class NativeTokenHttpClient {
  static async connect(o) { nativeCalls.push(['httpConnect', o]); return new NativeTokenHttpClient(); }
  async execute(db, bytes) { requestBytes = bytes; return response; }
}
async function nativeLogin(o) { nativeCalls.push(['login', o]); return response; }

const load = Module._load;
let wrapper;
try {
  Module._load = function (request, parent, isMain) {
    if (request === '../native/index.js') return {
      ShamirClient: NativeClient,
      ShamirTokenHttpClient: NativeTokenHttpClient,
      login: nativeLogin,
    };
    if (request === '@msgpack/msgpack') return {
      ...codec,
      decode(input, options) { decodedInputs.push(input); return codec.decode(input, options); },
    };
    return load.call(this, request, parent, isMain);
  };
  wrapper = require('../../js/wrapper.js');
} finally {
  Module._load = load;
}

test('raw user ID colliding with a kind-only error is returned unchanged', async () => {
  response = Buffer.from([0x82, 0xa4, 0x6b, 0x69, 0x6e, 0x64, 0xa5, 0x65,
    0x72, 0x72, 0x6f, 0x72, 0xa1, 0x78, 0xa1, 0x79]);
  assert.strictEqual(await new wrapper.ShamirClient().createScramUser('u', 'p', []), response);
});

test('raw response methods preserve successes and throw typed errors', async () => {
  const client = new wrapper.ShamirClient();
  response = Buffer.from(encode({ repl_kind: 'events', events: new Uint8Array([1, 2]) }));
  assert.strictEqual(await client.repl(Buffer.alloc(0)), response);
  const truncated = Buffer.from([0x83, 0xa4, 0x6b, 0x69, 0x6e, 0x64, 0xa5,
    0x65, 0x72, 0x72, 0x6f, 0x72]);
  response = truncated;
  assert.strictEqual(await client.repl(Buffer.alloc(0)), truncated);
  response = Buffer.from(encode({ kind: 'error', code: 'timeout', message: 'late' }));
  for (const call of [() => client.repl(Buffer.alloc(0)),
    () => client.createScramUser('u', 'p', []), () => client.setReplicator('u', true)]) {
    await assert.rejects(call, error => error instanceof wrapper.ShamirDbError &&
      error.code === 'timeout' && error.detail === 'late' && error.retryable);
  }
});

test('raw successes are not decoded and execute decodes the original Buffer', async () => {
  const client = new wrapper.ShamirClient();
  decodedInputs = [];
  response = Buffer.from(encode({ repl_kind: 'events', events: new Uint8Array([1, 2]) }));
  assert.strictEqual(await client.repl(Buffer.alloc(0)), response);
  response = Buffer.alloc(16, 7);
  assert.strictEqual(await client.createScramUser('u', 'p', []), response);
  assert.equal(decodedInputs.length, 0);
  response = Buffer.from(encode({ result: 3 }));
  await client.execute('db', {});
  assert.strictEqual(decodedInputs[0], response);
});

test('independent int64 and uint64 fixtures round trip without rounding', async () => {
  const client = new wrapper.ShamirClient();
  const fixtures = [
    ['d3 00 20 00 00 00 00 00 01', 9007199254740993n],
    ['d3 ff df ff ff ff ff ff ff', -9007199254740993n],
    ['d3 80 00 00 00 00 00 00 00', -(1n << 63n)],
    ['d3 7f ff ff ff ff ff ff ff', (1n << 63n) - 1n],
    ['d3 00 00 00 00 00 00 00 00', 0],
    ['cf 00 00 00 00 00 00 00 00', 0],
    ['cf 00 1f ff ff ff ff ff ff', Number.MAX_SAFE_INTEGER],
    ['d3 ff e0 00 00 00 00 00 01', Number.MIN_SAFE_INTEGER],
    ['cf 80 00 00 00 00 00 00 00', 1n << 63n],
    ['cf ff ff ff ff ff ff ff ff', (1n << 64n) - 1n],
    ['d3 ff ff ff ff ff ff ff ff', -1],
  ];
  for (const [hex, expected] of fixtures) {
    response = Buffer.from(hex.replaceAll(' ', ''), 'hex');
    const decoded = await client.execute('db', {});
    assert.equal(decoded, expected);
    await client.execute('db', BigInt(expected));
    assert.equal(codec.decode(requestBytes, { useBigInt64: true }), BigInt(expected));
  }
});

test('wide safe numbers use integer markers; nested binary and floats survive', async () => {
  const client = new wrapper.ShamirClient();
  response = Buffer.from('81a17694cf0000000100000000d3ffffffff7fffffffc402cfffcb3ff8000000000000', 'hex');
  const nested = await client.execute('db', {});
  assert.deepEqual(nested.v.slice(0, 2), [4294967296, -2147483649]);
  assert.deepEqual([...nested.v[2]], [0xcf, 0xff]);
  assert.equal(nested.v[3], 1.5);
  response = Buffer.from([0xc0]);
  await client.execute('db', [4294967296, -2147483649, 1.5, 'x', new Uint8Array([0xcf, 0xff]), Buffer.from([0xd3, 0])]);
  assert.equal(requestBytes[1], 0xcf);
  assert.equal(requestBytes[10], 0xd3);
  const decoded = codec.decode(requestBytes, { useBigInt64: true });
  assert.deepEqual(decoded.slice(0, 4), [4294967296n, -2147483649n, 1.5, 'x']);
  assert.deepEqual([...decoded[4]], [0xcf, 0xff]);
  assert.deepEqual([...decoded[5]], [0xd3, 0]);
});

test('unsafe Numbers and out-of-range BigInts are rejected before native execute', async () => {
  const client = new wrapper.ShamirClient();
  for (const value of [Number.MAX_SAFE_INTEGER + 1, Number.MIN_SAFE_INTEGER - 1,
    -(1n << 63n) - 1n, 1n << 64n]) {
    requestBytes = undefined;
    await assert.rejects(() => client.execute('db', { nested: [value] }), RangeError);
    assert.equal(requestBytes, undefined);
  }
});

test('documented actual TS builder executes through the Node wrapper', async () => {
  response = Buffer.from('81a27631cf0020000000000001', 'hex');
  const example = require('../../node_modules/.builder-test/shamir-client-node/src/tests/builder-example.js');
  const result = await example.builderExample(new wrapper.ShamirClient());
  assert.deepEqual(result, { v1: 9007199254740993n });
  const sent = codec.decode(requestBytes, { useBigInt64: true });
  assert.equal(sent.id, 'rw');
  assert.equal(sent.queries.rd.from, 'items');
  assert.equal(sent.queries.rd.temporal.at.version, 4294967296n);
  assert.notEqual(requestBytes.indexOf(Buffer.from('cf0000000100000000', 'hex')), -1);
});

test('execute decodes typed errors and ordinary success', async () => {
  const client = new wrapper.ShamirClient();
  response = Buffer.from(encode({ result: 3 }));
  assert.deepEqual(await client.execute('db', {}), { result: 3 });
  response = Buffer.from(encode({ kind: 'error', code: 'validation', message: 'bad' }));
  await assert.rejects(() => client.execute('db', {}), error =>
    error instanceof wrapper.ShamirDbError && error.code === 'validation');
});

const SECRET = 'S3CR3T-distinct-value';
const SECRET_BYTES = Buffer.from(SECRET);
const PIN = Buffer.alloc(32, 1);
const errorBuf = () => Buffer.from(encode({ kind: 'error', code: 'token_request_denied', message: 'no' }));
const tokenMap = () => ({
  token: 'tok', id: new Uint8Array(16).fill(5), owner: 'alice', label: 'l', mode: 'read_only',
  channels: 'ws', databases: null, created_at_ns: 5, expires_at_ns: 9007199254740993n,
});

test('new statics return wrapped instances and forward options', async () => {
  const opts = {
    connectWs: { host: 'h', port: 1, serverName: 's', username: 'u', password: SECRET },
    connectPlain: { host: '127.0.0.1', port: 1, username: 'u', password: SECRET },
    connectLocal: { addr: '/tmp/s', username: 'u', password: SECRET },
    resume: { host: 'h', port: 1, serverName: 's', ticket: SECRET_BYTES, trustedPin: PIN },
    resumeWs: { host: 'h', port: 1, serverName: 's', ticket: SECRET_BYTES, trustedPin: PIN },
    connectWsBrowser: { host: 'h', port: 1, serverName: 's', origin: 'https://app.test', username: 'u', password: SECRET },
    resumeWsBrowser: { host: 'h', port: 1, serverName: 's', origin: 'https://app.test', ticket: SECRET_BYTES, trustedPin: PIN },
    resumePlain: { host: '127.0.0.1', port: 1, ticket: SECRET_BYTES, trustedPin: PIN },
    resumeLocal: { addr: '/tmp/s', ticket: SECRET_BYTES, trustedPin: PIN },
    connectTokenWs: { host: 'h', port: 1, token: SECRET },
  };
  for (const [name, o] of Object.entries(opts)) {
    nativeCalls = [];
    const client = await wrapper.ShamirClient[name](o);
    assert.ok(client instanceof wrapper.ShamirClient, name);
    assert.deepEqual(nativeCalls.map(c => c[0]), [name]);
    assert.strictEqual(nativeCalls[0][1], o);
  }
});

test('validation rejects with TypeError, leaks no secret, skips native', async () => {
  const cases = [
    ['resume', { host: 'h', port: 1, serverName: 's', ticket: Buffer.alloc(0), trustedPin: PIN }],
    ['resume', { host: 'h', port: 1, serverName: 's', ticket: SECRET, trustedPin: PIN }],
    ['resumeWs', { host: 'h', port: 1, serverName: 's', ticket: SECRET_BYTES, trustedPin: SECRET_BYTES }],
    ['connectTokenWs', { host: 'h', port: 1, token: '' }],
    ['connectTokenWs', { host: 'h', port: 1, token: 42, caPem: SECRET }],
    ['connectTokenWs', { host: 'h', port: 1, token: SECRET, caPem: 7 }],
    ['connectPlain', { host: '127.0.0.1', port: 1, username: 'u', password: 5 }],
    ['connectPlain', { host: '127.0.0.1', port: 1, username: 'u', password: SECRET, trustedPin: SECRET_BYTES }],
    ['connectLocal', { addr: 5, username: 'u', password: SECRET }],
    ['connectWs', null],
    ['connectWsBrowser', { host: 'h', port: 1, serverName: 's', username: 'u', password: SECRET }],
    ['connectWsBrowser', { host: 'h', port: 1, serverName: 's', origin: 42, username: 'u', password: SECRET }],
    ['connectWsBrowser', { host: 'h', port: 1, serverName: 's', origin: 'https://app.test', username: 'u', password: SECRET, caPem: 7 }],
    ['resumeWsBrowser', { host: 'h', port: 1, serverName: 's', origin: 'https://app.test', ticket: Buffer.alloc(0), trustedPin: PIN }],
    ['resumeWsBrowser', { host: 'h', port: 1, serverName: 's', origin: 'https://app.test', ticket: SECRET_BYTES, trustedPin: Buffer.alloc(31) }],
    ['resumePlain', { host: 'h', port: 1, ticket: SECRET_BYTES, trustedPin: Buffer.alloc(31) }],
    ['resumePlain', { host: 5, port: 1, ticket: SECRET_BYTES, trustedPin: PIN }],
    ['resumeLocal', { addr: 5, ticket: SECRET_BYTES, trustedPin: PIN }],
    ['resumeLocal', { addr: '/tmp/s', ticket: SECRET, trustedPin: PIN }],
  ];
  for (const [name, o] of cases) {
    nativeCalls = [];
    await assert.rejects(() => wrapper.ShamirClient[name](o), error => {
      assert.ok(error instanceof TypeError);
      assert.ok(!error.message.includes(SECRET));
      return true;
    });
    assert.equal(nativeCalls.length, 0);
  }
  await assert.rejects(() => wrapper.ShamirClient.resume(
    { host: 'h', port: 1, serverName: 's', ticket: Buffer.alloc(0), trustedPin: PIN }),
  { message: 'ticket must be a non-empty Buffer' });
  await assert.rejects(() => wrapper.ShamirClient.resume(
    { host: 'h', port: 1, serverName: 's', ticket: SECRET_BYTES, trustedPin: Buffer.alloc(31) }),
  { message: 'trustedPin must be 32 bytes' });
  await assert.rejects(() => wrapper.ShamirClient.connectTokenWs({ host: 'h', port: 1, token: '' }),
    { message: 'token must be a non-empty string' });
  await assert.rejects(() => wrapper.ShamirClient.connectWsBrowser(
    { host: 'h', port: 1, serverName: 's', username: 'u', password: SECRET }),
  { message: 'origin must be a string' });
  await assert.rejects(() => wrapper.ShamirClient.resumeWsBrowser(
    { host: 'h', port: 1, serverName: 's', origin: 'https://app.test', ticket: SECRET_BYTES, trustedPin: Buffer.alloc(31) }),
  { message: 'trustedPin must be 32 bytes' });
  await assert.rejects(() => wrapper.ShamirClient.resumePlain(
    { host: 'h', port: 1, ticket: Buffer.alloc(0), trustedPin: PIN }),
  { message: 'ticket must be a non-empty Buffer' });
  await assert.rejects(() => wrapper.ShamirClient.resumeLocal(
    { addr: '/tmp/s', ticket: Buffer.alloc(0), trustedPin: PIN }),
  { message: 'ticket must be a non-empty Buffer' });
});

test('createApiToken maps the success map and throws typed errors', async () => {
  const client = await wrapper.ShamirClient.connectTokenWs({ host: 'h', port: 1, token: 't' });
  response = Buffer.from(
    encode({ ...tokenMap(), id: 'abcdefghijklmnopqrstuv' }, { useBigInt64: true }),
  );
  const created = await client.createApiToken({ label: 'l' });
  assert.equal(created.token, 'tok');
  assert.equal(created.owner, 'alice');
  assert.equal(created.mode, 'read_only');
  assert.equal(created.channels, 'ws');
  assert.equal(created.databases, null);
  assert.equal(created.createdAtNs, 5n);
  assert.equal(created.expiresAtNs, 9007199254740993n);
  assert.equal(created.id, 'abcdefghijklmnopqrstuv');
  response = errorBuf();
  await assert.rejects(() => client.createApiToken({ label: 'l' }), error =>
    error instanceof wrapper.ShamirDbError && error.code === 'token_request_denied');
  nativeCalls = [];
  await assert.rejects(() => client.createApiToken({ label: 'l', mode: 'admin' }), TypeError);
  await assert.rejects(() => client.createApiToken({ label: '' }), TypeError);
  assert.equal(nativeCalls.length, 0);
});

test('login maps the success map and validates input', async () => {
  response = Buffer.from(encode(tokenMap(), { useBigInt64: true }));
  nativeCalls = [];
  const o = { host: 'h', port: 1, username: 'u', password: SECRET, mode: 'read_only', channels: 'both' };
  const result = await wrapper.login(o);
  assert.strictEqual(nativeCalls[0][1], o);
  assert.equal(result.token, 'tok');
  assert.ok(Buffer.isBuffer(result.id));
  assert.equal(result.id.length, 16);
  assert.equal(result.databases, null);
  assert.equal(result.createdAtNs, 5n);
  assert.equal(result.expiresAtNs, 9007199254740993n);
  response = errorBuf();
  await assert.rejects(() => wrapper.login(o), error =>
    error instanceof wrapper.ShamirDbError && error.code === 'token_request_denied');
  nativeCalls = [];
  const bad = [{ mode: 'admin' }, { channels: 'all' }, { listenerId: Buffer.alloc(15) },
    { databases: 'x' }, { password: 1 }];
  for (const extra of bad) {
    await assert.rejects(() => wrapper.login({ ...o, ...extra }), error =>
      error instanceof TypeError && !error.message.includes(SECRET));
  }
  await assert.rejects(() => wrapper.login({ ...o, mode: 'admin' }),
    { message: 'mode must be read_only or read_write' });
  assert.equal(nativeCalls.length, 0);
});

test('ShamirTokenHttpClient rewraps, encodes and decodes like ShamirClient', async () => {
  nativeCalls = [];
  await assert.rejects(() => wrapper.ShamirTokenHttpClient.connect({ host: 'h', port: 1, token: '' }), TypeError);
  assert.equal(nativeCalls.length, 0);
  const client = await wrapper.ShamirTokenHttpClient.connect({ host: 'h', port: 1, token: SECRET });
  assert.ok(client instanceof wrapper.ShamirTokenHttpClient);
  response = Buffer.from(encode({ result: 3 }));
  assert.deepEqual(await client.execute('db', { n: 4294967296 }), { result: 3 });
  assert.equal(codec.decode(requestBytes, { useBigInt64: true }).n, 4294967296n);
  response = Buffer.from(encode({ kind: 'error', code: 'validation', message: 'bad' }));
  await assert.rejects(() => client.execute('db', {}), error =>
    error instanceof wrapper.ShamirDbError && error.code === 'validation');
});

test('authKind passes through', async () => {
  assert.equal(new wrapper.ShamirClient().authKind(), 'token');
});

test('exports are the wrapped classes and functions', () => {
  assert.ok(wrapper.ShamirClient.prototype instanceof NativeClient);
  assert.notEqual(wrapper.ShamirClient, NativeClient);
  assert.ok(wrapper.ShamirTokenHttpClient.prototype instanceof NativeTokenHttpClient);
  assert.notEqual(wrapper.ShamirTokenHttpClient, NativeTokenHttpClient);
  assert.notEqual(wrapper.login, nativeLogin);
  assert.equal(typeof wrapper.login, 'function');
});
