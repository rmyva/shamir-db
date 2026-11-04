/**
 * Shared e2e test harness for ShamirDB TS client tests.
 *
 * Provides server lifecycle management with ephemeral port allocation,
 * CARGO_TARGET_DIR-aware binary resolution, TLS cert generation,
 * and common helpers (br, uniqueDbName, setupDb, seed, connectAdmin, connectAs).
 */

import { spawn, ChildProcess } from 'node:child_process';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { connect, connectLocal } from '../index.js';
import type { ShamirClient, BatchResponse, WireValue } from '../index.js';
import { ddl, write } from '../index.js';

// ─── server binary path (CARGO_TARGET_DIR-aware) ─────────────────────────────

const _fileUrl = new URL(import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const REPO_ROOT = path.resolve(path.dirname(_fileUrl), '../../../..');

const EXE_NAME = process.platform === 'win32' ? 'shamir-server.exe' : 'shamir-server';

/**
 * Resolve the server binary path.
 *
 * 1. If SHAMIR_SERVER_BIN is set, use it verbatim — an explicit override that
 *    lets a faster `cargo build` (debug) profile feed the e2e suite without a
 *    20-minute release build. Highest priority.
 * 2. If CARGO_TARGET_DIR is set and the release binary exists there, use it.
 * 3. Otherwise fall back to <repo>/target/release/<exe>.
 *
 * Prefers CARGO_TARGET_DIR when both exist — `target/release` may contain
 * a stale binary that lacks recent server features.
 */
export function serverBinPath(): string {
  const explicit = process.env.SHAMIR_SERVER_BIN;
  if (explicit) return explicit;

  const cargoTargetDir = process.env.CARGO_TARGET_DIR;
  if (cargoTargetDir) {
    const candidate = path.join(cargoTargetDir, 'release', EXE_NAME);
    if (fs.existsSync(candidate)) return candidate;
  }
  return path.join(REPO_ROOT, 'target', 'release', EXE_NAME);
}

export const SERVER_BIN = serverBinPath();
export const SERVER_AVAILABLE = fs.existsSync(SERVER_BIN);

// ─── stale-binary guard ──────────────────────────────────────────────────────

/**
 * Walk `crates/` (plus the workspace `Cargo.toml`/`Cargo.lock`) and return
 * the newest `.rs`/`Cargo.toml` mtime found. Used to detect a `shamir-server`
 * binary that predates the current source tree (task #566 — a week-stale
 * binary silently ran pre-#557/#559 behaviour for the whole #552-#560 e2e
 * verification chain, reporting false-positive green runs).
 */
function newestSourceMtimeMs(): number {
  let newest = 0;
  const roots = [
    path.join(REPO_ROOT, 'crates'),
    path.join(REPO_ROOT, 'Cargo.toml'),
    path.join(REPO_ROOT, 'Cargo.lock'),
  ];
  const dirStack: string[] = [];
  for (const root of roots) {
    try {
      const st = fs.statSync(root);
      if (st.isDirectory()) dirStack.push(root);
      else if (st.mtimeMs > newest) newest = st.mtimeMs;
    } catch {
      // missing root — nothing to scan there.
    }
  }
  while (dirStack.length > 0) {
    const dir = dirStack.pop() as string;
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const entry of entries) {
      // Build output / vendored dirs don't feed the source build.
      if (entry.name === 'target' || entry.name === 'node_modules' || entry.name === '.git') {
        continue;
      }
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        dirStack.push(full);
      } else if (entry.name.endsWith('.rs') || entry.name === 'Cargo.toml') {
        const mtime = fs.statSync(full).mtimeMs;
        if (mtime > newest) newest = mtime;
      }
    }
  }
  return newest;
}

/**
 * Throws if `SERVER_BIN` predates the newest Rust source file that feeds it
 * — applies to BOTH the default release-binary path and the
 * `SHAMIR_SERVER_BIN` debug-binary override, since either can silently point
 * at code older than the current working tree. Set
 * `SHAMIR_SKIP_STALE_BINARY_CHECK=1` to bypass (not recommended — defeats
 * the purpose of task #566).
 */
export function assertServerBinaryFresh(): void {
  if (process.env.SHAMIR_SKIP_STALE_BINARY_CHECK === '1') return;
  if (!SERVER_AVAILABLE) return; // a separate check already skips suites when absent
  const binMtime = fs.statSync(SERVER_BIN).mtimeMs;
  const sourceMtime = newestSourceMtimeMs();
  if (sourceMtime > binMtime) {
    throw new Error(
      `stale shamir-server binary: ${SERVER_BIN} was built ${new Date(binMtime).toISOString()}, ` +
        `but Rust source under crates/ was modified ${new Date(sourceMtime).toISOString()} ` +
        `(newer). Rebuild before running e2e tests: cargo build --release -p shamir-server` +
        (process.env.CARGO_TARGET_DIR
          ? ` (CARGO_TARGET_DIR=${process.env.CARGO_TARGET_DIR})`
          : '') +
        `. To bypass (not recommended), set SHAMIR_SKIP_STALE_BINARY_CHECK=1.`,
    );
  }
}

// ─── constants ───────────────────────────────────────────────────────────────

export const HOST = '127.0.0.1';
export const ADMIN_USER = 'admin';
export const ADMIN_PASSWORD = 'correct horse battery staple';
export const ORIGIN = `https://${HOST}`;

// ─── types ───────────────────────────────────────────────────────────────────

export interface ServerHandle {
  stop: () => Promise<void>;
  logs: () => string;
  port: number;
}

// ─── config / cert ───────────────────────────────────────────────────────────

export function writeKtavConfig(
  dir: string,
  opts: {
    host: string;
    port: number;
    origin: string;
    cursors?: { maxCursorsPerSession?: number; idleTimeoutSecs?: number };
    /**
     * Opt the spawned test server into the experimental online
     * storage-migration API (`StartMigration`/`CommitMigration`/
     * `RollbackMigration`/`MigrationStatus`). Defaults to `false`. Only the
     * e2e-ddl migration-lifecycle blocks set this — the feature has known
     * gaps (see KNOWN_LIMITATIONS.md §2) and is disabled by default in every
     * shipped `deploy/*.ktav` profile.
     */
    enableExperimentalMigrationApi?: boolean;
  },
): string {
  const certPath = path.join(dir, 'cert.pem').replace(/\\/g, '/');
  const keyPath = path.join(dir, 'key.pem').replace(/\\/g, '/');
  const cursorsBlock = opts.cursors
    ? `
    cursors: {
        max_cursors_per_session: ${opts.cursors.maxCursorsPerSession ?? 16}
        idle_timeout_secs:       ${opts.cursors.idleTimeoutSecs ?? 60}
    }`
    : '';
  const cfg = `
data_dir: ${dir.replace(/\\/g, '/')}

logging: {
    level: info
    slow_query_threshold_ms: 0
}

kdf_defaults: {
    memory_kb: 19456
    time: 2
    parallelism: 1
    argon2_version: 19
}

argon2_concurrent_max: 4

listeners: [
    {
        kind: ws
        addr: ${opts.host}:${opts.port}
        path: /shamir/v1/browser
        profile: tls_no_export
        browser_origin_allowlist: [
            ${opts.origin}
        ]
    }
]

tls: {
    cert_path: ${certPath}
    key_path:  ${keyPath}
}

security: {
    connection: {
        auth_init_timeout_ms: 10000
        max_active_connections: 100
    }
    ## Default 10/sec (2.5/sec during the 60s post-boot warmup window,
    ## shamir-connect::server::rate_limit) is tuned for production abuse
    ## resistance, not a test harness that opens many new connections in a
    ## tight burst from one subnet (127.0.0.0/24) against a server that is
    ## ALWAYS inside its own warmup window (freshly spawned per test file).
    ## Matches the same workaround already used by the Rust e2e/integration
    ## suites (permission_e2e.rs, slow_loris.rs, slow_loris_pre_tls.rs,
    ## max_connections.rs) -- without this, e2e-permissions.test.ts's
    ## multi-connection setup steps intermittently hit rate_limited-at-accept
    ## and see the socket closed pre-handshake ("connection closed").
    auth_init_rate_per_second: 1000
    query_limits: {
        max_result_size_bytes:    10485760
        max_execution_time_secs:  30
        max_queries_per_batch:    32
    }${cursorsBlock}${opts.enableExperimentalMigrationApi ? '\n    enable_experimental_migration_api: true' : ''}
}

audit: {
    max_file_size_mb: 0
    retention_days: 0
}

observability: {
    addr: 127.0.0.1:0
}
`.trim();

  const configPath = path.join(dir, 'server.ktav');
  fs.writeFileSync(configPath, cfg);
  return configPath;
}

export function generateSelfSignedCert(dir: string): boolean {
  try {
    const { execFileSync } = require('node:child_process') as typeof import('node:child_process');
    execFileSync('openssl', [
      'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
      '-keyout', path.join(dir, 'key.pem'),
      '-out', path.join(dir, 'cert.pem'),
      '-days', '1',
      '-subj', '/CN=localhost',
      '-addext', 'subjectAltName=IP:127.0.0.1,DNS:localhost',
    ], { stdio: 'ignore', timeout: 15_000 });
    return true;
  } catch {
    return false;
  }
}

// ─── server lifecycle ────────────────────────────────────────────────────────

export async function startServer(opts?: {
  port?: number;
  cursors?: { maxCursorsPerSession?: number; idleTimeoutSecs?: number };
  enableExperimentalMigrationApi?: boolean;
}): Promise<ServerHandle> {
  assertServerBinaryFresh();
  // Port 0 unless pinned: the server binds an OS-assigned port itself and
  // logs it, so there is no pick-then-bind window another test file's server
  // (or an outbound ephemeral socket) can steal the port in ("Address already
  // in use").
  const requestedPort = opts?.port ?? 0;
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'shamir-ts-e2e-'));
  const hasCert = generateSelfSignedCert(dataDir);
  if (!hasCert) {
    fs.rmSync(dataDir, { recursive: true, force: true });
    throw new Error('openssl not available — cannot generate self-signed cert');
  }

  const configPath = writeKtavConfig(dataDir, {
    host: HOST,
    port: requestedPort,
    origin: ORIGIN,
    cursors: opts?.cursors,
    enableExperimentalMigrationApi: opts?.enableExperimentalMigrationApi,
  });
  const child = spawn(
    SERVER_BIN,
    ['--config', configPath, '--bootstrap-password', ADMIN_PASSWORD],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  ) as ChildProcess;

  let logBuf = '';
  child.stdout?.on('data', (chunk: Buffer) => { logBuf += chunk.toString(); });
  child.stderr?.on('data', (chunk: Buffer) => { logBuf += chunk.toString(); });

  let port = requestedPort;
  await new Promise<void>((resolve, reject) => {
    let done = false;
    const timer = setTimeout(() => {
      if (!done) {
        done = true;
        reject(new Error(`Server failed to bind within 15s.\nLogs:\n${logBuf}`));
      }
    }, 15_000);

    function check() {
      if (done) return;
      const plain = logBuf.replace(/\x1b\[[0-9;]*m/g, '');
      const boundLine = plain.split('\n').find((l) => /listener bound/i.test(l));
      if (boundLine !== undefined) {
        const bound = /local_addr=\S*:(\d+)/.exec(boundLine);
        // A chunk can end mid-line; with an OS-assigned port wait for the
        // rest (the 15s timer reports the logs if it never parses).
        if (bound === null && requestedPort === 0) return;
        if (bound !== null) port = Number(bound[1]);
        done = true;
        clearTimeout(timer);
        setTimeout(() => resolve(), 150);
      }
    }
    child.stdout?.on('data', check);
    child.stderr?.on('data', check);
    child.on('exit', (code: number | null, signal: string | null) => {
      if (!done) {
        done = true;
        clearTimeout(timer);
        reject(new Error(
          `Server exited prematurely (code=${code} signal=${signal}).\nLogs:\n${logBuf}`,
        ));
      }
    });
  });

  return {
    port,
    stop: async () => {
      if (!child.killed) {
        child.kill(process.platform === 'win32' ? 'SIGKILL' : 'SIGTERM');
        await new Promise<void>((r) => child.once('exit', r));
      }
      try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* ok */ }
    },
    logs: () => logBuf,
  };
}

// ─── multi-listener (browser + native ws + tcp) server ──────────────────────

export interface BoundListener {
  kind: string;
  profile: string;
  port: number;
}

export function parseBoundListeners(log: string): BoundListener[] {
  const plain = log.replace(/\x1b\[[0-9;]*m/g, '');
  const out: BoundListener[] = [];
  for (const line of plain.split('\n')) {
    if (!/listener bound/i.test(line)) continue;
    const addr = /local_addr=\S*:(\d+)/.exec(line);
    const kind = /kind=(\w+)/.exec(line);
    const profile = /profile=(?:Some\()?(\w+)/.exec(line);
    if (addr === null || kind === null || profile === null) continue;
    out.push({ kind: kind[1], profile: profile[1], port: Number(addr[1]) });
  }
  return out;
}

export interface MultiServerHandle {
  tcpPort: number;
  wsNativePort: number;
  browserPort: number;
  caPem: string;
  stop: () => Promise<void>;
  logs: () => string;
}

export function writeMultiListenerKtavConfig(dir: string, origin: string): string {
  const certPath = path.join(dir, 'cert.pem').replace(/\\/g, '/');
  const keyPath = path.join(dir, 'key.pem').replace(/\\/g, '/');
  const cfg = `
data_dir: ${dir.replace(/\\/g, '/')}

logging: {
    level: info
    slow_query_threshold_ms: 0
}

kdf_defaults: {
    memory_kb: 19456
    time: 2
    parallelism: 1
    argon2_version: 19
}

argon2_concurrent_max: 4

listeners: [
    {
        kind: ws
        addr: 127.0.0.1:0
        path: /shamir/v1/browser
        profile: tls_no_export
        browser_origin_allowlist: [
            ${origin}
        ]
    }
    {
        kind: ws
        addr: 127.0.0.1:0
        path: /shamir/v1
        profile: tls_exporter
    }
    {
        kind: tcp
        addr: 127.0.0.1:0
        profile: tls_exporter
    }
]

tls: {
    cert_path: ${certPath}
    key_path:  ${keyPath}
}

security: {
    connection: {
        auth_init_timeout_ms: 10000
        max_active_connections: 100
    }
    ## Tests open many connections in a burst from one subnet during the
    ## post-boot warmup window; the default rate limit would reject them.
    auth_init_rate_per_second: 1000
    query_limits: {
        max_result_size_bytes:    10485760
        max_execution_time_secs:  30
        max_queries_per_batch:    32
    }
}

audit: {
    max_file_size_mb: 0
    retention_days: 0
}

observability: {
    addr: 127.0.0.1:0
}
`.trim();

  const configPath = path.join(dir, 'server.ktav');
  fs.writeFileSync(configPath, cfg);
  return configPath;
}

export async function startMultiListenerServer(): Promise<MultiServerHandle> {
  assertServerBinaryFresh();
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'shamir-ts-e2e-multi-'));
  if (!generateSelfSignedCert(dataDir)) {
    fs.rmSync(dataDir, { recursive: true, force: true });
    throw new Error('openssl not available — cannot generate self-signed cert');
  }

  const configPath = writeMultiListenerKtavConfig(dataDir, ORIGIN);
  const child = spawn(
    SERVER_BIN,
    ['--config', configPath, '--bootstrap-password', ADMIN_PASSWORD],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  ) as ChildProcess;

  let logBuf = '';
  child.stdout?.on('data', (chunk: Buffer) => { logBuf += chunk.toString(); });
  child.stderr?.on('data', (chunk: Buffer) => { logBuf += chunk.toString(); });

  const find = (
    bound: BoundListener[],
    kind: string,
    profile: string,
  ): BoundListener | undefined =>
    bound.find((l) => l.kind === kind && l.profile === profile);

  let ports: { tcp: number; wsNative: number; browser: number };
  try {
    ports = await new Promise<{ tcp: number; wsNative: number; browser: number }>(
      (resolve, reject) => {
        let done = false;
        const timer = setTimeout(() => {
          if (!done) {
            done = true;
            reject(new Error(`Server failed to bind all listeners within 15s.\nLogs:\n${logBuf}`));
          }
        }, 15_000);

        function check() {
          if (done) return;
          const bound = parseBoundListeners(logBuf);
          const browser = find(bound, 'Ws', 'TlsNoExport');
          const wsNative = find(bound, 'Ws', 'TlsExporter');
          const tcp = find(bound, 'Tcp', 'TlsExporter');
          if (browser === undefined || wsNative === undefined || tcp === undefined) return;
          done = true;
          clearTimeout(timer);
          setTimeout(
            () => resolve({ tcp: tcp.port, wsNative: wsNative.port, browser: browser.port }),
            150,
          );
        }
        child.stdout?.on('data', check);
        child.stderr?.on('data', check);
        child.on('exit', (code: number | null, signal: string | null) => {
          if (!done) {
            done = true;
            clearTimeout(timer);
            reject(new Error(
              `Server exited prematurely (code=${code} signal=${signal}).\nLogs:\n${logBuf}`,
            ));
          }
        });
      },
    );
  } catch (e) {
    if (child.exitCode === null && !child.killed) child.kill('SIGKILL');
    try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* ok */ }
    throw e;
  }

  const caPem = fs.readFileSync(path.join(dataDir, 'cert.pem'), 'utf8');

  return {
    tcpPort: ports.tcp,
    wsNativePort: ports.wsNative,
    browserPort: ports.browser,
    caPem,
    stop: async () => {
      if (!child.killed) {
        child.kill(process.platform === 'win32' ? 'SIGKILL' : 'SIGTERM');
        await new Promise<void>((r) => child.once('exit', r));
      }
      try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* ok */ }
    },
    logs: () => logBuf,
  };
}

/** PEM of a freshly generated, unrelated self-signed CA (null without openssl). */
export function readSelfSignedCaPem(): string | null {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'shamir-ts-e2e-ca-'));
  try {
    if (!generateSelfSignedCert(dir)) return null;
    return fs.readFileSync(path.join(dir, 'cert.pem'), 'utf8');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

// ─── connection helpers ──────────────────────────────────────────────────────

export async function connectNativeAs(
  transport: 'tcp' | 'ws-native',
  host: string,
  port: number,
  caPem: string,
  username: string,
  password: string,
): Promise<ShamirClient> {
  return connect({ host, port, username, password, transport, tls: { ca: caPem } });
}

export async function connectAdmin(host: string, port: number): Promise<ShamirClient> {
  return connect({
    host,
    port,
    username: ADMIN_USER,
    password: ADMIN_PASSWORD,
    tls: { rejectUnauthorized: false },
    origin: ORIGIN,
  });
}

export async function connectAs(
  host: string,
  port: number,
  username: string,
  password: string,
): Promise<ShamirClient> {
  return connect({
    host,
    port,
    username,
    password,
    tls: { rejectUnauthorized: false },
    origin: ORIGIN,
  });
}

// ─── data helpers ────────────────────────────────────────────────────────────

/**
 * Unwrap the server's DbResponse envelope.
 */
export function br(raw: object): BatchResponse {
  const env = raw as { kind?: string; response?: BatchResponse };
  if (env.kind === 'batch' && env.response) return env.response;
  if ('results' in raw && 'execution_plan' in raw) return raw as BatchResponse;
  throw new Error(`unexpected response shape: ${JSON.stringify(Object.keys(raw))}`);
}

let dbCounter = 0;

export function uniqueDbName(label: string): string {
  dbCounter += 1;
  return `ts_${label}_${process.pid}_${dbCounter}`;
}

export async function setupDb(
  client: ShamirClient,
  label: string,
  tableNames: string[] = ['items'],
): Promise<string> {
  const db = uniqueDbName(label);

  await client.execute('default', {
    id: `setup-${db}-db`,
    queries: { mk: ddl.createDb(db) },
  });

  const queries: Record<string, object> = { mr: ddl.createRepo('main') };
  for (let i = 0; i < tableNames.length; i += 1) {
    queries[`tb${i}`] = ddl.createTable(tableNames[i], { repo: 'main' });
  }
  await client.execute(db, {
    id: `setup-${db}-tables`,
    queries,
  });

  return db;
}

export async function seed(
  client: ShamirClient,
  db: string,
  table: string,
  records: Array<Record<string, WireValue>>,
  keyFields: string[] = ['id'],
): Promise<BatchResponse> {
  const queries: Record<string, object> = {};
  records.forEach((r, i) => {
    const key: Record<string, WireValue> = {};
    for (const k of keyFields) key[k] = r[k];
    queries[`s${i}`] = write.upsert(table, key, r);
  });
  return br(await client.execute(db, { id: `seed-${db}-${table}`, queries }));
}

// ─── kind: unix (local IPC) server ──────────────────────────────────────────

let ipcCounter = 0;

/**
 * Local-IPC endpoint, unique per call. On Unix, a socket file inside the
 * fresh `dataDir`. On Windows, a Named Pipe name unique per process + call.
 */
function ipcAddr(dataDir: string): string {
  ipcCounter += 1;
  if (process.platform === 'win32') {
    return ['', '', '.', 'pipe', `shamir-ts-e2e-${process.pid}-${ipcCounter}-${Date.now()}`].join(path.win32.sep);
  }
  return path.join(dataDir, `shamir-ts-e2e-${ipcCounter}.sock`);
}

function writeUnixKtavConfig(dir: string, addr: string): string {
  const certPath = path.join(dir, 'cert.pem').split(path.win32.sep).join('/');
  const keyPath = path.join(dir, 'key.pem').split(path.win32.sep).join('/');
  const cfg = `
data_dir: ${dir.split(path.win32.sep).join('/')}

logging: {
    level: info
    slow_query_threshold_ms: 0
}

kdf_defaults: {
    memory_kb: 19456
    time: 2
    parallelism: 1
    argon2_version: 19
}

argon2_concurrent_max: 4

listeners: [
    {
        kind: unix
        addr: ${addr}
        profile: plain
    }
]

tls: {
    cert_path: ${certPath}
    key_path:  ${keyPath}
}

security: {
    connection: {
        auth_init_timeout_ms: 10000
        max_active_connections: 100
    }
    auth_init_rate_per_second: 1000
}

audit: {
    max_file_size_mb: 0
    retention_days: 0
}

observability: {
    addr: 127.0.0.1:0
}
`.trim();

  const configPath = path.join(dir, 'server.ktav');
  fs.writeFileSync(configPath, cfg);
  return configPath;
}

export interface LocalServerHandle {
  addr: string;
  stop: () => Promise<void>;
  logs: () => string;
}

/** Spawn a server with a single `kind: unix`, `profile: plain` listener. */
export async function startUnixServer(): Promise<LocalServerHandle> {
  assertServerBinaryFresh();
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'shamir-ts-e2e-unix-'));
  const addr = ipcAddr(dataDir);
  const configPath = writeUnixKtavConfig(dataDir, addr);

  const child = spawn(
    SERVER_BIN,
    ['--config', configPath, '--bootstrap-password', ADMIN_PASSWORD],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  ) as ChildProcess;

  let logBuf = '';
  child.stdout?.on('data', (chunk: Buffer) => {
    logBuf += chunk.toString();
  });
  child.stderr?.on('data', (chunk: Buffer) => {
    logBuf += chunk.toString();
  });

  await new Promise<void>((resolve, reject) => {
    let done = false;
    const timer = setTimeout(() => {
      if (!done) {
        done = true;
        reject(new Error(`Server failed to bind within 15s.\nLogs:\n${logBuf}`));
      }
    }, 15_000);

    function check() {
      if (done) return;
      if (/listener bound/i.test(logBuf)) {
        done = true;
        clearTimeout(timer);
        setTimeout(() => resolve(), 150);
      }
    }
    child.stdout?.on('data', check);
    child.stderr?.on('data', check);
    child.on('exit', (code: number | null, signal: string | null) => {
      if (!done) {
        done = true;
        clearTimeout(timer);
        reject(
          new Error(`Server exited prematurely (code=${code} signal=${signal}).\nLogs:\n${logBuf}`),
        );
      }
    });
  });

  return {
    addr,
    stop: async () => {
      if (!child.killed) {
        child.kill(process.platform === 'win32' ? 'SIGKILL' : 'SIGTERM');
        await new Promise<void>((r) => child.once('exit', r));
      }
      try {
        fs.rmSync(dataDir, { recursive: true, force: true });
      } catch {
        /* ok */
      }
    },
    logs: () => logBuf,
  };
}

/** Connect over the local IPC transport as `user`. */
export async function connectLocalAs(
  addr: string,
  user: string,
  password: string,
): Promise<ShamirClient> {
  return connectLocal({ addr, username: user, password });
}
