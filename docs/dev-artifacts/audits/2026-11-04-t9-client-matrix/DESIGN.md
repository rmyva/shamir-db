# T9 - handshake x transport matrix for all clients (design)

Base: master `03f7a2bda`. Read-only design by the `oh` reviewer plus orchestrator decisions. Line numbers refer to that base.

## 0. Principles

1. The server knows only the protocol, never the client. "Browser" variants are protocol profiles for platform limits (no TLS exporter, no custom WebSocket headers); any client may use them.
2. Two independent axes: **handshake** (session = SCRAM + resume; token; password-login -> token) and **transport** (TCP+TLS, TCP plain on loopback, WS, HTTP, local IPC; QUIC does not exist in code, mTLS does not exist in code).
3. Every client supports every cell the server protocol admits, except cells physically impossible on its platform (browser: no TCP, no IPC, no TLS exporter, no `Authorization` header on WebSocket).
4. Before any feature work, the layers are separated by a behaviour-preserving pre-refactor (T9-1, T9-1b). Out of T9: JSON bodies for REST (REST is MessagePack only) and mTLS.

## 1. Verified matrix

### 1.1 Server listeners

| Listener | binding_mode | Handshake | Code |
|---|---|---|---|
| `tcp`, `tls_exporter` | 0x01 | session, resume | `listeners.rs:250-254` |
| `tcp`, `plain` (loopback only) | 0x00 | session, resume | `listeners.rs:255-257`, `config.rs:731-737` |
| `ws`, `tls_exporter`, path `/shamir/v1` | 0x01 | session, resume | `listeners.rs:258-262` |
| `ws`, `tls_no_export`, path `/shamir/v1/browser`, Origin required | 0x02 | session, resume | `listeners.rs:263-270`, `config.rs:740-746` |
| `unix` (Unix socket / Named Pipe), `plain` | 0x00 | session, resume | `listeners.rs:220-235` |
| `http`: `POST /shamir/v1/request`, `GET /shamir/v1/ping` | - | token | `service.rs:87-89` |
| `http`: WS `/shamir/v1/ws` (Authorization header) | - | token | `http_ws/route.rs:131-135` |
| `http`: WS `/shamir/v1/ws/browser` (token in the first frame) | - | token | `browser/first_frame.rs` |
| `http`: `/shamir/v1/auth/begin`, `/finish` | - | password -> token | `service.rs:53` |

The profile belongs to the listener: the client must send the `binding_mode` of the listener (`handshake.rs:239-256`, before Argon2).

### 1.2 Cells outside the protocol (intentional)

- token x TCP / WS(`kind: ws`) / IPC: `TOKEN_API.md:22` ("listener `kind: tcp|ws|unix` does not accept API tokens"); a self-signed certificate on those listeners is justified only by the session Ed25519 pin, which a token client does not have.
- session x HTTP: `ADMIN_UI_HOSTING.md:7`, `TOKEN_LOGIN_HTTP.md:3`; SCRAM is a duplex exchange bound to a connection.
- QUIC and mTLS: not in code.

### 1.3 Client support today

See the draft table in section 5 (also the source of the documentation page).

## 2. T9-1 pre-refactor (behaviour unchanged)

Model: three layers and one link. **Transport** dials, does TLS, frames bytes/messages and yields `ChannelBinding {transport_kind, binding_mode, exporter[32]}`. **Handshake** works only on frames + binding (SCRAM, resume, bearer-WS, HTTP login) and never opens sockets. **Mux** (rid demux, push, lifecycle) sits on frames.

### 2.1 Rust `crates/shamir-client`

Findings: wire structs `WireAuthInit..WireResumeOk` live in `client/transport.rs:617-718`; the SCRAM code is duplicated in `connect.rs:396-485` (TCP) and `531-600` (IPC); resume works only over TLS (`connect.rs:638-678`).

Boundary ("behaviour does not change"): public signatures byte for byte - `Client::connect`, `connect_host`, `connect_local`, `resume`, `resume_secure`, the option structs (public fields, no `non_exhaustive`, so no field may be added), `bound_until`, `resolve_host`, `ClientError`, `CloseCause`, getters in `api.rs`, re-exports in `lib.rs`. Frame bytes, error texts (`"send auth_init"`, `"read challenge"`, ...), timeout phase names, no `handshake_timeout` for `connect_local`, Argon2 in `spawn_blocking`, `Zeroizing` of password and ticket, the ticket is sent only after the resume proof. Guard tests are NOT edited (`git diff --stat` on them is empty): all of `src/tests/**`, `client/subscription/tests`, `tests/{smoke.rs,smoke_local.rs,hostname,batch,cursor}`, the server e2e that use `shamir_client`, `examples/connect.rs`, `benches/roundtrip.rs`. Tests import `crate::client::transport::{Wire*, HookPoint, Lifecycle, ClosedState, await_pending_response, connect_with_timeout}` and `crate::client::{reader_task, ShutdownGuard, ...}`: keep those paths via re-exports.

Target layout (`client/` keeps at most 7 entries; `shamir-client/src` and `src/tests` are already at 7 - no new entries there):

```
client/
  mod.rs         re-exports only (public and test paths unchanged)
  api.rs, cursor_stream.rs, subscription/   unchanged (api.rs: write_prereserved + shutdown call sites)
  transport/     AXIS 1 (replaces transport.rs; path crate::client::transport kept)
    mod.rs binding.rs frame_io.rs dial.rs        [T9-2 ws.rs] [T9-3 http.rs, bearer_ws.rs]
  handshake/     AXIS 2
    mod.rs wire.rs scram.rs auth_ok.rs resume.rs [T9-3 bearer.rs] [T9-4 login.rs]
  session/       Client = transport x handshake x mux
    mod.rs client.rs entry.rs options.rs mux/(lifecycle.rs reader.rs hooks.rs)
```

Interfaces (`Client` stays one concrete type; the writer is an enum, the reader a generic parameter of the reader task, as today):

```rust
pub(crate) struct ChannelBinding { transport: TransportKind, mode: BindingMode, exporter: [u8; 32] }
pub(crate) trait FrameRead: Send + Unpin + 'static { /* read_frame_into(max, buf) */ }
impl<T: AsyncRead + Unpin + Send + 'static> FrameRead for T {}   // keeps reader_task(duplex, ..) tests unchanged
pub(crate) enum WriteSink { Tls(..), Ipc(..), #[cfg(test)] Test(..) }  // T9-2: Ws, T9-3: BearerWs
// methods write_frame / write_prereserved / shutdown
pub(crate) struct Dialed<R: FrameRead> { read: R, write: WriteSink, binding: ChannelBinding }
pub(crate) async fn run_scram<R: FrameRead>(d: &mut Dialed<R>, creds, phase) -> Result<HandshakeOutcome, ClientError>;
pub(crate) async fn resume_wire_exchange<R: FrameRead>(d: &mut Dialed<R>, ticket, pin) -> Result<ResumedHandshake, ClientError>;
// binding_mode and exporter come from d.binding, not from constants (connect.rs:738)
```

PR steps (each passes CI alone). **Order amendment (orchestrator):** R2 first, then R1, then R3, because `client/` already holds exactly 7 entries (`api.rs connect.rs cursor_stream.rs mod.rs options.rs subscription/ transport.rs`): R2 removes `connect.rs`/`options.rs` and adds `session/`, which frees the slots for `handshake/` (R1) and `transport/` (R3, replacing `transport.rs`). Briefs: `docs/dev-artifacts/prompts/t9/`.
- **R1 handshake:** create `handshake/` (wire, auth_ok, resume, scram) on top of `AsyncRead/AsyncWrite`; remove the SCRAM duplicate; add a unit test under `src/tests/wire/` (SCRAM over `tokio::io::duplex` with an emulated server for bindings (Tcp, TlsExporter, exporter) and (Unix, None, zeros); must not be `cfg(unix)`).
- **R2 session and mux:** create `session/` (client, entry, options, mux); old paths re-exported.
- **R3 transport seams:** `transport/{binding,frame_io,dial}.rs`; `WriteSink` loses `impl AsyncWrite` and gets methods; `reader_task` and `assemble` take `R: FrameRead`; dial returns `ChannelBinding`.

Checks without local runs: CI only (draft = fast tier; `[ci full]` marker alone on a line of the head commit = full tier). Manual diff checks: guard tests untouched, `pub fn|pub struct` lines in `lib.rs` and options unchanged, no `Cargo.toml` change. Registering a rust-guard conservation campaign is optional; start without it and add one only if the `structure` gate cannot accept the moves.

### 2.2 TypeScript `crates/shamir-client-ts`

Findings: `connect` and `resume` are hard-wired to `wss://.../shamir/v1/browser` (`client.ts:165, 285`); `runHandshake` defaults to `TRANSPORT_KIND_WS` and `BINDING_MODE_TLS_NO_EXPORT` (`protocol.ts:105-106`); `connectLocal` duplicates `connect` (`client.ts:219-267`); `changePassword` hard-codes the browser binding (`client-internals.ts:428-440`) while the server builds `auth_message_cp` from the real session parameters (`changepw.rs:140-150`) - after `connectLocal`, a password change very likely fails with `AuthFailed` (read from code, unproven; fix it in S4 with a red e2e first). Layout: `client.ts` is 993 lines; `src`, `core`, `core/client`, `__tests__`, `__tests__/connection` are at 7 entries.

Boundary: exports of `index.ts` and `browser.ts`, signatures of `connect`, `connectLocal`, `resume`, the static `ShamirClient.connect/connectLocal/resume(platform, opts)`, types `ConnectOptions`, `ConnectLocalOptions`, `ResumeOptions` stay. The token path (`entry/*`, `platform/token/*`, `core/protocol/token/*`, `login/*`) is not touched; its tests staying green is the proof. Guard tests: `__tests__/connection/{connect,resume,client-demux}.test.ts`, `core/client/__tests__/{client,connect-local}.test.ts`, `platform/__tests__/{node-ipc-socket,ws-subprotocol}.test.ts`, `platform/connect/__tests__/public-connect.test.ts`, `core/protocol/__tests__/*`; e2e: `e2e-connect-local`, `e2e-resume`, `e2e-subscriptions`, `auth/e2e-change-password`, data/query e2e.

Target interfaces (new `core/protocol/session/`, `platform/session/`):

```ts
interface ChannelBinding { transportKind: 0x01|0x02|0x03; bindingMode: 0x00|0x01|0x02; exporter: Uint8Array /*32*/ }
type SessionEndpoint =
  | { kind: 'ws-browser'; url; origin; tls } | { kind: 'ws-native'; url; tls }   // native: Node only
  | { kind: 'tcp'; host; port; tls } | { kind: 'ipc'; path };                    // Node only
interface SessionLink { socket: Socket; binding: ChannelBinding }
interface SessionDialer { dial(ep: SessionEndpoint, timeoutMs: number): Promise<SessionLink> } // unsupported -> 'unsupported_on_platform'
type SessionHandshake = { kind: 'scram'; username; password } | { kind: 'resume'; ticket; serverPubKey };
function openSession(p: Platform, link: SessionLink, hs: SessionHandshake): Promise<SessionOpened>; // data for the constructor, no import cycle
```

`Platform` keeps only crypto; `openSocket` and `openIpcSocket` become details of the dialers. `NodeIpcSocket` generalises to `NodeStreamSocket` (old export kept).

PR steps: **S0** move `__tests__/connection/{connect,resume,e2e-connect-local,e2e-resume}` into `connection/session/` (pure move; frees slots); **S1** `ChannelBinding`, `SessionEndpoint`, `SessionLink` types, `runHandshake` takes a binding, `ShamirClient` stores it, unit test on `auth_init` fields for browser and ipc; **S2** move bodies of `connect/connectLocal/resume` into `session/open.ts`, `session/resume.ts` (client.ts ~760 lines); **S3** `platform/session/{node-dialer,browser-dialer}.ts`; **S4** (a fix, not refactor) `changePassword` uses the session binding (red e2e over `connectLocal` first). TS e2e are outside the merge gate: run `gh workflow run ci.yml --ref <branch>` and `gh workflow run ts-e2e-nightly.yml --ref <branch>` per TS PR.

### 2.3 napi

Unchanged in T9-1: option mapping, secret protection (`secrets.rs`), promises and errors only; no protocol logic. Guard: `cargo check -p shamir-client-node` and the generated-file drift check in CI.

## 3. T9-2 .. T9-6

**T9-2 Rust session x WS and TCP plain.** TLS 1.3. Native: `TlsTrust::AppAuthenticated`. Browser profile 0x02 has no channel binding: it REQUIRES `WebPki` trust. Upgrade `GET <path>` with `Sec-WebSocket-Protocol: shamir-v1` (mandatory; 400 otherwise, wrong path 404). Native: no Origin; browser profile: Origin required and in the allowlist. Exporter label `EXPORTER-ShamirDB-AUTH-v1`, empty context, 32 bytes (same as TCP, so tickets move between TCP and WS). Frame = one BINARY message `[u32_be len][msgpack]`, `len` equals body length minus 4; TEXT is an error; 16 MiB limit, 4 KiB before auth. The server pings and closes with 4003 without a pong: the reader must keep polling. Implementation: `tokio_tungstenite::client_async_with_config` over an existing `TlsStream`, exporter taken before the upgrade; `ReadSource::Ws`, `WriteSink::Ws`; new `Client::connect_ws(WsConnectOptions)` (profiles `Native` / `Browser {origin}`), resume over WS, `Client::connect_plain` (loopback, 0x00). `shamir-transport-ws` as a path dependency, no new crates.

**T9-3 Rust token x REST and token x WS.** REST: `POST /shamir/v1/request`, exactly one `Authorization: Bearer shk1....`, `Content-Type: application/msgpack`, body = msgpack `DbRequest`; `GET /shamir/v1/ping`; HTTP/1.1 only, TLS 1.3 with ALPN `http/1.1`, mandatory chain and hostname verification, no redirects/cookies/query credentials. Statuses 400/401(`WWW-Authenticate: Bearer`)/403/404/405/408/413/415/429/503. No TxBegin/Execute/Commit/Rollback, cursors or subscriptions over REST (`not_supported_over_rest`): a separate `TokenHttpClient` (`ping`, `request`, `execute`, `close`). Token WS native route: `GET /shamir/v1/ws` with Authorization and exactly one `Sec-WebSocket-Protocol: shamir-v1`; the server first sends `{ok:true, sid:bin32, expires_at_ns}`; then each BINARY message is a raw msgpack envelope `{sid, rid, req}` without the u32 prefix (`BearerWs` strips it in `write_prereserved`). Close codes: 4003 heartbeat, 4010 wrong sid, 4011 token revoked, 4012 expired, 1013 busy; add `CloseCause::PeerClosed {code, reason}`. Secrets: `ApiToken(Zeroizing<String>)` validated by `shamir_connect::server::auth::token::format`, redacted `Debug`, `HeaderValue::set_sensitive(true)`, never in errors or tracing. HTTP client: decision deferred to the T9-3 brief (see section 6).

**T9-4 Rust password login -> token.** `POST /shamir/v1/auth/begin` then `/finish` (`TOKEN_LOGIN_HTTP.md` 1.1-1.2); transcript/scope from `shamir_connect::server::auth::http_login::{build_transcript, encode_scope, build_server_signature}`. The client verifies the server: expected `security` class derived from the URL and out-of-band policy (direct HTTPS 0x01, HTTPS via proxy 0x02, loopback HTTP 0x00; never trust the class from the challenge alone); `listener_id` pin if configured; Argon2id within client limits; constant-time `HMAC(server_key, T) == server_signature` BEFORE returning the token (on mismatch wipe the token and fail); the returned `id` must equal the id inside the token. No Ed25519 pin: TLS authenticates the server.

**T9-5 napi.** Expose `connectLocal`, `resume` (via `resume_secure`, ticket as Buffer), `connectWs`, `connectTokenWs`, class `ShamirTokenHttpClient`, `login()`, getter `authKind()`. Split `lib.rs` (527 lines) into `src/{client,token,login}.rs`. Generated `native/index.{js,d.ts}`: CI checks drift and local builds are banned, so add an `if: failure()` step uploading the generated files as an artifact and commit them from it (workflow edit belongs to this task only). Token string wrapped in `secrets::protect` immediately; no `derive(Debug)` on `#[napi(object)]`; errors never echo input; documented that the JS string cannot be wiped.

**T9-6 TS Node session x TCP and native WS.** `tls.connect({minVersion:'TLSv1.3'})`; WS native via `ws` with `['shamir-v1']` and no origin; exporter via `exportKeyingMaterial(32, 'EXPORTER-ShamirDB-AUTH-v1', Buffer.alloc(0))` (empty vs absent context is identical in TLS 1.3; prove equality with rustls in e2e); resume for 0x01 with `resume_proof` (Ed25519 via the existing `@noble/ed25519`); `changePassword` with the real exporter; defaults `rejectUnauthorized: true` plus `tls.ca` (no `identity_sig` pin check in TS, task #622). No new server paths or listeners. The e2e harness starts only the browser listener and parses the first `listener bound` line (`e2e-harness.ts:190-200, 312-314`): teach it several listeners.

## 4. Order, risks, tests

Order: D0 docs (any time); Rust R1 -> R2 -> R3, then T9-2 and T9-3 in parallel, T9-4 after T9-3, T9-5 after T9-2..T9-4; TS S0 -> S1 -> S2 -> S3 -> S4, then T9-6. The TS branch is independent of Rust.

Sizes: T9-1 large in total (R1-R3 medium; S0-S4 small/medium); T9-2 medium; T9-3 large; T9-4 medium; T9-5 medium; T9-6 medium to large.

Risks: token privacy (HTTPS without verification; `http://` off loopback needs an explicit flag like the server; Debug/errors/logs); listener limits in CI (1000 connections, 100 per IP; 8 token sockets per token, 32 per owner; pre-auth 256 and 8 per subnet; Argon2 semaphore - fast KDF and separate tokens in tests); layout (folders at 7, `client.ts` 993 lines); rust-guard visibility/test-discovery; napi size (`opt-level = "z"`) grows with tungstenite and hyper; WebPKI against a self-signed test certificate (`CaUsedAsEndEntity`); TS e2e outside the merge gate; `ClientError` is not `non_exhaustive` (crate is `publish = false`, alpha).

Tests: Rust `crates/shamir-client/tests/matrix/` (`main.rs`, `common.rs`, `session_ws.rs`, `resume.rs`, `token_http.rs`, `token_ws.rs`, `login.rs`): ping/execute/push per cell; ticket TCP -> WS allowed, 0x01 -> browser rejected; Origin in the browser profile; uniform 401; 403 on a wrong channel; `not_supported_over_rest`; 4011 on revoke, 4012 on a short TTL; login success and wrong password; forged `server_signature` and class mismatch as a unit test with an emulated server in `src/tests/wire/`. Integration tests run in the full CI tier only; the server does not change. TS: `__tests__/connection/session/{e2e-tcp-native,e2e-ws-native,e2e-change-password-ipc}.test.ts`, run through the nightly workflow dispatch.

## 5. Documentation (T9-7)

Canonical page: `docs/guide-docs/guide/client-matrix.md`, linked from `guide/README.md`; never duplicate the table, only link to it from: protocol spec README ("Architecture": server cells and why the three are forbidden), `guide/token-api.md` ("Browser and transport choices"), `KNOWN_LIMITATIONS.md` (cells with status "no", TS without pin #622, password change over IPC until S4, no QUIC/mTLS), `crates/shamir-client-ts/docs/README.md`, `TRANSPORT_SPEC.md` (stale: browser path only), `TOKENS.md`, new `crates/shamir-client/README.md` and `crates/shamir-client-node/README.md`.

Statuses: `yes (test: <path>)`, `planned T9-n`, `no` (gap, not planned), `not needed` (reason), `impossible` (physical reason), `outside the protocol` (link). A cell turns to `yes` only in the PR that carries the e2e proving it. Updates: T9-1 none except S4; T9-2 Rust rows 1b, 2, 3, 6b-6d; T9-3 Rust rows 7, 8; T9-4 Rust row 10; T9-5 the whole napi column; T9-6 TS Node rows 1, 2, 6a, 6b.

Draft table:

| # | Handshake x transport (listener) | Server | Rust | napi | TS Node | TS browser |
|---|---|---|---|---|---|---|
| 1 | session x TCP+TLS (`tcp/tls_exporter`, 0x01) | yes | yes (`connect`, `connect_host`) | yes (`connect`) | planned T9-6 | impossible: no TCP |
| 1b | session x TCP plain (loopback, 0x00) | yes | planned T9-2 | planned T9-5 | no | impossible |
| 2 | session x WS native (`ws/tls_exporter`, `/shamir/v1`, 0x01) | yes | planned T9-2 | planned T9-5 | planned T9-6 | impossible: no exporter |
| 3 | session x WS browser profile (`ws/tls_no_export`, `/shamir/v1/browser`, 0x02, Origin) | yes | planned T9-2 | planned T9-5 | yes (`connect`) | yes (`connect`) |
| 4 | session x IPC (`unix/plain`, 0x00) | yes | yes (`connect_local`) | planned T9-5 | yes (`connectLocal`; password change: S4) | impossible |
| 6a | resume x TCP+TLS | yes | yes (`resume`, `resume_secure`) | planned T9-5 | planned T9-6 | impossible |
| 6b | resume x WS native | yes | planned T9-2 | planned T9-5 | planned T9-6 | impossible |
| 6c | resume x WS browser profile | yes | planned T9-2 | planned T9-5 | yes (`resume`, no resume_proof) | no: not exported |
| 6d | resume x TCP plain | yes | planned T9-2 | planned T9-5 | no | impossible |
| 6e | resume x IPC | yes (`ipc_resume_e2e`) | no | no | no | impossible |
| 7 | token x REST (`http`) | yes | planned T9-3 | planned T9-5 | yes | yes |
| 8 | token x WS native (`/shamir/v1/ws`, Authorization) | yes | planned T9-3 | planned T9-5 | yes | impossible: no header on WebSocket |
| 9 | token x WS browser (`/shamir/v1/ws/browser`, first frame) | yes | not needed | not needed | not needed | yes |
| 10 | password login -> token (`/auth/begin`, `/finish`) | yes | planned T9-4 | planned T9-5 | yes (`login`) | yes (`login`) |
| - | token x TCP / IPC / `kind: ws` | outside the protocol | - | - | - | - |
| - | session x HTTP | outside the protocol | - | - | - | - |
| - | QUIC, mTLS | not in code | - | - | - | - |

## 6. Orchestrator decisions on the open questions

1. **HTTP client in Rust (T9-3).** Decided at the T9-3 brief, not now (it does not block the pre-refactor). Leaning: `hyper` 1.x with features `client` and `http1` (new crates `want`, `try-lock`, both MIT and allowed by `deny.toml`, must pass the 7-day cooldown) over a hand-written HTTP/1.1 on `httparse`. No version bump of any existing dependency.
2. **TS Node `connect()` default.** Non-breaking: keep the browser-profile default; add an explicit `transport` option in T9-6.
3. **Extra cells the protocol admits but T9 does not schedule** (resume x IPC for all clients, TCP plain for napi and TS Node, browser resume export, `identity_sig` pin in TS #622): collected as a follow-up task T9-8.
4. **Browser profiles for native clients.** Session x WS browser profile: yes (useful behind a TLS-terminating proxy that breaks the exporter). Token x WS browser route for native clients: no.
5. **napi generated files.** Artifact upload on drift in the `node-binding` job (workflow edit inside T9-5); no local `napi build`.

## T9-1b S0-S2 report

Status: DONE as written, UNVERIFIED by any compiler or test run (no local builds by policy); verification is CI only (`ci.yml` full tier and `ts-e2e-nightly.yml` via dispatch on the branch).

- **S0:** `connect.test.ts`, `resume.test.ts`, `e2e-connect-local.test.ts`, `e2e-resume.test.ts` were copied to `crates/shamir-client-ts/src/__tests__/connection/session/` with only relative import paths fixed (`../../index.js` to `../../../index.js`, `../../core/...` to `../../../core/...`, `../e2e-harness.js` to `../../e2e-harness.js`, REPO_ROOT depth one level deeper in `connect.test.ts`). The four old copies were removed by the orchestrator (pure renames); `__tests__/connection` has 4 entries.
- **S1:** new `core/protocol/session/{binding.ts,endpoint.ts,index.ts}` (`ChannelBinding`, `browserWsBinding()`, `ipcBinding()`, `SessionEndpoint`, `SessionLink`). `runHandshake` takes `ChannelBinding | number` (legacy numeric `transportKind, bindingMode` form kept, so `core/protocol/__tests__/protocol.test.ts` is unchanged); the exporter is NOT added to the SCRAM auth_message (wire identical). `ShamirClient` stores the binding in the private field `_channelBinding` (no new public member).
- **S2:** bodies of `connect`, `connectLocal`, `resume` moved to `core/protocol/session/{open.ts,resume.ts}` (`openSession`, `openScramSession`, `openLocalSession`, `resumeSession`) returning the plain `SessionOpened` (`opened.ts`); the statics in `client.ts` are thin delegates through a private `fromOpened`. `client.ts` went from 993 to 777 lines. Import graph: `client.ts` -> `protocol/session/index.ts` -> `{open,resume}.ts` -> `{protocol.ts, framing.ts, client/client-internals.ts, types/connection.ts, binding.ts}`; no session file imports `client.ts`, so there is no cycle.
- **Tests added:** `core/protocol/session/__tests__/binding.test.ts` (profile constants, fresh exporter arrays, `auth_init.binding_mode` for the browser, ipc and legacy numeric forms; fake socket, no network).
- **Not touched:** `src/index.ts`, `src/browser.ts`, `core/protocol/scram.ts`, token path, `package.json`, lockfiles, workflows. `changePassword` still hard-codes the browser binding (S4, out of scope).
- **Layout:** `python -I scripts/ci/check_layout.py` reports OK.
- **Known caveats:** `_channelBinding` is stored but not yet read (S4 will use it). `resume` yields `normalizedUser: undefined` exactly as before.

## T9-1 R2 report (Rust client session + mux move, behaviour unchanged)

Status: DONE in code. No local compile/test was run (policy); verified by a compiler-style read plus `cargo fmt -p shamir-client -- --check` and `python -I scripts/ci/check_layout.py`.

What moved (bodies verbatim, only imports and visibility adjusted):
- `client/connect.rs` -> `client/session/client.rs` (`SessionMeta`, `struct Client`, `Client::assemble`, the `cfg(test)` accessors) and `client/session/entry.rs` (`connect`, `connect_until`, `connect_host`, `handshake`, `connect_local`, `resume`, `resume_secure`, `verify_auth_ok`, `resume_wire_exchange`, `verify_resume_identity_proof`, `ResumedHandshake`, the `#[path]` link to `interner_cache/resume/security.rs`, now `../../interner_cache/resume/security.rs`).
- `client/options.rs` -> `client/session/options.rs` (byte-identical).
- From `client/transport.rs` -> `client/session/mux/lifecycle.rs` (`PendingSender`, `ClosedState`, `PendingRegistry`, `PendingMap`, `Lifecycle`, `ShutdownGuard`, `LifecycleHooks`), `mux/reader.rs` (`DemuxResult`, `IncomingEnvelope`, `decode_frame`, `await_pending_response`, `reader_task`), `mux/hooks.rs` (`HookPoint`, test-only).
- `client/transport.rs` keeps `WriteSink`, `connect_tcp`, `connect_ipc`, `connect_with_timeout` and all `Wire*` structs, and re-exports `HookPoint` (cfg(test)), `ClosedState`, `Lifecycle`, `LifecycleHooks`, `ShutdownGuard`, `await_pending_response`, `reader_task`, so every `crate::client::transport::{..}` path still resolves. `client/mod.rs` keeps every public and `pub(crate)` path.

Visibility widenings (all within the crate, none public): the `Client` fields were `pub(super)` (scope `client`) and are now `pub(in crate::client)` (same scope, path spelled out because the struct moved one level down); `resume_security` was `pub(super)` and is now `pub(in crate::client)`; `session::entry` is `pub(in crate::client)`; `session::mux` is `pub(crate)`. The effective visibility scope is unchanged.

Dropped: the `pub(super) use resume_security::deserialize_resume_ticket;` re-export (the `WireResumeOk` serde attribute now names `crate::client::session::entry::resume_security::deserialize_resume_ticket` directly).

Public API untouched: `lib.rs` re-exports, options file byte-identical. Guard tests untouched.

DONE by the orchestrator: `connect.rs` and `options.rs` were removed (renames into `session/`); `client/` has 6 entries.

Not verified locally: compilation, clippy, tests (CI only).

## T9-1b S4 report

Status: DONE in code, UNVERIFIED by any compiler or test run (no local builds by policy); verification is CI only (`ci.yml` full tier and `ts-e2e-nightly.yml` dispatched on the branch; the new IPC e2e is the red/green proof).

Step 0 finding (read from code): the server builds `auth_message_cp` in `crates/shamir-connect/src/server/auth/changepw.rs` (`verify_change_password_request_with_sid`, lines 140-150) from the SESSION's `transport_kind`, `binding_mode` and `channel_binding_at_auth`, which the handshake snapshots at session creation (`handshake.rs`: `transport_kind` from the listener, `binding_mode` from `auth_init`, `tls_exporter_or_zeros`). The client's `buildAuthMessageCp` (`core/protocol/scram.ts`) takes exactly these three inputs (`transportKind`, `bindingMode`, `channelBindingAtAuth`), so it can express them. The server therefore DOES depend on the session transport; the old hard-coded WS values only matched the browser-WS session, and a `connectLocal` session (UNIX / NONE) signed a different message.

Files changed (crates/shamir-client-ts/src):
- `core/client/client-internals.ts`: `changePassword` takes `binding: ChannelBinding | undefined` (after `normalizedUser`), refuses with the existing message when `normalizedUser` or `binding` is undefined (resumed client), and uses `binding.transportKind`, `binding.bindingMode`, `binding.exporter`; unused `TRANSPORT_KIND_WS` / `BINDING_MODE_TLS_NO_EXPORT` imports removed.
- `core/client/client.ts`: the `changePassword` method passes `this._channelBinding` (public signature unchanged).
- `core/protocol/session/__tests__/change-password-binding.test.ts` (new): fake-platform unit test; WS profile proof equals the legacy hard-coded wire, IPC profile uses UNIX/NONE, the two differ, undefined binding / user rejects without sending.
- `__tests__/e2e-harness.ts`: appended `LocalServerHandle`, `startUnixServer`, `connectLocalAs` (existing exports unchanged; `connectLocal` added to the index import).
- `__tests__/auth/e2e-change-password-ipc.test.ts` (new): change password over `connectLocal` (old pw rejected, new pw works) and wrong old password rejected.

Layout: `python -I scripts/ci/check_layout.py` reports OK. Not touched: token path, `login/`, `package.json`, lockfiles, workflows. Note: `e2e-connect-local.test.ts` still has its own file-local copy of the unix launcher (not refactored to the shared harness helper).

## T9-1 R1 report (Rust client handshake module, one SCRAM for every transport)

Status: DONE in code. No local compile/test was run (policy); checked by a compiler-style read plus `cargo fmt -p shamir-client -- --check` and `python -I scripts/ci/check_layout.py`. Verification is CI only.

What moved (bodies verbatim, only imports and visibility adjusted):
- `client/transport.rs` -> `client/handshake/wire.rs`: all eight `Wire*` structs. `transport.rs` re-exports them under `#[cfg(test)]` (non-test code no longer uses that path), so `crate::client::transport::Wire*` still resolves for the guard tests.
- `client/session/entry.rs` -> `client/handshake/auth_ok.rs` (`VerifiedAuthOk`, `verify_auth_ok`) and `client/handshake/resume.rs` (`ResumedHandshake`, `resume_wire_exchange`, `verify_resume_identity_proof`).
- New `client/handshake/scram.rs`: `run_scram<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(r, w, ScramRequest) -> ScramOutcome`, the single SCRAM exchange. `ScramRequest` carries username, `TransportKind`, `BindingMode`, exporter `[u8; 32]`, trusted pin, accept-new-host, password slice and `phase: Option<&AtomicU8>`. `Client::handshake` (TCP+TLS) and `Client::connect_local` (IPC) now both do: dial (unchanged) -> `run_scram` -> `Client::assemble` with `session_meta(outcome, request_timeout)`.
- `client/` has 7 entries: `api.rs cursor_stream.rs handshake/ mod.rs session/ subscription/ transport.rs`; `handshake/` has 5.
- New guard test `src/tests/wire/scram_binding_tests.rs` (`run_scram` over `tokio::io::duplex` against an emulated `ServerHandshake`; bindings `(Tcp, TlsExporter, [0x5A; 32])` and `(Unix, None, zeros)`; asserts the `auth_init.binding_mode` byte, completion, session id and `server_query_version`; not `cfg(unix)`).

Differences found between the two old SCRAM copies (each preserved as a parameter):
1. Phase tracking: only the TCP copy stored `PHASE_SCRAM` (start), `PHASE_KDF` (before `spawn_blocking`) and `PHASE_SCRAM` (after the join); IPC stored none -> `ScramRequest::phase` is `Some` for TCP, `None` for IPC (same three store points).
2. Channel binding: TCP `(Tcp, TlsExporter)` + `.tls_exporter(exporter)`; IPC `(Unix, None)` with the builder default (all zeros) -> now explicit parameters, IPC passes `[0u8; 32]` (identical to the default).
3. Order: TCP built the `HandshakeBuilder` before `split(tls)`; now `split` happens first (no I/O in between, `PHASE_SCRAM` is still stored before any frame I/O).
4. Writer variant: `WriteSink::Tls(w)` vs `WriteSink::Ipc(w)` stay in the call sites. `connect_local` still has no `handshake_timeout`. Error strings, `MAX_PRE_AUTH_FRAME`, Argon2 in `spawn_blocking`, `Zeroizing` of password and ticket unchanged. The two `SessionMeta` constructions were identical and are now `session_meta()` in `entry.rs`. Everything else (comments aside) was identical.

Visibility widenings vs base (all within the crate, none public): `VerifiedAuthOk` and its fields private -> `pub(crate)`; `verify_auth_ok` private -> `pub(super)` (inside `handshake`); `PHASE_SCRAM`/`PHASE_KDF` private (in `entry.rs`) -> `pub(in crate::client)` (in `scram.rs`); new `pub(crate)` items `ScramRequest`, `ScramOutcome`, `run_scram`; `resume_wire_exchange`/`ResumedHandshake`/`verify_resume_identity_proof` keep `pub(crate)`; `handshake::wire` is `pub(in crate::client)`. `PHASE_DNS/TCP/TLS` and `phase_name` stay in `entry.rs`.

Public API untouched (`lib.rs`, option structs, `Client::connect/connect_host/connect_local/resume/resume_secure`). No Cargo.toml/lockfile change. Only the doc links of `Client::resume` changed (full paths to the moved `Wire*` types) and the `connect_local` doc sentence now says it shares `run_scram`.

BLOCKED: nothing. Not verified locally: compilation, clippy, tests (CI only).

## T9-1 R3 report (Rust client transport module, behaviour unchanged)

Moved: `client/transport.rs` became the folder `client/transport/` (`mod.rs` re-exports only, `binding.rs`, `write_sink.rs`, `dial.rs`). `WriteSink` and its `AsyncWrite` impl moved verbatim to `write_sink.rs`; `connect_tcp`, `connect_ipc`, `connect_with_timeout` moved verbatim to `dial.rs`. New: `ChannelBinding {transport, mode, exporter}` (`Copy`, constructors `tcp_tls_exporter`, `ipc`), `Dialed<R> {read, write, binding}`, `dial_tcp_tls` (the TCP part of `Client::handshake` before SCRAM, with the `PHASE_TCP`/`PHASE_TLS` stores) and `dial_ipc`. `ScramRequest` now carries `binding: ChannelBinding` instead of three loose fields; `run_scram` reads `binding.transport/mode/exporter`. `session/entry.rs` reads dial -> `run_scram` -> `assemble` for `connect` and `connect_local`.

Resume decision: `resume_secure` reuses `dial_tcp_tls(addr, server_name, None, None)`. The code is identical to the old inline path (client config, server name, TCP connect with no timeout, TLS connect, exporter extraction, split; same error values and texts); the only difference is that no phase atomic is passed, as before. The exporter for `resume_wire_exchange` is `dialed.binding.exporter`.

Visibility widenings against the base: `PHASE_TCP` and `PHASE_TLS` in `session/entry.rs` went from private to `pub(in crate::client)` (needed by `dial.rs`). New items only otherwise: `ChannelBinding` and `Dialed` are `pub(crate)` with fields `pub(in crate::client)`; the helpers are `pub(crate)`.

Not done / notes: `Dialed` is deliberately not re-exported from `transport/mod.rs` (nothing outside `dial.rs` names the type, so an `pub(crate) use` would be an unused import under `-D warnings`). The old `client/transport.rs` must be removed by the orchestrator (it cannot coexist with `transport/mod.rs`); `cargo fmt` could not run for this reason. The only test edit is `src/tests/wire/scram_binding_tests.rs` (new `binding` argument). No local compilation or tests were run; CI is the check. `connect_local`'s doc comment still mentions `transport_kind`/`binding_mode` (untouched).

## T9-2 report (Rust client: session over native WebSocket, resume over WebSocket, session over plain loopback TCP)

Status: DONE in code. No local compile/test was run (policy); checked by a compiler-style read, `cargo fmt -p shamir-client` and `python -I scripts/ci/check_layout.py`. Verification is CI only.

Added (crate `shamir-client`):
- Public API, the only new public items: `Client::connect_ws`, `Client::resume_ws`, `Client::resume_ws_secure`, `Client::connect_plain` (all in the new `client/session/entry_ws.rs`; options reuse `ConnectOptions`, `ResumeOptions`, `SecureResumeOptions`). `connect_plain` refuses a non-loopback `opts.addr` before any socket is opened with `ClientError::Handshake` (`plain TCP is loopback-only`); `server_name` is ignored there.
- `client/transport/ws_stream.rs`: `WsByteStream<S>` adapter (see decision below).
- `client/transport/dial_ws.rs`: `dial_ws_native` = TCP connect -> TLS (exporter-bound app-layer auth, same config as TCP) -> TLS exporter extracted BEFORE the upgrade -> `GET /shamir/v1` upgrade with `Sec-WebSocket-Protocol: shamir-v1`, no `Origin`, tungstenite message/frame ceiling `MAX_FRAME_SIZE_DEFAULT + 4` (as on the server) -> `WsByteStream` -> split. Upgrade failure maps to `ClientError::Handshake` with context `websocket upgrade`; the upgrade runs under `PHASE_TLS`.
- `client/transport/dial.rs`: `dial_tcp_plain` (the file stays under 200 lines, so no `dial_plain.rs`); `ChannelBinding::ws_tls_exporter` = (WebSocket, TlsExporter, exporter) and `ChannelBinding::tcp_plain` = (Tcp, None, zeros) in `binding.rs`; `WriteSink::Tcp` and `WriteSink::Ws` variants with the delegating `AsyncWrite` arms.
- `Cargo.toml`: one new dependency line `tokio-tungstenite = "0.30"` (no features). `Cargo.lock` is NOT touched: the orchestrator must add the `tokio-tungstenite` edge to the `shamir-client` package entry.
- Tests: unit `src/tests/wire/ws_byte_stream_tests.rs` (10 cases: prefix and payload in separate writes arrive as ONE message and nothing is sent before the frame is complete; one buffer; two frames in one write become two messages; a frame split across messages; small reads; ping skipped; TEXT -> InvalidData; peer Close -> EOF; oversized declared length -> InvalidData; shutdown sends Close). Integration `tests/matrix/` (`main.rs`, `common.rs` booting one server with `tcp`+`tls_exporter`, `ws`+`tls_exporter` path `/shamir/v1` and `tcp`+`plain` listeners; `ws_session.rs`, `ws_resume.rs` incl. TCP<->WS ticket moves and `resume_ws_secure`, `plain_session.rs`, `plain_refusal.rs`).

Adapter decision: as designed, the WebSocket is adapted to a byte stream (`WsByteStream`), so `reader_task`, `run_scram`, `resume_wire_exchange`, `Client::assemble` and the write path are untouched. Read side: BINARY payloads feed an internal buffer (message boundaries irrelevant, the bytes already carry the `[u32_be len]` prefix); Ping/Pong/Frame are skipped (tungstenite answers pings while the socket is polled by the reader task); Close or end of stream is EOF; TEXT is `InvalidData`. Write side: bytes are buffered until exactly one `[u32_be len][payload]` frame is present and then sent as ONE BINARY message (prefix and payload may arrive in one or several writes; `poll_write` accepts only the bytes of the current frame, so two frames never share a message); `poll_flush` drives the sink; `poll_shutdown` sends Close; a declared length above `MAX_FRAME_SIZE_DEFAULT` is `InvalidData`. No deviation from the brief.

Visibility widenings (all inside the crate, none public): `phase_name` and `session_meta` in `session/entry.rs` private -> `pub(in crate::client)`; `mark_phase` in `transport/dial.rs` private -> `pub(super)`; `Dialed`, `dial_tcp_plain` and `dial_ws_native` are re-exported `pub(crate)` from `transport/mod.rs`; `WsByteStream` is re-exported only under `#[cfg(test)]`; `WriteSink` gained two variants. `session/mod.rs` gained the private `mod entry_ws;`.

Layout: `transport/` 6 entries, `session/` 6, `tests/wire/` 7, crate `tests/` 6, all files far below 1000 lines.

Existing public API and wire bytes of the existing paths: unchanged.

Not done / out of scope (as in the brief): the browser WebSocket profile (needs WebPKI trust), HTTP/token paths, napi, TypeScript. Not verified locally: compilation, clippy, tests (CI only). Known notes: `WsByteStream::poll_send_queued` clones the buffered frame into the message (one extra copy per frame, the original is zeroized); the `Cargo.lock` edge is the orchestrator's.

## T9-3a report (Rust client: token over REST)

Status: DONE in code, UNVERIFIED by any compiler or test run (no local builds by policy); verification is CI only (fast tier on the draft PR, `[ci full]` or `gh pr ready` for the full matrix). Checked locally: `cargo fmt -p shamir-client -- --check` and `python -I scripts/ci/check_layout.py` (both clean) plus a compiler-style read of every file against the hyper 1.9.0, http-body-util 0.1.3, hyper-util 0.1.20, sdd 4.8.11 (`scc::Bag`), tokio-rustls 0.26 and rustls 0.23 sources.

Added (crate `shamir-client`, nothing else changes):
- Public API (re-exported from `lib.rs`): `ApiToken`, `TokenEndpoint`, `TokenTrust`, `TokenHttpOptions`, `TokenHttpClient`; `ClientError::Http { status, code, retry_after }` and `ClientError::InvalidToken`. All new public structs/enums are `#[non_exhaustive]` with constructors.
- `client/transport/token/`: `endpoint.rs` (`TokenEndpoint::https` / `plain_loopback`, loopback check on `ip().to_canonical()` before any socket), `tls.rs` (`TokenTrust::{webpki_roots, pem, der}`, TLS 1.3 only, aws-lc-rs provider, standard WebPKI verifier, ALPN `http/1.1`, early data off; `make_client_config_app_authenticated` is not used), `http_conn.rs` (hyper `conn::http1` over `TokioIo`, `scc::Bag` + `AtomicUsize` pool bounded at 4 idle connections, retry only when hyper returns the request unsent or `ready()` fails, 16 MiB `Limited` body, redirects never followed, no cookies, ALPN must be `http/1.1`), `status.rs` (status table and `Retry-After` delta-seconds parsing clamped to 1..=3600), plus `tests/` (fake `TcpListener` server: exact request heads, keep-alive reuse, `Connection: close`, chunked, 307 not followed, oversize body, request timeout, refused connect, status table, endpoint and TLS builders).
- `client/session/token/`: `api_token.rs` (validation through `shamir_connect::server::token::parse_token`; `Zeroizing<String>`; redacted `Debug`; the `Authorization` header is built once into a zeroizing owner via `Bytes::from_owner`, `set_sensitive(true)`), `options.rs`, `request_filter.rs` (deny-by-default: `Ping`, `Execute`, `WaitForVisibility`, `GetDdlOpStatus` only; `Tx*` and cursor requests give `Db { code: "not_supported_over_rest" }`, everything else `token_request_denied`, nothing is sent), `http_client.rs` (`connect`, `ping`, `request`, `execute`, `wait_for_visibility`, `get_ddl_op_status`, `close`; reuses `visibility::map_response` and `receipts::wait_for_visibility_via`), plus `tests/`.
- Integration: `tests/matrix/token/{mod,common,http}.rs` (own fixture with one TLS and one plaintext loopback HTTP listener, seeded owner, read-write and read-only tokens through `token_service.create`, trust anchor = generated `cert.pem`; cases: ping and execute over TLS, unknown secret gives `Http { 401, "unauthorized" }`, read-only write gives `Db { "token_read_only" }`, `TxBegin` gives `not_supported_over_rest` locally, plaintext loopback, webpki roots reject the self-signed server). `mod token;` added to `tests/matrix/main.rs`.
- `Cargo.toml`: exactly the five lines of design section 3 (`hyper`, `hyper-util`, `http-body-util`, `bytes`, `webpki-roots`). `Cargo.lock` is NOT touched: the orchestrator regenerates it (expected new packages `want`, `try-lock` and the five edges in `shamir-client`).

Visibility widenings: only the new public exports above; `transport::token` and `session::token` are `pub mod` inside the crate-private/`pub(crate)` parents (not reachable except through the `lib.rs` re-exports); `TokenReply`, `TokenTransport`, `TokenScheme`, `map_reply`, `map_ping_reply`, `build_connector`, `parse_server_name`, `check_request_allowed`, `ApiToken::authorization_header` are `pub(crate)`.

Decisions: `TokenTransport::connect` dials up front (TCP + TLS + HTTP/1 handshake) under `connect_timeout`; a request that hits `request_timeout` drops its connection; an HTTP exchange failing after a possible write is a `Protocol` error and is never retried; after a failed retry on a fresh connection the error is `ConnectionClosed(PeerEof)`; `execute` over REST does not announce interner epochs or merge deltas; the encoded request body handed to hyper cannot be wiped (documented), the header value and the `ApiToken` are zeroizing.

Not done / open: no positive `TokenTrust::pem` unit test (no certificate fixture without a build step; covered by the integration test with the server's `cert.pem`); no connect-timeout unit test (racy on loopback; only refused connect is tested); the token WebSocket (T9-3b) and password login (T9-4) are out of scope; nothing was compiled, so CI may reveal compile or clippy errors that a read could not.

## T9-3b report (Rust client: token over native WebSocket)

Status: implemented in full but NOT compiled or run locally (policy); verification is CI only (`cargo fmt --check` and the layout check passed locally).

Added (crate `shamir-client`):
- `transport/token/dial_ws.rs`: hyper upgrade, sensitive `Authorization`, checks of `Accept` / subprotocol / no extensions, reject body capped at 64 KiB and mapped through the new `status::map_upgrade_reply`.
- `handshake/bearer.rs`: strict binding decoder (map marker check, `deny_unknown_fields`, no trailing bytes, `ok == true`, 32-byte non-zero `sid`, `expires_at_ns > 0`); `read_binding`.
- `transport/ws_stream.rs`: `WsFraming::{Prefixed, Raw}`, `WsByteStream::raw`, `WsPeerClose`.
- `WriteSink::TokenWs`; `CloseCause::PeerClosed {code, reason}` mapped in `reader_task`.
- `AuthKind` (`session/token/auth_kind.rs`), `SessionMeta.auth`, `Client.auth`, `Client::auth_kind`.
- `Client::require_scram`, first line of `create_api_token`, `list_api_tokens`, `revoke_api_token`, `revoke_user_tokens`, `create_scram_user`, `set_replicator`, `repl`.
- `TokenWsOptions`, `Client::connect_token_ws` in `session/entry_ws.rs` (224 lines).

Tests:
- Unit: `handshake/tests/bearer_tests.rs`, `session/token/tests/{ws_raw_tests,close_cause_tests,refusal_tests}.rs`.
- Integration `tests/matrix/token/ws.rs` (9 tests): ping, execute read/write, read-only denied, REST-only token -> `token_channel_denied`, unknown secret -> 401, revoke -> 4011, 2 s TTL -> 4012, admin refusal, plain loopback.
- Fixture helpers `issue_ws`, `revoke_token`, `issue_with` in `tests/matrix/token/common.rs`.

Public items: `Client::connect_token_ws`, `Client::auth_kind`, `AuthKind`, `TokenWsOptions`, `CloseCause::PeerClosed`.

Widenings beyond the public items:
- `WsByteStream` re-export in `transport/mod.rs` is no longer `cfg(test)`.
- New `pub(crate)` re-exports: `WsPeerClose`, `dial_token_ws`, `read_binding`.
- `session/client.rs`: `pub(crate) require_scram`, `#[cfg(test)] for_test_with_auth`.
- `pub(super) map_upgrade_reply` in `status.rs`.

Decisions / deviations:
- Raw mode rejects a zero declared length on write (`InvalidData`); Prefixed mode unchanged (Close = EOF).
- `connect_timeout` bounds only the TCP connect; `handshake_timeout` bounds TLS + upgrade + binding frame.
- The token is dropped right after the `Authorization` header is built.

Not done / risks:
- Subscriptions and cursors over the token WS have no integration tests (no fixture pattern).
- No unit test for `dial_ws.rs` (`transport/token/tests` is at the 7-entry limit).
- `server_query_version` is 0 on token sessions, as designed.
- Everything needs CI confirmation (compile, `clippy -D warnings`, tests).
- `api.rs` is 917 lines (< 1000).

Review fixes (orchestrator, after an independent compiler-style read):
- `oversized_incoming_message_is_invalid_data` lifts tungstenite's own limits so the Raw length check is what fires (the first version deadlocked on the small duplex).
- A write after a peer `Close` reports the same `WsPeerClose` (`peer_close` in `WsByteStream`, shared helper `WsPeerClose::cause_of`), so a concurrent writer cannot replace `PeerClosed` with `Io`.
- The binding decoder accepts `sid` only as a msgpack `bin` of 32 bytes (`str` and arrays are refused).
- Raw-mode outgoing messages no longer copy the payload.
- Added a `reader_task` close-mapping test; refusal and binding tests are bounded by timeouts.

## T9-6 report (TS Node: session over TCP+TLS and native WebSocket, resume, changePassword)

Status: implemented, UNVERIFIED. Nothing was compiled or run locally (project policy); only `python -I scripts/ci/check_layout.py` passed. Types and tests were checked by reading only; CI is the first compile.

Added:
- Core (platform-agnostic): `TRANSPORT_KIND_TCP`, `BINDING_MODE_TLS_EXPORTER` (`scram.ts`); `tlsExporterBinding(kind, exporter)` (`session/binding.ts`, copies, 32-byte check); `SessionTlsOptions` (adds `ca`), `TcpSessionEndpoint`, `WsNativeSessionEndpoint`, `NativeSessionEndpoint` (`session/endpoint.ts`); `endpointFromOptions` and `dialEndpoint` (`session/open.ts`) used by `openScramSession` and `resumeSession`; optional `Platform.openNativeLink`; `transport?: 'ws-browser' | 'tcp' | 'ws-native'` and `tls.ca` on `ConnectOptions` and `ResumeOptions`.
- `runHandshake` now passes the binding exporter to `buildAuthMessage` (`tlsExporterOrZeros`). Zero for browser/IPC/legacy forms, so those wires are unchanged; for mode 0x01 it is the real exporter, as the server computes it. Before this change the exporter was never put into the auth_message.
- `resumeSession`: on a 0x01 link, `verifyResumeIdentityProof` first sends the 1-field array `[client_nonce]`, checks the returned key against `serverPubKey` and verifies Ed25519 (`@noble/ed25519`, zip215 false) over `SHAMIR-RESUME-PROOF-v1 || nonce || exporter`; only then the ticket is sent (mirrors Rust `resume_wire_exchange`).
- Node dialers in `platform/session/`: `dial-tcp.ts` (`tls.connect`, TLSv1.3 minimum, no ALPN, `rejectUnauthorized` default true, `ca`), `dial-ws-native.ts` (`ws`, `['shamir-v1']`, no Origin, wss only, exporter from the upgrade response TLS socket), `tls-exporter.ts` (`exportKeyingMaterial(32, 'EXPORTER-ShamirDB-AUTH-v1', empty)`), `tls-frame-socket.ts`, `tls-options.ts`, `native-platform.ts` (`NodeSessionPlatform`, used by `connect()` and `resume()` in `src/index.ts` only; `connectLocal` and `browser.ts` untouched). `NodeSocket` in `platform/node.ts` is now exported.
- Tests: unit (`session/__tests__/tls-binding`, `endpoint-select`, `resume-proof`; `platform/session/__tests__/dial-tcp`, `dial-ws-native`); e2e `src/__tests__/connection/session/e2e-native-transports.test.ts` (TCP and native WS query, resume over both, bogus ticket, changePassword over TCP, wrong CA, no CA) with a multi-listener harness (`startMultiListenerServer`, `parseBoundListeners` finds listeners by kind and profile).
- Docs: matrix rows 1, 2, 6a, 6b for TS Node set to yes with the e2e path; TS README section.

Decisions and deviations:
- The exporter is taken from the TLS socket after the WebSocket upgrade (same TLS session, same value as the Rust pre-upgrade extraction).
- `tls.ca` with the default browser transport throws instead of being ignored.
- No `identity_sig` pin check (task #622); trust is `rejectUnauthorized` plus `ca`.

Not done / risks to check in CI: the ws-native resume path and the `listener bound` log format parse (`profile=Some(TlsExporter)`) are inferred from code, not observed. `@noble/hashes/sha512` import path assumed from hashes 1.8.0. The e2e needs `npm ci` and a release server binary.

Review fixes (orchestrator, after an independent read against the server and the Rust client):
- `TlsFrameSocket` no longer reuses the local-IPC reader: it has its own linear-time buffer, a 16 MiB frame ceiling, and treats a zero-length frame as the peer's close marker (as `shamir-transport-tcp` does).
- `runHandshake` hashes the exporter only for binding mode 0x01 (the server's rule); a unit test pins that a browser binding with a non-zero exporter field still gives the pre-change proof.
- `dialTcp` sends SNI for DNS names (not for IP literals); the browser entry's `connect` overload does not advertise Node-only `transport` values or `tls.ca`.
- Matrix rows 1, 2, 6a, 6b for TS Node stay `план T9-6` until `ts-e2e-nightly` has run green on this branch (the PR CI skips the e2e suite without a server binary); the status flips in the same PR once it has.
- Known and accepted: `open.ts` and `resume.ts` carry more than one export (the `session/` folder is at 7 entries and cannot take a subfolder without a larger move); `resume.ts` lazily sets `ed.etc.sha512Sync` on the shared `@noble/ed25519` module.

