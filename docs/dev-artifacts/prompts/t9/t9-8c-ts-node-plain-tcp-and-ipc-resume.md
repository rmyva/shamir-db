# T9-8c - TypeScript client on Node: session over plain loopback TCP, resume over plain TCP, resume over local IPC

Base: `master` (TS layers S0-S4 and T9-6: `crates/shamir-client-ts/src/core/protocol/session/{binding,endpoint,open,opened,resume,index}.ts`, `src/platform/session/{dial-tcp,dial-ws-native,native-platform,tls-exporter,tls-frame-socket,tls-options}.ts`, `transport: 'ws-browser' | 'tcp' | 'ws-native'` on `connect()` / `resume()`, `connectLocal` for IPC). Design: `docs/dev-artifacts/audits/2026-11-04-t9-client-matrix/DESIGN.md` (matrix in section 4, the T9-6 report at the end), the user-facing table `docs/guide-docs/guide/client-matrix.md`, then CLAUDE.md. Package: `crates/shamir-client-ts` (pure TypeScript; server and Rust crates do not change).

## Goal

Three cells of the "TS in Node" column become `да`, each proven by an end-to-end test against a real server:

- **row 1b** - SCRAM session over plain loopback TCP (listener `tcp`, profile `plain`, binding mode 0x00, no TLS, loopback only: the server refuses anything else);
- **row 6d** - ticket resume over plain loopback TCP;
- **row 6e** - ticket resume over local IPC (Unix socket on Linux/macOS, Named Pipe on Windows; listener `unix`, binding 0x00). The Rust client already does it (`ipc_resume_e2e` in `crates/shamir-server/tests/e2e/main.rs` shows the wire shape); TS has `connectLocal` for the session but no resume over IPC.

## What to build (deviations only with a written reason in the report)

1. **Explicit option, defaults unchanged.** Extend the Node `transport` option with a plain-TCP value (name it in the existing style, e.g. `'tcp-plain'`) on `connect()` and `resume()`; the default stays the browser-profile WebSocket. Plain TCP must be refused client-side, before any socket is opened, for a non-loopback host (same rule as the Rust `connect_plain`). Reuse the existing frame socket for TCP (`tls-frame-socket.ts` is TLS-specific: add a plain-socket variant in its own file instead of overloading it; the 16 MiB frame ceiling and linear buffering rules apply to it too). Binding for plain TCP = `{ transportKind: TCP, bindingMode: None (0x00) }` with zero exporter (see `binding.ts`: `ipcBinding()` is the closest existing constructor; add a `tcpPlainBinding()` next to it).
2. **Resume with binding 0x00.** Read `core/protocol/session/resume.ts` (it sends `[client_nonce]` and verifies the server key against the pin plus the Ed25519 `resume_proof` BEFORE the ticket is sent, for TLS-exporter links) and the Rust IPC resume path; for binding 0x00 copy the Rust behaviour exactly (what is proven, what is sent). Add `resumeLocal` (or an option on the existing local-connect path) following the naming of `connectLocal`.
3. **No behaviour change** for existing paths and wire bytes (the unit test that pins the legacy numeric `runHandshake` form stays unchanged).

## Tests (CI-only; they must exercise the real paths)

- Unit tests next to the code: binding constructor bytes, loopback refusal, plain frame socket framing (split/merged frames, oversized length refused).
- E2E in `src/__tests__/connection/session/` (read `e2e-native-transports.test.ts`, `e2e-connect-local.test.ts`, `e2e-resume.test.ts` and `src/__tests__/e2e-harness.ts`): the harness already starts several listeners (browser WS, native WS, TCP+TLS); add a `tcp` / `plain` listener and a `unix` listener to it WITHOUT changing the existing ones, and add cases: plain TCP session and a batch; plain TCP session then resume over plain TCP; ticket from the TLS TCP listener resumes over plain TCP and back; IPC session then resume over IPC; refusal of a wrong ticket; refusal of a non-loopback host before connecting. Skip the IPC cases on a platform where the harness cannot start the listener (follow how `e2e-connect-local.test.ts` does it). `ts-e2e-nightly` runs the e2e suite; a unit-level typecheck runs in the PR CI.

## Docs

- `docs/guide-docs/guide/client-matrix.md`: TS Node rows 1b, 6d, 6e become `да (тест: <path of the proving test file>)` ONLY for cells your tests prove.
- `docs/guide-docs/KNOWN_LIMITATIONS.md` "Client coverage": drop the gaps that no longer exist.
- `crates/shamir-client-ts/docs/README.md` (find the Node options section): document the new `transport` value and `resumeLocal`.
- Append a "T9-8c report" section to `docs/dev-artifacts/audits/2026-11-04-t9-client-matrix/DESIGN.md` as the last step.

## Rules

- NO LOCAL COMPILATION OR TESTS. The only command you may run is `python -I scripts/ci/check_layout.py`. No `cargo check/clippy/build/test`, no rustc, no `npm`/`npx`/`tsc`/`vitest`/`node`. Children (if any) only edit files and must not run commands. Read your TypeScript like a compiler (strict mode, `noUncheckedIndexedAccess` if set, import paths with the `.js` suffix as in neighbouring files, unused imports and exports, the `Platform` interface contract).
- Layout limits: at most 7 direct entries per folder, 1000 lines per file, one primary export per file.
- No `package.json`, lockfile, `Cargo.toml` or workflow edits (everything needed is already a dependency or a Node built-in); if something is missing, stop and report.
- ⛔ NEVER run `git reset` / `checkout` / `clean` / `stash` / `restore` / `rm`, or any git command that mutates the working tree or index (read-only `git show/log/diff` is fine). Only edit files; the orchestrator commits. No machine-local absolute paths in any file. Use forward slashes in all paths. Do not mention the continuous-integration setup of the orchestrator (which repository runs it) in any file; write "CI".
- Hard session limit 150 minutes: stop editing 40 minutes before the time in the run file and spend that time on the read-through. Never leave a half-edited item: finish it or record it BLOCKED.
