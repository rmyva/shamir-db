# T9-8d - TypeScript client: pin of the server identity key (`identity_sig`, task #622) and resume in the browser profile

Base: `master` (TS layers S0-S4 and T9-6). Design: `docs/dev-artifacts/audits/2026-11-04-t9-client-matrix/DESIGN.md` (matrix in section 4 and the T9-6 report: "no `identity_sig` pin check in TS (task #622): trust is `rejectUnauthorized` plus `ca`"), the user-facing table `docs/guide-docs/guide/client-matrix.md`, then CLAUDE.md. Package: `crates/shamir-client-ts` (pure TypeScript; the server and the Rust crates do not change). The Rust client is the reference implementation of the pin: read how `crates/shamir-client` (`client/handshake/auth_ok.rs`, `wire.rs`, the `trusted_pin` / `accept_new_host` options and `Client::server_pub_key_pin`) and the server side in `crates/shamir-connect` verify `identity_sig`, and mirror the order of checks.

## Goal

1. **Identity pin.** In every session handshake (browser WebSocket, native WebSocket, TCP+TLS, IPC) the TS client verifies the server's Ed25519 identity: the server public key is hashed to `SHA256(pub_key)` and compared with a pinned value when the caller supplies one; on first contact (no pin) the caller must opt in (`acceptNewHost`-style) and can read the captured pin back for persistence; `identity_sig` (the server's signature over its handshake data) is verified with the public key BEFORE any secret leaves the client (before the proof / ticket is sent) and a mismatch is a typed error (the Rust client's `ServerIdentityChanged` class, mapped to the existing TS error hierarchy in `core/errors`). Use `@noble/ed25519` that the package already depends on (no new dependency). Defaults must not break existing callers: if the existing options have no pin field today, add optional `trustedPin` and `acceptNewHost` (default: accept and expose the pin, i.e. today's behaviour) and document that a pin is the recommended hardening.
2. **Resume in the browser profile.** The browser entry point (`src/index.browser.ts` or the equivalent browser export; find it with the package `exports` map) exposes the resume entry point for the browser WebSocket profile (binding mode 0x02, resume without `resume_proof`, as the Node `resume` does for this profile). Today the matrix says "not exported".

## What to build (deviations only with a written reason in the report)

- The verification lives in the shared session layer `src/core/protocol/session/` (not duplicated per platform); the platform packages only pass options through.
- Keep the public API additive and typed; update the `.d.ts`-visible types in `src/core/types/connection.ts`.
- No behaviour change on the wire.

## Tests (CI-only)

- Unit: correct signature accepted; flipped signature byte refused; wrong pin refused with the typed error and NO later message sent (assert the fake transport saw nothing after the server hello); first contact without `acceptNewHost` refused when a pin policy is requested; captured pin equals `SHA256(pub_key)`. Mirror the Rust unit cases in `crates/shamir-client/src/tests/` (`auth` and `wire` folders).
- E2E (`src/__tests__/connection/session/`, harness `src/__tests__/e2e-harness.ts`): connect with the right pin succeeds on the browser WS listener and on native TCP+TLS; wrong pin refused; resume from the browser entry point over the browser WS listener after a browser-profile session.

## Docs

- `docs/guide-docs/guide/client-matrix.md`: the note about the missing pin in TS is removed; row "6c browser" becomes `да (тест: <path>)` only if proven.
- `docs/guide-docs/KNOWN_LIMITATIONS.md` "Client coverage": drop the two gaps that no longer exist.
- `crates/shamir-client-ts/docs/README.md`: pin usage and the browser resume.
- Append a "T9-8d report" section to `docs/dev-artifacts/audits/2026-11-04-t9-client-matrix/DESIGN.md` as the last step.

## Rules

- NO LOCAL COMPILATION OR TESTS. The only command you may run is `python -I scripts/ci/check_layout.py`. No `cargo check/clippy/build/test`, no rustc, no `npm`/`npx`/`tsc`/`vitest`/`node`. Children (if any) only edit files and must not run commands. Read your TypeScript like a compiler (strict mode, import paths with the `.js` suffix as in neighbouring files, unused imports and exports).
- Layout limits: at most 7 direct entries per folder (`core/protocol/session/` has 6 entries today: add a subfolder instead of a seventh+ file), 1000 lines per file, one primary export per file.
- No `package.json`, lockfile, `Cargo.toml` or workflow edits; if something is missing, stop and report.
- ⛔ NEVER run `git reset` / `checkout` / `clean` / `stash` / `restore` / `rm`, or any git command that mutates the working tree or index (read-only `git show/log/diff` is fine). Only edit files; the orchestrator commits. No machine-local absolute paths in any file. Use forward slashes in all paths. Do not mention the continuous-integration setup of the orchestrator (which repository runs it) in any file; write "CI".
- Hard session limit 150 minutes: stop editing 40 minutes before the time in the run file and spend that time on the read-through. Never leave a half-edited item: finish it or record it BLOCKED.
