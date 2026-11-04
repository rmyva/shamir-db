# Security Policy

## Reporting a Vulnerability

We take security reports seriously and ask that you **do not open a public
GitHub issue** for a suspected vulnerability.

**Please open a private security advisory via GitHub:** use
*"Security" → "Report a vulnerability"* on this repository
([GitHub's private vulnerability reporting][gh-pvr]). This keeps the report
between you and the maintainers and lets us coordinate a fix before public
disclosure.

> **Maintainer note:** this project has no dedicated security email yet. If a
> dedicated address or PGP key is added later, update this section to point to
> it. Until then, GitHub's private advisory flow is the channel of record.

When reporting, please include:

- a description of the issue and its security impact (what an attacker gains);
- the minimal reproduction steps, including the query / payload / client
  behavior that triggers it;
- the `shamir-db` commit hash and OS you tested against;
- any relevant logs (with secrets / PII redacted).

We aim to acknowledge reports within **72 hours** and to send a first
assessment (accepted / needs-more-info / declined) within **7 days**. A fix
(or, for lower-severity findings, a tracked mitigation plan with a timeline)
follows depending on severity. We are happy to coordinate a coordinated
disclosure date.

This project is **alpha-stage** (`Version 0.1.0 (Alpha)`); we do not commit to
back-porting fixes to anything other than `master`, see the table below.

## Supported Versions

S.H.A.M.I.R. DB has no versioned releases yet (no tags, no published binaries).
Only the latest commit on `master` is supported.

| Version | Supported |
|---------|-----------|
| `master` (latest commit) | ✅ |
| any other commit / snapshot | ❌ |
| tagged releases | none exist yet |

Once tagged releases exist, this table will list each supported line with its
support window.

## Scope

We are interested in reports that affect the security of the database, its
clients, or the host it runs on. In scope (non-exhaustive):

- **WASM sandbox escape** — the `shamir-wasm-host` executes guest code via
  `wasmtime`; any path that lets guest code read/write host memory, escape
  fuel/epoch limits, or reach host APIs is critical.
- **Compile-time / build-script exfiltration** — `shamir-wasm-host/src/compile.rs`
  compiles guest Rust on the host; paths that let guest source reach host
  environment / filesystem at compile time (`env!`, `include_str!`,
  `include_bytes!`) are in scope.
- **Authentication / authorization bypass** — SCRAM auth, identity rotation,
  ACL enforcement, session resumption binding, channel binding.
- **Memory-safety issues in `unsafe` code** — the workspace contains
  performance-oriented `unsafe`; soundness regressions there are in scope.
- **Cryptography misuse** — weaknesses in the Argon2id / HKDF / HMAC-SHA256 /
  Ed25519 / AES-GCM usage, or constant-time-comparison regressions.
- **Denial of service** — unbounded resource consumption on the auth, query,
  or connection-accept paths (slow-loris, connection cap bypass, unbounded
  query expansion, WAL/storage exhaustion).
- **Supply-chain concerns** — a malicious or compromised dependency, a
  tampered `Cargo.lock`, or a reproducibility break (e.g. an unresolvable /
  non-reproducible dependency pin).

**Out of scope** (please use a regular GitHub issue instead):

- self-inflicted data loss from misconfiguration with no external trigger;
- theoretical timing side-channels with no demonstrated exploit path beyond
  what the existing constant-time review already covers;
- findings from automated scanners reported without manual validation against
  the actual code path.

## Supply-chain posture

- **Dependency advisories & licenses** are gated in CI by `cargo deny check`
  (`.github/workflows/supply-chain.yml`, `deny.toml`), run on every push/PR.
- **Periodic advisory re-scan** runs weekly via `cargo audit`
  (`.github/workflows/supply-chain.yml`), because RUSTSEC advisories change
  independently of commits.
- The workspace uses a 3-day **dependency cooldown** (`cooldown.toml`,
  `cargo-cooldown`) so freshly-published registry versions cannot enter the
  tree without explicit review.
- No hardcoded secrets are tracked in the repository (confirmed by the
  2026-07-06 supply-chain audit); `server-cert.pem` and other local secrets
  are `.gitignore`d.

### Priority dependencies — `wasmtime`

`wasmtime` (currently 45.0.0, the entire `wasmtime-internal-*` cluster) is the
**untrusted-code execution boundary** for guest WASM (see `shamir-wasm-host`).
Its security advisories (sandbox escape, fuel/epoch bypass, cranelift OOB,
host-trap escape) are historically regular and high-impact. Because of this
role, `wasmtime` is treated as a **priority-upgrade dependency**:

- **Advisory tracking beyond RUSTSEC.** The general `cargo audit` /
  `cargo deny` flow (above) covers RUSTSEC, but wasmtime advisories are also
  announced through the Bytecode Alliance's own security channel, which can
  surface issues before/outside the RUSTSEC pipeline. Maintainers should watch:
  - the Bytecode Alliance security policy + notifications:
    <https://bytecodealliance.org/security>
  - the wasmtime GitHub security advisories tab (also the reporting channel):
    <https://github.com/bytecodealliance/wasmtime/security>
  These should be checked at the same cadence as the weekly `cargo audit`
  re-scan, and on any report of a new cranelift/wasmtime CVE.
- **No cooldown deferral.** Unlike lower-trust-boundary dependencies, `wasmtime`
  bumps that fix a security issue (or a sandbox-hardening release) should **not**
  be deferred through the standard 3-day dependency cooldown
  (`cooldown.toml` / `cargo-cooldown`) the way routine version bumps are. A
  security-motivated `wasmtime` bump may bypass the cooldown with a recorded
  justification in the PR description (the cooldown exists to let freshly-
  published registry versions bake; a security fix is the opposite case).

This policy closes audit finding C5
(`docs/dev-artifacts/audits/2026-07-06-security-compliance-supplychain.md`).

[gh-pvr]: https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability
