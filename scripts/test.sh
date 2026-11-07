#!/usr/bin/env bash
# Centralised test runner — the SINGLE blessed entry point.
#
# Enforces the CLAUDE.md test conventions so nobody (human or sub-agent)
# reaches for raw `cargo test --workspace --tests 2>&1 | grep ...` again.
# Three rules baked in:
#   1. cargo-nextest, not cargo test  — real-time progress, per-test timeout.
#   2. No pipe to grep                — nextest streams output directly.
#   3. Per-crate / per-scope by default — fast signal; full workspace opt-in.
#
# Usage:
#   ./scripts/test.sh                          # lib tests, all crates
#   ./scripts/test.sh --full                   # lib + integration + e2e, all crates
#   ./scripts/test.sh -p shamir-tx             # one crate (lib only)
#   ./scripts/test.sh -p shamir-tx --full      # one crate (lib + tests)
#   ./scripts/test.sh -- mvcc                  # filter by test name
#   ./scripts/test.sh -p shamir-tx -p shamir-engine   # multiple crates
#
# Named scopes (preset groups — see SCOPES below):
#   ./scripts/test.sh @tx                      # shamir-tx only, lib
#   ./scripts/test.sh @oracle                  # tx + engine (Version Oracle area)
#   ./scripts/test.sh @oracle --full           # + integration tests
#   ./scripts/test.sh @e2e                     # all e2e suites (--full implied)
#   ./scripts/test.sh @types @tx               # combine scopes
#   ./scripts/test.sh @oracle -- watermark     # scope + name filter
#   ./scripts/test.sh --archive-to <file.tar.zst>  # build+archive workspace lib test binaries (CI)
#   ./scripts/test.sh --from-archive <file.tar.zst> # run tests from a nextest archive (CI)
#
# CI archive modes:
#   --archive-to builds `cargo nextest archive [--workspace | -p ...] --locked`
#   by default; `--full` omits --lib (nextest default target selection).
#   Only the lib `test` job consumes the archive; `integration` builds in place.
#   Explicit `-p` args archive just those crates (affected-crate runs).
#   --from-archive runs archived tests, omitting only --lib and --locked while
#   forwarding every other argument unchanged; CI auto-selects profile ci.
#
# Available scopes (extend as needed):
#   @tx       — shamir-tx
#   @engine   — shamir-engine + shamir-engine-query + shamir-engine-validator
#   @oracle   — shamir-tx + shamir-engine (Version Oracle area)
#   @types    — shamir-types + shamir-collections
#   @storage  — shamir-storage + shamir-wal
#   @server   — shamir-server + shamir-connect
#   @e2e      — shamir-db + shamir-server + shamir-e2e (forces --full)
#   @all      — every workspace crate (explicit; same as no -p)
#
# Power-user: pass `-E '<nextest-filter-expression>'` for arbitrary
# nextest filter expressions (e.g. `'package(shamir-tx) and test(/mvcc.*/)'`).
#
# Output: streams test results live; final summary line.
# Exit code: 0 on green, non-zero on any failure / timeout / panic.
#
# Hang protection comes from .config/nextest.toml:
#   default slow-timeout = 30s × 6 = 180s kill
#   wasm_function_* override = 120s × 2 = 240s kill (legit ~99s)
#   shamir-connect SCRAM    = 10s × 6 = 60s kill (Argon2)

set -u

if ! command -v cargo-nextest >/dev/null 2>&1 && ! cargo nextest --version >/dev/null 2>&1; then
    # Binary exists but not on PATH? Check common locations.
    _found=""
    for _dir in "${CARGO_HOME:-$HOME/.cargo}/bin" "$HOME/.cargo/bin"; do
        for _ext in "" ".exe"; do
            if [ -x "$_dir/cargo-nextest$_ext" ]; then
                _found="$_dir"
                break 2
            fi
        done
    done
    if [ -n "$_found" ]; then
        export PATH="$_found:$PATH"
    else
        echo "ERROR: cargo-nextest is not installed." >&2
        echo "Install: cargo install cargo-nextest --version 0.9.137 --locked" >&2
        exit 2
    fi
fi

# ---------------------------------------------------------------------------
# Reap stray test/bench binaries from a previously-wedged run (Windows).
#
# A test that hangs hits the per-test slow-timeout (.config/nextest.toml) and
# nextest kills the test thread — but on Windows the test PROCESS can linger
# (orphaned blocking thread / held file handle) still holding its own `.exe`.
# That zombie then wedges the NEXT run's link step (LNK1104: cannot open file
# ...exe), turning one flaky hang into a cascade that looks like "tests run
# for 20 minutes". We reap such strays up front so a prior hang never blocks
# the next run.
#
# Targets ONLY hashed cargo/nextest binaries `shamir_<crate>-<hex>.exe`
# (note the underscore + hash), never the plain `shamir-server` / `shamir-db`
# runtime binary (dash, no hash). No-op off Windows (no `tasklist`).
# NOTE: do not run two `test.sh` concurrently — the reaper would kill the
# other run's binaries (sequential is the contract anyway).
# ---------------------------------------------------------------------------
if command -v tasklist >/dev/null 2>&1; then
    tasklist //FO CSV //NH 2>/dev/null \
        | awk -F'","' '$1 ~ /shamir_[a-z_]+-[0-9a-f]/ { gsub(/"/, "", $2); print $2 }' \
        | while read -r _pid; do
            [ -n "$_pid" ] && taskkill //F //PID "$_pid" >/dev/null 2>&1 || true
        done
fi

# ---------------------------------------------------------------------------
# Scope dictionary — short names → list of -p <crate> args.
# Add entries as the codebase grows; keep them short and topical.
# ---------------------------------------------------------------------------
scope_args() {
    case "$1" in
        @tx)       echo "-p shamir-tx" ;;
        @engine)   echo "-p shamir-engine -p shamir-engine-query -p shamir-engine-validator" ;;
        @oracle)   echo "-p shamir-tx -p shamir-engine" ;;
        @types)    echo "-p shamir-types -p shamir-collections" ;;
        @storage)  echo "-p shamir-storage -p shamir-wal" ;;
        @server)   echo "-p shamir-server -p shamir-connect" ;;
        @vector)   echo "-p shamir-index-core -p shamir-vector -p shamir-index -p shamir-engine" ;;
        @e2e)      echo "-p shamir-db -p shamir-server -p shamir-e2e" ;;
        @all)      echo "" ;;  # nextest default = workspace
        @*)
            echo "ERROR: unknown scope '$1'. See ./scripts/test.sh --help." >&2
            return 1
            ;;
        *)
            echo "ERROR: scope_args called with non-scope '$1'" >&2
            return 1
            ;;
    esac
}

print_help() {
    sed -n '/^# Usage:/,/^# Hang protection/p' "$0" | sed 's/^# //;s/^#//'
    exit 0
}

mode="lib"            # default: lib tests only — fastest signal
archive_to=""         # --archive-to: build+pack a nextest archive (CI)
archive_from=""       # --from-archive: run tests from a nextest archive (CI)
forces_full=""        # named scopes can force --full (e.g. @e2e)
extra_args=()
scope_seen=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --help|-h)
            print_help
            ;;
        --full|-f)
            mode="full"
            shift
            ;;
        --archive-to)
            [[ $# -ge 2 ]] || { echo "ERROR: --archive-to requires a file path" >&2; exit 2; }
            archive_to="$2"
            shift 2
            ;;
        --from-archive)
            [[ $# -ge 2 ]] || { echo "ERROR: --from-archive requires a file path" >&2; exit 2; }
            archive_from="$2"
            shift 2
            ;;
        --lib|-l)
            mode="lib"
            shift
            ;;
        @*)
            # Named scope — expand to one or more -p args.
            expanded=$(scope_args "$1") || exit 2
            scope_seen=1
            # Scopes like @e2e imply --full (integration suites only exist
            # in --tests builds).
            if [[ "$1" == "@e2e" ]]; then
                forces_full=1
            fi
            # shellcheck disable=SC2206
            for arg in $expanded; do
                extra_args+=("$arg")
            done
            shift
            ;;
        --)
            shift
            extra_args+=("$@")
            break
            ;;
        *)
            extra_args+=("$1")
            shift
            ;;
    esac
done

if [[ -n "$forces_full" ]]; then
    mode="full"
fi

if [[ -n "$archive_to" && -n "$archive_from" ]]; then
    echo "ERROR: --archive-to and --from-archive are mutually exclusive" >&2
    exit 2
fi

# ---------------------------------------------------------------------------
# Archive-build mode (CI): only the lib `test` job consumes this archive;
# `integration` builds in place. Benches are never built by nextest's default
# target selection.
# ---------------------------------------------------------------------------
if [[ -n "$archive_to" ]]; then
    # Target selection: explicit -p args when given (affected-crate
    # archives), otherwise the whole workspace. --locked is re-added
    # below; --workspace is the default and never passed twice.
    pkg_args=()
    for a in ${extra_args[@]+"${extra_args[@]}"}; do
        [[ "$a" == "--workspace" || "$a" == "--locked" ]] && continue
        pkg_args+=("$a")
    done
    archive_args=()
    if [[ ${#pkg_args[@]} -eq 0 ]]; then
        archive_args+=(--workspace)
    fi
    archive_args+=(--locked)
    if [[ "$mode" == "lib" ]]; then
        archive_args+=(--lib)
    fi
    archive_args+=(${pkg_args[@]+"${pkg_args[@]}"})
    echo "» cargo nextest archive ${archive_args[*]} --archive-file $archive_to" >&2
    exec cargo nextest archive "${archive_args[@]}" --archive-file "$archive_to"
fi

case "$mode" in
    lib)
        nextest_args=(--lib --no-fail-fast)
        ;;
    full)
        nextest_args=(--no-fail-fast)
        ;;
esac

# CI runners have far fewer real cores than a dev box (this repo's dev
# machines: 16 logical cores) and run the SAME 30+ test binaries under the
# SAME nextest parallelism — CPU-bound / heavily-contended tests (worker
# threads competing with busy-spin reader tasks, WASM cranelift compiles,
# etc.) can legitimately need much longer wall-clock time to make progress
# there, without being deadlocked. `[profile.ci]` in .config/nextest.toml
# already encodes a looser kill threshold (60s x 10 = 600s) for exactly
# this; this wrapper never selected it, so every CI run silently used
# `[profile.default]`'s 180s kill instead, misreporting real contention as
# a hung/failed test. `CI=true` is set automatically by GitHub Actions (and
# most other CI providers) — auto-select the ci profile there unless the
# caller already passed an explicit `--profile`.
if [[ "${CI:-}" == "true" ]] && ! printf '%s\n' "${extra_args[@]:-}" | grep -q -- '--profile'; then
    nextest_args+=(--profile ci)
fi

if [[ -n "$archive_from" ]]; then
    rest=()
    for a in ${extra_args[@]+"${extra_args[@]}"}; do
        # These options are build-only and unsupported while consuming archives.
        [[ "$a" == "--locked" || "$a" == "--lib" ]] && continue
        rest+=("$a")
    done
    run_args=(--no-fail-fast)
    if [[ "${CI:-}" == "true" ]] && ! printf '%s\n' "${rest[@]:-}" | grep -q -- '--profile'; then
        run_args+=(--profile ci)
    fi
    run_args+=(--archive-file "$archive_from" --workspace-remap .)
    echo "» cargo nextest run ${run_args[*]} ${rest[*]:-}" >&2
    exec cargo nextest run "${run_args[@]}" ${rest[@]+"${rest[@]}"}
fi

echo "» cargo nextest run ${nextest_args[*]} ${extra_args[*]:-}" >&2
# The cargo-runner guard (.cargo/config.toml) gates on $NEXTEST, which
# nextest sets itself for every test process it launches — so no
# project-specific escape flag is needed here. Bare `cargo test` (no
# $NEXTEST) is refused; this path always passes.
exec cargo nextest run "${nextest_args[@]}" "${extra_args[@]}"
