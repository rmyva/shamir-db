#!/usr/bin/env python3
"""Assert engine and forwarded query/validator-crate features match a matrix cell.

Why
    The ``engine-features`` CI job compiles one isolated cell per feature
    combination, but ``cargo check`` passing does not PROVE which features
    were enabled.  ``cargo metadata`` resolves the whole workspace graph and
    (with ``--all-targets`` cells) unifies dev-dependency and workspace
    features back in — CI round N proved exactly that: the cell labelled
    ``--no-default-features`` still resolved
    ``{default, all-backends, fjall, test-util}``.  And the previous
    approach here (``cargo tree -p shamir-engine``) was worse still: the
    ``-p`` ROOT's own features are never printed, so the no-default cell
    passed vacuously.

    This script therefore asks cargo with a THROWAWAY CONSUMER crate: a
    temp package ``shamir-engine-feature-probe`` depending on the engine
    by PATH with EXACTLY the cell's feature requests (and
    ``default-features = false`` when the cell says so), with
    ``[workspace]`` so no workspace unification can interfere.  It then
    runs::

        cargo tree --manifest-path <tmp>/Cargo.toml \
            -e normal,build,features --prefix none

    In that output the CONSUMER's dependency's enabled features appear as
    lines shaped::

        shamir-engine feature "default"
        shamir-engine feature "fjall" (*)

    (the ``(*)`` suffix marks repeat participants; it is stripped).  Lines
    for OTHER packages (the probe root itself, ``shamir-storage feature
    "fjall"``, ...) are ignored.  That set IS the engine's enabled feature
    set under a consumer with exactly the cell's requests.

CLI
    ``features_matrix_check.py --expected <cell-id>
    [--manifest-path crates/engine/shamir-engine/Cargo.toml] [--flags "<flags>"]``

    ``--flags`` takes the same feature flags the CI cell passes to cargo
    (``--no-default-features``, ``--features a,b``); they shape the probe
    manifest's dependency declaration.

Cells and their expected sets (``default`` INCLUDED — cargo tree names it
explicitly when defaults are on):

    no-default      {}
    fjall           {fjall}
    test-util       {test-util}
    fjall-test-util {fjall, test-util}
    default-plus    {default, all-backends, fjall, test-util, fts_indexed_long}
    loom            {default, all-backends, fjall, loom}

Exit codes: 0 resolved set matches, 1 mismatch (the COMPLETE cargo tree
stdout is printed to the log first, so the next CI round shows the
culprit), 2 fail-closed (unknown cell id, malformed flags, cargo
subprocess failure — stderr excerpt included, EMPTY tree output, no
engine package at all in the output, no engine feature lines while a
non-empty set is expected — an empty resolution must never pass
vacuously — or temp-dir/IO errors).
Python 3 standard library only; ``python3 -I`` compatible.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile

PACKAGE_NAME = "shamir-engine"
PROBE_PACKAGE_NAME = "shamir-engine-feature-probe"
TREE_ARGS = (
    "-e",
    "normal,build,features",
    "--prefix",
    "none",
)
# NOTE: no --depth limit — depth 1 truncated the engine's INTERNAL feature
# expansion (default -> all-backends -> fjall), so only requested features
# appeared; the default (full) depth shows the complete closure.
TREE_TIMEOUT_SECONDS = 600  # generous: a fresh probe resolves/compiles metadata

# e.g.  shamir-engine feature "default" (*)   |  shamir-engine feature fjall
FEATURE_LINE_RE = re.compile(
    r"^" + re.escape(PACKAGE_NAME) + r"\s+feature\s+[\"']?([A-Za-z0-9_+.-]+)[\"']?"
    r"\s*(?:\(\*\))?\s*$"
)
# the engine package must APPEAR in the tree at all (root line
# ``shamir-engine vX.Y.Z``, a feature line, ...) before we conclude anything
ENGINE_PRESENT_RE = re.compile(r"^" + re.escape(PACKAGE_NAME) + r"(\s|$)", re.M)

EXPECTED: dict[str, set[str]] = {
    "no-default": set(),
    "fjall": {"fjall"},
    "test-util": {"test-util"},
    "fjall-test-util": {"fjall", "test-util"},
    "default-plus": {
        "default",
        "all-backends",
        "fjall",
        "test-util",
        "fts_indexed_long",
    },
    "loom": {"default", "all-backends", "fjall", "loom"},
}

FORWARDED = {
    # the repo crate defines no `default` feature, but the facade depends on it
    # with default features on, so `cargo tree` still lists `default`; the
    # facade forwards `fjall` (also through its default `all-backends`) and
    # `test-util`
    "shamir-engine-repo": {
        cell: {"default"}
        | ({"fjall"} if "fjall" in EXPECTED[cell] else set())
        | ({"test-util"} if "test-util" in EXPECTED[cell] else set())
        for cell in EXPECTED
    },
    "shamir-engine-table": {
        cell: {"default"}
        | ({"fjall"} if "fjall" in EXPECTED[cell] else set())
        | ({"test-util"} if "test-util" in EXPECTED[cell] else set())
        | ({"loom"} if "loom" in EXPECTED[cell] else set())
        for cell in EXPECTED
    },
    "shamir-engine-tx": {
        cell: {"default"}
        | ({"fjall"} if "fjall" in EXPECTED[cell] else set())
        | ({"test-util"} if "test-util" in EXPECTED[cell] else set())
        for cell in EXPECTED
    },
    "shamir-engine-query": {
        cell: {"default"} | ({"test-util"} if cell in {"test-util", "fjall-test-util", "default-plus"} else set())
        for cell in EXPECTED
    },
    "shamir-engine-validator": {
        cell: {"default"} | ({"test-util"} if cell in {"test-util", "fjall-test-util", "default-plus"} else set())
        for cell in EXPECTED
    }
}

DEFAULT_RUNNER = subprocess.run  # replaced by tests


def parse_flags(flags: str) -> list[str]:
    """Split the CI cell's flags into their feature-shaping tokens.

    ``--no-default-features`` and ``--features <list>`` /
    ``--features=<list>`` are recognized (lists stay comma joined — the
    manifest embeds them verbatim); any other token is rejected as
    malformed for this resolver's purposes.
    """
    out: list[str] = []
    tokens = flags.split()
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok == "--no-default-features":
            out.append(tok)
        elif tok.startswith("--features="):
            out.extend(["--features", tok.split("=", 1)[1]])
        elif tok == "--features":
            i += 1
            if i >= len(tokens):
                raise ValueError("--features given without a value")
            out.extend([tok, tokens[i]])
        else:
            raise ValueError("malformed flag %r (only --no-default-features "
                             "and --features[=]<list> are accepted)" % tok)
        i += 1
    return out


def probe_manifest(engine_dir: str, flags: list[str]) -> str:
    """Render the throwaway consumer crate's ``Cargo.toml``.

    ``default-features = false`` iff the flags contain
    ``--no-default-features``; ``features = [...]`` from the cell's
    ``--features`` lists.  ``[workspace]`` detaches the probe from any
    workspace so feature unification cannot reach in.
    """
    no_default = "--no-default-features" in flags
    requested: list[str] = []
    for i, tok in enumerate(flags):
        if tok == "--features":
            requested.extend(flags[i + 1].split(","))
        elif tok.startswith("--features="):
            requested.extend(tok.split("=", 1)[1].split(","))
    requested = [f for f in requested if f]
    dep = 'shamir-engine = { path = %r' % engine_dir.replace("\\", "/")
    if no_default:
        dep += ", default-features = false"
    if requested:
        dep += ", features = [%s]" % ", ".join('"%s"' % f for f in requested)
    dep += " }"
    return (
        "[package]\n"
        'name = "%s"\n'
        'version = "0.0.0"\n'
        'edition = "2021"\n'
        "\n"
        "[dependencies]\n"
        "%s\n"
        "\n"
        "[workspace]\n" % (PROBE_PACKAGE_NAME, dep)
    )


def parse_tree_features(tree_text: str) -> set[str]:
    """Extract the engine's enabled features from ``cargo tree`` stdout.

    Only lines shaped ``shamir-engine feature "<name>" [ (*) ]`` count;
    every other line (the probe root, other packages' feature lines,
    dependency edges, blank lines) is ignored.  The ``(*)`` repeat marker
    is stripped.
    """
    features: set[str] = set()
    for line in tree_text.splitlines():
        m = FEATURE_LINE_RE.match(line)
        if m:
            features.add(m.group(1))
    return features


def run_cargo_tree(tmp_dir: str, runner) -> str:
    """Run ``cargo tree`` on the probe manifest; return stdout text."""
    cmd = ["cargo", "tree", "--manifest-path",
           os.path.join("Cargo.toml"), *TREE_ARGS]
    try:
        proc = runner(
            cmd,
            capture_output=True,
            text=True,
            cwd=tmp_dir,
            timeout=TREE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise ValueError(
            "cargo tree timed out after %ds" % TREE_TIMEOUT_SECONDS
        ) from exc
    except OSError as exc:
        raise ValueError("cannot run cargo: %s" % exc) from exc
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip().splitlines()
        excerpt = " | ".join(stderr[-3:]) or "(no stderr)"
        raise ValueError(
            "cargo tree failed (exit %d): %s" % (proc.returncode, excerpt)
        )
    return proc.stdout


def main(
    argv: list[str] | None = None,
    *,
    tree_text: str | None = None,
    runner=DEFAULT_RUNNER,
) -> int:
    """Entry point; ``tree_text`` and ``runner`` inject cargo output in tests.

    With ``tree_text`` given no temp dir is created and cargo is not run.
    With ``runner`` given the probe manifest is still generated (in a temp
    dir whose path reaches the runner via ``cwd``) so tests can assert its
    content without cargo.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--expected",
        required=True,
        metavar="CELL-ID",
        help="matrix cell id to assert (one of: %s)" % ", ".join(sorted(EXPECTED)),
    )
    parser.add_argument(
        "--manifest-path",
        default="crates/engine/shamir-engine/Cargo.toml",
        help="path to the engine manifest",
    )
    parser.add_argument(
        "--flags",
        default="",
        help="the cell's cargo feature flags (e.g. '--no-default-features --features fjall')",
    )
    args = parser.parse_args(argv)

    tmp_dir = None
    try:
        if args.expected not in EXPECTED:
            raise ValueError(
                "unknown cell id %r (expected one of: %s)"
                % (args.expected, ", ".join(sorted(EXPECTED)))
            )
        expected_set = EXPECTED[args.expected]
        flags = parse_flags(args.flags)
        if tree_text is None:
            engine_dir = os.path.dirname(os.path.abspath(args.manifest_path))
            if not os.path.isdir(engine_dir):
                raise ValueError(
                    "engine directory not found: %s" % engine_dir
                )
            tmp_dir = tempfile.mkdtemp(prefix="shamir-engine-feature-probe-")
            with open(os.path.join(tmp_dir, "Cargo.toml"), "w",
                      encoding="utf-8", newline="\n") as fh:
                fh.write(probe_manifest(engine_dir, flags))
            os.mkdir(os.path.join(tmp_dir, "src"))
            with open(os.path.join(tmp_dir, "src", "lib.rs"), "w",
                      encoding="utf-8", newline="\n") as fh:
                fh.write("pub fn probe() {}\n")
            tree_text = run_cargo_tree(tmp_dir, runner)
        if not (tree_text or "").strip():
            raise ValueError(
                "cargo tree produced no output (empty resolution — "
                "refusing to pass vacuously)"
            )
        if not ENGINE_PRESENT_RE.search(tree_text):
            raise ValueError(
                "%s does not appear in the probe's cargo tree output at all "
                "(dependency broken? refusing to pass vacuously)"
                % PACKAGE_NAME
            )
        forwarded_resolved = {}
        for package in FORWARDED:
            if not re.search(r"^" + re.escape(package) + r"(\s|$)", tree_text, re.M):
                raise ValueError("%s does not appear in the probe's cargo tree output at all" % package)
            pattern = re.compile(
                r"^" + re.escape(package) + r"\s+feature\s+[\"']?([A-Za-z0-9_+.-]+)[\"']?"
                r"\s*(?:\(\*\))?\s*$"
            )
            forwarded_resolved[package] = {m.group(1) for line in tree_text.splitlines()
                                           if (m := pattern.match(line))}
        resolved = parse_tree_features(tree_text)
        if resolved != expected_set and expected_set and not resolved:
            raise ValueError(
                "no %s feature lines found in cargo tree output "
                "(cannot verify expected %s)"
                % (PACKAGE_NAME, sorted(expected_set))
            )
    except (OSError, ValueError) as exc:
        print("features_matrix_check.py: %s" % exc, file=sys.stderr)
        return 2
    finally:
        if tmp_dir is not None:
            shutil.rmtree(tmp_dir, ignore_errors=True)  # best-effort cleanup

    if resolved != expected_set:
        print(
            "features_matrix_check.py: feature resolution mismatch for cell "
            "%r (flags: %s)\n  resolved: %s\n  expected: %s"
            % (
                args.expected,
                args.flags or "(none)",
                sorted(resolved),
                sorted(expected_set),
            ),
            file=sys.stderr,
        )
        print("--- complete cargo tree stdout ---", file=sys.stderr)
        print(tree_text or "(empty)", file=sys.stderr)
        print("--- end cargo tree stdout ---", file=sys.stderr)
        return 1
    for package, cells in FORWARDED.items():
        actual, expected = forwarded_resolved[package], cells[args.expected]
        if actual != expected:
            print("features_matrix_check.py: feature resolution mismatch for cell "
                  "%r (flags: %s), package %s\n  resolved: %s\n  expected: %s"
                  % (args.expected, args.flags or "(none)", package, sorted(actual), sorted(expected)),
                  file=sys.stderr)
            print("--- complete cargo tree stdout ---", file=sys.stderr)
            print(tree_text or "(empty)", file=sys.stderr)
            print("--- end cargo tree stdout ---", file=sys.stderr)
            return 1
    print(
        "features_matrix_check.py: cell %r OK (resolved %s)"
        % (args.expected, sorted(resolved))
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
