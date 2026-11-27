"""unittest suite for scripts/ci/gates/features_matrix_check.py.

Tests the THROWAWAY CONSUMER CRATE resolver: realistic ``cargo tree``
stdout from the probe (the ``shamir-engine-feature-probe`` root line,
engine feature lines with ``(*)`` repeat markers, and decoy
``shamir-storage feature "fjall"`` lines that MUST be ignored) injected
via the ``tree_text`` keyword, plus an injected subprocess runner that
captures the generated probe manifest — no real cargo, no network.
"""

import contextlib
import io
import os
import sys
import unittest

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "gates",
    ),
)

import features_matrix_check as fmc  # noqa: E402


def tree_text(engine_features, table_features=("default",)):
    """Build realistic captured-shape probe ``cargo tree`` stdout.

    Mirrors ``cargo tree --manifest-path <probe>/Cargo.toml -e
    normal,build,features --prefix none``: the probe root line
    first, then the engine's enabled features as ``shamir-engine feature
    "<name>"`` lines (some with the ``(*)`` repeat marker), interleaved
    with dependency edges and OTHER packages' feature lines which the
    parser must ignore.
    """
    lines = ["shamir-engine-feature-probe v0.0.0 (*)"]
    for i, feat in enumerate(sorted(engine_features)):
        lines.append('shamir-engine feature "%s"%s' % (feat, " (*)" if i % 2 else ""))
        # a dependency edge and another package's feature line follow —
        # neither may be picked up as an engine feature
        lines.append("shamir-storage v0.1.0")
        lines.append('shamir-storage feature "fjall"')
    if not lines[1:]:
        # no-default cell: the probe root, the engine's bare dependency
        # edge (present, but zero feature lines), and another package
        lines.extend(["shamir-engine v0.1.0", "shamir-storage v0.1.0"])
    # Both forwarded dependencies appear with defaults, and optionally test-util.
    for package in ("shamir-engine-query", "shamir-engine-validator"):
        lines.append(package + " v0.1.0")
        lines.append('%s feature "default"' % package)
        if "test-util" in engine_features:
            lines.append('%s feature "test-util"' % package)
    # The repo crate has no `default` feature of its own, but the dependency
    # edge enables default features, so `cargo tree` lists `default`; the
    # facade forwards fjall/test-util.
    lines.append("shamir-engine-repo v0.1.0")
    lines.append('shamir-engine-repo feature "default"')
    if "fjall" in engine_features:
        lines.append('shamir-engine-repo feature "fjall"')
    if "test-util" in engine_features:
        lines.append('shamir-engine-repo feature "test-util"')
    # The table package's features are supplied separately from engine_features.
    lines.append("shamir-engine-table v0.1.0")
    for feat in ("default", "fjall", "test-util", "loom"):
        if feat in table_features:
            lines.append('shamir-engine-table feature "%s"' % feat)
    # The tx crate's features are independently tracked from engine features.
    lines.append("shamir-engine-tx v0.1.0")
    for feat in ("default", "fjall", "test-util"):
        if feat == "default" or feat in engine_features:
            lines.append('shamir-engine-tx feature "%s"' % feat)
    return "\n".join(lines) + "\n"


def runner_ok(engine_features, captured=None, table_features=("default",)):
    """A subprocess.run stand-in returning captured-shape tree output.

    Reads the generated probe manifest from the ``cwd`` the script passes
    and records ``{"cmd": ..., "cwd": ..., "manifest": ...}`` into
    ``captured`` when given.
    """

    class Proc:
        returncode = 0
        stderr = ""
        stdout = tree_text(engine_features, table_features)

    def run(cmd, capture_output=True, text=True, cwd=None, timeout=None):
        manifest = ""
        manifest_path = os.path.join(cwd or "", "Cargo.toml")
        if cwd and os.path.exists(manifest_path):
            with open(manifest_path, encoding="utf-8") as fh:
                manifest = fh.read()
        if captured is not None:
            captured["cmd"] = cmd
            captured["cwd"] = cwd
            captured["manifest"] = manifest
        return Proc()

    return run


def runner_fail():
    """A subprocess.run stand-in simulating a cargo failure."""

    class Proc:
        returncode = 101
        stdout = ""
        stderr = "error: failed to select a version for `shamir-engine`.\n"

    def run(cmd, capture_output=True, text=True, cwd=None, timeout=None):
        return Proc()

    return run


def run_main(argv=(), tree=None, runner=None):
    """Run fmc.main with injected output; return (exit_code, stdout, stderr)."""
    kwargs = {}
    if tree is not None:
        kwargs["tree_text"] = tree
    if runner is not None:
        kwargs["runner"] = runner
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = fmc.main(list(argv), **kwargs)
    return code, out.getvalue(), err.getvalue()


class ExpectedSetsTests(unittest.TestCase):
    def test_default_is_included_in_expected_sets(self):
        self.assertEqual(fmc.EXPECTED["no-default"], set())
        self.assertEqual(fmc.EXPECTED["fjall"], {"fjall"})
        self.assertEqual(fmc.EXPECTED["test-util"], {"test-util"})
        self.assertEqual(fmc.EXPECTED["fjall-test-util"], {"fjall", "test-util"})
        self.assertEqual(
            fmc.EXPECTED["default-plus"],
            {"default", "all-backends", "fjall", "test-util", "fts_indexed_long"},
        )
        self.assertEqual(
            fmc.EXPECTED["loom"], {"default", "all-backends", "fjall", "loom"}
        )

    def test_parse_flags_passthrough_and_malformed(self):
        self.assertEqual(
            fmc.parse_flags("--no-default-features --features fjall,test-util"),
            ["--no-default-features", "--features", "fjall,test-util"],
        )
        self.assertEqual(
            fmc.parse_flags("--features=test-util"),
            ["--features", "test-util"],
        )
        with self.assertRaises(ValueError):
            fmc.parse_flags("--features")
        with self.assertRaises(ValueError):
            fmc.parse_flags("--locked")

    def test_parser_ignores_probe_root_other_packages_and_star_marker(self):
        text = (
            "shamir-engine-feature-probe v0.0.0 (*)\n"
            "shamir-engine feature \"default\" (*)\n"
            "shamir-engine feature 'fjall'\n"
            "shamir-engine feature test-util (*)\n"
            "shamir-storage feature \"fjall\" (*)\n"
            "shamir-storage\n"
            "\n"
            "shamir-foundation v0.1.0\n"
        )
        self.assertEqual(
            fmc.parse_tree_features(text),
            {"default", "fjall", "test-util"},
        )


class ProbeManifestTests(unittest.TestCase):
    """The generated throwaway consumer manifest shapes the cell flags."""

    def test_manifest_plain_dependency_without_flags(self):
        text = fmc.probe_manifest("/abs/crates/engine/shamir-engine", [])
        self.assertIn('name = "shamir-engine-feature-probe"', text)
        self.assertIn("edition = \"2021\"", text)
        self.assertIn("[workspace]", text)
        self.assertIn("path = '/abs/crates/engine/shamir-engine'", text)
        self.assertNotIn("default-features", text)
        self.assertNotIn("features = [", text)

    def test_manifest_no_default_features_flag(self):
        text = fmc.probe_manifest(
            "/abs/crates/engine/shamir-engine", ["--no-default-features"]
        )
        self.assertIn("default-features = false", text)
        self.assertNotIn("features = [", text)

    def test_manifest_features_list(self):
        text = fmc.probe_manifest(
            "/abs/crates/engine/shamir-engine",
            ["--no-default-features", "--features", "fjall,test-util"],
        )
        self.assertIn("default-features = false", text)
        self.assertIn('features = ["fjall", "test-util"]', text)

    def test_manifest_features_equals_form(self):
        text = fmc.probe_manifest("/abs/e", ["--features=test-util"])
        self.assertNotIn("default-features", text)
        self.assertIn('features = ["test-util"]', text)

    def test_generated_manifest_via_runner_injection(self):
        """The temp Cargo.toml actually written matches the cell's flags."""
        captured = {}
        code, _, err = run_main(
            [
                "--expected",
                "fjall-test-util",
                "--manifest-path",
                os.path.join(os.path.abspath("crates"), "engine", "shamir-engine", "Cargo.toml"),
                "--flags=--no-default-features --features fjall,test-util",
            ],
            runner=runner_ok(
                ["fjall", "test-util"],
                captured=captured,
                table_features={"default", "fjall", "test-util"},
            ),
        )
        self.assertEqual(code, 0, err)
        self.assertIn("default-features = false", captured["manifest"])
        self.assertIn('features = ["fjall", "test-util"]', captured["manifest"])
        self.assertIn("[workspace]", captured["manifest"])
        # the manifest lives in the temp dir handed to cargo as cwd
        self.assertTrue(captured["cwd"])
        self.assertEqual(
            captured["cmd"],
            [
                "cargo",
                "tree",
                "--manifest-path",
                "Cargo.toml",
                "-e",
                "normal,build,features",
                "--prefix",
                "none",
            ],
        )

    def test_temp_dir_is_cleaned_up(self):
        captured = {}
        run_main(
            ["--expected", "fjall", "--flags=--features fjall"],
            runner=runner_ok(["fjall"], captured=captured),
        )
        self.assertFalse(os.path.exists(captured["cwd"]))


class MainCellTests(unittest.TestCase):
    """Every cell id matches the captured cargo-resolved feature set."""

    CASES = [
        ("no-default", "--no-default-features", []),
        ("fjall", "--no-default-features --features fjall", ["fjall"]),
        ("test-util", "--no-default-features --features test-util", ["test-util"]),
        (
            "fjall-test-util",
            "--no-default-features --features fjall,test-util",
            ["fjall", "test-util"],
        ),
        (
            "default-plus",
            "--features test-util,fts_indexed_long",
            ["default", "all-backends", "fjall", "test-util", "fts_indexed_long"],
        ),
        (
            "loom",
            "--features loom",
            ["default", "all-backends", "fjall", "loom"],
        ),
    ]

    def test_every_cell_matches(self):
        for cell, flags, features in self.CASES:
            with self.subTest(cell=cell):
                code, out, _ = run_main(
                    ["--expected", cell, "--flags=" + flags],
                    tree=tree_text(
                        features,
                        {"default"} | (set(features) & {"fjall", "test-util", "loom"}),
                    ),
                    runner=runner_ok(features),
                )
                self.assertEqual(code, 0, out)
                self.assertIn("OK", out)

    def test_mismatch_exits_1_and_prints_tree(self):
        code, _, err = run_main(
            ["--expected", "no-default", "--flags=--features fjall"],
            tree=tree_text(["fjall"]),
        )
        self.assertEqual(code, 1)
        self.assertIn("mismatch", err)
        self.assertIn("'fjall'", err)
        # the COMPLETE cargo tree stdout is printed so the next CI round
        # shows the culprit
        self.assertIn("complete cargo tree stdout", err)
        self.assertIn("shamir-engine-feature-probe v0.0.0 (*)", err)
        self.assertIn('shamir-engine feature "fjall"', err)
        self.assertIn('shamir-storage feature "fjall"', err)


class ForwardedFeatureTests(unittest.TestCase):
    def test_missing_query_package_exits_2(self):
        tree = "".join(
            line + "\n"
            for line in tree_text([]).splitlines()
            if not line.startswith("shamir-engine-query")
        )
        code, _, err = run_main(["--expected", "no-default"], tree=tree)
        self.assertEqual(code, 2)
        self.assertIn("shamir-engine-query does not appear", err)

    def test_missing_validator_package_exits_2(self):
        tree = "".join(
            line + "\n"
            for line in tree_text([]).splitlines()
            if not line.startswith("shamir-engine-validator")
        )
        code, _, err = run_main(["--expected", "no-default"], tree=tree)
        self.assertEqual(code, 2)
        self.assertIn("shamir-engine-validator does not appear", err)

    def test_missing_repo_package_exits_2(self):
        tree = "".join(
            line + "\n"
            for line in tree_text([]).splitlines()
            if not line.startswith("shamir-engine-repo")
        )
        code, _, err = run_main(["--expected", "no-default"], tree=tree)
        self.assertEqual(code, 2)
        self.assertIn("shamir-engine-repo does not appear", err)

    def test_missing_table_package_exits_2(self):
        tree = "".join(
            line + "\n"
            for line in tree_text([]).splitlines()
            if not line.startswith("shamir-engine-table")
        )
        code, _, err = run_main(["--expected", "no-default"], tree=tree)
        self.assertEqual(code, 2)
        self.assertIn("shamir-engine-table does not appear", err)

    def test_table_test_util_forwarded_mismatch_exits_1(self):
        tree = tree_text(["test-util"], {"default", "test-util"}).replace(
            'shamir-engine-table feature "test-util"\n', ""
        )
        code, _, err = run_main(["--expected", "test-util"], tree=tree)
        self.assertEqual(code, 1)
        self.assertIn("shamir-engine-table", err)
        self.assertIn("complete cargo tree stdout", err)

    def test_table_loom_forwarded_mismatch_exits_1(self):
        tree = tree_text(
            ["default", "all-backends", "fjall", "loom"],
            {"default", "fjall", "loom"},
        ).replace(
            'shamir-engine-table feature "loom"\n', ""
        )
        code, _, err = run_main(["--expected", "loom"], tree=tree)
        self.assertEqual(code, 1)
        self.assertIn("shamir-engine-table", err)
        self.assertIn("complete cargo tree stdout", err)

    def test_tx_test_util_forwarded_mismatch_exits_1(self):
        tree = tree_text(["test-util"]).replace(
            'shamir-engine-tx feature "test-util"\n', ""
        )
        code, _, err = run_main(["--expected", "test-util"], tree=tree)
        self.assertEqual(code, 1)
        self.assertIn("shamir-engine-tx", err)

    def test_missing_tx_package_exits_2(self):
        tree = "\n".join(
            line for line in tree_text([]).splitlines()
            if not line.startswith("shamir-engine-tx")
        )
        code, _, err = run_main(["--expected", "no-default"], tree=tree)
        self.assertEqual(code, 2)
        self.assertIn("shamir-engine-tx does not appear", err)

    def test_validator_forwarded_mismatch_exits_1(self):
        tree = tree_text(["test-util"], {"default", "test-util"}).replace('shamir-engine-validator feature "test-util"\n', "")
        code, _, err = run_main(["--expected", "test-util"], tree=tree)
        self.assertEqual(code, 1)
        self.assertIn("shamir-engine-validator", err)
        self.assertIn("complete cargo tree stdout", err)

    def test_forwarded_mismatch_exits_1(self):
        tree = tree_text(["test-util"], {"default", "test-util"}).replace('shamir-engine-query feature "test-util"\n', "")
        code, _, err = run_main(["--expected", "test-util"], tree=tree)
        self.assertEqual(code, 1)
        self.assertIn("shamir-engine-query", err)
        self.assertIn("complete cargo tree stdout", err)


class MainErrorTests(unittest.TestCase):
    def test_unknown_cell_exits_2(self):
        code, _, err = run_main(["--expected", "bogus"], tree=tree_text([]))
        self.assertEqual(code, 2)
        self.assertIn("unknown cell id", err)

    def test_malformed_flag_exits_2(self):
        code, _, err = run_main(
            ["--expected", "fjall", "--flags=--locked"], tree=tree_text(["fjall"])
        )
        self.assertEqual(code, 2)
        self.assertIn("malformed flag", err)

    def test_empty_output_exits_2(self):
        code, _, err = run_main(["--expected", "fjall"], tree="")
        self.assertEqual(code, 2)
        self.assertIn("empty resolution", err)

    def test_whitespace_output_exits_2(self):
        code, _, err = run_main(["--expected", "fjall"], tree="\n  \n")
        self.assertEqual(code, 2)
        self.assertIn("empty resolution", err)

    def test_engine_absent_exits_2_even_for_empty_expected(self):
        # the no-default cell must NOT pass vacuously: without ANY engine
        # line in the output the check fails closed
        code, _, err = run_main(
            ["--expected", "no-default", "--flags=--no-default-features"],
            tree="shamir-engine-feature-probe v0.0.0 (*)\nshamir-storage v0.1.0\n",
        )
        self.assertEqual(code, 2)
        self.assertIn("does not appear in the probe's cargo tree output", err)

    def test_engine_present_no_feature_lines_passes_for_empty_expected(self):
        # the legitimate no-default resolution: engine present (its root
        # line) but zero feature lines — matches the empty expected set
        code, out, _ = run_main(
            ["--expected", "no-default", "--flags=--no-default-features"],
            tree=tree_text([]),
        )
        self.assertEqual(code, 0, out)
        self.assertIn("OK", out)

    def test_unparseable_output_exits_2_for_nonempty_expected(self):
        # renamed/misparsed invocation: engine package present but no
        # feature lines while a non-empty set is expected
        code, _, err = run_main(
            ["--expected", "fjall"],
            tree='shamir-engine v0.1.0\n' + tree_text([]) + 'shamir-storage feature "fjall" (*)\n',
        )
        self.assertEqual(code, 2)
        self.assertIn("no shamir-engine feature lines", err)

    def test_injected_cargo_failure_exits_2(self):
        code, _, err = run_main(["--expected", "fjall"], runner=runner_fail())
        self.assertEqual(code, 2)
        self.assertIn("cargo tree failed", err)
        self.assertIn("failed to select a version", err)

    def test_missing_engine_dir_exits_2(self):
        code, _, err = run_main(
            ["--expected", "fjall",
             "--manifest-path", "Z:/definitely/not/here/Cargo.toml"],
            runner=runner_ok(["fjall"]),
        )
        self.assertEqual(code, 2)
        self.assertIn("engine directory not found", err)


if __name__ == "__main__":
    unittest.main()
