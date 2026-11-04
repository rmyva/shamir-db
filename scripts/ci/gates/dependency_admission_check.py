#!/usr/bin/env python3
"""Dependency admission gate: the dependency cooldown (3 days) with auditable exceptions.

A registry package version may enter a committed Cargo.lock only if it was
published at least `global-min-publish-age` (cooldown.toml) before admission.
"Admission" means: present in the head lockfile, absent from the protected base
(the merge base with master on a PR, the previous master tip on a push).
Presence in a proposed lockfile is never approval and `cargo update --precise`
is a resolution, not an authorisation.

A too-young version is admitted only through an entry in
cooldown-exceptions.toml that names the exact package, source, version and
checksum, the registry publication time, the RUSTSEC advisories it fixes (or the
primary entry it is a lockstep companion of), the approver and an expiry equal
to the maturity date (publication + the configured minimum age). Exceptions waive publication age only.

Stdlib only (Python 3.11+, tomllib). Usage:

    dependency_admission_check.py --base-ref <sha> --advisory-db <dir>
    dependency_admission_check.py --expiry-only
    dependency_admission_check.py --base-ref <sha> --suggest \
        --primary NAME=RUSTSEC-A+RUSTSEC-B ... --reason TEXT \
        --approved-by WHO --approved-on YYYY-MM-DD
"""

import argparse
import datetime
import json
import re
import subprocess
import sys
import time
import tomllib
import urllib.request
from pathlib import Path

LOCKS = ("Cargo.lock", "tools/rust-guard/Cargo.lock")
EXCEPTIONS_FILE = "cooldown-exceptions.toml"
CONFIG_FILE = "cooldown.toml"
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
WARN_DAYS = 7
REQUEST_PAUSE_SECONDS = 1.1

ADVISORY_RE = re.compile(r"^RUSTSEC-\d{4}-\d{4}$")
COMPANION_RE = re.compile(r"^([A-Za-z0-9_-]+)@([0-9][^@\s]*)$")
REQUIRED = (
    "package",
    "version",
    "source",
    "checksum",
    "published",
    "expires",
    "kind",
    "reason",
    "approved_by",
    "approved_on",
)


class Package:
    """One `[[package]]` of a lockfile."""

    def __init__(self, name, version, source, checksum, deps):
        self.name = name
        self.version = version
        self.source = source
        self.checksum = checksum
        self.deps = deps

    @property
    def identity(self):
        return (self.name, self.version, self.source, self.checksum)


def parse_lock(text):
    """Return the list of Package entries of a Cargo.lock text."""
    if not text:
        return []
    data = tomllib.loads(text)
    return [
        Package(
            p["name"],
            p["version"],
            p.get("source", ""),
            p.get("checksum", ""),
            list(p.get("dependencies", [])),
        )
        for p in data.get("package", [])
    ]


def registry_identities(packages):
    return {p.identity for p in packages if p.source == REGISTRY}


def build_graph(packages):
    """Map (name, version) -> set of (name, version) it depends on."""
    versions = {}
    for p in packages:
        versions.setdefault(p.name, set()).add(p.version)
    graph = {}
    for p in packages:
        edges = set()
        for dep in p.deps:
            parts = dep.split(" ")
            name = parts[0]
            if len(parts) >= 2:
                edges.add((name, parts[1]))
            else:
                for version in versions.get(name, ()):
                    edges.add((name, version))
        graph.setdefault((p.name, p.version), set()).update(edges)
    return graph


def depends_on(graph, root, target):
    """True when `root` reaches `target` through the dependency graph."""
    seen = set()
    stack = [root]
    while stack:
        node = stack.pop()
        if node == target:
            return True
        if node in seen:
            continue
        seen.add(node)
        stack.extend(graph.get(node, ()))
    return False


def parse_min_age_days(config_text):
    value = tomllib.loads(config_text).get("registry", {}).get("global-min-publish-age", "")
    match = re.fullmatch(r"\s*(\d+)\s*days?\s*", str(value))
    if not match:
        raise ValueError(f"cooldown.toml: unsupported global-min-publish-age {value!r}")
    return int(match.group(1))


def parse_instant(value):
    """RFC 3339 instant (str or TOML datetime) -> aware UTC datetime, seconds."""
    if isinstance(value, datetime.datetime):
        moment = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        moment = datetime.datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=datetime.timezone.utc)
    return moment.astimezone(datetime.timezone.utc).replace(microsecond=0)


def parse_day(value):
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    return datetime.date.fromisoformat(str(value).strip())


def fetch_published(name, version):
    """Registry publication instant of one crate version (fail closed)."""
    url = f"https://crates.io/api/v1/crates/{name}/{version}"
    request = urllib.request.Request(
        url, headers={"User-Agent": "shamir-db-ci dependency-admission-check"}
    )
    last = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = json.load(response)
            return parse_instant(body["version"]["created_at"])
        except Exception as exc:  # network or schema failure: retry, then fail closed
            last = exc
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"cannot read the publication time of {name} {version}: {last}")


class Registry:
    """Memoising, rate-limited wrapper around `fetch_published`."""

    def __init__(self, fetch=fetch_published, pause=REQUEST_PAUSE_SECONDS):
        self._fetch = fetch
        self._pause = pause
        self._cache = {}

    def published(self, name, version):
        key = (name, version)
        if key not in self._cache:
            if self._cache and self._pause:
                time.sleep(self._pause)
            self._cache[key] = self._fetch(name, version)
        return self._cache[key]


def load_exceptions(text):
    """Parse cooldown-exceptions.toml -> (entries, errors)."""
    if not text:
        return [], []
    data = tomllib.loads(text)
    entries = data.get("exception", [])
    errors = []
    seen = set()
    for index, entry in enumerate(entries, 1):
        label = f"exception #{index} ({entry.get('package', '?')} {entry.get('version', '?')})"
        for field in REQUIRED:
            if field not in entry or entry[field] in ("", None):
                errors.append(f"{label}: missing field `{field}`")
        for field in ("package", "version", "source", "checksum"):
            if "*" in str(entry.get(field, "")):
                errors.append(f"{label}: wildcard in `{field}` is not an exception")
        if entry.get("kind") != "security":
            errors.append(f"{label}: `kind` must be \"security\"")
        has_adv = "advisories" in entry
        has_comp = "companion_of" in entry
        if has_adv == has_comp:
            errors.append(f"{label}: exactly one of `advisories` / `companion_of` is required")
        if has_adv:
            advisories = entry["advisories"]
            if not isinstance(advisories, list) or not advisories:
                errors.append(f"{label}: `advisories` must be a non-empty list")
            else:
                for adv in advisories:
                    if not ADVISORY_RE.match(str(adv)):
                        errors.append(f"{label}: malformed advisory id {adv!r}")
        if has_comp and not COMPANION_RE.match(str(entry["companion_of"])):
            errors.append(f"{label}: `companion_of` must look like name@version")
        key = (entry.get("package"), entry.get("version"), entry.get("source"), entry.get("checksum"))
        if key in seen:
            errors.append(f"{label}: duplicate entry")
        seen.add(key)
    return entries, errors


def entry_identity(entry):
    return (entry.get("package"), entry.get("version"), entry.get("source"), entry.get("checksum"))


def check(base_locks, head_locks, exceptions_text, config_text, today, registry,
          advisory_exists=None, expiry_only=False):
    """Run every rule. Returns (errors, warnings)."""
    errors = []
    warnings = []
    min_age = parse_min_age_days(config_text)
    entries, schema_errors = load_exceptions(exceptions_text)
    errors.extend(schema_errors)

    head_packages = [p for text in head_locks.values() for p in parse_lock(text)]
    base_packages = [p for text in base_locks.values() for p in parse_lock(text)]
    head_ids = registry_identities(head_packages)
    base_ids = registry_identities(base_packages)
    graph = build_graph(head_packages)
    by_identity = {}
    for entry in entries:
        by_identity.setdefault(entry_identity(entry), []).append(entry)

    if not expiry_only:
        for name, version, source, checksum in sorted(head_ids - base_ids):
            try:
                published = registry.published(name, version)
            except RuntimeError as exc:
                errors.append(str(exc))
                continue
            age = today - published.date()
            if age >= datetime.timedelta(days=min_age):
                continue
            matches = by_identity.get((name, version, source, checksum), [])
            if len(matches) != 1:
                errors.append(
                    f"{name} {version} was published {published.isoformat()} "
                    f"({age.days} days ago, minimum {min_age}) and has "
                    f"{'no' if not matches else 'several'} matching entry in {EXCEPTIONS_FILE}"
                )

    primaries = {
        (e.get("package"), e.get("version")): e for e in entries if "advisories" in e
    }
    for entry in entries:
        name, version = entry.get("package"), entry.get("version")
        label = f"{name} {version}"
        if entry_identity(entry) not in head_ids:
            errors.append(f"{label}: unused exception (not in the head lockfile); delete it")
            continue
        try:
            published = registry.published(name, version)
            if parse_instant(entry.get("published")) != published:
                errors.append(
                    f"{label}: `published` {entry.get('published')} differs from the "
                    f"registry value {published.isoformat()}"
                )
            expected = published.date() + datetime.timedelta(days=min_age)
            if parse_day(entry.get("expires")) != expected:
                errors.append(f"{label}: `expires` must be {expected.isoformat()} (published + {min_age} days)")
        except (ValueError, TypeError, RuntimeError) as exc:
            errors.append(f"{label}: unreadable `published`/`expires`: {exc}")
            continue
        expires = parse_day(entry.get("expires"))
        if today > expires:
            errors.append(f"{label}: exception expired on {expires.isoformat()}; the version matured, delete the entry")
        elif (expires - today).days <= WARN_DAYS:
            warnings.append(f"{label}: exception expires on {expires.isoformat()}")
        if "advisories" in entry and advisory_exists is not None:
            for adv in entry.get("advisories", []):
                if not advisory_exists(name, adv):
                    errors.append(f"{label}: advisory {adv} not found for crate {name} in the advisory database")
        if "companion_of" in entry:
            match = COMPANION_RE.match(str(entry["companion_of"]))
            if match:
                primary = (match.group(1), match.group(2))
                if primary not in primaries:
                    errors.append(f"{label}: companion_of {entry['companion_of']} is not an entry with advisories")
                elif not depends_on(graph, primary, (name, version)):
                    errors.append(f"{label}: {entry['companion_of']} does not depend on {name} {version} in the lockfile")
    return errors, warnings


def suggest(base_locks, head_locks, config_text, today, registry, primaries,
            reason, approved_by, approved_on, existing_text=""):
    """Render exception entries for admitted too-young versions that lack one."""
    min_age = parse_min_age_days(config_text)
    entries, _ = load_exceptions(existing_text)
    have = {entry_identity(e) for e in entries}
    head_packages = [p for text in head_locks.values() for p in parse_lock(text)]
    base_packages = [p for text in base_locks.values() for p in parse_lock(text)]
    admitted = registry_identities(head_packages) - registry_identities(base_packages)
    graph = build_graph(head_packages)
    primary_versions = {}
    for p in head_packages:
        if p.name in primaries:
            primary_versions[p.name] = p.version
    out = []
    for name, version, source, checksum in sorted(admitted):
        if (name, version, source, checksum) in have:
            continue
        published = registry.published(name, version)
        if today - published.date() >= datetime.timedelta(days=min_age):
            continue
        expires = published.date() + datetime.timedelta(days=min_age)
        lines = [
            "[[exception]]",
            f'package = "{name}"',
            f'version = "{version}"',
            f'source = "{source}"',
            f'checksum = "{checksum}"',
            f'published = "{published.strftime("%Y-%m-%dT%H:%M:%SZ")}"',
            f'expires = "{expires.isoformat()}"',
            'kind = "security"',
        ]
        if name in primaries:
            advisories = ", ".join(f'"{a}"' for a in primaries[name])
            lines.append(f"advisories = [{advisories}]")
        else:
            owner = next(
                (n for n in sorted(primaries)
                 if n in primary_versions and depends_on(graph, (n, primary_versions[n]), (name, version))),
                None,
            )
            if owner is None:
                lines.append("# TODO: no primary depends on this crate; name its advisory or remove the bump")
            else:
                lines.append(f'companion_of = "{owner}@{primary_versions[owner]}"')
        lines += [f'reason = "{reason}"', f'approved_by = "{approved_by}"', f'approved_on = "{approved_on}"', ""]
        out.append("\n".join(lines))
    return "\n".join(out)


def git_show(ref, path):
    result = subprocess.run(
        ["git", "show", f"{ref}:{path}"], capture_output=True, text=True, encoding="utf-8"
    )
    return result.stdout if result.returncode == 0 else ""


def read_text(path):
    file = Path(path)
    return file.read_text(encoding="utf-8") if file.exists() else ""


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--base-ref", help="git ref of the protected base to diff the lockfiles against")
    parser.add_argument("--advisory-db", help="checkout of rustsec/advisory-db")
    parser.add_argument("--expiry-only", action="store_true", help="check exception expiry only (scheduled run)")
    parser.add_argument("--suggest", action="store_true", help="print entries for admitted too-young versions")
    parser.add_argument("--primary", action="append", default=[], help="NAME=RUSTSEC-A+RUSTSEC-B (with --suggest)")
    parser.add_argument("--reason", default="security fix")
    parser.add_argument("--approved-by", default="")
    parser.add_argument("--approved-on", default="")
    args = parser.parse_args(argv)

    today = datetime.datetime.now(datetime.timezone.utc).date()
    config_text = read_text(CONFIG_FILE)
    exceptions_text = read_text(EXCEPTIONS_FILE)
    head_locks = {path: read_text(path) for path in LOCKS}
    registry = Registry()

    if args.suggest:
        if not args.base_ref:
            parser.error("--suggest needs --base-ref")
        base_locks = {path: git_show(args.base_ref, path) for path in LOCKS}
        primaries = {}
        for item in args.primary:
            name, _, advisories = item.partition("=")
            primaries[name] = advisories.split("+")
        print(suggest(base_locks, head_locks, config_text, today, registry, primaries,
                      args.reason, args.approved_by, args.approved_on, exceptions_text))
        return 0

    if args.expiry_only:
        base_locks = {path: "" for path in LOCKS}
        advisory_exists = None
    else:
        if not args.base_ref or not args.advisory_db:
            parser.error("--base-ref and --advisory-db are required (or use --expiry-only)")
        base_locks = {path: git_show(args.base_ref, path) for path in LOCKS}
        root = Path(args.advisory_db)

        def advisory_exists(package, advisory):
            return (root / "crates" / package / f"{advisory}.md").is_file()

    errors, warnings = check(base_locks, head_locks, exceptions_text, config_text, today,
                             registry, advisory_exists, expiry_only=args.expiry_only)
    for warning in warnings:
        print(f"::warning::{warning}")
    for error in errors:
        print(f"::error::{error}")
    if errors:
        print(f"dependency admission: {len(errors)} problem(s)")
        return 1
    print("dependency admission: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
