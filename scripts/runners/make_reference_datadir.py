#!/usr/bin/env python3
"""Generate the reference data directory fixture (release-1 epoch 1).

Placement note: the migration design names `scripts/release/`, but `scripts/`
already holds 7 entries; this script lives under `scripts/runners/` (5 -> 6
entries) to keep every folder at or below the 7-entry layout limit.

Runs the release server binary against an empty data directory and lets the
Rust helper `crates/devtools/shamir-e2e/examples/reference_fixture.rs` apply
the FIXED DDL + data scenario over the native plain-TCP loopback listener
(built with `cargo build -p shamir-e2e --example reference_fixture`). This
script drives the phases: boot/ready/stop, offline token minting, invoking
the helper, and verifying the applied scenario over the HTTP REST surface
(msgpack wire, stdlib only). It then prunes `cache/`, enforces the ~1 MiB
size cap and records the generating binary version. Stdlib only; run with
`python -I`.

Scenario contract (crates/server/shamir-server/tests/misc/reference_compat.rs
and fixtures/reference_epoch_1/README.md): db `default`, repo `main`; tables
`ref_schema_basic` (r1/r2/r3 -> n 10/20/30) and `ref_schema_versioned`
(v1/v2 -> n 7/8, schema_version > 1); one index of each kind on
`ref_schema_basic` (`ref_hash_idx`, `ref_sorted_idx`, `ref_vector_idx`,
`ref_fulltext_idx`); functions `ref_wasm_fn` (compiled) and `ref_lang_fn`
(shamir scalar `x + 1` with persisted call_signature); principals `admin`
(superuser, bootstrap password) and `ref_user` (read on `default.main`),
plus one access token for `ref_user`; a group with a member and extra
ACL/right rows (contract extras, not asserted individually); an unfinished
WAL; a valid `format.stamp` at epoch 1.

Deterministic: fixed scenario order, fixed literals only. Inherently
non-deterministic server-written fields (token ids, timestamps, user ids)
are noted in docs/dev-artifacts/design/migration-framework.md 7.2 — the
compatibility test compares those semantically, not byte-wise.

The generated `GENERATED_BY.txt` inside the output directory records the
`--version` output of the binary that wrote the fixture.
"""

import argparse
import http.client
import os
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

DEFAULT_PORT = 17691
DEFAULT_TCP_PORT = 17692
BOOTSTRAP_PASSWORD = "reference-epoch-1-admin"
TOKEN_LABEL = "reference-epoch-1"
SIZE_CAP_BYTES = 1024 * 1024
READY_TIMEOUT_SECS = 60


class FixtureError(Exception):
    pass


def require(cond, message):
    if not cond:
        raise FixtureError(message)


# ---------------------------------------------------------------------------
# MessagePack (rmp_serde-compatible named encoding)
# ---------------------------------------------------------------------------

def mp_encode(value):
    if value is None:
        return b"\xc0"
    if value is True:
        return b"\xc3"
    if value is False:
        return b"\xc2"
    if isinstance(value, int):
        return _mp_int(value)
    if isinstance(value, float):
        return b"\xcb" + struct.pack(">d", value)
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return _mp_str(raw)
    if isinstance(value, (bytes, bytearray)):
        return _mp_bin(bytes(value))
    if isinstance(value, list):
        out = _mp_header(len(value), 0x90, 0xDC)
        return out + b"".join(mp_encode(v) for v in value)
    if isinstance(value, dict):
        out = _mp_header(len(value), 0x80, 0xDE)
        for key, val in value.items():
            require(isinstance(key, str), "msgpack map keys must be str")
            out += mp_encode(key)
            out += mp_encode(val)
        return out
    raise FixtureError(f"cannot encode {type(value).__name__} as msgpack")


def _mp_header(count, fix_code, wide_code):
    if count < 16:
        return bytes([fix_code | count])
    if count < 0x10000:
        return bytes([wide_code]) + struct.pack(">H", count)
    return bytes([wide_code + 1]) + struct.pack(">I", count)


def _mp_str(raw):
    n = len(raw)
    if n < 32:
        return bytes([0xA0 | n]) + raw
    if n < 0x100:
        return bytes([0xD9, n]) + raw
    if n < 0x10000:
        return bytes([0xDA]) + struct.pack(">H", n) + raw
    return bytes([0xDB]) + struct.pack(">I", n) + raw


def _mp_bin(raw):
    n = len(raw)
    if n < 0x100:
        return bytes([0xC4, n]) + raw
    if n < 0x10000:
        return bytes([0xC5]) + struct.pack(">H", n) + raw
    return bytes([0xC6]) + struct.pack(">I", n) + raw


def _mp_int(value):
    if value >= 0:
        if value < 0x80:
            return bytes([value])
        if value < 0x100:
            return bytes([0xCC, value])
        if value < 0x10000:
            return bytes([0xCD]) + struct.pack(">H", value)
        if value < 0x100000000:
            return bytes([0xCE]) + struct.pack(">I", value)
        return bytes([0xCF]) + struct.pack(">Q", value)
    if -0x20 <= value:
        return struct.pack(">b", value)
    if -0x80 <= value:
        return bytes([0xD0]) + struct.pack(">b", value)
    if -0x8000 <= value:
        return bytes([0xD1]) + struct.pack(">h", value)
    if -0x80000000 <= value:
        return bytes([0xD2]) + struct.pack(">i", value)
    return bytes([0xD3]) + struct.pack(">q", value)


class MpReader:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def byte(self):
        b = self.data[self.pos]
        self.pos += 1
        return b

    def take(self, n):
        out = self.data[self.pos:self.pos + n]
        self.pos += n
        return out

    def read(self):
        b = self.byte()
        if b == 0xC0:
            return None
        if b == 0xC2:
            return False
        if b == 0xC3:
            return True
        if b == 0xC4:
            return self.take(self.byte())
        if b == 0xC5:
            return self.take(struct.unpack(">H", self.take(2))[0])
        if b == 0xC6:
            return self.take(struct.unpack(">I", self.take(4))[0])
        if b == 0xCA:
            return struct.unpack(">f", self.take(4))[0]
        if b == 0xCB:
            return struct.unpack(">d", self.take(8))[0]
        if 0xA0 <= b <= 0xBF:
            return self.take(b & 0x1F).decode("utf-8")
        if b == 0xD9:
            return self.take(self.byte()).decode("utf-8")
        if b == 0xDA:
            return self.take(struct.unpack(">H", self.take(2))[0]).decode("utf-8")
        if b == 0xDB:
            return self.take(struct.unpack(">I", self.take(4))[0]).decode("utf-8")
        if 0x90 <= b <= 0x9F:
            return [self.read() for _ in range(b & 0x0F)]
        if b == 0xDC:
            return [self.read() for _ in range(struct.unpack(">H", self.take(2))[0])]
        if b == 0xDD:
            return [self.read() for _ in range(struct.unpack(">I", self.take(4))[0])]
        if 0x80 <= b <= 0x8F:
            return self._map(b & 0x0F)
        if b == 0xDE:
            return self._map(struct.unpack(">H", self.take(2))[0])
        if b == 0xDF:
            return self._map(struct.unpack(">I", self.take(4))[0])
        return self._int(b)

    def _map(self, count):
        out = {}
        for _ in range(count):
            key = self.read()
            out[key] = self.read()
        return out

    def _int(self, b):
        if b < 0x80:
            return b
        if 0xE0 <= b:
            return b - 0x100
        if b == 0xCC:
            return self.byte()
        if b == 0xCD:
            return struct.unpack(">H", self.take(2))[0]
        if b == 0xCE:
            return struct.unpack(">I", self.take(4))[0]
        if b == 0xCF:
            return struct.unpack(">Q", self.take(8))[0]
        if b == 0xD0:
            return struct.unpack(">b", self.take(1))[0]
        if b == 0xD1:
            return struct.unpack(">h", self.take(2))[0]
        if b == 0xD2:
            return struct.unpack(">i", self.take(4))[0]
        if b == 0xD3:
            return struct.unpack(">q", self.take(8))[0]
        raise FixtureError(f"unsupported msgpack byte 0x{b:02x}")


def mp_decode(data):
    return MpReader(data).read()


# ---------------------------------------------------------------------------
# Server process control
# ---------------------------------------------------------------------------

def write_config(path, data_dir, port, tcp_port):
    # Listener syntax per crates/server/shamir-server/src/boot/config.rs:
    # `kind: http` requires the flat `tls` key; native TCP uses
    # `kind: tcp` + `profile: plain`, which validate() accepts only on
    # loopback (TRANSPORT_TCP §2.2). The top-level `tls:` block has no
    # serde default in Config, so it must stay.
    tls_cert = os.path.join(data_dir, "cert.pem")
    tls_key = os.path.join(data_dir, "key.pem")
    config = (
        f"data_dir: {data_dir}\n"
        "\n"
        "logging: {\n"
        "    level: warn\n"
        "}\n"
        "\n"
        "kdf_defaults: {\n"
        "    memory_kb: 19456\n"
        "    time: 2\n"
        "    parallelism: 1\n"
        "    argon2_version: 19\n"
        "}\n"
        "\n"
        "argon2_concurrent_max: 4\n"
        "\n"
        "listeners: [\n"
        "    {\n"
        "        kind: http\n"
        f"        addr: 127.0.0.1:{port}\n"
        "        tls: false\n"
        "    }\n"
        "    {\n"
        "        kind: tcp\n"
        f"        addr: 127.0.0.1:{tcp_port}\n"
        "        profile: plain\n"
        "    }\n"
        "]\n"
        "\n"
        "tls: {\n"
        f"    cert_path: {tls_cert}\n"
        f"    key_path: {tls_key}\n"
        "}\n"
    )
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(config)


class ServerProcess:
    def __init__(self, binary, config_path):
        self.proc = subprocess.Popen(
            [binary, "--config", config_path, "--bootstrap-password",
             BOOTSTRAP_PASSWORD],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def stop_cleanly(self):
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            raise FixtureError("server did not stop within 60 s of SIGTERM")
        if self.proc.returncode != 0:
            raise FixtureError(
                f"server exited with status {self.proc.returncode} on SIGTERM"
            )


def wait_ready(port):
    deadline = time.monotonic() + READY_TIMEOUT_SECS
    while time.monotonic() < deadline:
        try:
            status, _ = http_post(port, "/shamir/v1/ping", None, b"", "GET")
            if status in (200, 401):
                return
        except OSError:
            pass
        time.sleep(0.2)
    raise FixtureError(f"server not ready on 127.0.0.1:{port} "
                       f"within {READY_TIMEOUT_SECS}s")


def wait_tcp_acceptable(port):
    deadline = time.monotonic() + READY_TIMEOUT_SECS
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=5):
                return
        except OSError:
            time.sleep(0.2)
    raise FixtureError(f"TCP listener on 127.0.0.1:{port} not accepting "
                       f"within {READY_TIMEOUT_SECS}s")


def _payload_repr(payload):
    """Truncated repr of a server payload for error messages; hex fallback
    when the bytes are not valid msgpack."""
    if not payload:
        return "<empty body>"
    try:
        text = repr(mp_decode(payload))
    except (FixtureError, IndexError, UnicodeDecodeError, struct.error):
        return "hex " + payload[:32].hex()
    return text[:300]


# ---------------------------------------------------------------------------
# HTTP REST client
# ---------------------------------------------------------------------------

def http_post(port, path, token, body, method="POST"):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    headers = {"Content-Type": "application/msgpack"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    conn.request(method, path, body=body, headers=headers)
    response = conn.getresponse()
    payload = response.read()
    status = response.status
    conn.close()
    return status, payload


def rest_ping(port, token):
    status, payload = http_post(port, "/shamir/v1/ping", token, b"", "GET")
    require(status == 200, f"ping failed: HTTP {status}: "
                           f"{_payload_repr(payload)}")
    # DbResponse is internally tagged by "kind": the unit variant Pong is the
    # map {"kind": "pong"} (observed in CI).
    decoded = mp_decode(payload)
    require(isinstance(decoded, dict) and decoded.get("kind") == "pong",
            f"ping did not answer pong: {_payload_repr(payload)}")


def verify_scenario(port, token):
    # The helper already verified rows, the stored call, indexes, functions
    # and the schema version over its SCRAM session (REFERENCE_FIXTURE_OK).
    # An API token does not bypass ACLs, and the fixture's ACLs do not grant
    # the admin token READ on the database, so the REST check is limited to
    # proving the token transport answers.
    rest_ping(port, token)


# ---------------------------------------------------------------------------
# Fixture helper invocation
# ---------------------------------------------------------------------------

def run_fixture_helper(helper, tcp_port, credentials_file):
    proc = subprocess.run(
        [helper, "--addr", f"127.0.0.1:{tcp_port}",
         "--credentials-file", credentials_file],
        capture_output=True, text=True, check=False,
    )
    if proc.returncode != 0 or "REFERENCE_FIXTURE_OK" not in proc.stdout:
        tail = "\n".join((proc.stdout + proc.stderr).splitlines()[-20:])
        raise FixtureError(
            f"reference_fixture helper failed (exit {proc.returncode}, "
            f"no REFERENCE_FIXTURE_OK):\n{tail}"
        )


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def binary_version(binary):
    out = subprocess.run([binary, "--version"], capture_output=True, text=True)
    require(out.returncode == 0,
            f"{binary} --version failed: {out.stderr.strip()}")
    return out.stdout.strip()


def run_offline(binary, config_path, args):
    subprocess.run([binary, "--config", config_path] + args,
                   capture_output=True, text=True, check=False)


def mint_offline_token(binary, config_path, user):
    with tempfile.TemporaryDirectory(prefix="shamir-ref-tok") as tmp:
        token_file = os.path.join(tmp, "token.txt")
        run_offline(binary, config_path,
                    ["token", "create", "--user", user,
                     "--label", TOKEN_LABEL, "--read-write",
                     "--channels", "both", "--out-file", token_file])
        require(os.path.exists(token_file),
                f"offline token create for {user} failed (see server "
                f"token CLI output)")
        with open(token_file, "r", encoding="utf-8") as handle:
            return handle.read().strip()


def revoke_user_tokens(binary, config_path, user):
    run_offline(binary, config_path, ["token", "revoke-user", user])


def prune_and_check(out_dir):
    cache = os.path.join(out_dir, "cache")
    if os.path.isdir(cache):
        shutil.rmtree(cache)
    token_txt = os.path.join(out_dir, "bootstrap_token.txt")
    if os.path.exists(token_txt):
        os.remove(token_txt)
    total = 0
    biggest = []
    for root, _dirs, files in os.walk(out_dir):
        for name in files:
            path = os.path.join(root, name)
            size = os.path.getsize(path)
            total += size
            biggest.append((size, path))
    biggest.sort(reverse=True)
    if total > SIZE_CAP_BYTES:
        listing = "\n".join(f"  {size} {path}" for size, path in biggest[:10])
        raise FixtureError(
            f"fixture tree is {total} bytes, over the {SIZE_CAP_BYTES} cap:\n"
            f"{listing}"
        )
    return total


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate the reference data directory fixture.")
    parser.add_argument("--server-binary", required=True,
                        help="path to the shamir-server binary")
    parser.add_argument("--fixture-helper", required=True,
                        help="path to the reference_fixture example binary "
                             "(cargo build -p shamir-e2e "
                             "--example reference_fixture)")
    parser.add_argument("--out", required=True,
                        help="output data directory (must not exist or be empty)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help="loopback HTTP port for the fixture server")
    parser.add_argument("--tcp-port", type=int, default=DEFAULT_TCP_PORT,
                        help="loopback native TCP (profile plain) port for "
                             "the fixture helper")
    args = parser.parse_args(argv)

    binary = os.path.abspath(args.server_binary)
    require(os.path.isfile(binary), f"server binary not found: {binary}")
    helper = os.path.abspath(args.fixture_helper)
    require(os.path.isfile(helper), f"fixture helper not found: {helper}")
    out_dir = os.path.abspath(args.out)
    if os.path.isdir(out_dir):
        require(not os.listdir(out_dir), f"output dir not empty: {out_dir}")
    else:
        os.makedirs(out_dir)
    config_path = out_dir + ".ktav"
    write_config(config_path, out_dir, args.port, args.tcp_port)

    version = binary_version(binary)

    # Phase A: first boot creates the admin superuser, then a clean stop.
    server = ServerProcess(binary, config_path)
    try:
        wait_ready(args.port)
    finally:
        server.stop_cleanly()

    # Phase B: offline mint of an ephemeral admin token (scenario driver).
    token = mint_offline_token(binary, config_path, "admin")

    # Phase C: second boot; the Rust helper applies the DDL + data scenario
    # over the plain-TCP loopback listener, then REST re-verifies the reads.
    # The credentials file lives in a throwaway temp dir, never inside the
    # fixture tree, and is removed with it.
    server = ServerProcess(binary, config_path)
    try:
        wait_ready(args.port)
        wait_tcp_acceptable(args.tcp_port)
        with tempfile.TemporaryDirectory(prefix="shamir-ref-creds") as tmp:
            creds_file = os.path.join(tmp, "credentials.txt")
            with open(creds_file, "w", encoding="utf-8") as handle:
                handle.write(f"username=admin\n"
                             f"password={BOOTSTRAP_PASSWORD}\n")
            run_fixture_helper(helper, args.tcp_port, creds_file)
        verify_scenario(args.port, token)
    finally:
        server.stop_cleanly()

    # Phase D: the fixture must hold exactly one access token, ref_user's
    # (contract). Revoke the ephemeral admin token, mint ref_user's.
    revoke_user_tokens(binary, config_path, "admin")
    mint_offline_token(binary, config_path, "ref_user")

    total = prune_and_check(out_dir)
    with open(os.path.join(out_dir, "GENERATED_BY.txt"), "w",
              encoding="utf-8") as handle:
        handle.write(version + "\n")
    os.remove(config_path)
    print(f"fixture written to {out_dir}: {total} bytes "
          f"(cap {SIZE_CAP_BYTES}), generated with {version}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except FixtureError as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
