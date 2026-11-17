#!/usr/bin/env python3
"""Generate the reference data directory fixture (release-1 epoch 1).

Placement note: the migration design names `scripts/release/`, but `scripts/`
already holds 7 entries; this script lives under `scripts/runners/` (5 -> 6
entries) to keep every folder at or below the 7-entry layout limit.

Runs the release server binary against an empty data directory, applies a
FIXED scenario over the documented HTTP REST / native-token-WebSocket
surface (msgpack wire, stdlib only), stops the server cleanly, prunes
`cache/`, enforces the ~1 MiB size cap and records the generating binary
version. Stdlib only; run with `python -I`.

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
import base64
import hashlib
import hmac as hmac_mod
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

QUERY_VERSION = 4
DEFAULT_PORT = 17691
BOOTSTRAP_PASSWORD = "reference-epoch-1-admin"
REF_USER_PASSWORD = "reference-epoch-1-user"
TOKEN_LABEL = "reference-epoch-1"
SIZE_CAP_BYTES = 1024 * 1024
READY_TIMEOUT_SECS = 60

# Echo WASM module: memory(2) + bump global + shamir_alloc/shamir_call exports
# (byte-equivalent to ECHO_WAT in the server's create-function gating tests).

# Canonical HMAC grammar (shamir-query-types/src/security/hmac.rs, v2).
HMAC_PREFIX = b"shamir-db hmac canonical v2\0"
TAG_CHMOD = 0x000B
TAG_CHOWN = 0x000C
TAG_CREATE_GROUP = 0x0011
TAG_ADD_GROUP_MEMBER = 0x0014
TAG_CREATE_SCRAM_USER = 0x0019

BASIC_SCHEMA = [
    {"path": ["k"], "type": "string"},
    {"path": ["n"], "type": "int"},
    {"path": ["vec"], "type": "list", "array_of": "float"},
]
VERSIONED_SCHEMA = [
    {"path": ["k"], "type": "string"},
    {"path": ["n"], "type": "int"},
]
BASIC_ROWS = [
    {"k": "r1", "n": 10, "vec": [0.5, 0.25, 0.125, 0.0625]},
    {"k": "r2", "n": 20, "vec": [0.1, 0.2, 0.3, 0.4]},
    {"k": "r3", "n": 30, "vec": [1.0, 0.75, 0.5, 0.25]},
]
VERSIONED_ROWS = [
    {"k": "v1", "n": 7},
    {"k": "v2", "n": 8},
]
LANG_FN_SOURCE = "@entry scalar ref_lang_fn(x: i64) -> i64 { return x + 1; }"


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
# Canonical HMAC v2 encoders + session key derivation
# ---------------------------------------------------------------------------

def _canon_str(s):
    raw = s.encode("utf-8")
    return struct.pack(">I", len(raw)) + raw


def _canon_list(items):
    out = struct.pack(">I", len(items))
    return out + b"".join(_canon_str(s) for s in items)


def canonical_bytes(tag):
    return bytearray(HMAC_PREFIX + struct.pack(">H", tag))


def session_hmac_key(sid):
    return hashlib.sha256(b"shamir-db hmac key v1\0" + sid).digest()


def tag_hex(key, canonical):
    return hmac_mod.new(key, bytes(canonical), hashlib.sha256).hexdigest()


def principal64(user_id_bytes):
    require(len(user_id_bytes) == 16, "user_id must be 16 bytes")
    value = struct.unpack(">Q", user_id_bytes[:8])[0]
    return value & 0x7FFFFFFFFFFFFFFF


# ---------------------------------------------------------------------------
# Minimal WASM encoder for the ref_wasm_fn module
# ---------------------------------------------------------------------------

def _leb_u(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _leb_s(n):
    out = bytearray()
    more = True
    while more:
        b = n & 0x7F
        n >>= 7
        if (n == 0 and not (b & 0x40)) or (n == -1 and (b & 0x40)):
            more = False
        else:
            b |= 0x80
        out.append(b)
    return bytes(out)


def _section(section_id, payload):
    return bytes([section_id]) + _leb_u(len(payload)) + payload


def _name(s):
    raw = s.encode("utf-8")
    return _leb_u(len(raw)) + raw


def ref_wasm_fn_bytes():
    # types: 0=(i32)->i32, 1=(i32,i32)->i64
    types = bytes([0x01, 0x60, 0x01, 0x7F, 0x01, 0x7F,
                   0x60, 0x02, 0x7F, 0x7F, 0x01, 0x7E])
    funcs = bytes([0x02, 0x00, 0x01])
    memories = bytes([0x01, 0x00, 0x02])
    globals_ = (bytes([0x01, 0x7F, 0x01]) + bytes([0x41]) + _leb_s(1024) +
                bytes([0x0B]))
    exports = (bytes([0x03]) + _name("memory") + bytes([0x02, 0x00]) +
               _name("shamir_alloc") + bytes([0x00, 0x00]) +
               _name("shamir_call") + bytes([0x00, 0x01]))
    # shamir_alloc body
    body0 = (bytes([0x23, 0x00, 0x21, 0x01]) +      # ptr = global.get $bump
             bytes([0x23, 0x00, 0x20, 0x00, 0x6A, 0x24, 0x00]) +  # bump += len
             bytes([0x20, 0x01, 0x0B]))             # return ptr
    # shamir_call body
    body1 = (bytes([0xC0, 0x20, 0x00, 0x42]) + _leb_s(32) + bytes([0x86]) +
             bytes([0xC0, 0x20, 0x01, 0x85, 0x0B]))
    code0 = bytes([0x01, 0x01, 0x7F]) + body0       # 1 local group: 1 x i32
    code1 = bytes([0x00]) + body1
    codes = _leb_u(2) + _leb_u(len(code0)) + code0 + _leb_u(len(code1)) + code1
    return (b"\x00asm\x01\x00\x00\x00" +
            _section(1, types) + _section(3, funcs) + _section(5, memories) +
            _section(6, globals_) + _section(7, exports) + _section(10, codes))


# ---------------------------------------------------------------------------
# Server process control
# ---------------------------------------------------------------------------

def write_config(path, data_dir, port):
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
        f"        kind: http\n"
        f"        addr: 127.0.0.1:{port}\n"
        "        tls: false\n"
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


def rest_execute(port, token, batch):
    request = {
        "op": "execute",
        "query_version": QUERY_VERSION,
        "db": "default",
        "batch": batch,
    }
    status, payload = http_post(port, "/shamir/v1/request", token,
                                mp_encode(request))
    require(status == 200, f"REST request failed: HTTP {status}: "
                           f"{mp_decode(payload) if payload else ''}")
    decoded = mp_decode(payload)
    require(isinstance(decoded, dict) and decoded.get("op") == "batch",
            f"unexpected REST response: {decoded}")
    return decoded["response"]


def rest_ping(port, token):
    status, payload = http_post(port, "/shamir/v1/ping", token, b"", "GET")
    require(status == 200, f"ping failed: HTTP {status}")
    require(mp_decode(payload).get("op") == "pong", "ping did not answer pong")


def run_batch(port, token, alias, op, expect_records=None):
    batch = {"id": f"fixture-{alias}", "queries": {alias: op},
             "return_all": True}
    response = rest_execute(port, token, batch)
    results = response.get("results", {})
    require(alias in results, f"batch '{alias}' missing result: {response}")
    result = results[alias]
    if expect_records is not None:
        records = result.get("records", [])
        require(len(records) == expect_records,
                f"batch '{alias}': expected {expect_records} records, "
                f"got {len(records)}")
    return result


# ---------------------------------------------------------------------------
# Native token WebSocket client
# ---------------------------------------------------------------------------

class TokenWs:
    def __init__(self, port, token):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=30)
        key = base64.b64encode(os.urandom(16)).decode()
        head = (
            f"GET /shamir/v1/ws HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{port}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            f"Sec-WebSocket-Protocol: shamir-v1\r\n"
            f"Authorization: Bearer {token}\r\n\r\n"
        )
        self.sock.sendall(head.encode())
        response = b""
        while b"\r\n\r\n" not in response:
            chunk = self.sock.recv(4096)
            require(chunk, "WS upgrade: connection closed during handshake")
            response += chunk
        head_text, _, rest = response.partition(b"\r\n\r\n")
        require(b" 101 " in b" " + head_text.split(b"\r\n")[0] + b" ",
                f"WS upgrade rejected: {head_text.decode(errors='replace')}")
        self.buf = bytearray(rest)
        binding = mp_decode(self._next_frame())
        require(isinstance(binding, dict) and binding.get("ok") is True,
                f"WS binding not ok: {binding}")
        self.sid = binding["sid"]
        require(isinstance(self.sid, bytes) and len(self.sid) == 32,
                f"WS binding sid invalid: {binding}")
        self.key = session_hmac_key(self.sid)
        self.rid = 0

    def _read_exact(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            require(chunk, "WS connection closed unexpectedly")
            self.buf.extend(chunk)
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out

    def _next_frame(self):
        while True:
            head = self._read_exact(2)
            opcode = head[0] & 0x0F
            masked = head[1] & 0x80
            length = head[1] & 0x7F
            if length == 126:
                length = struct.unpack(">H", self._read_exact(2))[0]
            elif length == 127:
                length = struct.unpack(">Q", self._read_exact(8))[0]
            mask = self._read_exact(4) if masked else None
            payload = self._read_exact(length)
            if mask:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            if opcode == 0x9:  # ping -> pong
                self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:  # pong
                continue
            require(opcode in (0x1, 0x2), f"unexpected WS opcode {opcode}")
            return payload

    def _send_frame(self, opcode, payload):
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        head = bytes([0x80 | opcode])
        n = len(payload)
        if n < 126:
            head += bytes([0x80 | n])
        elif n < 0x10000:
            head += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            head += bytes([0x80 | 127]) + struct.pack(">Q", n)
        self.sock.sendall(head + mask + masked)

    def request(self, request, signed_canonical=None):
        body = mp_encode(request)
        if signed_canonical is not None:
            request = dict(request)
            request["hmac"] = tag_hex(self.key, signed_canonical)
            body = mp_encode(request)
        self.rid += 1
        rid = self.rid
        envelope = {"sid": self.sid, "rid": rid, "req": body}
        self._send_frame(0x2, mp_encode(envelope))
        response = mp_decode(self._next_frame())
        require(isinstance(response, dict) and response.get("rid") == rid,
                f"WS envelope mismatch: {response}")
        require("error" not in response, f"WS request error: {response}")
        decoded = mp_decode(response["res"])
        require(isinstance(decoded, dict),
                f"unexpected WS response payload: {decoded}")
        require(decoded.get("op") != "error",
                f"WS request failed: {decoded}")
        return decoded

    def ws_execute(self, alias, op, canonical=None):
        batch = {"id": f"fixture-{alias}", "queries": {alias: op},
                 "return_all": True}
        request = {"op": "execute", "query_version": QUERY_VERSION,
                   "db": "default", "batch": batch}
        response = self.request(request, signed_canonical=canonical)
        require(response.get("op") == "batch",
                f"WS execute '{alias}' unexpected response: {response}")
        results = response["response"].get("results", {})
        require(alias in results, f"WS execute '{alias}' missing result")
        return results[alias]

    def create_scram_user(self, name, password, roles):
        canonical = canonical_bytes(TAG_CREATE_SCRAM_USER)
        canonical += _canon_str(name)
        canonical += _canon_list(roles)
        request = {"op": "create_scram_user", "name": name,
                   "password": password, "roles": roles}
        response = self.request(request, signed_canonical=canonical)
        require(response.get("op") == "user_created",
                f"create_scram_user unexpected response: {response}")
        return response["user_id"]

    def close(self):
        try:
            self._send_frame(0x8, struct.pack(">H", 1000))
            self.sock.close()
        except OSError:
            pass


def gated_op(alias, op_map, tag, fields):
    canonical = canonical_bytes(tag) + fields
    return alias, op_map, canonical


# ---------------------------------------------------------------------------
# Fixed scenario
# ---------------------------------------------------------------------------

def apply_scenario(port, token):
    ws = TokenWs(port, token)
    try:
        rest_ping(port, token)

        # 1. Tables: basic (schema_version 1 suffices, contract: >= 1) and
        #    versioned (bumped via set_table_schema to 2, contract: > 1).
        run_batch(port, token, "create_basic", {
            "create_table": "ref_schema_basic", "repo": "main",
            "schema": BASIC_SCHEMA,
        })
        run_batch(port, token, "create_versioned", {
            "create_table": "ref_schema_versioned", "repo": "main",
            "schema": VERSIONED_SCHEMA,
        })
        run_batch(port, token, "bump_versioned_schema", {
            "set_table_schema": "ref_schema_versioned", "repo": "main",
            "schema": VERSIONED_SCHEMA, "expected_version": 1,
        })

        # 2. One index of every kind on ref_schema_basic, exact names from
        #    the contract. Vector uses the float-list column `vec`.
        run_batch(port, token, "idx_hash", {
            "create_index": "ref_hash_idx", "table": "ref_schema_basic",
            "fields": [["k"]], "repo": "main",
        })
        run_batch(port, token, "idx_sorted", {
            "create_index": "ref_sorted_idx", "table": "ref_schema_basic",
            "fields": [["n"]], "sorted": True, "repo": "main",
        })
        run_batch(port, token, "idx_vector", {
            "create_index": "ref_vector_idx", "table": "ref_schema_basic",
            "fields": [["vec"]], "index_type": "vector",
            "vector_dim": 4, "vector_metric": "cosine", "repo": "main",
        })
        run_batch(port, token, "idx_fts", {
            "create_index": "ref_fulltext_idx", "table": "ref_schema_basic",
            "fields": [["k"]], "index_type": "fts", "repo": "main",
        })

        # 3. Functions: shamir-source scalar (call_signature persisted) and
        #    a compiled WASM module.
        run_batch(port, token, "fn_lang", {
            "create_function": "ref_lang_fn", "source": LANG_FN_SOURCE,
            "lang": "shamir",
        })
        run_batch(port, token, "fn_wasm", {
            "create_function": "ref_wasm_fn",
            "wasm": base64.b64encode(ref_wasm_fn_bytes()).decode("ascii"),
            "lang": "wasm",
        })

        # 4. Rows (fixed literals only).
        run_batch(port, token, "insert_basic",
                  {"insert_into": "ref_schema_basic", "values": BASIC_ROWS},
                  expect_records=len(BASIC_ROWS))
        run_batch(port, token, "insert_versioned",
                  {"insert_into": "ref_schema_versioned",
                   "values": VERSIONED_ROWS},
                  expect_records=len(VERSIONED_ROWS))

        # 5. Principals, group, rights (HMAC-gated ops go over the WS
        #    session). ref_user is a SCRAM user with the reader role; the
        #    store-level chmod grants read on default.main.
        ref_user_id = ws.create_scram_user("ref_user", REF_USER_PASSWORD,
                                           ["reader"])
        ref_pid = principal64(ref_user_id)
        alias, op, canonical = gated_op(
            "create_group",
            {"create_group": "ref_group"},
            TAG_CREATE_GROUP,
            _canon_str("ref_group"),
        )
        ws.ws_execute(alias, op, canonical)
        alias, op, canonical = gated_op(
            "add_group_member",
            {"add_group_member": {"name": "ref_group"}, "user": ref_pid},
            TAG_ADD_GROUP_MEMBER,
            (bytes([0x00]) + _canon_str("ref_group")) +
            struct.pack(">Q", ref_pid),
        )
        ws.ws_execute(alias, op, canonical)
        store_resource = bytes([0x01]) + _canon_str("default") + \
            _canon_str("main")
        alias, op, canonical = gated_op(
            "chmod_store",
            {"chmod": {"store": ["default", "main"]}, "mode": 420},
            TAG_CHMOD,
            store_resource + struct.pack(">H", 420),
        )
        ws.ws_execute(alias, op, canonical)
        table_resource = bytes([0x02]) + _canon_str("default") + \
            _canon_str("main") + _canon_str("ref_schema_versioned")
        alias, op, canonical = gated_op(
            "chmod_versioned",
            {"chmod": {"table": ["default", "main",
                                "ref_schema_versioned"]}, "mode": 448},
            TAG_CHMOD,
            table_resource + struct.pack(">H", 448),
        )
        ws.ws_execute(alias, op, canonical)
        alias, op, canonical = gated_op(
            "chown_versioned",
            {"chown": {"table": ["default", "main",
                                "ref_schema_versioned"]},
             "owner": ref_pid},
            TAG_CHOWN,
            table_resource + struct.pack(">Q", ref_pid),
        )
        ws.ws_execute(alias, op, canonical)

        # 6. Verify reads and the stored call through the same surface.
        run_batch(port, token, "read_basic", {"from": "ref_schema_basic"},
                  expect_records=len(BASIC_ROWS))
        run_batch(port, token, "read_versioned",
                  {"from": "ref_schema_versioned"},
                  expect_records=len(VERSIONED_ROWS))
        call_result = run_batch(port, token, "call_lang_fn", {
            "call": "ref_lang_fn", "params": [41], "repo": "main",
        })
        require(call_result.get("value") == 42,
                f"stored call ref_lang_fn(41) did not return 42: "
                f"{call_result}")
    finally:
        ws.close()


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
    parser.add_argument("--out", required=True,
                        help="output data directory (must not exist or be empty)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help="loopback HTTP port for the fixture server")
    args = parser.parse_args(argv)

    binary = os.path.abspath(args.server_binary)
    require(os.path.isfile(binary), f"server binary not found: {binary}")
    out_dir = os.path.abspath(args.out)
    if os.path.isdir(out_dir):
        require(not os.listdir(out_dir), f"output dir not empty: {out_dir}")
    else:
        os.makedirs(out_dir)
    config_path = out_dir + ".ktav"
    write_config(config_path, out_dir, args.port)

    version = binary_version(binary)

    # Phase A: first boot creates the admin superuser, then a clean stop.
    server = ServerProcess(binary, config_path)
    try:
        wait_ready(args.port)
    finally:
        server.stop_cleanly()

    # Phase B: offline mint of an ephemeral admin token (scenario driver).
    token = mint_offline_token(binary, config_path, "admin")

    # Phase C: second boot, fixed scenario over REST + token WebSocket.
    server = ServerProcess(binary, config_path)
    try:
        wait_ready(args.port)
        apply_scenario(args.port, token)
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
