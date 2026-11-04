# TS Client Transport Spec (T1 — extracted from protocol docs)

Browser-path: WS binary + msgpack + SCRAM-Argon2id, binding_mode 0x02.

## Dependencies (from CLIENT_BROWSER.md §2)
```
"argon2-browser": "^1.18.0",
"@noble/ed25519": "^2.1.0",
"@noble/ciphers": "^0.5.0",
"@msgpack/msgpack": "^3.0.0",
"ws": "^8.0.0"
```
(`ws` for Node; in browser use native WebSocket later.)

## Connect
1. Open `wss://{host}:{port}/shamir/v1/browser` (binary mode).
2. Self-signed certs in e2e: `rejectUnauthorized: false` on the agent.

## SCRAM Handshake (4 messages, all msgpack-binary)

### msg1: auth_init → server
```
{ "auth_init": { "user": nfc_lower(username), "client_nonce": random(32), "binding_mode": 2, "version": 1 } }
```

### msg2: challenge ← server
```
{ "challenge": { "salt": bytes(16), "kdf": "argon2id", "memory_kb": u32, "time": u32, "parallelism": u32, "argon2_version": 0x13, "server_nonce": bytes(32) } }
```
Pre-validate: memory_kb≤262144, time≤8, parallelism≤8, argon2_version==0x13.

### msg3: client_proof → server
Derive:
```
salted_password = argon2id(password, salt, {memory_kb, time, parallelism})
client_key      = HMAC-SHA256(salted_password, "Client Key")
server_key      = HMAC-SHA256(salted_password, "Server Key")
stored_key      = SHA256(client_key)

auth_message    = canonical (see below)
client_signature = HMAC-SHA256(stored_key, auth_message)
client_proof     = XOR(client_key, client_signature)
```
Send: `{ "client_proof": client_proof(32) }`

### msg4: auth_ok | error ← server
```
{ "auth_ok": { "server_signature": bytes(32), "server_pub_key": bytes(32), "identity_sig": bytes(64), "session_id": bytes(32), "expires_at_ns": u64, ... } }
```
Verify: `HMAC-SHA256(server_key, auth_message) == server_signature`.
Ed25519 verify identity_sig over identity_input (optional for MVP).

### canonical auth_message (byte-for-byte)
```
"SHAMIR-AUTH-v1"                          // 14 bytes ASCII
|| u16_be(byte_len(username_nfc))
|| username_nfc_bytes
|| client_nonce(32)
|| server_nonce(32)
|| salt(16)
|| u32_be(memory_kb)
|| u32_be(time)
|| u32_be(parallelism)
|| u8(argon2_version)                     // 0x13
|| u8(transport_kind)                     // 0x02 (ws)
|| u8(binding_mode)                       // 0x02 (tls_no_export)
|| zeros(32)                              // tls_exporter_or_zeros (browser = zeros)
|| u8(1)                                  // supported_version
```

## Session Frames (post-auth)
Request: `{ "sid": session_id(32), "req": { "db": dbName, "batch": batchObject } }`
Response: `{ "rid": requestId, "res": responseObject }` or `{ "error": "..." }`

All frames = msgpack binary WS messages.

## ConnectOptions (mirror napi index.d.ts)
```ts
interface ConnectOptions {
  host: string;
  port: number;
  username: string;
  password: string;
  tls?: { rejectUnauthorized?: boolean };
}
```

Note: unlike the napi/Node client's historical gap, the TS client now
implements TOFU pinning of the server's Ed25519 identity in the shared
session layer: every handshake verifies `identity_sig`,
`acceptNewHost: false` refuses first contact before any I/O, and the
captured `SHA256(server_pub_key)` pin is exposed via
`ShamirClient.trustedPin()` (pass it back as the `trustedPin` option;
see docs/README.md). Persisting the pin in browser storage is up to the
application.

## ShamirClient API (mirror napi)
```ts
class ShamirClient {
  static connect(opts: ConnectOptions): Promise<ShamirClient>;
  execute(db: string, batch: object): Promise<object>;
  sessionId(): Uint8Array;
  serverPubKeyPin(): Uint8Array;
  close(): Promise<void>;
}
```
