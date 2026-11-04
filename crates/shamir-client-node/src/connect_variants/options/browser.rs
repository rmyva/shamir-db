use napi::bindgen_prelude::*;
use napi_derive::napi;

// Browser-profile option pair (connect + resume). JavaScript strings and
// Buffers cannot be wiped; only the owned native copies are zeroized.

/// Parameters of `ShamirClient.connectWsBrowser`: SCRAM over the
/// browser-profile WebSocket listener (`kind: ws`, `profile: tls_no_export`,
/// path `/shamir/v1/browser`, binding 0x02). TLS trusts WebPKI roots or
/// `caPem`; the Ed25519 TOFU-pin dance applies on top.
///
/// A JavaScript string cannot be wiped; only the owned native copy of
/// `password` is zeroized.
#[napi(object)]
pub struct ConnectWsBrowserOptions {
    /// Server host; only its first resolved address is used.
    pub host: String,
    /// Server port.
    pub port: u32,
    /// TLS server name the certificate is checked against.
    pub server_name: String,
    /// `Origin` header of the WebSocket upgrade; the listener refuses origins
    /// outside its allowlist. An empty string omits the header and is refused
    /// by the listener.
    pub origin: String,
    /// Username (raw — server-side normalisation applies).
    pub username: String,
    /// Plaintext password. Only the native copy is zeroized; the JavaScript
    /// string cannot be wiped.
    pub password: String,
    /// Trust-on-first-use, see `ConnectOptions.acceptNewHost`.
    pub accept_new_host: Option<bool>,
    /// Pre-pinned server identity hash, exactly 32 bytes.
    pub trusted_pin: Option<Buffer>,
    /// PEM bundle of trusted CAs for the TLS certificate check; defaults to
    /// the bundled WebPKI roots.
    pub ca_pem: Option<String>,
    /// Deadline (ms) for the TCP connect attempt.
    pub connect_timeout_ms: Option<u32>,
    /// Deadline (ms) for the whole handshake, upgrade included.
    pub handshake_timeout_ms: Option<u32>,
    /// Deadline (ms) for each request/response round-trip.
    pub request_timeout_ms: Option<u32>,
}

/// Parameters of `ShamirClient.resumeWsBrowser`: ticket resumption over the
/// browser-profile WebSocket listener (binding 0x02).
///
/// The ticket is copied into a native buffer that is zeroized on drop,
/// including when the resume is cancelled before it starts; the JavaScript
/// `Buffer` it came from cannot be wiped by the binding.
#[napi(object)]
pub struct ResumeWsBrowserOptions {
    /// Server host; only its first resolved address is used.
    pub host: String,
    /// Server port.
    pub port: u32,
    /// TLS server name the certificate is checked against.
    pub server_name: String,
    /// `Origin` header of the WebSocket upgrade (empty = omitted; refused by
    /// the listener).
    pub origin: String,
    /// Resumption ticket from a previous session.
    pub ticket: Buffer,
    /// Required pin of the server identity, exactly 32 bytes.
    pub trusted_pin: Buffer,
    /// PEM bundle of trusted CAs for the TLS certificate check; defaults to
    /// the bundled WebPKI roots.
    pub ca_pem: Option<String>,
    /// Deadline (ms) for the whole resume, DNS included.
    pub handshake_timeout_ms: Option<u32>,
    /// Deadline (ms) for each request/response round-trip.
    pub request_timeout_ms: Option<u32>,
}
