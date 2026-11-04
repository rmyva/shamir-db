use napi::bindgen_prelude::*;
use napi_derive::napi;

/// Parameters of `ShamirClient.resume` / `resumeWs`.
///
/// The ticket is copied into a native buffer that is zeroized on drop,
/// including when the resume is cancelled before it starts; the JavaScript
/// `Buffer` it came from cannot be wiped by the binding.
#[napi(object)]
pub struct ResumeOptions {
    /// Server host; only its first resolved address is used.
    pub host: String,
    /// Server port.
    pub port: u32,
    /// SNI hostname for TLS.
    pub server_name: String,
    /// Resumption ticket from a previous session.
    pub ticket: Buffer,
    /// Required pin of the server identity, exactly 32 bytes.
    pub trusted_pin: Buffer,
    /// Deadline (ms) for the whole resume, DNS included.
    pub handshake_timeout_ms: Option<u32>,
    /// Deadline (ms) for each request/response round-trip.
    pub request_timeout_ms: Option<u32>,
}

/// Parameters of `ShamirClient.resumePlain`: ticket resumption over plain
/// loopback TCP (binding 0x00). No TLS, so no server name; the core refuses
/// a non-loopback address before any socket is opened, and the server
/// refuses a ticket issued under a stronger binding (anti-downgrade).
///
/// The ticket is copied into a native buffer that is zeroized on drop,
/// including when the resume is cancelled before it starts; the JavaScript
/// `Buffer` it came from cannot be wiped by the binding.
#[napi(object)]
pub struct ResumePlainOptions {
    /// Server host; only its first resolved address is used.
    pub host: String,
    /// Server port.
    pub port: u32,
    /// Resumption ticket from a previous session.
    pub ticket: Buffer,
    /// Required pin of the server identity, exactly 32 bytes.
    pub trusted_pin: Buffer,
    /// Deadline (ms) for the whole resume, DNS included.
    pub handshake_timeout_ms: Option<u32>,
    /// Deadline (ms) for each request/response round-trip.
    pub request_timeout_ms: Option<u32>,
}
