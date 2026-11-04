use napi::bindgen_prelude::*;
use napi_derive::napi;

/// Parameters of `ShamirClient.resumeLocal`: ticket resumption over the local
/// IPC transport (Unix socket / Windows named pipe, binding 0x00).
///
/// The ticket is copied into a native buffer that is zeroized on drop,
/// including when the resume is cancelled before it starts; the JavaScript
/// `Buffer` it came from cannot be wiped by the binding.
#[napi(object)]
pub struct ResumeLocalOptions {
    /// Unix socket path or `\\.\pipe\name` Windows pipe, used as is.
    pub addr: String,
    /// Resumption ticket from a previous session.
    pub ticket: Buffer,
    /// Required pin of the server identity, exactly 32 bytes.
    pub trusted_pin: Buffer,
    /// Deadline (ms) for the IPC connect attempt.
    pub connect_timeout_ms: Option<u32>,
    /// Deadline (ms) for the whole resume.
    pub handshake_timeout_ms: Option<u32>,
    /// Deadline (ms) for each request/response round-trip.
    pub request_timeout_ms: Option<u32>,
}
