//! Node.js native binding for `shamir-client`.
//!
//! The Node API supports every Rust client entry point except subscriptions
//! and cursors: TLS, WebSocket (native and browser-profile), plain-loopback
//! and local (Unix socket / named pipe) SCRAM connects, ticket resumption
//! over TLS, WebSocket (native and browser-profile), plain loopback TCP and
//! local IPC, API-token WebSocket sessions, the token REST client, password
//! `login`, batch
//! execution, replication and API-token creation (`createApiToken`).
//! JavaScript sees:
//!
//! ```js
//! const { ShamirClient } = require('shamir-client');
//!
//! const client = await ShamirClient.connect({
//!   host: '127.0.0.1',
//!   port: 3742,
//!   serverName: 'localhost',
//!   username: 'admin',
//!   password: 'correct horse battery staple',
//!   acceptNewHost: true,
//! });
//!
//! await client.ping();
//!
//! const { Batch, Query } = require('@shamir/client');
//! const batch = Batch.create('rw')
//!   .add('rd', Query.from('items').asOfVersion(4294967296))
//!   .build();
//! const resp = await client.execute('prod', batch);
//!
//! await client.close();
//! ```
//!
//! `execute` is the low-level object FFI; use the TS builder's `.build()`.
//! `Batch.execute(client, db)` is not supported by this Node client: it requires
//! `executeWithTouch`. DTO reexports and a full adapter are deferred.
//!
//! All TLS / SCRAM / Argon2id / Ed25519 verification happens in the
//! native binary via `shamir-client` — JS never touches crypto. Drift
//! between server and client crypto is impossible: both built from the
//! same Rust source.

#![deny(clippy::all)]

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use napi::bindgen_prelude::*;
use tokio::sync::watch;

mod client;
mod connect_variants;
mod request_state;
mod secrets;
mod token;
use request_state::{Closed, State};
// Required: keeps the free `login` function from being dead code under cfg(test).
pub use token::{login, LoginOptions};

use shamir_client as core;
use shamir_query_types::wire::DbResponse;

type ClientFuture<'a, T> =
    Pin<Box<dyn Future<Output = std::result::Result<T, core::ClientError>> + Send + 'a>>;

/// Race request admission and execution against close. The read guard is
/// dropped when the request completes or shutdown cancellation wins
/// (105a725d / f90a248f).
async fn run_request<C, T, F>(
    state: &State<C>,
    shutdown: &mut watch::Receiver<bool>,
    deadline: Option<tokio::time::Instant>,
    timeout: Option<Duration>,
    operation: F,
) -> Result<std::result::Result<T, core::ClientError>>
where
    C: Send + Sync,
    F: for<'a> FnOnce(&'a C) -> ClientFuture<'a, T>,
{
    match core::bound_until(deadline, request_state::request(state, shutdown, operation)).await {
        Ok(Some(Ok(result))) => Ok(result),
        Ok(Some(Err(Closed))) => Err(Error::from_reason("client closed")),
        Ok(None) => Err(request_timeout_error(timeout)),
        Err(error) => Err(infra_error(error)),
    }
}

fn request_timeout_error(timeout: Option<Duration>) -> Error {
    Error::from_reason(format!(
        "request timed out after {:?}",
        timeout.unwrap_or_default()
    ))
}

/// Snapshot mutable Node-owned bytes before dispatching work to a Tokio
/// worker (067017c8).
fn snapshot_bytes(bytes: &[u8]) -> Vec<u8> {
    bytes.to_vec()
}

/// Copy the JS-owned trust pin synchronously before worker dispatch
/// (067017c8).
fn copy_pin(buffer: &[u8]) -> Result<[u8; 32]> {
    if buffer.len() != 32 {
        return Err(Error::from_reason(format!(
            "trustedPin must be 32 bytes, got {}",
            buffer.len()
        )));
    }
    let mut pin = [0u8; 32];
    pin.copy_from_slice(buffer);
    Ok(pin)
}

fn optional_pin(buffer: Option<Buffer>) -> Result<Option<[u8; 32]>> {
    buffer.map(|bytes| copy_pin(&bytes)).transpose()
}

fn millis(ms: Option<u32>) -> Option<Duration> {
    ms.map(|ms| Duration::from_millis(u64::from(ms)))
}

fn checked_port(port: u32) -> Result<u16> {
    u16::try_from(port).map_err(|_| Error::from_reason(format!("port out of range: {port}")))
}

/// Placeholder for core options whose address is filled in later from
/// `host:port`; never dialed as is.
fn unresolved_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
}

/// Run `fut` under an optional budget; an elapsed budget maps to
/// `HandshakeTimeout(budget, phase)` exactly like the core handshakes do.
async fn bounded<T, F>(
    budget: Option<Duration>,
    phase: &'static str,
    fut: F,
) -> std::result::Result<T, core::ClientError>
where
    F: Future<Output = std::result::Result<T, core::ClientError>>,
{
    match budget {
        None => fut.await,
        Some(budget) => tokio::time::timeout(budget, fut)
            .await
            .map_err(|_| core::ClientError::HandshakeTimeout(budget, Arc::from(phase)))?,
    }
}

fn encode_buffer<T: serde::Serialize>(value: &T, context: &str) -> Result<Buffer> {
    rmp_serde::to_vec_named(value)
        .map(Buffer::from)
        .map_err(|error| Error::from_reason(format!("{context}: {error}")))
}

// -----------------------------------------------------------------------
// Error mapping
// -----------------------------------------------------------------------
//
// napi-rs 3.x async `#[napi]` fns hard-pin the Result error type to
// `napi::Error<Status>`: the codegen funnels through
// `execute_tokio_future_with_finalize_callback` whose `Future` bound is
// `Output = Result<Data, impl Into<Error>>` where `Error = Error<Status>`.
// A custom `Error<S>` (where `S: AsRef<str>` carries the DB error code as the
// JS `.code` property) has no `From<Error<S>> for Error<Status>` impl, and the
// orphan rule blocks adding one — so the Rust-side custom-error-type approach
// does NOT compile for async fns (verified by real compilation, task #519).
//
// Instead, domain-level DB errors (`ClientError::Db { code, message }`) are
// NOT thrown at the napi boundary. The Rust method returns `Ok(Buffer)` where
// the buffer is a msgpack-encoded `DbResponse::Error { kind: "error", code,
// message }`. The JS wrapper in `js/wrapper.js` decodes every response buffer and,
// if it detects the `kind: "error"` marker, throws a typed `ShamirDbError`
// (mirrors the TS SDK's `shamir-client-ts/src/core/support/errors.ts`) with `.code`
// and `.retryable` properties.
//
// Infrastructure errors (connection closed, TLS, transport, timeout, …) stay
// as plain napi `Error<Status>` → JS `Error` with only `.message`, which is
// appropriate — these are not the `ShamirDbError`-parity case.

/// Map an **infrastructure** [`core::ClientError`] into a napi [`Error`].
///
/// Callers that return `Buffer` (`execute`, `repl`, `create_scram_user`)
/// route `ClientError::Db` through [`encode_db_error`] instead — this
/// function is only reached for non-Db variants or from `connect` / `ping`
/// where no structured-error surface is possible.
fn infra_error(e: core::ClientError) -> Error {
    Error::from_reason(e.to_string())
}

/// Encode a domain-level DB error as a `DbResponse::Error` msgpack Buffer.
///
/// Returns `Ok(Buffer)` so the JS wrapper can decode `{ kind: "error",
/// code, message }`, detect the marker, and throw a `ShamirDbError` with
/// typed `.code` / `.retryable` — instead of receiving it as a thrown napi
/// `Error` that only carries `.message`.
fn encode_db_error(code: String, message: String) -> Result<Buffer> {
    let err = DbResponse::Error { code, message };
    let bytes = rmp_serde::to_vec_named(&err)
        .map_err(|e| Error::from_reason(format!("encode db error: {e}")))?;
    Ok(Buffer::from(bytes))
}
