//! ShamirDB Rust client SDK.
//!
//! Wraps the production handshake (`shamir-connect`) and TLS transport
//! (`shamir-transport-tcp`) into a small ergonomic surface:
//!
//! ```no_run
//! # use shamir_client::{Client, ConnectOptions};
//! # use zeroize::Zeroizing;
//! # async fn demo() -> Result<(), shamir_client::ClientError> {
//! let client = Client::connect(ConnectOptions {
//!     addr: "127.0.0.1:3742".parse().unwrap(),
//!     server_name: "localhost".into(),
//!     username: "admin".into(),
//!     password: Zeroizing::new(b"correct horse battery staple".to_vec()),
//!     accept_new_host: true,
//!     trusted_pin: None,
//!     connect_timeout: None,
//!     handshake_timeout: None,
//!     request_timeout: None,
//! })
//! .await?;
//!
//! client.ping().await?;
//! client.close().await;
//! # Ok(()) }
//! ```
//!
//! The SDK is the same code path the reference Rust integration tests
//! exercise; the napi-rs Node binding (`shamir-client-node`) wraps this
//! surface 1:1, so all language clients share one implementation of
//! TLS+SCRAM+envelope handling.

mod client;
#[path = "visibility/repo_admin.rs"]
mod client_repo_admin;
mod error;
pub mod interner_cache;
mod receipts;

// Topical home under `receipts/` (outcome resolution); declared at the
// crate root via `#[path]` — no dependency on `receipts` internals.
#[path = "receipts/schema_migration/mod.rs"]
pub mod schema_migration;

#[cfg(test)]
mod tests;

pub use client::cursor_stream;
pub use client::cursor_stream::CursorStream;
pub use client::subscription;
pub use client::subscription::SubscriptionHandle;
pub use client::{
    bound_until, resolve_host, BrowserResumeOptions, BrowserWsOptions, Client, ConnectLocalOptions,
    ConnectOptions, ResumeLocalOptions, ResumeOptions, SecureResumeOptions,
};
pub use client::{
    login, ApiToken, AuthKind, LoginChannels, LoginMode, LoginOptions, LoginOutcome, LoginScope,
    TokenEndpoint, TokenHttpClient, TokenHttpOptions, TokenTrust, TokenWsOptions,
};
pub mod visibility;
pub use error::{ClientError, CloseCause};
pub use interner_cache::{FieldMap, InternerCacheRegistry};
pub use receipts::{extract_receipts, reconcile_receipts, AliasReceipt, RepoObservation};
pub use schema_migration::{
    ApplicationMigrationSet, MigrationError, MigrationStatus, PrefixCoverage, UnknownApply,
    UnknownApplyResolution,
};
pub use shamir_query_types::admin::ChangesSinceResponse;
pub use visibility::{
    CommitResult, TransactionInfo, VisibilityPendingReason, VisibilityState, VisibilityWait,
};

// Re-export the wire payloads so callers don't need to depend on
// `shamir-query-types` directly.
pub use shamir_query_types::batch::{BatchRequest, BatchResponse};
pub use shamir_query_types::wire::token_admin::{
    ApiTokenChannels, ApiTokenMode, ApiTokenView, CreatedApiToken,
};
pub use shamir_query_types::wire::{DbRequest, DbResponse, CURRENT_QUERY_LANG_VERSION};

// Re-export the fluent query builder under `shamir_client::builder` so a
// single `shamir-client` dependency gives callers both the transport and the
// request/response ergonomics:
//
//   use shamir_client::builder::{query::Query, batch::Batch};
//   use shamir_client::builder::{filter::*, val::*, response::BatchResponseExt};
//   let req = Batch::new().query("u", Query::from("users")).build();
//   let resp = client.execute("db", req).await?;
//   let users: Vec<User> = resp.rows_as("u")?;
pub use shamir_query_builder as builder;
