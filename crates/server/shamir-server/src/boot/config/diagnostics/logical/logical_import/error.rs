//! Typed refusals for the offline `logical-import` CLI. Every refusal is
//! specific and actionable; nothing is skipped or ignored during an import.

use shamir_db::DbError;
use shamir_sync::replication_seed::SeedError;

#[derive(Debug, thiserror::Error)]
pub enum LogicalImportError {
    /// Filesystem error outside the staged engine stores.
    #[error("logical-import: {0}")]
    Io(#[from] std::io::Error),
    /// Engine/store error raised while loading or re-reading the stage.
    #[error("logical-import: engine: {0}")]
    Engine(#[from] DbError),
    /// Artifact verifier refusal (magic, version, features, checksums,
    /// counts, trailer, EOF).
    #[error("logical-import: artifact: {0}")]
    Artifact(#[from] SeedError),
    /// The `<target>.import.lock` file could not be created exclusively.
    #[error("logical-import lock: {0}")]
    Lock(String),
    /// The target data directory exists and cannot be used.
    #[error("logical-import target: {0}")]
    Target(String),
    /// The artifact is verifiable but incoherent or unsupported.
    #[error("logical-import preflight: {0}")]
    Preflight(String),
    /// The staging directory could not be created, cleaned or published.
    #[error("logical-import stage: {0}")]
    Stage(String),
    /// Engine mutation while loading the stage failed.
    #[error("logical-import load: {0}")]
    Load(String),
    /// The staged content does not match the artifact.
    #[error("logical-import validation: {0}")]
    Validation(String),
    /// The receipt on an existing target is missing, unreadable or does not
    /// match the artifact and effective options.
    #[error("logical-import receipt: {0}")]
    Receipt(String),
    /// The final same-volume rename of the stage onto the target failed.
    #[error("logical-import publish: {0}")]
    Publish(String),
    /// One dump record cannot fit any transaction even with an empty batch:
    /// the complete-entry bound exceeds the per-transaction WAL budget after
    /// persisting the interner delta. Fires BEFORE any WAL submission.
    /// (A dump row's body is capped at 16 MiB — codec FRAME_LIMIT — so with
    /// small interner deltas this refusal is unreachable in practice; it
    /// guards the invariant.)
    #[error("logical-import record {position} exceeds the per-transaction WAL budget: complete entry bound {bound_bytes} bytes > {cap_bytes} even after persisting the interner delta")]
    RecordTooLarge {
        position: u64,
        bound_bytes: u64,
        cap_bytes: u64,
    },
    /// The artifact file changed between preflight and post-load re-hash.
    #[error("logical-import: input file changed after preflight verification")]
    InputChanged,
    /// Cancellation was observed at a checkpoint; the stage was discarded.
    #[error("logical-import cancelled")]
    Cancelled,
}
