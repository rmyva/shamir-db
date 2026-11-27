//! Orchestrator for the offline `logical-import` CLI:
//! lock → target check → preflight → idempotent-retry check → stage → load →
//! validate → receipt → publish, with progress on stderr. Cancellation is
//! observed at phase boundaries and between artifact frames and discards the
//! stage; after a successful publish it reports success.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use sha2::Digest;
use shamir_db::{ShamirDb, SystemStoreConfig};
use shamir_server_security::user_directory::FjallUserDirectory;
use shamir_sync::format_stamp::{inspect, StampState};
use std::io::Read;
use tokio_util::sync::CancellationToken;

use super::args::LogicalImportArgs;
use super::error::LogicalImportError;
use super::load::preflight::Preflight;
use super::load::{stage_load, validate};
use super::reader::CancelledRead;
use super::stage::dir::{prepare_target_name, ImportStage};
use super::stage::lock::ImportLock;
use super::stage::receipt::{Receipt, RECEIPT_NAME};
use crate::engine::server::wait_stores_released;

/// doc(hidden) re-export of the load stage's commit-boundary seam so the
/// integration tests (`tests/admin/logical_dump`) can reach it through the
/// crate root. NEW item — no existing visibility was widened.
#[doc(hidden)]
pub use super::load::stage_load::COMMIT_BOUNDARY;

/// Same bound as the server's shutdown barrier
/// (`engine::server::server_handle::STORE_RELEASE_TIMEOUT`, private there, so
/// mirrored here): how long we poll for every fjall store under a directory
/// to drop its lock before failing closed.
const STORE_RELEASE_TIMEOUT: Duration = Duration::from_secs(10);

/// Summary of a completed (or idempotently accepted) import.
#[derive(Clone, Debug)]
pub struct ImportReport {
    /// Effective imported database name (after `--as-db`).
    pub database: String,
    /// Portable items observed in the artifact.
    pub items: u64,
    /// Artifact size in bytes.
    pub bytes: u64,
    /// False for an idempotent retry of an already-published target.
    pub published: bool,
    /// `commit_tx` batch commits issued by the record-load stage (0 for an
    /// idempotent retry, which does not load records).
    pub tx_commits: u64,
    /// Highest staged bytes observed at any load flush decision point (0 for
    /// an idempotent retry).
    pub peak_staged_bytes: u64,
}

/// Offline import. Not cancel-safe as a future: the caller must retain and
/// join this future (the CLI does), signalling cancellation with the token.
/// The lock is held for the whole run and removed only after the stage
/// discard/publish outcome is resolved (RAII drop at function exit).
pub async fn import(
    args: LogicalImportArgs,
    cancel: CancellationToken,
) -> Result<ImportReport, LogicalImportError> {
    // Exclusive ownership BEFORE the target is inspected.
    let lock = ImportLock::acquire(&args.target)?;
    progress("lock acquired");
    if let Some(report) = retry_existing(&args, &cancel).await? {
        drop(lock);
        return Ok(report);
    }
    prepare_target_name(&args.target)?;
    let preflight_started = Instant::now();
    let preflight_result = Preflight::run(&args.input, &args, &cancel);
    let preflight_outcome = outcome_of(preflight_result.as_ref().err());
    let preflight = preflight_result?;
    phase(
        "preflight",
        preflight_started,
        &format!("outcome={preflight_outcome}"),
    );
    progress(&format!(
        "preflight verified: {} identities, {} repositories, {} tables",
        preflight.identities.len(),
        preflight.repos.len(),
        preflight.tables.len()
    ));
    let stage = ImportStage::prepare(&args.target)?;
    let outcome = import_into_stage(&stage, &args, &preflight, &cancel).await;
    match outcome {
        Ok(report) => {
            let publish_started = Instant::now();
            let published = stage.publish(&args.target, &cancel).await;
            phase(
                "publish",
                publish_started,
                &format!(
                    "outcome={} {}",
                    outcome_of(published.as_ref().err()),
                    args.target.display()
                ),
            );
            match published {
                Ok(()) => {
                    drop(lock);
                    progress(&format!("published {}", args.target.display()));
                    Ok(report)
                }
                Err(e) => Err(e),
            }
        }
        Err(e) => {
            // The staged engines were dropped on the error path (and flushed
            // before that); wait for their detached flusher tasks to release
            // the stage's fjall locks so the discard's remove cannot hit a
            // locked file on Windows. Fail closed: discard aborted on timeout.
            let close_started = Instant::now();
            let released = wait_stores_released(stage.path(), STORE_RELEASE_TIMEOUT)
                .await
                .map_err(|locked| {
                    LogicalImportError::Stage(format!(
                        "staged stores still locked before discard: {}",
                        locked
                            .iter()
                            .map(|p| p.display().to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                });
            phase(
                "close_wait",
                close_started,
                &format!(
                    "stage=discard outcome={}",
                    outcome_of(released.as_ref().err())
                ),
            );
            released?;
            stage.discard()?;
            Err(e)
        }
    }
}

async fn import_into_stage(
    stage: &ImportStage,
    args: &LogicalImportArgs,
    preflight: &Preflight,
    cancel: &CancellationToken,
) -> Result<ImportReport, LogicalImportError> {
    let loaded = stage_load::run(stage.path(), &args.input, preflight, args, cancel).await?;
    let db_name = loaded.db_name.clone();
    progress(&format!("loaded into stage as database '{db_name}'"));
    // Durability invariant (design §4.2): every repository's buffers — all
    // tables, the buffer-TTL ones included — are flushed and waited on
    // BEFORE the staged engine is dropped, the receipt is sealed and the
    // stage is published. Records that only lived in a membuffer would
    // otherwise vanish with the engine's memory and the published directory
    // would silently miss them.
    let flush_started = Instant::now();
    let flushed =
        loaded.home.flush_all_waiting().await.map_err(|e| {
            LogicalImportError::Stage(format!("cannot flush the staged database: {e}"))
        });
    phase(
        "final_flush",
        flush_started,
        &format!(
            "records={} outcome={}",
            loaded.records,
            outcome_of(flushed.as_ref().err())
        ),
    );
    flushed?;
    // Handle-lifecycle invariant: every handle the import opened (the staged
    // engine, its repositories and the user directory) is closed and dropped
    // BEFORE the publish rename, exactly once per instance — no database may
    // survive a publish or the next open of the directory fails on the lock.
    drop(loaded.home);
    // Deterministic close: the loading engine's detached flusher tasks may
    // still hold the stage's fjall locks (users subdir included); wait until
    // every store is lock-free BEFORE the fresh validation open, so the open
    // can never race a live store. Fail closed on timeout.
    let close_started = Instant::now();
    let released = wait_stores_released(stage.path(), STORE_RELEASE_TIMEOUT)
        .await
        .map_err(|locked| {
            LogicalImportError::Stage(format!(
                "staged stores still locked after drop: {}",
                locked
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        });
    phase(
        "close_wait",
        close_started,
        &format!(
            "stage=validation outcome={}",
            outcome_of(released.as_ref().err())
        ),
    );
    released?;
    // Stage validation reads from a FRESH open of the staged directory, so
    // "validation passed" can never again mean "the loading engine still had
    // the records in memory".
    let reopen_started = Instant::now();
    let reopen = ShamirDb::init(SystemStoreConfig::Fjall(
        stage.path().join("shamir_db_meta.redb"),
    ))
    .await
    .map_err(|e| LogicalImportError::Stage(format!("cannot re-open the staged engine: {e}")));
    phase(
        "staging_reopen",
        reopen_started,
        &format!("outcome={}", outcome_of(reopen.as_ref().err())),
    );
    let reopened = reopen?;
    let validate_started = Instant::now();
    let validated = validate::run(
        &reopened,
        preflight,
        &db_name,
        &loaded.owner_ids,
        &args.input,
        cancel,
    )
    .await;
    phase(
        "validate",
        validate_started,
        &format!(
            "records={} outcome={}",
            loaded.records,
            outcome_of(validated.as_ref().err())
        ),
    );
    validated?;
    drop(reopened);
    // Deterministic close (root-cause fix for the Windows rename): wait until
    // every staged store (shamir_db + users) is lock-free BEFORE the stamp,
    // receipt and publish — the rename below can never hit a locked file.
    let close_started = Instant::now();
    let released = wait_stores_released(stage.path(), STORE_RELEASE_TIMEOUT)
        .await
        .map_err(|locked| {
            LogicalImportError::Stage(format!(
                "staged stores still locked after validation: {}",
                locked
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        });
    phase(
        "close_wait",
        close_started,
        &format!(
            "stage=receipt outcome={}",
            outcome_of(released.as_ref().err())
        ),
    );
    released?;
    progress("validated against the artifact");
    let epoch = match inspect(stage.path()) {
        StampState::Valid(stamp) => stamp.format_epoch(),
        _ => {
            return Err(LogicalImportError::Stage(
                "staged format stamp is unreadable after load".into(),
            ))
        }
    };
    let hash_started = Instant::now();
    let hashed = artifact_sha256(&args.input, cancel);
    phase(
        "receipt_hash",
        hash_started,
        &format!(
            "bytes={} outcome={}",
            preflight.report.bytes,
            outcome_of(hashed.as_ref().err())
        ),
    );
    let artifact_digest = hashed?;
    let receipt = Receipt {
        artifact_sha256: artifact_digest,
        source_db: preflight.database.name.clone(),
        imported_db: loaded.db_name.clone(),
        schema_only: args.schema_only,
        format_epoch: epoch,
        generator: format!(
            "shamir-server logical-import v{}",
            env!("CARGO_PKG_VERSION")
        ),
    };
    receipt.write(stage.path())?;
    if cancel.is_cancelled() {
        return Err(LogicalImportError::Cancelled);
    }
    // (The flush-and-drop above already ran: see the durability invariant.)
    Ok(ImportReport {
        database: db_name,
        items: preflight.report.items,
        bytes: preflight.report.bytes,
        published: true,
        tx_commits: loaded.tx_commits,
        peak_staged_bytes: loaded.peak_staged_bytes,
    })
}

/// A complete existing target is accepted ONLY when its receipt matches the
/// artifact digest and effective options AND an integrity check passes
/// (readable format stamp, engine re-opens, content still equals the
/// artifact). Anything else is a typed refusal — never an overwrite.
async fn retry_existing(
    args: &LogicalImportArgs,
    cancel: &CancellationToken,
) -> Result<Option<ImportReport>, LogicalImportError> {
    let meta = match std::fs::symlink_metadata(&args.target) {
        Ok(meta) => meta,
        Err(_) => return Ok(None),
    };
    if !meta.is_dir() {
        return Err(LogicalImportError::Target(format!(
            "target {} exists and is not a directory",
            args.target.display()
        )));
    }
    let preflight_started = Instant::now();
    let preflight_result = Preflight::run(&args.input, args, cancel);
    let preflight_outcome = outcome_of(preflight_result.as_ref().err());
    let preflight = preflight_result?;
    phase(
        "preflight",
        preflight_started,
        &format!("stage=retry outcome={preflight_outcome}"),
    );
    let receipt = match Receipt::read(&args.target)? {
        Some(receipt) => receipt,
        None => {
            return Err(LogicalImportError::Receipt(format!(
                "target {} exists without a {RECEIPT_NAME} receipt",
                args.target.display()
            )))
        }
    };
    let db_name = args
        .as_db
        .clone()
        .unwrap_or_else(|| preflight.database.name.clone());
    let hash_started = Instant::now();
    let hashed = artifact_sha256(&args.input, cancel);
    phase(
        "receipt_hash",
        hash_started,
        &format!(
            "stage=retry bytes={} outcome={}",
            preflight.report.bytes,
            outcome_of(hashed.as_ref().err())
        ),
    );
    let artifact_digest = hashed?;
    if !receipt.matches(
        &artifact_digest,
        &preflight.database.name,
        &db_name,
        args.schema_only,
    ) {
        return Err(LogicalImportError::Receipt(format!(
            "target {} was imported from a different artifact or options",
            args.target.display()
        )));
    }
    progress("complete target with matching receipt; re-validating");
    match inspect(&args.target) {
        StampState::Valid(_) => {}
        _ => {
            return Err(LogicalImportError::Receipt(format!(
                "target {} does not carry a readable format stamp",
                args.target.display()
            )))
        }
    }
    let owner_ids = target_owner_ids(&args.target, &preflight)?;
    let home = ShamirDb::init(SystemStoreConfig::Fjall(
        args.target.join("shamir_db_meta.redb"),
    ))
    .await
    .map_err(|e| LogicalImportError::Receipt(format!("cannot re-open the target engine: {e}")))?;
    validate::run(&home, &preflight, &db_name, &owner_ids, &args.input, cancel)
        .await
        .map_err(|e| {
            LogicalImportError::Receipt(format!("target no longer matches the artifact: {e}"))
        })?;
    drop(home);
    // Deterministic close: the published target must be lock-free when this
    // returns — test helpers and later runs re-open it immediately.
    wait_stores_released(&args.target, STORE_RELEASE_TIMEOUT)
        .await
        .map_err(|locked| {
            LogicalImportError::Receipt(format!(
                "target stores still locked after validation: {}",
                locked
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
    Ok(Some(ImportReport {
        database: db_name,
        items: preflight.report.items,
        bytes: preflight.report.bytes,
        published: false,
        tx_commits: 0,
        peak_staged_bytes: 0,
    }))
}

/// The retry re-derives the owner-name → principal64 map from the target's
/// user directory (`list_all` includes disabled imported identities; the
/// `lookup_by_name` auth path stays closed by design).
fn target_owner_ids(
    target: &Path,
    preflight: &Preflight,
) -> Result<BTreeMap<String, u64>, LogicalImportError> {
    let directory = FjallUserDirectory::open(target.join("users"))
        .map_err(|e| LogicalImportError::Receipt(format!("cannot read target users: {e}")))?;
    let listed = directory
        .list_all()
        .map_err(|e| LogicalImportError::Receipt(format!("cannot list target users: {e}")))?;
    let mut by_name: BTreeMap<&str, u64> = BTreeMap::new();
    for (id, state) in &listed {
        by_name.insert(state.username.as_str(), *id);
    }
    let mut owners = BTreeMap::new();
    for name in preflight.identities.keys() {
        let id = by_name.get(name.as_str()).copied().ok_or_else(|| {
            LogicalImportError::Receipt(format!("target user directory lost identity '{name}'"))
        })?;
        owners.insert(name.clone(), id);
    }
    Ok(owners)
}

/// Lowercase hex SHA-256 over the whole artifact file (streamed, bounded by
/// the cancellation reader).
fn artifact_sha256(input: &Path, cancel: &CancellationToken) -> Result<String, LogicalImportError> {
    let file = std::fs::File::open(input)?;
    let mut hasher = sha2::Sha256::new();
    let mut reader = CancelledRead::new(file, cancel.clone());
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        hasher.update(&chunk[..read]);
    }
    Ok(hex(&hasher.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn progress(message: &str) {
    eprintln!("logical-import: {message}");
}

/// `outcome=ok|error|cancelled` for a fallible phase line: a
/// `LogicalImportError::Cancelled` maps to `cancelled`, any other `Err` to
/// `error`, `None` to `ok`. Printed BEFORE the result propagates.
fn outcome_of(err: Option<&LogicalImportError>) -> &'static str {
    match err {
        None => "ok",
        Some(LogicalImportError::Cancelled) => "cancelled",
        Some(_) => "error",
    }
}

/// Phase timing line: single-line, no secrets. Mirrors `progress` but with
/// a machine-readable `LOGICAL_IMPORT_PHASE phase=<name> elapsed_ms=<ms>
/// <extras>` shape so CI logs can pin per-phase budgets.
pub(super) fn phase(phase: &str, started: Instant, extras: &str) {
    let elapsed_ms = started.elapsed().as_millis();
    if extras.is_empty() {
        eprintln!("LOGICAL_IMPORT_PHASE phase={phase} elapsed_ms={elapsed_ms}");
    } else {
        eprintln!("LOGICAL_IMPORT_PHASE phase={phase} elapsed_ms={elapsed_ms} {extras}");
    }
}
