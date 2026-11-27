//! Definitions before records: format-stamp the stage like a fresh boot,
//! open the engine inside the stage, mint disabled identities, create the
//! database/repositories/tables with their exported settings, then stream
//! the records with their exact 16-byte keys in bounded batches.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use shamir_db::access::{principal64, Actor};
use shamir_db::codecs::interned::common::intern_string_key;
use shamir_db::codecs::interned::query_value_to_inner_with;
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig, RepoInstance};
use shamir_db::engine::table::{InternerManager, TableManager};
use shamir_db::storage::storage_membuffer::MemBufferConfig;
use shamir_db::{ShamirDb, SystemStoreConfig};
use shamir_server_security::user_directory::FjallUserDirectory;
use shamir_sync::format_stamp::{check_data_dir, EpochPolicy, IgnoreSet, StampMode};
use shamir_sync::replication_seed::{
    into_query_value, verify_dump_file, DumpReader, PortableItem, RecordItem, RepositoryEngine,
    RepositoryItem, TableItem,
};
use shamir_tx::{IsolationLevel, SnapshotGuard, TxContext};
use shamir_types::types::record_id::RecordId;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::super::driver::phase;
use super::super::error::LogicalImportError;
use super::super::reader::CancelledRead;
use super::super::LogicalImportArgs;
use super::preflight::Preflight;

/// Records written between interner persist flushes. The artifact is
/// streamed one item at a time; never a whole table in memory.
const RECORDS_PER_FLUSH: u32 = 500;

/// Records staged into one per-repo transaction before `commit_tx`. The
/// public tx path amortizes the per-record serialized batch commit into one
/// WAL entry + one data batch per [`RECORDS_PER_TX`] records; the load is
/// the sole writer, so Snapshot isolation cannot conflict.
const RECORDS_PER_TX: u32 = 512;

/// Staged-bytes cap per import transaction. One `commit_tx` becomes one
/// `WalEntryV2`, and one entry is one WAL frame; frames over 64 MiB are
/// rejected (`MAX_FRAME_BYTES`, shamir-wal `segment/frame_reader.rs`). The
/// staged value bytes are `to_bytes()` encodings and the dump caps every row
/// at 16 MiB (`FRAME_LIMIT`, shamir-sync codec.rs), so a single row always
/// fits; the 32 MiB cap leaves ≥2× headroom for op/index/entry/interner-delta
/// overhead inside the frame.
const RECORD_TX_MAX_BYTES: u64 = 32 * 1024 * 1024;

/// Conservative bound on the COMPLETE-entry fixed overhead of one
/// `commit_tx`'s `WalEntryV2` frame, derived from
/// shamir-wal `entry/wal_entry_v2.rs`:
/// - `encode` (wal_entry_v2.rs:269-290) writes a 5-byte header
///   (`WAL_V2_MAGIC` 4 bytes + `WAL_V2_VERSION` 1 byte, :32/:37);
/// - the bincode body carries four u64 scalars (`txn_id`,
///   `repo_id_interned`, `started_at_ns`, `commit_version`, :137-166)
///   = 32 bytes;
/// - two `Vec` length prefixes (`ops`, `interner_delta`) = 2 × 8 = 16
///   bytes;
/// - the `event: Option<WalEventHeader>` discriminant = 1 byte.
///
/// Sum = 54 bytes; 256 gives ≥4× slack (≥2× required) over that sum and
/// still leaves the staged-bytes cap the dominant term.
const ENTRY_HEADER_BYTES: u64 = 256;

/// Conservative per-record op overhead inside the entry's `ops` vector.
///
/// A `Put` op (wal_entry_v2.rs:52-66) is variant tag (bincode u32 = 4) +
/// `table_id_interned` u64 (8) + `rid` 16-byte `RecordId` + body length
/// prefix (8) = 36 bytes. An `IndexPut` (wal_entry_v2.rs:77-93) is tag (4)
/// plus u64 (8) plus u32 (4) plus two length prefixes (16) = 32 bytes.
/// 128 covers both with ≥2× slack, so the bound holds for indexed tables
/// too (the import's indexed table is small, but the bound must not depend
/// on that).
const OP_OVERHEAD_BYTES: u64 = 128;

/// Per-entry overhead of one interner-delta record
/// `(u64 table_token, String name)`: u64 (8) + String length prefix (8)
/// = 16 bytes; 32 gives 2× slack.
const DELTA_ENTRY_OVERHEAD_BYTES: u64 = 32;

/// Pending interner-delta bytes above which the repo interner is
/// persisted BEFORE any commit, so the delta resets and the names become
/// durable before any row that references them commits (the existing
/// CRIT-2 ordering: interner_manager.rs:447-461 flushes the chunk before
/// advancing the watermark).
const DELTA_PERSIST_TRIGGER_BYTES: u64 = RECORD_TX_MAX_BYTES / 4;

/// One admission decision for the next record against the current batch.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) enum Admission {
    /// Stage the row into the open batch.
    Stage,
    /// Commit the open batch first, then re-run the admission.
    CommitFirst,
    /// Persist the repo interner first (delta resets), then re-run.
    PersistFirst,
    /// Refuse: even an empty batch cannot carry this row.
    Refuse,
}

/// Single admission rule for EVERY row, including the first of an empty
/// batch: flush before the row that would cross the cap. `pending` is the
/// open batch's row count, `staged` its staged body+key bytes, `row_bound`
/// the complete next-row bound, `delta_bytes` the pending interner delta
/// (ENTRY included), `usable = RECORD_TX_MAX_BYTES - ENTRY_HEADER_BYTES`.
pub(super) fn admission_for(
    pending: u32,
    staged: u64,
    row_bound: u64,
    delta_bytes: u64,
) -> Admission {
    let usable = RECORD_TX_MAX_BYTES - ENTRY_HEADER_BYTES;
    let next = staged + delta_bytes + row_bound;
    if pending == 0 {
        if next <= usable {
            return Admission::Stage;
        }
        if delta_bytes > 0 {
            return Admission::PersistFirst;
        }
        return Admission::Refuse;
    }
    if pending >= RECORDS_PER_TX || next > usable {
        return Admission::CommitFirst;
    }
    Admission::Stage
}

/// Absolute bounded wait at an armed commit boundary; a timeout proceeds —
/// the cancel check that follows still decides. No sleeps.
const BOUNDARY_WAIT: Duration = Duration::from_secs(30);

/// doc(hidden) commit-boundary handshake for cancellation tests: when
/// armed, every commit site notifies `at_boundary` and waits for `resume`
/// (bounded) BEFORE its cancel check. Counts commits issued while armed.
#[doc(hidden)]
pub struct CommitBoundary {
    pub armed: AtomicBool,
    pub at_boundary: Notify,
    pub resume: Notify,
    pub commits: AtomicU64,
}

/// doc(hidden) global seam (see [`CommitBoundary`]). `pub` so the
/// integration tests in `tests/admin/logical_dump` can reach it through the
/// crate root without widening any EXISTING item; cost when disarmed is
/// one `Acquire` load per commit site.
#[doc(hidden)]
pub static COMMIT_BOUNDARY: CommitBoundary = CommitBoundary {
    armed: AtomicBool::new(false),
    at_boundary: Notify::const_new(),
    resume: Notify::const_new(),
    commits: AtomicU64::new(0),
};

/// Handshake at a commit site (called only when the armed check says so).
async fn commit_boundary_wait() {
    if !COMMIT_BOUNDARY.armed.load(Ordering::Acquire) {
        return;
    }
    COMMIT_BOUNDARY.at_boundary.notify_one();
    let _ = tokio::time::timeout(BOUNDARY_WAIT, COMMIT_BOUNDARY.resume.notified()).await;
}

fn commit_counted() {
    if COMMIT_BOUNDARY.armed.load(Ordering::Acquire) {
        COMMIT_BOUNDARY.commits.fetch_add(1, Ordering::Relaxed);
    }
}

/// Counters and peak staged bytes of one record-load attempt.
#[derive(Default)]
struct LoadStats {
    records: u64,
    tx_commits: u64,
    peak_staged_bytes: u64,
}

/// Sub-timer accumulators (unrounded `Duration`) of one record-load
/// attempt: `read_ms` is the artifact-frame await, `handle_ms` the
/// repo/interner/table handle acquisition, `decode_ms` the value
/// conversion + interning + `to_bytes` encoding.
#[derive(Default)]
struct LoadTimings {
    read_ms: Duration,
    handle_ms: Duration,
    decode_ms: Duration,
    stage_ms: Duration,
    commit_ms: Duration,
    interner_flush_ms: Duration,
}

pub(crate) struct Loaded {
    pub(crate) home: ShamirDb,
    pub(crate) db_name: String,
    /// Minted identity name → principal64 catalogue owner id.
    pub(crate) owner_ids: BTreeMap<String, u64>,
    /// Records written during the streaming record-load phase.
    pub(crate) records: u64,
    /// `commit_tx` batch commits issued by the record-load stage.
    pub(crate) tx_commits: u64,
    /// Highest staged bytes observed at any flush decision point.
    pub(crate) peak_staged_bytes: u64,
}

/// The staged directory must look like a fresh server data dir: the same
/// `check_data_dir` Writer gate a fresh boot runs (it writes the format
/// stamp as the first file of the empty stage), then `ShamirDb::init` with
/// the same `shamir_db_meta.redb` layout the launcher uses.
pub(crate) async fn run(
    stage: &Path,
    artifact: &Path,
    pf: &Preflight,
    args: &LogicalImportArgs,
    cancel: &CancellationToken,
) -> Result<Loaded, LogicalImportError> {
    if cancel.is_cancelled() {
        return Err(LogicalImportError::Cancelled);
    }
    let init_started = Instant::now();
    check_data_dir(
        stage,
        StampMode::Writer,
        &IgnoreSet::none(),
        &EpochPolicy::compiled(),
    )
    .map_err(|e| load(format!("format stamp gate refused the stage: {e}")))?;
    let home = ShamirDb::init(SystemStoreConfig::Fjall(stage.join("shamir_db_meta.redb")))
        .await
        .map_err(|e| load(format!("cannot open the staged engine: {e}")))?;
    let user_dir = FjallUserDirectory::open(stage.join("users"))
        .map_err(|e| load(format!("cannot open the staged user directory: {e}")))?;
    phase(
        "init",
        init_started,
        &format!("identities={}", pf.identities.len()),
    );

    let definitions_started = Instant::now();
    let mut owner_ids = BTreeMap::new();
    let mut user_ids = BTreeMap::new();
    for name in pf.identities.keys() {
        let user_id = user_dir
            .insert_disabled_import(name.clone(), args.as_db.clone())
            .map_err(|e| load(format!("cannot mint disabled identity '{name}': {e}")))?;
        user_ids.insert(name.clone(), user_id);
        owner_ids.insert(name.clone(), principal64(user_id));
    }

    let db_name = args
        .as_db
        .clone()
        .unwrap_or_else(|| pf.database.name.clone());
    let db_owner = actor_for(pf.database.owner.as_deref(), &user_ids)?;
    home.create_db_as(&db_name, db_owner)
        .await
        .map_err(|e| load(format!("cannot create database '{db_name}': {e}")))?;

    for item in pf.repos.values() {
        let owner = actor_for(item.owner.as_deref(), &user_ids)?;
        create_repo(&home, stage, &db_name, item, owner).await?;
    }
    for item in pf.tables.values() {
        let owner = actor_for(item.owner.as_deref(), &user_ids)?;
        create_table(&home, &db_name, item, owner).await?;
    }
    phase(
        "definitions",
        definitions_started,
        &format!("repositories={} tables={}", pf.repos.len(), pf.tables.len()),
    );
    let (records, interners, tx_commits, peak_staged_bytes) =
        load_records(&home, &db_name, artifact, cancel).await?;

    let persist_started = Instant::now();
    for (repo, interner) in &interners {
        interner
            .persist()
            .await
            .map_err(|e| load(format!("cannot persist interner for '{repo}': {e}")))?;
    }
    phase(
        "interner_persist",
        persist_started,
        &format!("repos={}", interners.len()),
    );

    let reverify_started = Instant::now();
    verify_input_unchanged(artifact, pf).await?;
    phase("input_reverify", reverify_started, "");

    Ok(Loaded {
        home,
        db_name,
        owner_ids,
        records,
        tx_commits,
        peak_staged_bytes,
    })
}

/// Owner names resolve ONLY through identities minted from the artifact;
/// an unresolved owner is a typed refusal, never a silent System grant.
fn actor_for(
    owner: Option<&str>,
    user_ids: &BTreeMap<String, [u8; 16]>,
) -> Result<Actor, LogicalImportError> {
    match owner {
        None => Ok(Actor::System),
        Some(name) => {
            let user_id = user_ids
                .get(name)
                .ok_or_else(|| load(format!("owner '{name}' is not an exported identity")))?;
            Ok(Actor::User(principal64(*user_id)))
        }
    }
}

async fn create_repo(
    home: &ShamirDb,
    stage: &Path,
    db_name: &str,
    item: &RepositoryItem,
    owner: Actor,
) -> Result<(), LogicalImportError> {
    if item.engine != RepositoryEngine::Fjall {
        return Err(load(format!(
            "repository '{}' has unsupported engine kind (only persisted Fjall is importable)",
            item.name
        )));
    }
    if item.durability != shamir_sync::replication_seed::Durability::Default {
        return Err(load(format!(
            "repository '{}' has a durability override that the exporter refuses to emit",
            item.name
        )));
    }
    // Mirror the launcher's default-repository path convention: one
    // `<data-dir>/shamir_db_<db>_<repo>.redb` store per repository.
    let path = stage.join(format!("shamir_db_{db_name}_{}.redb", item.name));
    let config = RepoConfig::new(item.name.clone(), BoxRepoFactory::fjall(path));
    home.add_repo_as_with_durability(db_name, config, owner, None)
        .await
        .map_err(|e| load(format!("cannot create repository '{}': {e}", item.name)))
}

async fn create_table(
    home: &ShamirDb,
    db_name: &str,
    item: &TableItem,
    owner: Actor,
) -> Result<(), LogicalImportError> {
    home.add_table_as(
        db_name,
        &item.repository,
        &item.name,
        item.enable_indexes,
        owner,
    )
    .await
    .map_err(|e| {
        load(format!(
            "cannot create table '{}/{}': {e}",
            item.repository, item.name
        ))
    })?;
    // The validate stage re-reads the staged content through the engine
    // read facade, which refuses unopened tables (a booted server opens
    // every catalogue table); open each created table regardless of TTL.
    let db = home
        .get_db(db_name)
        .ok_or_else(|| load("database disappeared during load".to_string()))?;
    let table = db
        .get_table(&item.repository, &item.name)
        .await
        .map_err(|e| {
            load(format!(
                "cannot open table '{}/{}': {e}",
                item.repository, item.name
            ))
        })?;
    let Some(ttl) = item.buffer_ttl_ms else {
        return Ok(());
    };
    table
        .set_buffer_config(&MemBufferConfig {
            ttl_ms: Some(ttl),
            ..MemBufferConfig::default()
        })
        .await
        .map_err(|e| {
            load(format!(
                "cannot set buffer TTL for '{}/{}': {e}",
                item.repository, item.name
            ))
        })
}

/// Per-repo caches and open transactions of one record-load attempt.
#[derive(Default)]
struct LoadCaches {
    tables: BTreeMap<String, TableManager>,
    interners: BTreeMap<String, InternerManager>,
    pending: BTreeMap<String, u32>,
    repos: BTreeMap<String, RepoInstance>,
    txs: BTreeMap<String, (TxContext, SnapshotGuard, u32, u64)>,
    /// Per-repo pending interner-delta byte bound (`DELTA_ENTRY_OVERHEAD`
    /// plus name bytes per first-touch entry). Names ride the commit's
    /// `interner_delta`; this is the term the staged-only meter missed.
    delta_bytes: BTreeMap<String, u64>,
    /// Per-repo new interned ids (id → original name length), dedup by id.
    delta_new: BTreeMap<String, BTreeMap<u64, usize>>,
}

/// Stream the Records section through `TableManager::set_tx` inside one
/// per-repo transaction per [`RECORDS_PER_TX`] records or
/// [`RECORD_TX_MAX_BYTES`] staged bytes, carrying the exact exported 16-byte
/// raw key and the converted value. Returns the record count, the per-repo
/// interners (their final persist runs in `run`) and the attempt's commit/
/// peak counters. Prints the `record_load` phase line on BOTH success and
/// failure/cancel exit paths (timings accumulated up to the failure point).
async fn load_records(
    home: &ShamirDb,
    db_name: &str,
    artifact: &Path,
    cancel: &CancellationToken,
) -> Result<(u64, BTreeMap<String, InternerManager>, u64, u64), LogicalImportError> {
    let started = Instant::now();
    let mut timings = LoadTimings::default();
    let mut stats = LoadStats::default();
    let result =
        load_records_inner(home, db_name, artifact, cancel, &mut timings, &mut stats).await;
    let outcome = match &result {
        Ok(_) => "ok",
        Err(LogicalImportError::Cancelled) => "cancelled",
        Err(_) => "error",
    };
    phase(
        "record_load",
        started,
        &format!(
            "records={} commits={} peak_bytes={} outcome={} read_ms={} handle_ms={} \
             decode_ms={} stage_ms={} commit_ms={} interner_flush_ms={}",
            stats.records,
            stats.tx_commits,
            stats.peak_staged_bytes,
            outcome,
            timings.read_ms.as_millis(),
            timings.handle_ms.as_millis(),
            timings.decode_ms.as_millis(),
            timings.stage_ms.as_millis(),
            timings.commit_ms.as_millis(),
            timings.interner_flush_ms.as_millis()
        ),
    );
    result.map(|(records, interners)| {
        (
            records,
            interners,
            stats.tx_commits,
            stats.peak_staged_bytes,
        )
    })
}

async fn load_records_inner(
    home: &ShamirDb,
    db_name: &str,
    artifact: &Path,
    cancel: &CancellationToken,
    timings: &mut LoadTimings,
    stats: &mut LoadStats,
) -> Result<(u64, BTreeMap<String, InternerManager>), LogicalImportError> {
    let db = home
        .get_db(db_name)
        .ok_or_else(|| load("database disappeared during load".to_string()))?;
    let file = std::fs::File::open(artifact)?;
    let mut reader = DumpReader::new(CancelledRead::new(file, cancel.clone()))
        .map_err(|e| artifact_err(e, cancel))?;
    let mut caches = LoadCaches::default();
    while let Some(item) = {
        let read_started = Instant::now();
        let item = reader.next_item().map_err(|e| artifact_err(e, cancel))?;
        timings.read_ms += read_started.elapsed();
        item
    } {
        let PortableItem::Record(record) = item else {
            continue;
        };
        load_record(&db, record, &mut caches, cancel, timings, stats).await?;
        stats.records += 1;
    }
    // Commit every still-open per-repo batch: the final, possibly partial,
    // transaction of each repository touched by the stream.
    for (repo_name, (tx, guard, _pending, _staged)) in caches.txs {
        // Names first: above the trigger the delta is persisted BEFORE the
        // commit so it resets (names durable before referencing rows
        // commit — CRIT-2 ordering) and cannot breach the WAL frame.
        if caches.delta_bytes.get(&repo_name).copied().unwrap_or(0) > DELTA_PERSIST_TRIGGER_BYTES {
            let interner = caches.interners.get(&repo_name).ok_or_else(|| {
                load(format!(
                    "interner '{repo_name}' disappeared before its final commit"
                ))
            })?;
            let flush_started = Instant::now();
            interner
                .persist()
                .await
                .map_err(|e| load(format!("cannot persist interner: {e}")))?;
            timings.interner_flush_ms += flush_started.elapsed();
            caches.delta_bytes.insert(repo_name.clone(), 0);
            caches.delta_new.insert(repo_name.clone(), BTreeMap::new());
        }
        // A cancelled import must not issue another commit: the stage is
        // discarded by the driver's error path and a later retry cleans any
        // stale stage, so stopping here loses nothing.
        commit_boundary_wait().await;
        if cancel.is_cancelled() {
            return Err(LogicalImportError::Cancelled);
        }
        let repo = caches.repos.get(&repo_name).ok_or_else(|| {
            load(format!(
                "repository '{repo_name}' disappeared before its final commit"
            ))
        })?;
        let commit_started = Instant::now();
        repo.commit_tx(tx)
            .await
            .map_err(|e| load(format!("cannot commit import batch: {e}")))?;
        timings.commit_ms += commit_started.elapsed();
        stats.tx_commits += 1;
        commit_counted();
        drop(guard);
    }
    Ok((stats.records, caches.interners))
}

async fn load_record(
    db: &shamir_db::engine::db_instance::db_instance::DbInstance,
    record: RecordItem,
    caches: &mut LoadCaches,
    cancel: &CancellationToken,
    timings: &mut LoadTimings,
    stats: &mut LoadStats,
) -> Result<(), LogicalImportError> {
    let key: [u8; 16] = record.raw_key.try_into().map_err(|_| {
        load(format!(
            "record key for '{}/{}' is not 16 bytes",
            record.repository, record.table
        ))
    })?;
    let decode_started = Instant::now();
    let value = into_query_value(record.value)
        .map_err(|e| load(format!("record value cannot be converted: {e}")))?;
    let repo = match caches.repos.get(&record.repository) {
        Some(cached) => cached.clone(),
        None => {
            let repo = db
                .get_repo(&record.repository)
                .ok_or_else(|| load(format!("repository '{}' disappeared", record.repository)))?;
            caches.repos.insert(record.repository.clone(), repo.clone());
            repo
        }
    };
    let repo_interner = match caches.interners.get(&record.repository) {
        Some(cached) => cached.clone(),
        None => {
            let manager = repo.repo_interner().await.map_err(|e| {
                load(format!(
                    "cannot open interner for '{}': {e}",
                    record.repository
                ))
            })?;
            caches
                .interners
                .insert(record.repository.clone(), manager.clone());
            manager
        }
    };
    let interner = repo_interner
        .get()
        .await
        .map_err(|e| load(format!("cannot load interner: {e}")))?;
    let table_key = format!("{}/{}", record.repository, record.table);
    let table = match caches.tables.get(&table_key) {
        Some(cached) => cached.clone(),
        None => {
            let manager = db
                .get_table(&record.repository, &record.table)
                .await
                .map_err(|e| load(format!("cannot open table '{table_key}': {e}")))?;
            caches.tables.insert(table_key.clone(), manager.clone());
            manager
        }
    };
    timings.handle_ms += decode_started.elapsed();
    let decode_started = Instant::now();
    // Wrap the interning closure: on each `intern_string_key` result, a
    // first-touch id above `persisted_high_water()` contributes its ORIGINAL
    // name bytes (plus [`DELTA_ENTRY_OVERHEAD_BYTES`]) to the repo's pending
    // delta — the reviewer's missing term, i.e. exactly the bytes the
    // commit's `interner_delta` will carry. `persist()` advances the
    // watermark, so this `id > high_water` check keeps the accounting
    // correct after every early persist. The closure is `Fn` at the call
    // site, so the delta maps are reached through `RefCell` (interior
    // mutability, single-threaded).
    let repo_name = record.repository.clone();
    let inner = {
        // The closure is `Fn` at the call site, so the delta maps are
        // reached through `RefCell` (interior mutability, single-threaded);
        // the block scope ends the borrows right after the call.
        let delta_new_cell = RefCell::new(&mut caches.delta_new);
        let delta_bytes_cell = RefCell::new(&mut caches.delta_bytes);
        query_value_to_inner_with(&value, &|key| {
            let id = intern_string_key(interner, key)?;
            if id.id() as usize > repo_interner.persisted_high_water() {
                let fresh = delta_new_cell
                    .borrow_mut()
                    .entry(repo_name.clone())
                    .or_default()
                    .insert(id.id(), key.len())
                    .is_none();
                if fresh {
                    *delta_bytes_cell
                        .borrow_mut()
                        .entry(repo_name.clone())
                        .or_insert(0) += DELTA_ENTRY_OVERHEAD_BYTES + key.len() as u64;
                }
            }
            Ok(id)
        })
        .map_err(|e| load(format!("record value cannot be interned: {e}")))?
    };
    timings.decode_ms += decode_started.elapsed();
    // Encoded size, computed exactly once: the staged value bytes are the
    // `to_bytes()` InnerValue encoding. `to_bytes` cannot produce an empty
    // body for the import's values (the zero-length-body refusal in shamir-tx
    // mvcc store.rs concerns tombstones, which the dump never emits).
    let body_len = inner
        .to_bytes()
        .map(|b| b.len() as u64)
        .map_err(|e| load(format!("record value cannot be encoded: {e}")))?;
    // Take the open tx slot out of the map so `tx` and `guard` are owned
    // here, not borrowed from the map. The raw-key set path bypasses the
    // query builder by design: a keyed upsert must carry the exact 16-byte
    // record id, which no query construction can express.
    let (mut tx, mut guard, mut tx_pending, mut tx_staged) =
        match caches.txs.remove(&record.repository) {
            Some(entry) => entry,
            None => {
                let (tx, guard) = pending_tx(&repo).await?;
                (tx, guard, 0, 0)
            }
        };
    // Flush BEFORE staging when the batch is full by count or bytes: a
    // cancelled import must not issue another commit (on cancel the stage is
    // discarded by the driver's error path and a retry cleans any stale
    // stage, so stopping here loses nothing). The admission rule covers the
    // FIRST row of an empty batch too (Refuse possible there). A dump row's
    // body is capped at 16 MiB (codec.rs FRAME_LIMIT), so with small deltas
    // the typed refusal below is unreachable in practice — it guards the
    // invariant that no un-splittable oversized entry is ever submitted.
    // 1-based record ordinal for refusal reporting.
    let position = stats.records + 1;
    // Above the trigger the pending delta alone persists the interner
    // before any commit so the delta resets (names durable before
    // referencing rows commit — CRIT-2 ordering).
    if caches
        .delta_bytes
        .get(&record.repository)
        .copied()
        .unwrap_or(0)
        > DELTA_PERSIST_TRIGGER_BYTES
    {
        let flush_started = Instant::now();
        repo_interner
            .persist()
            .await
            .map_err(|e| load(format!("cannot persist interner: {e}")))?;
        timings.interner_flush_ms += flush_started.elapsed();
        caches.delta_bytes.insert(record.repository.clone(), 0);
        caches
            .delta_new
            .insert(record.repository.clone(), BTreeMap::new());
    }
    let row_bound = OP_OVERHEAD_BYTES + 16 + body_len;
    let mut rounds = 0u8;
    loop {
        let delta_bytes = caches
            .delta_bytes
            .get(&record.repository)
            .copied()
            .unwrap_or(0);
        match admission_for(tx_pending, tx_staged, row_bound, delta_bytes) {
            Admission::Stage => break,
            Admission::CommitFirst => {
                commit_boundary_wait().await;
                // A cancelled import must not issue another commit (see above).
                if cancel.is_cancelled() {
                    return Err(LogicalImportError::Cancelled);
                }
                let commit_started = Instant::now();
                repo.commit_tx(tx)
                    .await
                    .map_err(|e| load(format!("cannot commit import batch: {e}")))?;
                timings.commit_ms += commit_started.elapsed();
                stats.tx_commits += 1;
                commit_counted();
                // Dropping the guard releases the committed snapshot back to GC.
                drop(guard);
                let (next_tx, next_guard) = pending_tx(&repo).await?;
                tx = next_tx;
                guard = next_guard;
                tx_pending = 0;
                tx_staged = 0;
                break;
            }
            Admission::PersistFirst => {
                let flush_started = Instant::now();
                repo_interner
                    .persist()
                    .await
                    .map_err(|e| load(format!("cannot persist interner: {e}")))?;
                timings.interner_flush_ms += flush_started.elapsed();
                caches.delta_bytes.insert(record.repository.clone(), 0);
                caches
                    .delta_new
                    .insert(record.repository.clone(), BTreeMap::new());
                rounds += 1;
                // Defensive cap: a persist resets delta_bytes to 0, so a
                // second PersistFirst in a row is impossible — never spin.
                if rounds > 3 {
                    let delta_bytes = caches
                        .delta_bytes
                        .get(&record.repository)
                        .copied()
                        .unwrap_or(0);
                    return Err(LogicalImportError::RecordTooLarge {
                        position,
                        bound_bytes: ENTRY_HEADER_BYTES + tx_staged + delta_bytes + row_bound,
                        cap_bytes: RECORD_TX_MAX_BYTES,
                    });
                }
            }
            Admission::Refuse => {
                let delta_bytes = caches
                    .delta_bytes
                    .get(&record.repository)
                    .copied()
                    .unwrap_or(0);
                return Err(LogicalImportError::RecordTooLarge {
                    position,
                    bound_bytes: ENTRY_HEADER_BYTES + tx_staged + delta_bytes + row_bound,
                    cap_bytes: RECORD_TX_MAX_BYTES,
                });
            }
        }
    }
    // Peak stays the round-2 metric (staged only) for comparability.
    stats.peak_staged_bytes = stats.peak_staged_bytes.max(tx_staged);
    let stage_started = Instant::now();
    table
        .set_tx(RecordId(key), &inner, Some(&mut tx))
        .await
        .map_err(|e| load(format!("cannot write record: {e}")))?;
    timings.stage_ms += stage_started.elapsed();
    // 16 = raw key length; per-row overhead constants are unnecessary at
    // this granularity (the 32 MiB headroom covers op/index/entry overhead).
    tx_staged += body_len + 16;
    tx_pending += 1;
    stats.peak_staged_bytes = stats.peak_staged_bytes.max(tx_staged);
    caches.txs.insert(
        record.repository.clone(),
        (tx, guard, tx_pending, tx_staged),
    );
    let count = caches.pending.entry(record.repository.clone()).or_insert(0);
    *count += 1;
    if *count >= RECORDS_PER_FLUSH {
        let flush_started = Instant::now();
        repo_interner
            .persist()
            .await
            .map_err(|e| load(format!("cannot persist interner: {e}")))?;
        timings.interner_flush_ms += flush_started.elapsed();
        // The persist advanced the watermark; the delta (already durable
        // now) resets and the admission accounting stays exact.
        caches.delta_bytes.insert(record.repository.clone(), 0);
        caches
            .delta_new
            .insert(record.repository.clone(), BTreeMap::new());
        *count = 0;
    }
    Ok(())
}

/// Fresh transaction for a repository: the import is the sole writer, so
/// Snapshot isolation is safe.
async fn pending_tx(repo: &RepoInstance) -> Result<(TxContext, SnapshotGuard), LogicalImportError> {
    repo.begin_tx(IsolationLevel::Snapshot)
        .await
        .map_err(|e| load(format!("cannot begin import transaction: {e}")))
}

/// Re-read and re-hash the whole input after loading; any drift from the
/// preflight prefix digest aborts the import.
async fn verify_input_unchanged(artifact: &Path, pf: &Preflight) -> Result<(), LogicalImportError> {
    let root = artifact.to_owned();
    let report = tokio::task::spawn_blocking(move || verify_dump_file(&root))
        .await
        .map_err(|e| load(format!("re-verification join failed: {e}")))?
        .map_err(|e| load(format!("artifact changed during import: {e}")))?;
    if report.prefix_sha256 != pf.report.prefix_sha256 {
        return Err(LogicalImportError::InputChanged);
    }
    Ok(())
}

fn load(message: String) -> LogicalImportError {
    LogicalImportError::Load(message)
}

fn artifact_err(
    e: shamir_sync::replication_seed::SeedError,
    cancel: &CancellationToken,
) -> LogicalImportError {
    if cancel.is_cancelled() {
        LogicalImportError::Cancelled
    } else {
        LogicalImportError::Artifact(e)
    }
}
