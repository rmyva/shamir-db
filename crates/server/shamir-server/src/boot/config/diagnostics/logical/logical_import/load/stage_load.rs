//! Definitions before records: format-stamp the stage like a fresh boot,
//! open the engine inside the stage, mint disabled identities, create the
//! database/repositories/tables with their exported settings, then stream
//! the records with their exact 16-byte keys in bounded batches.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

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

/// Counters and peak staged bytes of one record-load attempt.
#[derive(Default)]
struct LoadStats {
    records: u64,
    tx_commits: u64,
    peak_staged_bytes: u64,
}

/// Sub-timer accumulators (u64 ms) of one record-load attempt.
#[derive(Default)]
struct LoadTimings {
    decode_ms: u64,
    stage_ms: u64,
    commit_ms: u64,
    interner_flush_ms: u64,
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
    phase(
        "record_load",
        started,
        &format!(
            "records={} commits={} peak_bytes={} decode_ms={} stage_ms={} commit_ms={} \
             interner_flush_ms={}",
            stats.records,
            stats.tx_commits,
            stats.peak_staged_bytes,
            timings.decode_ms,
            timings.stage_ms,
            timings.commit_ms,
            timings.interner_flush_ms
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
    while let Some(item) = reader.next_item().map_err(|e| artifact_err(e, cancel))? {
        let PortableItem::Record(record) = item else {
            continue;
        };
        load_record(&db, record, &mut caches, cancel, timings, stats).await?;
        stats.records += 1;
    }
    // Commit every still-open per-repo batch: the final, possibly partial,
    // transaction of each repository touched by the stream.
    for (repo_name, (tx, guard, _pending, _staged)) in caches.txs {
        // A cancelled import must not issue another commit: the stage is
        // discarded by the driver's error path and a later retry cleans any
        // stale stage, so stopping here loses nothing.
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
        timings.commit_ms += commit_started.elapsed().as_millis() as u64;
        stats.tx_commits += 1;
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
    let inner = query_value_to_inner_with(&value, &|key| intern_string_key(interner, key))
        .map_err(|e| load(format!("record value cannot be interned: {e}")))?;
    timings.decode_ms += decode_started.elapsed().as_millis() as u64;
    // Encoded size, computed exactly once: the staged value bytes are the
    // `to_bytes()` InnerValue encoding. `to_bytes` cannot produce an empty
    // body for the import's values (the zero-length-body refusal in shamir-tx
    // mvcc store.rs concerns tombstones, which the dump never emits).
    let body_len = inner
        .to_bytes()
        .map(|b| b.len() as u64)
        .map_err(|e| load(format!("record value cannot be encoded: {e}")))?;
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
    // stage, so stopping here loses nothing). A single row whose body alone
    // exceeds RECORD_TX_MAX_BYTES still gets written in its own tx: dump rows
    // are capped at 16 MiB (codec.rs FRAME_LIMIT), far under the 64 MiB WAL
    // frame limit, so it can never breach the frame on its own.
    if tx_pending > 0
        && (tx_pending >= RECORDS_PER_TX || tx_staged + body_len + 16 > RECORD_TX_MAX_BYTES)
    {
        stats.peak_staged_bytes = stats.peak_staged_bytes.max(tx_staged);
        // A cancelled import must not issue another commit (see above).
        if cancel.is_cancelled() {
            return Err(LogicalImportError::Cancelled);
        }
        let commit_started = Instant::now();
        repo.commit_tx(tx)
            .await
            .map_err(|e| load(format!("cannot commit import batch: {e}")))?;
        timings.commit_ms += commit_started.elapsed().as_millis() as u64;
        stats.tx_commits += 1;
        // Dropping the guard releases the committed snapshot back to GC.
        drop(guard);
        let (next_tx, next_guard) = pending_tx(&repo).await?;
        tx = next_tx;
        guard = next_guard;
        tx_pending = 0;
        tx_staged = 0;
    }
    let stage_started = Instant::now();
    table
        .set_tx(RecordId(key), &inner, Some(&mut tx))
        .await
        .map_err(|e| load(format!("cannot write record: {e}")))?;
    timings.stage_ms += stage_started.elapsed().as_millis() as u64;
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
        timings.interner_flush_ms += flush_started.elapsed().as_millis() as u64;
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
