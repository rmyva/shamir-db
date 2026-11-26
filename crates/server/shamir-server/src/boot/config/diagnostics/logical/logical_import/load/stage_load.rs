//! Definitions before records: format-stamp the stage like a fresh boot,
//! open the engine inside the stage, mint disabled identities, create the
//! database/repositories/tables with their exported settings, then stream
//! the records with their exact 16-byte keys in bounded batches.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// doc(hidden) test seam: number of `commit_tx` batch commits issued by the
/// record-load stage. Reveals whether the load batches (<= ~records/512)
/// or commits per record.
#[doc(hidden)]
pub static IMPORT_TX_COMMITS: AtomicU64 = AtomicU64::new(0);

pub(crate) struct Loaded {
    pub(crate) home: ShamirDb,
    pub(crate) db_name: String,
    /// Minted identity name → principal64 catalogue owner id.
    pub(crate) owner_ids: BTreeMap<String, u64>,
    /// Records written during the streaming record-load phase.
    pub(crate) records: u64,
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
    let load_started = Instant::now();
    let (records, interners) = load_records(&home, &db_name, artifact, cancel).await?;
    phase("record_load", load_started, &format!("records={records}"));

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

/// Stream the Records section through `TableManager::set_tx` inside one
/// per-repo transaction per [`RECORDS_PER_TX`] records, carrying the exact
/// exported 16-byte raw key and the converted value. Returns the record
/// count and the per-repo interners (their final persist runs in `run`).
async fn load_records(
    home: &ShamirDb,
    db_name: &str,
    artifact: &Path,
    cancel: &CancellationToken,
) -> Result<(u64, BTreeMap<String, InternerManager>), LogicalImportError> {
    let db = home
        .get_db(db_name)
        .ok_or_else(|| load("database disappeared during load".to_string()))?;
    let file = std::fs::File::open(artifact)?;
    let mut reader = DumpReader::new(CancelledRead::new(file, cancel.clone()))
        .map_err(|e| artifact_err(e, cancel))?;
    let mut tables: BTreeMap<String, TableManager> = BTreeMap::new();
    let mut interners: BTreeMap<String, InternerManager> = BTreeMap::new();
    let mut pending: BTreeMap<String, u32> = BTreeMap::new();
    let mut repos: BTreeMap<String, RepoInstance> = BTreeMap::new();
    let mut txs: BTreeMap<String, (TxContext, SnapshotGuard, u32)> = BTreeMap::new();
    let mut records: u64 = 0;
    while let Some(item) = reader.next_item().map_err(|e| artifact_err(e, cancel))? {
        let PortableItem::Record(record) = item else {
            continue;
        };
        load_record(
            &db,
            record,
            &mut tables,
            &mut interners,
            &mut pending,
            &mut repos,
            &mut txs,
        )
        .await?;
        records += 1;
    }
    // Commit every still-open per-repo batch: the final, possibly partial,
    // transaction of each repository touched by the stream.
    for (repo_name, (tx, guard, _pending)) in txs {
        let repo = repos.get(&repo_name).ok_or_else(|| {
            load(format!(
                "repository '{repo_name}' disappeared before its final commit"
            ))
        })?;
        repo.commit_tx(tx)
            .await
            .map_err(|e| load(format!("cannot commit import batch: {e}")))?;
        IMPORT_TX_COMMITS.fetch_add(1, Ordering::Relaxed);
        drop(guard);
    }
    Ok((records, interners))
}

async fn load_record(
    db: &shamir_db::engine::db_instance::db_instance::DbInstance,
    record: RecordItem,
    tables: &mut BTreeMap<String, TableManager>,
    interners: &mut BTreeMap<String, InternerManager>,
    pending: &mut BTreeMap<String, u32>,
    repos: &mut BTreeMap<String, RepoInstance>,
    txs: &mut BTreeMap<String, (TxContext, SnapshotGuard, u32)>,
) -> Result<(), LogicalImportError> {
    let key: [u8; 16] = record.raw_key.try_into().map_err(|_| {
        load(format!(
            "record key for '{}/{}' is not 16 bytes",
            record.repository, record.table
        ))
    })?;
    let value = into_query_value(record.value)
        .map_err(|e| load(format!("record value cannot be converted: {e}")))?;
    let repo = match repos.get(&record.repository) {
        Some(cached) => cached.clone(),
        None => {
            let repo = db
                .get_repo(&record.repository)
                .ok_or_else(|| load(format!("repository '{}' disappeared", record.repository)))?;
            repos.insert(record.repository.clone(), repo.clone());
            repo
        }
    };
    let repo_interner = match interners.get(&record.repository) {
        Some(cached) => cached.clone(),
        None => {
            let manager = repo.repo_interner().await.map_err(|e| {
                load(format!(
                    "cannot open interner for '{}': {e}",
                    record.repository
                ))
            })?;
            interners.insert(record.repository.clone(), manager.clone());
            manager
        }
    };
    let interner = repo_interner
        .get()
        .await
        .map_err(|e| load(format!("cannot load interner: {e}")))?;
    let inner = query_value_to_inner_with(&value, &|key| intern_string_key(interner, key))
        .map_err(|e| load(format!("record value cannot be interned: {e}")))?;
    let table_key = format!("{}/{}", record.repository, record.table);
    let table = match tables.get(&table_key) {
        Some(cached) => cached.clone(),
        None => {
            let manager = db
                .get_table(&record.repository, &record.table)
                .await
                .map_err(|e| load(format!("cannot open table '{table_key}': {e}")))?;
            tables.insert(table_key.clone(), manager.clone());
            manager
        }
    };
    // Take the open tx slot out of the map so `tx` and `guard` are owned
    // here, not borrowed from the map. The raw-key set path bypasses the
    // query builder by design: a keyed upsert must carry the exact 16-byte
    // record id, which no query construction can express.
    let (mut tx, guard, tx_pending) = match txs.remove(&record.repository) {
        Some(entry) => entry,
        None => {
            // The import is the sole writer; Snapshot isolation is safe.
            let (tx, guard) = repo.begin_tx(IsolationLevel::Snapshot).await.map_err(|e| {
                load(format!(
                    "cannot begin import transaction for '{}': {e}",
                    record.repository
                ))
            })?;
            (tx, guard, 0)
        }
    };
    table
        .set_tx(RecordId(key), &inner, Some(&mut tx))
        .await
        .map_err(|e| load(format!("cannot write record: {e}")))?;
    let tx_pending = tx_pending + 1;
    if tx_pending >= RECORDS_PER_TX {
        repo.commit_tx(tx)
            .await
            .map_err(|e| load(format!("cannot commit import batch: {e}")))?;
        IMPORT_TX_COMMITS.fetch_add(1, Ordering::Relaxed);
        // Dropping the guard releases the committed snapshot back to GC.
        drop(guard);
    } else {
        txs.insert(record.repository.clone(), (tx, guard, tx_pending));
    }
    let count = pending.entry(record.repository.clone()).or_insert(0);
    *count += 1;
    if *count >= RECORDS_PER_FLUSH {
        repo_interner
            .persist()
            .await
            .map_err(|e| load(format!("cannot persist interner: {e}")))?;
        *count = 0;
    }
    Ok(())
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
