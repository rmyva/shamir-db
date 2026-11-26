//! Shared harness for the offline logical-import integration tests: real
//! database fixtures built through the query builder, offline export/import
//! drivers, the engine read facade used to re-read published targets, and
//! hand-crafted artifacts with frame-surgery helpers for the negative cases.

use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};
use shamir_connect::common::crypto::StoredKey;
use shamir_connect::common::kdf_params::KdfParams;
use shamir_connect::server::user_record::UserRecord;
use shamir_db::access::{principal64, Actor};
use shamir_db::engine::db_instance::logical_export::MaintenanceGate;
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::storage::storage_membuffer::MemBufferConfig;
use shamir_db::{DbError, ShamirDb, SystemStoreConfig};
use shamir_query_builder::batch::Batch;
use shamir_query_builder::{doc, write};
use shamir_server::logical_dump::{self, LogicalExportArgs};
use shamir_server::logical_import::{self, ImportReport, LogicalImportArgs, LogicalImportError};
use shamir_server_security::user_directory::FjallUserDirectory;
use shamir_sync::replication_seed::{
    from_query_value, DatabaseItem, DumpReader, DumpScope, DumpWriter, Durability, EncryptionMode,
    IdentityItem, IdentityKind, LogicalDumpHeader, LogicalValue, PortableItem, RecordItem,
    RepositoryEngine, RepositoryItem, RepositoryVersion, SectionInventory, SectionKind, SeedError,
    TableItem,
};
use shamir_types::types::value::QueryValue;
use tokio::runtime::{Builder, Runtime};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

/// Per-frame byte budget of the container format (16 MiB), as enforced by
/// the verifier before any allocation.
pub const FRAME_LIMIT: usize = 16 * 1024 * 1024;

/// Canonical fixstr encoding of the hand-crafted identity name "alice"
/// (`0xa5` + 5 bytes) — the surgery needle for the non-canonical case.
pub const ALICE_FIXSTR: [u8; 6] = [0xa5, b'a', b'l', b'i', b'c', b'e'];

/// Stage prefix written beside the target by `logical-import`.
const STAGE_PREFIX: &str = ".logical-import-";

/// Current-thread runtime, mirroring the `logical_dump` test precedent.
/// `enable_all` brings the time driver, so `tokio::time::timeout` works —
/// but only for futures created and awaited ON this runtime: a timer built
/// outside `block_on` has no reactor and panics.
pub fn runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

/// One generous bound per awaited test stage (the Windows runner can be up
/// to 10x slower; nothing here is a synchronization sleep).
pub const STAGE_TIMEOUT: Duration = Duration::from_secs(120);

/// Drive `fut` on the caller's single runtime, bounded by `STAGE_TIMEOUT`.
/// The timeout future is created AND polled inside `block_on`, so the timer
/// always has the runtime's time driver available.
pub fn stage_timeout<T>(rt: &Runtime, label: &'static str, fut: impl Future<Output = T>) -> T {
    rt.block_on(async {
        tokio::time::timeout(STAGE_TIMEOUT, fut)
            .await
            .unwrap_or_else(|_| panic!("{label} timed out"))
    })
}

/// Export; a `Locked` refusal comes from the driver's `MaintenanceOpen` (it
/// opens the source's Fjall directories), which fails when a just-dropped
/// engine's background tasks have not released its directory locks yet (seen
/// on Windows). Opening fails before anything is written, so retrying is
/// safe; the retry is bound to that specific error and a budget, and any
/// other error fails at once. The pause runs on the caller's runtime, which
/// also lets the dropped engine's tasks finish.
///
/// The export error can be downcast to `SeedError::Database`, but the Fjall
/// backend stores only `DbError::Storage(e.to_string())`. Match the exact
/// `fjall::Error::Locked` display text from Fjall 3.1.13, not the error chain's
/// formatted text.
pub async fn export_waiting_for_locks(
    root: &Path,
    args: &LogicalExportArgs,
) -> shamir_sync::replication_seed::DumpReport {
    let deadline = tokio::time::Instant::now() + STAGE_TIMEOUT;
    loop {
        match logical_dump::export(root, args, CancellationToken::new()).await {
            Ok(report) => return report,
            Err(error) => {
                let locked = matches!(
                    error.downcast_ref::<SeedError>(),
                    Some(SeedError::Database(DbError::Storage(message))) if message == "Locked"
                );
                let retry_at = tokio::time::Instant::now() + Duration::from_millis(50);
                if !locked || retry_at >= deadline {
                    panic!("logical export failed: {error:#}");
                }
                tokio::time::sleep_until(retry_at).await;
                if tokio::time::Instant::now() >= deadline {
                    panic!("logical export failed: {error:#}");
                }
            }
        }
    }
}

pub fn export_args(db: &str, output: &Path, schema_only: bool) -> LogicalExportArgs {
    LogicalExportArgs {
        db: db.into(),
        output: output.into(),
        repo: None,
        table: None,
        schema_only,
        include_artifacts: false,
    }
}

pub fn import_args(input: &Path, target: &Path) -> LogicalImportArgs {
    LogicalImportArgs {
        input: input.into(),
        target: target.into(),
        schema_only: false,
        as_db: None,
    }
}

pub fn import_args_as_db(input: &Path, target: &Path, name: &str) -> LogicalImportArgs {
    LogicalImportArgs {
        input: input.into(),
        target: target.into(),
        schema_only: false,
        as_db: Some(name.into()),
    }
}

/// Drive the offline export driver synchronously; the fixture database must
/// already be closed.
pub fn export_now(
    root: &Path,
    args: &LogicalExportArgs,
) -> shamir_sync::replication_seed::DumpReport {
    runtime()
        .block_on(logical_dump::export(root, args, CancellationToken::new()))
        .unwrap()
}

/// Drive the offline import driver synchronously and keep the typed result.
pub fn import_now(
    args: &LogicalImportArgs,
    cancel: CancellationToken,
) -> Result<ImportReport, LogicalImportError> {
    runtime().block_on(logical_import::import(args.clone(), cancel))
}

fn user_record() -> UserRecord {
    UserRecord {
        salt: [7; 16],
        stored_key: StoredKey([8; 32]),
        server_key: Zeroizing::new([9; 32]),
        kdf_params: KdfParams::DEFAULT,
        tickets_invalid_before_ns: 0,
    }
}

/// Create the `users/` directory with one enabled user "owner" and return
/// the principal64 id the catalogue stores for it. Ownership stamped with
/// this actor is what the export projects into named identities.
async fn create_owner_user(root: &Path) -> u64 {
    let directory = FjallUserDirectory::open(root.join("users")).unwrap();
    let user_id = directory
        .insert_with_scope("owner".to_string(), user_record(), None)
        .unwrap();
    principal64(user_id)
}

/// A real multi-repository, multi-table database: string/list/nested-map/
/// binary/int/float/null/bool record values, `enable_indexes` on one table
/// and a buffer TTL on another, plus a `root_meta` setting and a named
/// owner on every catalogue row. Returns the open engine so callers can
/// append more records; callers drop it before exporting.
pub struct Fixture {
    pub home: ShamirDb,
    pub owner_principal: u64,
    /// (repository, table, enable_indexes, ttl) for every built table.
    pub tables: Vec<(&'static str, &'static str, bool, Option<u64>)>,
}

pub async fn build_fixture(root: &Path) -> Fixture {
    let owner = create_owner_user(root).await;

    let home = ShamirDb::init(SystemStoreConfig::Fjall(root.join("shamir_db_meta.redb")))
        .await
        .unwrap();
    home.create_db_as("app", Actor::User(owner)).await.unwrap();
    home.add_repo_as_with_durability(
        "app",
        RepoConfig::new("main", BoxRepoFactory::fjall_raw(root.join("data_main"))),
        Actor::User(owner),
        None,
    )
    .await
    .unwrap();
    home.add_repo_as_with_durability(
        "app",
        RepoConfig::new("cold", BoxRepoFactory::fjall_raw(root.join("data_cold"))),
        Actor::User(owner),
        None,
    )
    .await
    .unwrap();
    home.add_table_as("app", "main", "rows", false, Actor::User(owner))
        .await
        .unwrap();
    home.add_table_as("app", "main", "users", true, Actor::User(owner))
        .await
        .unwrap();
    home.add_table_as("app", "cold", "events", false, Actor::User(owner))
        .await
        .unwrap();
    let instance = home.get_db("app").unwrap();
    // The export read facade refuses unopened tables; open every fixture
    // table the way a booted server does before the engine is dropped.
    for (repo, table) in [("main", "rows"), ("main", "users"), ("cold", "events")] {
        instance.get_table(repo, table).await.unwrap();
    }
    let users = instance.get_table("main", "users").await.unwrap();
    users
        .set_buffer_config(&MemBufferConfig {
            ttl_ms: Some(5_000),
            ..MemBufferConfig::default()
        })
        .await
        .unwrap();
    home.system_store()
        .save_setting(
            "root_meta",
            &doc! { "owner" => 0_i64, "mode" => 0o700_i64 }.into(),
        )
        .await
        .unwrap();

    let mut batch = Batch::new();
    {
        // Nested list/map values cannot travel through the `doc!` builder
        // (its `FilterValue` conversion refuses them), so the row value is
        // built as a `QueryValue` map directly.
        let mut l = shamir_types::types::common::new_map();
        l.insert("s".to_string(), QueryValue::Str("ambiguous".into()));
        l.insert("i".to_string(), QueryValue::Int(-42));
        l.insert("f".to_string(), QueryValue::F64(2.5));
        l.insert("b".to_string(), QueryValue::Bin(vec![0, 0xff, 0x7f]));
        l.insert("z".to_string(), QueryValue::Null);
        l.insert("flag".to_string(), QueryValue::Bool(true));
        l.insert(
            "l".to_string(),
            QueryValue::List(vec![QueryValue::Int(1), QueryValue::Str("two".into())]),
        );
        let mut nested = shamir_types::types::common::new_map();
        nested.insert("z".to_string(), QueryValue::Int(1));
        nested.insert("a".to_string(), QueryValue::Str("x".into()));
        l.insert("m".to_string(), QueryValue::Map(nested));
        batch.op(
            "put_rows_rich",
            write::Insert::with_repo("main", "rows").row(QueryValue::Map(l)),
        );
    }
    batch.op(
        "put_rows_second",
        write::Insert::with_repo("main", "rows").row(doc! { "s" => "second" }),
    );
    batch.op(
        "put_users_ada",
        write::Insert::with_repo("main", "users").row(doc! { "name" => "ada" }),
    );
    batch.op(
        "put_events_tick",
        write::Insert::with_repo("cold", "events").row(doc! { "kind" => "tick", "n" => 7_i64 }),
    );
    home.execute("app", &batch.build()).await.unwrap();
    home.flush_all_waiting().await.unwrap();
    Fixture {
        home,
        owner_principal: owner,
        tables: vec![
            ("main", "rows", false, None),
            ("main", "users", true, Some(5_000)),
            ("cold", "events", false, None),
        ],
    }
}

/// Read every record of the artifact into `repo/table → key → value`.
pub fn artifact_tables(artifact: &Path) -> BTreeMap<String, BTreeMap<Vec<u8>, LogicalValue>> {
    let mut reader = DumpReader::new(fs::File::open(artifact).unwrap()).unwrap();
    let mut tables: BTreeMap<String, BTreeMap<Vec<u8>, LogicalValue>> = BTreeMap::new();
    while let Some(item) = reader.next_item().unwrap() {
        if let PortableItem::Record(record) = item {
            tables
                .entry(format!("{}/{}", record.repository, record.table))
                .or_default()
                .insert(record.raw_key, record.value);
        }
    }
    tables
}

/// Every portable item of the artifact, in stream order (the diagnostic
/// header is not an item, so it is excluded by construction).
pub fn artifact_items(artifact: &Path) -> Vec<PortableItem> {
    let mut reader = DumpReader::new(fs::File::open(artifact).unwrap()).unwrap();
    let mut items = Vec::new();
    while let Some(item) = reader.next_item().unwrap() {
        items.push(item);
    }
    items
}

/// Engine snapshot of a published data directory, read through the same
/// maintenance-gate capture facade the export driver and the import
/// validator use.
pub struct EngineSnapshot {
    pub records: BTreeMap<String, BTreeMap<Vec<u8>, LogicalValue>>,
    /// `repo/table → (enable_indexes, buffer ttl_ms)`.
    pub table_meta: BTreeMap<String, (bool, Option<u64>)>,
    pub database_owner: Option<u64>,
    pub root_meta_owner: Option<u64>,
}

pub async fn engine_snapshot(
    root: &Path,
    db_name: &str,
    tables: &[(&str, &str)],
) -> EngineSnapshot {
    let home = ShamirDb::init(SystemStoreConfig::Fjall(root.join("shamir_db_meta.redb")))
        .await
        .unwrap();
    let owned = home.clone();
    let db_owned = db_name.to_owned();
    let tables_owned: Vec<(String, String)> = tables
        .iter()
        .map(|(repo, table)| ((*repo).to_owned(), (*table).to_owned()))
        .collect();
    let gate = MaintenanceGate::default();
    let snapshot = gate
        .with_freeze(move |guard| {
            Box::pin(async move {
                let instance = owned
                    .get_db(&db_owned)
                    .ok_or_else(|| DbError::Validation("database missing".into()))?;
                // The engine read facade refuses unopened tables (a booted
                // server opens every catalogue table); the snapshot opens
                // every requested table itself, so helpers that read
                // schema-only or freshly published targets — where nothing
                // else opens the tables — still work.
                for (repo, table) in &tables_owned {
                    instance.get_table(repo, table).await.map_err(|e| {
                        DbError::Validation(format!("cannot open table '{repo}/{table}': {e}"))
                    })?;
                }
                // Mirror the production drivers exactly: check_seed_catalogue
                // opens every system-repository table read handle (settings,
                // tables, databases, …), which the capture below reads.
                owned.check_seed_catalogue().await?;
                let reader = owned
                    .system_store()
                    .capture_logical_export(&instance, guard)
                    .await?;
                let mut records: BTreeMap<String, BTreeMap<Vec<u8>, LogicalValue>> =
                    BTreeMap::new();
                let mut table_meta = BTreeMap::new();
                let table_rows = catalogue(&reader, "tables").await?;
                for (repo, table) in &tables_owned {
                    let mut keys = BTreeMap::new();
                    let mut rows = reader.records(repo, table).await?;
                    while let Some(page) = rows.next_page().await? {
                        for (key, value) in page {
                            keys.insert(key.to_vec(), from_query_value(&value).unwrap());
                        }
                    }
                    records.insert(format!("{repo}/{table}"), keys);
                    let enable_indexes = table_rows
                        .iter()
                        .find(|(_, row)| {
                            row.get("repo_name").and_then(QueryValue::as_str) == Some(repo.as_str())
                                && row.get("table_name").and_then(QueryValue::as_str)
                                    == Some(table.as_str())
                        })
                        .and_then(|(_, row)| match row.get("enable_indexes") {
                            Some(QueryValue::Bool(value)) => Some(*value),
                            _ => None,
                        })
                        .ok_or_else(|| DbError::Validation("enable_indexes missing".into()))?;
                    let view = reader.table(repo, table)?;
                    let ttl = view.buffer_config().await?.and_then(|b| b.ttl_ms);
                    table_meta.insert(format!("{repo}/{table}"), (enable_indexes, ttl));
                }
                let mut database_owner = None;
                for (_, row) in catalogue(&reader, "databases").await? {
                    if row.get("name").and_then(QueryValue::as_str) == Some(db_owned.as_str()) {
                        database_owner = row_owner(&row);
                    }
                }
                let mut root_meta_owner = None;
                for (_, row) in catalogue(&reader, "settings").await? {
                    if row.get("key").and_then(QueryValue::as_str) == Some("root_meta") {
                        root_meta_owner = row_owner(&row);
                    }
                }
                Ok(EngineSnapshot {
                    records,
                    table_meta,
                    database_owner,
                    root_meta_owner,
                })
            })
        })
        .await
        .unwrap();
    snapshot
}

async fn catalogue(
    reader: &shamir_db::engine::db_instance::logical_export::LogicalExport<'_>,
    family: &str,
) -> Result<Vec<(Vec<u8>, QueryValue)>, DbError> {
    let mut rows = reader.catalogue_rows(family).await?;
    let mut out = Vec::new();
    while let Some(page) = rows.next_page().await? {
        for (key, row) in page {
            out.push((key.to_vec(), row));
        }
    }
    Ok(out)
}

fn row_owner(row: &QueryValue) -> Option<u64> {
    match row.get("owner") {
        Some(QueryValue::Int(id)) if *id > 0 => u64::try_from(*id).ok(),
        _ => None,
    }
}

/// Serialize a hand-crafted but fully verifiable artifact: identity "alice",
/// database "app" (optionally owned by a logical name), repository "main"
/// (Fjall, default durability), table "main/rows" and an optional record.
pub fn handcrafted_bytes(database_owner: Option<&str>, record_key: Option<Vec<u8>>) -> Vec<u8> {
    handcrafted_bytes_for_database("app", database_owner, record_key)
}

pub fn handcrafted_bytes_for_database(
    database_name: &str,
    database_owner: Option<&str>,
    record_key: Option<Vec<u8>>,
) -> Vec<u8> {
    let sections = SectionKind::ORDERED
        .into_iter()
        .map(|section| SectionInventory {
            section,
            expected_items: if section == SectionKind::End {
                Some(0)
            } else {
                None
            },
        })
        .collect();
    let header = LogicalDumpHeader {
        format_version: 1,
        required_features: Vec::new(),
        source_storage_epoch: None,
        application_schema_version: None,
        snapshot_instant: 1_700_000_000,
        repository_versions: vec![RepositoryVersion {
            repository: "main".into(),
            version: 1,
        }],
        scope: DumpScope {
            repository: None,
            table: None,
            schema_only: false,
            include_artifacts: false,
        },
        encryption: EncryptionMode::None,
        sections,
    };
    // Every `SectionKind::ORDERED` section must be written in order, the
    // empty ones included, or the reader refuses the section order.
    let items: Vec<(SectionKind, Vec<PortableItem>)> = vec![
        (
            SectionKind::Identities,
            vec![PortableItem::Identity(IdentityItem {
                logical_name: "alice".into(),
                kind: IdentityKind::User,
                disabled_on_import: true,
            })],
        ),
        (
            SectionKind::Database,
            vec![PortableItem::Database(DatabaseItem {
                name: database_name.into(),
                owner: database_owner.map(Into::into),
                group: None,
                mode: 0o644,
                settings: Vec::new(),
            })],
        ),
        (
            SectionKind::Repositories,
            vec![PortableItem::Repository(RepositoryItem {
                name: "main".into(),
                engine: RepositoryEngine::Fjall,
                durability: Durability::Default,
                owner: None,
                group: None,
                mode: 0o600,
            })],
        ),
        (
            SectionKind::Tables,
            vec![PortableItem::Table(TableItem {
                repository: "main".into(),
                name: "rows".into(),
                enable_indexes: false,
                schema_version: 0,
                schema: LogicalValue::Null,
                schema_validator: None,
                buffer_ttl_ms: None,
                owner: None,
                group: None,
                mode: 0o600,
            })],
        ),
        (SectionKind::PortableSchemas, Vec::new()),
        (SectionKind::ValidatorsAndFunctions, Vec::new()),
        (SectionKind::GuestWasmModules, Vec::new()),
        (SectionKind::Bindings, Vec::new()),
        (SectionKind::IndexDescriptors, Vec::new()),
        (SectionKind::Acls, Vec::new()),
        (
            SectionKind::Records,
            match record_key {
                Some(raw_key) => vec![PortableItem::Record(RecordItem {
                    repository: "main".into(),
                    table: "rows".into(),
                    raw_key,
                    value: LogicalValue::Map(vec![("v".into(), LogicalValue::Int(1))]),
                })],
                None => Vec::new(),
            },
        ),
        (SectionKind::End, Vec::new()),
    ];
    let mut bytes = Vec::new();
    let mut writer = DumpWriter::new(&mut bytes, header).expect("preamble writes");
    for (section, items) in items {
        writer.start_section(section).expect("section starts");
        for item in &items {
            writer.write_item(item).expect("item writes");
        }
        writer.end_section().expect("section ends");
    }
    writer.finish().expect("trailer writes");
    bytes
}

pub fn write_bytes(path: &Path, bytes: &[u8]) -> PathBuf {
    fs::write(path, bytes).unwrap();
    path.to_path_buf()
}

/// Nothing of a failed or cancelled attempt survives: the target is absent,
/// the lock file is gone and no stage directory remains beside the target.
pub fn assert_no_leftovers(root: &Path, target: &Path) {
    assert!(
        !target.exists(),
        "target {} must not exist",
        target.display()
    );
    let lock = lock_path(target);
    assert!(!lock.exists(), "lock {} must be removed", lock.display());
    assert!(
        !fs::read_dir(root).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(STAGE_PREFIX)),
        "a stage directory was left behind"
    );
}

/// The target legitimately exists (a foreign directory, or a previously
/// published import): it must be untouched on disk, with no lock file and
/// no stage directory beside it.
pub fn assert_target_untouched(root: &Path, target: &Path) {
    assert!(
        target.is_dir(),
        "target {} must still exist",
        target.display()
    );
    let lock = lock_path(target);
    assert!(!lock.exists(), "lock {} must be removed", lock.display());
    assert!(
        !fs::read_dir(root).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(STAGE_PREFIX)),
        "a stage directory was left behind"
    );
}

fn lock_path(target: &Path) -> PathBuf {
    let name = format!(
        "{}.import.lock",
        target.file_name().unwrap().to_string_lossy()
    );
    target.parent().unwrap().join(name)
}

pub struct FrameSpan {
    pub start: usize,
    pub wire_len: usize,
}

/// Walk every frame of an artifact (8-byte magic, version, flags, header
/// length, header, header digest, then frames: length + payload + checksum).
pub fn walk_frames(bytes: &[u8]) -> (usize, Vec<FrameSpan>) {
    let header_length = u32::from_be_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let frames_start = 16 + header_length + 32;
    let mut offset = frames_start;
    let mut frames = Vec::new();
    while offset < bytes.len() {
        let length = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        let wire_len = 4 + length + 32;
        frames.push(FrameSpan {
            start: offset,
            wire_len,
        });
        offset += wire_len;
    }
    (frames_start, frames)
}

/// Re-encode one frame with a replaced payload, recomputing its checksum
/// over the frame sequence and the new payload.
pub fn replace_frame_payload(
    bytes: &mut Vec<u8>,
    frame_start: usize,
    sequence: u64,
    payload: &[u8],
) {
    let old_len = 4
        + u32::from_be_bytes(bytes[frame_start..frame_start + 4].try_into().unwrap()) as usize
        + 32;
    let mut wire = Vec::with_capacity(payload.len() + 36);
    wire.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    wire.extend_from_slice(payload);
    let mut checksum = Sha256::new();
    checksum.update(sequence.to_be_bytes());
    checksum.update((payload.len() as u32).to_be_bytes());
    checksum.update(payload);
    wire.extend_from_slice(&checksum.finalize());
    bytes.splice(frame_start..frame_start + old_len, wire);
}
