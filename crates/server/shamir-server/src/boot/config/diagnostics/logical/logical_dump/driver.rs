use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use anyhow::{anyhow, bail, Error, Result};
use shamir_db::engine::db_instance::logical_export::{LogicalExport, MaintenanceGate};
use shamir_db::{DbError, ShamirDb};
use shamir_server_security::user_directory::FjallUserDirectory;
use shamir_sync::format_stamp::{inspect, StampState};
use shamir_sync::replication_seed::{
    preflight_artifact_size, verify_dump, DatabaseItem, DumpReport, DumpScope, DumpWriter,
    Durability, EncryptionMode, LogicalDumpHeader, LogicalValue, MaintenanceOpen, PortableItem,
    RecordItem, RepositoryEngine, RepositoryItem, RepositoryVersion, SectionInventory, SectionKind,
    SettingItem, SettingKey, TableItem,
};
use shamir_types::types::value::QueryValue;
use tokio_util::sync::CancellationToken;

use super::progress::{byte_counter, ByteCounter, CheckedIo};
use super::projection::{fields, record_value, rename_provenance, resource, text, Projection};
use super::sorting::{checkpoint, next_item, Sorter};
use super::staging::Stage;

/// Online observability only. The offline CLI keeps its stderr lines; a live
/// export job must never write to stderr and instead receives these events.
/// The last observed event tells the caller which phase failed: everything
/// before [`ExportProgress::Verifying`] is ordinary writing (staging,
/// catalogue projection, section emission); `Verifying` covers the verifier
/// and everything after it is post-verify (health check, publication).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExportProgress {
    ItemWritten {
        /// Frame bytes written since the previous item event (writer
        /// counter delta, including framing).
        bytes: u64,
    },
    SectionFinished(SectionKind),
    Verifying,
}

/// Observer for a live export. Implementations must be `Send` (the job runs
/// on a spawned task) and use interior mutability to record events; the
/// offline path ignores it and keeps stderr.
pub(crate) trait ExportObserver: Send + Sync {
    fn on_export_progress(&self, event: ExportProgress);
}

/// Offline stderr reporter: unchanged behavior for the CLI.
struct StderrObserver;

impl ExportObserver for StderrObserver {
    fn on_export_progress(&self, event: ExportProgress) {
        match event {
            ExportProgress::ItemWritten { .. } => eprintln!("logical-export item frame written"),
            ExportProgress::SectionFinished(section) => {
                eprintln!("logical-export section {section:?} complete");
            }
            ExportProgress::Verifying => eprintln!("logical-export verifying artifact"),
        }
    }
}

/// Where the export takes its identity projection from: the offline command
/// opens the user directory under `data_dir` itself, the live job reuses the
/// server's already-opened directory handle.
pub(crate) enum SecuritySource<'a> {
    OfflineDataDir(&'a Path),
    LiveDirectory(Option<Arc<FjallUserDirectory>>),
}

pub(crate) type PublishCallback<'a> = dyn Fn(&Path) -> Result<()> + Send + Sync + 'a;

/// Context for artifact production shared by the offline command and the
/// online export job. The caller owns the maintenance gate/snapshots and has
/// already captured `reader`; this function never takes a gate, opens the
/// data directory, or calls `check_seed_catalogue()` (that flushes live
/// state and stays offline-only, in [`export`]).
///
/// `on_publish`, when present, is the live publication protocol: a
/// synchronous, non-async callback invoked exactly once after trailer
/// verification with the staging directory path. The runner performs its
/// final cancel/deadline check inside the callback and commits under its own
/// record lock via [`super::staging::publish_live_stage`]; the commit point
/// is a single atomic no-replace rename. After the rename succeeds the
/// complete, verified artifact is irrevocably published. A subsequent Unix
/// final-directory-sync failure is logged; the artifact is retained and
/// publication is not retried.
/// `None` keeps the default offline publication (`publish_stage(stage,
/// &args.output)` right after a final checkpoint). `live` selects the live
/// `.shamir-export-` stage prefix that export-root cleanup recognizes;
/// offline keeps `.logical-export-`.
pub(crate) struct LiveExport<'a> {
    pub reader: &'a LogicalExport<'a>,
    pub security: SecuritySource<'a>,
    pub args: &'a LogicalExportArgs,
    pub epoch: Option<u32>,
    pub live: bool,
    pub cancel: &'a CancellationToken,
    pub deadline: std::time::Instant,
    pub observer: &'a dyn ExportObserver,
    pub on_publish: Option<&'a PublishCallback<'a>>,
    /// Offline only: the open handle the caller retains so the
    /// pre-publication catalogue health check can run inside the freeze. The
    /// live job passes `None` and performs no background health probe.
    pub offline_home: Option<&'a ShamirDb>,
}

/// Coarse, static failure categories for the online export job.
/// Classification is structural only: the cancellation token, the deadline
/// clock, and typed markers carried in the anyhow chain — never by matching
/// formatted error text. No variant carries a filesystem path, a secret, or
/// peer-supplied text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LiveExportFailure {
    /// The caller's cancellation token fired.
    Cancelled,
    /// The caller's deadline has passed.
    DeadlineExceeded,
    /// Creating the attempt-owned staging directory failed.
    StageIo,
    /// Coarse catch-all for failures during artifact production: writer
    /// frames, spill sorter, engine read cursors.
    WriterIo,
    /// Verifying the staged artifact (size preflight or verifier) failed.
    VerifierIo,
    /// An explicit projection/refusal abort: unsupported catalogue family,
    /// feature, scope, or record value. The artifact was never published.
    UnsupportedProjection,
}

/// Structural marker for an explicit projection refusal, tagged at each
/// refuse site. Never matched by formatted text.
#[derive(Debug)]
struct UnsupportedProjectionTag;

impl Display for UnsupportedProjectionTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("logical-export unsupported projection")
    }
}

impl std::error::Error for UnsupportedProjectionTag {}

/// Refuse with the exact offline message plus a typed marker in the chain.
fn unsupported(message: impl Display) -> Error {
    Error::new(UnsupportedProjectionTag).context(message.to_string())
}

/// Structural marker for the phase an I/O failure occurred in.
#[derive(Clone, Copy, Debug)]
struct PhaseTag(&'static str);

const PHASE_STAGE: &str = "logical-export staging";
const PHASE_VERIFY: &str = "logical-export verification";

impl Display for PhaseTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for PhaseTag {}

fn tagged<T>(result: Result<T>, phase: &'static str) -> Result<T> {
    result.map_err(|error| error.context(PhaseTag(phase)))
}

/// Classify a caught failure into a coarse static category for the wire.
/// Cancel/deadline dominate; the rest is decided by typed markers only.
pub(crate) fn classify_export_failure(
    error: Error,
    cancel: &CancellationToken,
    deadline: std::time::Instant,
) -> LiveExportFailure {
    if cancel.is_cancelled() {
        return LiveExportFailure::Cancelled;
    }
    if std::time::Instant::now() >= deadline {
        return LiveExportFailure::DeadlineExceeded;
    }
    if error.downcast_ref::<UnsupportedProjectionTag>().is_some() {
        return LiveExportFailure::UnsupportedProjection;
    }
    match error.downcast_ref::<PhaseTag>() {
        Some(PhaseTag(PHASE_STAGE)) => LiveExportFailure::StageIo,
        Some(PhaseTag(PHASE_VERIFY)) => LiveExportFailure::VerifierIo,
        _ => LiveExportFailure::WriterIo,
    }
}

/// Initial lossless subset: Fjall repositories with default durability, plain
/// unbound/index-free tables, default buffer sizing plus TTL, and unambiguous
/// scalar/map records. Unsupported definitions or values abort, never omit.
#[derive(Clone, Debug, clap::Args)]
pub struct LogicalExportArgs {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long)]
    pub repo: Option<String>,
    #[arg(long, requires = "repo")]
    pub table: Option<String>,
    #[arg(long)]
    pub schema_only: bool,
    /// Reserved guest-artifact scope; explicitly refused in this first subset.
    #[arg(long)]
    pub include_artifacts: bool,
}

/// Offline only; does not construct a server or spawn an export worker.
/// cancel-safe: NO during MaintenanceOpen recovery — caller must retain and
/// join this future (the CLI does), signalling cancellation with the token.
/// After recovery, attempt-owned staging and maintenance admission are RAII;
/// publication is synchronous and has no cancellation point after it.
pub async fn export(
    data_dir: &Path,
    args: &LogicalExportArgs,
    cancel: CancellationToken,
) -> Result<DumpReport> {
    if args.table.is_some() && args.repo.is_none() {
        bail!("--table requires --repo");
    }
    if args.include_artifacts {
        bail!("--include-artifacts is unsupported: guest functions/modules are not implemented");
    }
    if shamir_db::is_reserved_db_name(&args.db) {
        return Err(anyhow::Error::from(DbError::Validation(format!(
            "database name '{}' is reserved by the server",
            args.db
        ))));
    }
    checkpoint(&cancel).await?;
    // Keep ownership through verification and publication. Opening/recovery is
    // deliberately not raced with ctrl-c: detached recovery must not lose lock.
    let opened = MaintenanceOpen::open(data_dir).await?;
    let source_epoch = match inspect(data_dir) {
        StampState::Valid(stamp) if stamp.created_by().adopted != Some(true) => {
            Some(u32::try_from(stamp.format_epoch())?)
        }
        StampState::Valid(_) | StampState::Absent { .. } => None,
        _ => bail!("logical-export source stamp became unreadable"),
    };
    let db = opened
        .db
        .get_db(&args.db)
        .ok_or_else(|| anyhow!("logical-export database not found"))?;
    // All cold initialization/repair is before the freeze. init_seed already
    // bounded the complete catalogue; this also opens its table read handles.
    opened.db.check_seed_catalogue().await?;
    for name in db.list_repos() {
        if opened.db.read_quarantine_reason(&args.db, &name).is_some() {
            bail!("logical-export repository '{name}' is quarantined (resync_required)");
        }
        let repo = db
            .get_repo(&name)
            .ok_or_else(|| anyhow!("repository disappeared"))?;
        if repo.has_rename_in_doubt() {
            bail!("logical-export repository has table rename in doubt");
        }
        for name in repo.list_table_names() {
            repo.get_table(&name).await?;
        }
    }
    let data_dir = data_dir.to_owned();
    let args = args.clone();
    let home = opened.db.clone();
    let deadline = CheckedIo::<File>::deadline();
    let gate = MaintenanceGate::default();
    let result = gate
        .with_freeze(move |guard| {
            Box::pin(async move {
                let work = async {
                    let reader = home
                        .system_store()
                        .capture_logical_export(&db, guard)
                        .await?;
                    let observer = StderrObserver;
                    // Offline only: the catalogue health probe runs inside
                    // write_live_export, after verification and before the
                    // final checkpoint/publication.
                    write_live_export(LiveExport {
                        reader: &reader,
                        security: SecuritySource::OfflineDataDir(&data_dir),
                        args: &args,
                        epoch: source_epoch,
                        live: false,
                        cancel: &cancel,
                        deadline,
                        observer: &observer,
                        on_publish: None,
                        offline_home: Some(&home),
                    })
                    .await
                }
                .await;
                work.map_err(|e: anyhow::Error| DbError::Validation(format!("{e:#}")))
            })
        })
        .await?;
    Ok(result)
}

/// Shared artifact production. cancel-safe per the offline contract; on
/// failure the caller classifies with [`classify_export_failure`], which
/// decides structurally from the token, the clock, and typed markers.
/// Publication stays synchronous and never replaces an existing artifact.
pub(crate) async fn write_live_export(ctx: LiveExport<'_>) -> Result<DumpReport> {
    let LiveExport {
        reader,
        security,
        args,
        epoch,
        live,
        cancel,
        deadline,
        observer,
        on_publish,
        offline_home,
    } = ctx;
    if live {
        checkpoint(cancel).await?;
        if reader.has_schema_migration_history().await? {
            return Err(unsupported(
                "logical-export cannot preserve schema migration history; a nonempty system journal blocks live export",
            ));
        }
    }
    let mut security = match security {
        SecuritySource::OfflineDataDir(data_dir) => Projection::new(data_dir)?,
        SecuritySource::LiveDirectory(directory) => Projection::from_directory(directory),
    };
    let stage = tagged(
        if live {
            Stage::for_live(&args.output)
        } else {
            Stage::new(&args.output)
        },
        PHASE_STAGE,
    )?;
    let mut repos = BTreeMap::new();
    let mut tables = BTreeMap::new();
    let mut database = None;
    let mut settings = BTreeMap::new();
    let mut metadata_bytes = 0;
    // Stream system rows through closed allowlists. Refuse unsupported global
    // definitions conservatively: this cut does not yet compute their closure.
    for family in [
        "databases",
        "repositories",
        "tables",
        "settings",
        "functions",
        "validators",
        "function_folders",
        "groups",
    ] {
        let mut rows = reader.catalogue_rows(family).await?;
        while let Some(page) = rows.next_page().await? {
            checkpoint(cancel).await?;
            for (_, row) in page {
                if matches!(
                    family,
                    "functions" | "validators" | "function_folders" | "groups"
                ) {
                    return Err(unsupported(format!(
                        "logical-export unsupported catalogue family '{family}': reference/security closure is not implemented"
                    )));
                }
                if family == "settings" {
                    let key = text(&row, "key")?;
                    let kind = match key {
                        "root_meta" => SettingKey::RootMeta,
                        "fn_namespace_meta" if live => {
                            return Err(unsupported(
                                "logical-export cannot restore persisted fn_namespace_meta settings",
                            ));
                        }
                        "fn_namespace_meta" => SettingKey::FunctionNamespaceMeta,
                        _ => return Err(unsupported("unsupported exported setting")),
                    };
                    fields(&row, &["key", "value"], "setting")?;
                    let value = row
                        .get("value")
                        .ok_or_else(|| anyhow!("missing setting value"))?;
                    fields(value, &["owner", "group", "mode"], "setting ACL")?;
                    let (owner, group, mode) =
                        security.meta(value, resource(&args.db, None, None, Some(key)))?;
                    // Logical names replace numeric identity IDs even inside settings.
                    let value = LogicalValue::Map(vec![
                        (
                            "owner".into(),
                            owner.map(LogicalValue::Str).unwrap_or(LogicalValue::Null),
                        ),
                        (
                            "group".into(),
                            group.map(LogicalValue::Str).unwrap_or(LogicalValue::Null),
                        ),
                        ("mode".into(), LogicalValue::Int(i64::from(mode))),
                    ]);
                    if settings
                        .insert(key.to_owned(), SettingItem { key: kind, value })
                        .is_some()
                    {
                        bail!("duplicate setting");
                    }
                    continue;
                }
                let db_field = if family == "databases" {
                    "name"
                } else {
                    "db_name"
                };
                if text(&row, db_field)? != args.db {
                    continue;
                }
                match family {
                    "databases" => {
                        // `identity` is the node-local database identity; it is
                        // not exported, the importer assigns a new one.
                        fields(
                            &row,
                            &["name", "created_at", "identity", "owner", "group", "mode"],
                            "database",
                        )?;
                        let (owner, group, mode) =
                            security.meta(&row, resource(&args.db, None, None, None))?;
                        if database
                            .replace(DatabaseItem {
                                name: args.db.clone(),
                                owner,
                                group,
                                mode,
                                settings: vec![],
                            })
                            .is_some()
                        {
                            bail!("duplicate database");
                        }
                    }
                    "repositories" => {
                        let name = text(&row, "repo_name")?;
                        if args.repo.as_ref().is_some_and(|r| r != name) {
                            continue;
                        }
                        fields(
                            &row,
                            &[
                                "db_name",
                                "repo_name",
                                "engine",
                                "path",
                                "durability",
                                "owner",
                                "group",
                                "mode",
                            ],
                            "repository",
                        )?;
                        if !matches!(text(&row, "engine")?, "fjall" | "fjall_raw") {
                            return Err(unsupported(
                                "logical-export supports only persisted Fjall repositories; in-memory/hybrid state is unsupported",
                            ));
                        }
                        if !matches!(row.get("durability"), None | Some(QueryValue::Null)) {
                            return Err(unsupported(
                                "logical-export repository durability overrides are not representable in the initial projection",
                            ));
                        }
                        let (owner, group, mode) =
                            security.meta(&row, resource(&args.db, Some(name), None, None))?;
                        let item = RepositoryItem {
                            name: name.into(),
                            engine: RepositoryEngine::Fjall,
                            durability: Durability::Default,
                            owner,
                            group,
                            mode,
                        };
                        charge(&PortableItem::Repository(item.clone()), &mut metadata_bytes)?;
                        if repos.insert(name.to_owned(), item).is_some() {
                            bail!("duplicate repository");
                        }
                    }
                    "tables" => {
                        let repo = text(&row, "repo_name")?;
                        let name = text(&row, "table_name")?;
                        if args.repo.as_ref().is_some_and(|r| r != repo)
                            || args.table.as_ref().is_some_and(|t| t != name)
                        {
                            continue;
                        }
                        fields(
                            &row,
                            &[
                                "db_name",
                                "repo_name",
                                "table_name",
                                "enable_indexes",
                                "store_id",
                                "rename_from",
                                "rename_from_store_id",
                                "owner",
                                "group",
                                "mode",
                            ],
                            "table (schemas/bindings are unsupported)",
                        )?;
                        // Validated physical provenance of a successfully
                        // renamed table: accepted as input, never projected
                        // (the portable item names the table as it is now).
                        rename_provenance(&row)?;
                        let view = reader.table(repo, name)?;
                        if view.regular_indexes().next().is_some()
                            || view.unique_indexes().next().is_some()
                            || !view.sorted_indexes().is_empty()
                            || view.next_index2(0).await?.is_some()
                        {
                            return Err(unsupported(format!(
                                "logical-export index descriptors are not implemented for '{repo}/{name}'"
                            )));
                        }
                        if !view.bindings().is_empty() || !view.transforms().is_empty() {
                            return Err(unsupported(format!(
                                "logical-export validator/FK/transform bindings are not implemented for '{repo}/{name}'"
                            )));
                        }
                        let buffer = view.buffer_config().await?;
                        if let Some(buffer) = &buffer {
                            if buffer.max_bytes != 64 * 1024 * 1024
                                || buffer.max_entries != 100_000
                                || buffer.flush_interval_ms != 500
                                || buffer.flush_batch_size != 256
                            {
                                return Err(unsupported(
                                    "logical-export nondefault buffer sizing/flusher configuration is not representable",
                                ));
                            }
                        }
                        let enable_indexes = match row.get("enable_indexes") {
                            Some(QueryValue::Bool(v)) => *v,
                            _ => bail!("invalid enable_indexes catalogue field"),
                        };
                        let (owner, group, mode) = security
                            .meta(&row, resource(&args.db, Some(repo), Some(name), None))?;
                        let item = TableItem {
                            repository: repo.into(),
                            name: name.into(),
                            enable_indexes,
                            schema_version: 0,
                            schema: LogicalValue::Null,
                            schema_validator: None,
                            buffer_ttl_ms: buffer.and_then(|b| b.ttl_ms),
                            owner,
                            group,
                            mode,
                        };
                        charge(&PortableItem::Table(item.clone()), &mut metadata_bytes)?;
                        if tables
                            .insert((repo.to_owned(), name.to_owned()), item)
                            .is_some()
                        {
                            bail!("duplicate table");
                        }
                    }
                    _ => return Err(unsupported("unsupported catalogue family")),
                }
            }
        }
    }
    let mut database = database.ok_or_else(|| anyhow!("database catalogue row missing"))?;
    database.settings = settings.into_values().collect();
    charge(
        &PortableItem::Database(database.clone()),
        &mut metadata_bytes,
    )?;
    if args.repo.as_ref().is_some_and(|r| !repos.contains_key(r)) {
        bail!("logical-export selected repository not found");
    }
    if args.table.is_some() && tables.is_empty() {
        bail!("logical-export selected table not found");
    }
    let versions: Vec<_> = reader
        .repository_versions()
        .filter(|(r, _)| args.repo.as_ref().is_none_or(|selected| selected == r))
        .map(|(r, v)| RepositoryVersion {
            repository: r.into(),
            version: v,
        })
        .collect();
    if versions.len() != repos.len() || versions.iter().any(|v| !repos.contains_key(&v.repository))
    {
        bail!("logical-export catalogue/engine repository inventory mismatch");
    }
    for repo in repos.keys() {
        let actual: BTreeSet<_> = reader
            .table_names(repo)?
            .into_iter()
            .filter(|t| args.table.as_ref().is_none_or(|selected| selected == t))
            .collect();
        let expected: BTreeSet<_> = tables
            .keys()
            .filter(|(r, _)| r == repo)
            .map(|(_, t)| t.clone())
            .collect();
        if actual != expected {
            bail!("logical-export catalogue/engine table inventory mismatch");
        }
    }
    if tables.keys().any(|(r, _)| !repos.contains_key(r)) {
        bail!("logical-export table has no selected repository");
    }
    // Avoid accepting a blank stub: an empty database is legitimate only after
    // its actual catalogue and engine inventory agreed above.
    let header = LogicalDumpHeader {
        format_version: 1,
        required_features: vec![],
        source_storage_epoch: epoch,
        application_schema_version: None,
        snapshot_instant: u64::try_from(reader.instant().duration_since(UNIX_EPOCH)?.as_millis())?,
        repository_versions: versions,
        scope: DumpScope {
            repository: args.repo.clone(),
            table: args.table.clone(),
            schema_only: args.schema_only,
            include_artifacts: false,
        },
        encryption: EncryptionMode::None,
        sections: SectionKind::ORDERED
            .into_iter()
            .map(|section| SectionInventory {
                section,
                expected_items: if section == SectionKind::End {
                    Some(0)
                } else {
                    None
                },
            })
            .collect(),
    };
    checkpoint(cancel).await?;
    let counter = byte_counter();
    let mut writer = DumpWriter::new(
        CheckedIo::with_counter(stage.create()?, cancel, deadline, counter.clone()),
        header,
    )?;
    for section in SectionKind::ORDERED {
        checkpoint(cancel).await?;
        writer.start_section(section)?;
        match section {
            SectionKind::Identities => {
                for item in security.identities.values() {
                    emit(
                        &mut writer,
                        &counter,
                        observer,
                        &PortableItem::Identity(item.clone()),
                        cancel,
                    )
                    .await?;
                }
            }
            SectionKind::Database => {
                emit(
                    &mut writer,
                    &counter,
                    observer,
                    &PortableItem::Database(database.clone()),
                    cancel,
                )
                .await?
            }
            SectionKind::Repositories => {
                for item in repos.values() {
                    emit(
                        &mut writer,
                        &counter,
                        observer,
                        &PortableItem::Repository(item.clone()),
                        cancel,
                    )
                    .await?;
                }
            }
            SectionKind::Tables => {
                for item in tables.values() {
                    emit(
                        &mut writer,
                        &counter,
                        observer,
                        &PortableItem::Table(item.clone()),
                        cancel,
                    )
                    .await?;
                }
            }
            SectionKind::Acls => {
                for item in security.acls.values() {
                    emit(
                        &mut writer,
                        &counter,
                        observer,
                        &PortableItem::Acl(item.clone()),
                        cancel,
                    )
                    .await?;
                }
            }
            SectionKind::Records if !args.schema_only => {
                for (repo, table) in tables.keys() {
                    let sort_dir = tempfile::Builder::new()
                        .prefix("sort-")
                        .tempdir_in(stage.path())?;
                    let mut sorter = Sorter::new(sort_dir.path());
                    let mut rows = reader.records(repo, table).await?;
                    while let Some(page) = rows.next_page().await? {
                        checkpoint(cancel).await?;
                        for (key, value) in page {
                            let item = PortableItem::Record(RecordItem {
                                repository: repo.clone(),
                                table: table.clone(),
                                raw_key: key.to_vec(),
                                value: record_value(&value)
                                    .map_err(|error| error.context(UnsupportedProjectionTag))?,
                            });
                            tagged(
                                sorter.push(key.to_vec(), &item, cancel).await,
                                "logical-export record spill",
                            )?
                        }
                    }
                    if let Some(mut sorted) =
                        tagged(sorter.finish(cancel).await, "logical-export record spill")?
                    {
                        while let Some(item) =
                            tagged(next_item(&mut sorted), "logical-export record spill")?
                        {
                            emit(&mut writer, &counter, observer, &item, cancel).await?;
                        }
                    }
                }
            }
            _ => {} // Unsupported families were explicitly refused above.
        }
        checkpoint(cancel).await?;
        writer.end_section()?;
        observer.on_export_progress(ExportProgress::SectionFinished(section));
    }
    checkpoint(cancel).await?;
    let (file, _total_bytes) = writer.finish()?.into_parts();
    file.sync_all()?;
    drop(file); // Windows: verifier must never read a writer-held file.
    checkpoint(cancel).await?;
    observer.on_export_progress(ExportProgress::Verifying);
    let report = tagged(
        (|| {
            let input = File::open(stage.path().join("dump"))?;
            preflight_artifact_size(input.metadata()?.len())?;
            Ok(verify_dump(CheckedIo::new(input, cancel, deadline))?)
        })(),
        PHASE_VERIFY,
    )?;
    checkpoint(cancel).await?;
    // Offline only (the live job performs no background health probe inside
    // the freeze): run the catalogue health check after verification but
    // before the final checkpoint and publication, so a verified artifact is
    // published only by a healthy source.
    if let Some(home) = offline_home {
        home.check_seed_catalogue()
            .await
            .map_err(|e: DbError| anyhow!("{e}"))?;
    }
    // Final checkpoint before publication; publication itself is synchronous
    // with no async point after it. Once the no-clobber link commits, no
    // failure is reported for the completed artifact: the only possible error
    // is the explicit "published, retained" directory-sync durability error.
    checkpoint(cancel).await?;
    match on_publish {
        Some(publish) => publish(stage.path())?,
        None => stage.publish(&args.output)?,
    }
    Ok(report)
}

async fn emit(
    writer: &mut DumpWriter<CheckedIo<File>>,
    counter: &ByteCounter,
    observer: &dyn ExportObserver,
    item: &PortableItem,
    cancel: &CancellationToken,
) -> Result<()> {
    checkpoint(cancel).await?;
    let before = counter.load(Ordering::Relaxed);
    writer.write_item(item)?;
    let bytes = counter.load(Ordering::Relaxed).saturating_sub(before);
    observer.on_export_progress(ExportProgress::ItemWritten { bytes });
    Ok(())
}

fn charge(item: &PortableItem, total: &mut usize) -> Result<()> {
    *total = total
        .checked_add(shamir_sync::replication_seed::encode_canonical(item)?.len())
        .ok_or_else(|| anyhow!("metadata size overflow"))?;
    if *total > 8 * 1024 * 1024 {
        bail!("logical-export scoped metadata exceeds 8 MiB");
    }
    Ok(())
}
