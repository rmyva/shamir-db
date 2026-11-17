use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{anyhow, bail, Result};
use shamir_db::engine::db_instance::logical_export::{LogicalExport, MaintenanceGate};
use shamir_db::{DbError, ShamirDb};
use shamir_sync::format_stamp::{inspect, StampState};
use shamir_sync::replication_seed::{
    preflight_artifact_size, verify_dump, DatabaseItem, DumpReport, DumpScope, DumpWriter,
    Durability, EncryptionMode, LogicalDumpHeader, LogicalValue, MaintenanceOpen, PortableItem,
    RecordItem, RepositoryEngine, RepositoryItem, RepositoryVersion, SectionInventory, SectionKind,
    SettingItem, SettingKey, TableItem,
};
use shamir_types::types::value::QueryValue;
use tokio_util::sync::CancellationToken;

use super::progress::CheckedIo;
use super::projection::{fields, record_value, resource, text, Projection};
use super::sorting::{checkpoint, next_item, Sorter};
use super::staging::Stage;

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
    if args.db == "__system__" {
        bail!("logical-export cannot export the internal system database");
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
                    write_export(
                        &home,
                        &reader,
                        &data_dir,
                        &args,
                        source_epoch,
                        &cancel,
                        deadline,
                    )
                    .await
                }
                .await;
                work.map_err(|e: anyhow::Error| DbError::Validation(format!("{e:#}")))
            })
        })
        .await?;
    Ok(result)
}

async fn write_export(
    home: &ShamirDb,
    reader: &LogicalExport<'_>,
    data_dir: &Path,
    args: &LogicalExportArgs,
    epoch: Option<u32>,
    cancel: &CancellationToken,
    deadline: std::time::Instant,
) -> Result<DumpReport> {
    let stage = Stage::new(&args.output)?;
    let mut security = Projection::new(data_dir)?;
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
                    bail!("logical-export unsupported catalogue family '{family}': reference/security closure is not implemented");
                }
                if family == "settings" {
                    let key = text(&row, "key")?;
                    let kind = match key {
                        "root_meta" => SettingKey::RootMeta,
                        "fn_namespace_meta" => SettingKey::FunctionNamespaceMeta,
                        _ => bail!("unsupported exported setting"),
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
                            bail!("logical-export supports only persisted Fjall repositories; in-memory/hybrid state is unsupported");
                        }
                        if !matches!(row.get("durability"), None | Some(QueryValue::Null)) {
                            bail!("logical-export repository durability overrides are not representable in the initial projection");
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
                                "owner",
                                "group",
                                "mode",
                            ],
                            "table (schemas/bindings/renames are unsupported)",
                        )?;
                        let view = reader.table(repo, name)?;
                        if view.regular_indexes().next().is_some()
                            || view.unique_indexes().next().is_some()
                            || !view.sorted_indexes().is_empty()
                            || view.next_index2(0).await?.is_some()
                        {
                            bail!("logical-export index descriptors are not implemented for '{repo}/{name}'");
                        }
                        if !view.bindings().is_empty() || !view.transforms().is_empty() {
                            bail!("logical-export validator/FK/transform bindings are not implemented for '{repo}/{name}'");
                        }
                        let buffer = view.buffer_config().await?;
                        if let Some(buffer) = &buffer {
                            if buffer.max_bytes != 64 * 1024 * 1024
                                || buffer.max_entries != 100_000
                                || buffer.flush_interval_ms != 500
                                || buffer.flush_batch_size != 256
                            {
                                bail!("logical-export nondefault buffer sizing/flusher configuration is not representable");
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
                    _ => bail!("unsupported catalogue family"),
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
    let mut writer = DumpWriter::new(CheckedIo::new(stage.create()?, cancel, deadline), header)?;
    for section in SectionKind::ORDERED {
        checkpoint(cancel).await?;
        writer.start_section(section)?;
        match section {
            SectionKind::Identities => {
                for item in security.identities.values() {
                    emit(&mut writer, &PortableItem::Identity(item.clone()), cancel).await?;
                }
            }
            SectionKind::Database => {
                emit(
                    &mut writer,
                    &PortableItem::Database(database.clone()),
                    cancel,
                )
                .await?
            }
            SectionKind::Repositories => {
                for item in repos.values() {
                    emit(&mut writer, &PortableItem::Repository(item.clone()), cancel).await?;
                }
            }
            SectionKind::Tables => {
                for item in tables.values() {
                    emit(&mut writer, &PortableItem::Table(item.clone()), cancel).await?;
                }
            }
            SectionKind::Acls => {
                for item in security.acls.values() {
                    emit(&mut writer, &PortableItem::Acl(item.clone()), cancel).await?;
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
                                value: record_value(&value)?,
                            });
                            sorter.push(key.to_vec(), &item, cancel).await?;
                        }
                    }
                    if let Some(mut sorted) = sorter.finish(cancel).await? {
                        while let Some(item) = next_item(&mut sorted)? {
                            emit(&mut writer, &item, cancel).await?;
                        }
                    }
                }
            }
            _ => {} // Unsupported families were explicitly refused above.
        }
        checkpoint(cancel).await?;
        writer.end_section()?;
        eprintln!("logical-export section {section:?} complete");
    }
    checkpoint(cancel).await?;
    let file = writer.finish()?.into_inner();
    file.sync_all()?;
    drop(file); // Windows: verifier must never read a writer-held file.
    checkpoint(cancel).await?;
    let input = File::open(stage.path().join("dump"))?;
    preflight_artifact_size(input.metadata()?.len())?;
    let report = verify_dump(CheckedIo::new(input, cancel, deadline))?;
    checkpoint(cancel).await?;
    // Ensure repository storage is still healthy; no listener/scheduler exists.
    home.check_seed_catalogue().await?;
    checkpoint(cancel).await?;
    stage.publish(&args.output)?;
    Ok(report)
}

async fn emit(
    writer: &mut DumpWriter<CheckedIo<File>>,
    item: &PortableItem,
    cancel: &CancellationToken,
) -> Result<()> {
    checkpoint(cancel).await?;
    writer.write_item(item)?;
    eprintln!("logical-export item frame written");
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
