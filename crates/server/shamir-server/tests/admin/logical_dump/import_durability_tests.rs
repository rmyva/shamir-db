//! Durability and batching regression tests for the offline logical import
//! record-load stage on a small streaming fixture (1,200 `bulk/many` records
//! of ~8 KiB plus 2 `bulk/large` 256 KiB blobs — the same deterministic
//! builder as the budget tests, scaled down so this runs in seconds).
//!
//! The load must stage records through per-repo transactions
//! (`begin_tx` → `set_tx` → one `commit_tx` per [`RECORDS_PER_TX` = 512]
//! records) instead of one serialized batch commit per record, and the
//! driver's single `flush_all_waiting` before the staged engine drop +
//! reopen + publish rename must remain the durability point.

use std::collections::BTreeMap;
use std::time::Duration;

use shamir_db::engine::db_instance::logical_export::MaintenanceGate;
use shamir_db::{ShamirDb, SystemStoreConfig};
use shamir_server::logical_import::{self, IMPORT_TX_COMMITS};
use shamir_sync::replication_seed::{DumpReader, PortableItem};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::logical_dump::fixture::{
    build_streaming_fixture_scaled, export_args, export_on, import_args, runtime,
};

/// Fixture build budget (mirrors the budget test's generous bound).
const BUILD_BUDGET: Duration = Duration::from_secs(300);

/// Import allowance: data budget plus the indexed-table rebuild allowance.
const IMPORT_BUDGET: Duration = Duration::from_secs(240);

/// Bounded read-back of the published target.
const READ_BUDGET: Duration = Duration::from_secs(60);

/// Record counts of the scaled-down streaming fixture (must exceed 2 × 512
/// so at least two per-repo tx commits fire during the load).
const SMALL_RECORDS: usize = 1_200;
const LARGE_RECORDS: usize = 2;

/// One record of a table, keyed by its exact 16-byte raw key: the
/// publication's logical value must equal the artifact's byte for byte.
type Records = BTreeMap<(String, Vec<u8>), shamir_sync::replication_seed::LogicalValue>;

/// Build the small fixture, export it, and import it into `root/target` on
/// `rt`. Returns the artifact path; the target is published when this
/// returns.
fn export_and_import(root: &std::path::Path) -> std::path::PathBuf {
    let rt = runtime();
    let _enter = rt.enter();
    let fixture = rt
        .block_on(timeout(
            BUILD_BUDGET,
            build_streaming_fixture_scaled(root, SMALL_RECORDS, LARGE_RECORDS),
        ))
        .unwrap();
    assert_eq!(fixture.tables.len(), 2);
    drop(fixture.home);

    let artifact = root.join("dump.mpack");
    export_on(&rt, root, &export_args("app", &artifact, false));
    let target = root.join("target");
    let imported = rt
        .block_on(timeout(
            IMPORT_BUDGET,
            logical_import::import(import_args(&artifact, &target), CancellationToken::new()),
        ))
        .unwrap()
        .unwrap();
    assert!(imported.published);
    artifact
}

/// Every record of both fixture tables from the freshly opened PUBLISHED
/// target, read through the engine read facade (MaintenanceGate freeze +
/// `capture_logical_export`, exactly what the import's validate stage uses).
fn read_published_records(rt: &tokio::runtime::Runtime, target: &std::path::Path) -> Records {
    rt.block_on(timeout(READ_BUDGET, async {
        let imported = ShamirDb::init(SystemStoreConfig::Fjall(target.join("shamir_db_meta.redb")))
            .await
            .unwrap();
        imported.check_seed_catalogue().await.unwrap();
        let owned = imported.clone();
        let gate = MaintenanceGate::default();
        let records = gate
            .with_freeze(move |guard| {
                Box::pin(async move {
                    let db = owned.get_db("app").unwrap();
                    let mut records = Records::new();
                    for (repo, table) in [("bulk", "many"), ("bulk", "large")] {
                        let reader = owned
                            .system_store()
                            .capture_logical_export(&db, guard)
                            .await
                            .unwrap();
                        let mut rows = reader.records(repo, table).await.unwrap();
                        while let Some(page) = rows.next_page().await.unwrap() {
                            for (raw_key, value) in page {
                                let logical =
                                    shamir_sync::replication_seed::from_query_value(&value)
                                        .unwrap();
                                records.insert((table.to_string(), raw_key.to_vec()), logical);
                            }
                        }
                    }
                    Ok::<Records, shamir_db::DbError>(records)
                })
                    as std::pin::Pin<
                        Box<
                            dyn std::future::Future<Output = shamir_db::DbResult<Records>>
                                + std::marker::Send
                                + '_,
                        >,
                    >
            })
            .await?;
        drop(imported);
        Ok::<Records, shamir_db::DbError>(records)
    }))
    .unwrap()
    .unwrap()
}

/// Every record of both fixture tables straight from the artifact.
fn read_artifact_records(artifact: &std::path::Path) -> Records {
    let mut records = Records::new();
    let mut reader = DumpReader::new(std::fs::File::open(artifact).unwrap()).unwrap();
    while let Some(item) = reader.next_item().unwrap() {
        let PortableItem::Record(record) = item else {
            continue;
        };
        records.insert((record.table.clone(), record.raw_key.clone()), record.value);
    }
    records
}

/// Full publication durability: after the import completes, a FRESH open of
/// the PUBLISHED directory carries EVERY record of both tables with values
/// byte-identical to the artifact (count + full-value comparison per table).
///
/// Catches an implementation that removes or weakens the final
/// `flush_all_waiting` before the drop + reopen + publish sequence (records
/// visible only in RAM/staged buffers would vanish with the loading engine),
/// and one that loses records while switching the load to tx-batched
/// commits.
#[test]
fn published_stage_is_durable_after_import() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let artifact = export_and_import(&root);
    let target = root.join("target");
    assert!(target.join("shamir_db_meta.redb").exists());

    let rt = runtime();
    let published = read_published_records(&rt, &target);
    let artifact_side = read_artifact_records(&artifact);

    let many = published
        .keys()
        .filter(|(table, _)| table == "many")
        .count();
    let large = published
        .keys()
        .filter(|(table, _)| table == "large")
        .count();
    assert_eq!(many, SMALL_RECORDS, "every bulk/many record published");
    assert_eq!(large, LARGE_RECORDS, "every bulk/large record published");
    // Full-value equality per table: the published logical values equal the
    // artifact's, keyed by the exact exported raw record keys.
    assert_eq!(published, artifact_side, "published records == artifact");
}

/// The load must commit in batches, not per record: `IMPORT_TX_COMMITS`
/// counts `commit_tx` calls issued by the record-load stage. With 1,202
/// records and `RECORDS_PER_TX = 512`, exactly 3 commits are expected.
///
/// Catches (a) a revert to per-record `table.set` (one serialized batch
/// commit per record through the store's write worker) — that path issues
/// zero `commit_tx` calls and fails the lower bound; and (b) a per-record
/// `commit_tx` — `commits == records` violates the strict upper bound, as
/// does any batching constant above 512 records.
#[test]
fn import_load_commits_in_batches_not_per_record() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    IMPORT_TX_COMMITS.store(0, std::sync::atomic::Ordering::Relaxed);
    let _artifact = export_and_import(&root);
    let total_records = (SMALL_RECORDS + LARGE_RECORDS) as u64;
    let commits = IMPORT_TX_COMMITS.load(std::sync::atomic::Ordering::Relaxed);
    assert!(commits >= 1, "the load must open and commit transactions");
    assert!(
        commits * 512 >= total_records,
        "commits={commits} cannot cover records={total_records} at ≤512/commit"
    );
    assert!(
        commits < total_records,
        "commits={commits} must stay far below records={total_records} \
         (a per-record commit_tx regression lands exactly on records)"
    );
}
