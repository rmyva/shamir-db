//! Durability and batching regression tests for the offline logical import
//! record-load stage on a small streaming fixture (1,200 `bulk/many` records
//! of ~8 KiB plus 2 `bulk/large` 256 KiB blobs — the same deterministic
//! builder as the budget tests, scaled down so this runs in seconds).
//!
//! The load must stage records through per-repo transactions
//! (`begin_tx` → `set_tx` → one `commit_tx` per [`RECORDS_PER_TX` = 512]
//! records or [`RECORD_TX_MAX_BYTES`] staged bytes) instead of one serialized
//! batch commit per record, and the driver's single `flush_all_waiting`
//! before the staged engine drop + reopen + publish rename must remain the
//! durability point.

use std::collections::BTreeMap;
use std::time::Duration;

use shamir_db::engine::db_instance::logical_export::MaintenanceGate;
use shamir_db::{ShamirDb, SystemStoreConfig};
use shamir_server::logical_import;
use shamir_sync::replication_seed::{DumpReader, PortableItem};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::logical_dump::fixture::{
    build_streaming_fixture_scaled, export_args, export_on, import_args, runtime,
};

/// Staged-bytes tx cap from `stage_load.rs` (`RECORD_TX_MAX_BYTES`, private
/// there, so hard-coded here with this comment tying the two together).
const RECORD_TX_MAX_BYTES: u64 = 32 * 1024 * 1024;

/// Fixture build budget (mirrors the budget test's generous bound).
const BUILD_BUDGET: Duration = Duration::from_secs(300);

/// Import allowance: data budget plus the indexed-table rebuild allowance
/// (the byte-bound test's fixture is ~52 MiB, comfortably inside this).
const IMPORT_BUDGET: Duration = Duration::from_secs(240);

/// Bounded read-back of the published target.
const READ_BUDGET: Duration = Duration::from_secs(60);

/// Record counts of the scaled-down streaming fixture (must exceed 2 × 512
/// so at least two per-repo tx commits fire during the load).
const SMALL_RECORDS: usize = 1_200;
const LARGE_RECORDS: usize = 2;

/// Blob-only fixture for the byte-bound flush test (200 × 256 KiB).
const BYTE_TEST_RECORDS: usize = 200;

/// One record of a table, keyed by its exact 16-byte raw key: the
/// publication's logical value must equal the artifact's byte for byte.
type Records = BTreeMap<(String, Vec<u8>), shamir_sync::replication_seed::LogicalValue>;

/// Build the small fixture, export it, and import it into `root/target` on
/// `rt`. Returns the artifact path and the import report; the target is
/// published when this returns. Runs entirely inside `rt`'s context (the
/// caller holds `rt.enter()` for the whole test — `tokio::time::timeout`
/// needs the timer driver installed by `enter`, not just `block_on`).
fn export_and_import(
    root: &std::path::Path,
    rt: &tokio::runtime::Runtime,
    small: usize,
    large: usize,
) -> (std::path::PathBuf, logical_import::ImportReport) {
    let fixture = rt
        .block_on(timeout(
            BUILD_BUDGET,
            build_streaming_fixture_scaled(root, small, large),
        ))
        .unwrap();
    drop(fixture.home);

    let artifact = root.join("dump.mpack");
    export_on(rt, root, &export_args("app", &artifact, false));
    let target = root.join("target");
    let imported = rt
        .block_on(timeout(
            IMPORT_BUDGET,
            logical_import::import(import_args(&artifact, &target), CancellationToken::new()),
        ))
        .unwrap()
        .unwrap();
    assert!(imported.published);
    (artifact, imported)
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
        // A fresh boot registers table NAMES without initializing their
        // TableManagers; the export reader refuses unopened tables
        // ("table '{name}' must be opened before logical export"). Open
        // both fixture tables explicitly, mirroring the validate stage's
        // `open_catalogue_tables`.
        let db = imported.get_db("app").unwrap();
        for (repo, table) in [("bulk", "many"), ("bulk", "large")] {
            db.get_table(repo, table).await.unwrap();
        }
        let owned = imported.clone();
        let gate = MaintenanceGate::default();
        let records = gate
            .with_freeze(move |guard| {
                Box::pin(async move {
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

/// Full publication equality: after the import completes, a FRESH open of the
/// PUBLISHED directory carries EVERY record of both tables with values
/// byte-identical to the artifact (count + full-value comparison per table).
///
/// This does NOT prove the driver's final `flush_all_waiting` is load-bearing:
/// WAL replay re-materializes committed rows on reopen even without the final
/// data flush (shamir-wal recovery/v2.rs), so a build with that flush removed
/// can still pass here. The flush remains the driver's durability point
/// (driver.rs durability invariant before the drop + reopen + publish).
///
/// Catches (a) a load that drops records while switching to tx-batched
/// commits, and (b) an unopened-table reader refusal on the reopened target
/// (TableManagers uninitialized after a fresh boot), plus the runtime-context
/// bug this file once had (`timeout` evaluated outside any `rt.enter()`).
#[test]
fn published_stage_is_durable_after_import() {
    let rt = runtime();
    let _enter = rt.enter();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let (artifact, _report) = export_and_import(&root, &rt, SMALL_RECORDS, LARGE_RECORDS);
    let target = root.join("target");
    assert!(target.join("shamir_db_meta.redb").exists());

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

/// The load must commit in count-bounded batches, not per record: the import
/// report's `tx_commits` counts the record-load stage's `commit_tx` calls.
/// With 1,202 rows (~8 KiB small rows + two 256 KiB blobs ⇒ ~10 MiB total,
/// far under `RECORD_TX_MAX_BYTES` = 32 MiB, so the count cap governs) and
/// `RECORDS_PER_TX` = 512, the split is exactly 512 + 512 + 178 = 3 commits.
///
/// Catches (a) a revert to per-record `table.set` (zero `commit_tx` calls,
/// fails the lower bound) and (b) a per-record `commit_tx` — commits ==
/// records violates the exact bound — as does any batching constant above
/// 512 records. Also pins the report's `peak_staged_bytes` (count-only
/// batching must never approach the byte cap).
#[test]
fn import_load_commits_in_batches_not_per_record() {
    let rt = runtime();
    let _enter = rt.enter();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let (_artifact, imported) = export_and_import(&root, &rt, SMALL_RECORDS, LARGE_RECORDS);
    assert_eq!(
        imported.tx_commits, 3,
        "1202 rows at ≤512/tx and ≪32 MiB must split 512+512+178 = 3 commits"
    );
    assert!(
        imported.peak_staged_bytes > 0,
        "the load must stage bytes into transactions"
    );
    assert!(
        imported.peak_staged_bytes <= RECORD_TX_MAX_BYTES,
        "peak staged bytes {} must respect the 32 MiB tx cap",
        imported.peak_staged_bytes
    );
}

/// The load must flush on BYTES, not only on the record count: with 200 ×
/// 256 KiB blobs (`build_streaming_fixture_scaled(root, 0, 200)`; fixture.rs
/// `STREAM_BLOB_BYTES = 256 * 1024` = 262144 B of payload per `bulk/large`
/// row) a count-only policy would stage all 200 rows into ONE tx, but each
/// encoded row body is ≥ 262144 B, so a 32 MiB tx holds ≤
/// 33554432 / 262144 = 128 rows ⇒ ≥ ceil(200/128) = 2 commits; and each
/// encoded body is < 300_000 B (binary payload plus a small envelope), so a
/// tx holds ≥ 33554432 / 300000 ≥ 111 rows ⇒ ≤ ceil(200/111) = 2 commits —
/// the arithmetic pins exactly 2. `peak_staged_bytes` must cross near the
/// cap (a flush decision fires when staged + body + 16 > 32 MiB with body
/// ≥ 262144 B ⇒ peak > 32 MiB − 300_000), which is impossible under a
/// count-only policy at 200 rows.
///
/// Catches a batching policy that ignores staged bytes: it would either
/// blow the 32 MiB / 64 MiB WAL-frame bound (the oversized-frame commit
/// failure this guards against) or never report a near-cap peak.
#[test]
fn import_load_flushes_on_bytes_not_only_records() {
    let rt = runtime();
    let _enter = rt.enter();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let (artifact, imported) = export_and_import(&root, &rt, 0, BYTE_TEST_RECORDS);
    assert_eq!(
        imported.tx_commits, 2,
        "200 × 256 KiB rows at the 32 MiB byte cap must split into exactly 2 commits"
    );
    assert!(
        imported.peak_staged_bytes > RECORD_TX_MAX_BYTES - 300_000,
        "peak {} must cross near the 32 MiB cap (byte-triggered flush)",
        imported.peak_staged_bytes
    );
    assert!(
        imported.peak_staged_bytes <= RECORD_TX_MAX_BYTES,
        "peak staged bytes must never exceed the 32 MiB tx cap"
    );
    // End-to-end correctness of the byte-bound path: every record published.
    let target = root.join("target");
    let published = read_published_records(&rt, &target);
    let artifact_side = read_artifact_records(&artifact);
    assert_eq!(
        published, artifact_side,
        "all {} blob records published byte-identical",
        BYTE_TEST_RECORDS
    );
}
