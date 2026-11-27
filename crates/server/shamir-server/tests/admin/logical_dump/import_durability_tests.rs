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
use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::Duration;

use shamir_connect::common::crypto::StoredKey;
use shamir_connect::common::kdf_params::KdfParams;
use shamir_connect::server::user_record::UserRecord;
use shamir_db::access::{principal64, Actor};
use shamir_db::engine::db_instance::logical_export::MaintenanceGate;
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::{ShamirDb, SystemStoreConfig};
use shamir_query_builder::batch::Batch;
use shamir_query_builder::write;
use shamir_server::logical_import;
use shamir_server::logical_import::COMMIT_BOUNDARY;
use shamir_server_security::user_directory::FjallUserDirectory;
use shamir_sync::replication_seed::{DumpReader, PortableItem};
use shamir_types::types::common::new_map;
use shamir_types::types::value::QueryValue;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::logical_dump::fixture;
use crate::logical_dump::fixture::{
    build_streaming_fixture_scaled, export_args, export_on, import_args, runtime, system_store_path,
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
/// `tables` lists the (repo, table) pairs to open and read.
fn read_published_records(
    rt: &tokio::runtime::Runtime,
    target: &std::path::Path,
    tables: &[(&str, &str)],
) -> Records {
    rt.block_on(timeout(READ_BUDGET, async {
        let imported = ShamirDb::init(SystemStoreConfig::Fjall(target.join("shamir_db_meta.redb")))
            .await
            .unwrap();
        imported.check_seed_catalogue().await.unwrap();
        // A fresh boot registers table NAMES without initializing their
        // TableManagers; the export reader refuses unopened tables
        // ("table '{name}' must be opened before logical export"). Open
        // every fixture table explicitly, mirroring the validate stage's
        // `open_catalogue_tables`.
        let db = imported.get_db("app").unwrap();
        for (repo, table) in tables {
            db.get_table(repo, table).await.unwrap();
        }
        let owned = imported.clone();
        let gate = MaintenanceGate::default();
        let tables_owned: Vec<(String, String)> = tables
            .iter()
            .map(|(r, t)| (r.to_string(), t.to_string()))
            .collect();
        let records = gate
            .with_freeze(move |guard| {
                Box::pin(async move {
                    let mut records = Records::new();
                    for (repo, table) in &tables_owned {
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
                                records.insert((table.clone(), raw_key.to_vec()), logical);
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

    let published = read_published_records(&rt, &target, &[("bulk", "many"), ("bulk", "large")]);
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
    // With the round-3 delta term in the admission rule this split is
    // UNCHANGED: the delta is four interned field names ("id", "seed",
    // "pad", "tag" — each ≤ 4 chars ⇒ delta_bytes ≈ 4×(32+4) = 144 B,
    // reset by the row-500 interner persist), so
    // staged(≈10 MiB) + delta ≪ usable = 32 MiB − 256; the count cap
    // still governs every admission: 512/512/178 = 3 commits.
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
    let published = read_published_records(&rt, &target, &[("bulk", "large")]);
    let artifact_side = read_artifact_records(&artifact);
    assert_eq!(
        published, artifact_side,
        "all {} blob records published byte-identical",
        BYTE_TEST_RECORDS
    );
}

// ---------------------------------------------------------------------------
// Round-3: complete-entry admission (interner delta counted), count-cap
// commits, and the commit-boundary cancellation seam.
// ---------------------------------------------------------------------------

/// Distinct interner name bytes per custom-fixture row (`bulk/fields`).
const NAME_BYTES: usize = 1024 * 1024;

/// String body bytes per custom-fixture row.
const BODY_BYTES: usize = 512 * 1024;

/// Ops per Batch (mirrors fixture.rs `STREAM_BATCH_OPS` ≤ the 50-query cap).
const CUSTOM_BATCH_OPS: usize = 25;

/// Absolute deadline for the commit-boundary handshake waits (no sleeps).
const BOUNDARY_DEADLINE: Duration = Duration::from_secs(240);

/// Build a custom fixture (does NOT touch fixture.rs): db `app`, repo
/// `bulk` (fjall_raw), table `fields` (enable_indexes = false), then
/// `rows` inserts where row i carries ONE distinct ~1 MiB field name and
/// a ~512 KiB string value, in batches of [`CUSTOM_BATCH_OPS`] ops.
async fn build_field_fixture(root: &Path, rows: usize) {
    let owner = create_owner_user(root).await;
    let home = ShamirDb::init(SystemStoreConfig::Fjall(system_store_path(root)))
        .await
        .unwrap();
    home.check_seed_catalogue().await.unwrap();
    home.create_db_as(fixture::DB_NAME, Actor::User(owner))
        .await
        .unwrap();
    home.add_repo_as_with_durability(
        fixture::DB_NAME,
        RepoConfig::new("bulk", BoxRepoFactory::fjall_raw(root.join("data_bulk"))),
        Actor::User(owner),
        None,
    )
    .await
    .unwrap();
    home.add_table_as(
        fixture::DB_NAME,
        "bulk",
        "fields",
        false,
        Actor::User(owner),
    )
    .await
    .unwrap();
    let instance = home.get_db(fixture::DB_NAME).unwrap();
    instance.get_table("bulk", "fields").await.unwrap();
    let body = "y".repeat(BODY_BYTES);
    for start in (0..rows).step_by(CUSTOM_BATCH_OPS) {
        let mut batch = Batch::new();
        for i in start..(start + CUSTOM_BATCH_OPS).min(rows) {
            // Distinct name, exactly NAME_BYTES chars per row.
            let name = format!("f{i:020}_{}", "n".repeat(NAME_BYTES - 22));
            assert_eq!(name.chars().count(), NAME_BYTES);
            let mut row = new_map();
            row.insert(name, QueryValue::Str(body.clone()));
            batch.op(
                format!("put_field_{i:06}"),
                write::Insert::with_repo("bulk", "fields").row(QueryValue::Map(row)),
            );
        }
        home.execute(fixture::DB_NAME, &batch.build())
            .await
            .unwrap();
        home.flush_all_waiting().await.unwrap();
    }
}

/// Smallest mirror of fixture.rs's private `create_owner_user`: one
/// enabled user "owner" in a `users/` directory; returns the principal64
/// catalogue id (the fixture helper is private to its module, so it is
/// mirrored here rather than widened).
async fn create_owner_user(root: &Path) -> u64 {
    let directory = FjallUserDirectory::open(root.join(fixture::USER_DIRECTORY_DIR)).unwrap();
    let user_record = UserRecord {
        salt: [7; 16],
        stored_key: StoredKey([8; 32]),
        server_key: Zeroizing::new([9; 32]),
        kdf_params: KdfParams::DEFAULT,
        tickets_invalid_before_ns: 0,
    };
    let user_id = directory
        .insert_with_scope("owner".to_string(), user_record, None)
        .unwrap();
    principal64(user_id)
}

/// Build the given fixture shape, export it, and return the artifact path
/// (the builder engine is dropped before the export, as everywhere else).
fn build_and_export(
    root: &std::path::Path,
    rt: &tokio::runtime::Runtime,
    small: usize,
    large: usize,
) -> std::path::PathBuf {
    let fixture = rt
        .block_on(timeout(
            BUILD_BUDGET,
            fixture::build_streaming_fixture_scaled(root, small, large),
        ))
        .unwrap();
    drop(fixture.home);
    let artifact = root.join("dump.mpack");
    fixture::export_on(rt, root, &fixture::export_args("app", &artifact, false));
    artifact
}

/// The round-3 P0 regression: a huge per-row interner delta must be counted
/// at admission and persisted early, so no commit ever carries a staged +
/// delta entry bound over the 32 MiB tx cap / 64 MiB WAL frame.
///
/// Shape: 60 rows, each ONE distinct 1 MiB name + a 512 KiB string body
/// (`bulk/fields`, no indexes). Exact arithmetic with the real constants
/// (`usable = 32 MiB − 256 = 33 554 176`; `D = delta bytes per new name =
/// 32 + 1 048 576 = 1 048 608`; per-row staged `s = 524 288 + e + 16` with
/// `e` the small interned-encoding envelope, `0 ≤ e < 29 652` proven below;
/// row bound `rb = 128 + 16 + s`; the delta-persist trigger fires when the
/// pending delta exceeds 8 MiB = 8 388 608, i.e. at 8 pending names since
/// `8 × D = 8 388 864 > 8 388 608`):
///
/// ```text
/// row k  staged(before)      delta(admission)  next = staged+delta+rb  decision
/// 1      0                   1D                1.57 MB                 Stage
/// 2..8   (k−1)s              kD                ≤ 12.6 MB               Stage
/// 9      8s                  0 (trigger persist: 8D > 8 MiB)           Stage
/// 10..16 (k−1)s              (k−9)D            ≤ 16.8 MB               Stage
/// 17,25,33,41,49: trigger persists (delta reaches 8D each block); delta 0
/// 18..47 (k−1)s              (k−p)D ≤ 7D       ≤ 32.5 MB               Stage
///        (worst case row 48: 48s + 7D = 32 506 848 + 48e < usable)
/// 50..53 (k−1)s              (k−49)D           ≤ 31.98 MB              Stage
///        (row 53: 53s + 4D = 31 982 464 + 53e < usable by ≥ 1.57 MB)
/// 54     53s ≈ 27.78 MB      5D                33 555 520 + 54e        CommitFirst
///        (usable exceeded by 1 344 + 54e > 0 for ANY e ≥ 0 — exact)
/// ```
///
/// Commit #1 fires at row 54 with 53 staged rows + delta 5D ≈ 33.0 MB
/// entry bound < 64 MiB frame. Rows 54..60 then stage (row 57's block hits
/// another trigger persist; every next ≤ 6.8 MB); the tail commits them.
/// ⇒ EXACTLY 2 commits. The old meter (staged-only, delta invisible) never
/// crosses 32 MiB staged (60s ≈ 31.46 MB < 32 MiB) ⇒ ONE commit carrying
/// ~31.5 MB staged + ~60 × 1 MiB ≈ 63 MB interner delta ≈ 94 MB > the
/// 64 MiB frame ⇒ "WAL outcome indeterminate" — this test fails it.
///
/// Catches (a) a meter that ignores `delta_bytes` at admission and (b) a
/// meter that never persists the interner mid-stream.
#[test]
fn import_load_splits_large_interner_delta() {
    let rt = runtime();
    let _enter = rt.enter();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    rt.block_on(timeout(BUILD_BUDGET, build_field_fixture(&root, 60)))
        .unwrap();
    // The builder engine is local to `build_field_fixture` and is dropped
    // on return, before the export reopens the directories.
    let artifact = root.join("dump.mpack");
    fixture::export_on(&rt, &root, &fixture::export_args("app", &artifact, false));
    let target = root.join("target");
    let imported = rt
        .block_on(timeout(
            IMPORT_BUDGET,
            logical_import::import(
                fixture::import_args(&artifact, &target),
                CancellationToken::new(),
            ),
        ))
        .unwrap()
        .unwrap();
    assert_eq!(
        imported.tx_commits, 2,
        "60 × (1 MiB name + 512 KiB body) rows must split 53 + 7 = 2 commits"
    );
    assert!(
        imported.peak_staged_bytes <= RECORD_TX_MAX_BYTES,
        "peak staged bytes must respect the 32 MiB tx cap"
    );
    let published = read_published_records(&rt, &target, &[("bulk", "fields")]);
    let artifact_side = read_artifact_records(&artifact);
    assert_eq!(
        published, artifact_side,
        "all 60 huge-name rows published byte-identical"
    );
}

/// The load must commit exactly once at the count cap: 512 ~8 KiB rows
/// (`build_streaming_fixture_scaled(root, 512, 0)`) stage
/// 512 × (8 192 + e + 16) ≈ 4.4 MB ≪ usable (the delta term is 4 interned
/// names ≈ 144 B, reset at the row-500 interner persist), so every
/// admission stages by bytes and only the tail commit fires: 1 commit.
///
/// Catches a meter that commits early (e.g. per-record or on the delta
/// term): any extra commit breaks the exact bound.
#[test]
fn import_load_commits_once_at_the_count_cap() {
    let rt = runtime();
    let _enter = rt.enter();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let (_artifact, imported) = export_and_import(&root, &rt, 512, 0);
    assert_eq!(
        imported.tx_commits, 1,
        "512 rows ≈ 4.4 MB ≪ 32 MiB ⇒ no mid-stream commit, tail only"
    );
}

/// One row past the count cap must produce exactly one mid-stream commit
/// plus the tail: 513 rows — row 513's admission sees pending = 512 ≥
/// `RECORDS_PER_TX` ⇒ CommitFirst (bytes still ≈ 4.4 MB ≪ usable), the
/// 513th row stages into the fresh batch, the tail commits it ⇒ 2.
///
/// Catches a meter that drops the count cap or mis-accounts the tail.
#[test]
fn import_load_commits_twice_past_the_count_cap() {
    let rt = runtime();
    let _enter = rt.enter();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let (_artifact, imported) = export_and_import(&root, &rt, 513, 0);
    assert_eq!(
        imported.tx_commits, 2,
        "513 rows ⇒ CommitFirst at row 513 (pending == 512) + tail = 2 commits"
    );
}

/// RAII reset for the commit-boundary seam: disarms and clears the commit
/// counter even when the test panics.
struct BoundaryGuard;

impl BoundaryGuard {
    fn arm() -> Self {
        COMMIT_BOUNDARY.commits.store(0, Ordering::SeqCst);
        COMMIT_BOUNDARY.armed.store(true, Ordering::SeqCst);
        BoundaryGuard
    }
}

impl Drop for BoundaryGuard {
    fn drop(&mut self) {
        COMMIT_BOUNDARY.armed.store(false, Ordering::SeqCst);
        COMMIT_BOUNDARY.commits.store(0, Ordering::SeqCst);
    }
}

/// Cancellation at an armed commit boundary must issue NO commit: the load
/// waits at the boundary (first one = the row-513 mid-stream commit of the
/// 1,202-row fixture), the test observes it, cancels, releases — and the
/// import must return `Cancelled` with `commits == 0`.
///
/// Catches a load that commits AFTER cancellation (a dropped/detached
/// commit can leave a stage behind; the driver's error path discards it
/// and a later retry cleans any stale stage — that cleanup is what makes
/// zero-commits-safe acceptable).
#[test]
fn import_cancelled_before_a_mid_stream_commit_issues_no_commit() {
    let rt = runtime();
    let _enter = rt.enter();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let artifact = build_and_export(&root, &rt, SMALL_RECORDS, LARGE_RECORDS);
    let target = root.join("target");
    let token = CancellationToken::new();
    let _guard = BoundaryGuard::arm();
    // Spawned directly on `rt` (the whole test holds `rt.enter()`, so the
    // runtime context is installed) — the current-thread runtime drives the
    // spawned import while the test awaits the handshake below.
    let handle = rt.spawn(logical_import::import(
        fixture::import_args(&artifact, &target),
        token.clone(),
    ));
    // First armed boundary = the row-513 mid-stream commit.
    rt.block_on(timeout(
        BOUNDARY_DEADLINE,
        COMMIT_BOUNDARY.at_boundary.notified(),
    ))
    .unwrap();
    assert_eq!(
        COMMIT_BOUNDARY.commits.load(Ordering::SeqCst),
        0,
        "the boundary precedes the commit: nothing committed yet"
    );
    token.cancel();
    COMMIT_BOUNDARY.resume.notify_one();
    let outcome = rt
        .block_on(timeout(BOUNDARY_DEADLINE, async { handle.await.unwrap() }))
        .unwrap();
    assert!(
        matches!(outcome, Err(logical_import::LogicalImportError::Cancelled)),
        "cancelled import must return Cancelled, got {outcome:?}"
    );
    assert_eq!(
        COMMIT_BOUNDARY.commits.load(Ordering::SeqCst),
        0,
        "no commit may be issued after cancellation"
    );
    assert!(
        !target.join("shamir_db_meta.redb").exists(),
        "the target must never be published by a cancelled import"
    );
}

/// Same handshake at the TAIL commit: the (300, 0) fixture never crosses
/// the 512 count cap (300 × ~8.4 KB ≈ 2.5 MB ≪ usable) so its ONLY commit
/// is the tail — cancellation there must also issue no commit.
#[test]
fn import_cancelled_before_the_tail_commit_issues_no_commit() {
    let rt = runtime();
    let _enter = rt.enter();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let artifact = build_and_export(&root, &rt, 300, 0);
    let target = root.join("target");
    let token = CancellationToken::new();
    let _guard = BoundaryGuard::arm();
    // Spawned directly on `rt` (the whole test holds `rt.enter()`, so the
    // runtime context is installed) — the current-thread runtime drives the
    // spawned import while the test awaits the handshake below.
    let handle = rt.spawn(logical_import::import(
        fixture::import_args(&artifact, &target),
        token.clone(),
    ));
    rt.block_on(timeout(
        BOUNDARY_DEADLINE,
        COMMIT_BOUNDARY.at_boundary.notified(),
    ))
    .unwrap();
    assert_eq!(
        COMMIT_BOUNDARY.commits.load(Ordering::SeqCst),
        0,
        "the only commit (the tail) has not fired yet"
    );
    token.cancel();
    COMMIT_BOUNDARY.resume.notify_one();
    let outcome = rt
        .block_on(timeout(BOUNDARY_DEADLINE, async { handle.await.unwrap() }))
        .unwrap();
    assert!(
        matches!(outcome, Err(logical_import::LogicalImportError::Cancelled)),
        "cancelled import must return Cancelled, got {outcome:?}"
    );
    assert_eq!(
        COMMIT_BOUNDARY.commits.load(Ordering::SeqCst),
        0,
        "no tail commit may be issued after cancellation"
    );
    assert!(
        !target.join("shamir_db_meta.redb").exists(),
        "the target must never be published by a cancelled import"
    );
}
