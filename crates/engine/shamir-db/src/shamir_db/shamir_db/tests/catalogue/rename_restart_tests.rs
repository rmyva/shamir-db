use crate::access::{Actor, ResourceMeta};
use crate::engine::repo::{BoxRepoFactory, RepoConfig};
use crate::shamir_db::{ShamirDb, SystemStoreConfig};
use crate::types::value::InnerValue;
use futures::StreamExt;
use shamir_test_diag::FlakeGuard;
use shamir_types::types::common::new_map;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::QueryValue;
use tokio::runtime::Builder;

/// Not a latency assertion: these tests need the clean shutdown to finish.
/// Windows CI runners stall every process's fsync for 10+ s at times, so any
/// wall-clock budget here is a false failure; nextest's per-test timeout is
/// the hang detector and names the test.
const BOOT_TEST_FLUSH_BUDGET: std::time::Duration = std::time::Duration::from_secs(3600);

/// Re-open the system store expecting a boot rejection, retrying while the
/// dropped instance still holds the fjall file lock (released lazily after
/// the drop). Returns the rejection's message.
async fn boot_rejection(path: std::path::PathBuf) -> String {
    for _ in 0..100 {
        match ShamirDb::init(SystemStoreConfig::Fjall(path.clone())).await {
            Ok(_) => panic!("boot must be rejected"),
            Err(e) => {
                let message = e.to_string();
                if !message.contains("Locked") {
                    return message;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
    panic!("fjall lock was not released within the retry window");
}

async fn rows(table: &crate::engine::table::TableManager) -> Vec<(RecordId, InnerValue)> {
    table
        .list_stream(32)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flat_map(Result::unwrap)
        .map(|(id, value)| (id, value.into_inner().unwrap()))
        .collect()
}

fn assert_exact_rows(actual: &[(RecordId, InnerValue)], expected: &[(RecordId, InnerValue)]) {
    assert_eq!(actual.len(), expected.len());
    for row in expected {
        assert!(actual.contains(row));
    }
}

#[tokio::test]
async fn rv_db_reserved_name_persisted_catalogue_is_rejected_on_boot() {
    // Removing `validate_catalogue_db_names(&db_records)?` from core.rs makes this test pass on the broken build.
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("system.redb");
    let db = ShamirDb::init(SystemStoreConfig::Fjall(path.clone()))
        .await
        .unwrap();
    let mut fields = new_map();
    fields.insert("name".into(), QueryValue::Str("users".into()));
    let record = QueryValue::Map(fields);
    db.system_store()
        .save_database(
            "users",
            &record,
            &ResourceMeta::owned_enforced(Actor::System),
        )
        .await
        .unwrap();
    db.system_store()
        .flush_buffers_with_quiesce_timeout(BOOT_TEST_FLUSH_BUDGET)
        .await
        .unwrap();
    drop(db);
    let error = boot_rejection(path).await;
    assert!(error.contains("users"), "unexpected boot error: {error}");
    assert!(error.contains("reserved"), "unexpected boot error: {error}");
}

#[test]
fn in_doubt_rename_reopens_as_exactly_one_catalogued_incarnation() {
    // Failure-only flake diagnostics (design:
    // docs/dev-artifacts/design/flake-failure-logs.md): the guard prints its
    // timeline + captured log records ONLY while the test is panicking.
    let guard = FlakeGuard::new("in_doubt_rename_reopens_as_exactly_one_catalogued_incarnation");
    let directory = tempfile::tempdir().unwrap();
    let system_path = directory.path().join("system");
    let repo_path = directory.path().join("repo");
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let (expected_rows, old_store_id, new_store_id) = runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path.clone()))
                .await
                .unwrap();
            db.create_db("restart").await.unwrap();
            db.add_repo(
                "restart",
                RepoConfig::new("main", BoxRepoFactory::fjall(&repo_path)),
            )
            .await
            .unwrap();
            db.add_table("restart", "main", "a", false).await.unwrap();
            let repo = db.get_db("restart").unwrap().get_repo("main").unwrap();
            let source = repo.get_table("a").await.unwrap();
            let ids = source
                .insert_many(&[
                    InnerValue::Str("persisted-one".into()),
                    InnerValue::Str("persisted-two".into()),
                ])
                .await
                .unwrap();
            let expected = vec![
                (ids[0], InnerValue::Str("persisted-one".into())),
                (ids[1], InnerValue::Str("persisted-two".into())),
            ];
            // Make the rows durable before the injected fault, as the reopen reads them from disk.
            db.flush_all_for_test().await.unwrap();
            let source_record = db
                .system_store()
                .load_table_record("restart", "main", "a")
                .await
                .unwrap()
                .unwrap();
            let old_id = source_record
                .get("store_id")
                .and_then(crate::types::value::QueryValue::as_str)
                .unwrap()
                .to_owned();
            guard.note("pre-rename incarnation store_id", old_id.clone());
            db.system_store().arm_durability_tail_faults(2);
            guard.mark("rename started (2 durability-tail faults armed)");
            assert!(matches!(
                db.rename_table_as("restart", "main", "a", "b", crate::access::Actor::System).await,
                Err(crate::DbError::Conflict(message)) if message.starts_with("rename in doubt:")
            ));
            guard.mark("crash point: rename returned in-doubt");
            assert!(repo.has_rename_in_doubt());
            let destination_record = db
                .system_store()
                .load_table_record("restart", "main", "b")
                .await
                .unwrap()
                .unwrap();
            let new_id = destination_record
                .get("store_id")
                .and_then(crate::types::value::QueryValue::as_str)
                .unwrap()
                .to_owned();
            guard.note("post-crash destination store_id", new_id.clone());
            (expected, old_id, new_id)
        })
        .await
        .unwrap()
    });
    drop(runtime);

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path))
                .await
                .unwrap();
            guard.mark("reopen after the in-doubt crash");
            let repo = db.get_db("restart").unwrap().get_repo("main").unwrap();
            assert!(!repo.has_rename_in_doubt());
            let a_row = db
                .system_store()
                .load_table_record("restart", "main", "a")
                .await
                .unwrap();
            let b_row = db
                .system_store()
                .load_table_record("restart", "main", "b")
                .await
                .unwrap();
            guard.note(
                "catalogue after reopen",
                format!(
                    "a present: {}, b present: {}",
                    a_row.is_some(),
                    b_row.is_some()
                ),
            );
            guard.mark("catalogue contents read");
            assert_ne!(a_row.is_some(), b_row.is_some());
            let (name, row, other) = if let Some(row) = b_row {
                ("b", row, "a")
            } else {
                ("a", a_row.unwrap(), "b")
            };
            assert!(repo.has_table(name));
            assert!(!repo.has_table(other));
            let store_id = row
                .get("store_id")
                .and_then(crate::types::value::QueryValue::as_str)
                .unwrap();
            let expected_name = if store_id == old_store_id {
                "a"
            } else {
                assert_eq!(store_id, new_store_id);
                "b"
            };
            assert_eq!(name, expected_name);
            guard.note(
                "surviving incarnation",
                format!("table {name}, store_id {store_id}"),
            );
            let table = repo.get_table(name).await.unwrap();
            assert_exact_rows(&rows(&table).await, &expected_rows);
            assert!(db
                .system_store()
                .load_table_record("restart", "main", other)
                .await
                .unwrap()
                .is_none());
            db.add_table("restart", "main", other, false).await.unwrap();
            assert!(repo.has_table(other));
        })
        .await
        .unwrap()
    });
    drop(runtime);
}
