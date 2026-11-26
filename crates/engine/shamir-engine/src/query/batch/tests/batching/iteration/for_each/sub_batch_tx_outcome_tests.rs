#[cfg(feature = "fjall")]
use futures::StreamExt;
use shamir_query_builder::batch::{Batch, Isolation};
use shamir_query_builder::write::{self, doc};
use shamir_query_types::batch::BatchError;
use shamir_types::access::Actor;

use super::super::support::TxTestResolver;
use crate::query::batch::execute_batch_unchecked as execute_batch;
use crate::query::TableRef;
use crate::repo::repo_types::BoxRepoFactory;
use crate::repo::RepoInstance;
use crate::table::TableConfig;
#[cfg(feature = "fjall")]
use crate::tx::tests::drain::truncation::truncation_tests::reopen_disk_repo;

/// Resolver: on the EXECUTION-phase resolve of `gate` it runs `writer` (a real
/// transactional batch), committing a concurrent update between the body's
/// recorded SELECT and the body's commit — the
/// `for_each_conflict_tests.rs::CommitConflictResolver` shape, aimed at a
/// `BatchOp::Batch` child instead of a loop body. `gate_resolves` 0 =
/// `validate_tables` (pre-execution), 1 = the body's stage-1 `g` resolve.
struct CommitConflictResolver {
    repo: RepoInstance,
    gate_resolves: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    writer_resolve_index: usize,
    writer: shamir_query_types::batch::BatchRequest,
}

#[async_trait::async_trait]
impl crate::query::batch::TableResolver for CommitConflictResolver {
    async fn resolve(
        &self,
        table_ref: &TableRef,
    ) -> shamir_storage::error::DbResult<crate::table::TableManager> {
        if table_ref.table == "gate" {
            let n = self
                .gate_resolves
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == self.writer_resolve_index {
                let writer_resolver = TxTestResolver {
                    repo: self.repo.clone(),
                };
                let resp = execute_batch(
                    &self.writer,
                    &writer_resolver,
                    None,
                    None,
                    Actor::System,
                    "test",
                )
                .await
                .expect("the concurrent writer batch must execute");
                let info = resp.transaction.expect("writer batch carries tx metadata");
                assert_eq!(info.status, "committed", "writer must commit, got {info:?}");
            }
        }
        self.repo.get_table(&table_ref.table).await
    }
}

#[async_trait::async_trait]
impl crate::query::batch::BatchResolver for CommitConflictResolver {
    async fn resolve_repo(
        &self,
        _repo_name: &str,
    ) -> shamir_storage::error::DbResult<crate::tx::RepoCore> {
        Ok(self.repo.core.clone())
    }
}

/// Count the rows durably present in `orders`.
#[cfg(feature = "fjall")]
async fn count_orders(repo: &RepoInstance) -> usize {
    let table = repo.get_table("orders").await.expect("orders table");
    let stream = table.list_stream(64);
    futures::pin_mut!(stream);
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        rows.extend(batch.expect("orders rows decode"));
    }
    rows.len()
}

/// RED on the parent commit: `run_sub_batch_arm`'s non-tx arm used to keep
/// only `.results`, so an aborted transactional child commit looked like an
/// empty child value and the outer batch reported success.
#[tokio::test]
async fn transactional_sub_batch_aborted_commit_fails_the_container_not_empty_success() {
    let factory = BoxRepoFactory::in_memory();
    let repo = RepoInstance::from_factory(
        "test".into(),
        factory,
        vec![TableConfig::new("users"), TableConfig::new("gate")],
    )
    .await
    .unwrap();
    let tx_resolver = TxTestResolver { repo: repo.clone() };

    // Seed one visible row so the body's SELECT records a real version.
    let mut seed = Batch::new();
    seed.id("seed");
    seed.insert(
        "seed",
        write::insert("users").row(
            doc()
                .set("name", "alice")
                .unwrap()
                .set("val", "initial")
                .unwrap(),
        ),
    );
    let seed_req = seed.build();
    execute_batch(&seed_req, &tx_resolver, None, None, Actor::System, "test")
        .await
        .unwrap();

    // Concurrent writer: a transactional update that bumps the seeded row's
    // version while the child body's tx is still open.
    let mut wb = Batch::new();
    wb.id("writer");
    wb.transactional();
    wb.try_update(
        "w",
        write::update("users")
            .where_(shamir_query_builder::filter::eq("name", "alice"))
            .set(doc().set("val", "rewritten").unwrap()),
    )
    .unwrap();
    let writer_req = wb.build();

    // Transactional child body (Serializable): SELECT the seeded row, then a
    // gate read in a later stage that runs the writer; the child's commit
    // must detect it.
    let mut bb = Batch::new();
    bb.id("child");
    bb.transactional();
    bb.isolation(Isolation::Serializable);
    let r = bb.query("r", shamir_query_builder::query::Query::from("users"));
    bb.query(
        "g",
        shamir_query_builder::query::Query::from("gate").where_eq("probe", r.first().field("name")),
    );
    let child = bb.build();

    let mut outer = Batch::new();
    outer.id("outer");
    outer.sub_batch_no_bind("sub", child);
    let req = outer.build();
    let resolver = CommitConflictResolver {
        repo: repo.clone(),
        gate_resolves: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        writer_resolve_index: 1,
        writer: writer_req,
    };

    let err = execute_batch(&req, &resolver, None, None, Actor::System, "test")
        .await
        .expect_err("an aborted child commit must fail the container, not yield an empty value");
    assert_eq!(
        err.code(),
        Some("transaction_aborted"),
        "a sub-batch caller has no committed-prefix knowledge, so the retryable abort's own \
         code (tx_conflict) must never classify the request, got: {err:?}"
    );
    match &err {
        BatchError::QueryError { message, .. } => {
            assert!(
                message.contains("abort code tx_conflict"),
                "the abort cause must stay in the message, got: {message}"
            );
        }
        other => panic!("expected a coded query error, got: {other:?}"),
    }
}

#[cfg(feature = "fjall")]
struct SegmentCapEnvGuard(Option<std::ffi::OsString>);
#[cfg(feature = "fjall")]
impl Drop for SegmentCapEnvGuard {
    fn drop(&mut self) {
        if let Some(value) = self.0.take() {
            std::env::set_var("SHAMIR_WAL_SEGMENT_MAX_BYTES", value);
        } else {
            std::env::remove_var("SHAMIR_WAL_SEGMENT_MAX_BYTES");
        }
    }
}

/// RED on the parent commit: an indeterminate child commit used to return
/// `Ok` with an empty child value, so the outer batch's later alias ran and
/// no reopen-required error carried the attempted version.
#[cfg(feature = "fjall")]
#[tokio::test(flavor = "current_thread")]
#[serial_test::serial]
async fn transactional_sub_batch_indeterminate_commit_reports_typed_reopen_with_attempted_version()
{
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let _segment_cap_env = SegmentCapEnvGuard(std::env::var_os("SHAMIR_WAL_SEGMENT_MAX_BYTES"));
    std::env::set_var("SHAMIR_WAL_SEGMENT_MAX_BYTES", "1");
    let repo = reopen_disk_repo(
        "sub_batch_indeterminate",
        tempdir.path().join("repo"),
        vec![TableConfig::new("orders")],
    )
    .await;
    let resolver = TxTestResolver { repo: repo.clone() };

    // Baseline: one successful transactional commit pins the version clock.
    let mut baseline = Batch::new();
    baseline.id("baseline");
    baseline.transactional();
    baseline.insert(
        "ins",
        write::insert("orders").row(doc().set("user_id", 0_i64).unwrap()),
    );
    let baseline_response = execute_batch(
        &baseline.build(),
        &resolver,
        None,
        None,
        Actor::System,
        "test",
    )
    .await
    .expect("baseline transaction must commit");
    let v0 = baseline_response
        .transaction
        .expect("baseline response must include transaction info")
        .commit_version
        .expect("baseline commit must have a version");

    // Transactional child whose forced WAL seal failure leaves its commit
    // outcome indeterminate; the outer batch carries a later alias that must
    // never run once the child's outcome is unknown.
    // A literal insert: a body with an unbound `$param` would abort during
    // substitution and never reach the armed seal fault.
    let mut child = Batch::new();
    child.transactional();
    child.insert(
        "ins",
        write::insert("orders").row(doc().set("user_id", 1_i64).unwrap()),
    );
    let body = child.build();
    let mut outer = Batch::new();
    outer.id("outer");
    outer.sub_batch_no_bind("sub", body);
    outer.insert(
        "after",
        write::insert("orders").row(doc().set("user_id", 9_999_i64).unwrap()),
    );
    let req = outer.build();

    let wal = repo.wal_group().await.unwrap();
    wal.arm_fail_next_seal_sync_for_test();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        execute_batch(&req, &resolver, None, None, Actor::System, "test"),
    )
    .await
    .expect("SubBatch indeterminate execution must finish within 120 seconds");

    let err = result.expect_err("an indeterminate outcome must fail the container");
    assert_eq!(
        err.code(),
        Some("wal_requires_reopen"),
        "the indeterminate outcome must map to the reopen disposition, got: {err:?}"
    );
    match err {
        BatchError::WalOutcomeUnknown { commit_version, .. } => {
            assert_eq!(
                commit_version,
                v0 + 1,
                "the indeterminate outcome must carry the exact attempted version"
            );
        }
        other => panic!("expected typed WalOutcomeUnknown error, got: {other:?}"),
    }

    let rows = count_orders(&repo).await;
    assert!(
        rows <= 2,
        "only the baseline and the child's attempted insert may exist; the outer batch's \
         'after' alias must never have run, got {rows} rows"
    );
}
