use futures::StreamExt;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::write::{self, doc};
use shamir_query_types::batch::{BatchError, BatchRequest, ForEachOp};
use shamir_query_types::filter::FilterValue;
use shamir_types::access::Actor;

#[cfg(feature = "fjall")]
use super::super::support::insert_order_body;
use super::super::support::{wrap_for_each, TxTestResolver};
use crate::query::batch::execute_batch_unchecked as execute_batch;
use crate::query::TableRef;
use crate::repo::repo_types::BoxRepoFactory;
use crate::repo::RepoInstance;
use crate::table::TableConfig;
#[cfg(feature = "fjall")]
use crate::tx::tests::drain::truncation::truncation_tests::reopen_disk_repo;

/// Resolver: on the EXECUTION-phase resolve of `gate` it runs `writer` (a real
/// transactional batch), committing a concurrent update between the body's
/// recorded SELECT and the body's commit — the `ssi_tests.rs`
/// `GateBarrierResolver` shape. `gate_resolves` 0 = `validate_tables`
/// (pre-execution), 1 = the stage-1 `g` resolve (`runner.rs`).
struct CommitConflictResolver {
    repo: RepoInstance,
    gate_resolves: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    writer_resolve_index: usize,
    writer: BatchRequest,
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

#[tokio::test]
async fn transactional_body_commit_conflict_fails_the_iteration_with_tx_conflict() {
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
    // version while the body's tx is still open.
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

    // Body (transactional, Serializable): SELECT the seeded row, then a gate
    // read in a later stage that runs the writer; the commit must detect it.
    let mut bb = Batch::new();
    bb.id("body");
    bb.transactional();
    bb.isolation(shamir_query_builder::batch::Isolation::Serializable);
    let r = bb.query("r", shamir_query_builder::query::Query::from("users"));
    bb.query(
        "g",
        shamir_query_builder::query::Query::from("gate").where_eq("probe", r.first().field("name")),
    );
    let body = bb.build();

    let fe = ForEachOp {
        over: FilterValue::Array(vec![FilterValue::Int(1)]),
        bind_row: "uid".to_string(),
        batch: body,
    };
    let req = wrap_for_each(fe);
    let resolver = CommitConflictResolver {
        repo: repo.clone(),
        gate_resolves: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        writer_resolve_index: 1,
        writer: writer_req,
    };

    let err = execute_batch(&req, &resolver, None, None, Actor::System, "test")
        .await
        .expect_err("a commit-time conflict must fail the iteration, not yield an empty value");
    assert_eq!(
        err.code(),
        Some("tx_conflict"),
        "the commit-time classification must reach the iteration's code, got: {err:?}"
    );
}

#[tokio::test]
async fn transactional_body_commit_conflict_after_committed_prefix_keeps_prefix_and_is_not_retryable(
) {
    let factory = BoxRepoFactory::in_memory();
    let repo = RepoInstance::from_factory(
        "test".into(),
        factory,
        vec![
            TableConfig::new("users"),
            TableConfig::new("gate"),
            TableConfig::new("orders"),
        ],
    )
    .await
    .unwrap();
    let tx_resolver = TxTestResolver { repo: repo.clone() };

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
    execute_batch(
        &seed.build(),
        &tx_resolver,
        None,
        None,
        Actor::System,
        "test",
    )
    .await
    .unwrap();

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

    let mut bb = Batch::new();
    bb.id("body");
    bb.transactional();
    bb.isolation(shamir_query_builder::batch::Isolation::Serializable);
    bb.insert(
        "ins",
        write::insert("orders").row(doc().set("user_id", 1_i64).unwrap()),
    );
    let r = bb.query("r", shamir_query_builder::query::Query::from("users"));
    bb.query(
        "g",
        shamir_query_builder::query::Query::from("gate").where_eq("probe", r.first().field("name")),
    );
    let body = bb.build();
    let fe = ForEachOp {
        over: FilterValue::Array(vec![FilterValue::Int(1), FilterValue::Int(2)]),
        bind_row: "uid".to_string(),
        batch: body,
    };
    let req = wrap_for_each(fe);
    let gate_resolves = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let resolver = CommitConflictResolver {
        repo: repo.clone(),
        gate_resolves: gate_resolves.clone(),
        // Resolve 0 is validate_tables; execution resolves are 1 (iteration 0)
        // and 2 (iteration 1), so only the latter injects the writer.
        writer_resolve_index: 2,
        writer: writer_req,
    };

    let err = execute_batch(&req, &resolver, None, None, Actor::System, "test")
        .await
        .expect_err("the later iteration's commit conflict must fail the loop");
    // A retryable `tx_conflict` must not classify the request: replaying the
    // outer batch would repeat the committed prefix. The cause stays in the text.
    match &err {
        BatchError::QueryError { code, message, .. } => {
            assert_eq!(code.as_deref(), Some("transaction_aborted"), "got: {err:?}");
            assert!(
                message.contains("abort code tx_conflict"),
                "the abort cause must stay in the message, got: {message}"
            );
            assert!(
                message.contains("1 earlier iteration(s) committed"),
                "the committed prefix must be named, got: {message}"
            );
        }
        other => panic!("expected a coded query error, got: {other:?}"),
    }
    assert_eq!(
        gate_resolves.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "one validation resolve plus one execution resolve per iteration; writer runs on iteration 1 execution"
    );

    let table = repo.get_table("orders").await.unwrap();
    let stream = table.list_stream(64);
    futures::pin_mut!(stream);
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        rows.extend(batch.unwrap());
    }
    assert_eq!(
        rows.len(),
        1,
        "first iteration must remain durably committed"
    );
}

#[cfg(feature = "fjall")]
#[tokio::test(flavor = "current_thread")]
#[serial_test::serial]
async fn transactional_body_indeterminate_commit_stops_the_loop_with_wal_requires_reopen() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    struct SegmentCapEnvGuard(Option<std::ffi::OsString>);
    impl Drop for SegmentCapEnvGuard {
        fn drop(&mut self) {
            if let Some(value) = self.0.take() {
                std::env::set_var("SHAMIR_WAL_SEGMENT_MAX_BYTES", value);
            } else {
                std::env::remove_var("SHAMIR_WAL_SEGMENT_MAX_BYTES");
            }
        }
    }
    let _segment_cap_env = SegmentCapEnvGuard(std::env::var_os("SHAMIR_WAL_SEGMENT_MAX_BYTES"));
    std::env::set_var("SHAMIR_WAL_SEGMENT_MAX_BYTES", "1");
    let repo = reopen_disk_repo(
        "fe_indeterminate",
        tempdir.path().join("repo"),
        vec![TableConfig::new("orders")],
    )
    .await;
    let resolver = TxTestResolver { repo: repo.clone() };

    let mut baseline = Batch::new();
    baseline.id("indeterminate-baseline");
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

    let big = format!("fe-wal-{}", "p".repeat(512));
    // Keep the loop input within BatchLimits::default()'s 50-item cap; the
    // first transaction's forced WAL seal failure still exercises the
    // indeterminate-commit path and must stop subsequent iterations.
    let over: Vec<FilterValue> = (0..49)
        .map(|i| FilterValue::String(format!("{big}-{i:04}")))
        .collect();
    let mut body = insert_order_body("uid");
    body.transactional = true;
    let fe = ForEachOp {
        over: FilterValue::Array(over),
        bind_row: "uid".to_string(),
        batch: body,
    };
    let req = wrap_for_each(fe);

    let wal = repo.wal_group().await.unwrap();
    wal.arm_fail_next_seal_sync_for_test();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        execute_batch(&req, &resolver, None, None, Actor::System, "test"),
    )
    .await
    .expect("ForEach indeterminate execution must finish within 120 seconds");

    let err = result.expect_err("an indeterminate outcome must fail the loop");
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

    let table = repo.get_table("orders").await.unwrap();
    let stream = table.list_stream(64);
    futures::pin_mut!(stream);
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        rows.extend(batch.unwrap());
    }
    assert!(
        rows.len() <= 2,
        "only the baseline and iteration 0 may have inserted; later iterations must not run"
    );
}
