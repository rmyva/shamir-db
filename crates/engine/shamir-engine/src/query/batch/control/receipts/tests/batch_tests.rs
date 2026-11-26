use std::future::{poll_fn, Future};
use std::task::Poll;

use crate::db_instance::db_instance::DbInstance;
use crate::query::batch::{execute_batch_unchecked, BatchResolver, TableResolver};
use crate::query::TableRef;
use crate::repo::{BoxRepoFactory, RepoConfig};
use crate::table::{TableConfig, TableManager};
use crate::tx::commit::TEST_SYNCED_WAL_REPO;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::query::Query;
use shamir_query_builder::val::{lit, param};
use shamir_query_builder::write::{doc, Insert};
use shamir_query_types::batch::{VisibilityPendingReason, VisibilityState, VisibilityWait};
use shamir_storage::error::{DbError, DbResult};
use shamir_types::access::Actor;
use shamir_types::types::value::QueryValue;

const STATUS: VisibilityWait = VisibilityWait::Bounded { timeout_ms: 0 };

struct Resolver(DbInstance);

#[async_trait::async_trait]
impl TableResolver for Resolver {
    async fn resolve(&self, table: &TableRef) -> DbResult<TableManager> {
        self.0.get_table(&table.repo, &table.table).await
    }
}

#[async_trait::async_trait]
impl BatchResolver for Resolver {
    async fn resolve_repo(&self, name: &str) -> DbResult<crate::tx::RepoCore> {
        self.0
            .get_repo(name)
            .map(|repo| repo.core.clone())
            .ok_or_else(|| DbError::NotFound(name.into()))
    }
}

async fn fixture() -> Resolver {
    let configs = ["default", "other"]
        .into_iter()
        .map(|name| {
            RepoConfig::new(name, BoxRepoFactory::in_memory()).add_table(TableConfig::new("items"))
        })
        .collect();
    let resolver = Resolver(DbInstance::with_repos(configs).await.unwrap());
    for name in ["default", "other"] {
        resolver.0.get_table(name, "items").await.unwrap();
    }
    resolver
}

fn write(repo: &str) -> shamir_query_types::batch::BatchRequest {
    let mut batch = Batch::new();
    batch.insert(
        "written",
        Insert::with_repo(repo, "items").row(doc().set("n", 1).unwrap()),
    );
    batch.build()
}

// Red before VIS-4b: non-tx Bounded was rejected; suppressed aliases vanished.
#[tokio::test(start_paused = true)]
async fn implicit_pending_suppressed_multi_repo_and_dependent_read() {
    let resolver = fixture().await;
    let repo = resolver.0.get_repo("default").unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let lower = gate.assign_next_version_guarded();
    let mut batch = Batch::new();
    batch.visibility_wait(STATUS).return_flagged();
    let written = batch.op_silent(
        "written",
        Insert::with_repo("default", "items").row(doc().set("n", 1).unwrap()),
    );
    let read = batch.query("dependent", Query::with_repo("default", "items"));
    batch.after(&read, &written);
    batch.query(
        "consumer",
        Query::with_repo("other", "items").where_eq("n", read.first().field("n")),
    );
    batch.insert(
        "independent",
        Insert::with_repo("other", "items").row(doc().set("n", 2).unwrap()),
    );
    batch.query("unrelated", Query::with_repo("other", "items"));
    let request = batch.build();
    let task = tokio::spawn(async move {
        execute_batch_unchecked(&request, &resolver, None, None, Actor::System, "test").await
    });
    let response = task.await.unwrap().unwrap();
    assert!(response.transaction.is_none());
    let result = &response.results["written"];
    assert!(result.records.is_empty());
    let receipt = result.receipt.as_ref().unwrap();
    assert_eq!(receipt.commit_version, Some(2));
    assert_eq!(
        receipt.visibility,
        Some(VisibilityState::Pending {
            visible_through: 0,
            blocking_version: 1,
            reason: VisibilityPendingReason::Timeout,
        }),
    );
    assert!(response.results["independent"]
        .receipt
        .as_ref()
        .unwrap()
        .is_visible());
    assert!(response.results["unrelated"].stats.is_some());
    for alias in ["dependent", "consumer"] {
        let result = &response.results[alias];
        assert!(result.skipped);
        assert!(result.records.is_empty());
        assert!(result.stats.is_none());
    }
    let failure = response.results["dependent"].value.as_ref().unwrap();
    assert_eq!(
        failure["code"],
        QueryValue::Str("visibility_dependency_pending".into()),
    );
    assert_eq!(failure["commit_version"], QueryValue::Int(2));
    assert_eq!(failure["visible_through"], QueryValue::Int(0));
    lower.commit();
    assert_eq!(
        repo.observe_visibility(2, STATUS).await.unwrap(),
        VisibilityState::Visible,
    );
}

// Red before VIS-4b: nested Bounded rejected or TransactionInfo discarded.
#[tokio::test(start_paused = true)]
async fn nested_and_planned_transactional_bodies_retain_receipts() {
    for transactional in [false, true] {
        let resolver = fixture().await;
        let gate = resolver
            .0
            .get_repo("default")
            .unwrap()
            .tx_gate()
            .await
            .unwrap();
        let lower = gate.assign_next_version_guarded();
        let mut body = write("default");
        body.transactional = transactional;
        let mut outer = Batch::new();
        outer.visibility_wait(STATUS);
        outer.sub_batch_no_bind("nested", body.clone());
        outer.for_each("loop", vec![lit(1)], "row", body);
        let request = outer.build();
        let response = tokio::spawn(async move {
            execute_batch_unchecked(&request, &resolver, None, None, Actor::System, "test").await
        })
        .await
        .unwrap()
        .unwrap();
        for alias in ["nested", "loop"] {
            let pending = super::super::dependency::pending(&response.results[alias]).unwrap();
            assert!(pending.0 > lower.version());
            assert_eq!(pending.1, 0);
        }
        lower.commit();
    }
}

#[tokio::test(start_paused = true)]
async fn explicit_top_level_pending_retains_rows_and_metadata() {
    let resolver = fixture().await;
    let gate = resolver
        .0
        .get_repo("default")
        .unwrap()
        .tx_gate()
        .await
        .unwrap();
    let lower = gate.assign_next_version_guarded();
    let mut request = write("default");
    request.transactional = true;
    request.visibility_wait = Some(STATUS);
    let response = tokio::spawn(async move {
        execute_batch_unchecked(&request, &resolver, None, None, Actor::System, "test").await
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.results["written"].records.len(), 1);
    let receipt = response.transaction.unwrap();
    assert!(receipt.is_committed());
    assert!(!receipt.is_visible());
    assert_eq!(receipt.commit_version, Some(2));
    lower.commit();
}

// Each iteration shares pending positions, but unrelated inner ops still run.
#[tokio::test(start_paused = true)]
async fn foreach_pending_reads_fail_without_dropping_independent_inner_ops() {
    let resolver = fixture().await;
    let gate = resolver
        .0
        .get_repo("default")
        .unwrap()
        .tx_gate()
        .await
        .unwrap();
    let lower = gate.assign_next_version_guarded();
    let mut body = Batch::new();
    let read = body.query("read", Query::with_repo("default", "items"));
    let written = body.insert(
        "written",
        Insert::with_repo("default", "items").row(doc().set("n", param("row")).unwrap()),
    );
    body.after(&written, &read);
    body.insert(
        "independent",
        Insert::with_repo("other", "items").row(doc().set("n", param("row")).unwrap()),
    );
    let mut outer = Batch::new();
    outer.visibility_wait(STATUS);
    outer.for_each("loop", vec![lit(1), lit(2)], "row", body.build());
    let request = outer.build();
    let response = tokio::spawn(async move {
        execute_batch_unchecked(&request, &resolver, None, None, Actor::System, "test").await
    })
    .await
    .unwrap()
    .unwrap();
    let QueryValue::List(iterations) = response.results["loop"].value.as_ref().unwrap() else {
        panic!("expected iterations");
    };
    assert_eq!(iterations.len(), 2);
    assert_eq!(iterations[1]["read"]["skipped"], QueryValue::Bool(true));
    assert_eq!(
        iterations[1]["read"]["value"]["code"],
        QueryValue::Str("visibility_dependency_pending".into()),
    );
    for iteration in iterations {
        assert_eq!(
            iteration["independent"]["receipt"]["visibility"]["state"],
            QueryValue::Str("visible".into()),
        );
    }
    lower.commit();
}

// Red before VIS-4b: a known durability warning became a query error.
#[tokio::test(start_paused = true)]
async fn implicit_durability_warning_keeps_pending_receipt() {
    let resolver = fixture().await;
    let repo = resolver.0.get_repo("default").unwrap();
    TEST_SYNCED_WAL_REPO.set(repo.name().to_owned()).unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let lower = gate.assign_next_version_guarded();
    repo.repo_wal().await.unwrap().arm_fail_next_sync_for_test();
    let mut request = write("default");
    request.visibility_wait = Some(STATUS);
    let response = tokio::spawn(async move {
        execute_batch_unchecked(&request, &resolver, None, None, Actor::System, "test").await
    })
    .await
    .unwrap()
    .unwrap();
    let receipt = response.results["written"].receipt.as_ref().unwrap();
    assert!(receipt.is_committed());
    assert_eq!(receipt.error.as_deref(), Some("wal_durability"));
    assert!(!receipt.is_visible());
    assert_eq!(receipt.commit_version, Some(2));
    lower.commit();
}

#[tokio::test(start_paused = true)]
async fn strict_implicit_waits_until_lower_settles() {
    let resolver = fixture().await;
    let gate = resolver
        .0
        .get_repo("default")
        .unwrap()
        .tx_gate()
        .await
        .unwrap();
    let lower = gate.assign_next_version_guarded();
    let request = write("default");
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let signal = gate.clone();
    let task = tokio::spawn(async move {
        let mut future = Box::pin(execute_batch_unchecked(
            &request,
            &resolver,
            None,
            None,
            Actor::System,
            "test",
        ));
        let mut sender = Some(sender);
        poll_fn(|cx| {
            let result = future.as_mut().poll(cx);
            if signal.completion().waiter_count() == 1 {
                assert!(matches!(result, Poll::Pending));
                if let Some(sender) = sender.take() {
                    sender.send(()).unwrap();
                }
            }
            result
        })
        .await
    });
    receiver.await.unwrap();
    assert!(!task.is_finished());
    lower.commit();
    let response = task.await.unwrap().unwrap();
    assert!(response.results["written"]
        .receipt
        .as_ref()
        .unwrap()
        .is_visible());
}

struct BoundedResolver(Resolver);

#[async_trait::async_trait]
impl TableResolver for BoundedResolver {
    async fn resolve(&self, table: &TableRef) -> DbResult<TableManager> {
        self.0.resolve(table).await
    }
}

#[async_trait::async_trait]
impl BatchResolver for BoundedResolver {
    async fn resolve_repo(&self, name: &str) -> DbResult<crate::tx::RepoCore> {
        let mut core = self.0.resolve_repo(name).await?;
        core.set_commit_visibility_wait(STATUS);
        Ok(core)
    }
}

#[tokio::test]
async fn repository_bounded_default_preflights_call_before_prior_write() {
    let resolver = BoundedResolver(fixture().await);
    let gate = resolver
        .0
         .0
        .get_repo("default")
        .unwrap()
        .tx_gate()
        .await
        .unwrap();
    let mut batch = Batch::new();
    let written = batch.insert(
        "written",
        Insert::with_repo("default", "items").row(doc().set("n", 1).unwrap()),
    );
    let call = batch.call_in_repo("call", "unavailable", "default", Vec::<i32>::new());
    batch.after(&call, &written);
    let error =
        execute_batch_unchecked(&batch.build(), &resolver, None, None, Actor::System, "test")
            .await
            .unwrap_err();
    assert_eq!(error.code(), Some("validation"));
    assert_eq!(gate.version_allocation_high_water_mark(), 0);
}

#[tokio::test]
async fn receipt_preflight_reuses_root_classification_and_matches_fallback() {
    let resolver = BoundedResolver(fixture().await);
    let mut batch = Batch::new();
    batch.call_in_repo("call", "unavailable", "default", Vec::<i32>::new());
    let mut request = batch.build();
    request.transactional = true;
    let metadata = shamir_query_types::batch::RequestMetadata::collect(&request.queries, "test");
    let cached = super::super::policy::validate(&request, &resolver, Some(&metadata))
        .await
        .unwrap_err();
    let fallback = super::super::policy::validate(&request, &resolver, None)
        .await
        .unwrap_err();
    assert_eq!(cached.code(), fallback.code());
    assert_eq!(cached.code(), Some("validation"));

    // For a non-transactional request the root early return depends on the
    // write classification, so a deliberately different cache proves the root
    // path consumes it. Production metadata always comes from this request.
    request.transactional = false;
    let genuine = shamir_query_types::batch::RequestMetadata::collect(&request.queries, "test");
    assert!(
        super::super::policy::validate(&request, &resolver, Some(&genuine))
            .await
            .is_err()
    );
    let mut different =
        shamir_query_types::batch::RequestMetadata::collect(&request.queries, "test");
    *different.writes.get_mut("call").unwrap() = false;
    assert!(
        super::super::policy::validate(&request, &resolver, Some(&different))
            .await
            .is_ok()
    );
}

struct BoundedDefaultResolver(Resolver);

#[async_trait::async_trait]
impl TableResolver for BoundedDefaultResolver {
    async fn resolve(&self, table: &TableRef) -> DbResult<TableManager> {
        self.0.resolve(table).await
    }
}

#[async_trait::async_trait]
impl BatchResolver for BoundedDefaultResolver {
    async fn resolve_repo(&self, name: &str) -> DbResult<crate::tx::RepoCore> {
        let mut repo = self.0.resolve_repo(name).await?;
        if name == "default" {
            repo.horizon.set_commit_visibility_wait(STATUS);
        }
        Ok(repo)
    }
}

// The Call sits in an unbounded repository; only a write two bodies deeper
// touches the bounded one, so the preflight must see the grandchild's repository.
#[tokio::test]
async fn nested_preflight_sees_bounded_repository_of_deeper_body() {
    let resolver = BoundedDefaultResolver(fixture().await);
    let mut innermost = Batch::new();
    innermost.insert(
        "deep",
        Insert::with_repo("default", "items").row(doc().set("n", 1).unwrap()),
    );
    let mut middle = Batch::new();
    middle.call_in_repo("call", "unavailable", "other", Vec::<i32>::new());
    middle.sub_batch_no_bind("inner", innermost.build());
    let mut outer = Batch::new();
    outer.sub_batch_no_bind("middle", middle.build());
    let request = outer.build();
    let error = super::super::policy::validate(&request, &resolver, None)
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some("validation"));
}

#[test]
fn receipt_accounting_includes_metadata_and_nested_envelope_key() {
    let plain = crate::query::read::QueryResult::default();
    let receipt = shamir_query_types::batch::TransactionInfo::committed(1, 0, 2, true)
        .with_visibility(VisibilityState::Pending {
            visible_through: 0,
            blocking_version: 1,
            reason: VisibilityPendingReason::Timeout,
        });
    let result = crate::query::read::QueryResult {
        receipt: Some(receipt.clone()),
        ..plain.clone()
    };
    let size = crate::query::batch::result_size::ResultSize::result(&result);
    assert_eq!(
        size - crate::query::batch::result_size::ResultSize::result(&plain),
        super::super::accounting::receipt_size(&receipt),
    );
    let mut aliases = shamir_types::types::common::new_map();
    aliases.insert("write".into(), result);
    let with = crate::query::batch::result_size::ResultSize::assembled(&aliases);
    aliases.insert("write".into(), plain);
    let without = crate::query::batch::result_size::ResultSize::assembled(&aliases);
    assert_eq!(with - without, 16 + 7);
}

#[tokio::test]
async fn strict_implicit_visible_receipt_preserves_rows() {
    let resolver = fixture().await;
    let response = execute_batch_unchecked(
        &write("default"),
        &resolver,
        None,
        None,
        Actor::System,
        "test",
    )
    .await
    .unwrap();
    let result = &response.results["written"];
    assert_eq!(result.records.len(), 1);
    assert!(result.receipt.as_ref().unwrap().is_visible());
}
