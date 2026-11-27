//! R1c target admission through the embedded admin batch entry point.

use std::{future::Future, path::Path, time::Duration};

use shamir_query_builder::{batch::Batch, ddl, write, Query};
use shamir_query_types::{batch::BatchOp, WriteOp};
use shamir_test_diag::FlakeGuard;
use shamir_types::access::{Actor, ResourceMeta, ResourcePath, OWNER_SYSTEM};

use super::helpers::{init_waiting_for_locks, on_big_stack, persistent_db, run_admin_op};
use crate::engine::repo::{repo_types::BoxRepoFactory, RepoConfig};
use crate::query::batch::{BatchError, BatchResponse};
use crate::shamir_db::system_store::DatabaseIdentity;
use crate::types::value::QueryValue;
use crate::ShamirDb;

const SERVING: &str = "serving";
const TARGET: &str = "target";
const EMPTY: &str = "empty_target";
const RENAMED: &str = "renamed_target";
const REPO: &str = "main";
const TABLE: &str = "sentinel";
const NEW_GUARD: &str = "new_guard";
const OLD_GUARD: &str = "old_guard";
const HMAC: &str = "test-hmac";
const IN_PROGRESS: &str = "schema_migration_in_progress";
const IN_DOUBT: &str = "schema_migration_in_doubt";
const DEADLINE: Duration = Duration::from_secs(180);

/// Cancel-safe: NO — on timeout the test panics; it does not reuse the engine.
async fn bounded<T>(step: impl Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, Box::pin(step))
        .await
        .expect("target-fence matrix step timed out")
}

async fn admin(db: &ShamirDb, serving: &str, op: BatchOp) -> Result<QueryValue, BatchError> {
    bounded(run_admin_op(db, serving, "admin", op)).await
}

async fn execute(db: &ShamirDb, serving: &str, batch: &Batch) -> Result<BatchResponse, BatchError> {
    let request = batch.to_request_via_msgpack().unwrap();
    bounded(db.execute(serving, &request)).await
}

fn assert_refused<T>(result: Result<T, BatchError>, code: &str, case: &str) {
    let Err(error) = result else {
        panic!("{case}: unexpectedly admitted");
    };
    assert_eq!(error.code(), Some(code), "{case}: {error}");
}

async fn register_validators(db: &ShamirDb) {
    for name in [NEW_GUARD, OLD_GUARD] {
        bounded(db.register_native_validator(name, false, |_, _, _| {
            shamir_engine::validator::Validation::accept()
        }))
        .await
        .unwrap();
    }
}

fn insert(value: &str) -> BatchOp {
    BatchOp::Insert(
        write::insert(TABLE)
            .row(write::doc().set("marker", value).unwrap())
            .build(),
    )
}

async fn create_with_sentinel(db: &ShamirDb, root: &Path, name: &str, suffix: &str) {
    bounded(db.create_db(name)).await.unwrap();
    let path = root.join(format!("{name}-{suffix}"));
    bounded(db.add_repo(name, RepoConfig::new(REPO, BoxRepoFactory::fjall(&path))))
        .await
        .unwrap();
    let mut batch = Batch::new();
    batch.op("table", ddl::create_table(TABLE).repo(REPO).build());
    execute(db, name, &batch).await.unwrap();
    let mut batch = Batch::new();
    batch.op("seed", insert(name));
    execute(db, name, &batch).await.unwrap();
}

async fn assert_sentinel(db: &ShamirDb, name: &str, value: &str) {
    let mut batch = Batch::new();
    batch.op("read", Query::from(TABLE));
    let result = execute(db, name, &batch).await.unwrap();
    let records = &result.results["read"].records;
    assert_eq!(records.len(), 1, "{name}: sentinel row count");
    assert_eq!(records[0].as_value()["marker"].as_str(), Some(value));
}

async fn identity(db: &ShamirDb, name: &str) -> DatabaseIdentity {
    bounded(db.system_store().schema_migration_identity(name))
        .await
        .unwrap()
}

async fn assert_absent(db: &ShamirDb, name: &str) {
    assert!(!db.has_db(name), "{name} still serves");
    assert!(
        bounded(db.system_store().load_database(name))
            .await
            .unwrap()
            .is_none(),
        "{name} still has a catalogue row"
    );
}

fn bind(name: &str) -> BatchOp {
    ddl::bind_validator(name, TABLE)
        .db(TARGET)
        .repo(REPO)
        .ops([WriteOp::Insert])
        .build()
}

fn unbind() -> BatchOp {
    ddl::unbind_validator(OLD_GUARD, TABLE)
        .db(TARGET)
        .repo(REPO)
        .build()
}

async fn assert_binding(db: &ShamirDb, serving: &str, expected: &str, absent: &str) {
    let result = admin(
        db,
        serving,
        ddl::list_validators(TABLE).db(TARGET).repo(REPO).build(),
    )
    .await
    .expect("ListValidators must remain available");
    assert_eq!(result["table"].as_str(), Some(TABLE));
    let bindings = result["validators"].as_array().unwrap();
    let id = db.validators().id_for_name(expected).unwrap().to_string();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0]["validator_id"].as_str(), Some(id.as_str()));
    for (name, count) in [(expected, 1), (absent, 0)] {
        let row = bounded(db.system_store().load_validator(name))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row["bound_in"].as_array().unwrap().len(), count, "{name}");
    }
}

fn acl_paths() -> [ResourcePath; 3] {
    [
        ResourcePath::database(TARGET),
        ResourcePath::store(TARGET, REPO),
        ResourcePath::table(TARGET, REPO, TABLE),
    ]
}

fn acl_ops(group: u64) -> [[(&'static str, BatchOp); 3]; 3] {
    [
        ddl::res::database(TARGET),
        ddl::res::store(TARGET, REPO),
        ddl::res::table(TARGET, REPO, TABLE),
    ]
    .map(|resource| {
        [
            (
                "chmod",
                ddl::chmod(resource.clone(), 0o750)
                    .hmac(HMAC)
                    .build()
                    .unwrap(),
            ),
            (
                "chown",
                ddl::chown(resource.clone(), OWNER_SYSTEM)
                    .hmac(HMAC)
                    .build()
                    .unwrap(),
            ),
            (
                "chgrp",
                ddl::chgrp(resource, Some(group))
                    .hmac(HMAC)
                    .build()
                    .unwrap(),
            ),
        ]
    })
}

fn initial_meta() -> ResourceMeta {
    ResourceMeta::owned_enforced(Actor::User(7))
}

async fn prepare_target(db: &ShamirDb, root: &Path, suffix: &str) {
    create_with_sentinel(db, root, TARGET, suffix).await;
    bounded(db.create_db(EMPTY)).await.unwrap();
    for path in acl_paths() {
        bounded(db.set_resource_meta(&path, &initial_meta()))
            .await
            .unwrap();
    }
    admin(db, SERVING, bind(OLD_GUARD)).await.unwrap();
}

fn drop_db(name: &str, cascade: bool) -> BatchOp {
    let builder = ddl::drop_db(name).hmac(HMAC);
    let builder = if cascade { builder.cascade() } else { builder };
    builder.build().unwrap()
}

async fn assert_target_refusals(
    db: &ShamirDb,
    group: u64,
    target_id: DatabaseIdentity,
    empty_id: DatabaseIdentity,
    code: &str,
) {
    // Read-only probes run through both a healthy and a fenced serving DB.
    for serving in [SERVING, TARGET] {
        assert_binding(db, serving, OLD_GUARD, NEW_GUARD).await;
        let tree = admin(db, serving, ddl::access_tree().db(TARGET).build())
            .await
            .expect("AccessTree must remain available");
        let databases = tree["access_tree"]["resources"]["children"]
            .as_array()
            .unwrap();
        assert_eq!(databases.len(), 1);
        assert_eq!(databases[0]["name"].as_str(), Some(TARGET));
        assert_eq!(
            databases[0]["children"][0]["children"][0]["name"].as_str(),
            Some(TABLE)
        );
    }

    let noop = admin(db, SERVING, ddl::create_db(TARGET).if_not_exists().build())
        .await
        .expect("CreateDb IF NOT EXISTS remains a no-op");
    assert_eq!(noop["created"].as_bool(), Some(false));
    assert_eq!(noop["existed"].as_bool(), Some(true));
    assert_eq!(identity(db, TARGET).await, target_id);
    assert_eq!(db.schema_migration_refusal_code(target_id), Some(code));
    assert_sentinel(db, TARGET, TARGET).await;
    assert_binding(db, SERVING, OLD_GUARD, NEW_GUARD).await;
    for path in acl_paths() {
        assert_eq!(
            bounded(db.resource_meta(&path)).await.unwrap(),
            initial_meta()
        );
    }

    for (op, case) in [(bind(NEW_GUARD), "bind"), (unbind(), "unbind")] {
        assert_refused(admin(db, SERVING, op).await, code, case);
        assert_binding(db, SERVING, OLD_GUARD, NEW_GUARD).await;
    }

    for (path, ops) in acl_paths().into_iter().zip(acl_ops(group)) {
        for (case, op) in ops {
            assert_refused(
                admin(db, SERVING, op).await,
                code,
                &format!("{case} {path}"),
            );
            assert_eq!(
                bounded(db.resource_meta(&path)).await.unwrap(),
                initial_meta()
            );
        }
    }

    assert_refused(
        admin(db, SERVING, ddl::rename_db(TARGET, RENAMED).build()).await,
        IN_DOUBT,
        "rename",
    );
    assert_absent(db, RENAMED).await;
    assert_eq!(identity(db, TARGET).await, target_id);
    assert_sentinel(db, TARGET, TARGET).await;

    assert_refused(
        admin(db, SERVING, drop_db(EMPTY, false)).await,
        IN_DOUBT,
        "plain drop",
    );
    assert!(db.has_db(EMPTY));
    assert_eq!(identity(db, EMPTY).await, empty_id);
    assert_refused(
        admin(db, SERVING, drop_db(TARGET, false)).await,
        IN_DOUBT,
        "plain drop with children",
    );
    assert_refused(
        admin(db, SERVING, drop_db(TARGET, true)).await,
        IN_DOUBT,
        "cascade drop",
    );
    assert_eq!(identity(db, TARGET).await, target_id);
    assert_sentinel(db, TARGET, TARGET).await;
    assert_sentinel(db, SERVING, SERVING).await;
}

#[test]
fn foreign_admin_mutations_refuse_reserved_live_and_in_doubt_targets() {
    on_big_stack(|| async {
        let watchdog = FlakeGuard::with_watchdog(
            "foreign_admin_mutations_refuse_reserved_live_and_in_doubt_targets",
            Duration::from_secs(600),
        );
        let (dir, db) = bounded(persistent_db()).await;
        create_with_sentinel(&db, dir.path(), SERVING, "fixture").await;
        register_validators(&db).await;
        prepare_target(&db, dir.path(), "fixture").await;
        let group = bounded(db.create_group("target_group")).await.unwrap();
        let target_id = identity(&db, TARGET).await;
        let empty_id = identity(&db, EMPTY).await;

        watchdog.mark("reserved targets");
        let mut target = db.reserve_schema_migration_gate(TARGET).unwrap();
        let mut empty = db.reserve_schema_migration_gate(EMPTY).unwrap();
        let reserved_code = db.schema_migration_refusal_code(target_id);
        assert_eq!(reserved_code, Some(IN_PROGRESS));
        assert_eq!(
            db.schema_migration_refusal_code(empty_id),
            Some(IN_PROGRESS)
        );
        assert_target_refusals(&db, group, target_id, empty_id, IN_PROGRESS).await;

        watchdog.mark("live targets");
        target.hold(1);
        empty.hold(2);
        assert_eq!(db.schema_migration_gate_generation(TARGET), Some(1));
        assert_eq!(db.schema_migration_gate_generation(EMPTY), Some(2));
        assert_target_refusals(&db, group, target_id, empty_id, IN_PROGRESS).await;

        watchdog.mark("in-doubt targets");
        drop(target);
        drop(empty);
        assert_eq!(
            db.schema_migration_gate_in_doubt_generation(TARGET),
            Some(1)
        );
        assert_eq!(db.schema_migration_gate_in_doubt_generation(EMPTY), Some(2));
        assert_target_refusals(&db, group, target_id, empty_id, IN_DOUBT).await;
    });
}

async fn assert_foreign_effects(
    db: &ShamirDb,
    group: u64,
    serving_code: Option<&str>,
    fresh: &str,
) {
    let target_id = identity(db, TARGET).await;
    // Keep DDL and DML in a nested body to exercise the serving gate's full
    // tree walk. It must reject before even the foreign DDL takes effect.
    if let Some(code) = serving_code {
        let mut inner = Batch::new();
        inner.op("foreign_bind", bind(NEW_GUARD));
        inner.op("local_write", insert("must not be inserted"));
        let mut outer = Batch::new();
        outer.sub_batch_no_bind("mixed", inner.build());
        let error = execute(db, SERVING, &outer)
            .await
            .expect_err("the nested local write must refuse the whole mixed batch");
        assert_eq!(error.code(), Some(code), "{error}");
        // The offending leaf sits inside the nested `mixed` batch; the
        // refusal names the enclosing TOP-LEVEL alias, as a pre-fence
        // whole-batch write classification would.
        assert!(matches!(error, BatchError::QueryError { alias, .. } if alias == "mixed"));
        assert_binding(db, SERVING, OLD_GUARD, NEW_GUARD).await;
        assert_sentinel(db, SERVING, SERVING).await;
    }

    // A standalone bind and unbind prove each foreign route is admitted.
    admin(db, SERVING, unbind()).await.unwrap();
    let unbound = admin(
        db,
        SERVING,
        ddl::list_validators(TABLE).db(TARGET).repo(REPO).build(),
    )
    .await
    .unwrap();
    assert!(unbound["validators"].as_array().unwrap().is_empty());
    admin(db, SERVING, bind(NEW_GUARD)).await.unwrap();
    assert_binding(db, SERVING, NEW_GUARD, OLD_GUARD).await;

    // A fresh foreign CREATE through a fenced serving database claims only a
    // namespace slot no fence can own; it must be admitted and durably
    // catalogued (cheap addition to the free-target matrix).
    let created = admin(db, SERVING, ddl::create_db(fresh).build())
        .await
        .unwrap();
    assert_eq!(created["created"].as_bool(), Some(true));
    assert!(db.has_db(fresh));
    assert!(bounded(db.system_store().load_database(fresh))
        .await
        .unwrap()
        .is_some());
    admin(db, SERVING, drop_db(fresh, false)).await.unwrap();
    assert_absent(db, fresh).await;

    // A read plus nested foreign-only DDL must not trip the serving fence.
    let mut inner = Batch::new();
    inner.op(
        "mode",
        ddl::chmod(ddl::res::table(TARGET, REPO, TABLE), 0o711)
            .hmac(HMAC)
            .build()
            .unwrap(),
    );
    let mut outer = Batch::new();
    outer.op("read", Query::from(TABLE));
    outer.sub_batch_no_bind("foreign_only", inner.build());
    execute(db, SERVING, &outer).await.unwrap();
    let table = ResourcePath::table(TARGET, REPO, TABLE);
    assert_eq!(bounded(db.resource_meta(&table)).await.unwrap().mode, 0o711);

    for (path, ops) in acl_paths().into_iter().zip(acl_ops(group)) {
        let mut expected = bounded(db.resource_meta(&path)).await.unwrap();
        for (case, op) in ops {
            admin(db, SERVING, op).await.unwrap();
            match case {
                "chmod" => expected.mode = 0o750,
                "chown" => expected.owner = Actor::System,
                "chgrp" => expected.group = Some(group),
                _ => unreachable!(),
            }
            assert_eq!(
                bounded(db.resource_meta(&path)).await.unwrap(),
                expected,
                "{case} {path}"
            );
        }
    }

    // Leave no name-keyed validator reference when the next matrix row
    // recreates TARGET. Validator effects were already checked above.
    admin(
        db,
        SERVING,
        ddl::unbind_validator(NEW_GUARD, TABLE)
            .db(TARGET)
            .repo(REPO)
            .build(),
    )
    .await
    .unwrap();
    admin(db, SERVING, ddl::rename_db(TARGET, RENAMED).build())
        .await
        .unwrap();
    assert_absent(db, TARGET).await;
    assert_eq!(identity(db, RENAMED).await, target_id);
    assert_sentinel(db, RENAMED, TARGET).await;
    admin(db, SERVING, drop_db(EMPTY, false)).await.unwrap();
    assert_absent(db, EMPTY).await;
    assert_refused(
        admin(db, SERVING, drop_db(RENAMED, false)).await,
        "still_referenced",
        "unfenced plain drop with children reaches the normal integrity check",
    );
    assert_sentinel(db, RENAMED, TARGET).await;
    admin(db, SERVING, drop_db(RENAMED, true)).await.unwrap();
    assert_absent(db, RENAMED).await;
    let store = db.system_store();
    assert!(bounded(store.load_repository_record(RENAMED, REPO))
        .await
        .unwrap()
        .is_none());
    assert!(bounded(store.load_table_record(RENAMED, REPO, TABLE))
        .await
        .unwrap()
        .is_none());
    assert_sentinel(db, SERVING, SERVING).await;
    assert_eq!(
        db.schema_migration_refusal_code(identity(db, SERVING).await),
        serving_code,
        "foreign mutations must not change the serving fence"
    );
}

#[test]
fn foreign_admin_mutations_apply_to_free_targets_even_when_serving_is_fenced() {
    on_big_stack(|| async {
        let watchdog = FlakeGuard::with_watchdog(
            "foreign_admin_mutations_apply_to_free_targets_even_when_serving_is_fenced",
            Duration::from_secs(600),
        );
        let (dir, db) = bounded(persistent_db()).await;
        create_with_sentinel(&db, dir.path(), SERVING, "fixture").await;
        register_validators(&db).await;
        let group = bounded(db.create_group("target_group")).await.unwrap();

        watchdog.mark("free serving database");
        prepare_target(&db, dir.path(), "free").await;
        assert_foreign_effects(&db, group, None, "fresh_free").await;
        watchdog.mark("reserved serving database");
        prepare_target(&db, dir.path(), "reserved").await;
        let mut serving = db.reserve_schema_migration_gate(SERVING).unwrap();
        assert_foreign_effects(&db, group, Some(IN_PROGRESS), "fresh_reserved").await;
        prepare_target(&db, dir.path(), "live").await;
        serving.hold(3);
        assert_foreign_effects(&db, group, Some(IN_PROGRESS), "fresh_live").await;
        serving.mark_succeeded();
        drop(serving);
        assert!(db.schema_migration_gate_generation(SERVING).is_none());
        assert!(db
            .schema_migration_gate_in_doubt_generation(SERVING)
            .is_none());

        watchdog.mark("in-doubt serving database");
        prepare_target(&db, dir.path(), "in-doubt").await;
        let mut serving = db.reserve_schema_migration_gate(SERVING).unwrap();
        serving.hold(4);
        drop(serving);
        assert_eq!(
            db.schema_migration_gate_in_doubt_generation(SERVING),
            Some(4)
        );
        assert_foreign_effects(&db, group, Some(IN_DOUBT), "fresh_in_doubt").await;
    });
}

#[test]
fn same_target_explicit_ddl_refuses_whole_batch_before_first_effect() {
    on_big_stack(|| async {
        let watchdog = FlakeGuard::with_watchdog(
            "same_target_explicit_ddl_refuses_whole_batch_before_first_effect",
            Duration::from_secs(600),
        );
        let (dir, db) = bounded(persistent_db()).await;
        create_with_sentinel(&db, dir.path(), SERVING, "fixture").await;

        for (suffix, code) in [
            ("reserved", IN_PROGRESS),
            ("live", IN_PROGRESS),
            ("in-doubt", IN_DOUBT),
        ] {
            watchdog.mark(suffix);
            let fresh = format!("fresh_{suffix}");
            let renamed = format!("renamed_{suffix}");
            let mut serving = Some(db.reserve_schema_migration_gate(SERVING).unwrap());
            if suffix == "live" || suffix == "in-doubt" {
                serving.as_mut().unwrap().hold(7);
            }
            if suffix == "in-doubt" {
                // An interrupted live runner leaves the in-doubt marker.
                drop(serving.take());
                assert_eq!(
                    db.schema_migration_gate_in_doubt_generation(SERVING),
                    Some(7)
                );
            }

            // Standalone rename/drop of the fenced SERVING database are
            // batch-entry refusals with the serving-state code, NOT the
            // dispatch lifecycle code (`schema_migration_in_doubt`).
            assert_refused(
                admin(&db, SERVING, ddl::rename_db(SERVING, &renamed).build()).await,
                code,
                &format!("standalone rename ({suffix})"),
            );
            assert_absent(&db, &renamed).await;
            assert_refused(
                admin(&db, SERVING, drop_db(SERVING, false)).await,
                code,
                &format!("standalone plain drop ({suffix})"),
            );
            assert!(db.has_db(SERVING));

            // Nontransactional batches: a fresh foreign CREATE first, then
            // ONE same-target leaf each, so every whole-batch oracle reaches
            // its intended leaf. The whole batch must be refused at entry,
            // before the first effect: no C in the live database set or the
            // persisted catalogue, and the fence state is unchanged.
            for (alias, op, case) in [
                (
                    "chmod_b",
                    ddl::chmod(ddl::res::database(SERVING), 0o750)
                        .hmac(HMAC)
                        .build()
                        .unwrap(),
                    "same-target CHMOD",
                ),
                ("drop_b", drop_db(SERVING, false), "same-target DROP"),
            ] {
                let mut batch = Batch::new();
                batch.op("create_c", ddl::create_db(&fresh).build());
                batch.op(alias, op);
                let error = match execute(&db, SERVING, &batch).await {
                    Ok(_) => panic!("{case} ({suffix}): unexpectedly admitted"),
                    Err(error) => error,
                };
                assert_eq!(error.code(), Some(code), "{case} ({suffix}): {error}");
                assert!(
                    matches!(&error, BatchError::QueryError { alias: a, .. } if a == alias),
                    "{case}: {error}"
                );
                assert_absent(&db, &fresh).await;
                assert_sentinel(&db, SERVING, SERVING).await;
            }

            if let Some(guard) = serving.as_mut() {
                if suffix == "live" {
                    guard.mark_succeeded();
                }
            }
            drop(serving);
            assert!(db.schema_migration_gate_generation(SERVING).is_none());
            assert_eq!(
                db.schema_migration_gate_in_doubt_generation(SERVING),
                if suffix == "in-doubt" { Some(7) } else { None }
            );
        }
    });
}

/// Regression for the whole-batch refusal behind the TRANSACTIONAL single
/// entry and the INTERACTIVE-tx execute entry (`tx_begin` + `tx_execute`):
/// both must refuse a same-target offending leaf before any leaf effect, with
/// the serving fence's own code, leaving an earlier leaf in the same request
/// unapplied. Without the `db_tx.rs` gate the interactive batch would run its
/// DDL against the fenced serving database.
#[test]
fn same_target_ddl_refuses_transactional_and_interactive_transactions_before_first_effect() {
    on_big_stack(|| async {
        let watchdog = FlakeGuard::with_watchdog(
            "same_target_ddl_refuses_transactional_and_interactive_transactions_before_first_effect",
            Duration::from_secs(600),
        );
        let (dir, db) = bounded(persistent_db()).await;
        create_with_sentinel(&db, dir.path(), SERVING, "fixture").await;

        for (suffix, code) in [("reserved", IN_PROGRESS), ("in-doubt", IN_DOUBT)] {
            watchdog.mark(suffix);
            let fresh = format!("tx_fresh_{suffix}");
            let mut serving = Some(db.reserve_schema_migration_gate(SERVING).unwrap());
            if suffix == "in-doubt" {
                let mut guard = serving.take().unwrap();
                guard.hold(9);
                drop(guard);
                assert_eq!(
                    db.schema_migration_gate_in_doubt_generation(SERVING),
                    Some(9)
                );
            }

            // Transactional single-call batch: a foreign CREATE first, then
            // one same-target CHMOD leaf — refused at entry with the serving
            // state code; the earlier CREATE must not appear anywhere.
            let mut batch = Batch::new();
            batch.transactional();
            batch.op("data_anchor", Query::from(TABLE));
            batch.op("create_c", ddl::create_db(&fresh).build());
            batch.op(
                "chmod_b",
                ddl::chmod(ddl::res::database(SERVING), 0o750)
                    .hmac(HMAC)
                    .build()
                    .unwrap(),
            );
            let error = execute(&db, SERVING, &batch)
                .await
                .expect_err("transactional batch with a same-target write must refuse");
            assert_eq!(
                error.code(),
                Some(code),
                "transactional ({suffix}): {error}"
            );
            assert!(
                matches!(&error, BatchError::QueryError { alias: a, .. } if a == "chmod_b"),
                "transactional ({suffix}): {error}"
            );
            assert_absent(&db, &fresh).await;
            assert_sentinel(&db, SERVING, SERVING).await;

            // Interactive tx: BEGIN stays available (opening a tx is not a
            // write); tx_execute of the same shape refuses with the same code
            // before any leaf runs, and the tx is dropped uncommitted.
            let (mut tx, guard, _repo) = bounded(db.tx_begin(SERVING, REPO, "snapshot"))
                .await
                .expect("BEGIN must stay available while the fence is held");
            let mut batch = Batch::new();
            batch.op("create_c", ddl::create_db(&fresh).build());
            batch.op("drop_b", drop_db(SERVING, false));
            let request = batch.to_request_via_msgpack().unwrap();
            let error = bounded(db.tx_execute(SERVING, &request, &mut tx))
                .await
                .expect_err("interactive tx execute of a same-target write must refuse");
            assert_eq!(error.code(), Some(code), "interactive ({suffix}): {error}");
            assert!(
                matches!(&error, BatchError::QueryError { alias: a, .. } if a == "drop_b"),
                "interactive ({suffix}): {error}"
            );
            drop(tx);
            drop(guard);
            assert_absent(&db, &fresh).await;
            assert_sentinel(&db, SERVING, SERVING).await;

            if let Some(guard) = serving.as_mut() {
                guard.mark_succeeded();
            }
            drop(serving);
            assert!(db.schema_migration_gate_generation(SERVING).is_none());
        }
        assert_eq!(
            db.schema_migration_gate_in_doubt_generation(SERVING),
            Some(9),
            "the in-doubt marker of the last round must survive the test"
        );
    });
}

#[test]
fn reserved_system_database_name_is_rejected_and_normal_restart_opens() {
    on_big_stack(|| async {
        let watchdog = FlakeGuard::with_watchdog(
            "reserved_system_database_name_is_rejected_and_normal_restart_opens",
            Duration::from_secs(600),
        );
        let (dir, db) = bounded(persistent_db()).await;
        create_with_sentinel(&db, dir.path(), SERVING, "reserved-name").await;

        watchdog.mark("embedded and wire create refuse the reserved name");
        assert!(matches!(
            db.create_db_as("__system__", Actor::System).await,
            Err(crate::DbError::Validation(message)) if message.contains("reserved")
        ));
        let create_error = admin(
            &db,
            SERVING,
            ddl::create_db("__system__").if_not_exists().build(),
        )
        .await
        .expect_err("wire CREATE DATABASE must refuse the reserved name");
        assert!(
            matches!(&create_error, BatchError::QueryError { code: None, .. }),
            "reserved-name CREATE should be a plain validation query error: {create_error}"
        );
        assert!(
            create_error
                .to_string()
                .contains("database name '__system__' is reserved"),
            "{create_error}"
        );
        assert!(!db.has_db("__system__"));
        assert!(bounded(db.system_store().load_database("__system__"))
            .await
            .unwrap()
            .is_none());

        watchdog.mark("embedded and wire rename refuse the reserved destination");
        assert!(matches!(
            db.rename_db(SERVING, "__system__").await,
            Err(crate::DbError::Validation(message)) if message.contains("reserved")
        ));
        let rename_error = admin(&db, SERVING, ddl::rename_db(SERVING, "__system__").build())
            .await
            .expect_err("wire RENAME DATABASE must refuse the reserved destination");
        assert!(
            rename_error.to_string().contains("reserved"),
            "{rename_error}"
        );
        assert!(db.has_db(SERVING));
        assert!(!db.has_db("__system__"));

        watchdog.mark("an ordinary persisted database and repository reopen");
        bounded(db.flush_all_waiting())
            .await
            .expect("clean shutdown before repository reopen");
        drop(db);
        let reopened = bounded(init_waiting_for_locks(dir.path().join("system"))).await;
        assert!(reopened.has_db(SERVING));
        assert_sentinel(&reopened, SERVING, SERVING).await;
    });
}
