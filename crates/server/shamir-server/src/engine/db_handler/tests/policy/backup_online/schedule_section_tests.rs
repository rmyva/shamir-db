//! BackupStatus schedule-section regressions through the real engine admin adapter.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serial_test::serial;
use shamir_connect::common::time::UnixNanos;
use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::engine::table::TableConfig;
use shamir_db::{ShamirDb, SystemStoreConfig};
use shamir_query_builder::batch::Batch;
use shamir_query_builder::ddl::backup_status;
use shamir_query_builder::write;
use shamir_types::types::common::new_map;
use shamir_types::types::value::QueryValue;
use tokio_util::sync::CancellationToken;

use crate::boot::config::diagnostics::backup_online::metrics::{
    export_last_success_seconds_for_test, set_export_last_success_for_test, ScheduleInfo,
    ScheduleState, ScheduledLastRun,
};
use crate::boot::config::diagnostics::backup_online::schedule::runner::spawn_for_test;
use crate::boot::config::diagnostics::backup_online::OnlineBackupRegistry;
use crate::config::{BackupRetentionConfig, ResolvedSchedule};
use crate::db_handler::handler::{DbResponse, ShamirDbHandler};
use crate::sync::durability::backup::online::progress::BackupStage;
use crate::sync::durability::backup::online::protocol::OnlineBackupError;
use crate::version::CURRENT_QUERY_LANG_VERSION;

const WAIT: Duration = Duration::from_secs(120);
const OVERALL: Duration = Duration::from_secs(170);
const OWNER: &str = "schedule-section-test";

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now()).min(WAIT)
}

async fn fixture(root: &Path) -> (Arc<ShamirDb>, OnlineBackupRegistry) {
    let db = ShamirDb::init(SystemStoreConfig::Fjall(root.join("system")))
        .await
        .expect("init engine");
    db.create_db("app").await.expect("create app");
    db.add_repo(
        "app",
        RepoConfig::new("main", BoxRepoFactory::fjall_raw(root.join("repos/main")))
            .add_table(TableConfig::new("rows")),
    )
    .await
    .expect("add repo");
    let mut batch = Batch::new();
    for (alias, n) in [("a", 1), ("b", 2)] {
        let mut row = new_map();
        row.insert("n".into(), QueryValue::Int(n));
        batch.op(
            alias,
            write::Insert::with_repo("main", "rows").row(QueryValue::Map(row)),
        );
    }
    db.execute("app", &batch.build())
        .await
        .expect("insert rows");
    db.flush_all_waiting().await.expect("flush rows");
    let db = Arc::new(db);
    let registry = OnlineBackupRegistry::new(
        Arc::clone(&db),
        root.to_path_buf(),
        tokio::runtime::Handle::current(),
    );
    (db, registry)
}

#[async_trait]
impl shamir_db::OnlineBackupAdminPort for CountingPort {
    async fn start(&self, to: String) -> Result<QueryValue, shamir_db::OnlineBackupAdminError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        shamir_db::OnlineBackupAdminPort::start(&*self.inner, to).await
    }

    async fn status(&self) -> Result<QueryValue, shamir_db::OnlineBackupAdminError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        shamir_db::OnlineBackupAdminPort::status(&*self.inner).await
    }

    async fn cancel(&self) -> Result<QueryValue, shamir_db::OnlineBackupAdminError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        shamir_db::OnlineBackupAdminPort::cancel(&*self.inner).await
    }
}

struct CountingPort {
    inner: Arc<OnlineBackupRegistry>,
    calls: AtomicUsize,
}

async fn handler(root: &Path, port: Arc<dyn shamir_db::OnlineBackupAdminPort>) -> ShamirDbHandler {
    let db = ShamirDb::init(SystemStoreConfig::Fjall(root.join("handler-system")))
        .await
        .expect("init handler engine");
    db.create_db("app").await.expect("create handler app");
    db.set_online_backup_admin_port(port);
    ShamirDbHandler::new(Arc::new(db))
}

fn session(superuser: bool) -> Session {
    Session::new(
        [0xAB; 16],
        "alice".into(),
        SessionPermissions::new(superuser, false, vec!["read_write".into()]),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        [0; 32],
        UnixNanos::now().as_u64(),
    )
}

async fn status(
    handler: &ShamirDbHandler,
    superuser: bool,
    deadline: Instant,
) -> std::result::Result<QueryValue, String> {
    let conn = ConnectionServices::without_push(0);
    let response = tokio::time::timeout(
        remaining(deadline),
        handler.execute(
            &session(superuser),
            CURRENT_QUERY_LANG_VERSION,
            "app",
            {
                let mut batch = Batch::new();
                batch.op("status", backup_status());
                batch.build()
            },
            &conn,
        ),
    )
    .await
    .expect("bounded status dispatch");
    match response {
        DbResponse::Batch { mut response } => {
            let mut result = response
                .results
                .shift_remove("status")
                .expect("status alias");
            match result.records.pop() {
                Some(shamir_query_types::read::QueryRecord::Direct(value)) => Ok(value),
                other => panic!("expected direct record: {other:?}"),
            }
        }
        DbResponse::Error { code, .. } => Err(code),
        other => panic!("unexpected response: {other:?}"),
    }
}

fn schedule_info() -> ScheduleInfo {
    ScheduleInfo {
        interval_secs: 731,
        window: Some("weekday-window".into()),
        keep_last: 13,
        run_on_start: true,
    }
}

/// The complete adapter map kills omitted, renamed, mistyped, or fabricated fields; parent had no reply assertions for this section.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn superuser_reply_through_real_adapter_lists_schedule_keys() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let deadline = Instant::now() + OVERALL;
    let (_db, registry) = tokio::time::timeout(remaining(deadline), fixture(tmp.path()))
        .await
        .expect("bounded fixture");
    let registry = Arc::new(registry);
    // The section is served from THIS registry's own schedule cell: seed it
    // there, not in any process global.
    let schedule_state = registry.schedule_cell();
    let generation = schedule_state.install_schedule_info(schedule_info());
    schedule_state.note_schedule_last_run(ScheduledLastRun {
        started_at_unix_ns: 1_701_234_567_890_123_456,
        result: "completed",
        stage: "Done".into(),
        reason: Some("reported-reason".into()),
    });
    schedule_state.note_next_run_unix_ns(generation, 1_701_234_999);
    let h = tokio::time::timeout(remaining(deadline), handler(tmp.path(), registry))
        .await
        .expect("bounded handler fixture");
    let value = status(&h, true, deadline).await.expect("superuser status");
    let QueryValue::Map(reply) = value else {
        panic!("status reply map expected")
    };
    let Some(QueryValue::Map(schedule)) = reply.get("schedule") else {
        panic!("schedule map expected")
    };
    for (key, expected) in [
        ("enabled", QueryValue::Bool(true)),
        ("interval_seconds", QueryValue::Int(731)),
        ("window", QueryValue::Str("weekday-window".into())),
        ("keep_last", QueryValue::Int(13)),
        ("run_on_start", QueryValue::Bool(true)),
        ("next_run_unix_ns", QueryValue::Str("1701234999".into())),
    ] {
        assert_eq!(schedule.get(key), Some(&expected), "schedule.{key}");
    }
    let Some(QueryValue::Map(last)) = schedule.get("last_run") else {
        panic!("last_run map expected")
    };
    for (key, expected) in [
        (
            "started_at_unix_ns",
            QueryValue::Str("1701234567890123456".into()),
        ),
        ("result", QueryValue::Str("completed".into())),
        ("stage", QueryValue::Str("Done".into())),
        ("reason", QueryValue::Str("reported-reason".into())),
    ] {
        assert_eq!(last.get(key), Some(&expected), "last_run.{key}");
    }
    let Some(QueryValue::Map(runs)) = schedule.get("runs") else {
        panic!("runs map expected")
    };
    for key in [
        "scheduled_started",
        "scheduled_skipped_busy",
        "scheduled_skipped_window",
        "scheduled_failed",
        "scheduled_completed",
        "failure_streak",
        "retention_deleted",
        "retention_failed",
    ] {
        assert!(
            matches!(runs.get(key), Some(QueryValue::Int(_))),
            "runs.{key} must be Int"
        );
    }
    assert_eq!(schedule.len(), 8, "required schedule keys only");
    assert_eq!(last.len(), 4);
    assert_eq!(runs.len(), 8);
}

/// A configured scheduler's section disappears after its joined shutdown; kills a missing task-exit guard that leaves schedule cells installed on parent/round-2.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduler_exit_removes_the_schedule_section() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let deadline = Instant::now() + OVERALL;
    let (_db, registry) = tokio::time::timeout(remaining(deadline), fixture(tmp.path()))
        .await
        .expect("fixture");
    let registry = Arc::new(registry);
    let h = tokio::time::timeout(
        remaining(deadline),
        handler(
            tmp.path(),
            Arc::clone(&registry) as Arc<dyn shamir_db::OnlineBackupAdminPort>,
        ),
    )
    .await
    .expect("handler");
    let shutdown = CancellationToken::new();
    let task = spawn_for_test(
        ResolvedSchedule {
            destination: tmp.path().join("unused-backup"),
            interval: Duration::from_secs(3600),
            window: None,
            run_on_start: false,
            retention: BackupRetentionConfig::default(),
        },
        registry,
        OWNER.into(),
        shutdown.clone(),
        Duration::from_secs(3600),
        |_| {},
    );
    let QueryValue::Map(reply) = status(&h, true, deadline).await.expect("status") else {
        panic!("reply map")
    };
    let QueryValue::Map(schedule) = reply.get("schedule").expect("owned schedule") else {
        panic!("schedule map")
    };
    assert_eq!(
        schedule.get("interval_seconds"),
        Some(&QueryValue::Int(3600))
    );
    shutdown.cancel();
    tokio::time::timeout(WAIT, task)
        .await
        .expect("scheduler exit handshake")
        .expect("scheduler joins");
    let QueryValue::Map(reply) = status(&h, true, deadline).await.expect("status") else {
        panic!("reply map")
    };
    assert!(!reply.contains_key("schedule"));
}

/// Newer-generation ownership survives an older scheduler exit; kills unconditional clear on any exit, which erases B's section when A exits on parent/round-2.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_scheduler_generations_are_independent() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let deadline = Instant::now() + OVERALL;
    let (_db, registry) = tokio::time::timeout(remaining(deadline), fixture(tmp.path()))
        .await
        .expect("fixture");
    let registry = Arc::new(registry);
    let h = tokio::time::timeout(
        remaining(deadline),
        handler(
            tmp.path(),
            Arc::clone(&registry) as Arc<dyn shamir_db::OnlineBackupAdminPort>,
        ),
    )
    .await
    .expect("handler");
    let spawn = |interval_secs| ResolvedSchedule {
        destination: tmp.path().join("unused-backup"),
        interval: Duration::from_secs(interval_secs),
        window: None,
        run_on_start: false,
        retention: BackupRetentionConfig::default(),
    };
    let shutdown_a = CancellationToken::new();
    let task_a = spawn_for_test(
        spawn(731),
        Arc::clone(&registry),
        OWNER.into(),
        shutdown_a.clone(),
        Duration::from_secs(731),
        |_| {},
    );
    let shutdown_b = CancellationToken::new();
    let task_b = spawn_for_test(
        spawn(900),
        registry,
        OWNER.into(),
        shutdown_b.clone(),
        Duration::from_secs(900),
        |_| {},
    );
    let QueryValue::Map(reply) = status(&h, true, deadline).await.expect("status") else {
        panic!("reply map")
    };
    let QueryValue::Map(schedule) = reply.get("schedule").expect("schedule") else {
        panic!("schedule map")
    };
    assert_eq!(
        schedule.get("interval_seconds"),
        Some(&QueryValue::Int(900))
    );
    shutdown_a.cancel();
    tokio::time::timeout(WAIT, task_a)
        .await
        .expect("A exits")
        .expect("A joins");
    let QueryValue::Map(reply) = status(&h, true, deadline).await.expect("status") else {
        panic!("reply map")
    };
    let QueryValue::Map(schedule) = reply.get("schedule").expect("B schedule remains") else {
        panic!("schedule map")
    };
    assert_eq!(
        schedule.get("interval_seconds"),
        Some(&QueryValue::Int(900))
    );
    shutdown_b.cancel();
    tokio::time::timeout(WAIT, task_b)
        .await
        .expect("B exits")
        .expect("B joins");
    let QueryValue::Map(reply) = status(&h, true, deadline).await.expect("status") else {
        panic!("reply map")
    };
    assert!(!reply.contains_key("schedule"));
}

/// Abort drops the scheduler future without normal body completion; kills a body-end-only clear on parent/round-2, which cannot remove the key on task abort.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborting_the_scheduler_task_removes_the_section() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let deadline = Instant::now() + OVERALL;
    let (_db, registry) = tokio::time::timeout(remaining(deadline), fixture(tmp.path()))
        .await
        .expect("fixture");
    let registry = Arc::new(registry);
    let h = tokio::time::timeout(
        remaining(deadline),
        handler(
            tmp.path(),
            Arc::clone(&registry) as Arc<dyn shamir_db::OnlineBackupAdminPort>,
        ),
    )
    .await
    .expect("handler");
    let shutdown = CancellationToken::new();
    let task = spawn_for_test(
        ResolvedSchedule {
            destination: tmp.path().join("unused-backup"),
            interval: Duration::from_secs(3600),
            window: None,
            run_on_start: false,
            retention: BackupRetentionConfig::default(),
        },
        registry,
        OWNER.into(),
        shutdown,
        Duration::from_secs(3600),
        |_| {},
    );
    let QueryValue::Map(reply) = status(&h, true, deadline).await.expect("status") else {
        panic!("reply map")
    };
    assert!(reply.contains_key("schedule"));
    task.abort();
    let joined = tokio::time::timeout(WAIT, task)
        .await
        .expect("abort join handshake");
    assert!(joined.expect_err("abort must cancel task").is_cancelled());
    let QueryValue::Map(reply) = status(&h, true, deadline).await.expect("status") else {
        panic!("reply map")
    };
    assert!(!reply.contains_key("schedule"));
}

/// Scheduled failure publication is stable through a successful manual run; kills early final publication and cancellation-as-result mutants not covered on parent/round-2.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_scheduled_run_publishes_stable_last_run_and_survives_a_manual_run() {
    let root = tempfile::tempdir().expect("data root");
    let manual_dest = tempfile::tempdir().expect("manual destination");
    let deadline = Instant::now() + OVERALL;
    let (_db, registry) = tokio::time::timeout(remaining(deadline), fixture(root.path()))
        .await
        .expect("bounded engine fixture");
    let registry = Arc::new(registry);
    let h = tokio::time::timeout(
        remaining(deadline),
        handler(
            root.path(),
            Arc::clone(&registry) as Arc<dyn shamir_db::OnlineBackupAdminPort>,
        ),
    )
    .await
    .expect("bounded handler");
    let schedule = ResolvedSchedule {
        destination: root.path().join("missing-backup"),
        interval: Duration::from_secs(3600),
        window: None,
        run_on_start: true,
        retention: BackupRetentionConfig::default(),
    };
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let shutdown = CancellationToken::new();
    let task = spawn_for_test(
        schedule,
        Arc::clone(&registry),
        OWNER.into(),
        shutdown.clone(),
        Duration::from_secs(3600),
        move |ok| {
            let _ = tx.send(ok);
        },
    );
    let terminal = tokio::time::timeout(remaining(deadline), rx.recv())
        .await
        .expect("bounded terminal signal")
        .expect("terminal channel");
    assert!(
        !terminal,
        "invalid destination must fail scheduled preflight"
    );
    // The outcome callback precedes retention and the final record
    // publication; the registry has no completion channel for the final
    // publication, so wait (bounded) until the run left `running`.
    tokio::time::timeout(remaining(deadline), async {
        while registry.status().running {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("scheduled run final publication within bound");
    let admitted = registry.status();
    let admitted_started = admitted.started_at_unix_ns.to_string();
    let error = admitted.last_error.clone().expect("registry error");
    let (stage, registry_reason) = error.split_once(": ").expect("stage and stable reason");
    assert_eq!(stage, "preparing");
    assert!(!registry_reason.contains('/') && !registry_reason.contains('\\'));
    let expected_last = vec![
        (
            "started_at_unix_ns".to_string(),
            QueryValue::Str(admitted_started),
        ),
        ("result".to_string(), QueryValue::Str("failed".into())),
        ("stage".to_string(), QueryValue::Str("preparing".into())),
        (
            "reason".to_string(),
            QueryValue::Str(registry_reason.to_string()),
        ),
    ];
    let value = status(&h, true, deadline)
        .await
        .expect("status after scheduled run");
    let QueryValue::Map(reply) = value else {
        panic!("map")
    };
    let Some(QueryValue::Map(schedule)) = reply.get("schedule") else {
        panic!("schedule installed")
    };
    let Some(QueryValue::Map(last)) = schedule.get("last_run") else {
        panic!("last run")
    };
    assert_eq!(last.len(), 4);
    for (key, expected) in &expected_last {
        assert_eq!(last.get(key), Some(expected), "last_run.{key}");
    }
    assert_eq!(last.len(), 4);
    registry
        .start(manual_dest.path().to_path_buf())
        .expect("manual run start");
    // The registry exposes no manual-run completion channel, so this bounded status poll is the only available completion signal.
    tokio::time::timeout(remaining(deadline), async {
        while registry.status().running {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("manual copy completes within bound");
    let after = registry.status();
    assert!(!after.running);
    assert!(
        after.dest_dir.is_some(),
        "manual copy must have published a dest_dir"
    );
    assert!(after.last_error.is_none(), "manual run must have succeeded");
    assert!(after.dest_dir.expect("destination").exists());
    let value = status(&h, true, deadline)
        .await
        .expect("status after manual run");
    let QueryValue::Map(reply) = value else {
        panic!("map")
    };
    let Some(QueryValue::Map(schedule)) = reply.get("schedule") else {
        panic!("schedule")
    };
    let Some(QueryValue::Map(last)) = schedule.get("last_run") else {
        panic!("last run")
    };
    assert_eq!(last.len(), 4);
    for (key, expected) in &expected_last {
        assert_eq!(last.get(key), Some(expected), "last_run.{key}");
    }
    assert_eq!(last.len(), 4);
    // Cleanup: the scheduler is still alive here (its exit clears the section).
    shutdown.cancel();
    tokio::time::timeout(WAIT, task)
        .await
        .expect("scheduler completion handshake")
        .expect("scheduler joins");
}

/// Status must leave the success stamp unchanged; kills schedule reads routed through scrape-time clamping (the parent defect).
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_read_leaves_the_exported_success_stamp_untouched() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let deadline = Instant::now() + OVERALL;
    let (_db, registry) = tokio::time::timeout(remaining(deadline), fixture(tmp.path()))
        .await
        .expect("bounded fixture");
    let registry = Arc::new(registry);
    registry
        .schedule_cell()
        .install_schedule_info(schedule_info());
    let h = tokio::time::timeout(remaining(deadline), handler(tmp.path(), registry))
        .await
        .expect("bounded handler");
    let far_future = 4_000_000_000;
    set_export_last_success_for_test(far_future);
    let value = status(&h, true, deadline).await.expect("status");
    let QueryValue::Map(reply) = value else {
        panic!("reply map")
    };
    assert!(reply.contains_key("schedule"));
    assert_eq!(
        export_last_success_seconds_for_test(),
        far_future,
        "one status read must leave the exported success stamp untouched"
    );
    set_export_last_success_for_test(0);
}

/// Denial occurs before any admin-port call; kills permission-check-after-dispatch mutants absent from parent/round-2.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_superuser_is_refused_with_a_configured_schedule() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let deadline = Instant::now() + OVERALL;
    let (_db, registry) = tokio::time::timeout(remaining(deadline), fixture(tmp.path()))
        .await
        .expect("bounded fixture");
    let registry = Arc::new(registry);
    registry
        .schedule_cell()
        .install_schedule_info(schedule_info());
    let port = Arc::new(CountingPort {
        inner: registry,
        calls: AtomicUsize::new(0),
    });
    let h = tokio::time::timeout(remaining(deadline), handler(tmp.path(), port.clone()))
        .await
        .expect("bounded handler");
    match status(&h, false, deadline).await {
        Err(code) => assert_eq!(code, "permission_denied"),
        Ok(_) => panic!("non-superuser must be refused"),
    }
    assert_eq!(port.calls.load(Ordering::SeqCst), 0);
}

/// Typed cancellation maps to cancelled rather than generic failure; kills cancel-requested labeling mutants absent from parent/round-2.
#[serial]
#[test]
fn typed_cancelled_outcome_is_labeled_cancelled() {
    let schedule = ScheduleState::default();
    schedule.install_schedule_info(ScheduleInfo {
        interval_secs: 60,
        window: None,
        keep_last: 7,
        run_on_start: true,
    });
    let error = OnlineBackupError::Cancelled {
        stage: BackupStage::Preparing,
        cleanup: None,
    };
    schedule.note_scheduled_run_outcome(&Err(error), 42);
    let run = schedule
        .snapshot()
        .expect("schedule installed")
        .last_run
        .expect("cancel outcome");
    assert_eq!(run.result, "cancelled");
    assert_eq!(run.stage, "preparing");
}
