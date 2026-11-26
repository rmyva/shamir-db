use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{mpsc, Arc, Mutex};
use std::task::Context;
use std::time::Duration;

use futures_util::task::{waker, ArcWake};
use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::common::{crypto::StoredKey, kdf_params::KdfParams};
use shamir_connect::server::admin::UserDirectory;
use shamir_connect::server::user_record::UserRecord;
use shamir_connect::server::{Session, SessionPermissions, SessionStore};
use shamir_db::access::{principal64, Actor, ResourcePath};
use shamir_db::engine::db_instance::logical_export::MaintenanceGate;
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::{ShamirDb, SystemStoreConfig};
use shamir_query_builder::{batch::Batch, ddl, doc, write};
use shamir_query_types::admin::{ExportFailure, ExportPhase, ExportStartReceipt, StartExportOp};
use shamir_query_types::batch::BatchOp;
use shamir_server_security::user_directory::FjallUserDirectory;
use shamir_sync::replication_seed::verify_dump_file;
use shamir_test_diag::FlakeGuard;
use shamir_types::access::ResourceMeta;
use shamir_types::types::{common::new_map, value::QueryValue};
use tempfile::TempDir;
use tokio::runtime::{Builder, Handle};
use tokio::sync::{oneshot, Notify};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use super::super::service::WorkerControls;
use super::super::{ExportRegistry, ExportRoot, ExportRuntime, OnlineExport, ServiceError};
use crate::boot::config::diagnostics::logical::logical_dump::LIVE_STAGE_PREFIX;

const BOUND: Duration = Duration::from_secs(120);
const DB: &str = "app";

fn on_big_stack<F: Future<Output = ()>>(body: impl FnOnce() -> F + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            Builder::new_current_thread()
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async { tokio::time::timeout(BOUND, body()).await.unwrap() });
        })
        .unwrap()
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

struct Fixture {
    service: OnlineExport,
    owner: Arc<Session>,
    sessions: Arc<SessionStore>,
    other: Arc<Session>,
    shutdown: CancellationToken,
    connection: CancellationToken,
    gate: MaintenanceGate,
    db: Arc<ShamirDb>,
    output: PathBuf,
    _temp: TempDir,
}

impl Fixture {
    async fn new(controls: WorkerControls) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("data");
        fs::create_dir(&data).unwrap();
        let output = temp.path().join("exports");
        let root = Arc::new(ExportRoot::open(&output, &data).unwrap());
        let directory = Arc::new(FjallUserDirectory::open(data.join("users")).unwrap());
        let sessions = Arc::new(SessionStore::new());
        let owner = session(&directory, &sessions, "alice", 1, true);
        let other = session(&directory, &sessions, "bob", 2, false);
        let actor = Actor::User(principal64(owner.user_id));
        let db = Arc::new(
            ShamirDb::init(SystemStoreConfig::Fjall(data.join("shamir_db_meta.redb")))
                .await
                .unwrap(),
        );
        // Persist the actual root setting envelope used by the dump schema.
        let mut root_meta = QueryValue::Map(new_map());
        ResourceMeta::owned_enforced(actor.clone()).inject_into(&mut root_meta);
        db.system_store()
            .save_setting("root_meta", &root_meta)
            .await
            .unwrap();
        db.create_db_as(DB, actor.clone()).await.unwrap();
        db.add_repo_as(
            DB,
            RepoConfig::new("main", BoxRepoFactory::fjall_raw(data.join("main"))),
            actor.clone(),
        )
        .await
        .unwrap();
        db.add_table_as(DB, "main", "rows", false, actor)
            .await
            .unwrap();
        insert(&db, 1).await;
        db.flush_all_waiting().await.unwrap();
        let shutdown = CancellationToken::new();
        let gate = MaintenanceGate::default();
        let service = OnlineExport::new(
            Arc::new(ExportRegistry::default()),
            root,
            gate.clone(),
            Arc::clone(&db),
            directory,
            None,
            ExportRuntime {
                runtime: Handle::current(),
                shutdown: shutdown.clone(),
                sessions: Some(Arc::clone(&sessions)),
            },
        )
        .enable_for_test(controls);
        Self {
            service,
            owner,
            sessions,
            other,
            shutdown,
            connection: CancellationToken::new(),
            gate,
            db,
            output,
            _temp: temp,
        }
    }

    async fn start(&self) -> Result<ExportStartReceipt, ServiceError> {
        self.service
            .start(&self.owner, options(), self.connection.clone())
            .await
    }

    fn stages(&self) -> Vec<PathBuf> {
        self.outputs()
            .into_iter()
            .filter(|entry| {
                entry
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(LIVE_STAGE_PREFIX)
            })
            .collect()
    }

    fn outputs(&self) -> Vec<PathBuf> {
        fs::read_dir(&self.output)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|entry| entry.file_name().unwrap() != ".shamir-export-root.lock")
            .collect()
    }
}

fn session(
    directory: &FjallUserDirectory,
    sessions: &SessionStore,
    name: &str,
    tag: u8,
    admin: bool,
) -> Arc<Session> {
    let user = directory
        .insert(
            name.into(),
            UserRecord {
                salt: [0x61; 16],
                stored_key: StoredKey([0x72; 32]),
                server_key: Zeroizing::new([0x73; 32]),
                kdf_params: KdfParams::DEFAULT,
                tickets_invalid_before_ns: 0,
            },
        )
        .unwrap();
    if admin {
        directory.set_superuser(name, true, 0).unwrap();
    }
    sessions.insert(
        [tag; 32],
        Session::new(
            user,
            name.into(),
            SessionPermissions::new(admin, false, vec![]),
            TransportKind::Tcp,
            BindingMode::TlsExporter,
            [0; 32],
            0,
        ),
    )
}

fn options() -> StartExportOp {
    let BatchOp::StartExport(options) = ddl::start_export(DB).repo("main").table("rows").build()
    else {
        unreachable!("start_export builder returned another op");
    };
    options
}

async fn insert(db: &ShamirDb, value: i64) {
    let mut batch = Batch::new();
    batch.op(
        "put",
        write::Insert::with_repo("main", "rows").row(doc! { "value" => value }),
    );
    let result = db.execute(DB, &batch.build()).await.unwrap();
    assert!(!result.results.get("put").unwrap().skipped);
}

struct WriterWake(Notify);

impl ArcWake for WriterWake {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.notify_one();
    }
}

fn arm_writer<F: Future>(writer: Pin<&mut F>) -> Arc<WriterWake> {
    let wake = Arc::new(WriterWake(Notify::new()));
    let waker = waker(Arc::clone(&wake));
    assert!(writer.poll(&mut Context::from_waker(&waker)).is_pending());
    wake
}

#[derive(Clone, Copy, Debug)]
enum Exit {
    Success,
    VerificationError,
    Cancel,
    Shutdown,
    ConnectionClosed,
    SessionEvicted,
    Panic,
}

async fn staged_exit(exit: Exit) {
    let diag = FlakeGuard::new(format!("online export {exit:?}"));
    let (arrived_tx, arrived_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let channels = Mutex::new(Some((arrived_tx, release_rx)));
    let verifying = Arc::new(move |cancel: &CancellationToken| {
        let (arrived, release) = channels.lock().unwrap().take().unwrap();
        arrived.send(cancel.clone()).unwrap();
        // Synchronous observer on the blocking worker; a failed test drops
        // the sender and cannot strand this worker during runtime teardown.
        release.recv_timeout(BOUND).unwrap();
        assert!(
            !matches!(exit, Exit::Panic),
            "intentional export worker unwind"
        );
    });
    let fixture = Fixture::new(WorkerControls {
        verifying: Some(verifying),
        ..Default::default()
    })
    .await;
    let receipt = fixture.start().await.unwrap();
    let worker_cancel = arrived_rx.await.unwrap();
    diag.mark("verifying after stage creation");
    let stages = fixture.stages();
    assert_eq!(stages.len(), 1);
    let staged_dump = stages[0].join("dump");
    assert!(staged_dump.is_file());
    let status = fixture.service.status_for(&fixture.owner, DB).unwrap();
    assert_eq!(status.export_id, receipt.export_id);
    assert_eq!(status.phase, ExportPhase::Verifying);
    assert!(status.sections > 0 && status.items > 0 && status.bytes > 0);
    assert!(status.artifact_name.is_none());
    assert!(fixture.service.status_for(&fixture.other, DB).is_none());
    assert!(fixture
        .service
        .status_for(&fixture.owner, "wrong")
        .is_none());
    assert_eq!(
        fixture.start().await.unwrap_err(),
        ServiceError::AlreadyRunning
    );

    // A dedicated waker proves thaw notified the already parked writer.
    let mut writer = std::pin::pin!(async {
        let _admission = fixture.gate.wait_for_mutation().await;
        insert(&fixture.db, 2).await;
    });
    let wake = arm_writer(writer.as_mut());
    match exit {
        Exit::Success | Exit::Panic => {}
        Exit::VerificationError => fs::write(&staged_dump, b"corrupt").unwrap(),
        Exit::Cancel => assert!(
            fixture
                .service
                .cancel_for(&fixture.owner, DB)
                .unwrap()
                .cancelled
        ),
        Exit::Shutdown => fixture.shutdown.cancel(),
        Exit::ConnectionClosed => fixture.connection.cancel(),
        Exit::SessionEvicted => {
            assert!(fixture.sessions.remove(&fixture.owner.session_id).is_some());
        }
    }
    if matches!(exit, Exit::ConnectionClosed | Exit::SessionEvicted) {
        // Keep the worker parked until the supervisor forwards the signal.
        // This distinguishes eviction polling from the publication recheck.
        worker_cancel.cancelled().await;
    }
    release_tx.send(()).unwrap();
    wake.0.notified().await;
    writer.await;
    assert!(fixture.service.wait_idle().await);
    // The gate's per-instance transition counters: the admitted freeze left
    // the instance with exactly ONE rise and ONE thaw, whatever the exit
    // path was (success, verification error, cancel, shutdown, disconnect,
    // eviction, panic). Mutant: a double raise or a missed thaw leaves a
    // count that is not exactly (1, 1).
    assert_eq!(
        fixture.gate.freeze_transition_counts(),
        (1, 1),
        "one admitted freeze exit must leave exactly one rise and one thaw"
    );
    diag.mark("writer woke and worker drained");
    let status = fixture.service.status_for(&fixture.owner, DB).unwrap();
    let expected = match exit {
        Exit::Success => (ExportPhase::Done, None),
        Exit::Cancel | Exit::Shutdown | Exit::ConnectionClosed | Exit::SessionEvicted => {
            (ExportPhase::Cancelled, None)
        }
        Exit::VerificationError => (ExportPhase::Failed, Some(ExportFailure::Verification)),
        Exit::Panic => (ExportPhase::Failed, Some(ExportFailure::Internal)),
    };
    assert_eq!((status.phase, status.failure), expected);
    assert!(fixture.stages().is_empty());
    if matches!(exit, Exit::Success) {
        let artifact = fixture.output.join(status.artifact_name.unwrap());
        assert_eq!(fixture.outputs(), vec![artifact.clone()]);
        let report = verify_dump_file(&artifact).unwrap();
        assert_eq!(
            (status.sections, status.items, status.bytes),
            (report.sections, report.items, report.bytes)
        );
        let bytes = fs::read(artifact).unwrap();
        for secret in [&[0x61; 16][..], &[0x72; 32], &[0x73; 32]] {
            assert!(!bytes.windows(secret.len()).any(|window| window == secret));
        }
    } else {
        assert!(status.artifact_name.is_none());
        assert!(fixture.outputs().is_empty());
    }
}

#[test]
fn verifies_and_publishes_then_wakes_writer() {
    on_big_stack(|| staged_exit(Exit::Success));
}

#[test]
/// Mutant: skip thaw on the verification-failure exit from `ExportJob::drive`; `staged_exit` has a parked mutator and its wake handshake fails.
fn verification_error_cleans_stage_and_wakes_writer() {
    on_big_stack(|| staged_exit(Exit::VerificationError));
}

#[test]
/// Mutant: skip thaw on the `Cancelled` exit from `ExportJob::drive`; the mutator parked before cancellation never wakes and the bounded test fails.
fn cancel_after_stage_cleans_stage_and_wakes_writer() {
    on_big_stack(|| staged_exit(Exit::Cancel));
}

#[test]
/// Mutant: skip thaw when shutdown stops a running `ExportJob`; the already-parked writer is not woken and this test fails.
fn shutdown_after_stage_cleans_stage_and_wakes_writer() {
    on_big_stack(|| staged_exit(Exit::Shutdown));
}

#[test]
fn disconnect_after_stage_cleans_stage_and_wakes_writer() {
    on_big_stack(|| staged_exit(Exit::ConnectionClosed));
}

#[test]
fn session_eviction_after_stage_cleans_stage_and_wakes_writer() {
    on_big_stack(|| staged_exit(Exit::SessionEvicted));
}

#[test]
fn panic_after_stage_cleans_stage_and_wakes_writer() {
    on_big_stack(|| staged_exit(Exit::Panic));
}

#[test]
fn small_deadline_thaws_drain_even_while_original_writer_is_still_admitted() {
    on_big_stack(|| async {
        let arrived = Arc::new(Notify::new());
        let budget = Duration::from_secs(1);
        let fixture = Fixture::new(WorkerControls {
            budget: Some(budget),
            drain_pending: Some(Arc::clone(&arrived)),
            ..Default::default()
        })
        .await;
        let admitted = fixture.gate.enter_mutation().unwrap();
        // Only freeze/drain uses virtual time; setup runs with real clocks.
        tokio::time::pause();
        let receipt = fixture.start().await.unwrap();
        arrived.notified().await;
        let status = fixture.service.status_for(&fixture.owner, DB).unwrap();
        assert_eq!(status.export_id, receipt.export_id);
        assert_eq!(status.phase, ExportPhase::WaitingForGate);
        let mut writer = std::pin::pin!(fixture.gate.wait_for_mutation());
        let wake = arm_writer(writer.as_mut());
        tokio::time::advance(budget + Duration::from_millis(1)).await;
        // Restore the real watchdog before awaiting the worker on the blocking pool.
        tokio::time::resume();
        wake.0.notified().await;
        drop(writer.await);
        assert!(fixture.service.wait_idle().await);
        drop(admitted);
        let status = fixture.service.status_for(&fixture.owner, DB).unwrap();
        assert_eq!(
            (status.phase, status.failure),
            (ExportPhase::Failed, Some(ExportFailure::TimedOut))
        );
        assert!(status.artifact_name.is_none());
        assert!(fixture.outputs().is_empty());
    });
}

#[test]
/// Mutant: raise the freeze while returning the denied `start`; the concurrently polled mutator is refused instead of admitted, making the admission assertion fail.
fn directory_and_scope_denials_precede_freeze() {
    on_big_stack(|| async {
        let fixture = Fixture::new(WorkerControls::default()).await;
        let (denied, admission) = tokio::join!(
            fixture
                .service
                .start(&fixture.other, options(), CancellationToken::new()),
            async { fixture.gate.enter_mutation() },
        );
        assert_eq!(denied.unwrap_err(), ServiceError::Denied);
        assert!(
            admission.is_ok(),
            "mutator must be admitted concurrently with refusal"
        );
        // Counterfactual for the same race: a froze-and-thawed mutant leaves
        // one rise and one thaw behind even though the mutator won; the
        // per-instance counters must still be (0, 0) after the refusal.
        assert_eq!(
            fixture.gate.freeze_transition_counts(),
            (0, 0),
            "the concurrent denial must leave zero freeze transitions"
        );
        fixture
            .db
            .set_resource_meta(
                &ResourcePath::table(DB, "main", "rows"),
                &ResourceMeta::owned_enforced(Actor::User(principal64(fixture.other.user_id))),
            )
            .await
            .unwrap();
        let denied = fixture.start().await;
        assert_eq!(denied.unwrap_err(), ServiceError::Denied);
        // The refusal must NEVER have raised the freeze: with the gate's
        // per-instance transition counters at (0, 0) the test now
        // distinguishes never-froze from froze-and-thawed. Mutant: raising
        // the freeze while returning the denial (even if it thaws again)
        // leaves a nonzero rise count and fails here.
        assert_eq!(
            fixture.gate.freeze_transition_counts(),
            (0, 0),
            "a scope denial must leave the gate with zero freeze rises and zero thaws"
        );
        // This is an immediate post-refusal admission, not merely a later state check.
        assert!(fixture.gate.enter_mutation().is_ok());
        assert!(fixture.service.status_for(&fixture.owner, DB).is_none());
        assert!(fixture.service.status_for(&fixture.other, DB).is_none());
        assert!(fixture.gate.enter_mutation().is_ok());
        assert!(fixture.outputs().is_empty());
    });
}
