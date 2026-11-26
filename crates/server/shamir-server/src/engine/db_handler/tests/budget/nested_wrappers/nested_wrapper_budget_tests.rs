use std::sync::Arc;
use std::time::Duration;

use shamir_connect::common::kdf_params::KdfParams;
use shamir_connect::common::time::UnixNanos;
use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::argon2_semaphore::Argon2Permit;
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_db::ShamirDb;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::ddl::create_user;
use shamir_query_types::batch::{BatchLimits, BatchRequest};
use shamir_query_types::filter::FilterValue;
use shamir_query_types::hmac as canon;
use shamir_query_types::wire::{DbRequest, DbResponse};
use tempfile::TempDir;

use crate::byte_budget::{run_with_guard_slot, take_stashed_guard, ByteBudget};
use crate::db_handler::config::QueryLimitsCap;
use crate::db_handler::handler::ShamirDbHandler;
use crate::ports::DirectoryPorts;
use crate::user_directory::{FjallUserDirectory, ScramDerivationGate};
use crate::version::CURRENT_QUERY_LANG_VERSION;

struct BusyFixture<'a> {
    handler: ShamirDbHandler,
    session: Session,
    held: Option<Argon2Permit<'a>>,
    _tempdir: TempDir,
}

async fn busy_fixture(
    gate: &Arc<ScramDerivationGate>,
    budget: ByteBudget,
    hold_permit: bool,
) -> BusyFixture<'_> {
    let temp = tempfile::tempdir().expect("user directory tempdir");
    let user_dir = Arc::new(FjallUserDirectory::open(temp.path().join("users")).unwrap());
    let db = ShamirDb::init_memory().await.expect("init shamir");
    db.create_db("app").await.expect("create app");
    let (port, resolver) = DirectoryPorts::new(user_dir.clone(), KdfParams::DEFAULT, gate.clone())
        .into_trait_objects();
    let handler = ShamirDbHandler::with_admin(
        Arc::new(
            db.with_user_admin_port(port)
                .with_principal_resolver(resolver),
        ),
        crate::db_handler::AdminGlue {
            user_dir,
            kdf: KdfParams::DEFAULT,
            tables_registry: None,
            derivation_gate: gate.clone(),
        },
    )
    .with_query_limits(QueryLimitsCap {
        enforce_result_size: true,
        max_result_size_bytes: 8192,
        ..QueryLimitsCap::UNLIMITED
    })
    .with_byte_budget(budget);
    let session = Session::new(
        [0xAB; 16],
        "root".into(),
        SessionPermissions::from_roles(vec!["superuser".into()]),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        [0; 32],
        UnixNanos::now().as_u64(),
    );
    let held = if hold_permit {
        Some(gate.semaphore().try_acquire().expect("Argon2 permit free"))
    } else {
        None
    };
    BusyFixture {
        handler,
        session,
        held,
        _tempdir: temp,
    }
}

fn busy_op(session: &Session) -> shamir_query_types::batch::BatchOp {
    let op =
        shamir_query_types::batch::BatchOp::CreateUser(shamir_query_types::auth::CreateUserOp {
            create_user: "held_permit_user".into(),
            password: "fixture password".into(),
            roles: Vec::new(),
            database: None,
            hmac: None,
        });
    let tag = canon::compute_tag_hex(
        &session.hmac_key(),
        &canon::canonical_batch_op("app", &op).expect("canonical CreateUser"),
    );
    create_user("held_permit_user", "fixture password")
        .hmac(&tag)
        .build()
        .expect("build CreateUser")
}

fn wrapped_busy_batch(session: &Session) -> BatchRequest {
    let mut child = Batch::new();
    child.create_user("create", busy_op(session));
    let mut loop_body = Batch::new();
    loop_body.sub_batch_no_bind("child", child.build());
    let mut outer = Batch::new();
    outer.limits(BatchLimits {
        max_result_size: 4096,
        ..BatchLimits::default()
    });
    outer.for_each(
        "loop",
        FilterValue::Array(vec![FilterValue::Int(1)]),
        "row",
        loop_body.build(),
    );
    outer.build()
}

fn wire_request(batch: BatchRequest) -> Vec<u8> {
    rmp_serde::to_vec_named(&DbRequest::Execute {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: "app".into(),
        batch,
    })
    .expect("encode request")
}

async fn execute(fixture: &BusyFixture<'_>, batch: BatchRequest) -> DbResponse {
    let wire = wire_request(batch);
    let response = tokio::time::timeout(
        Duration::from_secs(120),
        run_with_guard_slot(async {
            let bytes = fixture
                .handler
                .handle(
                    &fixture.session,
                    &wire,
                    &ConnectionServices::without_push(0),
                )
                .await
                .expect("Execute response encoding");
            let _guard = take_stashed_guard();
            bytes
        }),
    )
    .await
    .expect("nested Execute must finish within 120 seconds");
    rmp_serde::from_slice(&response).expect("decode response")
}

/// Typed busy survives ForEach -> SubBatch with the engine result meter ON.
/// The caller limit of 4096 is both the engine meter cap after policy clamping
/// and the ordinary wire cap; the 4608 fixture budget exactly admits the
/// 4096-byte reservation plus 512-byte fixed-error allowance. The held
/// Argon2 permit is the only difference from the succeeding control test.
#[tokio::test]
async fn for_each_nested_busy_with_engine_byte_meter_on() {
    let gate = Arc::new(ScramDerivationGate::new(Arc::new(
        shamir_connect::server::argon2_semaphore::Argon2Semaphore::with_capacity(1),
    )));
    let fixture = busy_fixture(&gate, ByteBudget::new(Some(4608)), true).await;
    let response = execute(&fixture, wrapped_busy_batch(&fixture.session)).await;
    assert!(
        matches!(&response, DbResponse::Error { code, message }
        if code == "server_busy" && message == "server_busy"),
        "nested refusal must remain typed exactly once: {response:?}"
    );
    drop(fixture.held);
}

/// With the same 4096-byte engine meter and wrapped request, releasing the
/// Argon2 permit (the only difference) lets CreateUser succeed. The caller
/// limit is also the wire cap; the 4608 fixture budget exactly admits the
/// 4096-byte reservation plus its 512-byte fixed-error allowance.
#[tokio::test]
async fn for_each_nested_busy_same_op_with_capacity_succeeds() {
    let gate = Arc::new(ScramDerivationGate::new(Arc::new(
        shamir_connect::server::argon2_semaphore::Argon2Semaphore::with_capacity(1),
    )));
    let fixture = busy_fixture(&gate, ByteBudget::new(Some(4608)), false).await;
    let response = execute(&fixture, wrapped_busy_batch(&fixture.session)).await;
    assert!(
        matches!(response, DbResponse::Batch { .. }),
        "expected normal result: {response:?}"
    );
}
