//! Point-read authorization-binding and name-bound tests for
//! `DbRequest::GetRecord` (admin-shell follow-up).
//!
//! Covers the TOCTOU window the point read had when it authorized names,
//! parked on the global response byte budget, and then resolved those names
//! again: a concurrent ACL revocation or a drop+recreate of the table could
//! slip in while the request was parked. Also covers the cold-resolver
//! retention defect (an unregistered table name must never reach
//! `ShamirDb::get_table`) and the caller-name length bound.
//!
//! The park rendezvous is deterministic and sleep/yield free: the test holds
//! the whole byte budget, and the session's single request slot is the
//! observable — the handler runs the synchronous parent resolution,
//! configured-table check and incarnation capture BEFORE taking the slot,
//! then suspends at the budget acquire, so "slot taken" implies
//! "pre-budget resolution finished". The read future is polled
//! directly (not spawned), so nothing can race the mutation.
//!
//! Fixture mirrors `get_record_tests.rs` (in-memory `ShamirDb`, owner alice,
//! repo `main` with table `items`, one seeded row; requests built ONLY
//! through `shamir_query_builder` and dispatched through the real handler;
//! responses decoded as wire types). Every await is bounded by a 120 s
//! timeout (the Windows CI runner may be up to 10x slower).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use shamir_connect::common::time::UnixNanos;
use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::conn_services::ConnectionServices;
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::session::{Session, SessionPermissions, SessionStore};

use shamir_db::access::{principal64, Actor};
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::engine::table::TableConfig;
use shamir_db::ShamirDb;

use shamir_query_builder::batch::Batch;
use shamir_query_builder::ddl::{create_table, drop_table};
use shamir_query_builder::doc;
use shamir_query_builder::query::Query;
use shamir_query_builder::wire::admin::get_record;
use shamir_query_builder::write::insert;
use shamir_query_types::admin::{DropTableOp, ResourceRef};
use shamir_query_types::batch::BatchOp;
use shamir_query_types::hmac::{canonical_batch_op, compute_tag_hex};
use shamir_query_types::read::{QueryRecord, ReadQuery};
use shamir_query_types::wire::{DbRequest, DbResponse};

use shamir_types::types::record_id::RecordId;

use crate::byte_budget::ByteBudget;
use crate::db_handler::config::{CursorLimitsCap, QueryLimitsCap};
use crate::db_handler::handler::ShamirDbHandler;
use crate::version::CURRENT_QUERY_LANG_VERSION;

const ALICE_SID: [u8; 32] = [0xAA; 32];
/// Generous per-call bound (hang detector; slow Windows runner).
const CALL_TIMEOUT: Duration = Duration::from_secs(120);
/// The byte budget the TEST holds for the whole parked window; the handler's
/// deliberately bigger `max_result_size_bytes` cannot fit while it is held.
const SMALL_BUDGET: usize = 64;
/// The handler result cap used by the parked fixtures (above `SMALL_BUDGET` and the seeded record).
const PARKED_RESULT_CAP: usize = 1024;
/// Payload value of the seeded control row.
const SEEDED_V: i64 = 42;
/// Payload value of the row seeded in the REPLACEMENT table.
const REPLACEMENT_V: i64 = 99;
/// The fixed-text refusal of a table whose authorized registration changed while
/// the read was parked; asserted exactly, so a payload can never satisfy it.
const REGISTRATION_CHANGED: &str = "the authorized table registration changed during the request";
/// The fixed-text refusal for a table that is not configured in the
/// repository (`unknown_table_response`, `operator_state.rs:149-154`).
const UNKNOWN_TABLE_MESSAGE: &str = "table is unavailable";

fn alice_session() -> Session {
    let mut s = Session::new(
        [0xAB; 16],
        "alice".into(),
        SessionPermissions::from_roles(vec!["read_write".into()]),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        [0u8; 32],
        UnixNanos::now().as_u64(),
    );
    s.session_id = ALICE_SID;
    s
}

/// Superuser-class alice: `session_actor` maps `is_superuser` to
/// `Actor::Admin`, which short-circuits `authorize_access` — used ONLY
/// where the test's subject is name RESOLUTION, not ACL.
fn superuser_alice_session() -> Session {
    let mut s = Session::new(
        [0xAB; 16],
        "alice".into(),
        SessionPermissions::new(true, false, Vec::new()),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        [0u8; 32],
        UnixNanos::now().as_u64(),
    );
    s.session_id = ALICE_SID;
    s
}

fn parked_limits() -> QueryLimitsCap {
    QueryLimitsCap {
        max_result_size_bytes: PARKED_RESULT_CAP,
        max_in_flight_requests_per_session: 1,
        ..QueryLimitsCap::UNLIMITED
    }
}

/// Handler over an in-memory `ShamirDb` with `app.main.items` owned by alice
/// (principal `[0xAB; 16]`), plus one seeded row. Returns the handler and the
/// seeded row's storage `RecordId`.
async fn build_handler(
    byte_budget: ByteBudget,
    query_limits: QueryLimitsCap,
) -> (ShamirDbHandler, RecordId) {
    let shamir = tokio::time::timeout(CALL_TIMEOUT, ShamirDb::init_memory())
        .await
        .expect("init must complete within the 120s bound")
        .expect("init shamir");
    let owner = Actor::User(principal64([0xAB; 16]));
    tokio::time::timeout(CALL_TIMEOUT, shamir.create_db_as("app", owner.clone()))
        .await
        .expect("create_db must complete within the 120s bound")
        .expect("create_db");
    let cfg =
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items"));
    tokio::time::timeout(CALL_TIMEOUT, shamir.add_repo_as("app", cfg, owner))
        .await
        .expect("add repo must complete within the 120s bound")
        .expect("add repo");

    let handler = ShamirDbHandler::new(Arc::new(shamir))
        .with_query_limits(query_limits)
        .with_byte_budget(byte_budget);
    let seeded = tokio::time::timeout(CALL_TIMEOUT, seed_row(&handler))
        .await
        .expect("the seed insert must complete within the 120s bound");
    (handler, seeded)
}

/// Insert the control row through the ENGINE's real `execute_as` path and
/// return its STORAGE `RecordId` (never a payload field). The insert stays
/// below the fixture's budget/caps: it does not go through the handler.
async fn seed_row(handler: &ShamirDbHandler) -> RecordId {
    let mut b = Batch::new();
    b.insert(
        "seed",
        insert("items").row(doc! { "id" => "k000", "v" => SEEDED_V }),
    );
    let response = tokio::time::timeout(
        CALL_TIMEOUT,
        handler.db().execute_as(Actor::System, "app", &b.build()),
    )
    .await
    .expect("engine insert must complete within the 120s bound")
    .expect("engine insert must succeed");
    let result = response.results.get("seed").expect("insert alias returned");
    match result.records.first() {
        Some(QueryRecord::Inserted(record)) => {
            record.id.expect("insert must carry the storage RecordId")
        }
        other => panic!("expected an Inserted write-result row, got {other:?}"),
    }
}

/// Send a wire request through the REAL `RequestHandler::handle`, bounded by
/// `CALL_TIMEOUT`.
async fn send(handler: &ShamirDbHandler, session: &Session, req: DbRequest) -> DbResponse {
    let bytes = rmp_serde::to_vec_named(&req).expect("encode request");
    let conn = ConnectionServices::without_push(0);
    let resp_bytes = tokio::time::timeout(
        CALL_TIMEOUT,
        Box::pin(handler.handle(session, &bytes, &conn)),
    )
    .await
    .expect("handle must complete within the 120s bound")
    .expect("handle must not error at the protocol level");
    rmp_serde::from_slice(&resp_bytes).expect("decode response")
}

fn get_record_req(key: RecordId) -> DbRequest {
    get_record("app", "main", "items", key)
}

fn expect_error(resp: DbResponse, code: &str, context: &str) {
    match resp {
        DbResponse::Error { code: got, .. } => assert_eq!(got, code, "{context}"),
        other => panic!("{context}: expected error code '{code}', got {other:?}"),
    }
}

/// Assert the typed ABSENCE refusal: the `unknown_table` code and the fixed text
/// from `unknown_table_response` (`operator_state.rs:149-154`); no payload passes.
fn expect_unknown_table(resp: DbResponse, context: &str) {
    match resp {
        DbResponse::Error { code, message } => {
            assert_eq!(code, "unknown_table", "{context}");
            assert_eq!(message, UNKNOWN_TABLE_MESSAGE, "{context}");
        }
        other => panic!("{context}: expected the fixed unknown_table refusal, got {other:?}"),
    }
}

/// Drive the point read until it is parked at the upfront budget reservation
/// with the pre-budget resolution already done. The session's single request
/// slot is the observable: while we can still take it ourselves the child has
/// not reached the slot gate, so poll the child once (it must stay Pending,
/// the budget is held); once we cannot take it, the child has taken the slot
/// after its synchronous resolution/availability code, so the capture is done.
async fn drive_to_capture<F: Future<Output = DbResponse>>(
    read: &mut Pin<Box<F>>,
    session: &Session,
) {
    // The spin bound turns an unexpected liveness bug into a fast, clear
    // failure instead of a hang (a healthy in-memory read advances or parks
    // in a handful of polls).
    let mut polls = 0u32;
    loop {
        if let Some(probe) = session.try_acquire_request_slot(1) {
            drop(probe);
            let polled =
                std::future::poll_fn(|cx| std::task::Poll::Ready(Future::poll(read.as_mut(), cx)))
                    .await;
            assert!(
                polled.is_pending(),
                "the point read must park on the held budget, not complete"
            );
            polls += 1;
            assert!(
                polls <= 100_000,
                "the point read never reached the budget park"
            );
        } else {
            return;
        }
    }
}

/// Chmod `app.main.items` to `mode` as the System actor (the same ACL mutation
/// style the existing point-read tests use).
async fn chmod_items_table(handler: &ShamirDbHandler, mode: u16) {
    let mut batch = Batch::new();
    batch.chmod(
        "table",
        shamir_query_builder::ddl::chmod(
            ResourceRef::Table {
                table: ["app".into(), "main".into(), "items".into()],
            },
            mode,
        )
        .hmac("get-record-authz-test")
        .build()
        .expect("build chmod table"),
    );
    tokio::time::timeout(
        CALL_TIMEOUT,
        handler
            .db()
            .execute_as(Actor::System, "app", &batch.build()),
    )
    .await
    .expect("admin chmod must complete within the 120s bound")
    .expect("admin chmod must succeed");
}

/// The LIVE registration of `app.main.items`: its catalogue `store_id` and
/// whether the name is registered at all. Read through the repository handle
/// the request would resolve, so the assertions observe the real state a
/// parked read compares against.
fn items_registration(handler: &ShamirDbHandler) -> (Option<RecordId>, bool) {
    let repo = handler
        .db()
        .get_db("app")
        .and_then(|db| db.get_repo("main"))
        .expect("app.main must be registered");
    let store_id = repo
        .table_config("items")
        .expect("items must be registered")
        .store_id;
    (store_id, repo.has_table("items"))
}

/// Drop `app.main.items` and create a fresh `items` in the same repo as alice
/// through the REAL handler dispatch, then seed a unique-payload row in the
/// REPLACEMENT table, re-keyed at the caller-supplied `replacement_id` because
/// the parked read predates the park (a random insert id cannot be known then).
/// A second handler over the SAME `ShamirDb` uses an unbounded budget: the
/// bounded handler would park on the reservation the test holds (its upfront
/// reserve is `batch.limits.max_result_size`, 10 MB by default), deadlocking
/// the rendezvous.
///
/// Returns the live `store_id` read BEFORE the drop, the one read back AFTER
/// the create and the unbounded handler (so the caller can drive the control
/// read). The id assertions are made HERE: a silently no-op DDL would
/// otherwise leave the parked read comparing the table with itself and the
/// test would pass vacuously.
async fn replace_items_table(
    handler: &ShamirDbHandler,
    replacement_id: RecordId,
) -> (Option<RecordId>, Option<RecordId>, ShamirDbHandler) {
    let (pre_drop_store_id, _) = items_registration(handler);
    assert!(
        pre_drop_store_id.is_some(),
        "the fixture's table must carry a live store id before the drop"
    );
    // DROP TABLE and CREATE TABLE through the handler require a superuser
    // session. It shares the session id, hence the confirmation-tag key, with
    // the reading alice.
    let session = superuser_alice_session();
    // The builder refuses to build without the confirmation tag, so the
    // canonical form is taken from the unsigned wire op itself.
    let unsigned = BatchOp::DropTable(DropTableOp {
        drop_table: "items".into(),
        repo: "main".into(),
        hmac: None,
        if_exists: false,
        cascade: false,
    });
    let tag = compute_tag_hex(
        &session.hmac_key(),
        &canonical_batch_op("app", &unsigned).expect("drop-table canonical form"),
    );
    let drop_op = drop_table("items")
        .repo("main")
        .hmac(tag)
        .build()
        .expect("build signed drop table");

    let mut drop_batch = Batch::new();
    drop_batch.drop_table("destroy", drop_op);
    let mut create_batch = Batch::new();
    create_batch.create_table("recreate", create_table("items").repo("main"));

    let ddl_handler =
        ShamirDbHandler::new(Arc::clone(handler.db())).with_byte_budget(ByteBudget::unbounded());

    // Two separate dispatches: one batch would leave the drop/create order
    // to the batch executor, and a create-before-drop would merely fail.
    let drop_resp = tokio::time::timeout(
        CALL_TIMEOUT,
        send(
            &ddl_handler,
            &session,
            DbRequest::Execute {
                query_version: CURRENT_QUERY_LANG_VERSION,
                db: "app".into(),
                batch: drop_batch.build(),
            },
        ),
    )
    .await
    .expect("the drop dispatch must complete within the 120s bound");
    let drop_response = match drop_resp {
        DbResponse::Batch { response } => response,
        other => panic!("drop through the handler must succeed, got {other:?}"),
    };
    // `handle_drop_table` renders success as
    // `{"dropped_table": <name>, "existed": <removed>}` (see
    // `shamir-db` `admin_table_index.rs`), so the real leaf is `existed`.
    assert_eq!(
        drop_response.results["destroy"].records[0].get_value_bool("existed"),
        Some(true),
        "the destroy alias must report that the table existed"
    );

    let create_resp = tokio::time::timeout(
        CALL_TIMEOUT,
        send(
            &ddl_handler,
            &session,
            DbRequest::Execute {
                query_version: CURRENT_QUERY_LANG_VERSION,
                db: "app".into(),
                batch: create_batch.build(),
            },
        ),
    )
    .await
    .expect("the create dispatch must complete within the 120s bound");
    let create_response = match create_resp {
        DbResponse::Batch { response } => response,
        other => panic!("create through the handler must succeed, got {other:?}"),
    };
    // `handle_create_table` renders success as
    // `{"created_table": <name>, "repo": <repo>, "created": true,
    // "existed": false}`, so the real leaf is `created`.
    assert_eq!(
        create_response.results["recreate"].records[0].get_value_bool("created"),
        Some(true),
        "the recreate alias must report that the table was created"
    );

    let (post_create_store_id, has_table) = items_registration(handler);
    assert!(
        post_create_store_id.is_some(),
        "the recreated table must carry a live store id"
    );
    assert!(has_table, "the recreated table must be registered");
    assert_ne!(
        pre_drop_store_id, post_create_store_id,
        "the DDL must actually replace the table registration"
    );

    // Seed one row in the REPLACEMENT table with a UNIQUE payload: the
    // caller's control read returns THIS row, so it can only pass when a
    // NON-admin alice really holds the ACL to read the replacement.
    let mut seed_batch = Batch::new();
    seed_batch.insert(
        "replacement",
        insert("items").row(doc! { "id" => "replacement", "v" => REPLACEMENT_V }),
    );
    let seeded = tokio::time::timeout(
        CALL_TIMEOUT,
        handler
            .db()
            .execute_as(Actor::System, "app", &seed_batch.build()),
    )
    .await
    .expect("the replacement insert must complete within the 120s bound")
    .expect("the replacement insert must succeed");
    let inserted_id = match seeded
        .results
        .get("replacement")
        .expect("insert alias returned")
        .records
        .first()
    {
        Some(QueryRecord::Inserted(record)) => {
            record.id.expect("insert must carry the storage RecordId")
        }
        other => panic!("expected an Inserted write-result row, got {other:?}"),
    };

    // The parked read is built BEFORE this replacement, so its key must be
    // known in advance; the engine insert mints a RANDOM `RecordId`. Re-key
    // the very same row bytes at the caller's `replacement_id` (the raw-key
    // `set` path: no query construction can carry an exact 16-byte id) and
    // drop the random-key duplicate, so the parked read requests exactly the
    // row the replacement table holds.
    let table = tokio::time::timeout(CALL_TIMEOUT, handler.db().get_table("app", "main", "items"))
        .await
        .expect("replacement table acquisition must complete within the 120s bound")
        .expect("the replacement table must be acquirable");
    let inner = tokio::time::timeout(CALL_TIMEOUT, table.get(inserted_id))
        .await
        .expect("the seeded row read must complete within the 120s bound")
        .expect("the seeded replacement row must be readable");
    // `TableManager::set` / `TableManager::delete` are explicitly NOT
    // cancel-safe, so they are awaited directly and never wrapped in
    // `tokio::time::timeout`: a timeout between the row mutation and the
    // index/counter maintenance would leave a half-applied fixture.
    let rekeyed = table
        .set(replacement_id, &inner)
        .await
        .expect("re-keying the replacement row must succeed");
    assert!(rekeyed, "the keyed re-seed must CREATE the replacement row");
    let removed = table
        .delete(inserted_id)
        .await
        .expect("removing the random-key duplicate must succeed");
    assert!(
        removed,
        "the random-key duplicate of the replacement row must be removed"
    );

    (pre_drop_store_id, post_create_store_id, ddl_handler)
}

/// Drop `app.main.items` (no replacement) through the REAL handler dispatch,
/// leaving the name UNCONFIGURED: the destroy alias must report `existed: true`
/// and the name must be absent afterwards, so a silent no-op DDL cannot leave
/// the parked read vacuous. Unbounded second handler (the test holds the budget).
async fn drop_items_table(handler: &ShamirDbHandler) -> ShamirDbHandler {
    let session = superuser_alice_session();
    // The builder refuses to build without the confirmation tag, so the
    // canonical form is taken from the unsigned wire op itself.
    let unsigned = BatchOp::DropTable(DropTableOp {
        drop_table: "items".into(),
        repo: "main".into(),
        hmac: None,
        if_exists: false,
        cascade: false,
    });
    let tag = compute_tag_hex(
        &session.hmac_key(),
        &canonical_batch_op("app", &unsigned).expect("drop-table canonical form"),
    );
    let drop_op = drop_table("items")
        .repo("main")
        .hmac(tag)
        .build()
        .expect("build signed drop table");

    let mut drop_batch = Batch::new();
    drop_batch.drop_table("destroy", drop_op);

    let ddl_handler =
        ShamirDbHandler::new(Arc::clone(handler.db())).with_byte_budget(ByteBudget::unbounded());

    let drop_resp = tokio::time::timeout(
        CALL_TIMEOUT,
        send(
            &ddl_handler,
            &session,
            DbRequest::Execute {
                query_version: CURRENT_QUERY_LANG_VERSION,
                db: "app".into(),
                batch: drop_batch.build(),
            },
        ),
    )
    .await
    .expect("the drop dispatch must complete within the 120s bound");
    let drop_response = match drop_resp {
        DbResponse::Batch { response } => response,
        other => panic!("drop through the handler must succeed, got {other:?}"),
    };
    // `handle_drop_table` renders success as
    // `{"dropped_table": <name>, "existed": <removed>}` (see
    // `shamir-db` `admin_table_index.rs`), so the real leaf is `existed`.
    assert_eq!(
        drop_response.results["destroy"].records[0].get_value_bool("existed"),
        Some(true),
        "the destroy alias must report that the table existed"
    );

    let repo = ddl_handler
        .db()
        .get_db("app")
        .and_then(|db| db.get_repo("main"))
        .expect("app.main must be registered");
    assert!(
        !repo.has_table("items"),
        "the drop must leave `items` unconfigured"
    );
    ddl_handler
}

/// Replace the whole `app.main` REGISTRATION with a LEGACY one
/// (`table_config("items").store_id == None`), minting no store id.
///
/// `ShamirDb::add_repo_as` mints a fresh store id for every id-less inline
/// table (`add_repo_as_with_durability_locked` loops over `config.tables` and
/// calls `with_new_store_id`), so it can NEVER produce a legacy live
/// registration. `DbInstance::add_repo` installs the supplied `RepoConfig` as
/// given, keeping `store_id: None` — which makes the parked comparison
/// `None == None` before the identity fix. The `remove_repo` bool is asserted
/// here so a silent no-op removal cannot make the replacement vacuous.
async fn replace_repo_with_legacy_items(handler: &ShamirDbHandler) {
    let removed = tokio::time::timeout(CALL_TIMEOUT, handler.db().remove_repo("app", "main"))
        .await
        .expect("remove_repo must complete within the 120s bound")
        .expect("remove_repo must succeed");
    assert!(removed, "the fixture's `main` repository must be removed");

    let db = handler.db().get_db("app").expect("app database");
    let config =
        RepoConfig::new("main", BoxRepoFactory::in_memory()).add_table(TableConfig::new("items"));
    tokio::time::timeout(CALL_TIMEOUT, db.add_repo(config))
        .await
        .expect("add_repo must complete within the 120s bound")
        .expect("the legacy repository registration must succeed");
}

// ---------------------------------------------------------------------------
// P1#1 — authorization must stay bound to the incarnation that is read.
// ---------------------------------------------------------------------------

/// The read passes the early ACL, parks on the held budget, the table's Read
/// right is revoked, the budget is released: the reply must be the typed
/// `unknown_table` (the SAME code an absent table answers — the table ACL
/// cannot be used as an existence oracle), never a record payload.
///
/// WITHOUT the fix the handler never re-authorizes, so the parked read
/// resolves the table and returns the seeded `DbResponse::Record`.
#[tokio::test]
async fn get_record_refuses_when_table_read_is_revoked_while_parked_on_budget() {
    let budget = ByteBudget::new(Some(SMALL_BUDGET));
    let (handler, seeded) =
        tokio::time::timeout(CALL_TIMEOUT, build_handler(budget.clone(), parked_limits()))
            .await
            .expect("the handler fixture must build within the 120s bound");
    let store = SessionStore::new();
    let session = store.insert(ALICE_SID, alice_session());
    assert_ne!(session.activity_handle(), 0, "store-stamped session");

    let held = tokio::time::timeout(CALL_TIMEOUT, budget.acquire(SMALL_BUDGET))
        .await
        .expect("the test must reserve the byte budget within the 120s bound");
    let mut read = Box::pin(send(&handler, &session, get_record_req(seeded)));
    tokio::time::timeout(CALL_TIMEOUT, drive_to_capture(&mut read, &session))
        .await
        .expect("the read must reach the budget park within the 120s bound");

    // Mutate while the read is parked: revoke alice's Table Read.
    tokio::time::timeout(CALL_TIMEOUT, chmod_items_table(&handler, 0o000))
        .await
        .expect("the ACL mutation must complete within the 120s bound");
    drop(held);

    let resp = tokio::time::timeout(CALL_TIMEOUT, read)
        .await
        .expect("the released read must complete within the 120s bound");
    expect_error(
        resp,
        "unknown_table",
        "table Read revoked while parked on the budget",
    );
}

/// The read passes the early ACL and parks on the held budget; the table is
/// then dropped and recreated under the SAME name in the same repo, and a row
/// is seeded in the replacement. The name is configured and the ACL still
/// passes — the CONTROL below reads that row back as the SAME non-admin alice
/// — so only the captured registration comparison can refuse it with the fixed
/// typed `access_denied`.
///
/// WITHOUT the identity fix the read resolves the NEW table, finds the
/// replacement row under this same `replacement_id`, and returns THAT PAYLOAD
/// (never the fixed refusal).
#[tokio::test]
async fn get_record_refuses_when_the_authorized_table_is_replaced_while_parked() {
    let budget = ByteBudget::new(Some(SMALL_BUDGET));
    let (handler, _seeded) =
        tokio::time::timeout(CALL_TIMEOUT, build_handler(budget.clone(), parked_limits()))
            .await
            .expect("the handler fixture must build within the 120s bound");
    let store = SessionStore::new();
    let session = store.insert(ALICE_SID, alice_session());

    // The read parks BEFORE the replacement, so it cannot learn the engine's
    // random insert id: the fixture re-keys the replacement row at this key.
    let replacement_id = RecordId([7u8; 16]);

    let held = tokio::time::timeout(CALL_TIMEOUT, budget.acquire(SMALL_BUDGET))
        .await
        .expect("the test must reserve the byte budget within the 120s bound");
    let mut read = Box::pin(send(&handler, &session, get_record_req(replacement_id)));
    tokio::time::timeout(CALL_TIMEOUT, drive_to_capture(&mut read, &session))
        .await
        .expect("the read must reach the budget park within the 120s bound");

    let (pre_store_id, post_store_id, ddl_handler) =
        tokio::time::timeout(CALL_TIMEOUT, replace_items_table(&handler, replacement_id))
            .await
            .expect("the table replacement must complete within the 120s bound");
    assert_ne!(
        pre_store_id, post_store_id,
        "the parked read must be comparing two DIFFERENT registrations"
    );

    // CONTROL: the same NON-admin alice principal must read the replacement
    // row, so the parked refusal cannot be attributed to the ACL.
    let control = tokio::time::timeout(
        CALL_TIMEOUT,
        send(
            &ddl_handler,
            &alice_session(),
            get_record_req(replacement_id),
        ),
    )
    .await
    .expect("the control read must complete within the 120s bound");
    match control {
        DbResponse::Record { record } => assert_eq!(
            record.get_value_i64("v"),
            Some(REPLACEMENT_V),
            "the control must return the replacement's row"
        ),
        other => panic!("control: alice must read the replacement row, got {other:?}"),
    }

    drop(held);

    let resp = tokio::time::timeout(CALL_TIMEOUT, read)
        .await
        .expect("the released read must complete within the 120s bound");
    match resp {
        DbResponse::Error { code, message } => {
            assert_eq!(
                code, "access_denied",
                "authorized table registration replaced while parked"
            );
            assert_ne!(
                code, "unknown_table",
                "a REPLACEMENT is a changed registration, not an absent table"
            );
            assert_eq!(
                message, REGISTRATION_CHANGED,
                "the parked refusal must be the fixed registration text, never a payload"
            );
            assert_ne!(
                message, UNKNOWN_TABLE_MESSAGE,
                "the two absence arms must stay distinguishable by text too"
            );
        }
        other => panic!("expected the fixed access_denied refusal, got {other:?}"),
    }
}

/// P1 (legacy case): a table replaced by ANOTHER **legacy** registration —
/// `table_config("items").store_id` is `None` on BOTH sides — must be refused
/// exactly like an id-bearing replacement.
///
/// The fixture is built with `add_repo_as`, which mints a store id, so the
/// repository is first re-registered through
/// [`replace_repo_with_legacy_items`] (the only route that installs an
/// id-less table as-is). While the read is parked the whole registration is
/// replaced the same legacy way; no store id is ever minted, and the
/// `None`-before / `None`-after assertions below make this legacy-vs-legacy.
///
/// WITHOUT the registration identity the comparison is `None == None`, the
/// read proceeds into the replacement repository and returns `not_found`, so
/// this test fails without the fix.
#[tokio::test]
async fn get_record_refuses_when_a_legacy_table_is_replaced_by_a_legacy_registration() {
    let budget = ByteBudget::new(Some(SMALL_BUDGET));
    let (handler, _seeded) =
        tokio::time::timeout(CALL_TIMEOUT, build_handler(budget.clone(), parked_limits()))
            .await
            .expect("the handler fixture must build within the 120s bound");
    let store = SessionStore::new();
    let session = store.insert(ALICE_SID, alice_session());

    // Legacy on the AUTHORIZED side too: the parked read must capture `None`.
    tokio::time::timeout(CALL_TIMEOUT, replace_repo_with_legacy_items(&handler))
        .await
        .expect("the legacy registration swap must complete within the 120s bound");
    let (authorized_store_id, has_table) = items_registration(&handler);
    assert_eq!(
        authorized_store_id, None,
        "the authorized table must be a LEGACY registration (no store id)"
    );
    assert!(has_table, "the legacy registration must be registered");

    let held = tokio::time::timeout(CALL_TIMEOUT, budget.acquire(SMALL_BUDGET))
        .await
        .expect("the test must reserve the byte budget within the 120s bound");
    let mut read = Box::pin(send(
        &handler,
        &session,
        get_record_req(RecordId([0x42; 16])),
    ));
    tokio::time::timeout(CALL_TIMEOUT, drive_to_capture(&mut read, &session))
        .await
        .expect("the read must reach the budget park within the 120s bound");

    tokio::time::timeout(CALL_TIMEOUT, replace_repo_with_legacy_items(&handler))
        .await
        .expect("the legacy registration swap must complete within the 120s bound");
    let (replacement_store_id, has_table) = items_registration(&handler);
    assert_eq!(
        replacement_store_id, None,
        "the replacement must stay LEGACY: no store id may be minted"
    );
    assert_eq!(
        replacement_store_id, authorized_store_id,
        "legacy-vs-legacy: the captured store ids both stay `None`, so the \
         store-id comparison alone can never tell the two registrations apart"
    );
    assert!(has_table, "the replacement table must be registered");

    drop(held);

    let resp = tokio::time::timeout(CALL_TIMEOUT, read)
        .await
        .expect("the released read must complete within the 120s bound");
    match resp {
        DbResponse::Error { code, message } => {
            assert_eq!(
                code, "access_denied",
                "legacy registration replaced while parked"
            );
            assert_eq!(
                message, REGISTRATION_CHANGED,
                "the legacy replacement must use the fixed registration text"
            );
        }
        other => panic!("expected the fixed access_denied refusal, got {other:?}"),
    }
}

/// Control: identical park-and-release with NO mutation returns the seeded
/// record (the parking machinery itself must not refuse anything).
#[tokio::test]
async fn get_record_granted_control_returns_the_seeded_row_after_parking() {
    let budget = ByteBudget::new(Some(SMALL_BUDGET));
    let (handler, seeded) =
        tokio::time::timeout(CALL_TIMEOUT, build_handler(budget.clone(), parked_limits()))
            .await
            .expect("the handler fixture must build within the 120s bound");
    let store = SessionStore::new();
    let session = store.insert(ALICE_SID, alice_session());

    let held = tokio::time::timeout(CALL_TIMEOUT, budget.acquire(SMALL_BUDGET))
        .await
        .expect("the test must reserve the byte budget within the 120s bound");
    let mut read = Box::pin(send(&handler, &session, get_record_req(seeded)));
    tokio::time::timeout(CALL_TIMEOUT, drive_to_capture(&mut read, &session))
        .await
        .expect("the read must reach the budget park within the 120s bound");
    drop(held);

    let resp = tokio::time::timeout(CALL_TIMEOUT, read)
        .await
        .expect("the released read must complete within the 120s bound");
    match resp {
        DbResponse::Record { record } => {
            assert_eq!(
                record.get_value_i64("v"),
                Some(SEEDED_V),
                "the seeded row's payload must come back"
            );
        }
        other => panic!("expected the seeded Record, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// P1#2 — an unregistered table name must not reach the cold table resolver.
// ---------------------------------------------------------------------------

/// Eight DISTINCT never-created table names, each a typed `unknown_table`, must
/// leave the repository's direct cell map byte-for-byte unchanged:
/// `ShamirDb::get_table` inserts one catalogue cell per probe. WITHOUT the fix
/// these eight requests grow the cell map by eight.
#[tokio::test]
async fn missing_table_names_do_not_grow_the_catalogue_cells() {
    let (handler, _seeded) =
        build_handler(ByteBudget::unbounded(), QueryLimitsCap::UNLIMITED).await;
    let session = superuser_alice_session();

    let repo = handler
        .db()
        .get_db("app")
        .expect("app database")
        .get_repo("main")
        .expect("main repo");
    let before = repo.table_cell_count();

    for i in 0..8u8 {
        let name = format!("absent_table_{i}");
        let resp = send(
            &handler,
            &session,
            get_record("app", "main", name.as_str(), RecordId([9u8; 16])),
        )
        .await;
        // Extends the original code-only check: the STEP-5 absence arm must
        // answer BOTH the typed code and the fixed text from
        // `unknown_table_response` (`operator_state.rs:149-154`).
        expect_unknown_table(resp, "unregistered table name");
    }

    assert_eq!(
        repo.table_cell_count(),
        before,
        "a refused missing-table probe must not insert a catalogue cell"
    );
}

/// The read captures `items` at step 5, parks on the held byte budget, and the
/// table is then DROPPED with no replacement through the REAL handler dispatch
/// (`drop_items_table`). The step-7 re-probe therefore observes absence and
/// must answer the fixed `unknown_table` — never the changed-registration
/// refusal and never a `not_found` from the cold resolver.
///
/// The reading session is a store-stamped superuser so the re-run ACL cannot
/// mask the absence subject with an `access_denied`; this test is about the
/// step-7 `None` branch, not authorization.
///
/// WITHOUT the typed-absence branch the dropped name would fall through to
/// `ShamirDb::get_table` and surface `not_found`, so this test fails.
#[tokio::test]
async fn get_record_reports_unknown_table_when_the_table_is_dropped_while_parked() {
    let budget = ByteBudget::new(Some(SMALL_BUDGET));
    let (handler, seeded) =
        tokio::time::timeout(CALL_TIMEOUT, build_handler(budget.clone(), parked_limits()))
            .await
            .expect("the handler fixture must build within the 120s bound");
    let store = SessionStore::new();
    let session = store.insert(ALICE_SID, superuser_alice_session());
    assert_ne!(
        session.activity_handle(),
        0,
        "the store-stamped session is the park observable"
    );

    let held = tokio::time::timeout(CALL_TIMEOUT, budget.acquire(SMALL_BUDGET))
        .await
        .expect("the test must reserve the byte budget within the 120s bound");
    let mut read = Box::pin(send(&handler, &session, get_record_req(seeded)));
    tokio::time::timeout(CALL_TIMEOUT, drive_to_capture(&mut read, &session))
        .await
        .expect("the read must reach the budget park within the 120s bound");

    tokio::time::timeout(CALL_TIMEOUT, drop_items_table(&handler))
        .await
        .expect("the drop must complete within the 120s bound");
    drop(held);

    let resp = tokio::time::timeout(CALL_TIMEOUT, read)
        .await
        .expect("the released read must complete within the 120s bound");
    expect_unknown_table(resp, "table removed between the two re-probes");
}

// ---------------------------------------------------------------------------
// P2#3 — bound every caller-supplied name before it can be echoed/resolved.
// ---------------------------------------------------------------------------

fn expect_invalid_name(resp: DbResponse, context: &str) {
    match resp {
        DbResponse::Error { code, message } => {
            assert_eq!(code, "invalid_request", "{context}");
            assert_eq!(
                message, "db, repo and table names must be 1..=255 UTF-8 bytes",
                "{context}"
            );
            assert!(
                !message.contains("aaaa"),
                "{context}: the refusal must not echo the caller's name"
            );
        }
        other => panic!("{context}: expected the fixed invalid_request refusal, got {other:?}"),
    }
}

/// A 256-byte `db`, `repo` or `table` name is the fixed `invalid_request`
/// refusal that never echoes the input; a 255-byte table name passes the bound
/// (it then fails later with `unknown_table`, proving the boundary). WITHOUT
/// the fix the over-long name is echoed back in a resolution error and reaches
/// the catalogue/resolver instead of being refused up front.
#[tokio::test]
async fn get_record_refuses_over_long_names_without_echoing_them() {
    let (handler, _seeded) =
        build_handler(ByteBudget::unbounded(), QueryLimitsCap::UNLIMITED).await;
    let session = superuser_alice_session();
    let key = RecordId([9u8; 16]);

    let over_long = "a".repeat(256);
    let resp = send(
        &handler,
        &session,
        get_record(over_long.as_str(), "main", "items", key),
    )
    .await;
    expect_invalid_name(resp, "256-byte db name");

    let resp = send(
        &handler,
        &session,
        get_record("app", over_long.as_str(), "items", key),
    )
    .await;
    expect_invalid_name(resp, "256-byte repo name");

    let resp = send(
        &handler,
        &session,
        get_record("app", "main", over_long.as_str(), key),
    )
    .await;
    expect_invalid_name(resp, "256-byte table name");

    // Boundary: 255 bytes is accepted by the name bound and then refused by
    // the configured-table check.
    let at_limit = "a".repeat(255);
    let resp = send(
        &handler,
        &session,
        get_record("app", "main", at_limit.as_str(), key),
    )
    .await;
    expect_error(
        resp,
        "unknown_table",
        "255-byte table name passes the bound",
    );
}

// Cell-retention and slot-saturation tests of the point read live in a child
// module so this file stays under the layout limit; it reuses these helpers.
#[path = "../handler/get_record_cell_growth_tests.rs"]
mod cell_growth_tests;
