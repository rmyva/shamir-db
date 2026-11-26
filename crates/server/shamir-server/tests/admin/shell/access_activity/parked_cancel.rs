//! Cumulative decoded-request count is asserted in-crate by `decoded_request_counter_covers_cancellation_dispatches`; this shell test cannot reach the handler's test accessor, and no metric was added.
use super::access_activity_raw_scram::{
    checked_shell_timeout, decimal, keys_exact, scan_profile_home, scan_streams,
    scanner_positive_control, shell_own_handle, REQUEST_FIELDS, REQUEST_STATES,
};
use super::harness::{
    boot, overwrite_profile, seed_catalogue_timeout, seed_shell_user_named_timeout,
    seed_shell_user_timeout, ShellFixture, ShellRun, ADMIN, DB_NAME, PLAIN_TABLE, REPO_NAME,
    SHELL_USER,
};
use super::secure_fixture::{run_secret_process_timeout_scanned, set_acl_mode_timeout};
use super::state_data_common::{data, refusal, scram_profile};
use crate::shell_tests::access_activity_holders::raw_session::{
    batch_reply, close_session, exchange, login, remaining, rollback_tx, RawSession, FRAME_BOUND,
    STEP_BOUND,
};
use serde_json::Value;
use shamir_query_builder::batch::{Batch, Isolation};
use shamir_query_builder::{ddl, doc, filter, write};
use shamir_query_types::admin::ResourceRef;
use shamir_query_types::batch::IsolationLevel;
use shamir_query_types::hmac as canon;
use shamir_query_types::write::UpdateOp;
use shamir_server::db_handler::{DbRequest, DbResponse};
use shamir_server::version::CURRENT_QUERY_LANG_VERSION;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use zeroize::Zeroizing;
const TOTAL_BOUND: Duration = Duration::from_secs(175);
const CLEANUP_RESERVE: Duration = Duration::from_secs(30);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
const FOREIGN_USER: &str = "parked_cancel_foreign";
fn active_deadline(start: Instant) -> Instant {
    start + (TOTAL_BOUND - CLEANUP_RESERVE).min(STEP_BOUND)
}
// ── Fixture helpers ──────────────────────────────────────────────────
fn use_admin_profile(fixture: &ShellFixture) {
    let text = scram_profile(fixture, "tls-tcp", &fixture.tls_addr.to_string());
    overwrite_profile(fixture, &text);
}
fn use_foreign_profile(fixture: &ShellFixture) {
    let text = scram_profile(fixture, "tls-tcp", &fixture.tls_addr.to_string()).replace(
        &format!("username: {ADMIN}\n"),
        &format!("username: {FOREIGN_USER}\n"),
    );
    overwrite_profile(fixture, &text);
}
fn grant_write_access(fixture: &ShellFixture, deadline: Instant) {
    set_acl_mode_timeout(
        fixture,
        ResourceRef::Database {
            database: DB_NAME.into(),
        },
        0o777,
        remaining(deadline).min(STEP_BOUND),
    );
    set_acl_mode_timeout(
        fixture,
        ResourceRef::Store {
            store: [DB_NAME.into(), REPO_NAME.into()],
        },
        0o777,
        remaining(deadline).min(STEP_BOUND),
    );
    set_acl_mode_timeout(
        fixture,
        ResourceRef::Table {
            table: [DB_NAME.into(), REPO_NAME.into(), PLAIN_TABLE.into()],
        },
        0o777,
        remaining(deadline).min(STEP_BOUND),
    );
}
fn insert_target_row(fixture: &ShellFixture, session: RawSession) -> RawSession {
    let mut batch = Batch::new();
    batch.id("seed_target");
    batch.insert(
        "row",
        write::Insert::with_repo(REPO_NAME, PLAIN_TABLE).rows([doc! { "n" => 1_i64 }]),
    );
    let request = DbRequest::Execute {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: DB_NAME.into(),
        batch: batch.build(),
    };
    let (session, reply) = session.ask(fixture, request);
    let response = batch_reply(reply, "seed target row executes");
    let inserted = response
        .results
        .get("row")
        .expect("seed target alias present");
    assert_eq!(
        inserted.records.len(),
        1,
        "the target insert must echo one row"
    );
    session
}
fn update_n(from: i64, to: i64) -> UpdateOp {
    write::Update::with_repo(REPO_NAME, PLAIN_TABLE)
        .where_(filter::eq("n", from))
        .set(doc! { "n" => to })
        .build()
        .expect("fixture update")
}
fn begin_pessimistic_tx(fixture: &ShellFixture, session: RawSession) -> (RawSession, u64) {
    let request = DbRequest::TxBegin {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: DB_NAME.into(),
        repo: REPO_NAME.into(),
        isolation: Some(IsolationLevel::Pessimistic),
    };
    let (session, reply) = session.ask(fixture, request);
    match reply {
        DbResponse::TxOpened { tx_handle, .. } => (session, tx_handle),
        DbResponse::Error { code, .. } => {
            panic!("session A begins its interactive transaction: server replied `{code}`")
        }
        _ => panic!("session A begins its interactive transaction: unexpected reply shape"),
    }
}
fn stage_write(fixture: &ShellFixture, session: RawSession, tx_handle: u64) -> RawSession {
    let mut batch = Batch::new();
    batch.id("stage");
    batch.update("staged", update_n(1, -1));
    let request = DbRequest::TxExecute {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: DB_NAME.into(),
        tx_handle,
        batch: batch.build(),
    };
    let (session, reply) = session.ask(fixture, request);
    let response = batch_reply(reply, "staged write executes inside the transaction");
    let staged = response
        .results
        .get("staged")
        .expect("staged alias present");
    let stats = staged
        .stats
        .as_ref()
        .expect("the staged update result carries stats");
    assert_eq!(
        stats.records_returned, 1,
        "the staged update must hit one row"
    );
    session
}
fn spawn_parked_request(
    fixture: &ShellFixture,
    session: RawSession,
) -> tokio::task::JoinHandle<DbResponse> {
    let mut batch = Batch::new();
    batch.id("parked");
    batch.transactional().isolation(Isolation::Pessimistic);
    batch.update("blocked", update_n(1, 2));
    let request = DbRequest::Execute {
        query_version: CURRENT_QUERY_LANG_VERSION,
        db: DB_NAME.into(),
        batch: batch.build(),
    };
    // Keep the session alive until the server answers.
    let cap = remaining(session.deadline).min(STEP_BOUND);
    let state = session.state.clone();
    fixture
        .rt
        .run(async move {
            (tokio::spawn(async move {
                let mut state = state.lock().await;
                tokio::time::timeout(cap, exchange(&mut state, &request))
                    .await
                    .expect("victim request deadline")
            }),)
        })
        .0
}
// ── Output and leak helpers ──────────────────────────────────────────
fn checked_shell(
    fixture: &ShellFixture,
    needles: &[&str],
    args: &[&str],
    deadline: Instant,
) -> ShellRun {
    let cap = remaining(deadline).min(FRAME_BOUND);
    assert!(!cap.is_zero(), "active deadline expired before shell child");
    checked_shell_timeout(fixture, needles, args, cap)
}
fn signed_acl_probe(
    fixture: &ShellFixture,
    session: RawSession,
    deadline: Instant,
) -> (RawSession, Zeroizing<[u8; 32]>, Zeroizing<String>) {
    let admin = login(fixture, ADMIN, fixture.canary.as_bytes(), deadline);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let sid = fixture
            .rt
            .run_timeout(remaining(deadline).min(STEP_BOUND), {
                let state = admin.state.clone();
                async move {
                    tokio::time::timeout(remaining(deadline).min(STEP_BOUND), async move {
                        state.lock().await.sid
                    })
                    .await
                    .expect("signed probe session-id read deadline")
                }
            });
        let key = Zeroizing::new(canon::derive_session_hmac_key(&sid));
        let resource = shamir_query_builder::ddl::res::table(DB_NAME, REPO_NAME, PLAIN_TABLE);
        let unsigned = ddl::chmod(resource.clone(), 0o777)
            .hmac("0".repeat(64))
            .build()
            .expect("typed chmod with placeholder tag");
        let canonical =
            canon::canonical_batch_op("default", &unsigned).expect("canonical chmod bytes");
        let tag = Zeroizing::new(canon::compute_tag_hex(&key, &canonical));
        let op = ddl::chmod(resource, 0o777)
            .hmac(tag.as_str())
            .build()
            .expect("signed ACL probe");
        let mut batch = Batch::new();
        batch.id("signed_acl_probe");
        batch.chmod("mode", op);
        let (_, reply) = admin.ask(
            fixture,
            DbRequest::Execute {
                query_version: CURRENT_QUERY_LANG_VERSION,
                db: "default".into(),
                batch: batch.build(),
            },
        );
        let response = batch_reply(reply, "signed ACL probe executes");
        assert_eq!(
            response
                .results
                .get("mode")
                .expect("signed chmod result")
                .records
                .len(),
            1,
            "signed ACL probe is accepted by the server"
        );
        (key, tag)
    }));
    let close_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        close_session(fixture, admin);
    }));
    match result {
        Ok((key, tag)) => {
            close_result.expect("admin signed-probe session closes");
            (session, key, tag)
        }
        Err(panic) => {
            let _ = close_result;
            std::panic::resume_unwind(panic)
        }
    }
}
// ── Parked-row observation ───────────────────────────────────────────
// Renew the holder transaction around shell probes.
fn renew_holder_and_check_victim(
    fixture: &ShellFixture,
    holder: &RawSession,
    tx_handle: u64,
    victim: &tokio::task::JoinHandle<DbResponse>,
    deadline: Instant,
    context: &str,
) {
    assert!(!victim.is_finished(), "victim finished {context}");
    let mut batch = Batch::new();
    batch.id("keeper");
    batch.query(
        "keeper",
        shamir_query_builder::Query::with_repo(REPO_NAME, PLAIN_TABLE).build(),
    );
    let (_, reply) = holder.ask(
        fixture,
        DbRequest::TxExecute {
            query_version: CURRENT_QUERY_LANG_VERSION,
            db: DB_NAME.into(),
            tx_handle,
            batch: batch.build(),
        },
    );
    let response = batch_reply(reply, context);
    assert!(
        response.results.contains_key("keeper"),
        "keeper query failed {context}"
    );
    assert!(
        remaining(deadline) > Duration::ZERO,
        "deadline expired {context}"
    );
    assert!(!victim.is_finished(), "victim finished {context}");
}
/// Poll until A's request is listed as parked.
fn await_parked_row(
    fixture: &ShellFixture,
    needles: &[&str],
    deadline: Instant,
    holder: RawSession,
    tx_handle: u64,
    victim: &mut tokio::task::JoinHandle<DbResponse>,
) -> Value {
    loop {
        assert!(
            !victim.is_finished(),
            "victim finished before it was observed parked"
        );
        let cap = remaining(deadline);
        assert!(!cap.is_zero(), "active deadline expired while polling");
        // Read-only TxExecute renews the transaction lease.
        let mut batch = Batch::new();
        batch.id("keeper");
        let batch_query = shamir_query_builder::Query::with_repo(REPO_NAME, PLAIN_TABLE).build();
        batch.query("keeper", batch_query);
        let (_, reply) = holder.ask(
            fixture,
            DbRequest::TxExecute {
                query_version: CURRENT_QUERY_LANG_VERSION,
                db: DB_NAME.into(),
                tx_handle,
                batch: batch.build(),
            },
        );
        batch_reply(reply, "holder lease renewal");
        assert!(
            !victim.is_finished(),
            "victim finished during lease renewal"
        );
        let cap = remaining(deadline).min(FRAME_BOUND);
        assert!(!cap.is_zero(), "active deadline expired before probe");
        let run = checked_shell_timeout(fixture, needles, &["activity", "requests", "--all"], cap);
        assert!(!victim.is_finished(), "victim finished during probe");
        let (_, reply) = holder.ask(
            fixture,
            DbRequest::TxExecute {
                query_version: CURRENT_QUERY_LANG_VERSION,
                db: DB_NAME.into(),
                tx_handle,
                batch: {
                    let mut batch = Batch::new();
                    batch.id("keeper_after");
                    batch.query(
                        "keeper_after",
                        shamir_query_builder::Query::with_repo(REPO_NAME, PLAIN_TABLE).build(),
                    );
                    batch.build()
                },
            },
        );
        batch_reply(reply, "holder lease renewal after poll");
        assert!(
            !victim.is_finished(),
            "victim finished during post-probe renewal"
        );
        let cap = remaining(deadline);
        assert!(!cap.is_zero(), "active deadline expired during poll");
        let payload = data(&run, "requests");
        keys_exact(
            &payload,
            &["kind", "scope", "rows", "rows_returned", "rows_truncated"],
            "activity requests --all page",
        );
        assert_eq!(payload["kind"], "requests", "wrong requests page kind");
        assert_eq!(payload["scope"], "all", "`--all` must widen the scope");
        assert_eq!(
            payload["rows_truncated"], false,
            "requests page is not truncated"
        );
        let rows = payload["rows"]
            .as_array()
            .expect("activity requests `rows` is not an array");
        assert_eq!(
            payload["rows_returned"].as_str(),
            Some(rows.len().to_string().as_str()),
            "rows_returned must equal the decimal row count",
        );
        let mut parked = None;
        for row in rows {
            keys_exact(row, REQUEST_FIELDS, "activity requests row");
            let state = row["state"]
                .as_str()
                .expect("request row `state` is not a string");
            assert!(
                REQUEST_STATES.contains(&state),
                "request row `state` is not a documented rendering"
            );
            if state == "waiting_lock" {
                parked = Some(row.clone());
            }
        }
        if let Some(row) = parked {
            return row;
        }
        assert!(
            Instant::now() < deadline,
            "no A request was listed as waiting_lock before active deadline"
        );
    }
}
struct FixtureShutdownGuard {
    fixture: ShellFixture,
    shutdown_deadline: Instant,
}
impl Drop for FixtureShutdownGuard {
    fn drop(&mut self) {
        let cap = remaining(self.shutdown_deadline);
        if cap.is_zero() {
            self.fixture
                .guard
                .note("fixture shutdown on grace", "total deadline exhausted");
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.fixture
                    .shutdown_with_deadline(Instant::now() + SHUTDOWN_GRACE);
            }));
            return;
        }
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.fixture.shutdown_with_deadline(self.shutdown_deadline);
        }));
    }
}
struct CleanupGuard<'a> {
    fixture: &'a ShellFixture,
    holder: Option<RawSession>,
    victim_session: Option<RawSession>,
    tx_handle: Option<u64>,
    victim: Option<tokio::task::JoinHandle<DbResponse>>,
    cleanup_deadline: Instant,
}
impl Drop for CleanupGuard<'_> {
    fn drop(&mut self) {
        let mut outcomes = Vec::new();
        if let Some(victim) = self.victim.take() {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let deadline = self.cleanup_deadline;
                self.fixture
                    .rt
                    .run_timeout(remaining(deadline), async move {
                        let mut victim = victim;
                        victim.abort();
                        let cap = deadline.saturating_duration_since(Instant::now());
                        match tokio::time::timeout(cap, &mut victim).await {
                            Ok(Ok(_)) => "victim join: success".to_owned(),
                            Ok(Err(_)) => "victim join: task error".to_owned(),
                            Err(_) => "victim join: timeout (aborted; handle consumed)".to_owned(),
                        }
                    })
            }));
            outcomes.push(match result {
                Ok(status) => status,
                Err(_) => "victim join: panic".to_owned(),
            });
        }
        if let Some(session) = self.victim_session.take() {
            let state = session.state.clone();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let deadline = self.cleanup_deadline;
                self.fixture
                    .rt
                    .run_timeout(remaining(deadline), async move {
                        let cap = deadline.saturating_duration_since(Instant::now());
                        match tokio::time::timeout(cap, async {
                            let mut state = state.lock().await;
                            state.write.shutdown().await
                        })
                        .await
                        {
                            Ok(Ok(())) => "victim socket close: success".to_owned(),
                            Ok(Err(_)) => "victim socket close: error".to_owned(),
                            Err(_) => "victim socket close: timeout".to_owned(),
                        }
                    })
            }));
            outcomes.push(result.unwrap_or_else(|_| "victim socket close: panic".to_owned()));
        }
        if let Some(session) = self.holder.take() {
            let handle = self.tx_handle.take();
            let state = session.state.clone();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let deadline = self.cleanup_deadline;
                self.fixture
                    .rt
                    .run_timeout(remaining(deadline), async move {
                        let Some(tx_handle) = handle else {
                            return "rollback: not needed".to_owned();
                        };
                        let request = DbRequest::TxRollback {
                            db: DB_NAME.into(),
                            tx_handle,
                        };
                        let cap = deadline.saturating_duration_since(Instant::now());
                        match tokio::time::timeout(cap, async move {
                            let mut state = state.lock().await;
                            exchange(&mut state, &request).await
                        })
                        .await
                        {
                            Ok(DbResponse::TxRolledBack { tx_handle: got }) if got == tx_handle => {
                                "rollback: success".to_owned()
                            }
                            Ok(DbResponse::Error { .. }) => "rollback: server error".to_owned(),
                            Ok(_) => "rollback: unexpected reply".to_owned(),
                            Err(_) => "rollback: timeout".to_owned(),
                        }
                    })
            }));
            outcomes.push(result.unwrap_or_else(|_| "rollback: panic".to_owned()));
            let state = session.state.clone();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let deadline = self.cleanup_deadline;
                self.fixture
                    .rt
                    .run_timeout(remaining(deadline), async move {
                        let cap = deadline.saturating_duration_since(Instant::now());
                        match tokio::time::timeout(cap, async move {
                            let mut state = state.lock().await;
                            state.write.shutdown().await
                        })
                        .await
                        {
                            Ok(Ok(())) => "holder socket close: success".to_owned(),
                            Ok(Err(_)) => "holder socket close: error".to_owned(),
                            Err(_) => "holder socket close: timeout".to_owned(),
                        }
                    })
            }));
            outcomes.push(result.unwrap_or_else(|_| "holder socket close: panic".to_owned()));
        }
        if !outcomes.is_empty() {
            eprintln!("parked-cancel cleanup: {}", outcomes.join("; "));
        }
    }
}
/// Verify parked-request cancellation, authorization, and leak checks.
#[test]
fn parked_request_is_cancelled_through_the_shell() {
    let start = Instant::now();
    let deadline = active_deadline(start);
    let mut fixture_guard = FixtureShutdownGuard {
        fixture: boot("parked_request_is_cancelled_through_the_shell"),
        shutdown_deadline: start + TOTAL_BOUND,
    };
    let fixture = &fixture_guard.fixture;
    let mut cleanup = CleanupGuard {
        fixture,
        holder: None,
        victim_session: None,
        tx_handle: None,
        victim: None,
        cleanup_deadline: start + TOTAL_BOUND,
    };
    seed_catalogue_timeout(fixture, remaining(deadline).min(STEP_BOUND));
    let a_password = seed_shell_user_timeout(fixture, remaining(deadline).min(STEP_BOUND));
    let foreign_password =
        seed_shell_user_named_timeout(fixture, FOREIGN_USER, remaining(deadline).min(STEP_BOUND));
    grant_write_access(fixture, deadline);
    use_admin_profile(fixture);
    let conn1 = login(fixture, SHELL_USER, a_password.as_bytes(), deadline);
    cleanup.holder = Some(conn1.clone());
    let (conn1, signing_key, confirmation_tag) = signed_acl_probe(fixture, conn1, deadline);
    cleanup.holder = Some(conn1.clone());
    let conn2 = login(fixture, SHELL_USER, a_password.as_bytes(), deadline);
    cleanup.victim_session = Some(conn2.clone());
    let sid = fixture.rt.run_timeout(remaining(deadline), {
        let state = conn2.state.clone();
        async move {
            tokio::time::timeout(remaining(deadline), async move { state.lock().await.sid })
                .await
                .expect("victim session-id read deadline")
        }
    });
    let session_hex = hex::encode(sid);
    let key_hex = Zeroizing::new(hex::encode(*signing_key));
    let tag_hex = confirmation_tag.as_str();
    let needles = [
        fixture.canary.as_str(),
        a_password.as_str(),
        foreign_password.as_str(),
        session_hex.as_str(),
        key_hex.as_str(),
        tag_hex,
    ];
    scanner_positive_control();
    fixture.guard.mark("park: seed the target row");
    let conn1 = insert_target_row(fixture, conn1);
    fixture
        .guard
        .mark("park: A opens a pessimistic transaction and stages a write");
    let (conn1, tx_handle) = begin_pessimistic_tx(fixture, conn1);
    cleanup.holder = Some(conn1.clone());
    cleanup.tx_handle = Some(tx_handle);
    let conn1 = stage_write(fixture, conn1, tx_handle);
    fixture.guard.mark("park: A issues the conflicting write");
    let parked = spawn_parked_request(fixture, conn2);
    cleanup.victim = Some(parked);
    let parked = cleanup.victim.as_mut().expect("victim guard installed");
    fixture
        .guard
        .mark("park: shell observes the parked request");
    let row = await_parked_row(
        fixture,
        &needles,
        deadline,
        conn1.clone(),
        tx_handle,
        parked,
    );
    // ── Assertion 1: the parked row's shape, identity and isolation ──
    let parked_id = decimal(&row["request_exec_id"], "parked request_exec_id");
    let parked_handle = decimal(&row["session_handle"], "parked session_handle");
    assert_eq!(row["kind"], "request", "wrong row kind in a requests list");
    assert_eq!(
        row["state"], "waiting_lock",
        "the request must be listed as parked on a record lock"
    );
    assert_eq!(row["user"], SHELL_USER, "the parked request's user");
    assert_eq!(row["db"], DB_NAME, "the parked request's database");
    decimal(&row["elapsed_ns"], "parked elapsed_ns");
    assert_eq!(
        row["op_kind"], "write",
        "the parked update renders its wire operation kind"
    );
    renew_holder_and_check_victim(
        fixture,
        &conn1,
        tx_handle,
        parked,
        deadline,
        "before shell handle probe",
    );
    let shell_handle = shell_own_handle(fixture, &needles, &parked_handle, deadline);
    renew_holder_and_check_victim(
        fixture,
        &conn1,
        tx_handle,
        parked,
        deadline,
        "after shell handle probe",
    );
    assert_ne!(
        parked_handle, shell_handle,
        "the parked row belongs to A's session, never the shell's own"
    );
    renew_holder_and_check_victim(
        fixture,
        &conn1,
        tx_handle,
        parked,
        deadline,
        "before own requests probe",
    );
    let own_run = checked_shell(fixture, &needles, &["activity", "requests"], deadline);
    renew_holder_and_check_victim(
        fixture,
        &conn1,
        tx_handle,
        parked,
        deadline,
        "after own requests probe",
    );
    let own = data(&own_run, "requests");
    keys_exact(
        &own,
        &["kind", "scope", "rows", "rows_returned", "rows_truncated"],
        "activity requests own page",
    );
    assert_eq!(own["kind"], "requests", "wrong requests page kind");
    assert_eq!(own["scope"], "own", "the default scope must be own");
    assert_eq!(
        own["rows_truncated"], false,
        "own requests page is not truncated"
    );
    let own_rows = own["rows"].as_array().expect("own rows array");
    assert_eq!(
        own["rows_returned"].as_str(),
        Some(own_rows.len().to_string().as_str()),
        "own rows_returned must equal the decimal row count",
    );
    for own_row in own_rows {
        assert_ne!(
            own_row["request_exec_id"].as_str(),
            Some(parked_id.as_str()),
            "another user's request must not appear in the own scope"
        );
    }
    // A foreign principal can list own requests before its cancel authorization is tested.
    use_foreign_profile(fixture);
    let requests_cap = remaining(deadline).min(FRAME_BOUND).min(STEP_BOUND);
    renew_holder_and_check_victim(
        fixture,
        &conn1,
        tx_handle,
        parked,
        deadline,
        "before foreign own requests probe",
    );
    assert!(
        !requests_cap.is_zero(),
        "active deadline expired before foreign requests probe"
    );
    let requests_probe = run_secret_process_timeout_scanned(
        fixture,
        Some(foreign_password.clone()),
        false,
        None,
        &["activity", "requests"],
        requests_cap,
        (&[], &needles),
    );
    scan_streams(&requests_probe, &needles);
    renew_holder_and_check_victim(
        fixture,
        &conn1,
        tx_handle,
        parked,
        deadline,
        "after foreign own requests probe",
    );
    assert_eq!(
        requests_probe.exit_code, 0,
        "foreign user can list own requests"
    );
    let foreign_envelope: Value = serde_json::from_slice(
        requests_probe
            .stdout
            .strip_suffix(b"\n")
            .expect("LF-terminated foreign requests output"),
    )
    .expect("valid foreign requests envelope");
    keys_exact(
        &foreign_envelope,
        &["version", "command", "kind", "op", "node", "data"],
        "foreign activity requests envelope",
    );
    assert_eq!(
        foreign_envelope["version"].as_u64(),
        Some(1),
        "wrong wire version"
    );
    assert_eq!(
        foreign_envelope["command"].as_str(),
        Some("activity requests"),
        "wrong command id for `activity requests`"
    );
    assert_eq!(foreign_envelope["op"].as_str(), Some("ListActivity"));
    assert!(
        foreign_envelope["node"].is_null(),
        "non-status node is null"
    );
    assert_eq!(
        foreign_envelope["kind"].as_str(),
        Some("result"),
        "expected result kind"
    );
    let foreign_own = foreign_envelope["data"].clone();
    keys_exact(
        &foreign_own,
        &["kind", "scope", "rows", "rows_returned", "rows_truncated"],
        "foreign activity requests own page",
    );
    assert_eq!(foreign_own["kind"], "requests", "wrong requests page kind");
    assert_eq!(
        foreign_own["scope"], "own",
        "foreign default scope must be own"
    );
    assert_eq!(
        foreign_own["rows_truncated"], false,
        "own requests page is not truncated"
    );
    let foreign_rows = foreign_own["rows"]
        .as_array()
        .expect("foreign own rows array");
    assert_eq!(
        foreign_own["rows_returned"].as_str(),
        Some(foreign_rows.len().to_string().as_str()),
        "foreign own rows_returned must equal the decimal row count",
    );
    // ── Assertion 2: foreign cancel is refused without waking the victim ─
    fixture.guard.mark("park: foreign user cancel is denied");
    let foreign_cap = remaining(deadline).min(FRAME_BOUND).min(STEP_BOUND);
    renew_holder_and_check_victim(
        fixture,
        &conn1,
        tx_handle,
        parked,
        deadline,
        "before foreign cancel",
    );
    assert!(
        !foreign_cap.is_zero(),
        "active deadline expired before foreign cancel"
    );
    let foreign_cancel = run_secret_process_timeout_scanned(
        fixture,
        Some(foreign_password.clone()),
        false,
        None,
        &["activity", "cancel", &parked_id, "--yes"],
        foreign_cap,
        (&[], &needles),
    );
    scan_streams(&foreign_cancel, &needles);
    renew_holder_and_check_victim(
        fixture,
        &conn1,
        tx_handle,
        parked,
        deadline,
        "after foreign cancel",
    );
    refusal(&foreign_cancel, "cancel", "permission_denied", 4);
    assert!(!parked.is_finished(), "unauthorized cancel woke the victim");
    use_admin_profile(fixture);
    let still_parked = await_parked_row(
        fixture,
        &needles,
        deadline,
        conn1.clone(),
        tx_handle,
        parked,
    );
    assert_eq!(
        still_parked["request_exec_id"], parked_id,
        "unauthorized cancel leaves the same request waiting_lock"
    );
    // ── Assertion 3: documented authorized cancel outcome and typed failure ─
    fixture.guard.mark("park: shell cancels the parked request");
    renew_holder_and_check_victim(
        fixture,
        &conn1,
        tx_handle,
        parked,
        deadline,
        "before authorized cancel",
    );
    let cap = remaining(deadline).min(FRAME_BOUND).min(STEP_BOUND);
    assert!(
        !cap.is_zero(),
        "active deadline expired before cancel child"
    );
    let cancel_run = checked_shell_timeout(
        fixture,
        &needles,
        &["activity", "cancel", &parked_id, "--yes"],
        cap,
    );
    // No renewal here: the accepted cancel is what ends the victim, and the
    // join below asserts its typed failure.
    let cancel = data(&cancel_run, "cancel");
    keys_exact(&cancel, &["outcome"], "activity cancel");
    assert_eq!(
        cancel["outcome"], "cancelled",
        "the server must accept the cancellation"
    );
    assert!(Instant::now() < deadline, "active deadline expired");
    let victim = cleanup.victim.take().expect("victim guard installed");
    let cleanup_deadline = cleanup.cleanup_deadline;
    let joined = fixture.rt.run_timeout(remaining(deadline), async move {
        let mut victim = victim;
        match tokio::time::timeout(remaining(deadline), &mut victim).await {
            Ok(result) => Ok(result),
            Err(_) => {
                victim.abort();
                let cap = cleanup_deadline.saturating_duration_since(Instant::now());
                let _ = tokio::time::timeout(cap, &mut victim).await;
                Err(())
            }
        }
    });
    if joined.is_err() {
        panic!("the parked request task did not join before active deadline");
    }
    let joined = joined.expect("victim join result present after successful wait");
    let reply = match joined {
        Ok(reply) => reply,
        Err(_) => panic!("the parked request task failed to join"),
    };
    match reply {
        DbResponse::Error { code, .. } => assert_eq!(
            code, "cancelled",
            "the parked request must fail with the typed `cancelled` code"
        ),
        DbResponse::Batch { .. } => panic!("the cancelled request unexpectedly completed"),
        _other => panic!("the parked request failed with an unexpected typed error"),
    }
    // ── Assertion 3: no session material in streams or the profile home ─
    fixture.guard.mark("park: leak scan");
    scan_profile_home(fixture, &needles);
    fixture.guard.mark("park: rollback and close session A");
    let mut conn1 = conn1;
    conn1.deadline = cleanup.cleanup_deadline;
    let conn1 = rollback_tx(fixture, conn1, tx_handle);
    cleanup.tx_handle = None;
    let holder_state = conn1.state.clone();
    close_session(fixture, conn1);
    cleanup.holder = None;
    drop(cleanup);
    super::access_activity_holders::teardown_deadline::finish_until(
        &mut fixture_guard.fixture,
        holder_state,
        start + TOTAL_BOUND,
    );
}
