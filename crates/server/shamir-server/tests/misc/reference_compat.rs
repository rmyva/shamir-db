//! Reference data-directory compatibility test (design
//! `docs/dev-artifacts/design/migration-framework.md` 7.3).
//!
//! The committed fixture `tests/misc/fixtures/reference_epoch_1/` is a
//! data directory written by a real released-epoch binary (generator:
//! `scripts/runners/make_reference_datadir.py`, CI:
//! `.github/workflows/reference-datadir.yml`, artifact
//! `reference-epoch-1`). The tests prove: the stamp is at the recorded
//! epoch, `format-migrate` is a byte-identical no-op today, and a fresh
//! server opens the directory with all scenario content intact (WAL
//! recovery included).
//!
//! Scenario contract (mirrored in
//! `tests/misc/fixtures/reference_epoch_1/README.md`): db `default`,
//! repo `main`; tables `ref_schema_basic` (3 rows r1/r2/r3 → n 10/20/30,
//! schema_version ≥ 1) and `ref_schema_versioned` (2 rows v1/v2 → n 7/8,
//! schema_version > 1); one index of each kind on `ref_schema_basic`
//! named `ref_hash_idx`, `ref_sorted_idx`, `ref_vector_idx`,
//! `ref_fulltext_idx`; functions `ref_wasm_fn` (compiled) and
//! `ref_lang_fn` (`shamir` source, checked stored call with signature,
//! `ref_lang_fn(x) = x + 1`); principals `admin` (superuser) and
//! `ref_user` (read on `default.main`), both with fixed passwords; one
//! access token for `ref_user`; an unfinished WAL segment; a valid
//! `format.stamp` at epoch 1.

use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shamir_types::types::value::QueryValue;
use tempfile::TempDir;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use shamir_connect::client::handshake::{HandshakeBuilder, ServerAuthOk, ServerChallenge};
use shamir_connect::common::envelope::{RequestEnvelope, ResponseEnvelope};
use shamir_connect::common::kdf_params::KdfParams;
use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::common::username::NormalizedUsername;
use shamir_transport_tcp::framing::{read_frame, write_frame, MAX_FRAME_SIZE_DEFAULT};
use shamir_transport_tcp::tls::{
    extract_tls_exporter, make_client_config_app_authenticated, AppLayerServerAuth,
};

use shamir_server::db_handler::{DbRequest, DbResponse};
use shamir_server::migrate::{run as run_format_migrate, FormatMigrateArgs};
use shamir_sync::format_stamp::{
    inspect as inspect_stamp, StampState, BASE_FORMAT_EPOCH, CURRENT_FORMAT_EPOCH,
};

use crate::common::{make_test_config, spawn_with_password};
use shamir_test_diag::{try_install_global_tracing_ring, FlakeGuard};

/// Bootstrap password of the fixture's `admin` superuser (see the
/// fixture README contract).
const FIXTURE_ADMIN_PASSWORD: &[u8] = b"reference-epoch-1-admin";
/// Password of the fixture's secondary principal `ref_user`.
const FIXTURE_REF_USER_PASSWORD: &[u8] = b"reference-epoch-1-user";
/// Env var pointing at a freshly generated reference directory; when
/// set, the fresh run is compared semantically against the committed
/// fixture (CI re-generation check, design 7.2).
const FRESH_DIR_ENV: &str = "SHAMIR_REFERENCE_FRESH_DIR";
/// Committed fixture directory (populated in CI; never hand-edited).
const FIXTURE_REL: &str = "tests/misc/fixtures/reference_epoch_1";

// --------------------------------------------------------------------------
// Wire mirrors (same shapes as e2e/core/persistence_e2e.rs)
// --------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct WireAuthInit {
    user: String,
    #[serde(with = "serde_bytes")]
    client_nonce: Vec<u8>,
    binding_mode: u8,
    version: u8,
}

#[derive(Serialize, Deserialize)]
struct WireChallenge {
    #[serde(with = "serde_bytes")]
    salt: Vec<u8>,
    memory_kb: u32,
    time: u32,
    parallelism: u32,
    argon2_version: u8,
    #[serde(with = "serde_bytes")]
    server_nonce: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct WireClientProof {
    #[serde(with = "serde_bytes")]
    client_proof: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct WireAuthOk {
    #[serde(with = "serde_bytes")]
    server_signature: Vec<u8>,
    #[serde(with = "serde_bytes")]
    server_pub_key: Vec<u8>,
    #[serde(with = "serde_bytes")]
    identity_sig: Vec<u8>,
    #[serde(with = "serde_bytes")]
    session_id: Vec<u8>,
    expires_at_ns: u64,
    #[serde(default, with = "serde_bytes")]
    resumption_ticket: Vec<u8>,
    #[serde(default)]
    resumption_expires_at_ns: u64,
    #[serde(default)]
    server_query_version: u8,
}

// --------------------------------------------------------------------------
// Fixture handling
// --------------------------------------------------------------------------

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_REL)
}

/// The fixture is a required acceptance input, not an optional skip:
/// a missing fixture is a hard failure naming the producer workflow.
fn require_fixture() -> PathBuf {
    let dir = fixture_dir();
    assert!(
        dir.is_dir(),
        "reference fixture missing at {}; it is generated by \
         scripts/runners/make_reference_datadir.py and uploaded by \
         .github/workflows/reference-datadir.yml as artifact reference-epoch-1; \
         download the artifact and commit its content at this path",
        dir.display()
    );
    dir
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).expect("create copy target dir");
    for entry in fs::read_dir(src).expect("read source dir") {
        let entry = entry.expect("dir entry");
        let target = dst.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir_recursive(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).expect("copy file");
        }
    }
}

/// Copy the committed fixture into a fresh temp dir (the caller keeps
/// the `TempDir` alive for the duration of the test).
fn copy_fixture_to_temp() -> TempDir {
    let src = require_fixture();
    let temp = TempDir::new().expect("tempdir");
    copy_dir_recursive(&src, temp.path());
    temp
}

/// Recursive (rel path, bytes) listing; `.shamir.lock` is excluded (the
/// offline command may transiently hold it).
fn content_snapshot(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    snapshot_walk(dir, "", &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn snapshot_walk(dir: &Path, prefix: &str, out: &mut Vec<(String, Vec<u8>)>) {
    let Ok(read) = fs::read_dir(dir) else {
        return;
    };
    for entry in read.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let rel = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        if rel == ".shamir.lock" {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            snapshot_walk(&path, &rel, out);
        } else {
            let bytes = fs::read(&path).unwrap_or_default();
            out.push((rel, bytes));
        }
    }
}

// --------------------------------------------------------------------------
// Offline: stamp + format-migrate no-op
// --------------------------------------------------------------------------

/// Inspect check: the fixture stamp is a valid stamp at the recorded
/// epoch (epoch 1 == BASE == CURRENT today).
#[test]
fn reference_stamp_is_current_epoch() {
    let temp = copy_fixture_to_temp();
    let stamp = match inspect_stamp(temp.path()) {
        StampState::Valid(stamp) => stamp,
        other => panic!("fixture stamp is not a valid stamp: {other:?}"),
    };
    assert_eq!(stamp.format_epoch(), 1);
    assert_eq!(BASE_FORMAT_EPOCH, 1);
    assert_eq!(CURRENT_FORMAT_EPOCH, 1);
    assert_eq!(BASE_FORMAT_EPOCH, CURRENT_FORMAT_EPOCH);
}

/// The migration command path must report "nothing to do" today and
/// leave the copy byte-identical (the chain is empty while
/// CURRENT == BASE).
#[test]
fn format_migrate_noop_is_byte_identical() {
    let temp = copy_fixture_to_temp();
    let before = content_snapshot(temp.path());

    let config = make_test_config(&temp, "127.0.0.1:0");
    let args = FormatMigrateArgs {
        plan: false,
        from_backup: None,
        max_backup_age_hours: 24,
        i_have_a_backup: false,
        yes: false,
    };
    run_format_migrate(&config, &args).expect("format-migrate must succeed with nothing to do");

    let after = content_snapshot(temp.path());
    assert_eq!(before, after, "no-op format-migrate must not touch content");
    // The stamp must still be valid and at the current epoch afterwards.
    let stamp = match inspect_stamp(temp.path()) {
        StampState::Valid(stamp) => stamp,
        other => panic!("stamp invalid after no-op format-migrate: {other:?}"),
    };
    assert_eq!(stamp.format_epoch(), CURRENT_FORMAT_EPOCH);
}

// --------------------------------------------------------------------------
// Online: open-and-compare (semantic)
// --------------------------------------------------------------------------

/// Everything the semantic comparison observes about one opened
/// directory. Two directories match when all fields are equal; volatile
/// server-written state (stamp timestamps, TLS material, cache/) is not
/// observed here.
#[derive(Debug, Default)]
struct SemanticView {
    basic_rows: Vec<(String, i64)>,
    versioned_rows: Vec<(String, i64)>,
    basic_schema_version: i64,
    versioned_schema_version: i64,
    index_names: Vec<String>,
    wasm_fn_loaded: bool,
    wasm_fn_lang: Option<String>,
    lang_fn_answer: Option<i64>,
    access_tree_present: bool,
    ref_user_rows_ok: bool,
    stamp_epoch: u64,
}

/// What the committed fixture must contain per the scenario contract.
fn assert_matches_contract(view: &SemanticView) {
    assert_eq!(
        view.basic_rows,
        vec![
            ("r1".to_owned(), 10),
            ("r2".to_owned(), 20),
            ("r3".to_owned(), 30)
        ],
        "ref_schema_basic rows"
    );
    assert_eq!(
        view.versioned_rows,
        vec![("v1".to_owned(), 7), ("v2".to_owned(), 8)],
        "ref_schema_versioned rows"
    );
    assert!(
        view.basic_schema_version > 0,
        "basic table must have a schema"
    );
    assert!(
        view.versioned_schema_version > 1,
        "versioned table must have schema_version > 1"
    );
    assert_eq!(
        view.index_names,
        vec![
            "ref_fulltext_idx".to_owned(),
            "ref_hash_idx".to_owned(),
            "ref_sorted_idx".to_owned(),
            "ref_vector_idx".to_owned(),
        ],
        "one index meta entry per kind (hash, sorted, vector, full-text)"
    );
    assert!(
        view.wasm_fn_loaded,
        "ref_wasm_fn must be present and loaded"
    );
    assert!(
        view.wasm_fn_lang.is_some(),
        "ref_wasm_fn must report its language"
    );
    assert_eq!(
        view.lang_fn_answer,
        Some(42),
        "ref_lang_fn signature-backed stored call must return x + 1"
    );
    assert!(
        view.access_tree_present,
        "rights (ACL tree) must be readable"
    );
    assert!(
        view.ref_user_rows_ok,
        "ref_user principal must authenticate and read per its rights"
    );
    assert_eq!(
        view.stamp_epoch, CURRENT_FORMAT_EPOCH,
        "stamp epoch must be CURRENT after open-and-recovery"
    );
}

/// Fresh-vs-committed equality (volatile fields are not observed).
fn assert_views_equal(committed: &SemanticView, fresh: &SemanticView) {
    assert_eq!(committed.basic_rows, fresh.basic_rows, "basic rows");
    assert_eq!(
        committed.versioned_rows, fresh.versioned_rows,
        "versioned rows"
    );
    assert_eq!(
        committed.basic_schema_version, fresh.basic_schema_version,
        "basic schema_version"
    );
    assert_eq!(
        committed.versioned_schema_version, fresh.versioned_schema_version,
        "versioned schema_version"
    );
    assert_eq!(committed.index_names, fresh.index_names, "index names");
    assert_eq!(
        committed.wasm_fn_loaded, fresh.wasm_fn_loaded,
        "wasm fn loaded"
    );
    assert_eq!(committed.wasm_fn_lang, fresh.wasm_fn_lang, "wasm fn lang");
    assert_eq!(
        committed.lang_fn_answer, fresh.lang_fn_answer,
        "lang fn call"
    );
    assert_eq!(
        committed.access_tree_present, fresh.access_tree_present,
        "access tree presence"
    );
    assert_eq!(
        committed.ref_user_rows_ok, fresh.ref_user_rows_ok,
        "ref_user access"
    );
    assert_eq!(committed.stamp_epoch, fresh.stamp_epoch, "stamp epoch");
}

/// Drive a SCRAM client through to authentication (same flow as
/// e2e/core/persistence_e2e.rs).
async fn login(
    addr: SocketAddr,
    username: &str,
    password: &[u8],
) -> (
    ReadHalf<tokio_rustls::client::TlsStream<TcpStream>>,
    WriteHalf<tokio_rustls::client::TlsStream<TcpStream>>,
    [u8; 32],
) {
    let user = NormalizedUsername::from_raw(username).expect("username");
    let client_cfg =
        make_client_config_app_authenticated(AppLayerServerAuth::exporter_bound()).unwrap();
    let connector = TlsConnector::from(client_cfg);
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let tcp = TcpStream::connect(addr).await.expect("tcp connect");
    let tls = connector
        .connect(server_name, tcp)
        .await
        .expect("tls handshake");
    let exporter = extract_tls_exporter(&tls).expect("exporter");

    let (mut r, mut w) = split(tls);

    let hs = HandshakeBuilder::new(user, TransportKind::Tcp, BindingMode::TlsExporter)
        .tls_exporter(exporter)
        .accept_new_host(true)
        .build()
        .expect("handshake builder");

    let init = hs.auth_init();
    let init_wire = WireAuthInit {
        user: init.user,
        client_nonce: init.client_nonce.to_vec(),
        binding_mode: init.binding_mode,
        version: init.version,
    };
    write_frame(&mut w, &rmp_serde::to_vec(&init_wire).unwrap())
        .await
        .expect("send auth_init");

    let ch_bytes = read_frame(&mut r, MAX_FRAME_SIZE_DEFAULT)
        .await
        .expect("read challenge");
    let ch_wire: WireChallenge = rmp_serde::from_slice(&ch_bytes).unwrap();
    let mut salt = [0u8; 16];
    salt.copy_from_slice(&ch_wire.salt);
    let mut server_nonce = [0u8; 32];
    server_nonce.copy_from_slice(&ch_wire.server_nonce);
    let challenge = ServerChallenge {
        salt,
        kdf_params: KdfParams {
            memory_kb: ch_wire.memory_kb,
            time: ch_wire.time,
            parallelism: ch_wire.parallelism,
            argon2_version: ch_wire.argon2_version,
        },
        server_nonce,
    };

    let mut password_buf = password.to_vec();
    let (proof, derived, am) = hs
        .process_challenge(&challenge, &mut password_buf)
        .expect("process challenge");
    let proof_wire = WireClientProof {
        client_proof: proof.to_vec(),
    };
    write_frame(&mut w, &rmp_serde::to_vec(&proof_wire).unwrap())
        .await
        .expect("send proof");

    let ok_bytes = read_frame(&mut r, MAX_FRAME_SIZE_DEFAULT)
        .await
        .expect("read auth_ok");
    let ok_wire: WireAuthOk = rmp_serde::from_slice(&ok_bytes).unwrap();
    let mut sig32 = [0u8; 32];
    sig32.copy_from_slice(&ok_wire.server_signature);
    let mut pub32 = [0u8; 32];
    pub32.copy_from_slice(&ok_wire.server_pub_key);
    let mut id_sig = [0u8; 64];
    id_sig.copy_from_slice(&ok_wire.identity_sig);
    let mut session_id = [0u8; 32];
    session_id.copy_from_slice(&ok_wire.session_id);
    let auth_ok = ServerAuthOk {
        server_signature: sig32,
        server_pub_key: pub32,
        identity_sig: id_sig,
        session_id,
        expires_at_ns: ok_wire.expires_at_ns,
        resumption_ticket: Some(ok_wire.resumption_ticket.clone()),
        resumption_expires_at_ns: Some(ok_wire.resumption_expires_at_ns),
        rotation_in_progress: None,
        kdf_upgrade_required: None,
    };
    let _success = hs
        .process_auth_ok(&auth_ok, &derived, &am, |_pin| {})
        .expect("process auth_ok");

    (r, w, session_id)
}

async fn roundtrip<R, W>(
    req: &DbRequest,
    sid: [u8; 32],
    rid: u32,
    w: &mut W,
    r: &mut R,
) -> DbResponse
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let bytes = rmp_serde::to_vec_named(req).expect("encode req");
    let envelope = RequestEnvelope::new(sid, Some(rid), bytes);
    let envelope_bytes = envelope.to_msgpack().expect("envelope encode");
    write_frame(w, &envelope_bytes).await.expect("send request");
    let resp_bytes = tokio::time::timeout(
        Duration::from_secs(30),
        read_frame(r, MAX_FRAME_SIZE_DEFAULT),
    )
    .await
    .expect("response within 30s")
    .expect("read response");
    let resp_envelope = ResponseEnvelope::from_msgpack(&resp_bytes).expect("response envelope");
    rmp_serde::from_slice(&resp_envelope.res).expect("decode DbResponse")
}

async fn execute<R, W>(
    req: DbRequest,
    sid: [u8; 32],
    rid: u32,
    w: &mut W,
    r: &mut R,
) -> shamir_db::query::BatchResponse
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    match roundtrip(&req, sid, rid, w, r).await {
        DbResponse::Batch { response } => response,
        other => panic!("execute failed: {other:?}"),
    }
}

fn read_req(table: &str) -> DbRequest {
    let mut b = shamir_query_builder::batch::Batch::new();
    b.id("rd");
    b.query("rd", shamir_query_builder::Query::from(table));
    DbRequest::Execute {
        query_version: shamir_server::version::CURRENT_QUERY_LANG_VERSION,
        db: "default".into(),
        batch: b.build(),
    }
}

fn describe_req(table: &str) -> DbRequest {
    let mut b = shamir_query_builder::batch::Batch::new();
    b.id("dsc");
    b.describe_table("dsc", shamir_query_builder::ddl::describe_table(table));
    DbRequest::Execute {
        query_version: shamir_server::version::CURRENT_QUERY_LANG_VERSION,
        db: "default".into(),
        batch: b.build(),
    }
}

fn describe_function_req(name: &str) -> DbRequest {
    let mut b = shamir_query_builder::batch::Batch::new();
    b.id("dfn");
    b.describe_function("dfn", shamir_query_builder::ddl::describe_function(name));
    DbRequest::Execute {
        query_version: shamir_server::version::CURRENT_QUERY_LANG_VERSION,
        db: "default".into(),
        batch: b.build(),
    }
}

fn call_lang_fn_req(arg: i64) -> DbRequest {
    let mut b = shamir_query_builder::batch::Batch::new();
    b.id("fn_call");
    b.call(
        "fn_call",
        "ref_lang_fn",
        [shamir_query_builder::FilterValue::from(arg)],
    );
    DbRequest::Execute {
        query_version: shamir_server::version::CURRENT_QUERY_LANG_VERSION,
        db: "default".into(),
        batch: b.build(),
    }
}

fn access_tree_req() -> DbRequest {
    let mut b = shamir_query_builder::batch::Batch::new();
    b.id("tree");
    b.access_tree("tree", shamir_query_builder::ddl::access_tree().depth(3));
    DbRequest::Execute {
        query_version: shamir_server::version::CURRENT_QUERY_LANG_VERSION,
        db: "default".into(),
        batch: b.build(),
    }
}

fn rows_of(resp: &shamir_db::query::BatchResponse, alias: &str) -> Vec<(String, i64)> {
    let rd = resp
        .results
        .get(alias)
        .unwrap_or_else(|| panic!("alias {alias} missing"));
    let mut rows = Vec::new();
    for record in &rd.records {
        let k = record
            .get_value_str("k")
            .unwrap_or_else(|| panic!("row missing key field 'k' in {alias}"))
            .to_owned();
        let n = record
            .get_value_i64("n")
            .unwrap_or_else(|| panic!("row missing field 'n' in {alias}"));
        rows.push((k, n));
    }
    rows.sort();
    rows
}

fn describe_map(resp: &shamir_db::query::BatchResponse, alias: &str) -> QueryValue {
    let rd = resp
        .results
        .get(alias)
        .unwrap_or_else(|| panic!("alias {alias} missing"));
    rd.records[0].as_value().as_ref().clone()
}

fn schema_version_of(desc: &QueryValue) -> i64 {
    desc.get("schema_version")
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
}

fn index_names_of(desc: &QueryValue) -> Vec<String> {
    let mut names = Vec::new();
    if let Some(indexes) = desc.get("indexes").and_then(|v| v.as_array()) {
        for idx in indexes {
            if let Some(name) = idx.get("name").and_then(|v| v.as_str()) {
                names.push(name.to_owned());
            }
        }
    }
    names.sort();
    names
}

/// Open one reference directory with a freshly started server, observe
/// every contract point, shut down, and return the semantic view (the
/// trailing stamp epoch proves the stamp survived the open; the rows
/// surviving the open prove the WAL was recovered).
async fn open_and_observe(data_dir: &TempDir) -> SemanticView {
    let mut view = SemanticView::default();
    // Failure-only evidence: the server's tracing events say why a login
    // was dropped (the client only sees the connection close).
    try_install_global_tracing_ring();
    let guard = FlakeGuard::new("reference_compat open_and_observe");

    let handle = spawn_with_password(data_dir, FIXTURE_ADMIN_PASSWORD, "127.0.0.1:0").await;
    let addr = handle.first_tls_exporter_addr().expect("bound addr");
    guard.mark("server started");

    // Admin: rows, schema, indexes, functions, rights tree.
    {
        guard.mark("admin login");
        let (mut r, mut w, sid) = login(addr, "admin", FIXTURE_ADMIN_PASSWORD).await;
        guard.mark("admin logged in");
        let resp = execute(read_req("ref_schema_basic"), sid, 1, &mut w, &mut r).await;
        view.basic_rows = rows_of(&resp, "rd");
        let resp = execute(read_req("ref_schema_versioned"), sid, 2, &mut w, &mut r).await;
        view.versioned_rows = rows_of(&resp, "rd");
        let resp = execute(describe_req("ref_schema_basic"), sid, 3, &mut w, &mut r).await;
        let desc = describe_map(&resp, "dsc");
        view.basic_schema_version = schema_version_of(&desc);
        view.index_names = index_names_of(&desc);
        let resp = execute(describe_req("ref_schema_versioned"), sid, 4, &mut w, &mut r).await;
        view.versioned_schema_version = schema_version_of(&describe_map(&resp, "dsc"));
        let resp = execute(describe_function_req("ref_wasm_fn"), sid, 5, &mut w, &mut r).await;
        let desc = describe_map(&resp, "dfn");
        view.wasm_fn_loaded = desc
            .get("state")
            .and_then(|s| s.get("kind"))
            .and_then(|v| v.as_str())
            == Some("loaded");
        view.wasm_fn_lang = desc.get("lang").and_then(|v| v.as_str()).map(str::to_owned);
        let resp = execute(call_lang_fn_req(41), sid, 6, &mut w, &mut r).await;
        view.lang_fn_answer = resp
            .results
            .get("fn_call")
            .and_then(|qr| qr.value.as_ref())
            .and_then(|v| v.as_i64());
        let resp = execute(access_tree_req(), sid, 7, &mut w, &mut r).await;
        view.access_tree_present = resp.results.contains_key("tree");
        let _ = w.shutdown().await;
        let mut drain = [0u8; 1];
        let _ = r.read(&mut drain).await;
    }

    // Secondary principal: authentication + rights over the wire.
    {
        guard.mark("ref_user login");
        let (mut r, mut w, sid) = login(addr, "ref_user", FIXTURE_REF_USER_PASSWORD).await;
        guard.mark("ref_user logged in");
        let resp = execute(read_req("ref_schema_basic"), sid, 1, &mut w, &mut r).await;
        view.ref_user_rows_ok = rows_of(&resp, "rd") == view.basic_rows;
        let _ = w.shutdown().await;
        let mut drain = [0u8; 1];
        let _ = r.read(&mut drain).await;
    }

    handle.shutdown().await;

    let stamp = match inspect_stamp(data_dir.path()) {
        StampState::Valid(stamp) => stamp,
        other => panic!("stamp invalid after server open: {other:?}"),
    };
    view.stamp_epoch = stamp.format_epoch();
    view
}

/// Open the committed fixture with a freshly started server and prove
/// the scenario content is intact: WAL recovered (rows written through
/// the unfinished WAL are present), per-table rows/values, schema
/// versions, index meta per kind, function rows (compiled loaded,
/// signature-backed stored call), rights tree, principals, stamp at
/// CURRENT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reference_dir_opens_semantically() {
    let temp = copy_fixture_to_temp();
    let view = open_and_observe(&temp).await;
    assert_matches_contract(&view);
}

/// CI re-generation check (design 7.2): when SHAMIR_REFERENCE_FRESH_DIR
/// points at a directory produced by a fresh generator run, it must
/// match the committed fixture semantically — same rows, schema
/// versions, indexes, functions, principals, stamp epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_run_matches_committed_fixture() {
    let Ok(fresh) = env::var(FRESH_DIR_ENV) else {
        return; // committed-fixture mode; the fresh check runs only in CI
    };
    let fresh_src = PathBuf::from(&fresh);
    assert!(
        fresh_src.is_dir(),
        "{FRESH_DIR_ENV}={fresh} is not a directory"
    );
    let fresh_temp = TempDir::new().expect("tempdir");
    copy_dir_recursive(&fresh_src, fresh_temp.path());

    let committed_temp = copy_fixture_to_temp();
    let committed = open_and_observe(&committed_temp).await;
    let fresh_view = open_and_observe(&fresh_temp).await;
    assert_views_equal(&committed, &fresh_view);
}
