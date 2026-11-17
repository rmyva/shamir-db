//! Reference fixture loader: connects to a RUNNING ShamirDB server as the
//! bootstrap admin and applies a fixed DDL + data scenario.
//!
//! Mirrors `scripts/runners/make_reference_datadir.py`'s fixture intent, but
//! drives a live server through `shamir-client` + the typed
//! `shamir-query-builder` surface (no raw JSON anywhere).
//!
//! CLI contract:
//!
//! ```text
//! reference_fixture --addr <ip:port> --credentials-file <path>
//! ```
//!
//! The credentials file holds exactly two lines, `username=<n>` and
//! `password=<b>`; they are read into `Zeroizing` buffers and never logged.
//!
//! Success prints `REFERENCE_FIXTURE_OK` and exits 0; any failure prints a
//! single reason line to stderr and exits 1.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use zeroize::Zeroizing;

use shamir_client::builder::batch::Batch;
use shamir_client::builder::ddl::{self, GroupRef};
use shamir_client::builder::val::lit;
use shamir_client::builder::write::{doc, insert};
use shamir_client::builder::Query;
use shamir_client::{Client, ConnectOptions};
use shamir_connect::common::crypto::derive_session_hmac_key;
use shamir_query_types::admin::{AddGroupMemberOp, ChmodOp, ChownOp, CreateGroupOp};
use shamir_query_types::batch::BatchOp;
use shamir_query_types::hmac;
use shamir_types::access::principal64;
use shamir_types::types::value::QueryValue;

const DB: &str = "default";
const REPO: &str = "main";

const BASIC_TABLE: &str = "ref_schema_basic";
const VERSIONED_TABLE: &str = "ref_schema_versioned";

const REF_USER: &str = "ref_user";
const REF_USER_PASSWORD: &str = "reference-epoch-1-user";
const REF_GROUP: &str = "ref_group";

const LANG_FN: &str = "ref_lang_fn";
const WASM_FN: &str = "ref_wasm_fn";

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => {
            println!("REFERENCE_FIXTURE_OK");
            ExitCode::SUCCESS
        }
        Err(reason) => {
            eprintln!("reference_fixture failed: {reason}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let args = Args::parse()?;
    let creds = Credentials::read(&args.credentials_file)?;

    // The generator's TCP listener is the loopback-only plain profile.
    let client = Client::connect_plain(ConnectOptions {
        addr: args.addr,
        server_name: "localhost".into(),
        username: creds.username,
        password: creds.password,
        accept_new_host: true,
        trusted_pin: None,
        connect_timeout: None,
        handshake_timeout: None,
        request_timeout: None,
    })
    .await
    .map_err(|e| format!("connect failed: {e}"))?;

    apply_ddl(&client).await?;
    apply_admin(&client).await?;
    verify(&client).await?;

    client.close().await;
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
// CLI parsing
// ═══════════════════════════════════════════════════════════════════════

struct Args {
    addr: SocketAddr,
    credentials_file: PathBuf,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut addr = None;
        let mut credentials_file = None;
        let mut iter = std::env::args().skip(1);
        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--addr" => {
                    let raw = iter.next().ok_or("--addr requires a value")?;
                    addr = Some(
                        raw.parse::<SocketAddr>()
                            .map_err(|e| format!("--addr '{raw}' is not ip:port: {e}"))?,
                    );
                }
                "--credentials-file" => {
                    let raw = iter.next().ok_or("--credentials-file requires a value")?;
                    credentials_file = Some(PathBuf::from(raw));
                }
                other => return Err(format!("unknown argument '{other}'")),
            }
        }
        Ok(Self {
            addr: addr.ok_or("missing required --addr <ip:port>")?,
            credentials_file: credentials_file
                .ok_or("missing required --credentials-file <path>")?,
        })
    }
}

struct Credentials {
    username: String,
    password: Zeroizing<Vec<u8>>,
}

impl Credentials {
    /// Read `username=<n>` / `password=<b>` lines. Values are never logged.
    fn read(path: &PathBuf) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read credentials file {}: {e}", path.display()))?;
        let mut username = None;
        let mut password = None;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some(value) = line.strip_prefix("username=") {
                username = Some(value.to_owned());
            } else if let Some(value) = line.strip_prefix("password=") {
                password = Some(Zeroizing::new(value.as_bytes().to_vec()));
            }
        }
        Ok(Self {
            username: username.ok_or("credentials file missing 'username=' line")?,
            password: password.ok_or("credentials file missing 'password=' line")?,
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════
// HMAC confirmation for admin batch ops
// ═══════════════════════════════════════════════════════════════════════

/// Compute the session-bound confirmation tag for a typed admin op. The
/// canonical form is derived from the UNSIGNED op (`hmac: None`), exactly
/// like `Client::create_scram_user` / the drop-table path in
/// `tests/cursor/lifecycle.rs`.
fn sign_op(client: &Client, op: &BatchOp) -> Result<String, String> {
    let key = derive_session_hmac_key(&client.session_id());
    let canonical = hmac::canonical_batch_op(DB, op)
        .map_err(|e| format!("canonical op encoding failed: {e:?}"))?;
    Ok(hmac::compute_tag_hex(&key, &canonical))
}

/// Build a signed batch op from its unsigned DTO form and a typed builder
/// closure that attaches the tag.
fn signed(
    client: &Client,
    unsigned: &BatchOp,
    attach: impl FnOnce(String) -> Result<BatchOp, String>,
) -> Result<BatchOp, String> {
    attach(sign_op(client, unsigned)?)
}

// ═══════════════════════════════════════════════════════════════════════
// Phase 1-2 + 3-4: DDL, data, indexes, functions
// ═══════════════════════════════════════════════════════════════════════

async fn execute(client: &Client, batch: Batch, what: &'static str) -> Result<(), String> {
    client
        .execute(DB, batch.build())
        .await
        .map_err(|e| format!("{what} failed: {e}"))?;
    Ok(())
}

async fn apply_ddl(client: &Client) -> Result<(), String> {
    // ── tables + schema version bump ─────────────────────────────
    let mut batch = Batch::new();
    batch.id("fixture-tables");
    batch.create_table(
        "create-basic",
        ddl::create_table(BASIC_TABLE).repo(REPO).schema([
            ddl::field(["k"]).string().required().build(),
            ddl::field(["n"]).int().build(),
            ddl::field(["vec"]).list().array_of("f64").build(),
        ]),
    );
    batch.create_table(
        "create-versioned",
        ddl::create_table(VERSIONED_TABLE).repo(REPO).schema([
            ddl::field(["k"]).string().required().build(),
            ddl::field(["n"]).int().build(),
        ]),
    );
    // `create_table` stores the schema at version 0; each `set_table_schema`
    // bumps it by one. The compatibility test expects basic >= 1, versioned > 1.
    batch.set_table_schema(
        "bump-basic",
        ddl::set_table_schema(BASIC_TABLE)
            .repo(REPO)
            .rules([
                ddl::field(["k"]).string().required().build(),
                ddl::field(["n"]).int().build(),
                ddl::field(["vec"]).list().array_of("f64").build(),
            ])
            .expected_version(0),
    );
    batch.set_table_schema(
        "bump-versioned-1",
        ddl::set_table_schema(VERSIONED_TABLE)
            .repo(REPO)
            .rules([
                ddl::field(["k"]).string().required().build(),
                ddl::field(["n"]).int().build(),
            ])
            .expected_version(0),
    );
    batch.set_table_schema(
        "bump-versioned-2",
        ddl::set_table_schema(VERSIONED_TABLE)
            .repo(REPO)
            .rules([
                ddl::field(["k"]).string().required().build(),
                ddl::field(["n"]).int().build(),
                ddl::field(["note"]).string().build(),
            ])
            .expected_version(1),
    );
    execute(client, batch, "table DDL").await?;

    // ── rows ─────────────────────────────────────────────────────
    let vec_of =
        |values: &[f64]| QueryValue::List(values.iter().map(|v| QueryValue::F64(*v)).collect());
    let rows = [
        ("r1", 10i64, [0.5, 0.25, 0.125, 0.0625]),
        ("r2", 20, [0.1, 0.2, 0.3, 0.4]),
        ("r3", 30, [1.0, 0.75, 0.5, 0.25]),
    ];
    let mut batch = Batch::new();
    batch.id("fixture-rows");
    batch.insert(
        "insert-basic",
        insert(BASIC_TABLE).rows(rows.iter().map(|(k, n, vec)| {
            doc()
                .set_value("k", *k)
                .set_value("n", *n)
                .set_value("vec", vec_of(vec))
                .build()
        })),
    );
    batch.insert(
        "insert-versioned",
        insert(VERSIONED_TABLE).rows([
            doc().set_value("k", "v1").set_value("n", 7i64).build(),
            doc().set_value("k", "v2").set_value("n", 8i64).build(),
        ]),
    );
    execute(client, batch, "row inserts").await?;

    // ── indexes ──────────────────────────────────────────────────
    use ddl::{Metric, Quantization, Tokenizer};
    let mut batch = Batch::new();
    batch.id("fixture-indexes");
    batch.create_index(
        "idx-hash",
        ddl::create_index("ref_hash_idx", BASIC_TABLE)
            .repo(REPO)
            .hash(vec![vec!["k".into()]])
            .map_err(|e| format!("hash index spec invalid: {e}"))?,
    );
    batch.create_index(
        "idx-sorted",
        ddl::create_index("ref_sorted_idx", BASIC_TABLE)
            .repo(REPO)
            .sorted_index(vec!["n".into()])
            .map_err(|e| format!("sorted index spec invalid: {e}"))?,
    );
    batch.create_index(
        "idx-vector",
        ddl::create_index("ref_vector_idx", BASIC_TABLE)
            .repo(REPO)
            .vector(
                vec!["vec".into()],
                std::num::NonZeroU32::new(4).expect("dim 4"),
                Metric::Cosine,
                Quantization::Off,
            )
            .map_err(|e| format!("vector index spec invalid: {e}"))?,
    );
    batch.create_index(
        "idx-fulltext",
        ddl::create_index("ref_fulltext_idx", BASIC_TABLE)
            .repo(REPO)
            .fts(vec!["k".into()], Tokenizer::Whitespace)
            .map_err(|e| format!("fts index spec invalid: {e}"))?,
    );
    execute(client, batch, "index DDL").await?;

    // ── functions ────────────────────────────────────────────────
    let mut batch = Batch::new();
    batch.id("fixture-functions");
    batch.create_function(
        "create-lang-fn",
        ddl::create_function(LANG_FN)
            .lang("shamir")
            .source("@entry scalar ref_lang_fn(x: i64) -> i64 { return x + 1; }")
            .build()
            .map_err(|e| format!("lang function spec invalid: {e}"))?,
    );
    batch.create_function(
        "create-wasm-fn",
        ddl::create_function(WASM_FN)
            .lang("wasm")
            .wasm(base64_encode(&ref_wasm_fn_bytes()))
            .build()
            .map_err(|e| format!("wasm function spec invalid: {e}"))?,
    );
    execute(client, batch, "function DDL").await
}

// ═══════════════════════════════════════════════════════════════════════
// Phase 5: admin — user, chmod, group, chown
// ═══════════════════════════════════════════════════════════════════════

async fn apply_admin(client: &Client) -> Result<(), String> {
    // SCRAM user via the client's typed method — the HMAC confirmation is
    // computed from the session key inside `create_scram_user` itself.
    let user_id = client
        .create_scram_user(
            REF_USER,
            Zeroizing::new(REF_USER_PASSWORD.to_owned()),
            vec!["reader".into()],
        )
        .await
        .map_err(|e| format!("create_scram_user failed: {e}"))?;
    let user_id: [u8; 16] = user_id.as_slice().try_into().map_err(|_| {
        format!(
            "directory returned a {}-byte user id, expected 16",
            user_id.len()
        )
    })?;
    let owner = principal64(user_id);

    let mut batch = Batch::new();
    batch.id("fixture-admin");

    // chmod database default → 0o755 (493): ref_user reads through it.
    let database = ddl::res::database(DB);
    batch.chmod(
        "chmod-db",
        signed(client, &unsigned_chmod(database.clone(), 493), |tag| {
            ddl::chmod(database, 493)
                .hmac(tag)
                .build()
                .map_err(|e| e.to_string())
        })?,
    );

    // chmod store default/main → 0o644 (420).
    let store = ddl::res::store(DB, REPO);
    batch.chmod(
        "chmod-store",
        signed(client, &unsigned_chmod(store.clone(), 420), |tag| {
            ddl::chmod(store, 420)
                .hmac(tag)
                .build()
                .map_err(|e| e.to_string())
        })?,
    );

    // chmod table ref_schema_versioned → 0o700 (448).
    let table = ddl::res::table(DB, REPO, VERSIONED_TABLE);
    batch.chmod(
        "chmod-table",
        signed(client, &unsigned_chmod(table.clone(), 448), |tag| {
            ddl::chmod(table, 448)
                .hmac(tag)
                .build()
                .map_err(|e| e.to_string())
        })?,
    );

    // create group ref_group, add ref_user to it.
    let unsigned_group = BatchOp::CreateGroup(CreateGroupOp {
        create_group: REF_GROUP.into(),
        hmac: None,
    });
    batch.create_group(
        "create-group",
        signed(client, &unsigned_group, |tag| {
            ddl::create_group(REF_GROUP)
                .hmac(tag)
                .build()
                .map_err(|e| e.to_string())
        })?,
    );

    let unsigned_member = BatchOp::AddGroupMember(AddGroupMemberOp {
        add_group_member: GroupRef::Name {
            name: REF_GROUP.into(),
        },
        user: owner,
        hmac: None,
    });
    batch.add_group_member(
        "add-member",
        signed(client, &unsigned_member, |tag| {
            ddl::add_group_member(
                GroupRef::Name {
                    name: REF_GROUP.into(),
                },
                owner,
            )
            .hmac(tag)
            .build()
            .map_err(|e| e.to_string())
        })?,
    );

    // chown ref_schema_versioned → ref_user's principal id.
    let table = ddl::res::table(DB, REPO, VERSIONED_TABLE);
    let unsigned_own = BatchOp::Chown(ChownOp {
        chown: table.clone(),
        owner,
        hmac: None,
    });
    batch.chown(
        "chown-table",
        signed(client, &unsigned_own, |tag| {
            ddl::chown(table, owner)
                .hmac(tag)
                .build()
                .map_err(|e| e.to_string())
        })?,
    );

    execute(client, batch, "admin DDL").await
}

fn unsigned_chmod(resource: ddl::ResourceRef, mode: u16) -> BatchOp {
    BatchOp::Chmod(ChmodOp {
        chmod: resource,
        mode,
        hmac: None,
    })
}

// ═══════════════════════════════════════════════════════════════════════
// Phase 6: verification
// ═══════════════════════════════════════════════════════════════════════

const BASIC_ROWS: [(&str, i64, [f64; 4]); 3] = [
    ("r1", 10, [0.5, 0.25, 0.125, 0.0625]),
    ("r2", 20, [0.1, 0.2, 0.3, 0.4]),
    ("r3", 30, [1.0, 0.75, 0.5, 0.25]),
];

async fn verify(client: &Client) -> Result<(), String> {
    let mut batch = Batch::new();
    batch.id("fixture-verify");
    batch.query("read-basic", Query::from(BASIC_TABLE).order_by_asc("n"));
    batch.query(
        "read-versioned",
        Query::from(VERSIONED_TABLE).order_by_asc("n"),
    );
    batch.call("call-lang-fn", LANG_FN, [lit(41i64)]);
    batch.describe_table(
        "describe-basic",
        ddl::describe_table(BASIC_TABLE).repo(REPO),
    );
    batch.describe_table(
        "describe-versioned",
        ddl::describe_table(VERSIONED_TABLE).repo(REPO),
    );
    batch.list_functions("list-functions", ddl::list_functions());

    let resp = client
        .execute(DB, batch.build())
        .await
        .map_err(|e| format!("verification batch failed: {e}"))?;

    // ── rows read back, exact ────────────────────────────────────
    let basic = resp
        .results
        .get("read-basic")
        .ok_or("verification response missing 'read-basic'")?;
    if basic.records.len() != BASIC_ROWS.len() {
        return Err(format!(
            "{} read back {} rows, expected {}",
            BASIC_TABLE,
            basic.records.len(),
            BASIC_ROWS.len()
        ));
    }
    for (record, row) in basic.records.iter().zip(BASIC_ROWS.iter()) {
        let (k, n, vec) = *row;
        if record.get_value_str("k") != Some(k) || record.get_value_i64("n") != Some(n) {
            return Err(format!(
                "{} row mismatch: got k={:?} n={:?}, expected k={k} n={n}",
                BASIC_TABLE,
                record.get_value_str("k"),
                record.get_value_i64("n"),
            ));
        }
        let got_vec = record
            .get_value("vec")
            .ok_or_else(|| format!("{BASIC_TABLE} row missing 'vec'"))?;
        let expected: Vec<f64> = vec.to_vec();
        if let QueryValue::List(items) = got_vec {
            for (item, want) in items.iter().zip(expected.iter()) {
                if item.as_f64() != Some(*want) {
                    return Err(format!(
                        "{BASIC_TABLE} row {k} vec mismatch: got {items:?}, expected {expected:?}"
                    ));
                }
            }
        } else {
            return Err(format!("{BASIC_TABLE} row {k} 'vec' is not a list"));
        }
    }

    let versioned = resp
        .results
        .get("read-versioned")
        .ok_or("verification response missing 'read-versioned'")?;
    let versioned_rows: Vec<(String, i64)> = versioned
        .records
        .iter()
        .filter_map(|r| Some((r.get_value_str("k")?.to_owned(), r.get_value_i64("n")?)))
        .collect();
    if versioned_rows != [("v1".into(), 7), ("v2".into(), 8)] {
        return Err(format!(
            "{VERSIONED_TABLE} rows mismatch: got {versioned_rows:?}"
        ));
    }

    // ── typed call ref_lang_fn(41) == 42 ─────────────────────────
    let called = resp
        .results
        .get("call-lang-fn")
        .ok_or("verification response missing 'call-lang-fn'")?;
    let value = called
        .value
        .as_ref()
        .ok_or("ref_lang_fn returned no scalar value")?;
    if value.as_i64() != Some(42) {
        return Err(format!(
            "ref_lang_fn(41) returned {value:?}, expected Int(42)"
        ));
    }

    // ── describe: 4 indexes ──────────────────────────────────────
    let described = resp
        .results
        .get("describe-basic")
        .ok_or("verification response missing 'describe-basic'")?;
    let record = described
        .records
        .first()
        .ok_or("describe_table returned no record")?;
    let described = record.as_value().into_owned();
    let indexes = described
        .get("indexes")
        .and_then(|v| v.as_array())
        .ok_or("describe_table response has no 'indexes' section")?;
    let index_names: Vec<&str> = indexes
        .iter()
        .filter_map(|i| i.get("name").and_then(|v| v.as_str()))
        .collect();
    for expected in [
        "ref_hash_idx",
        "ref_sorted_idx",
        "ref_vector_idx",
        "ref_fulltext_idx",
    ] {
        if !index_names.contains(&expected) {
            return Err(format!(
                "describe_table indexes missing '{expected}' (found {index_names:?})"
            ));
        }
    }
    let versioned_describe = resp
        .results
        .get("describe-versioned")
        .ok_or("verification response missing 'describe-versioned'")?;
    let versioned_record = versioned_describe
        .records
        .first()
        .ok_or("describe_table returned no record for the versioned table")?;
    let schema_version = versioned_record
        .as_value()
        .get("schema_version")
        .and_then(|v| v.as_i64())
        .ok_or("describe_table response has no 'schema_version'")?;
    if schema_version != 2 {
        return Err(format!(
            "expected schema_version 2 after the set_table_schema bump, describe reports {schema_version}"
        ));
    }

    // ── list_functions: 2 functions ──────────────────────────────
    let listed = resp
        .results
        .get("list-functions")
        .ok_or("verification response missing 'list-functions'")?;
    let record = listed
        .records
        .first()
        .ok_or("list_functions returned no record")?;
    let listed_functions = record.as_value().into_owned();
    let functions = listed_functions
        .get("functions")
        .and_then(|v| v.as_array())
        .ok_or("list_functions response has no 'functions' section")?;
    let fn_names: Vec<&str> = functions
        .iter()
        .filter_map(|f| f.get("name").and_then(|v| v.as_str()))
        .collect();
    for expected in [LANG_FN, WASM_FN] {
        if !fn_names.contains(&expected) {
            return Err(format!(
                "list_functions missing '{expected}' (found {fn_names:?})"
            ));
        }
    }

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
// Echo WASM module — Rust port of make_reference_datadir.ref_wasm_fn_bytes
// ═══════════════════════════════════════════════════════════════════════

/// LEB128-encode an unsigned value.
fn leb_u(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return;
        }
    }
}

/// LEB128-encode a signed value.
fn leb_s(mut value: i64, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        let done = (value == 0 && byte & 0x40 == 0) || (value == -1 && byte & 0x40 != 0);
        if done {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn section(id: u8, payload: &[u8], out: &mut Vec<u8>) {
    out.push(id);
    leb_u(payload.len() as u64, out);
    out.extend_from_slice(payload);
}

fn wasm_name(name: &str, out: &mut Vec<u8>) {
    let raw = name.as_bytes();
    leb_u(raw.len() as u64, out);
    out.extend_from_slice(raw);
}

/// The echo module: exports `memory`, `shamir_alloc` and `shamir_call`;
/// `shamir_call` decodes the guest i64 payload and echoes it back.
fn ref_wasm_fn_bytes() -> Vec<u8> {
    // types: 0 = (i32) -> i32, 1 = (i32, i32) -> i64
    let types: Vec<u8> = [
        vec![0x02, 0x60, 0x01, 0x7f, 0x01, 0x7f],
        vec![0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7e],
    ]
    .concat();
    let funcs: Vec<u8> = vec![0x02, 0x00, 0x01];
    let memories: Vec<u8> = vec![0x01, 0x00, 0x02];
    let mut globals: Vec<u8> = vec![0x01, 0x7f, 0x01, 0x41];
    leb_s(1024, &mut globals);
    globals.push(0x0b);
    let mut exports: Vec<u8> = vec![0x03];
    wasm_name("memory", &mut exports);
    exports.extend_from_slice(&[0x02, 0x00]);
    wasm_name("shamir_alloc", &mut exports);
    exports.extend_from_slice(&[0x00, 0x00]);
    wasm_name("shamir_call", &mut exports);
    exports.extend_from_slice(&[0x00, 0x01]);
    // shamir_alloc body: ptr = global.get $bump; bump += len; return ptr.
    let body0: Vec<u8> = [
        vec![0x23, 0x00, 0x21, 0x01],
        vec![0x23, 0x00, 0x20, 0x00, 0x6a, 0x24, 0x00],
        vec![0x20, 0x01, 0x0b],
    ]
    .concat();
    // shamir_call body: (i64(ptr) << 32) | i64(len), echoing the input buffer.
    let mut body1: Vec<u8> = vec![0x20, 0x00, 0xad, 0x42];
    leb_s(32, &mut body1);
    body1.extend_from_slice(&[0x86, 0x20, 0x01, 0xad, 0x84, 0x0b]);
    let code0: Vec<u8> = [vec![0x01, 0x01, 0x7f], body0].concat(); // 1 local group: 1 x i32
    let mut code1: Vec<u8> = vec![0x00];
    code1.extend_from_slice(&body1);
    let mut codes: Vec<u8> = Vec::new();
    leb_u(2, &mut codes);
    leb_u(code0.len() as u64, &mut codes);
    codes.extend_from_slice(&code0);
    leb_u(code1.len() as u64, &mut codes);
    codes.extend_from_slice(&code1);

    let mut module: Vec<u8> = b"\x00asm\x01\x00\x00\x00".to_vec();
    section(1, &types, &mut module);
    section(3, &funcs, &mut module);
    section(5, &memories, &mut module);
    section(6, &globals, &mut module);
    section(7, &exports, &mut module);
    section(10, &codes, &mut module);
    module
}

/// Minimal RFC 4648 standard base64 encoder (no external deps allowed here).
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(triple >> 18) as usize & 0x3f] as char);
        out.push(ALPHABET[(triple >> 12) as usize & 0x3f] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(triple >> 6) as usize & 0x3f] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[triple as usize & 0x3f] as char
        } else {
            '='
        });
    }
    out
}
