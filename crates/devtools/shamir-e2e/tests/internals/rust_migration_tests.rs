//! Real-server R1c application-schema migration integration test
//! (release 48, step 3a; design `docs/dev-artifacts/design/app-schema-migrations.md`
//! §5.2/§5.3, §7.2).
//!
//! Boots the real server with a persistent store, discovers the database's
//! stable identity through the high-level `status` mismatch echo
//! (`MigrationStatus::server_error == IdentityMismatch` while
//! `reported_target` carries the SERVING identity — the documented
//! identity-discovery path), then drives the client migration tool through
//! a REAL RESUME sequence of two high-level `apply_next` calls for two
//! different tables:
//!
//! 1. read-only `status` on the empty journal (`PrefixCoverage::Empty`,
//!    released `fence_generation == 0` — the server ALWAYS emits the fence,
//!    including 0) and `plan` reporting exactly the next migration
//!    (id + ordinal),
//! 2. one real typed apply of migration 1 through `apply_next` (expected
//!    generation 0, acquired generation 1 in the apply result), then a
//!    REPEAT with the first-only set refused typed
//!    `MigrationError::NothingToApply` BEFORE the second apply — the design
//!    assertion that a fully covered set issues no apply (the read-only
//!    status comparison still runs; after the second apply the same
//!    one-element set would merely be SHORTER than the journal; the repeat
//!    here proves the no-apply refusal on coverage alone),
//! 3. honest status re-check after the first apply: the applied ENTRY
//!    (id/ordinal/exact canonical checksum) is now reported in
//!    `MigrationStatus::applied` alongside the head fields, coverage is
//!    `PrefixCoverage::Verified { ordinal: 1 }`, and the RELEASED fence
//!    generation is 2 — three distinct quantities: the EXPECTED generation
//!    the client sent (0), the ACQUIRED generation the apply result
//!    returned (1), and the RELEASED generation status reports (2), which
//!    is exactly the expected generation of the next fresh apply,
//! 4. the SAME high-level path runs the second apply for a different
//!    table: expected generation 2 (from status), acquired generation 3 in
//!    the public apply result,
//! 5. the actually created tables are read back through the typed query
//!    builder (not only through the journal),
//! 6. NON-HEAD drift after the second apply: a changed FIRST body (same id
//!    and ordinal, different canonical checksum) ahead of an IDENTICAL
//!    second body and a pending third (so that without the drift the set
//!    would still have something to send) is reported typed
//!    `PrefixCoverage::Drift { ordinal: 1, .. }` through the read-only
//!    status comparison (ids/ordinals/canonical checksums, not raw body
//!    bytes), refused typed
//!    `MigrationError::Drift { ordinal: 1, .. }` by `apply_next` BEFORE any
//!    APPLY send, and refused typed `MigrationError::ChecksumMismatch` through
//!    `plan` (the server verifies the applied prefix entry by entry),
//! 7. final status reports the journal applied BOTH migrations with exact
//!    ids/ordinals/checksums and the RELEASED fence generation 4.
//!
//! Fence-generation vocabulary used throughout: EXPECTED = what the client
//! sends (or status implies as the next fresh apply's expectation),
//! ACQUIRED = the `fence_generation` the apply RESULT reports for the
//! generation the server handed the event, RELEASED = the highest durable
//! fence generation a status reports afterwards.

use std::path::PathBuf;
use std::time::Duration;

use tempfile::TempDir;
use zeroize::Zeroizing;

use shamir_client::builder::batch::{Batch, IntoBatchOp};
use shamir_client::builder::ddl::{self, RepoEngine};
use shamir_client::builder::query::Query;
use shamir_client::builder::SchemaMigration;
use shamir_client::schema_migration::{
    ApplicationMigrationSet, MigrationError, PrefixCoverage, SchemaMigrationAppliedEntry,
    SchemaMigrationErrorCode, SchemaMigrationTarget,
};
use shamir_client::{Client, ConnectOptions};

use shamir_server::config::{
    Config, KdfConfig, ListenerConfig, ListenerKind, LoggingConfig, ProfileKind, TlsConfig,
};
use shamir_server::server::{BootstrapMode, ServerHandle, ServerLauncher};
use shamir_types::types::record_id::RecordId;

const DB: &str = "migdb";
const M1_ID: &str = "mig-create-items";
const M2_ID: &str = "mig-create-audit";
const M3_ID: &str = "mig-create-metrics";
const DRIFT_TABLE: &str = "items_tampered";
const TEST_TIMEOUT: Duration = Duration::from_secs(180);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

fn migration_config(temp: &TempDir) -> Config {
    let data_dir: PathBuf = temp.path().to_path_buf();
    Config {
        tokens: Default::default(),
        data_dir: data_dir.clone(),
        logging: LoggingConfig {
            level: "warn".into(),
            slow_query_threshold_ms: 0,
            file: None,
            flush_interval_ms: 2000,
        },
        durability: Default::default(),
        transactions: Default::default(),
        kdf_defaults: KdfConfig {
            memory_kb: 19_456,
            time: 2,
            parallelism: 1,
            argon2_version: 0x13,
        },
        argon2_concurrent_max: 4,
        listeners: vec![ListenerConfig {
            kind: ListenerKind::Tcp,
            addr: "127.0.0.1:0".into(),
            profile: Some(ProfileKind::TlsExporter),
            path: None,
            kdf_override: None,
            browser_origin_allowlist: vec![],
            http: Default::default(),
        }],
        tls: TlsConfig {
            cert_path: data_dir.join("cert.pem"),
            key_path: data_dir.join("cert.key"),
        },
        security: Default::default(),
        audit: Default::default(),
        observability: shamir_server::config::ObservabilityConfig {
            addr: String::new(),
            allow_public_metrics: false,
        },
        replication: None,
    }
}

fn migration_body(
    id: &str,
    ordinal: u64,
    name: &str,
    table: &str,
) -> shamir_client::schema_migration::SchemaMigrationBody {
    SchemaMigration::new(id, ordinal, name)
        .op(ddl::create_table(table).repo("main").into_batch_op())
        .expect("create_table is an admitted migration operation")
        .build()
        .expect("migration body validates")
}

/// Every await inside is bounded by `REQUEST_TIMEOUT` (also set as the
/// client's request deadline); the scenario as a whole is bounded by the
/// global `TEST_TIMEOUT` at the call site.
async fn run_scenario() {
    // aws-lc must be installed before any TLS handshake (existing fixture
    // pattern of `smoke_local` / `auth_identity_tests`).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let temp = TempDir::new().expect("tempdir");
    let password = b"correct horse battery staple".to_vec();

    // Launch/connect futures are Box::pinned so their deep server-side
    // frames live on the heap, and each is bounded.
    let launcher = ServerLauncher {
        node_mode: Default::default(),
        config: migration_config(&temp),
        bootstrap: BootstrapMode::Password {
            username: "admin".into(),
            password: Zeroizing::new(password.clone()),
        },
    };
    let handle: ServerHandle = tokio::time::timeout(TEST_TIMEOUT, Box::pin(launcher.launch()))
        .await
        .expect("server launch deadline")
        .expect("server launch");
    let addr = handle.first_tls_exporter_addr().expect("bound listener");

    let client: Client = tokio::time::timeout(
        REQUEST_TIMEOUT,
        Box::pin(Client::connect(ConnectOptions {
            addr,
            server_name: "localhost".into(),
            username: "admin".into(),
            password: Zeroizing::new(password),
            accept_new_host: true,
            trusted_pin: None,
            connect_timeout: None,
            handshake_timeout: None,
            request_timeout: Some(REQUEST_TIMEOUT),
        })),
    )
    .await
    .expect("client connect deadline")
    .expect("client connect");

    // Fixture: db + a PERSISTENT Fjall repo (explicit durable engine), so
    // the migration DDL has a real persisted home.
    let mut mk = Batch::new();
    mk.id("mk-db");
    mk.create_db(DB, ddl::create_db(DB));
    tokio::time::timeout(REQUEST_TIMEOUT, client.execute("default", mk.build()))
        .await
        .expect("create db deadline")
        .expect("create db");
    let mut repo = Batch::new();
    repo.id("mk-repo");
    repo.create_repo("mr", ddl::create_repo("main").engine(RepoEngine::Fjall));
    tokio::time::timeout(REQUEST_TIMEOUT, client.execute(DB, repo.build()))
        .await
        .expect("create repo deadline")
        .expect("create repo");

    // The migration sets (validating constructor).
    let first = migration_body(M1_ID, 1, "create items table", "items");
    let second = migration_body(M2_ID, 2, "create audit table", "audit");
    let set_full = ApplicationMigrationSet::new(vec![first.clone(), second.clone()])
        .expect("the two-migration set validates");
    let set_first_only =
        ApplicationMigrationSet::new(vec![first]).expect("the single-migration set validates");

    // --- stable identity discovery through the mismatch echo -----------
    // A deliberately wrong identity is answered for the SERVING identity:
    // typed server_error + the real target in reported_target.
    let probe = client
        .status(
            DB,
            SchemaMigrationTarget {
                database_identity: RecordId::from_ts_seq(1, 1),
            },
            &set_full,
        )
        .await
        .expect("status is read-only; a mismatch is a result, not an error");
    assert_eq!(
        probe.server_error,
        Some(SchemaMigrationErrorCode::SchemaMigrationIdentityMismatch),
        "the probe must report the typed identity mismatch"
    );
    let identity = probe.reported_target.database_identity;
    assert_ne!(
        identity,
        RecordId::from_ts_seq(1, 1),
        "the echo must carry the serving identity, not the requested one"
    );
    let target = SchemaMigrationTarget {
        database_identity: identity,
    };

    // The honest empty-journal view. The server ALWAYS emits the fence
    // generation, including 0 on a genuinely empty journal.
    let empty = client.status(DB, target, &set_full).await.expect("status");
    assert_eq!(empty.server_error, None, "no error on the matched identity");
    assert_eq!(
        empty.coverage,
        PrefixCoverage::Empty,
        "the journal must report empty before the first apply"
    );
    assert_eq!(empty.last_applied_ordinal, 0, "empty journal head");
    assert!(
        empty.applied.is_empty(),
        "no applied entries on the empty journal"
    );
    assert_eq!(
        empty.fence_generation, 0,
        "RELEASED fence generation is explicitly 0 on the empty journal"
    );
    assert_eq!(
        empty.next_local_ordinal,
        Some(1),
        "ordinal 1 is the first uncovered local migration"
    );

    // The plan reports exactly the NEXT migration.
    let plan = client.plan(DB, target, &set_full).await.expect("plan");
    assert_eq!(plan.migration.id, M1_ID, "plan targets ordinal 1's id");
    assert_eq!(plan.migration.ordinal, 1, "plan reports ordinal 1");

    // --- first real apply (ordinal 1, table `items`) --------------------
    // EXPECTED generation: 0 (the released generation the empty status
    // reported). ACQUIRED: reported by the apply result.
    let applied_first = client
        .apply_next(DB, target, &set_full)
        .await
        .expect("first apply");
    assert_eq!(applied_first.migration_id, M1_ID, "applied migration id");
    assert_eq!(applied_first.ordinal, 1, "applied ordinal");
    assert_eq!(applied_first.status, "applied", "applied status");
    assert_eq!(
        applied_first.fence_generation,
        Some(1),
        "ACQUIRED generation: expected 0, the server acquired generation 1"
    );

    // REPEAT with the first-only set, immediately AFTER the first apply and
    // BEFORE the second: the set is fully covered by the reported applied
    // prefix, so `apply_next` refuses typed `NothingToApply` before any
    // APPLY send (the read-only status comparison it runs does occur) —
    // the design assertion that a covered set issues no apply. Note the
    // comparison checks ids/ordinals/canonical checksums, not raw body
    // bytes.
    // (After the second apply below this same one-element set would merely
    // be SHORTER than the journal; here coverage alone proves the refusal.)
    match client.apply_next(DB, target, &set_first_only).await {
        Err(MigrationError::NothingToApply) => {}
        other => panic!("expected NothingToApply, got {other:?}"),
    }

    // Honest status re-check: the applied ENTRY is now reported in the
    // full applied prefix alongside the retained head fields, and the
    // RELEASED fence generation is observable (2).
    let head = client.status(DB, target, &set_full).await.expect("status");
    assert_eq!(
        head.coverage,
        PrefixCoverage::Verified { ordinal: 1 },
        "the journal head must verify against the local ordinal-1 body"
    );
    assert_eq!(
        head.last_applied_id.as_deref(),
        Some(M1_ID),
        "head id after the first apply"
    );
    assert!(head.journal_checksum_verified(), "checksum verified");
    assert_eq!(head.in_flight, None, "no unresolved journal entry");
    assert_eq!(
        head.next_local_ordinal,
        Some(2),
        "ordinal 2 is the next uncovered local migration"
    );
    assert_eq!(
        head.applied,
        vec![SchemaMigrationAppliedEntry {
            id: M1_ID.to_string(),
            ordinal: 1,
            checksum: set_full.checksums()[0],
        }],
        "the applied prefix carries the exact applied entry (id/ordinal/ \
         canonical checksum equal to the local set's)"
    );
    assert_eq!(
        head.fence_generation, 2,
        "RELEASED generation after the first apply: the EXPECTED generation \
         of the next fresh apply"
    );

    // The plan STILL reports the next migration (ordinal 2) — and this
    // time the fresh second apply CAN run: status reported the released
    // generation, so the same high-level path resumes.
    let plan_second = client.plan(DB, target, &set_full).await.expect("plan");
    assert_eq!(
        plan_second.migration.id, M2_ID,
        "plan targets ordinal 2's id"
    );
    assert_eq!(plan_second.migration.ordinal, 2, "plan reports ordinal 2");

    // --- second real apply (ordinal 2, table `audit`) -------------------
    // EXPECTED generation: 2 (released by the first apply, reported by
    // status). ACQUIRED: reported by the public apply result.
    let applied_second = client
        .apply_next(DB, target, &set_full)
        .await
        .expect("second apply through the same high-level path");
    assert_eq!(applied_second.migration_id, M2_ID, "applied migration id");
    assert_eq!(applied_second.ordinal, 2, "applied ordinal");
    assert_eq!(applied_second.status, "applied", "applied status");
    assert_eq!(
        applied_second.fence_generation,
        Some(3),
        "ACQUIRED generation: expected 2, the server acquired generation 3"
    );

    // The actually created tables exist — read back through the typed
    // query builder (a query against a missing table errors), not only
    // through the migration journal.
    let mut read_tables = Batch::new();
    read_tables.id("read-migrated-tables");
    read_tables.query("sel-items", Query::from("items"));
    read_tables.query("sel-audit", Query::from("audit"));
    let tables = tokio::time::timeout(REQUEST_TIMEOUT, client.execute(DB, read_tables.build()))
        .await
        .expect("table read deadline")
        .expect("both migrated tables must be queryable");
    assert!(
        tables.results.contains_key("sel-items") && tables.results.contains_key("sel-audit"),
        "both created tables answer a typed builder query"
    );

    // --- NON-HEAD drift after the second apply --------------------------
    // A changed FIRST body (same id and ordinal, different table name,
    // hence a different canonical checksum) ahead of an IDENTICAL second
    // body and a pending third: without the drift this set would still
    // have ordinal 3 to send, so any refusal below is attributable to the
    // drift alone, not to a covered set.
    let drift_first = migration_body(M1_ID, 1, "create items table", DRIFT_TABLE);
    let third = migration_body(M3_ID, 3, "create metrics table", "metrics");
    let set_drift = ApplicationMigrationSet::new(vec![drift_first, second, third])
        .expect("the drifted three-migration set validates");
    let drifted_status = client.status(DB, target, &set_drift).await.expect("status");
    match &drifted_status.coverage {
        PrefixCoverage::Drift {
            ordinal,
            journal_id,
            journal_checksum,
            local_id,
        } => {
            assert_eq!(*ordinal, 1, "drift reported at ordinal 1");
            assert_eq!(journal_id, M1_ID, "the journal keeps the applied id");
            assert_eq!(
                journal_checksum,
                &set_full.checksums()[0],
                "the journal checksum is the UNMODIFIED applied body's"
            );
            assert_eq!(
                local_id, M1_ID,
                "the local set has the SAME id, a changed body"
            );
        }
        other => panic!("expected Drift at ordinal 1, got {other:?}"),
    }
    // Refused typed by apply_next BEFORE any APPLY send (its read-only
    // preflight status does occur; it compares ids/ordinals/canonical
    // checksums): the set is LONGER than the journal, so a no-apply
    // refusal could only come from the drift check.
    match client.apply_next(DB, target, &set_drift).await {
        Err(MigrationError::Drift { ordinal: 1, .. }) => {}
        other => panic!("expected Drift {{ ordinal: 1, .. }} for the drifted set, got {other:?}"),
    }
    // Refused typed through the plan too: its request carries the applied
    // prefix for the server to verify entry by entry.
    match client.plan(DB, target, &set_drift).await {
        Err(MigrationError::ChecksumMismatch { .. }) => {}
        other => panic!("expected ChecksumMismatch for the drifted set, got {other:?}"),
    }

    // Final honest observation with the two-migration set: BOTH entries
    // reported exactly, head verified at ordinal 2, released generation 4
    // (distinct from the acquired 3 and the expected 2 of the second
    // apply). The refusals above must not have advanced the journal: note
    // an unchanged head ALONE would not prove nothing was sent — the
    // refusals are typed pre-send client checks and server-side typed
    // rejections, and the scripted tests verify the exact bytes on the
    // wire; here the full applied prefix additionally confirms the journal
    // contents are byte-for-byte the two originally applied bodies.
    let unchanged = client.status(DB, target, &set_full).await.expect("status");
    assert_eq!(
        unchanged.coverage,
        PrefixCoverage::Verified { ordinal: 2 },
        "the refusals must not have advanced the journal head"
    );
    assert_eq!(
        unchanged.applied,
        vec![
            SchemaMigrationAppliedEntry {
                id: M1_ID.to_string(),
                ordinal: 1,
                checksum: set_full.checksums()[0],
            },
            SchemaMigrationAppliedEntry {
                id: M2_ID.to_string(),
                ordinal: 2,
                checksum: set_full.checksums()[1],
            },
        ],
        "the applied prefix is exactly the two migrations, with the exact \
         ids, ordinals and canonical checksums"
    );
    assert_eq!(
        unchanged.fence_generation, 4,
        "RELEASED generation after the second apply (expected 2, acquired 3)"
    );

    client.close().await;
    // Shutdown drains in-flight work and flushes/closes the persistent
    // repositories; the report must be clean (the Fjall repo included).
    let report = handle.shutdown().await;
    assert!(
        report.is_clean(),
        "clean shutdown, including the persistent repo: {:?}",
        report.repository_failures()
    );
}

/// The deep server + client async frames overflow a default test stack
/// (same Windows debug-build mechanism the `shamir-db` schema-migration
/// tests solve with `on_big_stack`): one 64 MiB thread owns ONE multi-thread
/// runtime with roomy worker stacks; the scenario is Box::pinned and
/// globally bounded.
#[test]
fn rust_migration_real_server_resume_two_applies_repeat_nothing_to_apply_drift_refused() {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(16 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(Box::pin(async {
                    tokio::time::timeout(TEST_TIMEOUT, Box::pin(run_scenario()))
                        .await
                        .expect("the whole migration scenario must finish inside the deadline");
                }));
        })
        .expect("spawn big-stack test thread")
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}
