//! `ServerHandle::shutdown()` contract: when it returns, every store of the
//! data dir is released, so the same dir can be relaunched at once.
//!
//! The relaunch below has NO retry on purpose: before the fix, shutdown
//! returned while detached tasks (buffered-store flushers, connection tasks)
//! still held the fjall handles, and the next boot failed with `Locked`.

use std::path::Path;
use std::time::Duration;

use shamir_connect::server::audit_chain::{AuditChain, AuditEntry};
use shamir_server::audit_appender::FjallAuditAppender;
use shamir_server::config::{
    Config, KdfConfig, ListenerConfig, ListenerKind, LoggingConfig, ProfileKind, TlsConfig,
};
use shamir_server::server::{BootstrapMode, ServerHandle, ServerLauncher};
use shamir_server::server_meta::{MetaError, ServerMetaStore};
use shamir_test_diag::FlakeGuard;
use tokio::time::{sleep, timeout};
use zeroize::Zeroizing;

const CYCLES: usize = 8;
const PASSWORD: &[u8] = b"admin-password";
const AUDIT_PHASE_TIMEOUT: Duration = Duration::from_secs(120);

fn make_config(data_dir: &Path) -> Config {
    Config {
        durability: Default::default(),
        transactions: Default::default(),
        tokens: Default::default(),
        data_dir: data_dir.to_path_buf(),
        logging: LoggingConfig {
            level: "warn".into(),
            slow_query_threshold_ms: 0,
            file: None,
            flush_interval_ms: 2000,
        },
        kdf_defaults: KdfConfig {
            memory_kb: 19_456,
            time: 2,
            parallelism: 1,
            argon2_version: 0x13,
        },
        argon2_concurrent_max: 4,
        listeners: vec![ListenerConfig {
            kind: ListenerKind::Tcp,
            addr: "127.0.0.1:0".to_string(),
            profile: Some(ProfileKind::TlsExporter),
            path: None,
            kdf_override: None,
            browser_origin_allowlist: vec![],
            http: Default::default(),
        }],
        tls: TlsConfig {
            cert_path: data_dir.join("cert.pem"),
            key_path: data_dir.join("key.pem"),
        },
        // The pre-auth rate limit persists across restarts and runs at a quarter
        // of its rate for 60 s after each boot (spec §8.6); eight boots with one
        // login each would hit it, and a rate-limited connect looks like `early eof`.
        security: shamir_server::config::SecurityConfig {
            auth_init_rate_per_second: 100_000,
            ..Default::default()
        },
        audit: shamir_server::config::AuditConfig {
            max_file_size_mb: 0,
            retention_days: 0,
        },
        observability: shamir_server::config::ObservabilityConfig {
            addr: String::new(),
            allow_public_metrics: false,
        },
        replication: None,
        backup: None,
    }
}

fn make_launcher(data_dir: &Path) -> ServerLauncher {
    ServerLauncher {
        config: make_config(data_dir),
        node_mode: Default::default(),
        bootstrap: BootstrapMode::Password {
            username: "admin".into(),
            password: Zeroizing::new(PASSWORD.to_vec()),
        },
    }
}

async fn launch(data_dir: &Path) -> ServerHandle {
    make_launcher(data_dir)
        .launch()
        .await
        .expect("launch must not hit a store lock left by the previous shutdown")
}

async fn connect(handle: &ServerHandle) -> shamir_client::Client {
    shamir_client::Client::connect(shamir_client::ConnectOptions {
        addr: handle.first_tls_exporter_addr().expect("bound address"),
        server_name: "localhost".to_string(),
        username: "admin".to_string(),
        password: Zeroizing::new(PASSWORD.to_vec()),
        accept_new_host: true,
        trusted_pin: None,
        connect_timeout: None,
        handshake_timeout: None,
        request_timeout: None,
    })
    .await
    .expect("admin connect")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relaunch_right_after_shutdown_never_hits_a_held_store() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().to_path_buf();

    for cycle in 0..CYCLES {
        let handle = launch(&data_dir).await;
        let client = connect(&handle).await;

        if cycle == 0 {
            let mut batch = shamir_query_builder::batch::Batch::new();
            batch
                .id(1)
                .create_table("items", shamir_query_builder::ddl::create_table("items"));
            client
                .execute("default", batch.build())
                .await
                .expect("create table");
        }

        // A buffered (write-behind) write right before shutdown: the
        // flusher of the buffered store is still alive when shutdown runs.
        let mut batch = shamir_query_builder::batch::Batch::new();
        batch.id("wr");
        batch.insert(
            "ins",
            shamir_query_builder::write::insert("items").rows([shamir_query_builder::doc! {
                "cycle" => cycle as i64
            }]),
        );
        client
            .execute("default", batch.build())
            .await
            .expect("insert");

        drop(client);
        let report = handle.shutdown().await;
        assert!(report.is_clean(), "{report:?}");
        assert_eq!(report.exit_code(), 0);
    }

    let handle = launch(&data_dir).await;
    let client = connect(&handle).await;
    let mut batch = shamir_query_builder::batch::Batch::new();
    batch.id("rd");
    batch.query("rd", shamir_query_builder::Query::from("items"));
    let resp = client
        .execute("default", batch.build())
        .await
        .expect("read back");
    let rows = resp.results.get("rd").expect("rd alias").records.len();
    assert_eq!(
        rows, CYCLES,
        "every cycle's row must survive the relaunches"
    );

    drop(client);
    handle.shutdown().await;
}

/// Same contract with a client that is still connected when `shutdown()` is
/// called: the per-connection task (and the stores it reaches through its
/// handler context) must not outlive `shutdown()`, otherwise the relaunch hits
/// a held store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relaunch_right_after_shutdown_with_a_client_still_connected() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().to_path_buf();

    let handle = launch(&data_dir).await;
    let client = connect(&handle).await;
    let mut batch = shamir_query_builder::batch::Batch::new();
    batch
        .id(1)
        .create_table("items", shamir_query_builder::ddl::create_table("items"));
    client
        .execute("default", batch.build())
        .await
        .expect("create table");

    // The client stays connected across the shutdown.
    handle.shutdown().await;

    let handle = launch(&data_dir).await;
    drop(client);
    handle.shutdown().await;
}

async fn authenticated_audit_run(
    data_dir: &Path,
    cycle: usize,
    guard: &FlakeGuard,
) -> (Vec<u8>, Vec<AuditEntry>) {
    // The deadlines only detect hangs. Authentication and shutdown synchronize
    // the test with the audit emission and final flush, respectively.
    let handle = timeout(AUDIT_PHASE_TIMEOUT, Box::pin(launch(data_dir)))
        .await
        .unwrap_or_else(|_| panic!("audit boot {cycle} timed out"));
    guard.mark(format!("audit boot {cycle} launched"));
    let client = timeout(AUDIT_PHASE_TIMEOUT, Box::pin(connect(&handle)))
        .await
        .unwrap_or_else(|_| panic!("audit boot {cycle} login timed out"));
    guard.mark(format!("audit boot {cycle} authenticated"));
    drop(client);
    let report = timeout(AUDIT_PHASE_TIMEOUT, Box::pin(handle.shutdown()))
        .await
        .unwrap_or_else(|_| panic!("audit boot {cycle} shutdown timed out"));
    assert!(report.is_clean(), "audit boot {cycle}: {report:?}");
    assert_eq!(report.exit_code(), 0, "audit boot {cycle}: {report:?}");
    guard.mark(format!("audit boot {cycle} shut down cleanly"));

    let bytes = std::fs::read(data_dir.join("audit.log"))
        .unwrap_or_else(|err| panic!("read audit.log after boot {cycle}: {err}"));
    let entries = FjallAuditAppender::read_log_for_verify(data_dir)
        .unwrap_or_else(|err| panic!("parse audit log after boot {cycle}: {err}"));
    guard.mark(format!(
        "audit boot {cycle}: {} disk entries",
        entries.len()
    ));
    (bytes, entries)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clean_restart_continues_persisted_audit_chain() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let guard = FlakeGuard::new("clean_restart_continues_persisted_audit_chain");
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();

    let (before_bytes, before) = authenticated_audit_run(data_dir, 1, &guard).await;
    let old_tail = before
        .last()
        .expect("first boot must persist audit entries");
    let is_admin_login = |entry: &AuditEntry| {
        entry.event == "auth_success" && entry.user == "admin" && entry.result == "ok"
    };
    assert!(before.iter().any(is_admin_login), "first login not audited");

    let (after_bytes, after) = authenticated_audit_run(data_dir, 2, &guard).await;
    assert!(
        after_bytes.starts_with(&before_bytes),
        "restart changed the existing audit.log prefix"
    );
    let first_new = after
        .get(before.len())
        .expect("second boot must append audit entries");
    assert!(
        after[before.len()..].iter().any(is_admin_login),
        "second login not audited in the appended suffix"
    );
    assert_eq!(
        first_new.seq,
        old_tail.seq + 1,
        "first appended audit entry did not continue from persisted tail seq {}",
        old_tail.seq
    );
    assert!(
        first_new.prev_hmac == old_tail.hmac,
        "first appended audit entry does not link to the persisted tail HMAC"
    );

    // Shutdown already waits for store release. Only a lingering Fjall lock
    // warrants retrying the offline metadata open; never retry other errors.
    let meta_path = data_dir.join("server_meta");
    let meta = timeout(AUDIT_PHASE_TIMEOUT, async {
        loop {
            match ServerMetaStore::open_or_init(&meta_path) {
                Err(MetaError::Fjall(fjall::Error::Locked)) => {
                    sleep(Duration::from_millis(50)).await;
                }
                result => break result,
            }
        }
    })
    .await
    .expect("server_meta remained Locked after the second clean shutdown")
    .expect("reopen server_meta after the second clean shutdown");
    let key = Zeroizing::new(meta.audit_chain_key().expect("persisted audit chain key"));
    AuditChain::verify_chain(&key, &after).expect("verify entire persisted chain after restart");

    let final_tail = after
        .last()
        .expect("second boot must leave audit entries on disk");
    // With the finding-1 fix the clean close checkpoints the FINAL position,
    // so BOTH real stores must record exactly the second shutdown's tail.
    let final_position = (final_tail.seq + 1, final_tail.hmac);
    assert_eq!(
        meta.audit_checkpoint(),
        Some(final_position),
        "metadata checkpoint must match the final shutdown tail"
    );
    let appender_position = FjallAuditAppender::load_checkpoint(data_dir)
        .expect("load audit appender checkpoint after the second clean shutdown")
        .expect("audit appender checkpoint must be persisted");
    assert_eq!(
        appender_position, final_position,
        "appender checkpoint must match the final shutdown tail"
    );
    AuditChain::verify_against_checkpoint(&after, final_tail.seq + 1, &final_tail.hmac)
        .expect("final checkpoint must match the persisted chain");
}

/// Finding 4 of the audit-chain review: a final record that is authenticated
/// but lost only its LF is refused by boot AND by the offline token CLI with
/// the SAME typed `UnterminatedActiveRecord`, and nothing may rewrite it.
/// This test covers the boot surface; the CLI surface is covered by
/// `create_and_list_refuse_unterminated_final_record_and_never_append` in the
/// token CLI tests on the same fixture boundary (real stores, real key).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_rejects_authenticated_final_record_missing_only_lf_without_rewriting_it() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let guard = FlakeGuard::new("boot_rejects_authenticated_final_record_missing_only_lf");
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let (complete, _) = authenticated_audit_run(data_dir, 1, &guard).await;
    assert!(complete.ends_with(b"\n"), "expected a complete audit log");
    let stripped = &complete[..complete.len() - 1];
    // The typed offset is the unterminated record's BEGINNING, not its end.
    let record_start = stripped
        .iter()
        .rposition(|&b| b == b'\n')
        .map_or(0usize, |pos| pos + 1);
    let log_path = data_dir.join("audit.log");
    std::fs::write(&log_path, stripped).expect("write audit.log without its final LF");
    guard.mark("wrote audit.log missing only the final LF");

    let result = timeout(
        AUDIT_PHASE_TIMEOUT,
        Box::pin(make_launcher(data_dir).launch()),
    )
    .await
    .expect("unterminated-record boot timed out");
    let error = match result {
        Err(error) => error,
        Ok(handle) => {
            timeout(AUDIT_PHASE_TIMEOUT, Box::pin(handle.shutdown()))
                .await
                .expect("shutdown after unexpected audit boot timed out");
            panic!("boot accepted a final record missing only its LF");
        }
    };
    assert!(
        matches!(
            &error,
            shamir_server::server::BootError::AuditRecovery(
                shamir_server::audit_appender::AuditRecoveryError::UnterminatedActiveRecord {
                    path,
                    offset,
                }
            ) if *offset as usize == record_start && path.ends_with("audit.log")
        ),
        "expected typed UnterminatedActiveRecord at offset {record_start}, got {error:?}"
    );
    guard.mark("boot rejected the unterminated final record");
    assert_eq!(
        std::fs::read(&log_path).expect("read audit.log after rejected boot"),
        stripped,
        "rejected boot must preserve the exact file bytes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_rejects_tampered_audit_log_without_rewriting_it() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let guard = FlakeGuard::new("restart_rejects_tampered_audit_log_without_rewriting_it");
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path();
    let (mut tampered, _) = authenticated_audit_run(data_dir, 1, &guard).await;

    assert!(tampered.ends_with(b"\n"), "expected a complete audit log");
    let event = b"\tauth_success\t";
    let offset = tampered
        .windows(event.len())
        .position(|field| field == event)
        .expect("login must persist an auth_success event");
    // Flip one bit in the event value ('a' -> 'c'), leaving framing and HMAC intact.
    tampered[offset + 1] ^= 0b10;
    let log_path = data_dir.join("audit.log");
    std::fs::write(&log_path, &tampered).expect("write tampered audit.log");
    let entries =
        FjallAuditAppender::read_log_for_verify(data_dir).expect("tampered log must still parse");
    assert!(entries.iter().any(|entry| entry.event == "cuth_success"));
    guard.mark("wrote parseable tampered audit.log");

    let result = timeout(
        AUDIT_PHASE_TIMEOUT,
        Box::pin(make_launcher(data_dir).launch()),
    )
    .await
    .expect("tampered audit boot timed out");
    let error = match result {
        Err(error) => error,
        Ok(handle) => {
            timeout(AUDIT_PHASE_TIMEOUT, Box::pin(handle.shutdown()))
                .await
                .expect("shutdown after unexpected audit boot timed out");
            panic!("boot accepted the tampered audit log");
        }
    };
    assert!(
        matches!(
            error,
            shamir_server::server::BootError::AuditRecovery(
                shamir_server::audit_appender::AuditRecoveryError::HmacMismatch { .. }
            )
        ),
        "expected audit HMAC mismatch, got {error:?}"
    );
    guard.mark("boot rejected the tampered audit log");
    assert_eq!(
        std::fs::read(&log_path).expect("read audit.log after rejected boot"),
        tampered,
        "rejected boot changed audit.log"
    );
}

/// An EXPORT-ENABLED server (`security.export_root` set, runner enabled at
/// boot, NO export ever started) must still release every store at shutdown:
/// `ShutdownOwner` keeps only the export DRAIN handle, and the service
/// reference — owner of `Arc<ShamirDb>` and the durable user directory — is
/// dropped before the store-release probe.
///
/// Kills the mutant that restores `online_export: self.online_export.clone()`
/// in `ServerHandle::take_shutdown_owner` (or removes the
/// `drop(self.online_export.take())` before the probe): the probe then
/// observes the stores still held, the report is unclean (`StillLocked`,
/// exit 70) and the immediate relaunch below can fail with `Locked`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn export_enabled_idle_shutdown_releases_the_stores() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().to_path_buf();
    // The export root must lie OUTSIDE the data directory (placement check in
    // `export_online/root.rs`), so it gets its own temporary parent.
    let export_temp = tempfile::TempDir::new().expect("export tempdir");
    let export_root = export_temp.path().join("export-root");

    let mut config = make_config(&data_dir);
    config.security.export_root = Some(export_root.clone());
    let launcher = ServerLauncher {
        config,
        node_mode: Default::default(),
        bootstrap: BootstrapMode::Password {
            username: "admin".into(),
            password: Zeroizing::new(PASSWORD.to_vec()),
        },
    };
    let handle = launcher.launch().await.expect("export-enabled launch");
    let report = timeout(AUDIT_PHASE_TIMEOUT, Box::pin(handle.shutdown()))
        .await
        .expect("export-enabled shutdown timed out");
    assert!(
        report.is_clean(),
        "export-enabled idle shutdown: {report:?}"
    );
    assert_eq!(
        report.exit_code(),
        0,
        "an idle export-enabled server must not report still-held stores"
    );

    // Release proof: the same data dir relaunches immediately without a lock
    // error (no retry, on purpose).
    let mut config = make_config(&data_dir);
    config.security.export_root = Some(export_root);
    let handle = ServerLauncher {
        config,
        node_mode: Default::default(),
        bootstrap: BootstrapMode::Password {
            username: "admin".into(),
            password: Zeroizing::new(PASSWORD.to_vec()),
        },
    }
    .launch()
    .await
    .expect("relaunch must not hit a store lock left by the export-enabled shutdown");
    drop(handle.shutdown().await);
}
