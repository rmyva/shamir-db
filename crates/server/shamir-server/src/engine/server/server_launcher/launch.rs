//! [`ServerLauncher`]: builds the server runtime from a
//! [`Config`](crate::config::Config) + bootstrap policy in `launch()`.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use zeroize::Zeroizing;

use shamir_connect::server::argon2_semaphore::Argon2Semaphore;
use shamir_connect::server::audit_chain::{AuditAppender, AuditChain, AuditChainWriter};
use shamir_connect::server::dispatch::RequestHandler;
use shamir_connect::server::durable_counters::FjallConsumedCounters;
use shamir_connect::server::lockout::{
    InMemoryLockoutStore, LockoutSnapshotSink, PairDropReason, PairStoreObserver,
};
use shamir_connect::server::rate_limit::{InMemoryRateLimiter, RateLimitSnapshotSink};
use shamir_connect::server::resume::ResumeConfig;
use shamir_connect::server::session::SessionStore;
use shamir_sync::format_stamp::{check_data_dir, EpochPolicy, IgnoreSet, StampMode};

use shamir_tunables::runtime::RuntimeTunables;

use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_db::shamir_db::SystemStoreConfig;
use shamir_db::ShamirDb;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::audit_appender::FjallAuditAppender;
use crate::boot::config::diagnostics::backup_online::{schedule, OnlineBackupRegistry};
use crate::bootstrap::{
    ensure_superuser, BootstrapOutcome, BootstrapPolicy, DEFAULT_BOOTSTRAP_NAME,
};
use crate::byte_budget::ByteBudget;
use crate::config::{Config, ResolvedSchedule};
use crate::connection::ConnTasks;
use crate::db_handler::{
    AdminGlue, CursorLimitsCap, QueryLimitsCap, ShamirDbHandler, SlowQueryConfig, TxLimitsCap,
};
use crate::http_ws::{PreAuthAdmission, TokenWsCancelRegistry};
use crate::ports::DirectoryPorts;
use crate::scheduler::{Scheduler, SchedulerConfig, SchedulerInputs};
use crate::server::boot_error::BootError;
use crate::server::bootstrap_mode::BootstrapMode;
use crate::server::meta_sinks::{spawn_meta_snapshot_task, MetaLockoutSink, MetaRateLimitSink};
use crate::server::server_handle::ServerHandle;
use crate::server_meta::ServerMetaStore;
use crate::tables_registry::TablesRegistry;
use crate::user_directory::FjallUserDirectory;

use super::helpers::{
    bind_backup_owner, boot_now_ns, default_node_id, kdf_from_config, log_bootstrap_outcome,
    resolve_backup, REPL_RECONCILE_INTERVAL,
};
use super::http::BootGuard;
use super::listeners::{spawn_listeners, ListenerEnv, SpawnedListeners};
use super::load_metrics::bind_load_source;

/// Launcher: build the server runtime from a [`Config`] + bootstrap policy.
pub struct ServerLauncher {
    pub config: Config,
    pub bootstrap: BootstrapMode,
    pub node_mode: crate::db_handler::NodeMode,
}

impl ServerLauncher {
    /// New launcher with the given config and the default bootstrap mode
    /// (random token, written to `data_dir/bootstrap_token.txt`).
    pub fn new(config: Config) -> Self {
        Self {
            config,
            bootstrap: BootstrapMode::default(),
            node_mode: crate::db_handler::NodeMode::ReadWrite,
        }
    }

    pub fn with_node_mode(mut self, mode: crate::db_handler::NodeMode) -> Self {
        self.node_mode = mode;
        self
    }

    pub fn with_bootstrap(mut self, bootstrap: BootstrapMode) -> Self {
        self.bootstrap = bootstrap;
        self
    }

    /// Run the boot sequence — everything from "open the redb files" to
    /// "spawn the accept loops". Returns when all listeners are bound but
    /// before any connection arrives.
    pub async fn launch(self) -> Result<ServerHandle, BootError> {
        self.launch_inner(schedule::runner::spawn).await
    }

    #[cfg(test)]
    pub(super) async fn launch_with_backup_test(
        self,
        spawn_backup: impl FnOnce(
            ResolvedSchedule,
            Arc<OnlineBackupRegistry>,
            String,
            CancellationToken,
        ) -> JoinHandle<()>,
    ) -> Result<ServerHandle, BootError> {
        self.launch_inner(spawn_backup).await
    }

    async fn launch_inner(
        self,
        spawn_backup: impl FnOnce(
            ResolvedSchedule,
            Arc<OnlineBackupRegistry>,
            String,
            CancellationToken,
        ) -> JoinHandle<()>,
    ) -> Result<ServerHandle, BootError> {
        let ServerLauncher {
            config,
            bootstrap,
            node_mode,
        } = self;
        config.validate()?;
        let durability = config.durability.resolve_policy()?;
        config.durability.warn_unimplemented_retention();

        // Root shutdown token — the single source of truth for "the server
        // is stopping". Every long-lived task (accept loops, meta-snapshot,
        // tx-reaper) holds a clone; `ServerHandle::shutdown` calls `.cancel()`
        // exactly once and the signal cascades to all of them. The tree of
        // tasks maps onto one cancellation: shutdown is a LEVEL (a state),
        // and CancellationToken is level-triggered (persistent) — unlike
        // `Notify::notify_waiters`, which is edge-triggered and lossy across
        // the `select!` subscribe-window (a notify that lands while a task is
        // executing its tick-branch body, not parked in `select!`, is
        // silently dropped — the cold-start hang we chased through the accept
        // loops). One concept, race-free by construction.
        let shutdown_token = CancellationToken::new();
        let mut boot_guard = BootGuard::new(shutdown_token.clone());
        let conn_tasks = ConnTasks::new(shutdown_token.clone());

        // 1. Durable stores.
        std::fs::create_dir_all(&config.data_dir)?;
        let backup = resolve_backup(&config)?;

        // Single-instance guard: acquire an advisory OS file lock on
        // `<data_dir>/.shamir.lock` BEFORE opening any redb store. The
        // lock is held for the entire process lifetime via the `File`
        // stored in `ServerHandle._data_dir_lock` — drop releases it
        // (and the kernel releases on crash). This prevents two servers
        // from opening the same data_dir and corrupting/contending the
        // redb stores.
        let data_dir_lock = crate::maintenance_lock::acquire_server(&config.data_dir)?;

        // Format-stamp gate (design 3.2): between the single-instance lock
        // and the first store open, so a refusal leaves every store closed.
        // The configured log file counts as data only when it lives in the
        // data directory itself (design 3.1).
        let format_stamp_ignore = config
            .logging
            .file
            .as_ref()
            .and_then(|log| {
                let log = Path::new(log);
                if log.parent() != Some(config.data_dir.as_path()) {
                    return None;
                }
                log.file_name()
                    .map(|name| IgnoreSet::with_log_file(name.to_string_lossy()))
            })
            .unwrap_or_else(IgnoreSet::none);
        let format_stamp = check_data_dir(
            &config.data_dir,
            StampMode::Writer,
            &format_stamp_ignore,
            &EpochPolicy::compiled(),
        )?;

        let meta = Arc::new(
            ServerMetaStore::open_or_init(config.data_dir.join("server_meta"))
                .map_err(|e| BootError::ServerMeta(e.to_string()))?,
        );

        let backup = bind_backup_owner(backup, &meta)?;

        let user_dir = Arc::new(
            FjallUserDirectory::open(config.data_dir.join("users"))
                .map_err(|e| BootError::UserDirectory(e.to_string()))?,
        );
        let counters = Arc::new(
            FjallConsumedCounters::open(config.data_dir.join("counters"))
                .map_err(|e| BootError::Counters(e.to_string()))?,
        );

        let audit_max_bytes = if config.audit.max_file_size_mb == 0 {
            None
        } else {
            Some(config.audit.max_file_size_mb.saturating_mul(1024 * 1024))
        };
        let audit_appender = FjallAuditAppender::open_batched_with_rotation(
            &config.data_dir,
            std::time::Duration::from_secs(5),
            audit_max_bytes,
            config.audit.retention_days,
        )
        .map_err(|e| BootError::AuditAppender(e.to_string()))?;

        // Hoisted from step 2 below (CR-A6): the TTL sweep now also needs
        // `kdf_for_bootstrap` to rotate an expired-and-unused token's SCRAM
        // credential, so this construction moved ahead of the sweep block.
        let kdf_for_bootstrap = kdf_from_config(&config.kdf_defaults);

        // 1b. Boot-time bootstrap-token TTL sweep (RI-9 + CR-A6). Runs
        // unconditionally, in ALL bootstrap modes, BEFORE the bootstrap step
        // below: a previously-issued token from an earlier `RandomToken`
        // boot can still be outstanding and expired even if this boot is
        // `Skip`/`Password`. Best-effort — an I/O failure deleting the file,
        // or a rotation failure, is logged and does not fail boot. CR-A6:
        // also rotates the account's SCRAM credential to a fresh random
        // value — without this an unused, TTL-expired token would still
        // work as the account's password forever, contradicting the
        // "one-time token" guarantee.
        //
        // F-3: ordering is rotate-FIRST → consume-on-success → delete-file-
        // last. If the rotate fails — or there is no username to rotate —
        // the token metadata row is LEFT active so the next boot's sweep
        // gets another chance. The username MUST be read BEFORE
        // `consume_bootstrap_token()` clears the bookkeeping row (order
        // matters); the token path is likewise read before consume.
        {
            let now_ns = boot_now_ns()?;
            if meta.bootstrap_token_expired(now_ns) {
                let expired_username = meta.bootstrap_username();

                let rotated_ok = match expired_username {
                    Some(name) => {
                        match crate::bootstrap::rotate_bootstrap_credential_to_random(
                            &user_dir,
                            &name,
                            kdf_for_bootstrap,
                            now_ns,
                        )
                        .await
                        {
                            Ok(()) => true,
                            Err(e) => {
                                tracing::warn!(
                                    ?e,
                                    "bootstrap: failed to rotate SCRAM credential for expired \
                                     token -- token left active for retry"
                                );
                                false
                            }
                        }
                    }
                    None => false,
                };

                if rotated_ok {
                    let expired_token_path = meta.bootstrap_token_path();
                    if let Err(e) = meta.consume_bootstrap_token() {
                        tracing::warn!(?e, "bootstrap: failed to consume expired token record");
                    }
                    if let Some(path) = expired_token_path {
                        if let Err(e) = std::fs::remove_file(&path) {
                            if e.kind() != std::io::ErrorKind::NotFound {
                                tracing::warn!(
                                    ?path,
                                    ?e,
                                    "bootstrap: failed to delete expired token file"
                                );
                            }
                        }
                    }
                }
            }
        }

        // 2. Bootstrap (idempotent).
        match &bootstrap {
            BootstrapMode::Password { username, password } => {
                let outcome = ensure_superuser(
                    &user_dir,
                    &config.data_dir,
                    username,
                    BootstrapPolicy::Password(password),
                    &kdf_for_bootstrap,
                )?;
                log_bootstrap_outcome(username, &outcome);
            }
            BootstrapMode::RandomToken {
                username,
                token_path,
            } => {
                let name = username.as_deref().unwrap_or(DEFAULT_BOOTSTRAP_NAME);
                let now_ns = boot_now_ns()?;
                let outcome = ensure_superuser(
                    &user_dir,
                    &config.data_dir,
                    name,
                    BootstrapPolicy::RandomToken(token_path.clone()),
                    &kdf_for_bootstrap,
                )?;
                if let BootstrapOutcome::Created {
                    token: Some(tok),
                    token_path: Some(path),
                } = &outcome
                {
                    if let Err(e) = meta.set_bootstrap_token(
                        name,
                        shamir_connect::common::crypto::sha256(tok.as_bytes()),
                        now_ns.saturating_add(crate::bootstrap::BOOTSTRAP_TOKEN_TTL_NS),
                        path.clone(),
                    ) {
                        tracing::warn!(?e, "bootstrap: failed to persist token TTL/consume record");
                    }
                }
                log_bootstrap_outcome(name, &outcome);
            }
            BootstrapMode::Skip => {
                tracing::debug!("bootstrap: skipped per BootstrapMode::Skip");
            }
        }

        // 3. In-memory stores.
        //
        // Lockout state is rehydrated from `server_meta.redb` and a
        // background task (`LOCKOUT_SNAPSHOT_INTERVAL` below) persists it
        // every 60s so brute-force bookkeeping survives server restarts
        // — otherwise an attacker who knows the restart cadence resets
        // all per-pair failure counts and re-bursts the auth path with
        // no lockout penalty. See `shamir_connect::server::lockout` for
        // the trade-off rationale (≤60s loss window vs fsync-per-failure).
        let lockout_sink: Arc<dyn LockoutSnapshotSink> =
            Arc::new(MetaLockoutSink::new(meta.clone()));
        let lockout_observer: Arc<PairStoreObserver> = Arc::new(|occupied, reason| {
            metrics::gauge!("shamir_login_http_pair_slots").set(occupied as f64);
            match reason {
                Some(PairDropReason::PairQuota) => {
                    metrics::counter!("shamir_login_http_accounting_dropped", "reason" => "pair_quota").increment(1);
                }
                Some(PairDropReason::TableFull) => {
                    metrics::counter!("shamir_login_http_accounting_dropped", "reason" => "pair_table_full").increment(1);
                }
                None => {}
            }
        });
        let lockout = Arc::new(InMemoryLockoutStore::with_snapshot_sink_and_observer(
            lockout_sink,
            Some(lockout_observer),
        ));
        let now_ns = boot_now_ns()?;
        // Rate-limit buckets are persisted by the SAME periodic task as
        // lockout (one 60s tick writes both). On boot the depleted-bucket
        // state is rehydrated conservatively: token levels survive but each
        // bucket's refill clock is re-anchored to `now_ns`, so the downtime
        // grants no free refill (the secure direction). The §8.6 warmup
        // window (re-armed from `now_ns`) layers extra throttling on top
        // for the first 60s. See `shamir_connect::server::rate_limit`.
        let rate_limit_sink: Arc<dyn RateLimitSnapshotSink> =
            Arc::new(MetaRateLimitSink::new(meta.clone()));
        let rate_limit = Arc::new(InMemoryRateLimiter::with_snapshot_sink_and_rate(
            rate_limit_sink,
            now_ns,
            config.security.auth_init_rate_per_second,
        ));
        let argon2_sem = Arc::new(Argon2Semaphore::with_capacity(config.argon2_concurrent_max));
        let session_store = Arc::new(SessionStore::new());

        // Spawn the periodic meta-snapshot task. One 60s tick persists
        // BOTH the lockout store and the rate-limiter buckets. Errors are
        // logged and ignored — losing a snapshot is not fatal, the next
        // tick will retry, and a hard crash between writes loses at most
        // one interval of new state. The returned handle is stored on
        // `ServerHandle` so `shutdown()` can join it (otherwise it would
        // hold the redb file lock past shutdown and block any
        // same-data-dir restart).
        let meta_snapshot_task = Some(spawn_meta_snapshot_task(
            lockout.clone(),
            rate_limit.clone(),
            shutdown_token.clone(),
        ));

        // The metrics recorder is installed later in boot, so pair-slot
        // updates published before then are lost; republish the absolute
        // occupancy.
        {
            let lockout = lockout.clone();
            let token = shutdown_token.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval_at(
                    tokio::time::Instant::now() + Duration::from_secs(5),
                    Duration::from_secs(30),
                );
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = token.cancelled() => break,
                        _ = tick.tick() => {
                            lockout.republish_occupancy();
                        }
                    }
                }
            });
        }

        // 4. Audit chain — load from checkpoint if present.
        //
        // CRITICAL: the writer (used by every connection task to emit
        // audit events) and the scheduler (which periodically persists
        // the truncation-defence checkpoint) MUST share a single chain
        // state. Constructing two independent `AuditChain` instances
        // here produces a split-brain — appends advance one chain while
        // the checkpoint snapshots the other (empty) one, so audit.log
        // restarts at seq=1 on every reboot. See
        // `audit_writer_and_checkpoint_share_chain_state` for the
        // regression test.
        let ack = meta
            .audit_chain_key()
            .map_err(|e| BootError::ServerMeta(e.to_string()))?;
        let audit_chain = match meta.audit_checkpoint() {
            Some((seq, hmac)) => Arc::new(AuditChain::from_checkpoint(ack, seq, hmac)),
            None => Arc::new(AuditChain::new(ack)),
        };
        let audit_writer = Arc::new(AuditChainWriter::new_with_shared(
            audit_chain.clone(),
            audit_appender.clone() as Arc<dyn AuditAppender>,
        ));

        // 5. ShamirDb — durable system store + a pre-configured `default.main`
        //    repo backed by redb. Layout under `data_dir`:
        //
        //      shamir_db_meta.redb         — system store (db/repo/setting metadata)
        //      shamir_db_default_main.redb — the `default.main` repo data
        //
        //    Wire-side admin ops can still create additional dbs/repos, but
        //    those are in-memory (`ShamirAdminExecutor` hardcodes engine =
        //    "in_memory"; a future patch can extend it to accept "redb" with
        //    auto-derived paths). For v1 this means the `default.main` repo
        //    is the durable target for application data.
        let meta_path = config.data_dir.join("shamir_db_meta.redb");
        let default_main_path = config.data_dir.join("shamir_db_default_main.redb");
        // Task #559: inject the identity-seam ports (PrincipalResolver +
        // UserAdminPort) over the real FjallUserDirectory BEFORE wrapping
        // ShamirDb in Arc. The builder methods consume Self and return
        // Self, so this must happen on the owned value.
        let (port, resolver) =
            DirectoryPorts::new(user_dir.clone(), kdf_for_bootstrap).into_trait_objects();
        let shamir = Arc::new(
            ShamirDb::init_with_durability(SystemStoreConfig::Fjall(meta_path), durability)
                .await
                .map_err(|e| BootError::ShamirDbInit(e.to_string()))?
                .with_user_admin_port(port)
                .with_principal_resolver(resolver),
        );

        // Consume only repos loaded from the restored catalogue, before new
        // default repos, handlers, listeners, or replication tasks can exist.
        crate::restore::consume_restore_marker(&config.data_dir, &shamir)
            .await
            .map_err(|e| BootError::ShamirDbInit(format!("restore history epoch: {e}")))?;

        // R2a step 3a (design D6): construct the per-server online-backup
        // registry over this process's data dir and install it as the
        // engine's `OnlineBackupAdminPort`, so the node-local
        // `StartBackup`/`BackupStatus`/`CancelBackup` admin ops reach it.
        // The registry holds an `Arc<ShamirDb>` clone, so the port must be
        // installed after the engine is wrapped — same one-shot discipline
        // as the `set_replication_resume_probe` seam. The run registry is
        // wired to this server's root shutdown token: each run takes a
        // child token, so shutdown cancels the in-flight run while an
        // admin `CancelBackup` cancels only the child (never the server).
        let online_backup_registry = Arc::new(OnlineBackupRegistry::new_with_shutdown(
            Arc::clone(&shamir),
            config.data_dir.clone(),
            tokio::runtime::Handle::current(),
            shutdown_token.clone(),
        ));
        shamir.set_online_backup_admin_port(online_backup_registry.clone());

        // F-15: opt into the experimental online storage-migration API when
        // the operator explicitly set `security.enable_experimental_migration_api`
        // in the config file. This single `Arc<ShamirDb>` (via its internal
        // `Arc<AtomicBool>` toggle) is the SAME instance every wire-level
        // `StartMigration` handler reads `experimental_migration_enabled()`
        // from, so this one boot-time toggle suffices for the whole process.
        // The flag is `false` by default — the feature has known correctness
        // gaps (see KNOWN_LIMITATIONS.md §2); never enable in production.
        if config.security.enable_experimental_migration_api {
            shamir.enable_experimental_migration_api();
            tracing::warn!(
                "experimental online storage-migration API is ENABLED — see \
                 KNOWN_LIMITATIONS.md §2 for known gaps; do not use in production"
            );
        }

        // Task #559 §8: one-time boot audit (read-only, WARN-log only, no
        // auto-apply). Diffs shamir-db's Store B against the directory and
        // logs WARN lines for usernames/roles/superuser grants that were
        // never live. Removable after one release.
        super::audit_store_b_vs_directory(&shamir, user_dir.as_ref()).await;

        // Idempotent: on the first boot the database+repo are created; on
        // every subsequent boot `ShamirDb::init` already loaded them from
        // the system store, so we skip and just use them.
        let default_db = super::helpers::ensure_default_database(&shamir).await?;
        if !default_db.has_repo("main") {
            let factory = BoxRepoFactory::fjall(&default_main_path);
            shamir
                .add_repo("default", RepoConfig::new("main", factory))
                .await
                .map_err(|e| BootError::ShamirDbInit(format!("add_repo default.main: {e}")))?;
            tracing::info!(path = ?default_main_path, "created durable default.main repo");
        }

        // Replay the wire-tables registry: re-register every table that a
        // wire client created in a previous boot so its data file is
        // re-attached to the in-memory `RepoInstance`. Without this step
        // the redb file on disk still exists but the running server has no
        // table config pointing at it, so reads return "table not found".
        let tables_registry = Arc::new(TablesRegistry::open(&config.data_dir)?);
        {
            let snap = tables_registry.snapshot();
            for (db_name, repo_name, table_name) in snap.iter_entries() {
                if let Some(db) = shamir.get_db(db_name) {
                    if !db.has_table(repo_name, table_name) {
                        if let Err(e) = db.create_table_checked(repo_name, table_name).await {
                            tracing::warn!(
                                db = db_name,
                                repo = repo_name,
                                table = table_name,
                                ?e,
                                "tables_registry replay: create_table failed"
                            );
                        }
                    }
                }
            }
        }
        // RI-15 — global in-flight response-byte budget. F-29 (#822): the
        // code-level default is now finite
        // (`default_max_inflight_response_bytes`, 4x `max_result_size_bytes`'s
        // own default) — `None` here means the operator EXPLICITLY set
        // `max_inflight_response_bytes: null`, opting back into unbounded
        // (pre-RI-15) behavior: only the per-batch `max_result_size_bytes`
        // cap applies, no ceiling on the SUM across concurrent connections.
        // Warn at boot so that choice isn't silent. `Config::validate`
        // already rejected a configured `Some(_)` value below
        // `max_result_size_bytes` before boot reached this point.
        let inflight_cap = config.security.query_limits.max_inflight_response_bytes;
        if inflight_cap.is_none() {
            tracing::warn!(
                "security.query_limits.max_inflight_response_bytes is explicitly unbounded \
                 (null) — no ceiling on the SUM of in-flight response bytes across every \
                 concurrent connection; only the per-batch max_result_size_bytes cap applies. \
                 This is an explicit opt-out of the F-29 finite default — confirm this is \
                 intentional for this deployment"
            );
        }
        let byte_budget = ByteBudget::new(inflight_cap);

        let token_key = Zeroizing::new(
            *meta
                .token_key()
                .map_err(|e| BootError::ServerMeta(e.to_string()))?
                .as_bytes(),
        );
        let token_ws_cancel_registry = Arc::new(TokenWsCancelRegistry::default());
        let pre_auth_admission = Arc::new(PreAuthAdmission::default());
        let token_service = Arc::new(
            crate::security::tokens::TokenService::open(
                crate::security::tokens::TokenServiceOptions {
                    data_dir: config.data_dir.clone(),
                    token_key,
                    owners: Arc::new(crate::security::tokens::DirectoryOwnerFloors::new(
                        user_dir.clone(),
                    )),
                    sessions: Arc::new(shamir_connect::server::TokenSessionStore::new()),
                    audit: audit_writer.clone(),
                    config: config.tokens.clone(),
                    read_only_node: node_mode == crate::db_handler::NodeMode::ReadOnly,
                    clock: Arc::new(|| {
                        shamir_connect::common::time::UnixNanos::try_now()
                            .ok()
                            .map(|v| v.as_u64())
                    }),
                },
            )
            .await
            .map_err(|e| BootError::TokenService(e.to_string()))?
            .with_canceller(token_ws_cancel_registry.clone()),
        );

        // R1c §4.5: lower the node mode into the engine so application-schema
        // apply also fails closed engine-side; the server gate above stays
        // the first refusal.
        shamir.set_node_read_only(node_mode == crate::db_handler::NodeMode::ReadOnly);

        let handler_concrete: Arc<ShamirDbHandler> = Arc::new(
            ShamirDbHandler::with_admin(
                shamir.clone(),
                AdminGlue {
                    user_dir: user_dir.clone(),
                    kdf: kdf_for_bootstrap,
                    tables_registry: Some(tables_registry.clone()),
                },
            )
            .with_audit_writer(audit_writer.clone())
            .with_slow_query(SlowQueryConfig::from_ms(
                config.logging.slow_query_threshold_ms,
            ))
            .with_query_limits(QueryLimitsCap::from(&config.security.query_limits))
            .with_transactions(config.transactions.clone())
            .with_tx_limits(TxLimitsCap {
                max_tx_bytes: config.security.tx.max_tx_bytes,
            })
            .with_cursor_limits(CursorLimitsCap {
                max_cursors_per_session: config.security.cursors.max_cursors_per_session,
                idle_timeout_secs: config.security.cursors.idle_timeout_secs,
                max_cursor_page_size: config.security.cursors.max_cursor_page_size,
            })
            .with_byte_budget(byte_budget.clone())
            .with_session_store(session_store.clone())
            .with_node_mode(node_mode)
            .with_token_service(token_service.clone(), config.tokens.allow_self_service),
        );
        let cancel_refusal_audit = handler_concrete.cancel_refusal_audit();
        let tx_registry_for_reaper = handler_concrete.tx_registry();
        let cursor_registry_for_reaper = handler_concrete.cursor_registry();
        let handler: Arc<dyn RequestHandler> = handler_concrete.clone();

        // 5b. Follower-replication supervisor (386-c). Constructed here where
        //     `shamir: Arc<ShamirDb>` is available. It is NOT reconciled yet —
        //     the initial `reconcile()` runs AFTER all listeners are bound
        //     (step 10b) so a follower does not dial an upstream before this
        //     server is itself ready to serve. The prod factory builds a
        //     `WireReplSource` (TLS+SCRAM) per subscription; when no replicator
        //     credentials are configured the factory is a no-op stub and any
        //     `active` subscription is logged-and-skipped by reconcile.
        let repl_cfg = config.replication.clone().unwrap_or_default();
        let node_id = repl_cfg.node_id.clone().unwrap_or_else(default_node_id);
        let repl_factory = crate::replication::build_prod_factory(
            repl_cfg.replicator_user.clone(),
            repl_cfg.replicator_password.clone(),
            repl_cfg.server_name.clone(),
            repl_cfg.upstream_pins.clone(),
        )
        .unwrap_or_else(|| {
            // No creds → a stub factory that yields a never-connecting source.
            // reconcile() still runs (idempotent, no-op for empty catalogues);
            // a subscription with a real upstream simply cannot progress until
            // credentials are configured. This keeps boot working with the
            // default `replication = None`.
            Arc::new(|_: &crate::replication::Subscription| {
                Arc::new(crate::replication::prod_factory::NoopReplSource)
                    as Arc<dyn crate::replication::ReplSource>
            })
        });
        let supervisor = Arc::new(
            crate::replication::SubscriptionSupervisor::new(shamir.clone(), repl_factory, node_id)
                .with_validate_replicated_payload(repl_cfg.validate_replicated_payload),
        );

        // Phase B Stage 6 — background reaper for interactive txs that
        // outlive their idle TTL or absolute deadline. Shares the root
        // shutdown token: `ServerHandle::shutdown` cancels it once and this
        // task observes the cancel on its next `select!` poll.
        let interactive_tx_reaper = Some(crate::tx_registry::spawn_reaper_task(
            tx_registry_for_reaper,
            crate::tx_registry::DEFAULT_INTERACTIVE_TX_IDLE_TTL,
            crate::tx_registry::DEFAULT_REAPER_INTERVAL,
            shutdown_token.clone(),
        ));

        // FG-5b — background reaper for result cursors that outlive their
        // idle TTL. Shares the root shutdown token, same pattern as the
        // interactive-tx reaper above.
        let cursor_reaper = Some(crate::cursor_registry::spawn_reaper_task(
            cursor_registry_for_reaper,
            Duration::from_secs(config.security.cursors.idle_timeout_secs),
            crate::cursor_registry::DEFAULT_CURSOR_REAPER_INTERVAL,
            shutdown_token.clone(),
        ));

        // 6. ResumeConfig.
        let (current_key, previous_key) = meta
            .ticket_keys()
            .map_err(|e| BootError::ServerMeta(e.to_string()))?;
        let resume_config = Arc::new(ResumeConfig::new(
            current_key,
            previous_key,
            true,  // allow_browser_ticket_upgrade
            false, // disable_plain_ticket_upgrade
        ));

        // 7. Identity material.
        let identity = Arc::new(
            meta.identity_state()
                .map_err(|e| BootError::ServerMeta(e.to_string()))?,
        );
        tracing::info!(pin = %hex::encode(shamir_connect::common::crypto::sha256(&identity.current_pub())), "server Ed25519 identity SHA-256 pin");
        let identity_seed = meta
            .current_identity_seed()
            .map_err(|e| BootError::ServerMeta(e.to_string()))?;
        let secrets = Arc::new(
            meta.server_secrets()
                .map_err(|e| BootError::ServerMeta(e.to_string()))?,
        );

        // 8. Scheduler.
        let scheduler = Scheduler::spawn(
            SchedulerInputs {
                counters: counters.clone(),
                lockout: lockout.clone(),
                rate_limit: rate_limit.clone(),
                session_store: session_store.clone(),
                session_max_age_ns: 24 * shamir_connect::common::time::ns::HOUR,
                session_idle_ttl_ns: 30 * shamir_connect::common::time::ns::MINUTE,
                audit_chain: audit_chain.clone(),
                audit_appender: audit_appender.clone(),
                identity: identity.clone(),
            },
            SchedulerConfig::default_for_production(),
        );

        // 9-10. TLS material, then bind every listener and spawn its accept
        //       loop. They share the root `shutdown_token` created at the top
        //       of `launch()` — `shutdown()` cancels it once and every accept
        //       loop observes the cancel on its next `select!` poll.
        let SpawnedListeners {
            conn_limiter,
            bound_addrs,
            bound_ipc_paths,
            listener_tasks,
            http_gates,
        } = match spawn_listeners(
            &ListenerEnv {
                config: &config,
                identity: &identity,
                identity_seed,
                secrets: &secrets,
                kdf: kdf_for_bootstrap,
                session_store: &session_store,
                user_dir: &user_dir,
                lockout: &lockout,
                rate_limit: &rate_limit,
                argon2_sem: &argon2_sem,
                audit_writer: &audit_writer,
                resume_config: &resume_config,
                handler: &handler,
                meta: &meta,
                token_service: &token_service,
                token_ws_cancel_registry: &token_ws_cancel_registry,
                pre_auth_admission: &pre_auth_admission,
                handler_concrete: &handler_concrete,
                conn_tasks: &conn_tasks,
                read_only: node_mode == crate::db_handler::NodeMode::ReadOnly,
            },
            &shutdown_token,
        )
        .await
        {
            Ok(listeners) => listeners,
            Err(error) => {
                cancel_refusal_audit.close();
                shutdown_token.cancel();
                cancel_refusal_audit.drain().await;
                audit_appender.shutdown().await;
                scheduler.shutdown().await;
                if let Some(snap) = meta_snapshot_task {
                    let _ = snap.handle.await;
                }
                if let Some(reaper) = interactive_tx_reaper {
                    let _ = reaper.handle.await;
                }
                if let Some(reaper) = cursor_reaper {
                    let _ = reaper.handle.await;
                }
                return Err(error);
            }
        };
        // 11. Optional observability HTTP server. Spawned LAST so its
        //     `/readyz` flips to 200 only after every listener is bound.
        let observability_result: Result<
            Option<crate::observability::ObservabilityHandle>,
            BootError,
        > = async {
            if config.observability.addr.is_empty() {
                return Ok(None);
            }
            let addr: std::net::SocketAddr = config.observability.addr.parse().map_err(|e| {
                BootError::Bind(format!(
                    "observability.addr {} invalid: {e}",
                    config.observability.addr
                ))
            })?;
            let state = crate::observability::ObservabilityState::new();
            // R4b-1: install the per-server activity gauge source on THIS
            // server's observability state. The four `shamir_active_*`
            // gauges are appended per scrape from this handler's
            // `activity_counts()` mirrors, never written into the
            // process-global Prometheus recorder — two servers in one
            // process share that recorder, so a concurrent scrape of one
            // must never return the other's counts. The source holds the
            // handler only by `Weak`: the observability state cannot keep
            // the handler alive past shutdown, and after the handler is
            // dropped the gauges render zeros instead of a stale server's
            // mirrors.
            let handler_for_activity = Arc::downgrade(&handler_concrete);
            state.set_activity_source(Arc::new(move || {
                handler_for_activity
                    .upgrade()
                    .map_or_else(Default::default, |handler| handler.activity_counts())
            }));
            bind_load_source(
                &state,
                conn_limiter,
                &session_store,
                token_service.counters(),
            );
            // Record what the data-path bound to — `/info` surfaces this.
            state.set_bound_addrs(bound_addrs.iter().filter_map(|a| *a).collect());
            // A duplicate global recorder must not stop boot; the fallback
            // starts the observability listener without a Prometheus handle.
            // M-tier audit M5: `allow_public_metrics` comes straight from
            // the parsed config (`ObservabilityConfig::allow_public_metrics`,
            // default `false`). A non-loopback `addr` is rejected up-front
            // unless the operator has explicitly opted in.
            let allow_public_metrics = config.observability.allow_public_metrics;
            let handle = match crate::observability::spawn_with_byte_budget(
                addr,
                state.clone(),
                true,
                None,
                Some(byte_budget.clone()),
                Some(shamir.clone()),
                allow_public_metrics,
            )
            .await
            {
                Ok(h) => h,
                Err(crate::observability::ObservabilityError::RecorderInstall(_)) => {
                    // Recorder already installed (typical in test process):
                    // re-spawn without trying to install again.
                    crate::observability::spawn_with_byte_budget(
                        addr,
                        state.clone(),
                        false,
                        None,
                        Some(byte_budget.clone()),
                        Some(shamir.clone()),
                        allow_public_metrics,
                    )
                    .await
                    .map_err(|e| BootError::Bind(format!("observability: {e}")))?
                }
                Err(e) => return Err(BootError::Bind(format!("observability: {e}"))),
            };
            Ok(Some(handle))
        }
        .await;
        let observability = match observability_result {
            Ok(handle) => handle,
            Err(error) => {
                cancel_refusal_audit.close();
                shutdown_token.cancel();
                let listener_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                for mut task in listener_tasks {
                    let abort = task.abort_handle();
                    match tokio::time::timeout_at(listener_deadline, &mut task).await {
                        Ok(Ok(())) => {}
                        Ok(Err(join_error)) => {
                            tracing::error!(%join_error, "listener task failed during launch rollback");
                        }
                        Err(_) => {
                            tracing::error!(
                                "HTTP listener drain exceeded during launch rollback; aborting"
                            );
                            abort.abort();
                            if tokio::time::timeout(Duration::from_secs(2), &mut task)
                                .await
                                .is_err()
                            {
                                tracing::error!(
                                    "listener task did not stop after launch rollback abort"
                                );
                            }
                        }
                    }
                }
                if tokio::time::timeout(Duration::from_secs(10), conn_tasks.wait_idle())
                    .await
                    .is_err()
                {
                    conn_tasks.abort_all();
                    let _ =
                        tokio::time::timeout(Duration::from_secs(2), conn_tasks.wait_idle()).await;
                }
                for gate in http_gates {
                    gate.shutdown().await;
                }
                token_service.shutdown_gc().await;
                cancel_refusal_audit.drain().await;
                audit_appender.shutdown().await;
                scheduler.shutdown().await;
                if let Some(snap) = meta_snapshot_task {
                    let _ = snap.handle.await;
                }
                if let Some(reaper) = interactive_tx_reaper {
                    let _ = reaper.handle.await;
                }
                if let Some(reaper) = cursor_reaper {
                    let _ = reaper.handle.await;
                }
                return Err(error);
            }
        };
        token_service.start_gc(shutdown_token.clone()).await;

        // 10b. Now that every listener is bound, converge the follower loops
        //      to the declarative `system/subscriptions` catalogue and spawn a
        //      periodic reconcile-tick so a subscription added/paused/dropped
        //      at runtime is picked up without a restart. §5.6: the tick runs
        //      on its own task and never blocks the server.
        //
        //      TODO(386-c): replace the fixed-interval tick with an
        //      event-driven watch on the `system` repo changelog (table
        //      `subscriptions`) → `supervisor.notify_changed()` on each event.
        supervisor.reconcile().await;
        let repl_supervisor_task = {
            let sup = supervisor.clone();
            let token = shutdown_token.clone();
            let handle = tokio::spawn(async move {
                let mut tick = tokio::time::interval(REPL_RECONCILE_INTERVAL);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                // First tick fires immediately; skip it (we just reconciled).
                tick.tick().await;
                loop {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => break,
                        _ = tick.tick() => sup.reconcile().await,
                    }
                }
            });
            crate::server::server_handle::ReplSupervisorTask { handle }
        };

        if let Some(obs) = &observability {
            obs.state.mark_ready();
        }
        let backup_schedule_task = backup.map(|(resolved, owner)| {
            let shutdown = shutdown_token.clone();
            let handle = spawn_backup(resolved, online_backup_registry, owner, shutdown.clone());
            (shutdown.drop_guard(), handle)
        });

        boot_guard.disarm();
        Ok(ServerHandle {
            backup_schedule_task,
            token_service,
            bound_addrs,
            bound_ipc_paths,
            listener_tasks,
            http_gates,
            scheduler,
            audit_appender,
            cancel_refusal_audit,
            #[cfg(test)]
            refusal_drain_pending: None,
            shutdown_token,

            meta_snapshot_task,
            interactive_tx_reaper,
            cursor_reaper,
            repl_supervisor: supervisor,
            repl_supervisor_task: Some(repl_supervisor_task),
            shamir,
            data_dir: config.data_dir.clone(),
            conn_tasks: conn_tasks.clone(),
            format_stamp,
            _data_dir_lock: data_dir_lock,
            tunables: Arc::new(RuntimeTunables::new()),
            // Tests drive the real handler and scrape the actual bound metrics address.
            #[cfg(test)]
            activity_test_seam: observability.as_ref().map(|obs| {
                crate::server::server_handle::ActivityGaugeTestSeam {
                    handler: Arc::clone(&handler_concrete),
                    byte_budget: byte_budget.clone(),
                    session_store: session_store.clone(),
                    audit_chain: audit_chain.clone(),
                    observability_bound_addr: obs.bound_addr,
                }
            }),
            observability,
        })
    }
}
