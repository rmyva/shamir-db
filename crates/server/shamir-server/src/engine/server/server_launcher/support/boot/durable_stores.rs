use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::{Handle, RuntimeFlavor};
use zeroize::Zeroizing;

use shamir_connect::common::kdf_params::KdfParams;
use shamir_connect::server::audit_chain::{AuditAppender, AuditChain};
use shamir_connect::server::durable_counters::FjallConsumedCounters;
use shamir_sync::format_stamp::{
    check_data_dir, inspect, EpochPolicy, IgnoreSet, StampError as FsStampError, StampMode,
    StampOutcome, StampState, CURRENT_FORMAT_EPOCH,
};

use crate::audit_appender::FjallAuditAppender;
use crate::bootstrap::{
    ensure_superuser, BootstrapOutcome, BootstrapPolicy, DEFAULT_BOOTSTRAP_NAME,
};
use crate::config::{Config, ResolvedSchedule};
use crate::server::boot_error::BootError;
use crate::server::bootstrap_mode::BootstrapMode;
use crate::server_meta::ServerMetaStore;
use crate::user_directory::FjallUserDirectory;

use super::helpers::{
    bind_backup_owner, boot_now_ns, kdf_from_config, log_bootstrap_outcome, resolve_backup,
};

pub(super) struct DurableStores {
    pub(super) data_dir_lock: File,
    pub(super) format_stamp: StampOutcome,
    pub(super) meta: Arc<ServerMetaStore>,
    pub(super) backup: Option<(ResolvedSchedule, String)>,
    pub(super) user_dir: Arc<FjallUserDirectory>,
    pub(super) counters: Arc<FjallConsumedCounters>,
    pub(super) audit_appender: Arc<FjallAuditAppender>,
    pub(super) audit_chain: Arc<AuditChain>,
    pub(super) kdf_for_bootstrap: KdfParams,
    /// Format epoch observed in the data directory after a successful
    /// Writer check (fail-closed classification below). Used by the boot
    /// path for operator diagnostics.
    pub(super) observed_format_epoch: u64,
}

pub(super) async fn open(
    config: &Config,
    bootstrap: &BootstrapMode,
) -> Result<DurableStores, BootError> {
    // 1. Durable stores.
    std::fs::create_dir_all(&config.data_dir)?;
    let backup = resolve_backup(config)?;

    // Single-instance guard: acquire an advisory OS file lock on
    // `<data_dir>/.shamir.lock` (that ONE file — no parent locks) BEFORE
    // opening any durable store. Every store under this root is fjall
    // (repository stores, counters, sessions, users and `server_meta`), and
    // the redb-era wording here was stale. Ownership runs from this point to
    // the shutdown: the `File` lives wrapped in `ServerHandle.data_dir_lock`
    // (a `DataDirLock`) until `ShutdownOwner::drain` releases it cleanly (the
    // kernel also releases on crash; any other end of the owner's life leaks
    // the file so the lock survives until process exit).
    // This keeps two servers from opening the same data_dir and corrupting or
    // contending its stores.
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

    // Capture the observed format epoch (design 2.5). A successful Writer
    // check guarantees a valid stamp; the re-read below must agree or boot
    // fails closed with the corresponding typed error.
    let observed_format_epoch = match inspect(&config.data_dir) {
        StampState::Valid(stamp) => stamp.format_epoch(),
        StampState::Corrupt { kind } => {
            return Err(BootError::FormatStamp(FsStampError::Corrupt {
                dir: config.data_dir.clone(),
                kind,
            }));
        }
        StampState::NewerLayout { layout, epoch } => {
            return Err(BootError::FormatStamp(FsStampError::NewerLayout {
                dir: config.data_dir.clone(),
                layout,
                epoch,
            }));
        }
        StampState::Absent { .. } => {
            return Err(BootError::FormatStamp(FsStampError::AbsentOlderBase {
                dir: config.data_dir.clone(),
                current: CURRENT_FORMAT_EPOCH,
            }));
        }
    };

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
    // Keep recovery and both durable checkpoint writes within the data-dir
    // lock's scope, even if boot is cancelled. Never detach this work.
    let recover_audit = || {
        let legacy_checkpoint = meta
            .audit_checkpoint_checked()
            .map_err(BootError::AuditCheckpointRead)?;
        let key = Zeroizing::new(meta.audit_chain_key().map_err(BootError::AuditKeyRead)?);
        // Rotation-boundary anchor (finding 1): persist the meta anchor at
        // every rotation boundary AND the final batched shutdown, before
        // retention can prune, so a legitimately pruned history always
        // keeps a usable anchor. A MetaError becomes an io::Error that
        // keeps it as its source.
        let meta_anchor = Arc::clone(&meta);
        let on_rotation_or_close = move |next_seq: u64, prev_hmac: [u8; 32]| {
            meta_anchor
                .store_audit_checkpoint(next_seq, prev_hmac)
                .map_err(std::io::Error::other)
        };
        let (appender, (next_seq, prev_hmac)) =
            FjallAuditAppender::open_recovered_batched_with_meta(
                &config.data_dir,
                &key,
                legacy_checkpoint,
                Duration::from_secs(5),
                audit_max_bytes,
                config.audit.retention_days,
                on_rotation_or_close,
            )
            .map_err(BootError::AuditRecovery)?;

        // Recovery has made the verified tail durable. Refresh both anchors
        // (including genesis), before any writer can append to the chain.
        appender
            .checkpoint(next_seq, &prev_hmac)
            .map_err(BootError::AuditAppenderCheckpoint)?;
        meta.store_audit_checkpoint(next_seq, prev_hmac)
            .map_err(BootError::AuditMetaCheckpoint)?;
        let chain = Arc::new(AuditChain::from_checkpoint(*key, next_seq, prev_hmac));
        Ok::<_, BootError>((appender, chain))
    };
    let (audit_appender, audit_chain) = if Handle::try_current()
        .is_ok_and(|handle| handle.runtime_flavor() == RuntimeFlavor::MultiThread)
    {
        tokio::task::block_in_place(recover_audit)
    } else {
        recover_audit()
    }?;

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
    match bootstrap {
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

    Ok(DurableStores {
        data_dir_lock,
        format_stamp,
        meta,
        backup,
        user_dir,
        counters,
        audit_appender,
        audit_chain,
        kdf_for_bootstrap,
        observed_format_epoch,
    })
}
