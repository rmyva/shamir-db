use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::meta::recovery_marker::save_last_committed_flushed;
use crate::meta::repl::bookmark_v3::{save_bookmark_v3, BookmarkV3};
use crate::repo::{BoxRepoFactory, RepoFactory, RepoInstance};
use crate::table::TableConfig;
use shamir_storage::error::{DbError, DbResult};
use shamir_storage::types::{Repo as _, Store};
use shamir_tx::changefeed::cold::{encode_event, read_event};
use shamir_tx::changefeed::{clean_horizon_key, ChangelogEvent};
use shamir_wal::{WalDurability, WalEntryV2};

pub(super) const REPO_NAME: &str = "cold_reconcile_test";

pub(super) async fn open_with_retry(path: &Path, name: &str) -> RepoInstance {
    for attempt in 0..25u32 {
        match RepoInstance::from_factory(
            name.to_owned(),
            BoxRepoFactory::fjall_raw(path),
            vec![TableConfig::new("items")],
        )
        .await
        {
            Ok(repo) => return repo,
            Err(error) if attempt < 24 => {
                let _ = error;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(error) => panic!("reopen failed after retries: {error}"),
        }
    }
    unreachable!("retry loop always returns or panics")
}

/// Open a legacy follower store and return the typed refusal. The refusal
/// itself must be immediate (fail fast — no replay); only fjall's OS-level
/// lock lag after a close is retried.
pub(super) async fn open_refusal_with_retry(path: &Path, name: &str) -> DbError {
    let mut last: Option<shamir_storage::error::DbError> = None;
    for attempt in 0..25u32 {
        match RepoInstance::from_factory(
            name.to_owned(),
            BoxRepoFactory::fjall_raw(path),
            vec![TableConfig::new("items")],
        )
        .await
        {
            Ok(_) => panic!("legacy follower store must refuse the open"),
            Err(error)
                if matches!(
                    &error,
                    shamir_storage::error::DbError::FollowerStateRefused {
                        reason: shamir_storage::error::FollowerRefusalReason::LegacyBookmarkFormat
                    }
                ) =>
            {
                return error
            }
            Err(error) if attempt < 24 => {
                last = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(error) => {
                panic!("expected the legacy refusal after retries: last={last:?}, got {error}")
            }
        }
    }
    panic!("legacy follower store never refused: last={:?}", last)
}

pub(super) async fn open_unrecovered(path: &Path) -> RepoInstance {
    let repo = RepoInstance::from_factory_for_recovery_test(
        REPO_NAME.into(),
        BoxRepoFactory::fjall_raw(path.to_path_buf()),
        vec![TableConfig::new("items")],
    )
    .await
    .expect("open raw Fjall repo");
    repo.disable_background_drainer_for_test();
    repo
}

pub(super) async fn prepare_image(
    path: &Path,
    entries: &[WalEntryV2],
    journal_events: &[ChangelogEvent],
    bookmark: Option<u64>,
    clean_horizon: u64,
) {
    persist_image(path, entries, journal_events, bookmark, None, clean_horizon).await;
}

/// Follower fixture image: no surviving WAL entries, an upstream bookmark,
/// and the PR6 record that may carry a known local journal horizon.
pub(super) async fn prepare_follower_image(
    path: &Path,
    journal_events: &[ChangelogEvent],
    bookmark: u64,
    record_clean_through: Option<u64>,
    clean_horizon: u64,
) {
    persist_image(
        path,
        &[],
        journal_events,
        Some(bookmark),
        record_clean_through,
        clean_horizon,
    )
    .await;
}

async fn persist_image(
    path: &Path,
    entries: &[WalEntryV2],
    journal_events: &[ChangelogEvent],
    bookmark: Option<u64>,
    record_clean_through: Option<u64>,
    clean_horizon: u64,
) {
    let repo = open_unrecovered(path).await;
    let info = repo.tx_info_store().await.expect("tx info store");
    let top = entries
        .iter()
        .map(|entry| entry.commit_version)
        .chain(journal_events.iter().map(|event| event.commit_version))
        .chain(bookmark)
        .max()
        .unwrap_or(clean_horizon)
        .max(clean_horizon);
    save_last_committed_flushed(&info, top)
        .await
        .expect("persist allocation floor");
    if let Some(bookmark) = bookmark {
        // v3-only fixture: tests own the record shape; no legacy writer exists.
        save_bookmark_v3(
            &info,
            &BookmarkV3 {
                fence: None,
                bookmark,
                clean_through: record_clean_through,
                outstanding_receipt: None,
            },
        )
        .await
        .expect("persist follower v3 record");
    }
    let wal = repo.repo_wal().await.expect("repo WAL");
    for entry in entries {
        wal.begin_grouped(entry, WalDurability::Buffered)
            .await
            .expect("append WAL entry");
    }
    wal.sync_wal().await.expect("sync WAL entries");
    let store = repo
        .changelog_store_for_cold_reconcile()
        .await
        .expect("changelog store");
    for event in journal_events {
        store
            .set(
                shamir_tx::changefeed::version_key(event.commit_version).into(),
                encode_event(event).expect("encode journal event"),
            )
            .await
            .expect("write journal event");
    }
    store.flush().await.expect("flush fixture journal");
    let adapter = crate::repo::changelog_store::StoreChangelog::new(Arc::clone(&store));
    if clean_horizon > 0 {
        shamir_tx::ChangelogStore::put(
            &adapter,
            clean_horizon_key(),
            shamir_tx::changefeed::encode_clean_horizon(clean_horizon),
        )
        .await
        .expect("persist clean horizon fixture");
    }
    drop(adapter);
    drop(store);
    drop(wal);
    drop(info);
    drop(repo);
}

pub(super) async fn open_and_reconcile(path: &Path) -> DbResult<RepoInstance> {
    let repo = open_unrecovered(path).await;
    let wal = repo.repo_wal().await?;
    let entries = match wal.recover().await {
        Ok(entries) => entries,
        Err(error) => {
            repo.stop_journal_writer_for_test().await?;
            return Err(error);
        }
    };
    // Admit through the write domain FIRST (uncontended at open) and drop
    // the permit after — cold recovery threads the permit, it never
    // re-admits itself.
    let permit = repo.write_domain().admit().await.map_err(|error| {
        shamir_storage::error::DbError::Internal(format!("test admission rejected: {error}"))
    })?;
    let result = repo.certify_cold_start(&entries, &permit).await;
    drop(permit);
    if let Err(error) = result {
        repo.stop_journal_writer_for_test().await?;
        return Err(error);
    }
    Ok(repo)
}

pub(super) async fn raw_store(path: &Path, name: &str) -> Arc<dyn Store> {
    let raw = BoxRepoFactory::fjall_raw(path.to_path_buf())
        .create()
        .await
        .expect("open raw factory");
    let store = raw.store_get(name).await.expect("open raw store");
    drop(raw);
    store
}

/// A fresh exclusive-lock open right after a close can hit brief fjall lock
/// lag.
pub(super) async fn raw_store_with_retry(path: &Path, name: &str) -> Arc<dyn Store> {
    let mut last_error = None;
    for _ in 0..25 {
        match BoxRepoFactory::fjall_raw(path.to_path_buf()).create().await {
            Ok(raw) => {
                let store = raw.store_get(name).await.expect("open raw store");
                drop(raw);
                return store;
            }
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!(
        "raw store open with retry exhausted: {:?}",
        last_error.expect("at least one attempt")
    );
}

pub(super) async fn journal_event(store: &dyn Store, version: u64) -> Option<ChangelogEvent> {
    read_event(store, version)
        .await
        .expect("read journal event")
}

pub(super) fn temp_path(temp: &tempfile::TempDir) -> PathBuf {
    temp.path().join("cold_repo")
}
