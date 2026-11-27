//! Shared fixtures for the durability_policy tests.

#[cfg(feature = "fjall")]
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "fjall")]
use shamir_storage::error::DbResult;
use shamir_storage::storage_in_memory::InMemoryRepo;
#[cfg(feature = "fjall")]
use shamir_storage::types::Repo;
#[cfg(feature = "fjall")]
use shamir_tx::{ChangeOp, ChangelogEvent, CompletionState, RecordChange};
#[cfg(feature = "fjall")]
use shamir_types::access::Actor;
#[cfg(feature = "fjall")]
use shamir_types::types::record_id::RecordId;
#[cfg(feature = "fjall")]
use tempfile::TempDir;

use crate::repo::BoxRepo;
#[cfg(feature = "fjall")]
use crate::repo::BoxRepoFactory;
use crate::repo::RepoInstance;
use crate::table::TableConfig;

/// In-memory repo with one `items` table (Mem WAL sink: `R == W`).
pub(crate) fn mem_repo(name: &str) -> RepoInstance {
    RepoInstance::new(
        name.into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("items")],
    )
}

// Disk-backed (fjall_raw) fixture — requires the `fjall` feature.
#[cfg(feature = "fjall")]
pub(crate) struct DiskFixture {
    pub(crate) repo: RepoInstance,
    /// Sibling-of-backing layout keeps the WAL directory
    /// (`data.shamirwal`) INSIDE the TempDir, so cleanup removes it.
    pub(crate) _dir: TempDir,
    pub(crate) backing: PathBuf,
}

/// Disk (fjall_raw) repo over `<tmp>/data`, one `items` table — its
/// `repo_wal()` builds a `File`-sink `WalGroupCommit` at `<tmp>/data.shamirwal`.
#[cfg(feature = "fjall")]
pub(crate) async fn disk_repo(name: &str) -> DiskFixture {
    let dir = TempDir::new().unwrap();
    let backing = dir.path().join("data");
    std::fs::create_dir_all(&backing).unwrap();
    let repo = RepoInstance::from_factory(
        name.into(),
        BoxRepoFactory::fjall_raw(backing.clone()),
        vec![TableConfig::new("items")],
    )
    .await
    .unwrap();
    DiskFixture {
        repo,
        _dir: dir,
        backing,
    }
}

#[cfg(feature = "fjall")]
impl DiskFixture {
    /// Active (first) WAL segment file of this fixture's repo.
    pub(crate) fn wal_file(&self) -> PathBuf {
        self.backing
            .with_file_name("data.shamirwal")
            .join("00000000.wal")
    }
}

#[cfg(feature = "fjall")]
fn journal_event(repo: &str, commit_version: u64) -> ChangelogEvent {
    ChangelogEvent {
        repo: repo.into(),
        commit_version,
        tx_id: commit_version,
        actor: Actor::User(42),
        timestamp_ns: 0,
        changes: vec![RecordChange {
            table: "items".into(),
            key: RecordId([1; 16]).to_bytes(),
            op: ChangeOp::Delete,
            value: None,
        }],
    }
}

/// Seed journal rows for `versions` and settle their completion watermark,
/// so the completion/journal bound computed by
/// `read_changelog_bounded_impl` covers exactly `versions`.
#[cfg(feature = "fjall")]
pub(crate) async fn seed_journal(repo: &RepoInstance, versions: &[u64]) {
    let store = repo.core.repo().store_get("__changelog__").await.unwrap();
    for &v in versions {
        let event = journal_event(repo.name(), v);
        store
            .set(
                shamir_tx::changefeed::version_key(v).into(),
                rmp_serde::to_vec(&event).unwrap().into(),
            )
            .await
            .unwrap();
    }
    let gate = repo.tx_gate().await.unwrap();
    for &v in versions {
        gate.completion().mark(v, CompletionState::Materialized);
    }
}

/// Append one (empty-ops) WAL entry at `version` through the repo's group.
#[cfg(feature = "fjall")]
pub(crate) async fn append_wal(
    repo: &RepoInstance,
    version: u64,
    tier: shamir_wal::WalDurability,
) -> DbResult<()> {
    let entry = shamir_wal::WalEntryV2::new(version, 0, Vec::new()).with_commit_version(version);
    repo.wal_group()
        .await?
        .append(entry.encode()?, version, tier)
        .await
}

/// Highest commit_version recovered from the repo's WAL.
#[cfg(feature = "fjall")]
pub(crate) async fn recovered_horizon(repo: &RepoInstance) -> u64 {
    repo.repo_wal()
        .await
        .unwrap()
        .recover()
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.commit_version)
        .max()
        .unwrap_or(0)
}
