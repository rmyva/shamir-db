use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::InnerValue;
use shamir_wal::{WalDurability, WalEntryV2, WalOpV2, WalTicket, WalTicketState};

use crate::repo::{repo_token, BoxRepo, RepoInstance};
use crate::table::table_manager::table_token_for;

pub(crate) fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

pub(crate) fn rid(n: u8) -> RecordId {
    let mut a = [0u8; 16];
    a[15] = n;
    RecordId(a)
}

/// Seed ONE inflight `Put` entry (NO history/overlay write) at
/// `commit_version`, exactly the shape the ack-path leaves behind once the
/// history write is deferred. Returns the body bytes for later comparison.
pub(crate) async fn seed_inflight_put(
    repo: &RepoInstance,
    table: &str,
    record: RecordId,
    value: &str,
    commit_version: u64,
) -> bytes::Bytes {
    let wal = repo.repo_wal().await.unwrap();
    let body = InnerValue::Str(value.into()).to_bytes().unwrap();
    let entry = WalEntryV2::new(
        wal.fresh_txn_id(),
        repo_token(repo.name()),
        vec![WalOpV2::Put {
            table_id_interned: table_token_for(table),
            rid: record,
            body: body.clone(),
        }],
    )
    .with_commit_version(commit_version);
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();
    body
}

/// Seed ONE inflight entry carrying an arbitrary op list at `commit_version`
/// — generalizes `seed_inflight_put` for index-posting (IndexPut/IndexDel)
/// regression tests, which need to control `WalOpV2` shapes directly.
pub(crate) async fn seed_inflight_entry(
    repo: &RepoInstance,
    ops: Vec<WalOpV2>,
    commit_version: u64,
) {
    let wal = repo.repo_wal().await.unwrap();
    let entry = WalEntryV2::new(wal.fresh_txn_id(), repo_token(repo.name()), ops)
        .with_commit_version(commit_version);
    wal.begin_grouped(&entry, WalDurability::Buffered)
        .await
        .unwrap();
}

pub(crate) async fn transfer_pending(repo: &RepoInstance, ops: Vec<WalOpV2>) -> Arc<WalTicket> {
    let gate = repo.tx_gate().await.unwrap();
    let mut guard = gate.assign_next_version_guarded();
    let entry = Arc::new(
        WalEntryV2::new(guard.version(), repo_token(repo.name()), ops)
            .with_commit_version(guard.version()),
    );
    let ticket = Arc::new(WalTicket::detached_for_test(WalTicketState::Pending));
    guard.wal_queued(ticket.clone(), entry);
    drop(guard);
    ticket
}
