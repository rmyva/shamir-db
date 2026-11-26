//! RV-WAL-3B29232E — WAL outcome indeterminate: seal-fsync and write-fault
//! integration regressions.
//!
//! TEST A drives the real disk-repo commit path through the EXPLICIT
//! transactional-batch client surface (`execute_batch_unchecked` + a tiny
//! `Resolver`) with `SHAMIR_WAL_SEGMENT_MAX_BYTES` tiny and an injected SEAL
//! fsync failure (`WalSink::arm_fail_next_seal_sync_for_test`). The failing
//! batch's frames WERE written (`append_batch` writes all frames before
//! rotating), so batch_execute surfaces `Ok(response)` whose
//! `transaction = Some(info)` is INDETERMINATE at the attempted version with
//! EMPTY results — not a definite abort, and never `Err(BatchError::..)`
//! (explicit batches convert commit errors via `into_transaction_info`
//! inline). After reopen, WAL replay decides the CRC-valid frame COMMITTED
//! at its version. Pre-fix (commit_policy.rs's Unknown|Indeterminate arm
//! mapped to `WalRequiresReopen`) this fails: the failing batch yields
//! aborted metadata with a `wal_requires_reopen` reason, so the
//! indeterminate / commit_version / empty-results assertions and the
//! aborted-reason distinction below all fail.
//!
//! TEST B is the inverse Mem case: an injected Mem write fault surfaces as
//! `TxError::Storage` (proven NotWritten), does NOT require reopen, the real
//! recovery API surfaces ZERO entries, and the row is ABSENT after
//! re-opening over the same backing store with `recover_v2_inflight` run
//! explicitly. Sensitivity to the card's production fix is asserted DIRECTLY
//! via `wal_completion`: reverting commit_policy.rs's Unknown|Indeterminate
//! arm back to `WalRequiresReopen` fails the classifier assertion at the end
//! of TEST B, while the NotWritten assertions above pin that a
//! provably-absent write stays a definite error.

use std::sync::Arc;

use serial_test::serial;
use shamir_query_builder::write::doc;
use shamir_query_builder::{batch::Batch, write};
use shamir_query_types::batch::{BatchResponse, VisibilityWait};
use shamir_storage::error::DbResult;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::IsolationLevel;
use shamir_types::access::Actor;
use shamir_types::types::value::InnerValue;
use shamir_wal::{WalAppendError, WalOpV2};

use crate::query::batch::{execute_batch_unchecked, BatchResolver, TableResolver};
use crate::query::TableRef;
use crate::repo::{BoxRepo, RepoInstance};
use crate::table::{TableConfig, TableManager};
use crate::tx::tests::drain::truncation::truncation_tests::reopen_disk_repo;
use crate::tx::CommitError;

const STATUS: VisibilityWait = VisibilityWait::Bounded { timeout_ms: 0 };

struct Resolver(RepoInstance);

#[async_trait::async_trait]
impl TableResolver for Resolver {
    async fn resolve(&self, table: &TableRef) -> DbResult<TableManager> {
        self.0.get_table(&table.table).await
    }
}

#[async_trait::async_trait]
impl BatchResolver for Resolver {
    async fn resolve_repo(&self, _: &str) -> DbResult<crate::tx::RepoCore> {
        Ok(self.0.core.clone())
    }
}

/// One transactional batch inserting a single ~520-char row through the
/// explicit client surface. `Err` here is itself a contract violation for a
/// commit-outcome error — callers panic.
async fn batch_insert(repo: &RepoInstance, value: &str) -> BatchResponse {
    let mut batch = Batch::new();
    batch.transactional().visibility_wait(STATUS);
    batch.insert(
        "w",
        write::insert("t").row(doc().set("name", value).unwrap()),
    );
    let request = batch.build();
    let resolver = Resolver(repo.clone());
    match execute_batch_unchecked(&request, &resolver, None, None, Actor::System, "test").await {
        Ok(response) => response,
        Err(e) => panic!(
            "explicit batch must surface commit outcome as Ok + transaction \
             metadata (batch_execute converts via into_transaction_info), got: {e:?}"
        ),
    }
}

/// TEST A: a seal fsync failing after the frames were written must report the
/// batch as INDETERMINATE (not a definite abort), fail-stop admission until
/// reopen, and let WAL replay settle the frame as committed.
#[tokio::test(flavor = "current_thread")]
#[serial]
async fn rv_wal_3b29232e_seal_fsync_failure_leaves_commit_indeterminate() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let repo_dir = tempdir.path().join("seal_fault_repo");

    // Tiny cap so ~4-8 large commits cross the rotation threshold. Set BEFORE
    // the repo's `repo_wal` OnceCell initialises; process-global state, hence
    // #[serial] + remove_var before the (blocking) reopen below.
    std::env::set_var("SHAMIR_WAL_SEGMENT_MAX_BYTES", "1024");
    let repo = reopen_disk_repo("sealfault", repo_dir.clone(), vec![TableConfig::new("t")]).await;
    let wal = repo.wal_group().await.expect("repository WAL");
    // Arm only this repository's WAL after initialization.
    wal.arm_fail_next_seal_sync_for_test();

    let big = format!("rv-wal-{}", "p".repeat(512));
    let mut failure = None;
    for i in 0u64..64 {
        let value = format!("{big}-{i:04}");
        // Serial test, no concurrent writers: this is the visible floor the
        // batch's tx registers as its snapshot (register_snapshot).
        let snap = repo.consistent_version().await.unwrap();
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            batch_insert(&repo, &value),
        )
        .await
        .expect("WAL indeterminate test batch must finish within 120 seconds");
        let info = response
            .transaction
            .expect("transactional batch always attaches transaction metadata");
        if info.status != "indeterminate" {
            assert!(
                info.is_committed(),
                "unexpected non-committed status: {info:?}"
            );
            assert!(!response.results.is_empty());
            continue;
        }
        // The failing batch: indeterminate at the attempted version, no
        // abort reason, empty results (no receipts for a non-committed tx).
        assert_eq!(
            info.commit_version,
            Some(i + 1),
            "attempted version reported"
        );
        // The snapshot is the pre-commit sampled watermark, not a constant.
        assert_eq!(
            info.snapshot_version,
            Some(snap),
            "batch snapshot must be the pre-commit sampled watermark"
        );
        assert_eq!(info.reason, None);
        assert_eq!(info.error.as_deref(), Some("wal_outcome_unknown"));
        assert!(info.validate().is_ok(), "malformed indeterminate info");
        assert!(!info.is_committed());
        assert!(
            response.results.is_empty(),
            "no receipts stamped for the failed batch"
        );
        failure = Some((i + 1, value));
        break;
    }
    let (v, fail_value) = failure.expect("seal never fired within 64 batches");

    // The failing batch's frames WERE written (all frames precede the
    // rotation), so the group fail-stops: reopen required, and the NEXT
    // batch is refused at admission — aborted, still, but with the reopen
    // reason, NOT indeterminate.
    assert!(repo.repo_wal().await.unwrap().requires_reopen());
    {
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            batch_insert(&repo, "after-fault"),
        )
        .await
        .expect("post-fault batch must finish within 120 seconds");
        let info = response.transaction.expect("tx metadata");
        assert_eq!(info.status, "aborted");
        assert_eq!(info.reason.as_deref(), Some("wal_requires_reopen"));
    }

    // Drop BEFORE clearing the env var, then reopen: fresh RepoInstance runs
    // recovery (the fault is NOT re-armed) and WAL replay decides the
    // CRC-valid frame COMMITTED at its version. This is the core proof: the
    // response said indeterminate, recovery says committed — no definite
    // abort receipt for replayable frames.
    std::env::remove_var("SHAMIR_WAL_SEGMENT_MAX_BYTES");
    drop(repo);
    let repo2 = reopen_disk_repo("sealfault", repo_dir, vec![TableConfig::new("t")]).await;
    let tbl2 = repo2.get_table("t").await.unwrap();

    // WAL-recovery evidence at the attempted version: the failing batch
    // inserted exactly ONE row, so its entry carries exactly ONE Put whose
    // body is the interned encoding of the document. Identity is proven by
    // rid + field value read back through the written shape — not by
    // whole-body bytes.
    let recovered = repo2.repo_wal().await.unwrap().recover().await.unwrap();
    let entry = recovered
        .iter()
        .find(|entry| entry.commit_version == v)
        .unwrap_or_else(|| panic!("no WAL entry at attempted version {v}"));
    let puts: Vec<_> = entry
        .ops
        .iter()
        .filter(|op| matches!(op, WalOpV2::Put { .. }))
        .collect();
    assert_eq!(
        puts.len(),
        1,
        "failing batch inserts exactly one row; expected one Put, got {puts:?}"
    );
    let fail_rid = match puts[0] {
        WalOpV2::Put { rid, .. } => *rid,
        _ => unreachable!(),
    };
    let got = tbl2.get(fail_rid).await.unwrap();
    match &got {
        InnerValue::Map(m) => {
            assert_eq!(m.len(), 1, "document had one field, got {m:?}");
            assert!(
                m.values()
                    .any(|val| *val == InnerValue::Str(fail_value.clone())),
                "replayed frame must carry the field value {fail_value:?}, got {m:?}"
            );
        }
        other => panic!("document must resurrect as a map, got {other:?}"),
    }

    // Recovery seeded the version floor past the attempted version.
    assert!(
        repo2.tx_gate().await.unwrap().last_committed() >= v,
        "recovery must advance the commit-version floor past {v}"
    );
}

/// TEST B (Mem inverse): a proven NotWritten (never-queued) write settles as
/// `TxError::Storage`, does not require reopen, is invisible to the real
/// recovery API, and never resurrects after recovery re-runs over the same
/// backing store.
#[tokio::test]
async fn rv_wal_3b29232e_write_failure_is_not_written_and_absent_after_reopen() {
    let underlying = Arc::new(InMemoryRepo::new());
    let repo = RepoInstance::new(
        "wal_not_written".into(),
        BoxRepo::InMemory(Arc::clone(&underlying)),
        Vec::new(),
    );
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();

    repo.repo_wal()
        .await
        .unwrap()
        .arm_fail_next_append_for_test();
    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&InnerValue::Str("doomed".into()), Some(&mut tx))
        .await
        .unwrap();
    // Proven NotWritten: a definite Storage error, not an unknown outcome.
    match repo.commit_tx(tx).await {
        Err(CommitError::Storage(_)) => {}
        other => panic!("expected Storage (NotWritten), got {other:?}"),
    }
    // A provably absent write is a definite outcome — no reopen needed.
    assert!(!repo.repo_wal().await.unwrap().requires_reopen());
    // The failed frame never reached the sink: real recovery surfaces ZERO
    // entries on the ORIGINAL repo.
    assert_eq!(
        repo.repo_wal()
            .await
            .unwrap()
            .recover()
            .await
            .unwrap()
            .len(),
        0,
        "proven-NotWritten frame must not appear in WAL recovery"
    );

    // Simulated crash: rebuild over the SAME backing Arc; `RepoInstance::new`
    // deliberately does NOT run recovery — invoke the real API explicitly.
    drop(repo);
    let repo2 = RepoInstance::new(
        "wal_not_written".into(),
        BoxRepo::InMemory(underlying),
        Vec::new(),
    );
    repo2.add_table_for_test(TableConfig::new("t"));
    repo2.recover_v2_inflight().await.unwrap();
    assert_eq!(
        repo2.tx_gate().await.unwrap().last_committed(),
        0,
        "recovery must not seed a version floor for a NotWritten commit"
    );
    let tbl2 = repo2.get_table("t").await.unwrap();
    let got = tbl2.get_many(&[rid]).await.unwrap();
    assert!(
        got[0].is_none(),
        "proven-NotWritten commit must never resurrect, found {:?}",
        got[0]
    );

    // Card-fix sensitivity: the SAME production classifier must keep an
    // Unknown ticket (frame queued, fate unprovable) INDETERMINATE with its
    // attempted version. Reverting commit_policy.rs's Unknown|Indeterminate
    // arm back to `WalRequiresReopen` (→ aborted metadata) makes exactly this
    // assertion fail, while the NotWritten assertions above pin the
    // definite-outcome contrast.
    let gate = repo2.tx_gate().await.unwrap();
    let (res, proven) = crate::tx::commit::wal_completion(&gate, 1, Err(WalAppendError::Unknown));
    assert!(
        matches!(
            res,
            Err(crate::tx::commit::TxError::WalOutcomeUnknown { commit_version: 1 })
        ),
        "reverted Unknown arm must fail here: {res:?}"
    );
    assert!(!proven);
    assert!(gate.requires_reopen());
}
