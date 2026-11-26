//! Group 15 (2026-08-14 cross-crate review, SUMMARY.md 6.2 /
//! error-handling-lifecycle.md #2) regression: `commit_tx_inner`'s
//! `tx_gate()` / `repo_wal()` resolution must release Level-3 pessimistic
//! locks before propagating an `Err`, exactly like every other early-exit
//! path in that function. Pre-fix, `let wal = repo.repo_wal().await?;`
//! propagated the `?` directly — no `release_pessimistic_locks` call — so
//! a Pessimistic tx whose commit hit a `repo_wal()` I/O failure (its first
//! WAL touch for the repo) leaked `locked_keys` permanently, and a younger
//! waiter parked in `lock_key` (no timeout) would hang forever.
//!
//! Fault injection needs NO test-only seam: `RepoInstance::repo_wal()`
//! lazily does a REAL `std::fs::create_dir_all` on the WAL sibling
//! directory (`<repo_dir>.shamirwal`) on first touch. Placing a plain FILE
//! at that exact path before the repo ever resolves its WAL makes
//! `create_dir_all` fail with a genuine "already exists / not a directory"
//! disk error — the exact class of fault the audit describes — with zero
//! production-code changes required to trigger it.

use std::sync::Arc;
use std::time::Duration;

use shamir_tx::IsolationLevel;
use shamir_types::types::value::InnerValue;

use crate::repo::{BoxRepoFactory, RepoInstance};
use crate::table::TableConfig;
use crate::tx::CommitError;

/// Opens a disk-backed repo at `path` whose WAL sibling directory
/// (`<path>.shamirwal` — see `RepoInstance::repo_wal`'s doc for the exact
/// naming rule) is already occupied by a plain file, so the FIRST
/// `repo_wal()` call's `create_dir_all` fails with a real I/O error. The
/// fjall repo itself opens normally — only the WAL directory is sabotaged.
async fn open_repo_with_sabotaged_wal_dir(path: &std::path::Path) -> RepoInstance {
    let mut wal_dir_name = path
        .file_name()
        .expect("path must have a file name")
        .to_os_string();
    wal_dir_name.push(".shamirwal");
    let wal_dir = path.with_file_name(wal_dir_name);
    std::fs::write(&wal_dir, b"not a directory").expect("seed wal-dir sabotage file");

    RepoInstance::from_factory_for_recovery_test(
        "r".into(),
        BoxRepoFactory::fjall_raw(path.to_path_buf()),
        vec![TableConfig::new("t")],
    )
    .await
    .expect("from_factory must succeed: only the WAL sibling dir is sabotaged, not the repo dir")
}

/// A Pessimistic tx's commit that fails at `repo.repo_wal().await?` (a
/// real, first-touch WAL directory-creation error) must still release its
/// Level-3 locks. Before the fix, `commit_tx_inner` propagated the `?`
/// with no `release_pessimistic_locks` call, so the locked key stayed held
/// forever. Proven with a bounded `tokio::time::timeout`: a real
/// regression FAILS FAST here instead of hanging the whole suite.
#[tokio::test]
async fn pessimistic_lock_released_when_repo_wal_init_fails_on_commit() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().join("repo");
    let repo = Arc::new(open_repo_with_sabotaged_wal_dir(&path).await);

    let tbl = repo.get_table("t").await.unwrap();
    let rid = tbl.insert(&InnerValue::Str("seed".into())).await.unwrap();

    // tx1: acquire the Exclusive lock on `rid` via update_tx, then attempt
    // to commit. The commit must fail at `repo_wal()` init (the sabotaged
    // directory), NOT at anything earlier — tx_gate()'s own internal
    // repo_wal() touch (CRIT-B's inflight pre-scan) swallows the same
    // error via `.unwrap_or(0)`, so tx_gate() itself always succeeds and
    // the repo_wal `OnceCell` stays uninitialised (retried) until here.
    let (mut tx1, _g1) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();
    tbl.update_tx(rid, &InnerValue::Str("v1".into()), Some(&mut tx1))
        .await
        .unwrap();

    let result = repo.commit_tx(tx1).await;
    assert!(
        matches!(result, Err(CommitError::Storage(_))),
        "commit must fail with a Storage error from the sabotaged repo_wal() init, got {:?}",
        result.map(|o| o.commit_version)
    );

    // Decisive assertion: a second Level-3 tx must be able to acquire the
    // SAME key without blocking. If the failed commit leaked tx1's lock,
    // this read blocks forever — bounded so a real regression fails fast
    // and clearly instead of hanging the test run.
    let (tx2, _g2) = repo.begin_tx(IsolationLevel::Pessimistic).await.unwrap();
    let read = tokio::time::timeout(Duration::from_secs(3), tbl.read_one_tx(rid, Some(&tx2)))
        .await
        .expect(
            "DEADLOCK: second Level-3 tx hung on tx1's key after tx1's commit failed at \
             repo_wal() init — lock leaked (group 15 regression)",
        )
        .unwrap();

    // Non-vacuous: tx1 never published (its commit failed pre-WAL), so the
    // second tx must still observe the original seeded value.
    assert!(
        matches!(read, InnerValue::Str(ref s) if s == "seed"),
        "second tx must read the pre-tx1 value (tx1 never published), got {:?}",
        read
    );
}
