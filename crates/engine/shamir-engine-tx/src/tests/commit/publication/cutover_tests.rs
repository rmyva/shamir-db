//! D2 P1d-2b — cutover contract tests.
//!
//! After the cutover the commit ack-path writes ONLY the in-memory overlay
//! (visible half). The value is durable in `history` only after the
//! background drainer (generalized recovery) replays the inflight WAL entry.
//! These tests pin the new contract:
//!
//!   1. `durable_watermark()` genuinely LAGS `last_committed()` right after a
//!      commit, yet the value is already readable (served by the overlay) —
//!      `drain_all` converges the two and lands the value in `history`.
//!   2. After `drain_all` the value is durable in `history` directly and the
//!      durable watermark has caught up to visibility.
//!   3. Crash-safety by WAL: commit, drop the repo WITHOUT draining, reopen,
//!      run recovery — the value is reconstructed in `history` from the WAL.
//!
//! The first two use the `current_thread` runtime so the spawned drainer
//! cannot sneak a pass in between a commit and a synchronous watermark read
//! (it only progresses at `.await` points), making the lag deterministic.

#[cfg(feature = "fjall")]
use std::path::PathBuf;
use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::version_codec::encode_version_key;
use shamir_tx::{CommitVisibility, IsolationLevel};
use shamir_types::types::value::InnerValue;

#[cfg(feature = "fjall")]
use crate::repo::BoxRepoFactory;
use crate::repo::{BoxRepo, RepoInstance};
use crate::table::table_manager::table_token_for;
use crate::table::TableConfig;

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("cutover".into(), BoxRepo::InMemory(repo), Vec::new())
}

/// Reopen a disk-backed repo, tolerating Windows' lazy file-lock release.
/// Disk-backed (fjall_raw) — requires the `fjall` feature.
#[cfg(feature = "fjall")]
async fn reopen_disk_repo(name: &str, path: PathBuf, tables: Vec<TableConfig>) -> RepoInstance {
    let mut last_err = None;
    for _ in 0..10 {
        match RepoInstance::from_factory(
            name.into(),
            BoxRepoFactory::fjall_raw(path.clone()),
            tables.clone(),
        )
        .await
        {
            Ok(r) => return r,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
    panic!("reopen_disk_repo({name:?}) failed after 10 retries: {last_err:?}");
}

/// (3-overlay) Right after commit the durable watermark LAGS visibility, but
/// the record is already readable (served by the overlay, NOT history). After
/// `drain_all` the two watermarks converge.
#[tokio::test(flavor = "current_thread")]
async fn durable_lags_visibility_but_overlay_serves_read() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let token = table_token_for("t");

    // Commit a record through the real tx pipeline.
    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&InnerValue::Str("v1".into()), Some(&mut tx))
        .await
        .unwrap();
    let outcome = repo.commit_tx(tx).await.unwrap();
    assert!(outcome.materialized());

    // NO `.await` between commit and these reads → the spawned drainer cannot
    // have run (current_thread). Durable lags visibility.
    let dur = gate.durable_watermark();
    let vis = gate.last_committed();
    assert!(dur <= vis, "durable={dur} must not lead vis={vis}");
    assert!(
        dur < vis,
        "cutover: durable={dur} must LAG visibility={vis} before drain"
    );

    // The value is NOT yet in history (only the overlay holds it) ...
    let mvcc = repo
        .per_table_mvcc()
        .read_async(&token, |_, m| Arc::clone(m))
        .await
        .expect("mvcc store registered for table t");
    let hist_key = encode_version_key(&rid.to_bytes(), outcome.commit_version);
    assert!(
        mvcc.history_store()
            .get(hist_key.clone().into())
            .await
            .is_err(),
        "value must NOT be in history before drain (overlay holds the only copy)"
    );

    // ... yet a normal read sees it (overlay-served, read-your-writes).
    let read_back = tbl.get(rid).await.unwrap();
    assert!(
        matches!(read_back, InnerValue::Str(ref s) if s == "v1"),
        "overlay must serve the committed value before drain, got {read_back:?}"
    );

    // Drain → durable catches up AND the value lands in history.
    repo.drainer().drain_all(&repo.core).await.unwrap();
    let dur = gate.durable_watermark();
    let vis = gate.last_committed();
    assert!(dur <= vis);
    assert_eq!(dur, vis, "after drain → durable == visibility");

    let in_history = mvcc
        .history_store()
        .get(hist_key.into())
        .await
        .expect("value must be durable in history after drain");
    let expected = InnerValue::Str("v1".into()).to_bytes().unwrap();
    assert_eq!(in_history, expected, "history holds the committed value");

    // Read still resolves the value post-drain.
    let read_back = tbl.get(rid).await.unwrap();
    assert!(matches!(read_back, InnerValue::Str(ref s) if s == "v1"));
}

/// (2-history) Commit → `drain_all` → the value is durable in `history`
/// directly and `durable_watermark()` has caught up to `last_committed()`.
#[tokio::test(flavor = "current_thread")]
async fn commit_then_drain_all_makes_value_durable_in_history() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let token = table_token_for("t");

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&InnerValue::Str("durable".into()), Some(&mut tx))
        .await
        .unwrap();
    let outcome = repo.commit_tx(tx).await.unwrap();

    let drained = repo.drainer().drain_all(&repo.core).await.unwrap();
    assert!(drained >= 1, "drain_all must drain the committed version");

    assert_eq!(
        gate.durable_watermark(),
        gate.last_committed(),
        "durable caught up to visibility after drain_all"
    );
    assert_eq!(gate.durable_watermark(), outcome.commit_version);

    let mvcc = repo
        .per_table_mvcc()
        .read_async(&token, |_, m| Arc::clone(m))
        .await
        .unwrap();
    let in_history = mvcc
        .history_store()
        .get(encode_version_key(&rid.to_bytes(), outcome.commit_version).into())
        .await
        .expect("value durable in history after drain_all");
    let expected = InnerValue::Str("durable".into()).to_bytes().unwrap();
    assert_eq!(in_history, expected);
}

/// (1-reopen) Crash-safety by WAL: commit on a disk repo, drop WITHOUT
/// draining (simulated crash), reopen over the same dir, recover — the value
/// is reconstructed in `history` from the WAL and reads back. Proves the
/// cutover keeps the WAL the source of truth: nothing is lost even though the
/// ack-path never wrote `history` and the tail was never drained.
/// Disk-backed (fjall_raw) crash-safety test — requires the `fjall` feature.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn reopen_recovery_without_drain_reconstructs_history() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let path = tempdir.path().to_path_buf();

    let rid;
    {
        let factory = BoxRepoFactory::fjall_raw(path.clone());
        let repo = RepoInstance::from_factory(
            "cutover_reopen".into(),
            factory,
            vec![TableConfig::new("t")],
        )
        .await
        .expect("from_factory");
        let tbl = repo.get_table("t").await.unwrap();

        let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
        rid = tbl
            .insert_tx(&InnerValue::Str("crash-safe".into()), Some(&mut tx))
            .await
            .unwrap();
        repo.commit_tx(tx).await.unwrap();

        // SIMULATED CRASH: drop WITHOUT `flush_buffers` / `drain_all`. The
        // value lives only in the overlay (RAM, lost on drop) + the durable
        // file WAL. The cutover never wrote it to `history` inline.
        drop(repo);
    }

    // Reopen + recover: recovery replays the inflight WAL entry into history.
    let repo2 = reopen_disk_repo("cutover_reopen", path.clone(), vec![TableConfig::new("t")]).await;
    let tbl2 = repo2.get_table("t").await.unwrap();

    let recovered = repo2.recover_v2_inflight().await.unwrap();
    assert!(
        recovered >= 1,
        "recovery must replay the inflight WAL entry, got {recovered}"
    );

    // The record reads back — reconstructed purely from the WAL → history.
    let read_back = tbl2
        .get(rid)
        .await
        .unwrap_or_else(|e| panic!("record {rid:?} must recover from the WAL: {e}"));
    assert!(
        matches!(read_back, InnerValue::Str(ref s) if s == "crash-safe"),
        "expected recovered Str(\"crash-safe\"), got {read_back:?}"
    );

    // Recovery also caught durable up to visibility (history is durable now).
    let gate2 = repo2.tx_gate().await.unwrap();
    assert_eq!(
        gate2.durable_watermark(),
        gate2.last_committed(),
        "recovery marks the replayed version durable"
    );

    // And it landed in history directly.
    let token = table_token_for("t");
    let mvcc = repo2
        .per_table_mvcc()
        .read_async(&token, |_, m| Arc::clone(m))
        .await
        .unwrap();
    let in_history = mvcc
        .history_store()
        .get(encode_version_key(&rid.to_bytes(), gate2.last_committed()).into())
        .await
        .expect("recovered value durable in history");
    let expected = InnerValue::Str("crash-safe".into()).to_bytes().unwrap();
    assert_eq!(in_history, expected);
}

/// (4-async) AsyncIndex drain contract — the AsyncIndex commit path
/// materializes on a background tail, and post-cutover the ack-path writes ONLY
/// the overlay, so the value reaches `history` solely via the drainer/recovery.
/// This pins the end-to-end contract: after the tail completes, `drain_all`
/// must land the value durably in `history` and converge `durable_watermark` to
/// `last_committed`.
///
/// FORWARD GUARD: the cutover removed the inline Phase-7 `wal.commit` from the
/// AsyncIndex tail (`materialize_async_tail`) so the drainer is the SOLE
/// truncator (it truncates only AFTER writing history). That removal is
/// currently a no-op behaviourally — `wal.commit` does not truncate until F6
/// (P1e) — so this test passes whether or not the tail truncates. Once F6 makes
/// truncation real, a regressed tail that truncated inline would remove the WAL
/// entry before the drainer wrote `history`, and this assertion (`durable ==
/// vis` + value in `history` after `drain_all`) would FAIL — the entry would be
/// gone with the value only ever in the volatile overlay. The crash-injection
/// (e) guard that exercises the same path under real truncation lands in P1e.
#[tokio::test(flavor = "current_thread")]
async fn async_index_tail_must_not_truncate_drainer_converges() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("t"));
    let tbl = repo.get_table("t").await.unwrap();
    let gate = repo.tx_gate().await.unwrap();
    let token = table_token_for("t");

    let (mut tx, _g) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    let rid = tbl
        .insert_tx(&InnerValue::Str("async-v".into()), Some(&mut tx))
        .await
        .unwrap();
    tx.set_visibility(CommitVisibility::AsyncIndex);
    let mut outcome = repo.commit_tx(tx).await.unwrap();
    let commit_version = outcome.commit_version;
    assert!(outcome.background.is_some(), "AsyncIndex hands back a tail");

    // Drive the background tail to completion — on the BUGGY code this is where
    // the inline Phase-7 `wal.commit` would remove the marker.
    let bg = outcome.take_background().expect("async tail");
    let _ = bg.join().await;

    // The value must still be recoverable: either the drainer already wrote it
    // to history, OR the inflight WAL entry survived for the drainer to drain.
    // `drain_all` forces convergence. On the buggy code the tail truncated the
    // entry before the drainer wrote history → there is nothing to drain →
    // `durable` cannot reach `vis` and `history` stays empty.
    repo.drainer().drain_all(&repo.core).await.unwrap();

    assert_eq!(
        gate.durable_watermark(),
        gate.last_committed(),
        "AsyncIndex version must become durable via the drainer (tail must not \
         have truncated the WAL out from under it)"
    );

    let mvcc = repo
        .per_table_mvcc()
        .read_async(&token, |_, m| Arc::clone(m))
        .await
        .unwrap();
    let in_history = mvcc
        .history_store()
        .get(encode_version_key(&rid.to_bytes(), commit_version).into())
        .await
        .expect("AsyncIndex value must be durable in history after drain");
    let expected = InnerValue::Str("async-v".into()).to_bytes().unwrap();
    assert_eq!(in_history, expected);

    // And the record reads back end-to-end.
    let read_back = tbl.get(rid).await.unwrap();
    assert!(matches!(read_back, InnerValue::Str(ref s) if s == "async-v"));
}
