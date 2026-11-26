//! F-48b (#867, P0) — tx-commit-path writer-drain barrier proof.
//!
//! Sibling of `crate::table::tests::schema_activation_barrier_tests.rs`'s
//! F-48 drain test (`f48_drain_catches_writer_that_read_false_before_flag_went_up`).
//! That test proved the drain closes the check-then-act race for the NON-tx
//! `TableManager::insert` path. This test proves the SAME drain closes the
//! identical race for the TX-COMMIT path (`pre_commit.rs::pre_commit_prelock`
//! Phase 2.5 → `materialize` Phase 5c) — the path essentially ALL production
//! client writes actually take.
//!
//! ## The gap (pre-F-48b)
//!
//! `pre_commit_prelock`'s Phase 2.5 reads `needs_write_barrier()` ONCE per
//! table in `tx.write_set`; if `false` at that moment, the table's token never
//! enters `unique_tokens`, so `unique_write_lock` is never taken for it, and
//! the transaction's ACTUAL data write (Phase 5c `materialize`) proceeds with
//! no further check. A schema-activation DDL that raises the barrier AFTER
//! this check (and calls `drain_writers()`) sees zero in-flight writers
//! (incorrectly, since this tx never entered the drain set), reads
//! `count() == 0`, and stamps `keyset_safe = true`. Then this tx's write
//! lands, violating the just-stamped proof.
//!
//! ## The fix (F-48b)
//!
//! For every table in `tx.write_set`, enter the drain set BEFORE reading
//! `needs_write_barrier()`. If the flag is `true` (slow path: gets a
//! `unique_write_lock` guard), drop the drain guard immediately (avoids
//! deadlock — the lock alone provides exclusion). If `false`, keep the drain
//! guard alive until Phase 5c materialize lands — so a DDL that raises the
//! barrier AFTER this check genuinely waits for this tx to finish.
//!
//! ## Determinism
//!
//! Same established Notify-handshake style as F-46/F-47/F-48: a `#[cfg(test)]`
//! `OnceLock<Arc<Hook>>` seam in `pre_commit.rs` parks the tx's commit task
//! strictly AFTER Phase 2.5's flag-check and BEFORE Phase 5c's materialize
//! write. The harness busy-polls `reached` (no sleeps) to detect the park,
//! drives the DDL's raise+lock+drain+count-proof while parked, then releases
//! the tx. On the UNFIXED code the DDL's count-proof reads 0 (race); on the
//! FIXED code the drain blocks until the tx finishes, so the count-proof sees
//! the in-flight writer's row.

use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::IsolationLevel;
use shamir_types::core::interner::{InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::value::InnerValue;
use tokio::sync::Notify;

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::TableConfig;
use crate::table::TableManager;
use crate::tx::pre_commit::{
    PostPrelockPreMaterializeHook, TEST_POST_PRELOCK_PRE_MATERIALIZE_HOOK,
};

fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

async fn key_id(tbl: &TableManager, name: &str) -> u64 {
    let interner = tbl.interner().get().await.unwrap();
    match interner.touch_ind(name).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    }
}

fn record_with_str(key: u64, val: &str) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(InternerKey::new(key), InnerValue::Str(val.into()));
    InnerValue::Map(m)
}

// ============================================================================
// THE F-48b PROOF — a tx whose Phase 2.5 reads needs_write_barrier()==false
// for a table parks strictly after that check and before its Phase 5c
// materialize write. While parked, a schema-activation DDL raises the
// barrier, takes unique_write_lock, calls drain_writers(), and reads its
// count-proof.
//
// - UNFIXED code: the tx never entered the drain set, so `drain_writers()`
//   returns immediately (counter == 0); count reads 0 while the tx is still
//   parked; the tx's write then lands → keyset_safe proof violated.
// - FIXED code: the tx entered the drain set before reading the flag, so
//   `drain_writers()` blocks until the tx finishes; the count-proof sees the
//   in-flight writer's row → proof sound.
// ============================================================================

#[tokio::test]
async fn f48b_drain_catches_tx_commit_that_read_false_before_flag_went_up() {
    let repo = make_repo();
    repo.add_table_for_test(TableConfig::new("people"));
    let tbl = repo.get_table("people").await.unwrap();
    let name_field = key_id(&tbl, "name").await;

    // Precondition: no barrier active → lock-free fast path. The tx's Phase
    // 2.5 will read needs_write_barrier()==false for this table.
    assert!(
        !tbl.needs_write_barrier(),
        "precondition: barrier down → lock-free path"
    );

    // Install the F-48b test seam (mirrors F-46/F-47/F-48): one-shot `armed`
    // CAS so only the FIRST committer to reach the seam parks; `reached` for
    // busy-poll detection (no sleeps); `resume` for the release signal.
    // nextest isolates this global per test process.
    let hook = Arc::new(PostPrelockPreMaterializeHook {
        reached: std::sync::atomic::AtomicUsize::new(0),
        resume: Notify::new(),
        armed: std::sync::atomic::AtomicBool::new(true),
    });
    TEST_POST_PRELOCK_PRE_MATERIALIZE_HOOK
        .set(Arc::clone(&hook))
        .expect("hook installed once per test process");

    // 1. Open a Snapshot tx and stage an insert into "people". The tx's
    //    write_set now contains the table token, so Phase 2.5 will scan it.
    let (mut tx, _guard) = repo.begin_tx(IsolationLevel::Snapshot).await.unwrap();
    tbl.insert_tx(&record_with_str(name_field, "Bob"), Some(&mut tx))
        .await
        .expect("stage insert");

    // 2. Spawn the tx's commit. It runs Phase 1, Phase 2.5 (reads
    //    needs_write_barrier()==false → fast path, no lock), and parks at
    //    the seam — BEFORE its Phase 5c materialize write.
    let repo_for_tx = repo.clone();
    let commit_task = tokio::spawn(async move { repo_for_tx.commit_tx(tx).await });

    // Rendezvous: wait until the commit has actually parked at the seam
    // (busy-poll `reached`, no sleeps — same handshake as F-46/F-47/F-48).
    while hook.reached.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    // The tx has read false and is parked. It has NOT written.
    assert_eq!(
        tbl.count().await.unwrap(),
        0,
        "tx parked before its materialize write — table must be empty"
    );

    // 3. The DDL raises the barrier + takes the lock + drains (post-fix) +
    //    reads its count-proof. Run the DDL's critical section as a task and
    //    receive the count result via a oneshot channel.
    //
    //    - UNFIXED code: `drain_writers()` returns immediately (no tx in the
    //      drain set) → count reads 0 while the tx is still parked.
    //    - FIXED code: the tx entered the drain set before parking, so the
    //      drain blocks here until we release the tx (step 4).
    let tbl_d = tbl.clone();
    let (proof_tx, proof_rx) = tokio::sync::oneshot::channel::<usize>();
    let ddl = tokio::spawn(async move {
        let _uwl = tbl_d.unique_write_lock().lock_owned().await;
        let _barrier = SchemaBarrierFlag::raise(&tbl_d);
        // F-48b drain: wait for in-flight fast-path tx-commit writers.
        tbl_d.drain_writers().await;
        let count_at_proof = tbl_d.count().await.unwrap();
        let _ = proof_tx.send(count_at_proof);
    });

    // Give the DDL enough time to acquire the lock, raise the flag, and
    // (post-fix) enter the drain loop. On the unfixed code the DDL completes
    // its count-proof in well under this window. (Same idiom as the F-48
    // non-tx sibling test.)
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    // 4. Release the parked tx. It completes its Phase 5c materialize write
    //    (data-store write + record-counter bump), and (post-fix) exits the
    //    drain set when the kept-alive drain guard drops inside materialize.
    hook.resume.notify_one();
    let outcome = commit_task
        .await
        .expect("commit task must not panic")
        .expect("tx commit must succeed once released");

    // 5. The DDL finishes (on the fixed code, the drain returned after the
    //    tx exited the drain set; on the unfixed code, it finished long ago
    //    with count==0).
    ddl.await.unwrap();
    let count_at_proof = proof_rx.await.unwrap();

    // Sanity: the tx's row IS physically present now (the write landed).
    assert_eq!(
        tbl.count().await.unwrap(),
        1,
        "tx committed — row must be present after release"
    );
    assert!(
        outcome.materialized(),
        "tx must report materialized Complete"
    );

    // 6. THE PROOF.
    //    - UNFIXED (check-then-act): count_at_proof == 0 — the DDL proved the
    //      table empty while a lock-free tx-commit writer was still in flight,
    //      then the writer landed → keyset_safe proof violated. FAIL here.
    //    - FIXED (drain): count_at_proof == 1 — the drain waited for the
    //      in-flight tx-commit writer; the count-proof saw its row. PASS.
    assert_eq!(
        count_at_proof, 1,
        "F-48b drain: the DDL's count-proof must observe the in-flight \
         tx-commit writer's row. Got {count_at_proof} = the check-then-act \
         race (DDL proved count()==0 while a lock-free tx-commit writer was \
         parked mid-flight, then the writer landed — keyset_safe proof \
         violated)."
    );
}

// ============================================================================
// RAII helper mirroring admin_schema's `SchemaActivationBarrierGuard`: raise
// the flag on construction, clear on drop. Local to this test module so the
// test drives the EXACT same set/clear discipline the production DDL uses.
// (Identical to the helper in `schema_activation_barrier_tests.rs`.)
// ============================================================================

struct SchemaBarrierFlag<'a> {
    table: &'a TableManager,
}

impl<'a> SchemaBarrierFlag<'a> {
    fn raise(table: &'a TableManager) -> Self {
        table.set_schema_activation_barrier(true);
        Self { table }
    }
}

impl Drop for SchemaBarrierFlag<'_> {
    fn drop(&mut self) {
        self.table.set_schema_activation_barrier(false);
    }
}
