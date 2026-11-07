use crate::backend::{IndexBackend, IndexError, IndexQuery, IndexResult};
use crate::descriptor::IndexDescriptor;
use crate::expr::IndexExpr;
use crate::kind::{
    FunctionalConfig, IndexKind, TokenizerKind, VectorBackendRef, VectorConfig, VectorMetric,
};
use crate::persistence::{load_index2_metadata, save_index2_metadata};
use crate::registry::IndexRegistry;
use crate::state::IndexState;
use async_trait::async_trait;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use smallvec::SmallVec;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(super) struct DummyBackend(pub(super) IndexDescriptor);

#[async_trait]
impl IndexBackend for DummyBackend {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn descriptor(&self) -> &IndexDescriptor {
        &self.0
    }
    async fn lookup(&self, _: IndexQuery) -> Result<IndexResult, IndexError> {
        Ok(IndexResult::Set(BTreeSet::new()))
    }
    async fn rebuild(&self, _: Arc<dyn Store>) -> Result<(), IndexError> {
        Ok(())
    }
    async fn drop_all(&self) -> Result<(), IndexError> {
        Ok(())
    }
}

fn make(id: u32, name_interned: u64) -> Arc<dyn IndexBackend> {
    Arc::new(DummyBackend(IndexDescriptor::new(
        id,
        format!("idx_{id}"),
        name_interned,
        SmallVec::new(),
        IndexKind::Btree { unique: false },
    )))
}

fn make_field(id: u32, name: u64, field: u64) -> Arc<dyn IndexBackend> {
    Arc::new(DummyBackend(IndexDescriptor::new(
        id,
        format!("idx_{id}"),
        name,
        SmallVec::from_vec(vec![vec![field]]),
        IndexKind::Btree { unique: false },
    )))
}

struct CountingBackend {
    descriptor: IndexDescriptor,
    calls: AtomicUsize,
}

#[async_trait]
impl IndexBackend for CountingBackend {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn descriptor(&self) -> &IndexDescriptor {
        self.calls.fetch_add(1, Ordering::SeqCst);
        &self.descriptor
    }
    async fn lookup(&self, _: IndexQuery) -> Result<IndexResult, IndexError> {
        Ok(IndexResult::Set(BTreeSet::new()))
    }
    async fn rebuild(&self, _: Arc<dyn Store>) -> Result<(), IndexError> {
        Ok(())
    }
    async fn drop_all(&self) -> Result<(), IndexError> {
        Ok(())
    }
}

#[tokio::test]
async fn reverse_candidate_absent_bucket_skips_unrelated_descriptors() {
    let reg = IndexRegistry::new();
    let mut backends = Vec::new();
    for id in 1..=32 {
        let backend = Arc::new(CountingBackend {
            descriptor: IndexDescriptor::new(
                id,
                format!("idx_{id}"),
                id as u64,
                SmallVec::from_vec(vec![vec![id as u64]]),
                IndexKind::Btree { unique: false },
            ),
            calls: AtomicUsize::new(0),
        });
        reg.insert(backend.clone()).await.unwrap();
        backends.push(backend);
    }
    for backend in &backends {
        backend.calls.store(0, Ordering::SeqCst);
    }
    assert!(reg
        .lease_by_field_and_kind(&[999], "btree")
        .await
        .unwrap()
        .is_none());
    assert!(backends
        .iter()
        .all(|backend| backend.calls.load(Ordering::SeqCst) == 0));
}

#[tokio::test]
async fn reverse_candidates_multiple_states_rename_and_recreation() {
    let reg = IndexRegistry::new();
    reg.insert(make_field(1, 101, 7)).await.unwrap();
    reg.insert(make_field(2, 102, 7)).await.unwrap();
    let selected = reg
        .lease_by_field_and_kind(&[7], "btree")
        .await
        .unwrap()
        .unwrap();
    assert!([1, 2].contains(&selected.backend.descriptor().id));
    drop(selected);
    reg.set_state(1, IndexState::Building).await;
    assert_eq!(
        reg.lease_by_field_and_kind(&[7], "btree")
            .await
            .unwrap()
            .unwrap()
            .backend
            .descriptor()
            .id,
        2
    );
    reg.set_failed(2, "failed").await;
    assert!(reg
        .lease_by_field_and_kind(&[7], "btree")
        .await
        .unwrap()
        .is_none());
    reg.set_state(1, IndexState::Ready).await;
    assert!(reg.rename_entry(101, "renamed".into(), 201).await);
    assert_eq!(
        reg.lease_by_field_and_kind(&[7], "btree")
            .await
            .unwrap()
            .unwrap()
            .backend
            .descriptor()
            .id,
        1
    );
    reg.remove_by_id(1).await.unwrap();
    assert!(reg
        .lease_by_field_and_kind(&[7], "btree")
        .await
        .unwrap()
        .is_none());
    reg.insert(make_field(1, 301, 8)).await.unwrap();
    assert!(reg
        .lease_by_field_and_kind(&[7], "btree")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        reg.lease_by_field_and_kind(&[8], "btree")
            .await
            .unwrap()
            .unwrap()
            .backend
            .descriptor()
            .id,
        1
    );
    assert!(reg
        .lease_by_field_and_kind(&[8], "unknown")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn reverse_candidates_rejected_registrations_do_not_publish() {
    let reg = IndexRegistry::new();
    reg.insert(make_field(1, 101, 7)).await.unwrap();
    assert!(reg.insert(make_field(2, 101, 8)).await.is_err());
    assert!(reg.insert(make_field(1, 102, 9)).await.is_err());
    assert!(reg
        .lease_by_field_and_kind(&[8], "btree")
        .await
        .unwrap()
        .is_none());
    assert!(reg
        .lease_by_field_and_kind(&[9], "btree")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn reverse_candidate_lease_holds_drain_and_blocks_new_readers() {
    let reg = IndexRegistry::new();
    reg.insert(make_field(1, 101, 7)).await.unwrap();
    let lease = reg
        .lease_by_field_and_kind(&[7], "btree")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reg.reader_gate().in_flight_count(), 1);
    let drain = reg.reader_gate().begin_drop();
    assert!(reg.lease_by_field_and_kind(&[7], "btree").await.is_err());
    let mut waiter = Box::pin(drain.wait_for_drain());
    assert!(futures::poll!(&mut waiter).is_pending());
    drop(lease);
    waiter.as_mut().await;
    drop(waiter);
    assert_eq!(reg.reader_gate().in_flight_count(), 0);
    drop(drain);
    assert!(reg
        .lease_by_field_and_kind(&[7], "btree")
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn insert_and_lookup_by_id() {
    let reg = IndexRegistry::new();
    let b = make(10, 100);
    reg.insert(b).await.unwrap();
    let got = reg.get_by_id(10).await.unwrap();
    assert_eq!(got.descriptor().id, 10);
}

#[tokio::test]
async fn insert_and_lookup_by_name() {
    let reg = IndexRegistry::new();
    reg.insert(make(11, 200)).await.unwrap();
    let got = reg.get_by_name(200).await.unwrap();
    assert_eq!(got.descriptor().id, 11);
}

#[tokio::test]
async fn allocate_id_monotonic() {
    let reg = IndexRegistry::new();
    let a = reg.allocate_id();
    let b = reg.allocate_id();
    let c = reg.allocate_id();
    assert!(a < b && b < c);
}

#[tokio::test]
async fn duplicate_id_rejected() {
    let reg = IndexRegistry::new();
    reg.insert(make(42, 1)).await.unwrap();
    let err = reg.insert(make(42, 2)).await.unwrap_err();
    assert!(matches!(err, IndexError::Backend(_)));
}

// ============================================================================
// R0-C (#1009) — insert() atomicity: a colliding name must leave `by_id`
// untouched, not partially-published.
// ============================================================================
//
// Before the fix, `insert()` published to `by_id` FIRST and `by_name`
// SECOND. A colliding name failed the `by_name` publish and returned `Err`
// WITHOUT rolling back `by_id` — the backend ended up registered by id
// (visible to `all_backends()`/`backends_newer_than()`/any by-id lookup) but
// unreachable by name (a DROP by that name would never find it). This test
// fails against the pre-fix code: the pre-fix `by_id.insert_async` for the
// colliding backend (id=99) SUCCEEDS (its id is unique — only the id=42
// original occupies id 42; the collision is on NAME, not id), so
// `reg.get_by_id(99)` would find the orphan and this assertion would fail.
#[tokio::test]
async fn insert_with_colliding_name_leaves_by_id_untouched() {
    let reg = IndexRegistry::new();
    // Original backend: id=42, name_interned=500.
    reg.insert(make(42, 500)).await.unwrap();

    // Colliding backend: DIFFERENT id (99), SAME name_interned (500).
    let err = reg
        .insert(make(99, 500))
        .await
        .expect_err("insert with a colliding name must fail");
    assert!(
        matches!(err, IndexError::Backend(_)),
        "expected a Backend error naming the collision, got {err:?}"
    );

    // The colliding backend's id must NOT be visible in by_id — checked
    // IMMEDIATELY after the failed call, not "eventually consistent".
    assert!(
        reg.get_by_id(99).await.is_none(),
        "#1009: a failed insert (name collision) must leave NO by_id entry \
         for the rejected backend — found one, meaning by_id was published \
         before the by_name check/publish and never rolled back"
    );
    // The original occupant is unaffected.
    assert_eq!(
        reg.get_by_id(42).await.unwrap().descriptor().id,
        42,
        "the original occupant of the name must be untouched"
    );
    assert_eq!(
        reg.get_by_name(500).await.unwrap().descriptor().id,
        42,
        "by_name must still resolve to the ORIGINAL occupant, not the \
         rejected colliding backend"
    );
}

#[tokio::test]
async fn remove_drops_both_maps() {
    let reg = IndexRegistry::new();
    reg.insert(make(7, 300)).await.unwrap();
    assert!(reg.get_by_name(300).await.is_some());
    let removed = reg.remove_by_id(7).await.unwrap();
    assert_eq!(removed.descriptor().id, 7);
    assert!(reg.get_by_id(7).await.is_none());
    assert!(reg.get_by_name(300).await.is_none());
}

// ============================================================================
// P0-5a (#961) — RENAME INDEX survives restart for index2 backends
// ============================================================================
//
// Before the fix, `rename_entry` updated ONLY the `by_name` reverse index,
// leaving the backend's own (immutable) descriptor carrying the stale original
// name. `all_descriptors()` — the persistence path — only overrode `.state`
// from the tuple, so `save_index2_metadata` wrote the OLD name to disk. After a
// restart the registry reloaded the persisted blob with the old name: the
// rename was silently reverted. These registry-level tests cover the
// persistence round-trip with a stand-in backend; the real FTS / functional /
// vector construction paths are covered in `shamir-index`
// (`tests/registry_reopen_tests.rs`).

/// Full rename → save → reload → reopen round-trip for one index2 kind.
///
/// Proves three things at once:
///   1. The persisted descriptor carries BOTH the new `name` and the new
///      `name_interned` (a lookup-only test could pass by accident if only
///      one field were fixed).
///   2. After a simulated restart (load blob + rebuild backends into a fresh
///      registry), a lookup by the NEW name succeeds and by the OLD name
///      fails.
///   3. The live (non-restart) in-memory rename still works.
async fn assert_rename_survives_reopen(kind: IndexKind, kind_label: &str) {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let reg = IndexRegistry::new();

    let old_ni = 1000_u64;
    let new_ni = 2000_u64;

    // Insert a stand-in backend under the OLD name (the real FTS / functional /
    // vector backends are exercised in shamir-index `registry_reopen_tests`).
    let backend = Arc::new(DummyBackend(IndexDescriptor::new(
        1,
        "orig_name",
        old_ni,
        SmallVec::new(),
        kind,
    )));
    reg.insert(backend).await.unwrap();

    // ── (3) Live in-memory rename still works ─────────────────────────────
    // The by_name reverse index flips to the new id; old name stops resolving.
    assert!(
        reg.rename_entry(old_ni, "renamed_name".to_string(), new_ni)
            .await,
        "rename_entry must succeed for a present {kind_label} backend"
    );
    assert!(
        reg.get_by_name(new_ni).await.is_some(),
        "live rename: new name must resolve in-memory ({kind_label})"
    );
    assert!(
        reg.get_by_name(old_ni).await.is_none(),
        "live rename: old name must NOT resolve in-memory ({kind_label})"
    );

    // ── Persist (this is the all_descriptors path — the original bug site) ──
    save_index2_metadata(&reg, &store).await.unwrap();

    // ── (2) Assert BOTH name fields in the persisted descriptor ───────────
    let loaded = load_index2_metadata(&store).await.unwrap().unwrap().1;
    assert_eq!(
        loaded.descriptors.len(),
        1,
        "exactly one {kind_label} descriptor must be persisted"
    );
    let d = &loaded.descriptors[0];
    assert_eq!(
        d.name, "renamed_name",
        "persisted {kind_label} descriptor must carry the NEW name (P0-5a)"
    );
    assert_eq!(
        d.name_interned, new_ni,
        "persisted {kind_label} descriptor must carry the NEW name_interned (P0-5a)"
    );

    // ── (1) Reopen into a fresh registry: lookup by new/old name ──────────
    let fresh = IndexRegistry::new();
    for d in &loaded.descriptors {
        let b = Arc::new(DummyBackend(d.clone()));
        fresh.insert(b).await.unwrap();
    }
    assert!(
        fresh.get_by_name(new_ni).await.is_some(),
        "after reopen, {kind_label} must be findable by the NEW name"
    );
    assert!(
        fresh.get_by_name(old_ni).await.is_none(),
        "after reopen, {kind_label} must NOT be findable by the OLD name"
    );
}

#[tokio::test]
async fn rename_persists_across_reopen_fts() {
    assert_rename_survives_reopen(
        IndexKind::Fts {
            tokenizer: TokenizerKind::Whitespace,
            language: None,
        },
        "fts",
    )
    .await;
}

#[tokio::test]
async fn rename_persists_across_reopen_functional() {
    assert_rename_survives_reopen(
        IndexKind::Functional(Box::new(FunctionalConfig {
            expr: IndexExpr::Lower(Box::new(IndexExpr::Field(vec![200]))),
        })),
        "functional",
    )
    .await;
}

#[tokio::test]
async fn rename_persists_across_reopen_vector() {
    assert_rename_survives_reopen(
        IndexKind::Vector(Box::new(VectorConfig {
            dim: 2,
            metric: VectorMetric::Cosine,
            backend: VectorBackendRef::InProcessHnsw {
                ef_construct: 200,
                m: 16,
            },
            quantization: None,
        })),
        "vector",
    )
    .await;
}

/// Regression: the EXISTING in-memory rename behavior (which already worked via
/// `by_name` before the fix) must be unaffected by the new `by_id` name-slot
/// update. A rename with a destination name that is ALREADY taken must still
/// be rejected and leave the old name intact.
#[tokio::test]
async fn rename_conflict_leaves_old_name_intact() {
    let reg = IndexRegistry::new();
    reg.insert(make(30, 3000)).await.unwrap();
    reg.insert(make(31, 3100)).await.unwrap();

    // Renaming 3000 → 3100 must fail (3100 is occupied) and roll back.
    let ok = reg.rename_entry(3000, "taken".to_string(), 3100).await;
    assert!(!ok, "rename onto an occupied name must be rejected");
    // Old name still resolves (rollback restored it).
    assert!(
        reg.get_by_name(3000).await.is_some(),
        "failed rename must leave the source name intact"
    );
    // The would-be victim is untouched.
    assert_eq!(
        reg.get_by_name(3100).await.unwrap().descriptor().id,
        31,
        "destination backend must be the original occupant, not the rename source"
    );
}

/// P0-5a (#961) secondary fix: after a rename, `remove_by_id` must unlink the
/// AUTHORITATIVE (new) interned name from `by_name`, not the backend's stale
/// construction-time `descriptor().name_interned`. Before the fix,
/// `remove_by_id` read `backend.descriptor().name_interned` (the OLD id) and
/// would leave a dangling `by_name` entry under the new id pointing at a
/// removed backend.
#[tokio::test]
async fn remove_after_rename_unlinks_new_name() {
    let reg = IndexRegistry::new();
    reg.insert(make(40, 4000)).await.unwrap();

    // Rename 4000 → 4100.
    assert!(reg.rename_entry(4000, "renamed_40".to_string(), 4100).await);
    // The new name resolves, the old does not.
    assert!(reg.get_by_name(4100).await.is_some());
    assert!(reg.get_by_name(4000).await.is_none());

    // Drop by id — must clean up the NEW name (4100), not the stale old (4000).
    let removed = reg.remove_by_id(40).await;
    assert!(removed.is_some(), "backend must be removed");

    // Neither name resolves now: no dangling entry under the new id.
    assert!(
        reg.get_by_name(4100).await.is_none(),
        "remove_by_id after rename must unlink the NEW name (no dangling by_name entry)"
    );
    assert!(reg.get_by_id(40).await.is_none());
}

// ============================================================================
// P1 (#992) / R0-A (#1006) — IndexRegistry::insert generation tagging
// ============================================================================
//
// #992: before that fix, `insert()` computed its per-entry generation tag as
// `generation.load(Acquire) + 1` — a classic read-then-write race UNDER TRUE
// CONCURRENT CALLERS. Two concurrent inserts that both read `generation == G`
// both computed `G+1`: the second's `fetch_max(G+1)` was a no-op, leaving
// `generation()` flat even though the second backend was published. At
// commit, `pre_commit.rs:825`'s `generation() == stage_gen` shortcut then
// skipped re-derivation ENTIRELY (not just `backends_newer_than` — the filter
// was never even called), so the tx committed with zero ops for the second
// backend (the exact "guaranteed miss" class #958/#987 exist to prevent).
// #992's fix replaced the racy `load() + 1` with a DEDICATED ticket counter
// (`insert_ticket`), decoupled from `generation`, whose `fetch_add` guaranteed
// distinct tickets regardless of interleaving.
//
// R0-A (#1006): the dedicated ticket counter is gone. Every registry-mutating
// DDL op (CREATE/DROP/RENAME, all four index families) now holds
// `TableManager::ddl_admission` for its ENTIRE critical section, including
// this `insert()` call — see `IndexRegistry::insert`'s doc and the R0-A
// brief. That serializes every caller of `insert`/`remove_by_id` on a given
// table to at most one in flight at a time, so the SAME `generation.load() +
// 1` pattern #992 replaced is safe again: there is no concurrent second
// caller left to race it. The tests below still pass unchanged — they assert
// the OUTCOME (distinct tags, generation advances by exactly N), which holds
// under EITHER scheme; they do not probe which counter produces it. Kept as
// regression coverage for the outcome, not for the (now-removed) two-counter
// mechanism.
//
// These tests are property-based: they assert the invariant the fix
// guarantees (`fetch_add`/serialized-`load()+1` uniqueness), which holds
// deterministically under the new code regardless of scheduling. A
// `tokio::sync::Barrier` synchronizes all tasks to enter `insert()` at the
// same instant — maximizing the contention that would trigger the OLD race —
// without any production-code pause-hook seam. A multi-threaded runtime gives
// true parallelism so the race window is exercised, not just simulated.
//
// NOTE: these two tests call `reg.insert()` DIRECTLY on a bare
// `IndexRegistry` (no `TableManager`/`ddl_admission` in the loop), so they do
// NOT themselves rely on admission serialization — they still race N
// concurrent inserts against the SAME registry instance with no external
// lock. Post-R0-A, `IndexRegistry::insert` no longer has an independent
// safety net against that: it is correct only because every REAL caller
// (via `TableManager`) is admission-serialized. A bare-registry concurrent
// insert as exercised here is exactly the case the merged counter would
// mishandle if admission were ever bypassed — seeing these tests still pass
// is a coincidence of `fetch_add`-style loads racing coherently in practice
// on this Barrier-synchronized shape, not a guarantee the merged counter
// makes on its own. Real callers must never call `insert()` without holding
// `ddl_admission`.

/// #992 / R0-A (#1006): N concurrent callers, each serialized through an
/// external admission lock (mirroring `TableManager::ddl_admission`), must
/// still produce `insert()` calls with DISTINCT generation tags.
///
/// R0-A (#1006) note: post-merge, `IndexRegistry::insert` computes its tag
/// via a plain `generation.load(Acquire) + 1` — safe ONLY because the real
/// production caller (`TableManager`) holds `ddl_admission` across the whole
/// call, serializing every registry mutation on a table to one at a time.
/// This test now models that external serialization explicitly (a
/// `tokio::sync::Mutex` wrapping each `insert()` call) instead of racing
/// `insert()` directly — racing it directly would (correctly, post-merge)
/// no longer be a supported usage: `IndexRegistry` alone provides no
/// concurrency guard of its own anymore, by design, since admission is the
/// caller's job. The property under test (distinct tags, `backends_newer_than`
/// consistency) is unchanged from #992; only the harness reflects the new
/// contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_inserts_get_distinct_generation_tags() {
    let reg = Arc::new(IndexRegistry::new());
    const N: u32 = 32;

    // Models `TableManager::ddl_admission`: only one `insert()` call may be
    // in flight at a time. Without this (i.e. racing `insert()` directly,
    // as the pre-R0-A version of this test did), the merged single-counter
    // design would legitimately collide — that IS the invariant R0-A's
    // admission-coverage fix establishes, not a gap in this test.
    let admission = Arc::new(tokio::sync::Mutex::new(()));

    // Synchronize all tasks so they contend for the admission lock at the
    // same instant, maximizing scheduler interleaving opportunities.
    let barrier = Arc::new(tokio::sync::Barrier::new(N as usize));
    let mut handles = Vec::new();
    for id in 1..=N {
        let reg = reg.clone();
        let barrier = barrier.clone();
        let admission = admission.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let _guard = admission.lock().await;
            reg.insert(make(id, id as u64 * 10)).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Read back each entry's gen tag via the test-only accessor and assert
    // all are distinct. Admission-serialized `load() + 1` guarantees distinct
    // values (no concurrent caller left to collide with), so the set of gens
    // is exactly {1, 2, ..., N}.
    let mut gens = Vec::new();
    for id in 1..=N {
        gens.push(
            reg.entry_gen(id)
                .await
                .expect("backend must be registered after concurrent insert"),
        );
    }
    let distinct: BTreeSet<u64> = gens.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        N as usize,
        "#992/#1006: admission-serialized inserts must receive DISTINCT generation tags; got {gens:?}"
    );

    // Cross-check via the public `backends_newer_than` API: with gens
    // {1, ..., N}, `backends_newer_than(t)` returns exactly N-t backends
    // for every threshold t in 0..=N. If any two gens collided (old bug),
    // this sweep would return a wider-than-expected count for some t.
    for t in 0..=N as u64 {
        let got = reg.backends_newer_than(t).await.len() as u64;
        let expected = N as u64 - t;
        assert_eq!(
            got, expected,
            "#992: backends_newer_than({t}) returned {got} backends, expected {expected} \
             (gens must be the contiguous set {{1..={N}}})"
        );
    }
}

/// #992 / R0-A (#1006): N concurrent callers, each serialized through an
/// external admission lock (mirroring `TableManager::ddl_admission`), must
/// advance `generation()` by exactly N.
///
/// R0-A (#1006) note: see the sibling test above for why this now wraps each
/// `insert()` call in an external `tokio::sync::Mutex` rather than racing
/// `insert()` directly — post-merge, `IndexRegistry` alone provides no
/// concurrency guard; serialization is the caller's (`ddl_admission`'s) job.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_inserts_advance_generation_by_exactly_n() {
    let reg = Arc::new(IndexRegistry::new());
    const N: u32 = 32;
    let gen_before = reg.generation();

    let admission = Arc::new(tokio::sync::Mutex::new(()));
    let barrier = Arc::new(tokio::sync::Barrier::new(N as usize));
    let mut handles = Vec::new();
    for id in 1..=N {
        let reg = reg.clone();
        let barrier = barrier.clone();
        let admission = admission.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let _guard = admission.lock().await;
            reg.insert(make(id, id as u64 * 10)).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let gen_after = reg.generation();
    assert_eq!(
        gen_after - gen_before,
        N as u64,
        "#992: {N} concurrent inserts must advance generation() by exactly {N} \
         (got {gen_before} -> {gen_after}); a smaller delta means some inserts \
         raced on the old `generation.load() + 1` and computed the same tag"
    );
}

// ============================================================================
// R0-A (#1006) — CREATE A → DROP A → stage → CREATE B → commit must NOT
// skip re-derivation for B.
// ============================================================================
//
// This is the deterministic (no concurrency needed) failure Part 1 of the
// R0-A brief describes:
//
//   CREATE A (ticket 1, publish → generation 1)
//   → DROP A (fetch_add → generation 2, insert_ticket untouched at 1)
//   → tx stages at generation 2 (A already gone, correctly empty plan)
//   → CREATE B (ticket 2, fetch_max(2) is a no-op since generation is
//     already 2)
//   → commit sees generation() == stage_gen (2), skips re-derivation
//     entirely — B's posting is never written for a row committed after B
//     existed.
//
// Pre-fix (two-counter `insert_ticket`/`generation` split), this test FAILS:
// CREATE B's `insert_ticket.fetch_add` produces ticket 2, but `generation`
// (already bumped to 2 by the DROP) sees `fetch_max(2)` as a no-op — so
// `generation()` after CREATE B is STILL 2, identical to `stage_gen`. The
// commit-path gate (`pre_commit.rs`'s `reg.generation() == stage_gen` at
// the site this test's assertion mirrors) then wrongly treats B as already
// accounted for.
//
// Post-fix (merged counter): CREATE B's tag is drawn from the SAME counter
// DROP A already advanced, so publishing B necessarily bumps `generation`
// PAST `stage_gen` — `generation() != stage_gen` is now provably always
// true whenever a registry mutation happened after the snapshot, closing
// the guaranteed-miss class structurally (one counter cannot go
// "backwards" or plateau the way two independently-progressing counters
// could).
#[tokio::test]
async fn create_a_drop_a_stage_create_b_commit_does_not_skip_rederivation() {
    let reg = IndexRegistry::new();

    // CREATE A.
    reg.insert(make(1, 100)).await.unwrap();
    assert_eq!(reg.generation(), 1, "CREATE A must publish generation 1");

    // DROP A.
    let removed = reg.remove_by_id(1).await;
    assert!(removed.is_some(), "A must be found and removed");
    assert_eq!(reg.generation(), 2, "DROP A must advance generation to 2");

    // A tx stages here: `all_backends()` is empty (A already dropped, B does
    // not exist yet) — a CORRECT, empty plan for this snapshot. The tx
    // captures the CURRENT generation as its stage-time watermark.
    let stage_gen = reg.generation();
    assert_eq!(stage_gen, 2, "stage-time snapshot captures generation 2");

    // CREATE B (a DIFFERENT backend, distinct id/name — this is the crux:
    // B did not exist at stage time, so the tx's stage-time plan has zero
    // ops for B; the commit-time re-derivation gate is the ONLY mechanism
    // that can add B's posting to this tx).
    reg.insert(make(2, 200)).await.unwrap();

    // THE ASSERTION: the commit-time gate is
    // `if reg.generation() == stage_gen { skip re-derivation }`
    // (mirrors `pre_commit.rs`'s `rederive_index2_ops_post_stage`). For the
    // tx's row (committed after B existed) to get B's posting, this
    // condition must be FALSE — i.e. generation() must have moved past
    // stage_gen.
    let current_gen = reg.generation();
    assert_ne!(
        current_gen, stage_gen,
        "R0-A (#1006): after CREATE A -> DROP A -> stage -> CREATE B, \
         generation() ({current_gen}) must NOT equal the stage-time snapshot \
         ({stage_gen}) — otherwise the commit-time re-derivation gate wrongly \
         skips B's posting entirely (the exact guaranteed-miss the two-counter \
         insert_ticket/generation split allowed)"
    );

    // Cross-check via the actual planner-facing API: B must be visible as
    // "newer than stage_gen" so the commit path's re-derivation actually
    // picks it up.
    let newer = reg.backends_newer_than(stage_gen).await;
    assert_eq!(
        newer.len(),
        1,
        "backends_newer_than(stage_gen) must return exactly B — the commit \
         path re-derives postings from precisely this set"
    );
    assert_eq!(newer[0].descriptor().id, 2, "the newer backend must be B");
}

// ============================================================================
// R0-D (#1013) — IndexState::Failed: set_failed / failure_reason_of /
// planner-visibility gate.
// ============================================================================
//
// These tests exercise the registry-level primitives the table-open
// recovery path (`table_manager.rs`) uses to fail a backend CLOSED when a
// `drop_all` (Building self-heal) or `restore_on_open` call genuinely
// fails. Before R0-D, `IndexState` had only `Ready`/`Building` and there was
// no `set_failed` — a failed recovery attempt left the backend registered
// at whatever state it already had (typically `Building`, still counted
// degraded but NOT reporting WHY, and in the `restore_on_open` case
// sometimes left as `Ready` from construction). These tests fail to even
// COMPILE against the pre-fix code (no `IndexState::Failed`, no
// `set_failed`, no `failure_reason_of`) — that IS the regression proof for
// the enum/registry surface; the behavioural assertions below prove the
// surface does what R0-D promises once it exists.

#[tokio::test]
async fn set_failed_moves_state_and_records_reason() {
    let reg = IndexRegistry::new();
    reg.insert(make(1, 100)).await.unwrap();
    assert_eq!(reg.state_of(1).await, Some(IndexState::Ready));

    let updated = reg.set_failed(1, "drop_all failed: injected fault").await;
    assert!(
        updated,
        "set_failed must find and update a registered backend"
    );
    assert_eq!(
        reg.state_of(1).await,
        Some(IndexState::Failed),
        "set_failed must move the authoritative state to Failed"
    );
    assert_eq!(
        reg.failure_reason_of(1).await.as_deref(),
        Some("drop_all failed: injected fault"),
        "failure_reason_of must surface the exact reason passed to set_failed"
    );
}

#[tokio::test]
async fn set_failed_on_unregistered_id_is_noop_false() {
    let reg = IndexRegistry::new();
    let updated = reg.set_failed(999, "no such backend").await;
    assert!(!updated, "set_failed on an absent id must return false");
    assert_eq!(reg.failure_reason_of(999).await, None);
}

#[tokio::test]
async fn failure_reason_of_is_none_before_and_after_healing() {
    let reg = IndexRegistry::new();
    reg.insert(make(2, 200)).await.unwrap();
    assert_eq!(
        reg.failure_reason_of(2).await,
        None,
        "a fresh Ready backend must carry no failure reason"
    );

    reg.set_failed(2, "boom").await;
    assert_eq!(reg.failure_reason_of(2).await.as_deref(), Some("boom"));

    // Healing back to Ready (mirrors doctor::repair()'s set_state(Ready)
    // call) must clear the stale reason — otherwise a later, unrelated
    // Failed transition (or a stale verify() read) could report the OLD
    // message.
    reg.set_state(2, IndexState::Ready).await;
    assert_eq!(reg.state_of(2).await, Some(IndexState::Ready));
    assert_eq!(
        reg.failure_reason_of(2).await,
        None,
        "set_state to a non-Failed state must clear the recorded failure reason"
    );
}

#[tokio::test]
async fn failed_backend_is_invisible_to_planner_lookup() {
    // Mirrors `lease_by_field_and_kind`'s existing Building-exclusion test
    // (none exists standalone today — this is the R0-D regression for the
    // *same* Ready-gate now also covering Failed). A Building backend was
    // already proven invisible by the index2_lifecycle_state_tests suite in
    // shamir-engine; this proves the identical gate (`entry.state !=
    // IndexState::Ready`) also excludes Failed, at the registry unit level,
    // independent of the engine's higher-level table-open plumbing.
    let reg = IndexRegistry::new();
    let field_path = vec![42u64];
    let desc = IndexDescriptor::new(
        7,
        "failed_idx",
        700,
        SmallVec::from_vec(vec![field_path.clone()]),
        IndexKind::Btree { unique: false },
    );
    let backend: Arc<dyn IndexBackend> = Arc::new(DummyBackend(desc));
    reg.insert(backend).await.unwrap();

    // Sanity: visible while Ready.
    assert!(
        reg.lease_by_field_and_kind(&field_path, "btree")
            .await
            .unwrap()
            .is_some(),
        "a Ready backend must be planner-visible"
    );

    reg.set_failed(7, "restore_on_open failed: injected fault")
        .await;

    assert!(
        reg.lease_by_field_and_kind(&field_path, "btree")
            .await
            .unwrap()
            .is_none(),
        "a Failed backend must be planner-INVISIBLE, exactly like Building -- \
         lease_by_field_and_kind must return Ok(None) so callers fall back to a \
         full scan instead of querying a broken/incomplete backend"
    );
}
