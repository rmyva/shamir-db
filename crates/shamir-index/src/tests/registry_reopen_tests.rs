//! Rename-survives-restart for the real FTS / functional / vector backends.
//!
//! The registry-level version of these tests lives in `shamir-index-core`
//! (with a stand-in backend); the backend factory is here, so the
//! construction paths exercised by the reopen rebuild are covered here.

use crate::kind::{
    FunctionalConfig, IndexKind, TokenizerKind, VectorBackendRef, VectorConfig, VectorMetric,
};
use crate::persistence::{load_index2_metadata, save_index2_metadata};
use crate::registry::IndexRegistry;
use crate::{build_index2_backend, IndexDescriptor, IndexExpr};
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use smallvec::SmallVec;
use std::sync::Arc;

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

    // Build + insert a real backend under the OLD name.
    let backend = build_index2_backend(
        IndexDescriptor::new(1, "orig_name", old_ni, SmallVec::new(), kind),
        &store,
    );
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
        let b = build_index2_backend(d.clone(), &store);
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
