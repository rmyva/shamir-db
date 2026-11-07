use crate::descriptor::IndexDescriptor;
use crate::kind::{
    FunctionalConfig, IndexKind, TokenizerKind, VectorBackendRef, VectorConfig, VectorMetric,
};
use crate::persistence::{
    add_to_dropping_index2, clear_from_dropping_index2, legacy_indexes_need_rebuild,
    load_dropping_index2, load_index2_metadata, load_legacy_index_version, save_index2_metadata,
    save_legacy_index_version, PersistedIndexes, LEGACY_INDEX_FORMAT_VERSION,
};
use crate::tests::runtime::registry_tests::DummyBackend;
use crate::MetaEnvelope;
use bytes::Bytes;
use shamir_storage::storage_in_memory::InMemoryStore;
use shamir_storage::types::Store;
use shamir_types::types::record_id::RecordId;
use smallvec::SmallVec;
use std::sync::Arc;

// The meta key tag "_m.idx" is byte-identical to MetaKey::Indexes.tag() in the engine.
fn meta_key_indexes() -> RecordId {
    RecordId::system("_m.idx")
}

#[tokio::test]
async fn p34_12_index2_envelope_version_tracks_fts_functional_membership() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let registry = crate::IndexRegistry::new();
    let vector = IndexDescriptor::new(
        71,
        "vector_only",
        710,
        SmallVec::new(),
        IndexKind::Vector(Box::new(VectorConfig {
            dim: 3,
            metric: VectorMetric::Cosine,
            backend: VectorBackendRef::InProcessHnsw {
                ef_construct: 64,
                m: 16,
            },
            quantization: None,
        })),
    );
    registry
        .insert(Arc::new(DummyBackend(vector.clone())))
        .await
        .unwrap();

    save_index2_metadata(&registry, &store).await.unwrap();
    let (v, persisted) = load_index2_metadata(&store).await.unwrap().unwrap();
    assert_eq!(v, 1);
    assert!(MetaEnvelope::<PersistedIndexes>::open(
        &store
            .get(meta_key_indexes().to_bytes().into())
            .await
            .unwrap()
    )
    .is_ok());
    assert_eq!(persisted.descriptors.len(), 1);
    assert!(matches!(
        persisted.descriptors[0].kind,
        IndexKind::Vector(_)
    ));

    let fts = IndexDescriptor::new(
        72,
        "fts",
        720,
        SmallVec::new(),
        IndexKind::Fts {
            tokenizer: TokenizerKind::Whitespace,
            language: None,
        },
    );
    registry.insert(Arc::new(DummyBackend(fts))).await.unwrap();
    save_index2_metadata(&registry, &store).await.unwrap();
    let (v, persisted) = load_index2_metadata(&store).await.unwrap().unwrap();
    assert_eq!(v, 2);
    assert!(matches!(
        MetaEnvelope::<PersistedIndexes>::open(
            &store
                .get(meta_key_indexes().to_bytes().into())
                .await
                .unwrap()
        ),
        Err(crate::MetaError::UnsupportedVersion(2))
    ));
    assert!(persisted
        .descriptors
        .iter()
        .any(|d| matches!(d.kind, IndexKind::Vector(_))));
    assert!(persisted
        .descriptors
        .iter()
        .any(|d| matches!(d.kind, IndexKind::Fts { .. })));

    let functional = IndexDescriptor::new(
        73,
        "functional",
        730,
        SmallVec::new(),
        IndexKind::Functional(Box::new(FunctionalConfig {
            expr: crate::expr::IndexExpr::Lower(Box::new(crate::expr::IndexExpr::Field(vec![730]))),
        })),
    );
    registry
        .insert(Arc::new(DummyBackend(functional)))
        .await
        .unwrap();
    save_index2_metadata(&registry, &store).await.unwrap();
    let (v, persisted) = load_index2_metadata(&store).await.unwrap().unwrap();
    assert_eq!(v, 2);
    assert!(persisted
        .descriptors
        .iter()
        .any(|d| matches!(d.kind, IndexKind::Functional(_))));

    assert!(registry.remove_by_id(72).await.is_some());
    save_index2_metadata(&registry, &store).await.unwrap();
    let (v, persisted) = load_index2_metadata(&store).await.unwrap().unwrap();
    assert_eq!(v, 2);
    assert!(persisted
        .descriptors
        .iter()
        .any(|d| matches!(d.kind, IndexKind::Functional(_))));
    assert!(!persisted
        .descriptors
        .iter()
        .any(|d| matches!(d.kind, IndexKind::Fts { .. })));

    assert!(registry.remove_by_id(73).await.is_some());
    save_index2_metadata(&registry, &store).await.unwrap();
    let (v, persisted) = load_index2_metadata(&store).await.unwrap().unwrap();
    assert_eq!(v, 1);
    assert_eq!(persisted.descriptors.len(), 1);
    assert!(matches!(
        persisted.descriptors[0].kind,
        IndexKind::Vector(_)
    ));
}

#[tokio::test]
async fn round_trip_save_load() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let registry = crate::IndexRegistry::new();
    // Allocate IDs to advance counter.
    let _ = registry.allocate_id();
    let _ = registry.allocate_id();

    save_index2_metadata(&registry, &store).await.unwrap();
    let loaded = load_index2_metadata(&store).await.unwrap().unwrap().1;
    assert_eq!(loaded.next_id, 3);
    assert!(loaded.descriptors.is_empty());
}

#[tokio::test]
async fn round_trip_with_descriptors() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let _registry = crate::IndexRegistry::new();

    // Simulate: 2 descriptors persisted (via save, not through registry —
    // just testing save/load serialization).
    let d1 = IndexDescriptor::new(
        1,
        "fts_body",
        100,
        SmallVec::new(),
        IndexKind::Fts {
            tokenizer: TokenizerKind::Whitespace,
            language: None,
        },
    );
    let d2 = IndexDescriptor::new(
        2,
        "lower_email",
        200,
        SmallVec::new(),
        IndexKind::Functional(Box::new(FunctionalConfig {
            expr: crate::expr::IndexExpr::Lower(Box::new(crate::expr::IndexExpr::Field(vec![200]))),
        })),
    );

    // Save manually constructed PersistedIndexes.
    let p = PersistedIndexes {
        next_id: 3,
        descriptors: vec![d1, d2],
    };
    let envelope = MetaEnvelope::new(p);
    let bytes = envelope.encode().unwrap();
    let key = meta_key_indexes();
    store
        .set(key.to_bytes().into(), Bytes::from(bytes))
        .await
        .unwrap();

    let loaded = load_index2_metadata(&store).await.unwrap().unwrap().1;
    assert_eq!(loaded.next_id, 3);
    assert_eq!(loaded.descriptors.len(), 2);
    assert_eq!(loaded.descriptors[0].name, "fts_body");
    assert_eq!(loaded.descriptors[1].name, "lower_email");
}

#[tokio::test]
async fn load_missing_returns_none() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let loaded = load_index2_metadata(&store).await.unwrap();
    assert!(loaded.is_none());
}

// ============================================================================
// S9 — legacy index format version
// ============================================================================

#[tokio::test]
async fn legacy_version_missing_returns_zero() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let v = load_legacy_index_version(&store).await.unwrap();
    assert_eq!(v, 0, "missing version marker must return 0");
}

#[tokio::test]
async fn legacy_version_save_load_roundtrip() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    save_legacy_index_version(&store).await.unwrap();
    let v = load_legacy_index_version(&store).await.unwrap();
    assert_eq!(v, LEGACY_INDEX_FORMAT_VERSION);
}

#[tokio::test]
async fn legacy_needs_rebuild_when_missing() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    assert!(
        legacy_indexes_need_rebuild(&store).await.unwrap(),
        "pre-S9 data (no version marker) must trigger rebuild"
    );
}

#[tokio::test]
async fn legacy_no_rebuild_when_current() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    save_legacy_index_version(&store).await.unwrap();
    assert!(
        !legacy_indexes_need_rebuild(&store).await.unwrap(),
        "current version must NOT trigger rebuild"
    );
}

#[tokio::test]
async fn legacy_rebuild_when_old_version() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    // Simulate an old version (1) in the store.
    let key = RecordId::system("_m.idx.lfv");
    let old_ver: u32 = 1;
    store
        .set(
            key.to_bytes().into(),
            Bytes::from(old_ver.to_le_bytes().to_vec()),
        )
        .await
        .unwrap();
    assert!(
        legacy_indexes_need_rebuild(&store).await.unwrap(),
        "old version must trigger rebuild"
    );
}

// ============================================================================
// #1051 — index2 DROP tombstone backward compatibility
// ============================================================================

/// #1204 DELIBERATE BACK-COMPAT BREAK (see `decode_dropping_index2`'s doc
/// for the full reasoning): the pre-#1051 bare `Vec<u32>` tombstone shape
/// (no name/op_id fields) is no longer decoded. This inverts what this test
/// asserted pre-#1204 — a bare `Vec<u32>` blob used to be silently
/// resurrected with a synthesized empty name / `op_id: None`; it must now
/// fail closed with `DbError::Codec`. Rationale: that shape could only be
/// produced by a process that crashed mid-DROP on a pre-#1051 binary
/// (before 2026-08-09) and has never been reopened since — any reopen on a
/// #1051-or-later binary already recovers and rewrites this key.
#[tokio::test]
async fn p1051_old_format_index2_drop_tombstone_now_errors() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let old_format: Vec<u32> = vec![10, 20];
    let key = RecordId::system("_m.idx.drop").to_bytes();
    let bytes = bincode::serialize(&old_format).unwrap();
    store.set(key.into(), Bytes::from(bytes)).await.unwrap();

    let result = load_dropping_index2(&store).await;
    assert!(
        result.is_err(),
        "pre-#1051 bare Vec<u32> tombstone must now fail closed \
         (#1204 deliberately dropped this fallback), got: {:?}",
        result
    );
}

/// #1204: a single-entry pre-#1051 `Vec<u32>` tombstone must ALSO fail
/// closed, not just the two-entry case above — structurally it's too short
/// to parse as the current tuple shape (only 4 bytes remain after the
/// length prefix; one tuple entry needs at least 13).
#[tokio::test]
async fn p1051_old_format_single_entry_index2_drop_tombstone_now_errors() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let old_format: Vec<u32> = vec![7];
    let key = RecordId::system("_m.idx.drop").to_bytes();
    let bytes = bincode::serialize(&old_format).unwrap();
    store.set(key.into(), Bytes::from(bytes)).await.unwrap();

    let result = load_dropping_index2(&store).await;
    assert!(
        result.is_err(),
        "single-entry pre-#1051 tombstone must fail closed too, got: {:?}",
        result
    );
}

/// #1051: a NEW-format index2 DROP tombstone round-trips its name and op_id
/// through `add_to_dropping_index2` / `load_dropping_index2`.
#[tokio::test]
async fn p1051_new_format_index2_drop_tombstone_round_trips_name_and_op_id() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());

    let op_id = RecordId::new();
    add_to_dropping_index2(5, "lower_name".to_string(), Some(op_id.to_string()), &store)
        .await
        .unwrap();
    add_to_dropping_index2(6, "upper_name".to_string(), None, &store)
        .await
        .unwrap();

    let loaded = load_dropping_index2(&store).await.unwrap();
    assert_eq!(
        loaded,
        vec![
            (5, "lower_name".to_string(), Some(op_id.to_string())),
            (6, "upper_name".to_string(), None),
        ],
        "new-format tombstone must round-trip name and op_id exactly"
    );
}

// ============================================================================
// #1204 — index2 DROP tombstone version-byte envelope
// ============================================================================

/// #1204: `add_to_dropping_index2` writes the version-tagged envelope — the
/// first on-disk byte is `0x81`, not a raw bincode length prefix.
#[tokio::test]
async fn p1204_write_path_carries_version_byte() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    add_to_dropping_index2(1, "idx".to_string(), None, &store)
        .await
        .unwrap();

    let key = RecordId::system("_m.idx.drop").to_bytes();
    let bytes = store.get(key.into()).await.unwrap();
    assert_eq!(
        bytes[0], 0x81,
        "on-disk tombstone must start with the #1204 version byte"
    );
}

/// #1204: round-trips through the real add/load/clear path, with the
/// version envelope in place end to end.
#[tokio::test]
async fn p1204_round_trip_versioned_envelope() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let op_id = RecordId::new();
    add_to_dropping_index2(42, "vec_idx".to_string(), Some(op_id.to_string()), &store)
        .await
        .unwrap();
    add_to_dropping_index2(43, "fts_idx".to_string(), None, &store)
        .await
        .unwrap();

    let loaded = load_dropping_index2(&store).await.unwrap();
    assert_eq!(
        loaded,
        vec![
            (42, "vec_idx".to_string(), Some(op_id.to_string())),
            (43, "fts_idx".to_string(), None),
        ]
    );

    clear_from_dropping_index2(42, &store).await.unwrap();
    let loaded = load_dropping_index2(&store).await.unwrap();
    assert_eq!(loaded, vec![(43, "fts_idx".to_string(), None)]);
}

/// #1204: an unrecognized version byte must fail closed with a decode
/// error, never silently misinterpreted as the current shape.
#[tokio::test]
async fn p1204_unrecognized_version_byte_fails_closed() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let key = RecordId::system("_m.idx.drop").to_bytes();
    // 0x82 looks like a versioned envelope (high bit set) but is not the
    // current version (0x81) — must not be decoded as if it were, and must
    // not fall through to the legacy tier either.
    let mut bytes = vec![0x82u8];
    bytes.extend_from_slice(
        &bincode::serialize(&Vec::<(u32, String, Option<String>)>::new()).unwrap(),
    );
    store.set(key.into(), Bytes::from(bytes)).await.unwrap();

    let result = load_dropping_index2(&store).await;
    assert!(
        result.is_err(),
        "unrecognized version byte must fail closed, got: {:?}",
        result
    );
}

/// #1204 verdict B: a pre-#1204 UNVERSIONED tombstone — the exact shape
/// every `add_to_dropping_index2` wrote between #1051 and #1204 — must
/// still decode correctly. This is deliberately kept (unlike the pre-#1051
/// `Vec<u32>` shape above): dropping it would fail `TableManager::create`
/// on every database that has ever completed one index2 DROP, since
/// `clear_from_dropping_index2` never deletes the key.
#[tokio::test]
async fn p1204_legacy_unversioned_tuple_tombstone_still_decodes() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let op_id = RecordId::new();
    let legacy: Vec<(u32, String, Option<String>)> = vec![
        (1, "lower_name".to_string(), Some(op_id.to_string())),
        (2, "upper_name".to_string(), None),
    ];
    let key = RecordId::system("_m.idx.drop").to_bytes();
    let bytes = bincode::serialize(&legacy).unwrap();
    store.set(key.into(), Bytes::from(bytes)).await.unwrap();

    let loaded = load_dropping_index2(&store).await.unwrap();
    assert_eq!(loaded, legacy);
}

/// #1204: the legacy-tuple steady-state empty blob — what
/// `clear_from_dropping_index2` wrote pre-#1204, and what every already-
/// completed index2 DROP leaves on disk forever — must decode to an empty
/// Vec, not error.
#[tokio::test]
async fn p1204_legacy_unversioned_empty_tombstone_decodes_to_empty() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let key = RecordId::system("_m.idx.drop").to_bytes();
    let bytes = bincode::serialize(&Vec::<(u32, String, Option<String>)>::new()).unwrap();
    store.set(key.into(), Bytes::from(bytes)).await.unwrap();

    let loaded = load_dropping_index2(&store).await.unwrap();
    assert!(loaded.is_empty());
}

/// #1204: proves the `0x81` version byte choice does NOT collide with a
/// genuine one-entry legacy tombstone's first byte (`0x01` — the low byte
/// of bincode's `u64` length-1 prefix). The legacy blob must decode via the
/// legacy tier, not be misread as a (garbage) versioned payload.
#[tokio::test]
async fn p1204_version_byte_does_not_collide_with_legacy_len_1_first_byte() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let legacy: Vec<(u32, String, Option<String>)> = vec![(99, String::new(), None)];
    let bytes = bincode::serialize(&legacy).unwrap();
    assert_eq!(
        bytes[0], 0x01,
        "precondition: a one-entry legacy tuple-vec's first byte is 0x01"
    );

    let key = RecordId::system("_m.idx.drop").to_bytes();
    store.set(key.into(), Bytes::from(bytes)).await.unwrap();

    let loaded = load_dropping_index2(&store).await.unwrap();
    assert_eq!(loaded, legacy);
}
