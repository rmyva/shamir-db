use crate::descriptor::IndexDescriptor;
use crate::kind::{
    IndexKind, StemLanguage, VectorBackendRef, VectorConfig, VectorMetric, VectorQuantization,
};
use crate::persistence::{decode_persisted_indexes, PersistedIndexes};
use crate::MetaEnvelope;
use smallvec::SmallVec;

const VECTOR_PERSISTED_GOLDEN: &[u8] = &[
    // Header: magic (4), version (2), timestamp (8).
    0x53, 0x44, 0x42, 0x32, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, // next_id (4).
    2, 0, 0, 0, // descriptors length (8).
    1, 0, 0, 0, 0, 0, 0, 0, // descriptor id (4).
    1, 0, 0, 0, // name length (8), name (1).
    1, 0, 0, 0, 0, 0, 0, 0, b'v', // name_interned (8).
    1, 0, 0, 0, 0, 0, 0, 0, // paths length (8).
    0, 0, 0, 0, 0, 0, 0, 0, // IndexKind::Vector tag (3), dim (3), metric Cosine (1).
    3, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0,
    // VectorBackendRef::InProcessHnsw tag (0), ef_construct (200), m (16).
    0, 0, 0, 0, 200, 0, 0, 0, 16, 0, 0, 0,
    // created_at_nanos (8), options length (8), IndexState::Ready (0).
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

#[test]
fn serde_round_trip_btree() {
    let k = IndexKind::Btree { unique: true };
    let bytes = bincode::serialize(&k).unwrap();
    let got: IndexKind = bincode::deserialize(&bytes).unwrap();
    assert!(matches!(got, IndexKind::Btree { unique: true }));
}

#[test]
fn serde_round_trip_vector() {
    let k = IndexKind::Vector(Box::new(VectorConfig {
        dim: 384,
        metric: VectorMetric::Cosine,
        backend: VectorBackendRef::InProcessHnsw {
            ef_construct: 200,
            m: 16,
        },
        quantization: None,
    }));
    let bytes = bincode::serialize(&k).unwrap();
    let got: IndexKind = bincode::deserialize(&bytes).unwrap();
    match got {
        IndexKind::Vector(c) => assert_eq!(c.dim, 384),
        _ => panic!("wrong variant"),
    }
}

// ----- V5.2 (#411) VectorQuantization -----

#[test]
fn vector_quantization_from_dsl() {
    assert_eq!(
        VectorQuantization::from_dsl("sq8"),
        Some(VectorQuantization::Sq8)
    );
    assert_eq!(
        VectorQuantization::from_dsl("SQ8"),
        Some(VectorQuantization::Sq8)
    );
    assert_eq!(
        VectorQuantization::from_dsl("Sq8"),
        Some(VectorQuantization::Sq8)
    );
    assert_eq!(VectorQuantization::from_dsl("pq"), None);
    assert_eq!(VectorQuantization::from_dsl(""), None);
}

#[test]
fn vector_quantization_sq8_ordinal_zero() {
    // Bincode ordinal stability: Sq8 MUST remain ordinal 0 (append-only).
    let bytes = bincode::serialize(&VectorQuantization::Sq8).unwrap();
    let ordinal: u32 = bincode::deserialize(&bytes).unwrap();
    assert_eq!(ordinal, 0, "Sq8 must remain ordinal 0");
}

#[test]
fn serde_round_trip_vector_with_sq8() {
    // NOTE: `VectorConfig.quantization` is `#[serde(skip)]` (see the struct
    // doc) — bincode does NOT persist the quantization mode in #411. The
    // round-trip therefore yields `None`; the mode is carried by the WIRE
    // op (`CreateIndexOp.vector_quantization`) and threaded into the
    // adapter at create time, NOT by the persisted IndexDescriptor.
    // Snapshot codec for quantization is #412.
    let k = IndexKind::Vector(Box::new(VectorConfig {
        dim: 128,
        metric: VectorMetric::Cosine,
        backend: VectorBackendRef::InProcessHnsw {
            ef_construct: 200,
            m: 16,
        },
        quantization: Some(VectorQuantization::Sq8),
    }));
    let bytes = bincode::serialize(&k).unwrap();
    let got: IndexKind = bincode::deserialize(&bytes).unwrap();
    match got {
        IndexKind::Vector(c) => {
            assert_eq!(c.dim, 128);
            // #[serde(skip)] → quantization is NOT persisted; round-trip
            // yields None. This is by design (see VectorConfig doc).
            assert_eq!(c.quantization, None);
        }
        _ => panic!("wrong variant"),
    }
}

/// DDL round-trip: the wire string `"sq8"` parses through `from_dsl` into
/// `VectorConfig.quantization == Some(Sq8)`, and `None`/unknown → `None`
/// (legacy f32 path). This mirrors what `table_manager_index_mgmt.rs`
/// does at create-index time.
#[test]
fn ddl_roundtrip_sq8_into_vector_config() {
    let q = VectorQuantization::from_dsl("sq8");
    let cfg = VectorConfig {
        dim: 128,
        metric: VectorMetric::Cosine,
        backend: VectorBackendRef::InProcessHnsw {
            ef_construct: 200,
            m: 16,
        },
        quantization: q,
    };
    assert_eq!(cfg.quantization, Some(VectorQuantization::Sq8));

    // Unknown / None → None (legacy f32 path).
    let cfg_none = VectorConfig {
        dim: 128,
        metric: VectorMetric::Cosine,
        backend: VectorBackendRef::InProcessHnsw {
            ef_construct: 200,
            m: 16,
        },
        quantization: VectorQuantization::from_dsl("unknown"),
    };
    assert!(cfg_none.quantization.is_none());
}

#[test]
fn vector_backend_persisted_indexes_golden_bytes() {
    let persisted = PersistedIndexes {
        next_id: 2,
        descriptors: vec![IndexDescriptor {
            id: 1,
            name: "v".to_string(),
            name_interned: 1,
            paths: SmallVec::new(),
            kind: IndexKind::Vector(Box::new(VectorConfig {
                dim: 3,
                metric: VectorMetric::Cosine,
                backend: VectorBackendRef::InProcessHnsw {
                    ef_construct: 200,
                    m: 16,
                },
                quantization: None,
            })),
            created_at_nanos: 0,
            options: Vec::new(),
            state: crate::state::IndexState::Ready,
        }],
    };
    let envelope = MetaEnvelope {
        magic: crate::ENVELOPE_MAGIC,
        version: 1,
        written_at_nanos: 0,
        payload: persisted,
    };
    assert_eq!(
        envelope.encode().unwrap().as_slice(),
        VECTOR_PERSISTED_GOLDEN
    );
    let decoded = decode_persisted_indexes(VECTOR_PERSISTED_GOLDEN).unwrap();
    assert!(matches!(
        &decoded.descriptors[0].kind,
        IndexKind::Vector(config) if matches!(config.backend, VectorBackendRef::InProcessHnsw { .. })
    ));
}

#[test]
fn vector_backend_removed_legacy_variant_fails_decode() {
    let mut bytes = VECTOR_PERSISTED_GOLDEN[..67].to_vec();
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(
        &bincode::serialize(&(
            "legacy-driver".to_string(),
            "https://example.invalid".to_string(),
            "legacy-key".to_string(),
        ))
        .unwrap(),
    );
    bytes.extend_from_slice(&VECTOR_PERSISTED_GOLDEN[79..]);

    let legacy_payload: (String, String, String) = bincode::deserialize(&bytes[71..]).unwrap();
    assert_eq!(legacy_payload.0, "legacy-driver");
    assert_eq!(legacy_payload.1, "https://example.invalid");
    assert_eq!(legacy_payload.2, "legacy-key");
    assert!(matches!(
        MetaEnvelope::<PersistedIndexes>::open(&bytes),
        Err(crate::MetaError::Decode(_))
    ));
    assert!(matches!(
        decode_persisted_indexes(&bytes),
        Err(shamir_storage::error::DbError::Internal(_))
    ));
}

// ----- StemLanguage ordinal stability -----

#[test]
fn stem_language_english_ordinal_zero() {
    let bytes = bincode::serialize(&StemLanguage::English).unwrap();
    let ordinal: u32 = bincode::deserialize(&bytes).unwrap();
    assert_eq!(ordinal, 0, "English must remain ordinal 0");
}

#[test]
fn stem_language_russian_ordinal_one() {
    let bytes = bincode::serialize(&StemLanguage::Russian).unwrap();
    let ordinal: u32 = bincode::deserialize(&bytes).unwrap();
    assert_eq!(ordinal, 1, "Russian must remain ordinal 1");
}

// ----- StemLanguage::from_dsl -----

#[test]
fn from_dsl_full_names() {
    assert_eq!(
        StemLanguage::from_dsl("english"),
        Some(StemLanguage::English)
    );
    assert_eq!(
        StemLanguage::from_dsl("russian"),
        Some(StemLanguage::Russian)
    );
    assert_eq!(StemLanguage::from_dsl("french"), Some(StemLanguage::French));
    assert_eq!(StemLanguage::from_dsl("german"), Some(StemLanguage::German));
    assert_eq!(
        StemLanguage::from_dsl("spanish"),
        Some(StemLanguage::Spanish)
    );
    assert_eq!(StemLanguage::from_dsl("arabic"), Some(StemLanguage::Arabic));
    assert_eq!(StemLanguage::from_dsl("tamil"), Some(StemLanguage::Tamil));
}

#[test]
fn from_dsl_two_letter_codes() {
    assert_eq!(StemLanguage::from_dsl("en"), Some(StemLanguage::English));
    assert_eq!(StemLanguage::from_dsl("ru"), Some(StemLanguage::Russian));
    assert_eq!(StemLanguage::from_dsl("fr"), Some(StemLanguage::French));
    assert_eq!(StemLanguage::from_dsl("de"), Some(StemLanguage::German));
    assert_eq!(StemLanguage::from_dsl("es"), Some(StemLanguage::Spanish));
    assert_eq!(StemLanguage::from_dsl("nl"), Some(StemLanguage::Dutch));
    assert_eq!(StemLanguage::from_dsl("sv"), Some(StemLanguage::Swedish));
    assert_eq!(StemLanguage::from_dsl("no"), Some(StemLanguage::Norwegian));
    assert_eq!(StemLanguage::from_dsl("da"), Some(StemLanguage::Danish));
    assert_eq!(StemLanguage::from_dsl("fi"), Some(StemLanguage::Finnish));
    assert_eq!(StemLanguage::from_dsl("hu"), Some(StemLanguage::Hungarian));
    assert_eq!(StemLanguage::from_dsl("ro"), Some(StemLanguage::Romanian));
    assert_eq!(StemLanguage::from_dsl("tr"), Some(StemLanguage::Turkish));
    assert_eq!(StemLanguage::from_dsl("el"), Some(StemLanguage::Greek));
    assert_eq!(StemLanguage::from_dsl("ar"), Some(StemLanguage::Arabic));
    assert_eq!(StemLanguage::from_dsl("ta"), Some(StemLanguage::Tamil));
    assert_eq!(StemLanguage::from_dsl("pt"), Some(StemLanguage::Portuguese));
    assert_eq!(StemLanguage::from_dsl("it"), Some(StemLanguage::Italian));
}

#[test]
fn from_dsl_unknown_returns_none() {
    assert_eq!(StemLanguage::from_dsl("klingon"), None);
    assert_eq!(StemLanguage::from_dsl(""), None);
    assert_eq!(StemLanguage::from_dsl("EN"), None); // case-sensitive
}
