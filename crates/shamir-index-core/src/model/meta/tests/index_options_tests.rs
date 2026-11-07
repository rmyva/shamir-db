//! Descriptor options compatibility and preservation regressions.

use super::super::index_options::{decode, write};
use crate::kind::VectorQuantization;
use crate::kind::{IndexKind, VectorBackendRef, VectorConfig, VectorMetric};
use crate::state::IndexState;
use crate::IndexDescriptor;
use serde::{Deserialize, Serialize};
use shamir_storage::error::DbError;
use smallvec::SmallVec;

#[test]
fn empty_round_trip_and_unknown_version() {
    let mut bytes = Vec::new();
    assert_eq!(decode(&bytes).unwrap(), None);
    write(&mut bytes, None).unwrap();
    assert!(bytes.is_empty());
    write(&mut bytes, Some(VectorQuantization::Sq8)).unwrap();
    assert_eq!(decode(&bytes).unwrap(), Some(VectorQuantization::Sq8));
    let mut future = vec![2, 3, 4];
    assert_eq!(decode(&future).unwrap(), None);
    write(&mut future, Some(VectorQuantization::Sq8)).unwrap();
    assert_eq!(future, vec![2, 3, 4]);
}

#[test]
fn malformed_and_unknown_string() {
    assert!(matches!(decode(&[1, 0xc1]), Err(DbError::Codec(_))));
    let mut bytes = vec![1, 0x81, 0xb3];
    bytes.extend_from_slice(b"vector_quantization");
    bytes.extend_from_slice(&[0xa6, b'f', b'u', b't', b'u', b'r', b'e']);
    assert_eq!(decode(&bytes).unwrap(), None);
}

#[test]
fn unknown_fields_survive_setting_sq8() {
    let mut bytes = vec![1, 0x81, 0xa6];
    bytes.extend_from_slice(b"future");
    bytes.extend_from_slice(&[0x92, 1, 2]);
    write(&mut bytes, Some(VectorQuantization::Sq8)).unwrap();
    assert_eq!(decode(&bytes).unwrap(), Some(VectorQuantization::Sq8));
    assert!(bytes
        .windows(10)
        .any(|window| window == b"\xa6future\x92\x01\x02"));
    let saved = bytes.clone();
    write(&mut bytes, Some(VectorQuantization::Sq8)).unwrap();
    assert_eq!(bytes, saved);
}

// Mirrors the pre-carrier descriptor and vector shape, independently of the
// current skipped quantization field.
#[derive(Serialize, Deserialize)]
struct OldDescriptor {
    id: u32,
    name: String,
    name_interned: u64,
    paths: SmallVec<[Vec<u64>; 2]>,
    kind: OldKind,
    created_at_nanos: u64,
    options: Vec<u8>,
    state: IndexState,
}

#[derive(Serialize, Deserialize)]
enum OldKind {
    Btree {
        unique: bool,
    },
    Fts {
        tokenizer: crate::kind::TokenizerKind,
        language: Option<String>,
    },
    Functional(Box<crate::kind::FunctionalConfig>),
    Vector(Box<OldVector>),
}

#[derive(Serialize, Deserialize)]
struct OldVector {
    dim: u32,
    metric: VectorMetric,
    backend: VectorBackendRef,
}

#[test]
fn golden_descriptor_is_readable_and_writable_by_old_shape() {
    const GOLDEN: &[u8] = &[
        7, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 120, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3,
        0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 200, 0, 0, 0, 16, 0, 0, 0, 11, 0, 0, 0, 0, 0,
        0, 0, 26, 0, 0, 0, 0, 0, 0, 0, 1, 129, 179, 118, 101, 99, 116, 111, 114, 95, 113, 117, 97,
        110, 116, 105, 122, 97, 116, 105, 111, 110, 163, 115, 113, 56, 0, 0, 0, 0,
    ];
    let mut options = Vec::new();
    write(&mut options, Some(VectorQuantization::Sq8)).unwrap();
    let descriptor = IndexDescriptor {
        id: 7,
        name: "x".into(),
        name_interned: 9,
        paths: SmallVec::new(),
        kind: IndexKind::Vector(Box::new(VectorConfig {
            dim: 3,
            metric: VectorMetric::L2,
            backend: VectorBackendRef::InProcessHnsw {
                ef_construct: 200,
                m: 16,
            },
            quantization: Some(VectorQuantization::Sq8),
        })),
        created_at_nanos: 11,
        options,
        state: IndexState::Ready,
    };
    assert_eq!(bincode::serialize(&descriptor).unwrap(), GOLDEN);
    let old: OldDescriptor = bincode::deserialize(GOLDEN).unwrap();
    assert_eq!(old.id, 7);
    assert_eq!(decode(&old.options).unwrap(), Some(VectorQuantization::Sq8));
    assert_eq!(bincode::serialize(&old).unwrap(), GOLDEN);
}
