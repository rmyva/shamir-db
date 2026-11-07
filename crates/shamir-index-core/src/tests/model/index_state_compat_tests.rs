//! F-50 Step 3a spike — prove the forward-compat serialization mechanism
//! for adding a new `state: IndexState` field to `IndexDescriptor`.
//!
//! bincode 1.3.3 (the workspace version, pinned in
//! `crates/shamir-index/Cargo.toml`) is a NON-self-describing, positional
//! format: structs are laid out as their fields in declaration order with
//! NO field tags and NO trailing terminator. The two questions this spike
//! MUST settle by measurement, not assumption:
//!
//! 1. Does `#[serde(default)]` rescue a NEW trailing field when reading
//!    OLD bytes (encoded before that field existed)? — see
//!    `serde_default_does_not_rescue_trailing_field`.
//! 2. Does the chosen fallback mechanism (try current shape, fall back to
//!    a pre-`state` shadow shape) round-trip BOTH old and new blobs? — see
//!    `decode_loads_pre_state_blob_as_ready` and
//!    `decode_preserves_explicit_building_state`.
//!
//! The shadow types (`OldDescriptor`/`NewDescriptor`) are byte-faithful to
//! the real pre-/post-`state` `IndexDescriptor` (same field order, types,
//! and `#[serde(default)]` on `options`); they exist ONLY to hand-encode
//! the OLD shape against the NEW shape in isolation. The last two tests
//! exercise the REAL `IndexDescriptor` + `persistence::decode_persisted_indexes`
//! load path end-to-end.

use crate::kind::IndexKind;
use crate::persistence::decode_persisted_indexes;
use crate::persistence::PersistedIndexes;
use crate::MetaEnvelope;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use smallvec::SmallVec;

// ============================================================================
// Shadow shapes (byte-faithful to the real pre-/post-`state` descriptor)
// ============================================================================

/// On-disk shape of `IndexDescriptor` BEFORE the `state` field is added.
#[derive(Serialize, Deserialize)]
struct OldDescriptor {
    pub id: u32,
    pub name: String,
    pub name_interned: u64,
    pub paths: SmallVec<[Vec<u64>; 2]>,
    pub kind: IndexKind,
    pub created_at_nanos: u64,
    #[serde(default)]
    pub options: Vec<u8>,
}

/// NEW shape: `OldDescriptor` + a trailing `state` with `#[serde(default)]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
enum SpikeState {
    #[default]
    Ready,
    Building,
}

#[derive(Debug, Serialize, Deserialize)]
struct NewDescriptor {
    pub id: u32,
    pub name: String,
    pub name_interned: u64,
    pub paths: SmallVec<[Vec<u64>; 2]>,
    pub kind: IndexKind,
    pub created_at_nanos: u64,
    #[serde(default)]
    pub options: Vec<u8>,
    #[serde(default)]
    pub state: SpikeState,
}

#[derive(Serialize, Deserialize)]
struct OldPersisted {
    pub next_id: u32,
    pub descriptors: Vec<OldDescriptor>,
}

fn sample_old() -> OldDescriptor {
    let mut paths: SmallVec<[Vec<u64>; 2]> = SmallVec::new();
    paths.push(vec![1, 2, 3]);
    OldDescriptor {
        id: 7,
        name: "users_email".to_string(),
        name_interned: 42,
        paths,
        kind: IndexKind::Btree { unique: false },
        created_at_nanos: 1_700_000_000_000,
        options: Vec::new(),
    }
}

// ============================================================================
// 1. THE proof: `#[serde(default)]` does NOT rescue a trailing field.
// ============================================================================

/// Serialize the OLD shape, attempt to read it back as the NEW shape.
///
/// RESULT (measured, bincode 1.3.3): the read FAILS with
/// `io error: unexpected end of file` (`ErrorKind::Io(UnexpectedEof)`).
/// `#[serde(default)]` is a NO-OP for bincode's positional struct reader:
/// it unconditionally tries to consume a value for every declared field,
/// and the trailing field's bytes simply are not there. This is the SAME
/// failure `VectorConfig::quantization`'s doc comment flagged for
/// `#[serde(skip)]` (`kind.rs`), now confirmed for `#[serde(default)]`.
///
/// A wrong assumption here would silently break `MetaEnvelope::open` for
/// every existing on-disk index descriptor the first time `state` is added.
#[test]
fn serde_default_does_not_rescue_trailing_field() {
    let old_bytes = bincode::serialize(&sample_old()).unwrap();

    // Sanity: the OLD shape round-trips with itself.
    let _: OldDescriptor = bincode::deserialize(&old_bytes).unwrap();

    let err = bincode::deserialize::<NewDescriptor>(&old_bytes).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("unexpected end of file"),
        "expected an UnexpectedEof-style failure from bincode reading a \
         pre-`state` blob as the new shape, got: {msg}"
    );
}

// ============================================================================
// 2. Control: the NEW shape round-trips with itself (state present both ends).
// ============================================================================

#[test]
fn new_shape_round_trips_with_itself() {
    let mut paths: SmallVec<[Vec<u64>; 2]> = SmallVec::new();
    paths.push(vec![9]);
    let n = NewDescriptor {
        id: 5,
        name: "idx".to_string(),
        name_interned: 99,
        paths,
        kind: IndexKind::Btree { unique: true },
        created_at_nanos: 42,
        options: vec![0xAA, 0xBB],
        state: SpikeState::Building,
    };
    let bytes = bincode::serialize(&n).unwrap();
    let rt: NewDescriptor = bincode::deserialize(&bytes).unwrap();
    assert_eq!(rt.id, 5);
    assert_eq!(rt.name_interned, 99);
    assert_eq!(rt.options, vec![0xAA, 0xBB]);
    assert_eq!(rt.state, SpikeState::Building);
}

// ============================================================================
// 3. Integration: the REAL load path round-trips a pre-`state` blob as Ready.
// ============================================================================

/// Hand-encode a pre-`state` `PersistedIndexes` blob (via the shadow shape,
/// under the SAME `MetaEnvelope`/version=1 the real writer uses), then feed
/// it through the REAL `decode_persisted_indexes`. It MUST decode and lift
/// every descriptor to `state = Ready` (every pre-`state` persisted index
/// was fully built — a `Building` index could not have been persisted before
/// the field existed).
#[test]
fn decode_loads_pre_state_blob_as_ready() {
    let legacy = OldPersisted {
        next_id: 8,
        descriptors: vec![sample_old()],
    };
    let bytes = MetaEnvelope::new(legacy).encode().unwrap();

    let got = decode_persisted_indexes(&bytes).unwrap();
    assert_eq!(got.next_id, 8);
    assert_eq!(got.descriptors.len(), 1);
    assert_eq!(got.descriptors[0].id, 7);
    assert_eq!(got.descriptors[0].name, "users_email");
    assert_eq!(got.descriptors[0].paths[0], vec![1, 2, 3]);
    assert_eq!(
        got.descriptors[0].state,
        crate::state::IndexState::Ready,
        "a pre-`state` blob must lift to Ready, not Building"
    );
}

/// A NEW-shape blob with an explicit `Building` state must round-trip
/// through the REAL load path preserving `Building` (no false fallback).
#[test]
fn decode_preserves_explicit_building_state() {
    use crate::descriptor::IndexDescriptor;
    use crate::state::IndexState;

    let desc = {
        let mut d = IndexDescriptor::new(
            3,
            "lower_email",
            200,
            smallvec![vec![200]],
            IndexKind::Btree { unique: false },
        );
        d.state = IndexState::Building;
        d
    };
    let p = PersistedIndexes {
        next_id: 4,
        descriptors: vec![desc],
    };
    let bytes = MetaEnvelope::new(p).encode().unwrap();

    let got = decode_persisted_indexes(&bytes).unwrap();
    assert_eq!(got.descriptors.len(), 1);
    assert_eq!(
        got.descriptors[0].state,
        IndexState::Building,
        "a new-shape Building blob must NOT fall back to the legacy path"
    );
}

/// A genuinely corrupt blob (decodes as neither shape) must surface an
/// error, NOT be silently lifted to Ready.
#[test]
fn decode_corrupt_blob_errors() {
    let garbage = b"SDB2\x01\x00NOT_A_VALID_PAYLOAD_AT_ALL___________";
    assert!(decode_persisted_indexes(garbage).is_err());
}
