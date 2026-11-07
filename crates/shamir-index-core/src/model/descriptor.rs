//! Persisted description of a single index instance.

use crate::kind::IndexKind;
use crate::state::IndexState;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDescriptor {
    /// Compact numeric ID used as the posting-key prefix (4 bytes,
    /// auto-incremented by `IndexRegistry`). Identifies the index in
    /// the hot path.
    pub id: u32,
    /// Human-readable name (DDL surface, error messages).
    pub name: String,
    /// Interned name — used for in-memory dispatch where strings are
    /// expensive (cross-references with `Interner`).
    pub name_interned: u64,
    /// Field paths the index covers. Each path is `Vec<u64>` of
    /// interned segment keys. SmallVec inline cap = 2 (most indexes
    /// are single- or two-field).
    pub paths: SmallVec<[Vec<u64>; 2]>,
    pub kind: IndexKind,
    pub created_at_nanos: u64,
    /// Opaque backend-specific tuning (bincode-friendly). Encoded
    /// as msgpack or whatever the backend wants; empty by default.
    #[serde(default)]
    pub options: Vec<u8>,
    /// Persisted build lifecycle state (F-50 Step 3a). `Ready` for a
    /// freshly-constructed descriptor; `Building` is set by Step 3b's
    /// `create_index_v2` between the backfill start and the final
    /// `save_index2_metadata`. See `state::IndexState` for why
    /// `#[serde(default)]` alone does NOT provide bincode forward-compat
    /// (the load path in `persistence::load_index2_metadata` does).
    #[serde(default)]
    pub state: IndexState,
}

impl IndexDescriptor {
    pub fn new(
        id: u32,
        name: impl Into<String>,
        name_interned: u64,
        paths: SmallVec<[Vec<u64>; 2]>,
        kind: IndexKind,
    ) -> Self {
        let created_at_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Self {
            id,
            name: name.into(),
            name_interned,
            paths,
            kind,
            created_at_nanos,
            options: Vec::new(),
            state: IndexState::default(),
        }
    }
}
