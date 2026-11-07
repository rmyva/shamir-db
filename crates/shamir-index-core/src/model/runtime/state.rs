//! Persisted lifecycle state of an [`crate::IndexDescriptor`].
//!
//! F-50 Step 3a (#871 spike). `Building` marks an index whose backfill was
//! interrupted (crash between the Building persist point and the final
//! `Ready` persist); `Ready` marks a fully-built, queryable index. A
//! freshly-constructed descriptor is always `Ready` (the current
//! `create_index_v2` backfills-then-registers, so a backend is effectively
//! ready the moment it appears — see the Step 1 memo §1.2).
//!
//! # Unified per-family lifecycle contract (F-72 + F-76)
//!
//! The full `Ok`/`Err`/crash contract for every index family × transition
//! (CREATE / DROP / RENAME) — including the conceptual
//! `Absent → Building → Ready → Dropping → Absent` state machine and the
//! explicit deferred follow-up gaps — lives in [`crate::lifecycle`]. Refer
//! there to answer "what does `Err` mean for family X transition Y".
//!
//! # Serialization / forward-compat (PROVEN, not assumed)
//!
//! bincode 1.3.3 (this workspace's pinned version) is a positional,
//! non-self-describing format: `#[serde(default)]` on a NEW trailing field
//! does NOT rescue a read of OLD bytes that predate the field — the read
//! fails with `io error: unexpected end of file` (`ErrorKind::Io(
//! UnexpectedEof)`). This was empirically proven for THIS struct's exact
//! shape by the F-50 Step 3a round-trip test
//! (`tests/index_state_compat_tests.rs`) — the same failure the
//! `VectorConfig::quantization` doc comment (`kind.rs`) flagged for
//! `#[serde(skip)]` now confirmed for `#[serde(default)]`.
//!
//! Forward-compat is therefore provided NOT by `#[serde(default)]` but by
//! the LOAD path: [`crate::persistence::load_index2_metadata`] first
//! attempts the current (with-`state`) shape and, on a decode failure,
//! falls back to a legacy pre-`state` shadow shape, lifting each legacy
//! descriptor to `state = Ready` (every pre-`state` on-disk index was
//! fully built — a `Building` index could never have been persisted before
//! this field existed). The `#[serde(default)]` attribute below is kept
//! for JSON/msgpack friendliness and consistency with `options`; it is a
//! no-op for the bincode path.

use serde::{Deserialize, Serialize};

/// Persisted lifecycle state of an index2 backend.
///
/// Minimal two-variant form for the F-50 Step 3a spike, extended with
/// `Failed` by R0-D (#1013): any new variant is an additive,
/// backward-compatible enum change (bincode tags enum variants by ordinal,
/// so appending is safe — only a re-order is breaking).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum IndexState {
    /// Fully built and queryable. The default: every pre-`state` on-disk
    /// descriptor and every freshly-constructed descriptor is `Ready`.
    #[default]
    Ready,
    /// Backfill started but not confirmed complete (the final `Ready`
    /// persist has not run). On restart a `Building` index MUST NOT be
    /// served by the planner and MUST be reconciled (restart-from-scratch
    /// — see the F-50 Step 3a memo).
    Building,
    /// R0-D (#1013): open-path recovery for this backend was attempted and
    /// genuinely failed (a `drop_all` during the Building self-heal errored,
    /// or `restore_on_open` errored). Set INSTEAD of leaving the backend at
    /// whatever state it had before the failed recovery attempt — fail
    /// CLOSED rather than silently serving a half-initialised or
    /// possibly-empty backend as if it were `Ready`. Like `Building`, a
    /// `Failed` backend MUST NOT be served by the planner (every
    /// `state != IndexState::Ready` / `state == IndexState::Ready` gate
    /// already excludes it without modification — see
    /// `IndexRegistry::find_by_field_and_kind` and
    /// `SortedIndexManager::find_by_field_ready`) and IS counted by
    /// `degraded_index_count()` (which also gates on `!= Ready`). Recovery
    /// from `Failed` is manual today: an operator runs `doctor::repair()`
    /// (or reopens the table after fixing the underlying storage fault).
    Failed,
}
