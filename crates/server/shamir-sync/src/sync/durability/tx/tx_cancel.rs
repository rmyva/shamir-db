//! Operator cancellation of a parked interactive transaction (R4b step 3).
//!
//! One lookup and one removal, each decided under the authoritative `open`
//! map's own shard guard: a handle is never removed on the strength of an
//! earlier read, so a handle re-minted for a DIFFERENT transaction between
//! the authorization and the removal is left alone.

use std::sync::Arc;

use dashmap::mapref::entry::Entry;

use super::tx_registry::{InteractiveTx, Parked, TxRegistry};

/// How a handle stands in the authoritative `open` map at one lookup.
/// Returned by both [`TxRegistry::peek`] and
/// [`TxRegistry::remove_identity`]; on removal `Open` carries the
/// transaction that was removed.
pub enum TxParkedState {
    /// Parked and executable (or, from a removal, removed and returned).
    Open(Arc<InteractiveTx>),
    /// The commit pipeline owns the handle: `begin_commit` reserved it and
    /// only its `TxCommitGuard` finishes the teardown.
    Committing(Arc<InteractiveTx>),
    /// No such handle (never minted, already removed, or replaced by a
    /// different transaction this call did not authorize).
    Gone,
}

impl TxRegistry {
    /// One lock-free lookup of the parked transaction under `handle`.
    /// Unlike [`TxRegistry::get_owned`] this needs no owning session id: the
    /// operator-cancel path authorizes the transaction it finds.
    pub fn peek(&self, handle: u64) -> TxParkedState {
        match self.open.get(&handle) {
            Some(entry) => match &*entry {
                Parked::Open(tx) => TxParkedState::Open(Arc::clone(tx)),
                Parked::Committing(tx) => TxParkedState::Committing(Arc::clone(tx)),
            },
            None => TxParkedState::Gone,
        }
    }

    /// Identity-checked removal for the operator-cancel path: remove
    /// `handle` only if it still parks the very transaction the caller
    /// authorized (`Arc::ptr_eq`, the same discipline as the reaper and
    /// [`TxRegistry::remove_owned`]). The whole decision — variant, identity
    /// and teardown — happens under the `open` entry's shard guard, so the
    /// reported state and the removal cannot disagree.
    pub fn remove_identity(&self, handle: u64, expected: &Arc<InteractiveTx>) -> TxParkedState {
        match self.open.entry(handle) {
            Entry::Occupied(slot) => match slot.get() {
                Parked::Open(tx) if Arc::ptr_eq(tx, expected) => {
                    TxParkedState::Open(self.teardown_open(slot, handle))
                }
                Parked::Open(_) => TxParkedState::Gone,
                Parked::Committing(tx) if Arc::ptr_eq(tx, expected) => {
                    TxParkedState::Committing(Arc::clone(tx))
                }
                Parked::Committing(_) => TxParkedState::Gone,
            },
            Entry::Vacant(_) => TxParkedState::Gone,
        }
    }
}
