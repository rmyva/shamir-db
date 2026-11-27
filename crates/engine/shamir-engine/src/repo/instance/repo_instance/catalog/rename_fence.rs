use super::rename_freeze::RenameFreeze;
use super::RepoInstance;
use crate::tx::commit::rename_fence::{RenameFenceEntry, RenameInDoubt};
use shamir_storage::error::DbResult;
use std::sync::atomic::Ordering;

impl RepoInstance {
    /// Reject access to a name in an unresolved rename pair.
    pub fn check_table_rename_fence(&self, name: &str) -> DbResult<()> {
        self.core.rename_fence().check_table_rename_fence(name)
    }

    pub fn check_rename_pair(&self, from: &str, to: &str) -> DbResult<()> {
        self.core.rename_fence().check_rename_pair(from, to)
    }

    /// Whether this repository has an unresolved rename.
    pub fn has_rename_in_doubt(&self) -> bool {
        self.rename_fence_len.load(Ordering::Acquire) != 0
    }

    pub(super) fn take_rename_fence(&self, from: &str, to: &str) -> Option<RenameInDoubt> {
        self.check_rename_pair(from, to).ok()?;
        self.rename_fence
            .update_sync(from, |_, entry| match entry {
                RenameFenceEntry::Source(pair) if pair.from == from && pair.to == to => {
                    let old = std::mem::replace(
                        entry,
                        RenameFenceEntry::InFlight {
                            from: from.into(),
                            to: to.into(),
                        },
                    );
                    match old {
                        RenameFenceEntry::Source(pair) => Some(*pair),
                        _ => unreachable!("entry replaced while exclusively updated"),
                    }
                }
                _ => None,
            })
            .flatten()
    }

    /// Panicking here is forbidden: the fence can be raised from `Drop`
    /// (via the in-doubt transfer path), so invariant violations must be
    /// logged, not asserted.
    pub(super) fn raise_rename_fence(&self, pair: RenameInDoubt) {
        let from = pair.from.clone();
        let to = pair.to.clone();
        let mut pair = Some(pair);
        let updated = self
            .rename_fence
            .update_sync(&from, |_, entry| match entry {
                RenameFenceEntry::InFlight {
                    from: old_from,
                    to: old_to,
                } if *old_from == from && *old_to == to => match pair.take() {
                    Some(retained) => {
                        *entry = RenameFenceEntry::Source(Box::new(retained));
                        true
                    }
                    None => false,
                },
                _ => false,
            })
            .unwrap_or(false);
        if updated {
            debug_assert!(pair.is_none());
            return;
        }
        let Some(source) = pair.take() else {
            log::error!("rename fence pair missing before insertion for '{from}' to '{to}'");
            return;
        };
        // The write permit and pair checks make a destination collision impossible; never ignore it.
        match self.rename_fence.entry_sync(to.clone()) {
            scc::hash_map::Entry::Occupied(occupied) => {
                // Release-build fallback: the write permit and pair checks
                // make a destination collision impossible; log and continue
                // rather than panicking from `Drop`.
                debug_assert!(
                    matches!(
                        occupied.get(),
                        RenameFenceEntry::Destination { from: old_from } if old_from == &from
                    ),
                    "rename destination name unexpectedly occupied"
                );
                if !matches!(
                    occupied.get(),
                    RenameFenceEntry::Destination { from: old_from } if old_from == &from
                ) {
                    log::error!(
                        "rename fence invariant violated: destination name '{to}' occupied by a foreign entry while raising '{from}' to '{to}'"
                    );
                }
            }
            scc::hash_map::Entry::Vacant(vacant) => {
                vacant.insert_entry(RenameFenceEntry::Destination { from: from.clone() });
            }
        }
        match self.rename_fence.entry_sync(from.clone()) {
            scc::hash_map::Entry::Occupied(_) => {
                debug_assert!(
                    false,
                    "rename source name unexpectedly occupied outside retry"
                );
                log::error!(
                    "rename fence invariant violated: source name '{from}' already present outside retry while raising '{from}' to '{to}'"
                );
                // Keep the source frozen (fail closed) and leave the counter alone.
                match source.freeze.into_inner::<RenameFreeze>() {
                    Ok(freeze) => freeze.retire(),
                    Err(_) => log::error!(
                        "rename fence invariant violated: retained freeze for '{from}' to '{to}' is not a RenameFreeze"
                    ),
                }
                return;
            }
            scc::hash_map::Entry::Vacant(vacant) => {
                vacant.insert_entry(RenameFenceEntry::Source(Box::new(source)));
            }
        }
        self.rename_fence_len.fetch_add(1, Ordering::Release);
    }

    pub(super) fn clear_rename_fence(&self, from: &str, to: &str) {
        let source = self.rename_fence.remove_sync(from);
        let destination = self.rename_fence.remove_sync(to);
        let decrement = source.as_ref().is_some_and(|(_, entry)| {
            matches!(entry,
                RenameFenceEntry::Source(pair) if pair.from == from && pair.to == to
            ) || matches!(entry, RenameFenceEntry::InFlight { from: source, to: destination }
            if source == from && destination == to)
        });
        if decrement {
            self.rename_fence_len.fetch_sub(1, Ordering::AcqRel);
        }
        drop((source, destination));
    }
}
