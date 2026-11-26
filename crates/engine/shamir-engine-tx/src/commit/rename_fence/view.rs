use super::fence_entry::RenameFenceEntry;
use shamir_storage::error::{DbError, DbResult};
use shamir_types::types::common::THasher;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Shared read-side view of the shell's rename fence: the same two `Arc`s the
/// shell owns, so a name check here sees every raise/clear the shell makes.
#[derive(Clone)]
pub struct RenameFenceView {
    map: Arc<scc::HashMap<String, RenameFenceEntry, THasher>>,
    len: Arc<AtomicUsize>,
}

impl RenameFenceView {
    pub fn new(
        map: Arc<scc::HashMap<String, RenameFenceEntry, THasher>>,
        len: Arc<AtomicUsize>,
    ) -> Self {
        Self { map, len }
    }

    /// Reject access to a name in an unresolved rename pair.
    pub fn check_table_rename_fence(&self, name: &str) -> DbResult<()> {
        if self.len.load(Ordering::Acquire) == 0 {
            return Ok(());
        }
        let pair = fence_pair(&self.map, name);
        pair.map_or(Ok(()), |(from, to)| Err(rename_conflict(&from, &to)))
    }

    /// Check source and destination before a rename, allowing a retry of its exact pair.
    pub fn check_rename_pair(&self, from: &str, to: &str) -> DbResult<()> {
        if self.len.load(Ordering::Acquire) == 0 {
            return Ok(());
        }
        for name in [from, to] {
            if let Some((source, destination)) = fence_pair(&self.map, name) {
                if source != from || destination != to {
                    return Err(rename_conflict(&source, &destination));
                }
            }
        }
        Ok(())
    }
}

fn fence_pair(
    fence: &scc::HashMap<String, RenameFenceEntry, impl BuildHasher>,
    name: &str,
) -> Option<(String, String)> {
    fence
        .read_sync(name, |_, entry| match entry {
            RenameFenceEntry::Source(pair) => Some((pair.from.clone(), pair.to.clone())),
            RenameFenceEntry::Destination { from } => Some((from.clone(), name.to_string())),
            RenameFenceEntry::InFlight { from, to } => Some((from.clone(), to.clone())),
        })
        .flatten()
}

fn rename_conflict(from: &str, to: &str) -> DbError {
    DbError::Conflict(format!("rename in doubt: '{from}' to '{to}'"))
}
