use super::in_doubt::RenameInDoubt;

/// Name-indexed value for an in-doubt rename pair.
pub enum RenameFenceEntry {
    /// Source entry owns the retained freeze.
    Source(Box<RenameInDoubt>),
    /// Source entry is temporarily held in place while an exact-pair retry runs.
    InFlight { from: String, to: String },
    /// Destination entry points to the source name.
    Destination { from: String },
}
