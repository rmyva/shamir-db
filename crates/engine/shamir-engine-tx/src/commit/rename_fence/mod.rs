//! Read-side view of a repository's in-doubt rename fence (ENGINE-LAYERING
//! R2c2). The write side (raise/take/clear) stays on the `RepoInstance`
//! shell; this module owns the plain-data entry types and the check the
//! `tx` layer needs to resolve a table name.

mod fence_entry;
mod in_doubt;
mod retained_freeze;
mod view;

pub use fence_entry::RenameFenceEntry;
pub(crate) use in_doubt::RenameInDoubt;
pub(crate) use retained_freeze::RetainedFreeze;
pub use view::RenameFenceView;
