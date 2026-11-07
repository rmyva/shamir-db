//! `IndexBackend` wrapper around any `VectorAdapter`.
//!
//! Extracts the vector field from records, delegates to the adapter
//! for similarity search. Returns `IndexResult::Ranked`.
mod backend;
mod cutover;
mod gate;
mod maintenance;
mod slot;
mod target;
pub use backend::VectorBackend;
#[cfg(test)]
pub(crate) use maintenance::run_background_compaction;
#[cfg(test)]
pub(crate) use slot::AdapterSlot;
#[cfg(test)]
pub(crate) use target::CompactionTarget;
