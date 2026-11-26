//! Commit admission gate (R2a step 1c) — primitive only.
//!
//! One gate per repository, owned next to the commit tail barrier
//! (`TxHorizon::commit_admission`). Design:
//! `docs/dev-artifacts/design/online-backup.md`, section 2 "Commit admission
//! barrier" (settled). Single `AtomicUsize` state word, no `Mutex`/`RwLock`:
//!
//! ```text
//! bit 63      CLOSE_TAKEN — one exclusive RAII close owner
//! bit 62      CLOSED      — admission refused
//! bits 0..62  ACTIVE      — bounded active lease count
//! ```
//!
//! This step ships the primitive, the leader/follower wiring and the close
//! owner. The drain pause guard, the seal and the facade are later steps.

// Close-side API (close owner): consumed by the R2a-1d capture/seal
// barriers (repo/instance/repo_instance/lifecycle/online_backup.rs).
pub mod close_owner;
pub mod error;
pub mod gate;
pub(crate) mod lease;

#[cfg(test)]
pub(crate) mod tests;
