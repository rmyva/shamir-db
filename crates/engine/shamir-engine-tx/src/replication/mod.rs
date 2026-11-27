// `tx::replication` — follower apply + replicated events (stage E1 regroup).
// Declarations and re-exports only. `follower_durability` stays a child
// module of `apply_replicated` exactly as before.
pub mod apply_replicated;
pub mod replicated_event;

pub use apply_replicated::follower_durability;
