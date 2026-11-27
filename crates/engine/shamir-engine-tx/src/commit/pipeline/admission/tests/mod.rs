//! CI-only tests for the commit admission gate (R2a 1c). Deterministic
//! handshakes only: armed `Notify`s and poll-based pending checks — NO
//! sleeps as correctness evidence. Wiring-level tests live beside them.

#[doc(hidden)]
pub mod close_helpers;
#[doc(hidden)]
pub mod gate_tests;
#[doc(hidden)]
pub mod wait_tests;
