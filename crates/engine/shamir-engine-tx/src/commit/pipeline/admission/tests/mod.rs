//! CI-only tests for the commit admission gate (R2a 1c). Deterministic
//! handshakes only: armed `Notify`s and poll-based pending checks — NO
//! sleeps as correctness evidence. Wiring-level tests live beside them.

mod gate_tests;
mod wait_tests;
