//! R2a step 1d tests (brief item 8): the drain-pause guard, the
//! capture/seal barriers under concurrent commits, the seal hold's
//! store-level exclusion, cancellation at every barrier await point,
//! typed fail-closed refusals, and the caller-owned truncation pin.
//!
//! Deterministic handshakes only: armed oneshot/notify handoffs and
//! poll-based pending checks — NO sleeps as correctness evidence. Every
//! real-time bound below (`*_BUDGET`) is a generous timeout HARNESS so a
//! regression deadlocks into a fast, loud failure instead of hanging CI;
//! it is never the mechanism that makes a test pass.

mod cancellation_tests;
mod capture_seal_tests;
mod fail_closed_tests;
mod pause_guard_tests;
mod pin_tests;
mod seal_hold_tests;
