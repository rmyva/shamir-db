//! Unit tests for the `ListActivity` serving path (R4b-1).
//!
//! `continuation_token_tests` — pure wire-format tests: round trip,
//! bounded opaque output, strict validation of every forged/garbled form,
//! and the chain-nonce and section-order invariants.
//! `paging_tests` — the production keyset selection helper
//! (`list::select_window`) with duplicate ids, exact limits and
//! continuation boundaries. No server harness — these exercise the same
//! code the paging worker calls.
//! `refusal_fairness_tests` — one principal cannot occupy the whole
//! refusal-audit queue while another principal is still recorded.
//! `refusal_close_race_tests` — a refusal that reserved a slot before
//! `close()` is never spawned after the discard sweep.
//! `cancel_release_tests` (EXPORT-ONLINE-TAILS round 2) — the cancel reply awaits the record-lock release, and a registry `Drop` release landing after tail admission closed is still not lost.
//! `dispatch_drop_tests` (round 3) — a cancel dispatch dropped while the inline (closed-admission) release is pending still completes it via the drop-guard's detached fallback; the parked waiter wakes.
//! `tx_release_tests` — every end of an interactive transaction (owner rollback, reaper, session close, an in-flight request at session close) releases the Level-3 record locks it held; the release is idempotent and keyed by the transaction's own id.

#[path = "cancellation/busy_reread_tests.rs"]
mod busy_reread_tests;
#[path = "cancellation/cancel_release_tests.rs"]
mod cancel_release_tests;
#[path = "cancellation/tail_tests.rs"]
mod cancellation_tail_tests;
#[path = "cancellation/legacy_tests.rs"]
mod cancellation_tests;
#[path = "cancellation/commit_cleanup_tests.rs"]
mod commit_cleanup_tests;
mod continuation_token_tests;
#[path = "cancellation/cursor_removal_tests.rs"]
mod cursor_removal_tests;
#[path = "cancellation/dispatch_drop_tests.rs"]
mod dispatch_drop_tests;
mod paging_tests;
mod refusal_close_race_tests;
mod refusal_fairness_tests;
mod tx_release_tests;
