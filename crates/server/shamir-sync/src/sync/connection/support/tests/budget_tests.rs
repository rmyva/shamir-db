//! Connection admission budgets: pre-auth frame ceilings, handshake time, and connection caps.

use std::time::Duration;

use shamir_connect::common::types::limits::MAX_PRE_AUTH_FRAME;
use shamir_transport_tcp::framing::MAX_FRAME_SIZE_DEFAULT;

use crate::conn_limiter::ConnLimiter;
use crate::connection::handshake_budget;

// HIGH-1 compile-time invariants: pin the pre-auth frame ceiling at
// the spec §8 value and prove it stays strictly below the post-auth
// ceiling the request loop uses. `const` assertions trip at compile
// time so a future spec edit that weakens either bound fails the
// build, not just the test suite.
const _: () = assert!(
    MAX_PRE_AUTH_FRAME == 4 * 1024,
    "MAX_PRE_AUTH_FRAME must equal 4 KiB per spec §8",
);
const _: () = assert!(
    MAX_PRE_AUTH_FRAME < MAX_FRAME_SIZE_DEFAULT,
    "pre-auth ceiling must be strictly smaller than post-auth ceiling",
);

/// HIGH-1 regression: `run_handshake` must read pre-auth frames with
/// the 4 KiB ceiling, not the post-auth 16 MiB ceiling. The compile-
/// time `const` asserts above pin the constants; this runtime test
/// surfaces a human-readable failure when the bounds are tightened
/// or loosened in the future, and additionally documents the
/// resource-exhaustion budget.
#[test]
fn pre_auth_frame_budget_is_safe_for_ten_thousand_unauth_peers() {
    // Defense-in-depth: 10 000 concurrent unauthenticated peers
    // multiplied by the pre-auth cap must stay well under
    // commodity-server RAM. 10 000 × 4 KiB = 40 MiB, vs. the
    // 10 000 × 16 MiB = ~160 GiB that the old shape allowed.
    let max_unauth_memory = MAX_PRE_AUTH_FRAME.saturating_mul(10_000);
    assert!(
        max_unauth_memory < 128 * 1024 * 1024,
        "pre-auth cap × 10k connections should be under 128 MiB; got {}",
        max_unauth_memory,
    );
}

/// Spec §8 pure formula: `max(15 s, kdf_time × 5)`. The listener default in
/// these tests is time = 2 (floor → 15 s); a stored user with time = 8 must
/// get the full 40 s.
#[test]
fn handshake_budget_is_max_of_15s_and_kdf_time_times_5() {
    assert_eq!(handshake_budget(2), Duration::from_secs(15));
    assert_eq!(handshake_budget(1), Duration::from_secs(15));
    assert_eq!(handshake_budget(0), Duration::from_secs(15));
    assert_eq!(handshake_budget(8), Duration::from_secs(40));
    assert_eq!(handshake_budget(5), Duration::from_secs(25));
}

#[test]
fn capped_connection_samples_count_each_refusal_across_clones() {
    let limiter = ConnLimiter::new(1);
    let clone = limiter.clone();
    assert_eq!(limiter.cap(), 1);
    assert_eq!(limiter.live(), 0);
    assert_eq!(limiter.rejected_total(), 0);

    let guard = limiter.try_acquire().expect("first connection fits");
    assert_eq!(limiter.live(), 1);
    assert_eq!(limiter.rejected_total(), 0);

    assert!(clone.try_acquire().is_none());
    assert_eq!(limiter.rejected_total(), 1);
    assert!(limiter.try_acquire().is_none());
    assert_eq!(clone.rejected_total(), 2);
    assert_eq!(limiter.live(), 1);

    let other = ConnLimiter::new(1);
    assert_eq!(other.rejected_total(), 0);

    drop(guard);
    assert_eq!(limiter.live(), 0);
    assert_eq!(limiter.rejected_total(), 2);
}

#[test]
fn unlimited_connection_samples_track_live_without_refusals() {
    let limiter = ConnLimiter::new(0);
    assert_eq!(limiter.cap(), 0);
    assert_eq!(limiter.live(), 0);
    assert_eq!(limiter.rejected_total(), 0);

    let first = limiter.try_acquire().expect("first unlimited connection");
    let second = limiter.try_acquire().expect("second unlimited connection");
    assert_eq!(limiter.live(), 2);
    assert_eq!(limiter.rejected_total(), 0);

    drop(first);
    drop(second);
    assert_eq!(limiter.live(), 0);
    assert_eq!(limiter.rejected_total(), 0);
}
