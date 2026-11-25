//! Wall-clock time helpers shared across the workspace.
//!
//! Every call site that needs "now" as a plain `u64` unix timestamp used to
//! hand-roll `SystemTime::now().duration_since(UNIX_EPOCH)` and either
//! `.unwrap()` (panics if the system clock is set before 1970) or its own
//! ad-hoc `.unwrap_or(0)` fallback. The Unix timestamp helpers centralize
//! that fallback.

use std::time::{SystemTime, UNIX_EPOCH};

/// Current local minute of day (0–1439), using the system's local time zone.
/// Jiff reports these valid civil clock components as signed integers; convert
/// through their unsigned representation before arithmetic.
pub fn local_minute_of_day() -> u32 {
    let now = jiff::Zoned::now();
    u32::from(now.hour() as u8) * 60 + u32::from(now.minute() as u8)
}

/// Wall-clock milliseconds since the UNIX epoch; `0` if the clock is set
/// before 1970 (never panics).
#[inline]
pub fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Wall-clock nanoseconds since the UNIX epoch; `0` if the clock is set
/// before 1970 (never panics).
#[inline]
pub fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
