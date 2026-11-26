//! UTC timestamp formatting for snapshot directory names, split out of
//! `offline.rs` (which would otherwise exceed the 1000-line file budget).

use std::time::{SystemTime, UNIX_EPOCH};

pub fn utc_timestamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    utc_timestamp_at(secs)
}

/// F-68 (#895, cluster D): format an arbitrary unix-epoch-seconds value the
/// same way [`utc_timestamp`] formats "now". Split out so a test can
/// compute the stamp for a KNOWN instant (e.g. "now" and "now + 1s")
/// without racing `SystemTime::now()` a second time — see
/// `tests::restore_tests::preexisting_temp_dir_name_collision_still_reports_already_exists`,
/// which pre-creates a collision directory at BOTH `now` and `now + 1s` so
/// the test is deterministic regardless of which UTC second `restore()`'s
/// own internal `utc_timestamp()` call lands in.
pub fn utc_timestamp_at(secs: u64) -> String {
    let (y, mo, d, h, mi, s) = unix_to_ymd_hms(secs);
    format!("{y:04}{mo:02}{d:02}_{h:02}{mi:02}{s:02}")
}

/// Convert a unix epoch second into (year, month, day, hour, minute, second)
/// in UTC. Algorithm: civil_from_days from Howard Hinnant's date library
/// (public domain). Adapted to Rust ints.
fn unix_to_ymd_hms(secs: u64) -> (i32, u32, u32, u32, u32, u32) {
    let days = (secs / 86_400) as i64;
    let s = (secs % 86_400) as u32;
    let h = s / 3600;
    let mi = (s % 3600) / 60;
    let se = s % 60;

    // Hinnant's civil_from_days, valid for any day in [-12687428, 11248737].
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let mo = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = (if mo <= 2 { y + 1 } else { y }) as i32;
    (y, mo, d, h, mi, se)
}
