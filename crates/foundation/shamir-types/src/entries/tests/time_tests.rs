use crate::time::{unix_millis, unix_nanos};

#[test]
fn system_time_zone_is_resolvable_not_utc_fallback() {
    // If the OS zone cannot be mapped, TimeZone::system() silently falls back
    // to Etc/Unknown (UTC-like) and local_minute_of_day() would schedule in
    // UTC on non-UTC Windows hosts. try_system() fails exactly in that case,
    // so this test fails on any runner where the fallback would trigger.
    assert!(jiff::tz::TimeZone::try_system().is_ok());
}

#[test]
fn unix_millis_is_plausible_wall_clock() {
    // 2023-11-14T22:13:20Z in ms — any correct clock reads well past this.
    assert!(unix_millis() > 1_700_000_000_000);
}

#[test]
fn unix_nanos_agrees_with_unix_millis() {
    let ms = unix_millis();
    let ns = unix_nanos();
    let ns_as_ms = ns / 1_000_000;
    let diff = ns_as_ms.abs_diff(ms);
    assert!(
        diff <= 1000,
        "unix_nanos() and unix_millis() diverged by {diff}ms (ns={ns}, ms={ms})"
    );
}
