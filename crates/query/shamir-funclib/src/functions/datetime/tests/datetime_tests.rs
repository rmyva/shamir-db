//! Per-function `/datetime` tests — at least one correct-result assert and one
//! error/edge case per registered function. Expected values are derived from
//! `chrono` directly so the asserts do not duplicate hand-computed constants.

use crate::datetime;
use crate::registry::{arg_i64, arg_str, v_bool, v_int, v_str, ScalarRegistry};
use chrono::format::{Item, StrftimeItems};
use chrono::{
    DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc, Weekday,
};
use shamir_types::types::value::QueryValue;

fn reg() -> ScalarRegistry {
    let mut r = ScalarRegistry::new();
    datetime::register(&mut r);
    r
}

/// 2021-06-02T15:30:45Z — a Wednesday — as canonical epoch-millis.
const WED_MS: i64 = 1_622_647_845_000;

fn ts(ms: i64) -> QueryValue {
    QueryValue::Int(ms)
}

fn millis_of(s: &str) -> i64 {
    DateTime::parse_from_rfc3339(s)
        .unwrap()
        .with_timezone(&Utc)
        .timestamp_millis()
}

#[test]
fn local_minute_of_day_matches_chrono_local() {
    let before = chrono::Local::now();
    let before_minute = before.hour() * 60 + before.minute();
    let actual = shamir_types::time::local_minute_of_day();
    let after = chrono::Local::now();
    let after_minute = after.hour() * 60 + after.minute();

    assert!(
        actual == before_minute || actual == after_minute,
        "actual {actual} did not match chrono minutes before ({before_minute}) or after ({after_minute})",
    );
}

#[test]
fn now_is_nondeterministic_metadata() {
    let r = reg();
    // metadata: now is impure + non-deterministic.
    let e = r.get("now").unwrap();
    assert!(!e.pure);
    assert!(!e.deterministic);
    // result is a positive epoch-millis Int.
    match r.call("now", &[]).unwrap() {
        QueryValue::Int(n) => assert!(n > 1_600_000_000_000),
        other => panic!("expected Int, got {other:?}"),
    }
    // error: arity (now takes no args).
    assert_eq!(r.call("now", &[ts(0)]).unwrap_err().code, "arity");
}

#[test]
fn age_is_nondeterministic_and_nonnegative() {
    let r = reg();
    let e = r.get("age").unwrap();
    assert!(!e.pure);
    assert!(!e.deterministic);
    // age of a long-past instant is positive seconds.
    match r.call("age", &[ts(WED_MS)]).unwrap() {
        QueryValue::Int(n) => assert!(n > 0),
        other => panic!("expected Int, got {other:?}"),
    }
    // error: wrong type.
    assert_eq!(
        r.call("age", &[QueryValue::Str("x".into())])
            .unwrap_err()
            .code,
        "type_mismatch"
    );
}

#[test]
fn epoch_conversions() {
    let r = reg();
    assert_eq!(
        r.call("to_epoch_s", &[ts(WED_MS)]).unwrap(),
        v_int(1_622_647_845)
    );
    assert_eq!(r.call("to_epoch_ms", &[ts(WED_MS)]).unwrap(), v_int(WED_MS));
    assert_eq!(
        r.call("from_epoch_s", &[ts(1_622_647_845)]).unwrap(),
        v_int(WED_MS)
    );
    assert_eq!(
        r.call("from_epoch_ms", &[ts(WED_MS)]).unwrap(),
        v_int(WED_MS)
    );
    // floor division for negative millis (200ms before epoch -> -1s).
    assert_eq!(r.call("to_epoch_s", &[ts(-200)]).unwrap(), v_int(-1));
    // error: from_epoch_s overflow.
    assert_eq!(
        r.call("from_epoch_s", &[ts(i64::MAX)]).unwrap_err().code,
        "out_of_range"
    );
}

#[test]
fn rfc3339_roundtrip() {
    let r = reg();
    let ms = r
        .call(
            "parse_rfc3339",
            &[v_str("2021-06-02T15:30:45+00:00".into())],
        )
        .unwrap();
    assert_eq!(ms, v_int(WED_MS));
    let s = r.call("format_rfc3339", &[ts(WED_MS)]).unwrap();
    // re-parse the formatted string and confirm it round-trips.
    match s {
        QueryValue::Str(text) => assert_eq!(millis_of(&text), WED_MS),
        other => panic!("expected Str, got {other:?}"),
    }
    // error: unparseable string.
    assert_eq!(
        r.call("parse_rfc3339", &[v_str("not-a-date".into())])
            .unwrap_err()
            .code,
        "parse"
    );
}

#[test]
fn components() {
    let r = reg();
    assert_eq!(r.call("year", &[ts(WED_MS)]).unwrap(), v_int(2021));
    assert_eq!(r.call("month", &[ts(WED_MS)]).unwrap(), v_int(6));
    assert_eq!(r.call("day", &[ts(WED_MS)]).unwrap(), v_int(2));
    assert_eq!(r.call("hour", &[ts(WED_MS)]).unwrap(), v_int(15));
    assert_eq!(r.call("minute", &[ts(WED_MS)]).unwrap(), v_int(30));
    assert_eq!(r.call("second", &[ts(WED_MS)]).unwrap(), v_int(45));
    // error: out-of-range timestamp rejected by extractor.
    assert_eq!(
        r.call("year", &[ts(i64::MAX)]).unwrap_err().code,
        "out_of_range"
    );
}

#[test]
fn weekday_and_weekend() {
    let r = reg();
    // Wednesday -> num_days_from_monday == 2.
    assert_eq!(r.call("weekday", &[ts(WED_MS)]).unwrap(), v_int(2));
    assert_eq!(r.call("is_weekend", &[ts(WED_MS)]).unwrap(), v_bool(false));
    // Saturday 2021-06-05T12:00:00Z is a weekend.
    let sat = millis_of("2021-06-05T12:00:00+00:00");
    assert_eq!(r.call("weekday", &[ts(sat)]).unwrap(), v_int(5));
    assert_eq!(r.call("is_weekend", &[ts(sat)]).unwrap(), v_bool(true));
    // error: wrong type for weekday.
    assert_eq!(
        r.call("is_weekend", &[QueryValue::Str("x".into())])
            .unwrap_err()
            .code,
        "type_mismatch"
    );
}

#[test]
fn arithmetic() {
    let r = reg();
    assert_eq!(
        r.call("add_secs", &[ts(WED_MS), ts(60)]).unwrap(),
        v_int(WED_MS + 60_000)
    );
    assert_eq!(
        r.call("add_days", &[ts(WED_MS), ts(1)]).unwrap(),
        v_int(WED_MS + 86_400_000)
    );
    assert_eq!(
        r.call("diff_secs", &[ts(WED_MS + 5_000), ts(WED_MS)])
            .unwrap(),
        v_int(5)
    );
    // error: add_days overflow.
    assert_eq!(
        r.call("add_days", &[ts(WED_MS), ts(i64::MAX)])
            .unwrap_err()
            .code,
        "out_of_range"
    );
}

#[test]
fn diff_secs_overflow_returns_error() {
    let r = reg();
    // i64::MAX - i64::MIN overflows i64 — must return "out_of_range", not panic.
    assert_eq!(
        r.call("diff_secs", &[ts(i64::MAX), ts(i64::MIN)])
            .unwrap_err()
            .code,
        "out_of_range"
    );
    // Reverse direction also overflows.
    assert_eq!(
        r.call("diff_secs", &[ts(i64::MIN), ts(i64::MAX)])
            .unwrap_err()
            .code,
        "out_of_range"
    );
}

#[test]
fn period_starts() {
    let r = reg();
    assert_eq!(
        r.call("start_of_day", &[ts(WED_MS)]).unwrap(),
        v_int(millis_of("2021-06-02T00:00:00+00:00"))
    );
    // week starts on Monday 2021-05-31.
    assert_eq!(
        r.call("start_of_week", &[ts(WED_MS)]).unwrap(),
        v_int(millis_of("2021-05-31T00:00:00+00:00"))
    );
    assert_eq!(
        r.call("start_of_month", &[ts(WED_MS)]).unwrap(),
        v_int(millis_of("2021-06-01T00:00:00+00:00"))
    );
    // error: out-of-range input.
    assert_eq!(
        r.call("start_of_day", &[ts(i64::MAX)]).unwrap_err().code,
        "out_of_range"
    );
}

#[test]
fn truncate_units() {
    let r = reg();
    assert_eq!(
        r.call("truncate", &[ts(WED_MS + 123), v_str("second".into())])
            .unwrap(),
        v_int(WED_MS)
    );
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("minute".into())])
            .unwrap(),
        v_int(millis_of("2021-06-02T15:30:00+00:00"))
    );
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("hour".into())])
            .unwrap(),
        v_int(millis_of("2021-06-02T15:00:00+00:00"))
    );
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("day".into())])
            .unwrap(),
        v_int(millis_of("2021-06-02T00:00:00+00:00"))
    );
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("week".into())])
            .unwrap(),
        v_int(millis_of("2021-05-31T00:00:00+00:00"))
    );
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("month".into())])
            .unwrap(),
        v_int(millis_of("2021-06-01T00:00:00+00:00"))
    );
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("year".into())])
            .unwrap(),
        v_int(millis_of("2021-01-01T00:00:00+00:00"))
    );
    // error: unknown unit.
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("fortnight".into())])
            .unwrap_err()
            .code,
        "bad_unit"
    );
}

/// Sanity: ensure the canonical constant matches its RFC-3339 spelling so the
/// other asserts rest on a correct baseline.
#[test]
fn baseline_constant_is_consistent() {
    assert_eq!(
        WED_MS,
        Utc.timestamp_opt(1_622_647_845, 0)
            .unwrap()
            .timestamp_millis()
    );
}

// ============================================================================
// strftime-pattern format / parse
// ============================================================================

#[test]
fn format_with_pattern() {
    let r = reg();
    // 2021-06-02T15:30:45Z → "%Y-%m-%d" = "2021-06-02".
    assert_eq!(
        r.call("format", &[ts(WED_MS), v_str("%Y-%m-%d".into())])
            .unwrap(),
        v_str("2021-06-02".into())
    );
    // Full date+time pattern.
    assert_eq!(
        r.call("format", &[ts(WED_MS), v_str("%Y-%m-%d %H:%M:%S".into())])
            .unwrap(),
        v_str("2021-06-02 15:30:45".into())
    );
}

#[test]
fn format_with_malformed_pattern_returns_err_not_panic() {
    // CRITICAL: chrono's `DateTime::format(pattern)` returns a LAZY
    // `DelayedFormat` that only interprets specifiers when displayed. An
    // unknown specifier like `%Q` causes a PANIC at `.to_string()` time, not
    // an `Err`. The `validate_pattern` pre-check (using `StrftimeItems`)
    // converts this panic into a clean `ScalarError("parse")`.
    //
    // This test would FAIL BY PANICKING (crashing the test binary) if the
    // `StrftimeItems` pre-validation step were removed — proving the
    // footgun-prevention works.
    let r = reg();
    // %Q is not a valid chrono strftime specifier.
    assert_eq!(
        r.call("format", &[ts(WED_MS), v_str("%Y-%Q-%d".into())])
            .unwrap_err()
            .code,
        "parse"
    );
    // %J is also not a valid chrono specifier (distinct from %q = quarter).
    assert_eq!(
        r.call("format", &[ts(WED_MS), v_str("%J".into())])
            .unwrap_err()
            .code,
        "parse"
    );
}

#[test]
fn parse_date_only_pattern() {
    let r = reg();
    // "2024-03-15" with "%Y-%m-%d" → midnight UTC epoch-millis.
    let result = r
        .call(
            "parse",
            &[v_str("2024-03-15".into()), v_str("%Y-%m-%d".into())],
        )
        .unwrap();
    let expected = Utc
        .with_ymd_and_hms(2024, 3, 15, 0, 0, 0)
        .unwrap()
        .timestamp_millis();
    assert_eq!(result, v_int(expected));
}

#[test]
fn parse_datetime_pattern() {
    let r = reg();
    // Full date+time pattern with time component.
    let result = r
        .call(
            "parse",
            &[
                v_str("2024-03-15 10:30:00".into()),
                v_str("%Y-%m-%d %H:%M:%S".into()),
            ],
        )
        .unwrap();
    let expected = Utc
        .with_ymd_and_hms(2024, 3, 15, 10, 30, 0)
        .unwrap()
        .timestamp_millis();
    assert_eq!(result, v_int(expected));
}

#[test]
fn parse_mismatched_input_returns_err() {
    let r = reg();
    // Input doesn't match pattern → Err, not panic or garbage.
    assert_eq!(
        r.call(
            "parse",
            &[v_str("not-a-date".into()), v_str("%Y-%m-%d".into())]
        )
        .unwrap_err()
        .code,
        "parse"
    );
    // Right shape, wrong values (month 13).
    assert_eq!(
        r.call(
            "parse",
            &[v_str("2024-13-01".into()), v_str("%Y-%m-%d".into())]
        )
        .unwrap_err()
        .code,
        "parse"
    );
}

#[test]
fn parse_malformed_pattern_returns_err_not_panic() {
    // Same footgun-prevention as format: a bad pattern must not panic.
    let r = reg();
    assert_eq!(
        r.call(
            "parse",
            &[v_str("2024-03-15".into()), v_str("%Y-%Q-%d".into())]
        )
        .unwrap_err()
        .code,
        "parse"
    );
}

#[test]
fn format_parse_roundtrip() {
    let r = reg();
    // Round-trip through a second-granularity pattern. The pattern has no
    // sub-second precision, so we use a timestamp with zero milliseconds
    // (WED_MS = 1_622_647_845_000, which is exactly on a second boundary).
    let pattern = "%Y-%m-%d %H:%M:%S";
    let formatted = r
        .call("format", &[ts(WED_MS), v_str(pattern.into())])
        .unwrap();
    let reparsed = r
        .call("parse", &[formatted, v_str(pattern.into())])
        .unwrap();
    assert_eq!(reparsed, v_int(WED_MS));
}

// Self-contained copy of the pre-migration chrono implementation, kept only
// for differential tests. Do not call production helpers from this oracle.
fn chrono_reference(name: &str, args: &[QueryValue]) -> Result<QueryValue, String> {
    let dt = |index| -> Result<DateTime<Utc>, String> {
        match Utc.timestamp_millis_opt(arg_i64(args, index).map_err(|e| e.code)?) {
            chrono::LocalResult::Single(value) => Ok(value),
            _ => Err("out_of_range".into()),
        }
    };
    let parse_pattern = |input: &str, pattern: &str| -> Result<NaiveDateTime, String> {
        if StrftimeItems::new(pattern).any(|item| matches!(item, Item::Error)) {
            return Err("parse".into());
        }
        NaiveDateTime::parse_from_str(input, pattern)
            .or_else(|_| {
                NaiveDate::parse_from_str(input, pattern)
                    .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
            })
            .map_err(|_| "parse".into())
    };
    let start_day = |value: DateTime<Utc>| -> Result<i64, String> {
        value
            .with_hour(0)
            .and_then(|x| x.with_minute(0))
            .and_then(|x| x.with_second(0))
            .and_then(|x| x.with_nanosecond(0))
            .map(|x| x.timestamp_millis())
            .ok_or_else(|| "out_of_range".into())
    };
    match name {
        "now" => Ok(v_int(Utc::now().timestamp_millis())),
        "age" => {
            let elapsed = Utc::now()
                .timestamp_millis()
                .checked_sub(arg_i64(args, 0).map_err(|e| e.code)?)
                .ok_or("out_of_range")?;
            Ok(v_int(elapsed / 1000))
        }
        "to_epoch_s" => Ok(v_int(
            arg_i64(args, 0).map_err(|e| e.code)?.div_euclid(1000),
        )),
        "to_epoch_ms" | "from_epoch_ms" => Ok(v_int(arg_i64(args, 0).map_err(|e| e.code)?)),
        "from_epoch_s" => arg_i64(args, 0)
            .map_err(|e| e.code)?
            .checked_mul(1000)
            .map(v_int)
            .ok_or_else(|| "out_of_range".into()),
        "add_secs" | "add_days" => {
            let value = dt(0)?;
            let amount = arg_i64(args, 1).map_err(|e| e.code)?;
            let delta = if name == "add_secs" {
                Duration::try_seconds(amount)
            } else {
                Duration::try_days(amount)
            }
            .ok_or("out_of_range")?;
            value
                .checked_add_signed(delta)
                .map(|result| v_int(result.timestamp_millis()))
                .ok_or_else(|| "out_of_range".into())
        }
        "diff_secs" => {
            let difference = arg_i64(args, 0)
                .map_err(|e| e.code)?
                .checked_sub(arg_i64(args, 1).map_err(|e| e.code)?)
                .ok_or("out_of_range")?;
            Ok(v_int(difference.div_euclid(1000)))
        }
        "year" => Ok(v_int(dt(0)?.year() as i64)),
        "month" => Ok(v_int(dt(0)?.month() as i64)),
        "day" => Ok(v_int(dt(0)?.day() as i64)),
        "hour" => Ok(v_int(dt(0)?.hour() as i64)),
        "minute" => Ok(v_int(dt(0)?.minute() as i64)),
        "second" => Ok(v_int(dt(0)?.second() as i64)),
        "weekday" => Ok(v_int(dt(0)?.weekday().num_days_from_monday() as i64)),
        "is_weekend" => Ok(v_bool(matches!(
            dt(0)?.weekday(),
            Weekday::Sat | Weekday::Sun
        ))),
        "format_rfc3339" => Ok(v_str(dt(0)?.to_rfc3339())),
        "parse_rfc3339" => DateTime::parse_from_rfc3339(arg_str(args, 0).map_err(|e| e.code)?)
            .map(|x| v_int(x.with_timezone(&Utc).timestamp_millis()))
            .map_err(|_| "parse".into()),
        "format" => {
            let value = dt(0)?;
            let pattern = arg_str(args, 1).map_err(|e| e.code)?;
            if StrftimeItems::new(pattern).any(|item| matches!(item, Item::Error)) {
                return Err("parse".into());
            }
            Ok(v_str(value.format(pattern).to_string()))
        }
        "parse" => {
            let value = parse_pattern(
                arg_str(args, 0).map_err(|e| e.code)?,
                arg_str(args, 1).map_err(|e| e.code)?,
            )?;
            Ok(v_int(value.and_utc().timestamp_millis()))
        }
        "start_of_day" => Ok(v_int(start_day(dt(0)?)?)),
        "start_of_week" => {
            let value = dt(0)?;
            let monday = value
                .checked_sub_signed(Duration::days(value.weekday().num_days_from_monday() as i64))
                .ok_or("out_of_range")?;
            Ok(v_int(start_day(monday)?))
        }
        "start_of_month" => {
            let value = dt(0)?.with_day(1).ok_or("out_of_range")?;
            Ok(v_int(start_day(value)?))
        }
        "truncate" => {
            let value = dt(0)?;
            let unit = arg_str(args, 1).map_err(|e| e.code)?;
            let millis = match unit {
                "second" => value.timestamp_millis() - value.timestamp_millis().rem_euclid(1000),
                "minute" => value
                    .with_second(0)
                    .and_then(|x| x.with_nanosecond(0))
                    .map(|x| x.timestamp_millis())
                    .ok_or("out_of_range")?,
                "hour" => value
                    .with_minute(0)
                    .and_then(|x| x.with_second(0))
                    .and_then(|x| x.with_nanosecond(0))
                    .map(|x| x.timestamp_millis())
                    .ok_or("out_of_range")?,
                "day" => start_day(value)?,
                "week" => {
                    let monday = value
                        .checked_sub_signed(Duration::days(
                            value.weekday().num_days_from_monday() as i64
                        ))
                        .ok_or("out_of_range")?;
                    start_day(monday)?
                }
                "month" => {
                    let first = value.with_day(1).ok_or("out_of_range")?;
                    start_day(first)?
                }
                "year" => {
                    let first = value
                        .with_month(1)
                        .and_then(|x| x.with_day(1))
                        .ok_or("out_of_range")?;
                    start_day(first)?
                }
                _ => return Err("bad_unit".into()),
            };
            Ok(v_int(millis))
        }
        _ => Err("unsupported oracle operation".into()),
    }
}

fn datetime_parity_mismatch(name: &str, args: &[QueryValue]) -> Option<String> {
    let actual = reg().call(name, args).map_err(|error| error.code);
    let expected = chrono_reference(name, args);
    (actual != expected)
        .then(|| format!("{name}({args:?}): actual={actual:?}, chrono={expected:?}"))
}

#[test]
fn chrono_differential_boundary_grid_and_seeded_sweep() {
    let mut mismatches = Vec::new();
    macro_rules! assert_datetime_parity {
        ($name:expr, $args:expr) => {
            if let Some(mismatch) = datetime_parity_mismatch($name, $args) {
                mismatches.push(mismatch);
            }
        };
    }
    let mut values = vec![
        i64::MIN,
        i64::MIN + 1,
        i64::MAX,
        i64::MAX - 1,
        -1,
        0,
        1,
        -1001,
        -1000,
        -999,
        999,
        1000,
        1001,
    ];
    // Derive chrono's inclusive civil-date endpoints from its own constants;
    // timestamp_millis_opt then establishes the exact accepted millisecond edge.
    for date in [NaiveDate::MIN, NaiveDate::MAX] {
        let midnight = date
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();
        let last_second = date
            .and_hms_opt(23, 59, 59)
            .unwrap()
            .and_utc()
            .timestamp_millis();
        values.extend([
            midnight.saturating_sub(1),
            midnight,
            midnight.saturating_add(1),
        ]);
        values.extend([
            last_second.saturating_sub(1),
            last_second,
            last_second.saturating_add(999),
        ]);
        if date == NaiveDate::MAX {
            values.push(last_second.saturating_add(1000));
        }
    }
    for year in [
        -9999, -400, -100, -4, -1, 0, 1, 4, 100, 400, 999, 1900, 2000, 2100, 9999,
    ] {
        for date in [
            (1, 1),
            (1, 31),
            (2, 28),
            (2, 29),
            (3, 1),
            (12, 30),
            (12, 31),
        ] {
            if let Some(value) = Utc
                .with_ymd_and_hms(year, date.0, date.1, 23, 59, 59)
                .single()
            {
                let ms = value.timestamp_millis();
                values.extend([ms - 1, ms, ms + 1, ms + 999]);
            }
        }
    }
    let mut seed = 0x6a09_e667_f3bc_c909u64;
    for _ in 0..512 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        values.push((seed as i64) % 8_000_000_000_000_000);
    }
    values.sort_unstable();
    values.dedup();
    let operations = [
        "to_epoch_s",
        "to_epoch_ms",
        "from_epoch_ms",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "weekday",
        "is_weekend",
        "format_rfc3339",
        "start_of_day",
        "start_of_week",
        "start_of_month",
    ];
    for millis in values.iter().copied() {
        for operation in operations {
            assert_datetime_parity!(operation, &[ts(millis)]);
        }
        assert_datetime_parity!("from_epoch_s", &[ts(millis)]);
        for delta in [-86_401, -1, 0, 1, 86_401, i64::MIN, i64::MAX] {
            assert_datetime_parity!("add_secs", &[ts(millis), ts(delta)]);
            assert_datetime_parity!("add_days", &[ts(millis), ts(delta)]);
            assert_datetime_parity!("diff_secs", &[ts(millis), ts(delta)]);
        }
        for unit in [
            "second",
            "minute",
            "hour",
            "day",
            "week",
            "month",
            "year",
            "fortnight",
        ] {
            assert_datetime_parity!("truncate", &[ts(millis), v_str(unit.into())]);
        }
    }
    // All chrono strftime conversions (including modifiers) plus invalid and truncated directives.
    let patterns = [
        "%a", "%A", "%b", "%B", "%c", "%C", "%d", "%D", "%e", "%F", "%G", "%g", "%h", "%H", "%I",
        "%j", "%k", "%l", "%m", "%M", "%n", "%p", "%P", "%q", "%r", "%R", "%s", "%S", "%t", "%T",
        "%u", "%U", "%V", "%v", "%w", "%W", "%x", "%X", "%y", "%Y", "%z", "%:z", "%::z", "%:::z",
        "%Z", "%+", "%%", "%E4Y", "%Od", "%_d", "%-d", "%0d", "%^B", "%#c", "%f", "%6f", "%9f",
        "%.f", "%.3f", "%.6f", "%.9f", "%3f", "%-f", "%_f", "%-G", "%_g", "%Q", "%J", "%", "%E",
        "%O",
    ];
    for pattern in patterns {
        assert_datetime_parity!("format", &[ts(1_622_647_845_123), v_str(pattern.into())]);
    }
    for (input, pattern) in [
        ("2021-06-02", "%Y-%m-%d"),
        ("2021-06-02 15:30:45", "%Y-%m-%d %H:%M:%S"),
        ("Wed Jun  2 15:30:45 2021", "%a %b %e %T %Y"),
        ("2021-06-02T15:30:45Z", "%Y-%m-%dT%H:%M:%S%#z"),
        ("not-a-date", "%Y-%m-%d"),
        ("2021-13-02", "%Y-%m-%d"),
    ] {
        assert_datetime_parity!("parse", &[v_str(input.into()), v_str(pattern.into())]);
    }
    for input in [
        "2021-06-02T15:30:45Z",
        "2021-06-02T15:30:45.1Z",
        "2021-06-02T15:30:45.12Z",
        "2021-06-02T15:30:45.123Z",
        "2021-06-02T15:30:45.123456Z",
        "2021-06-02T15:30:45.123456789Z",
        "2021-06-02T15:30:45.1234567890Z",
        "2021-06-02T15:30:45.1+02:30",
        "2021-06-02T15:30:45.12+02:30",
        "2021-06-02T15:30:45.123+02:30",
        "2021-06-02T15:30:45.123456+02:30",
        "2021-06-02T15:30:45.123456789+02:30",
        "2021-06-02T15:30:45.1234567890+02:30",
        "2021-06-02T15:30:45.1−05:00",
        "2021-06-02T15:30:45.12−05:00",
        "2021-06-02T15:30:45.123−05:00",
        "2021-06-02T15:30:45.123456−05:00",
        "2021-06-02T15:30:45.123456789−05:00",
        "2021-06-02T15:30:45.1234567890−05:00",
        "2021-06-02T15:30:45.Z",
        "2021-06-02T15:30:45.",
        "1969-12-31T23:59:59.9Z",
        "2021-06-02T15:30:45+",
        "2021-06-02T15:30:45−",
        "2021-06-02T15:30:45+0",
        "2021-06-02T15:30:45+02:30",
        "2021-06-02T15:30:45-07:00",
        "2021-06-02t15:30:45z",
        "2016-12-31T23:59:60Z",
        "2021-06-02T15:30:45",
        "2021-06-02T15:30:45+25:00",
        "not-a-date",
    ] {
        assert_datetime_parity!("parse_rfc3339", &[v_str(input.into())]);
    }
    for (name, args) in [
        ("year", vec![v_str("x".into())]),
        ("to_epoch_s", vec![v_str("x".into())]),
        ("format", vec![ts(0), ts(1)]),
        ("parse", vec![v_str("x".into()), ts(0)]),
        ("add_secs", vec![ts(0), v_str("1".into())]),
        ("truncate", vec![ts(0), ts(1)]),
    ] {
        assert_datetime_parity!(name, &args);
    }
    assert!(
        mismatches.is_empty(),
        "{} chrono differential mismatches:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn chrono_differential_wall_clock_bounds() {
    let before_now = Utc::now().timestamp_millis();
    let now = reg().call("now", &[]).unwrap();
    let after_now = Utc::now().timestamp_millis();
    let QueryValue::Int(now_ms) = now else {
        panic!("now must return Int: {now:?}");
    };
    assert!(
        (before_now..=after_now).contains(&now_ms),
        "now={now_ms}, bounds={before_now}..={after_now}"
    );

    let then_negative = Utc::now().timestamp_millis() + 90_000;
    let before_negative_age = Utc::now().timestamp_millis();
    let negative_age = reg().call("age", &[ts(then_negative)]).unwrap();
    let after_negative_age = Utc::now().timestamp_millis();
    let QueryValue::Int(negative_age_secs) = negative_age else {
        panic!("age must return Int: {negative_age:?}");
    };
    let negative_lower = (before_negative_age - then_negative) / 1000;
    let negative_upper = (after_negative_age - then_negative) / 1000;
    assert!(negative_age_secs < 0);
    assert!((negative_lower..=negative_upper).contains(&negative_age_secs));
    assert!(negative_age_secs == negative_lower || negative_age_secs == negative_upper);

    let then = Utc::now().timestamp_millis() - 10_000;
    let before_age = Utc::now().timestamp_millis();
    let age = reg().call("age", &[ts(then)]).unwrap();
    let after_age = Utc::now().timestamp_millis();
    let QueryValue::Int(age_secs) = age else {
        panic!("age must return Int: {age:?}");
    };
    let lower = (before_age - then) / 1000;
    let upper = (after_age - then) / 1000;
    assert!(
        (lower..=upper).contains(&age_secs),
        "age={age_secs}, bounds={lower}..={upper}"
    );
    assert!(age_secs == lower || age_secs == upper);
}

#[test]
fn rv_funclib_d45cf639_age_checked_subtraction() {
    let r = reg();
    // now (positive epoch millis) - i64::MIN overflows i64: the old
    // unchecked subtraction panicked (debug) / wrapped (release). The
    // contract is a defined "out_of_range" error instead.
    assert_eq!(
        r.call("age", &[ts(i64::MIN)]).unwrap_err().code,
        "out_of_range"
    );
}
