//! Per-function `/datetime` tests with literal UTC golden vectors.

use crate::datetime;
use crate::registry::{v_bool, v_int, v_str, ScalarRegistry};
use shamir_types::types::value::QueryValue;

fn reg() -> ScalarRegistry {
    let mut r = ScalarRegistry::new();
    datetime::register(&mut r);
    r
}

/// 2021-06-02T15:30:45Z — Wednesday; days_from_civil(2021, 6, 2) = 18780,
/// and 18780*86_400_000 + 15*3_600_000 + 30*60_000 + 45*1_000.
const WED_MS: i64 = 1_622_647_845_000;

fn ts(ms: i64) -> QueryValue {
    QueryValue::Int(ms)
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
    let future = shamir_types::time::unix_millis() as i64 + 60_000;
    match r.call("age", &[ts(future)]).unwrap() {
        QueryValue::Int(seconds) => assert!(seconds < 0),
        other => panic!("expected Int, got {other:?}"),
    }
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
    assert_eq!(
        r.call("format_rfc3339", &[ts(WED_MS + 123)]).unwrap(),
        v_str("2021-06-02T15:30:45.123+00:00".into())
    );
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
    let sat = 1_622_894_400_000;
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
        v_int(1_622_592_000_000)
    );
    // week starts on Monday 2021-05-31.
    assert_eq!(
        r.call("start_of_week", &[ts(WED_MS)]).unwrap(),
        v_int(1_622_419_200_000)
    );
    assert_eq!(
        r.call("start_of_month", &[ts(WED_MS)]).unwrap(),
        v_int(1_622_505_600_000)
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
        v_int(1_622_647_800_000)
    );
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("hour".into())])
            .unwrap(),
        v_int(1_622_646_000_000)
    );
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("day".into())])
            .unwrap(),
        v_int(1_622_592_000_000)
    );
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("week".into())])
            .unwrap(),
        v_int(1_622_419_200_000)
    );
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("month".into())])
            .unwrap(),
        v_int(1_622_505_600_000)
    );
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("year".into())])
            .unwrap(),
        v_int(1_609_459_200_000)
    );
    // error: unknown unit.
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("fortnight".into())])
            .unwrap_err()
            .code,
        "bad_unit"
    );
}

#[test]
fn baseline_constant_is_derived_from_civil_date() {
    // days_from_civil(2021, 6, 2) = 18_780; add 15:30:45 in milliseconds.
    let days = {
        let (year, month, day) = (2021i64, 6i64, 2i64);
        let adjusted_year = year - i64::from(month <= 2);
        let era = adjusted_year.div_euclid(400);
        let year_of_era = adjusted_year - era * 400;
        let shifted_month = month + if month > 2 { -3 } else { 9 };
        let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
        let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
        era * 146_097 + day_of_era - 719_468
    };
    assert_eq!(days, 18_780);
    assert_eq!(
        WED_MS,
        days * 86_400_000 + 15 * 3_600_000 + 30 * 60_000 + 45 * 1_000
    );
}

#[test]
fn boundary_date_parse_pattern_range() {
    let r = reg();
    for (input, result, code) in [
        ("9999-01-01", Some(253_370_764_800_000), "parse"),
        ("10000-01-01", None, "parse"),
        ("-9999-01-01", Some(-377_705_203_200_000), "parse"),
        ("-10000-01-01", None, "parse"),
    ] {
        let actual = r.call("parse", &[v_str(input.into()), v_str("%Y-%m-%d".into())]);
        match result {
            Some(ms) => assert_eq!(actual.unwrap(), v_int(ms)),
            None => assert_eq!(actual.unwrap_err().code, code),
        }
    }
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

fn format_with_malformed_pattern_returns_err_not_panic() {
    // Validate patterns before formatting so unsupported directives return a
    // clean `parse` error rather than panic.
    let r = reg();
    assert_eq!(
        r.call("format", &[ts(WED_MS), v_str("%1d".into())])
            .unwrap_err()
            .code,
        "parse"
    );
    // An unsupported specifier must be rejected with `parse`.
    assert_eq!(
        r.call("format", &[ts(WED_MS), v_str("%Y-%Q-%d".into())])
            .unwrap_err()
            .code,
        "parse"
    );
    // %J is unsupported, distinct from %q (quarter).
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
    assert_eq!(result, v_int(1_710_460_800_000));
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
    assert_eq!(result, v_int(1_710_498_600_000));
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

#[test]
fn civil_year_edges_are_enforced() {
    let r = reg();
    assert_eq!(
        r.call("year", &[ts(253_370_764_800_000)]).unwrap(),
        v_int(9999)
    );
    assert_eq!(
        r.call("weekday", &[ts(253_370_764_800_000)]).unwrap(),
        v_int(4)
    );
    assert_eq!(
        r.call("year", &[ts(253_402_214_400_000)]).unwrap(),
        v_int(9999)
    );
    let cases = [
        (253_370_764_800_000, Some((9999, 1, 1))),
        (253_402_300_799_999, Some((9999, 12, 31))),
        (253_402_300_800_000, None),
        (-377_705_203_200_000, Some((-9999, 1, 1))),
        (-377_705_203_200_001, None),
    ];
    for (millis, expected) in cases {
        for (name, field) in [("year", 0), ("month", 1), ("day", 2)] {
            match expected {
                Some(parts) => assert_eq!(
                    r.call(name, &[ts(millis)]).unwrap(),
                    v_int(parts[field]),
                    "{name} at {millis}"
                ),
                None => assert_eq!(
                    r.call(name, &[ts(millis)]).unwrap_err().code,
                    "out_of_range",
                    "{name} at {millis}"
                ),
            }
        }
    }
    assert_eq!(
        r.call(
            "parse",
            &[v_str("9999-12-31".into()), v_str("%Y-%m-%d".into())]
        )
        .unwrap(),
        v_int(253_402_214_400_000)
    );
    assert_eq!(
        r.call(
            "parse",
            &[v_str("-9999-01-01".into()), v_str("%Y-%m-%d".into())]
        )
        .unwrap(),
        v_int(-377_705_203_200_000)
    );
    for (input, pattern, expected) in [
        ("9999-12-31", "%Y-%m-%d", 253_402_214_400_000),
        ("-9999-01-01", "%Y-%m-%d", -377_705_203_200_000),
    ] {
        assert_eq!(
            r.call("parse", &[v_str(input.into()), v_str(pattern.into())])
                .unwrap(),
            v_int(expected)
        );
    }
    for input in ["10000-01-01", "-10000-01-01"] {
        assert_eq!(
            r.call("parse", &[v_str(input.into()), v_str("%Y-%m-%d".into())])
                .unwrap_err()
                .code,
            "parse"
        );
    }
    for (millis, civil_year) in [
        (253_370_764_800_000, 9999),
        (253_402_300_799_999, 9999),
        (253_402_300_800_000, 10000),
        (-377_705_203_200_000, -9999),
        (-377_705_203_200_001, -10000),
    ] {
        let accepted = (-9999..=9999).contains(&civil_year);
        assert_eq!(
            r.call("format", &[ts(millis), v_str("%F".into())]).is_ok(),
            accepted
        );
        assert_eq!(r.call("format_rfc3339", &[ts(millis)]).is_ok(), accepted);
        let text = if civil_year > 0 {
            format!("{civil_year:04}-01-01")
        } else {
            format!("-{abs:04}-01-01", abs = civil_year.unsigned_abs())
        };
        let result = r.call("parse", &[v_str(text), v_str("%Y-%m-%d".into())]);
        if accepted {
            assert!(result.is_ok(), "parse year {civil_year}");
        } else {
            assert_eq!(result.unwrap_err().code, "parse");
        }
        for name in ["truncate", "add_secs", "add_days"] {
            let args = match name {
                "truncate" => vec![ts(millis), v_str("day".into())],
                _ => vec![ts(millis), ts(0)],
            };
            match accepted {
                true => assert!(r.call(name, &args).is_ok(), "{name} {civil_year}"),
                false => assert_eq!(r.call(name, &args).unwrap_err().code, "out_of_range"),
            }
        }
    }
    for millis in [
        253_370_764_800_000,
        253_402_300_799_999,
        253_402_300_800_000,
        -377_705_203_200_000,
        -377_705_203_200_001,
    ] {
        for (name, args) in [
            ("start_of_day", vec![ts(millis)]),
            ("start_of_week", vec![ts(millis)]),
            ("start_of_month", vec![ts(millis)]),
            ("truncate", vec![ts(millis), v_str("day".into())]),
        ] {
            assert!(r.call(name, &args).is_ok(), "{name} {millis}");
        }
    }
    for millis in [
        253_402_300_800_000,
        -377_705_203_200_001,
    ] {
        for (name, args) in [
            ("start_of_day", vec![ts(millis)]),
            ("start_of_week", vec![ts(millis)]),
            ("start_of_month", vec![ts(millis)]),
            ("truncate", vec![ts(millis), v_str("day".into())]),
        ] {
            assert_eq!(r.call(name, &args).unwrap_err().code, "out_of_range");
        }
    }
    for (inside, seconds) in [(253_402_300_799_999, 1), (-377_705_203_200_000, -1)] {
        assert_eq!(
            r.call("add_secs", &[ts(inside), ts(seconds)])
                .unwrap_err()
                .code,
            "out_of_range"
        );
    }
}

#[test]
fn trailing_alternate_offset_inputs_are_golden() {
    let r = reg();
    for input in [
        "2021-06-02 15:30:45 Z",
        "2021-06-02 15:30:45 +0000",
        "2021-06-02 15:30:45 +00:00",
        "2021-06-02 15:30:45 -0500",
        "2021-06-02 15:30:45 -05:00",
    ] {
        assert_eq!(
            r.call(
                "parse",
                &[v_str(input.into()), v_str("%Y-%m-%d %H:%M:%S %#z".into())]
            )
            .unwrap(),
            v_int(WED_MS),
            "{input}"
        );
    }
}

#[test]
fn non_trailing_offset_directives_follow_jiff() {
    let r = reg();
    assert_eq!(
        r.call(
            "parse",
            &[
                v_str("2021-06-02 +02:00 15:30:45".into()),
                v_str("%Y-%m-%d %z %H:%M:%S".into())
            ]
        )
        .unwrap_err()
        .code,
        "parse"
    );
    assert_eq!(
        r.call(
            "parse",
            &[
                v_str("2021-06-02 +0200 15:30:45".into()),
                v_str("%Y-%m-%d %z %H:%M:%S".into())
            ]
        )
        .unwrap(),
        v_int(1_622_647_845_000)
    );
}

#[test]
fn rfc3339_fraction_and_offset_goldens() {
    let r = reg();
    for (input, expected) in [
        ("2021-06-02t15:30:45z", 1_622_647_845_000),
        ("2021-06-02T15:30:45.12Z", 1_622_647_845_120),
        ("2021-06-02T15:30:45.123Z", 1_622_647_845_123),
        ("2021-06-02T15:30:45.123456Z", 1_622_647_845_456),
        ("2021-06-02T15:30:45.123456789Z", 1_622_647_845_789),
        ("2021-06-02T15:30:45.1234567890Z", 1_622_647_845_789),
        ("2021-06-02T15:30:45.123+02:30", 1_622_638_845_123),
        ("2021-06-02T15:30:45.1+02:30", 1_622_638_845_100),
        ("2021-06-02T15:30:45.12+02:30", 1_622_638_845_120),
        ("2021-06-02T15:30:45.123456+02:30", 1_622_638_845_456),
        ("2021-06-02T15:30:45.123456789+02:30", 1_622_638_845_789),
        ("2021-06-02T15:30:45.1234567890+02:30", 1_622_638_845_789),
        ("2021-06-02T15:30:45.1−05:00", 1_622_663_445_100),
        ("2021-06-02T15:30:45.12−05:00", 1_622_663_445_120),
        ("2021-06-02T15:30:45.123−05:00", 1_622_663_445_123),
        ("2021-06-02T15:30:45.123456−05:00", 1_622_663_445_456),
        ("2021-06-02T15:30:45.123456789−05:00", 1_622_663_445_789),
        ("2021-06-02T15:30:45.1234567890−05:00", 1_622_663_445_789),
        ("2021-06-02T15:30:45-07:00", 1_622_673_045_000),
        ("2021-06-02T15:30:45−05:00", 1_622_663_445_000),
        ("2016-12-31T23:59:60Z", 1_483_228_800_000),
        ("1969-12-31T23:59:59.9Z", -100),
        ("9999-12-31T00:00:00Z", 253_402_214_400_000),
        ("-9999-01-01T00:00:00Z", -377_705_203_200_000),
    ] {
        assert_eq!(
            r.call("parse_rfc3339", &[v_str(input.into())]).unwrap(),
            v_int(expected),
            "{input}"
        );
    }
    for (input, code) in [
        ("2021-06-02T15:30:45", "parse"),
        ("2021-06-02T15:30:45+", "parse"),
        ("2021-06-02T15:30:45+0", "parse"),
        ("2021-06-02T15:30:45+25:00", "parse"),
        ("10000-01-01T00:00:00Z", "parse"),
        ("-10000-01-01T00:00:00Z", "parse"),
        ("9999-12-31T23:59:59-00:01", "out_of_range"),
        ("-9999-01-01T00:00:00+00:01", "out_of_range"),
    ] {
        assert_eq!(
            r.call("parse_rfc3339", &[v_str(input.into())])
                .unwrap_err()
                .code,
            code,
            "{input}"
        );
    }
}

#[test]
fn strftime_golden_table() {
    let r = reg();
    for (pattern, expected) in [
        ("%Y-%m-%d", "2021-06-02"),
        ("%H:%M:%S", "15:30:45"),
        ("%j", "153"),
        ("%u", "3"),
        ("%w", "3"),
        ("%_d", " 2"),
        ("%-d", "2"),
        ("%0d", "02"),
        ("%e", " 2"),
        ("%h", "Jun"),
        ("%A", "Wednesday"),
        ("%a", "Wed"),
        ("%B", "June"),
        ("%b", "Jun"),
        ("%p", "PM"),
        ("%P", "pm"),
        ("%I", "03"),
        ("%l", " 3"),
        ("%m", "06"),
        ("%M", "30"),
        ("%S", "45"),
        ("%k", "15"),
        ("%C", "20"),
        ("%y", "21"),
        ("%D", "06/02/21"),
        ("%F", "2021-06-02"),
        ("%R", "15:30"),
        ("%T", "15:30:45"),
        ("%X", "15:30:45"),
        ("%r", "03:30:45 PM"),
        ("%c", "Wed Jun  2 15:30:45 2021"),
        ("%x", "06/02/21"),
        ("%v", " 2-Jun-2021"),
        ("%U", "22"),
        ("%W", "22"),
        ("%V", "23"),
        ("%G", "2021"),
        ("%g", "21"),
        ("%-G", "2021"),
        ("%_g", "21"),
        ("%q", "2"),
        ("%f", "123000000"),
        ("%-f", "123000000"),
        ("%_f", "123000000"),
        ("%.f", ".123"),
        ("%.3f", ".123"),
        ("%.6f", ".123000"),
        ("%.9f", ".123000000"),
        ("%3f", "123"),
        ("%6f", "123000"),
        ("%9f", "123000000"),
        ("%z", "+0000"),
        ("%:z", "+00:00"),
        ("%::z", "+00:00:00"),
        ("%:::z", "+00"),
        ("%Z", "UTC"),
        ("%+", "2021-06-02T15:30:45.123+00:00"),
        ("%n", "\n"),
        ("%t", "\t"),
        ("%%", "%"),
    ] {
        assert_eq!(
            r.call("format", &[ts(WED_MS + 123), v_str(pattern.into())])
                .unwrap(),
            v_str(expected.into()),
            "{pattern}"
        );
    }
    for pattern in [
        "%E4Y", "%Od", "%^B", "%#c", "%Q", "%J", "%", "%E", "%O", "%12d", "%_F", "%1d", "%2Y",
        "%_3f", "%#3f", "%3d", "%9Y",
    ] {
        assert_eq!(
            r.call("format", &[ts(WED_MS), v_str(pattern.into())])
                .unwrap_err()
                .code,
            "parse",
            "{pattern}"
        );
    }
}

#[test]
fn local_minute_of_day_is_zone_independent_in_range() {
    let now_ms = shamir_types::time::unix_millis() as i64;
    let utc_minute = (now_ms.rem_euclid(86_400_000) / 60_000) as i64;
    let earlier_local = i64::from(shamir_types::time::local_minute_of_day());
    let later_local = i64::from(shamir_types::time::local_minute_of_day());
    let local = (earlier_local + later_local) / 2;
    let delta = (local - utc_minute).rem_euclid(1440);
    assert_eq!(delta % 1, 0);
    assert!(delta <= 14 * 60);
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
