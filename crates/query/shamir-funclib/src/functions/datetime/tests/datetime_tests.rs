//! Per-function `/datetime` tests with literal UTC golden vectors.

use crate::datetime;
use crate::registry::{v_bool, v_int, v_str, ScalarRegistry};
use shamir_types::types::value::QueryValue;

fn reg() -> ScalarRegistry {
    let mut r = ScalarRegistry::new();
    datetime::register(&mut r);
    r
}
const WED_MS: i64 = 1_622_647_845_000;
fn ts(ms: i64) -> QueryValue {
    QueryValue::Int(ms)
}

#[test]
fn now_is_nondeterministic_metadata() {
    let r = reg();
    let e = r.get("now").unwrap();
    assert!(!e.pure);
    assert!(!e.deterministic);
    match r.call("now", &[]).unwrap() {
        QueryValue::Int(n) => assert!(n > 1_600_000_000_000),
        other => panic!("expected Int, got {other:?}"),
    }
    assert_eq!(r.call("now", &[ts(0)]).unwrap_err().code, "arity");
}
#[test]
fn age_is_nondeterministic_and_nonnegative() {
    let r = reg();
    let e = r.get("age").unwrap();
    assert!(!e.pure);
    assert!(!e.deterministic);
    match r.call("age", &[ts(WED_MS)]).unwrap() {
        QueryValue::Int(n) => assert!(n > 0),
        other => panic!("expected Int, got {other:?}"),
    }
    assert_eq!(
        r.call("age", &[QueryValue::Str("x".into())])
            .unwrap_err()
            .code,
        "type_mismatch"
    );
    let future = shamir_types::time::unix_millis() as i64 + 60_000;
    let before = shamir_types::time::unix_millis() as i64;
    let result = r.call("age", &[ts(future)]).unwrap();
    let after = shamir_types::time::unix_millis() as i64;
    let a = (before - future) / 1000;
    let b = (after - future) / 1000;
    let (lo, hi) = (a.min(b), a.max(b));
    match result {
        QueryValue::Int(n) => {
            assert!((lo..=hi).contains(&n));
            assert!(n < 0)
        }
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
    assert_eq!(r.call("to_epoch_s", &[ts(-200)]).unwrap(), v_int(-1));
    assert_eq!(
        r.call("from_epoch_s", &[ts(i64::MAX)]).unwrap_err().code,
        "out_of_range"
    );
}
#[test]
fn rfc3339_roundtrip() {
    let r = reg();
    assert_eq!(
        r.call(
            "parse_rfc3339",
            &[v_str("2021-06-02T15:30:45+00:00".into())]
        )
        .unwrap(),
        v_int(WED_MS)
    );
    assert_eq!(
        r.call("format_rfc3339", &[ts(WED_MS + 123)]).unwrap(),
        v_str("2021-06-02T15:30:45.123+00:00".into())
    );
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
    for (n, v) in [
        ("year", 2021),
        ("month", 6),
        ("day", 2),
        ("hour", 15),
        ("minute", 30),
        ("second", 45),
    ] {
        assert_eq!(r.call(n, &[ts(WED_MS)]).unwrap(), v_int(v));
    }
    assert_eq!(
        r.call("year", &[ts(i64::MAX)]).unwrap_err().code,
        "out_of_range"
    );
}
#[test]
fn weekday_and_weekend() {
    let r = reg();
    assert_eq!(r.call("weekday", &[ts(WED_MS)]).unwrap(), v_int(2));
    assert_eq!(r.call("is_weekend", &[ts(WED_MS)]).unwrap(), v_bool(false));
    let sat = 1_622_894_400_000;
    assert_eq!(r.call("weekday", &[ts(sat)]).unwrap(), v_int(5));
    assert_eq!(r.call("is_weekend", &[ts(sat)]).unwrap(), v_bool(true));
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
    assert_eq!(
        r.call("diff_secs", &[ts(i64::MAX), ts(i64::MIN)])
            .unwrap_err()
            .code,
        "out_of_range"
    );
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
    assert_eq!(
        r.call("start_of_week", &[ts(WED_MS)]).unwrap(),
        v_int(1_622_419_200_000)
    );
    assert_eq!(
        r.call("start_of_month", &[ts(WED_MS)]).unwrap(),
        v_int(1_622_505_600_000)
    );
    assert_eq!(
        r.call("start_of_day", &[ts(i64::MAX)]).unwrap_err().code,
        "out_of_range"
    );
}
#[test]
fn truncate_units() {
    let r = reg();
    for (u, v) in [
        ("second", WED_MS),
        ("minute", 1_622_647_800_000),
        ("hour", 1_622_646_000_000),
        ("day", 1_622_592_000_000),
        ("week", 1_622_419_200_000),
        ("month", 1_622_505_600_000),
        ("year", 1_609_459_200_000),
    ] {
        let ms = if u == "second" { WED_MS + 123 } else { WED_MS };
        assert_eq!(
            r.call("truncate", &[ts(ms), v_str(u.into())]).unwrap(),
            v_int(v)
        );
    }
    assert_eq!(
        r.call("truncate", &[ts(WED_MS), v_str("fortnight".into())])
            .unwrap_err()
            .code,
        "bad_unit"
    );
}
#[test]
fn baseline_constant_is_derived_from_civil_date() {
    let (y, m, d) = (2021i64, 6i64, 2i64);
    let y = y - i64::from(m <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    assert_eq!(days, 18_780);
    assert_eq!(
        WED_MS,
        days * 86_400_000 + 15 * 3_600_000 + 30 * 60_000 + 45 * 1_000
    );
}

#[test]
fn format_with_pattern() {
    let r = reg();
    assert_eq!(
        r.call("format", &[ts(WED_MS), v_str("%Y-%m-%d".into())])
            .unwrap(),
        v_str("2021-06-02".into())
    );
    assert_eq!(
        r.call("format", &[ts(WED_MS), v_str("%Y-%m-%d %H:%M:%S".into())])
            .unwrap(),
        v_str("2021-06-02 15:30:45".into())
    );
}

#[test]
fn malformed_pattern_and_input_cases() {
    let r = reg();
    for pattern in ["%Y-%Q-%d", "%J", "%"] {
        assert_eq!(
            r.call("format", &[ts(WED_MS), v_str(pattern.into())])
                .unwrap_err()
                .code,
            "parse"
        );
        assert_eq!(
            r.call(
                "parse",
                &[v_str("2024-03-15".into()), v_str(pattern.into())]
            )
            .unwrap_err()
            .code,
            "parse"
        );
    }
    for (input, pattern) in [("not-a-date", "%Y-%m-%d"), ("2024-13-01", "%Y-%m-%d")] {
        assert_eq!(
            r.call("parse", &[v_str(input.into()), v_str(pattern.into())])
                .unwrap_err()
                .code,
            "parse"
        );
    }
    let pattern = "%Y-%m-%d %H:%M:%S";
    assert_eq!(
        r.call(
            "parse",
            &[v_str("2024-03-15".into()), v_str("%Y-%m-%d".into())]
        )
        .unwrap(),
        v_int(1_710_460_800_000)
    );
    assert_eq!(
        r.call(
            "parse",
            &[v_str("2024-03-15 10:30:00".into()), v_str(pattern.into())]
        )
        .unwrap(),
        v_int(1_710_498_600_000)
    );
    let formatted = r
        .call("format", &[ts(WED_MS), v_str(pattern.into())])
        .unwrap();
    assert_eq!(
        r.call("parse", &[formatted, v_str(pattern.into())])
            .unwrap(),
        v_int(WED_MS)
    );
}

#[test]
fn parse_date_only_pattern() {
    let r = reg();
    assert_eq!(
        r.call(
            "parse",
            &[v_str("2024-03-15".into()), v_str("%Y-%m-%d".into())]
        )
        .unwrap(),
        v_int(1_710_460_800_000)
    );
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
fn parse_datetime_pattern() {
    let r = reg();
    assert_eq!(
        r.call(
            "parse",
            &[
                v_str("2024-03-15 10:30:00".into()),
                v_str("%Y-%m-%d %H:%M:%S".into())
            ]
        )
        .unwrap(),
        v_int(1_710_498_600_000)
    );
}

#[test]
fn format_parse_roundtrip() {
    let r = reg();
    let pattern = "%Y-%m-%d %H:%M:%S";
    let formatted = r
        .call("format", &[ts(WED_MS), v_str(pattern.into())])
        .unwrap();
    assert_eq!(
        r.call("parse", &[formatted, v_str(pattern.into())])
            .unwrap(),
        v_int(WED_MS)
    );
}

#[test]
fn parse_malformed_pattern_returns_err_not_panic() {
    let r = reg();
    for (input, pattern) in [("2024-03-15", "%Y-%Q-%d"), ("2024-03-15", "%")] {
        assert_eq!(
            r.call("parse", &[v_str(input.into()), v_str(pattern.into())])
                .unwrap_err()
                .code,
            "parse"
        );
    }
}

#[test]
fn civil_year_edges_are_enforced() {
    // days_from_civil(2021, 6, 2) = 18780; weekday is Wednesday.
    let r = reg();
    let edges = [
        (
            253_370_764_800_000,
            Some((9999, 1, 1)),
            Some(253_370_764_800_000),
            Some(253_370_419_200_000),
            Some(253_370_764_800_000),
        ),
        (
            253_402_300_799_999,
            Some((9999, 12, 31)),
            Some(253_402_214_400_000),
            Some(253_401_868_800_000),
            Some(253_399_622_400_000),
        ),
        (253_402_300_800_000, None, None, None, None),
        (
            -377_705_116_800_000,
            Some((-9999, 1, 1)),
            Some(-377_705_116_800_000),
            Some(-377_705_116_800_000),
            Some(-377_705_116_800_000),
        ),
        (-377_705_116_800_001, None, None, None, None),
        (-377_736_739_200_000, None, None, None, None),
    ];
    for (ms, parts, day, week, month) in edges {
        for (n, i) in [("year", 0), ("month", 1), ("day", 2)] {
            let got = r.call(n, &[ts(ms)]);
            match parts {
                Some((y, m, d)) => assert_eq!(got.unwrap(), v_int([y, m, d][i]), "{n} {ms}"),
                None => assert_eq!(got.unwrap_err().code, "out_of_range"),
            }
        }
        for (unit, expected) in [
            ("second", day),
            ("minute", day),
            ("hour", day),
            ("day", day),
            ("week", week),
            ("month", month),
            (
                "year",
                if parts.is_some() {
                    Some(if ms > 0 {
                        253_370_764_800_000
                    } else {
                        -377_705_116_800_000
                    })
                } else {
                    None
                },
            ),
        ] {
            let value = if matches!(unit, "second" | "minute" | "hour") && parts.is_some() {
                let day_start = day.unwrap();
                let elapsed = ms - day_start;
                let divisor = match unit {
                    "second" => 1_000,
                    "minute" => 60_000,
                    _ => 3_600_000,
                };
                Some(day_start + elapsed.div_euclid(divisor) * divisor)
            } else {
                expected
            };
            let got = r.call("truncate", &[ts(ms), v_str(unit.into())]);
            match (got, value) {
                (Ok(v), Some(expected_ms)) => {
                    assert_eq!(v, v_int(expected_ms), "truncate {unit} {ms}")
                }
                (Err(e), None) => assert_eq!(e.code, "out_of_range", "truncate {unit} {ms}"),
                (a, b) => panic!("truncate {unit} {ms}: {a:?} {b:?}"),
            }
        }
        for (n, expected) in [
            ("start_of_day", day),
            ("start_of_week", week),
            ("start_of_month", month),
        ] {
            match (r.call(n, &[ts(ms)]), expected) {
                (Ok(v), Some(ms)) => assert_eq!(v, v_int(ms)),
                (Err(e), None) => assert_eq!(e.code, "out_of_range"),
                (a, b) => panic!("{n}: {a:?}, {b:?}"),
            }
        }
        let f = r.call("format", &[ts(ms), v_str("%F".into())]);
        let rf = r.call("format_rfc3339", &[ts(ms)]);
        match (parts, f, rf) {
            (Some((y, m, d)), Ok(f), Ok(rf)) => {
                assert_eq!(f, v_str(format!("{y:04}-{m:02}-{d:02}")));
                let text = if y == 9999 && m == 1 {
                    "9999-01-01T00:00:00+00:00"
                } else if y == 9999 {
                    "9999-12-31T23:59:59.999+00:00"
                } else {
                    "-9999-01-01T00:00:00+00:00"
                };
                assert_eq!(rf, v_str(text.into()))
            }
            (None, Err(a), Err(b)) => {
                assert_eq!(a.code, "out_of_range");
                assert_eq!(b.code, "out_of_range")
            }
            (p, a, b) => panic!("{p:?} {a:?} {b:?}"),
        }
    }
    for (input, pattern, expected) in [
        ("9999-01-01", "%Y-%m-%d", Some(253_370_764_800_000)),
        ("9999-12-31", "%Y-%m-%d", Some(253_402_214_400_000)),
        ("10000-01-01", "%Y-%m-%d", None),
        ("-9999-01-01", "%Y-%m-%d", Some(-377_705_116_800_000)),
        ("-10000-01-01", "%Y-%m-%d", None),
        ("-10000-12-31", "%Y-%m-%d", None),
        (
            "9999-12-31 23:59:59.999",
            "%Y-%m-%d %H:%M:%S%.3f",
            Some(253_402_300_799_999),
        ),
    ] {
        match (
            r.call("parse", &[v_str(input.into()), v_str(pattern.into())]),
            expected,
        ) {
            (Ok(v), Some(ms)) => assert_eq!(v, v_int(ms)),
            (Err(e), None) => assert_eq!(e.code, "parse"),
            (a, b) => panic!("{input} {a:?} {b:?}"),
        }
    }
    for (input, expected) in [
        ("9999-01-01T00:00:00Z", Some(253_370_764_800_000)),
        ("9999-12-31T23:59:59.999Z", Some(253_402_300_799_999)),
        ("10000-01-01T00:00:00Z", None),
        ("-9999-01-01T00:00:00Z", None),
        ("-10000-01-01T00:00:00Z", None),
        ("-10000-12-31T23:59:59.999Z", None),
    ] {
        match (r.call("parse_rfc3339", &[v_str(input.into())]), expected) {
            (Ok(v), Some(ms)) => assert_eq!(v, v_int(ms)),
            (Err(e), None) => assert_eq!(e.code, "parse"),
            (a, b) => panic!("{input} {a:?} {b:?}"),
        }
    }
    for (input, code) in [("9999-12-31T23:59:59-00:01", "out_of_range")] {
        assert_eq!(
            r.call("parse_rfc3339", &[v_str(input.into())])
                .unwrap_err()
                .code,
            code
        )
    }
    for (ms, delta, n) in [
        (253_402_300_799_999, 1, "add_secs"),
        (-377_705_116_800_000, -1, "add_secs"),
        (253_402_300_799_999, 1, "add_days"),
        (-377_705_116_800_000, -1, "add_days"),
    ] {
        assert_eq!(
            r.call(n, &[ts(ms), ts(delta)]).unwrap_err().code,
            "out_of_range"
        )
    }
}

#[test]
fn trailing_alternate_offset_inputs_are_golden() {
    let r = reg();
    for (input, pattern, expected) in [
        ("2021-06-02 15:30:45 Z", "%Y-%m-%d %H:%M:%S %#z", Ok(WED_MS)),
        (
            "2021-06-02 15:30:45 +0000",
            "%Y-%m-%d %H:%M:%S %#z",
            Ok(WED_MS),
        ),
        (
            "2021-06-02 15:30:45 +00:00",
            "%Y-%m-%d %H:%M:%S %#z",
            Ok(WED_MS),
        ),
        (
            "2021-06-02 15:30:45 -0500",
            "%Y-%m-%d %H:%M:%S %#z",
            Ok(WED_MS),
        ),
        (
            "2021-06-02 15:30:45 -05:00",
            "%Y-%m-%d %H:%M:%S %#z",
            Ok(WED_MS),
        ),
        (
            "2021-06-02 15:30:45 -07",
            "%Y-%m-%d %H:%M:%S %#z",
            Ok(WED_MS),
        ),
        (
            "2021-06-02 15:30:45 +30:00",
            "%Y-%m-%d %H:%M:%S %#z",
            Ok(WED_MS),
        ),
        (
            "2021-06-02 15:30:45 -99:59",
            "%Y-%m-%d %H:%M:%S %#z",
            Ok(WED_MS),
        ),
        (
            "2021-06-02 15:30:45 +02:00:30",
            "%Y-%m-%d %H:%M:%S %#z",
            Err("parse"),
        ),
        ("2021-06-02 15:30:45", "%Y-%m-%d %H:%M:%S %#z", Err("parse")),
        ("2021-06-02T15:30:45Z", "%Y-%m-%dT%H:%M:%S%#z", Ok(WED_MS)),
        ("2021-06-02T15:30:45z", "%Y-%m-%dT%H:%M:%S%#z", Ok(WED_MS)),
        ("2021-06-02T15:30:45+02", "%Y-%m-%dT%H:%M:%S%#z", Ok(WED_MS)),
        (
            "2021-06-02T15:30:45+0200",
            "%Y-%m-%dT%H:%M:%S%#z",
            Ok(WED_MS),
        ),
        (
            "2021-06-02T15:30:45+02:00",
            "%Y-%m-%dT%H:%M:%S%#z",
            Ok(WED_MS),
        ),
        (
            "2021-06-02T15:30:45+02 00",
            "%Y-%m-%dT%H:%M:%S%#z",
            Ok(WED_MS),
        ),
        (
            "2021-06-02T15:30:45+02:00:30",
            "%Y-%m-%dT%H:%M:%S%#z",
            Err("parse"),
        ),
        ("2021-06-02T15:30:45-07", "%Y-%m-%dT%H:%M:%S%#z", Ok(WED_MS)),
        (
            "2021-06-02T15:30:45+30:00",
            "%Y-%m-%dT%H:%M:%S%#z",
            Ok(WED_MS),
        ),
        ("2021-06-02T15:30:45", "%Y-%m-%dT%H:%M:%S%#z", Err("parse")),
        (
            "2021-06-02T15:30:45-99:59",
            "%Y-%m-%dT%H:%M:%S%#z",
            Ok(WED_MS),
        ),
    ] {
        let got = r.call("parse", &[v_str(input.into()), v_str(pattern.into())]);
        match expected {
            Ok(ms) => assert_eq!(got.unwrap(), v_int(ms)),
            Err(e) => assert_eq!(got.unwrap_err().code, e),
        }
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
        v_int(WED_MS)
    );
}
#[test]
fn rfc3339_boundary_utc_outcomes() {
    let r = reg();
    for (s, ms) in [
        ("9999-01-01T00:00:00Z", 253_370_764_800_000),
        ("9999-12-31T23:59:59.999Z", 253_402_300_799_999),
    ] {
        assert_eq!(
            r.call("parse_rfc3339", &[v_str(s.into())]).unwrap(),
            v_int(ms)
        );
    }
    for (timestamp, expected) in [
        (253_370_764_800_000, "9999-01-01T00:00:00+00:00"),
        (253_402_300_799_999, "9999-12-31T23:59:59.999+00:00"),
        (-377_705_116_800_000, "-9999-01-01T00:00:00+00:00"),
    ] {
        assert_eq!(
            r.call("format_rfc3339", &[ts(timestamp)]).unwrap(),
            v_str(expected.into())
        );
    }
    for (s, e) in [
        ("10000-01-01T00:00:00Z", "parse"),
        ("-10000-01-01T00:00:00Z", "parse"),
        ("-10000-12-31T23:59:59.999Z", "parse"),
        ("-9999-01-01T00:00:00+00:01", "parse"),
        ("9999-12-31T23:59:59-00:01", "out_of_range"),
    ] {
        assert_eq!(
            r.call("parse_rfc3339", &[v_str(s.into())])
                .unwrap_err()
                .code,
            e,
            "{s}"
        );
    }
}
#[test]
fn rfc3339_fraction_and_offset_goldens() {
    let r = reg();
    for (s, ms) in [
        ("2021-06-02T15:30:45.1Z", 1_622_647_845_100),
        ("2021-06-02T15:30:45.12Z", 1_622_647_845_120),
        ("2021-06-02T15:30:45.123Z", 1_622_647_845_123),
        ("2021-06-02T15:30:45.123456Z", 1_622_647_845_123),
        ("2021-06-02T15:30:45.123456789Z", 1_622_647_845_123),
        ("2021-06-02T15:30:45.1234567890Z", 1_622_647_845_123),
        ("2021-06-02T15:30:45.123+02:30", 1_622_638_845_123),
        ("2021-06-02T15:30:45.1+02:30", 1_622_638_845_100),
        ("2021-06-02T15:30:45.12+02:30", 1_622_638_845_120),
        ("2021-06-02T15:30:45.123456+02:30", 1_622_638_845_123),
        ("2021-06-02T15:30:45.123456789+02:30", 1_622_638_845_123),
        ("2021-06-02T15:30:45.1234567890+02:30", 1_622_638_845_123),
        ("2021-06-02T15:30:45.1−05:00", 1_622_663_445_100),
        ("2021-06-02T15:30:45.12−05:00", 1_622_663_445_120),
        ("2021-06-02T15:30:45.123−05:00", 1_622_663_445_123),
        ("2021-06-02T15:30:45.123456−05:00", 1_622_663_445_123),
        ("2021-06-02T15:30:45.123456789−05:00", 1_622_663_445_123),
        ("2021-06-02T15:30:45.1234567890−05:00", 1_622_663_445_123),
        ("2021-06-02T15:30:45-07:00", 1_622_673_045_000),
        ("2021-06-02T15:30:45−05:00", 1_622_663_445_000),
        ("2021-06-02t15:30:45z", 1_622_647_845_000),
        ("2016-12-31T23:59:60Z", 1_483_228_800_000),
        ("1969-12-31T23:59:59.9Z", -100),
        ("9999-12-31T00:00:00Z", 253_402_214_400_000),
    ] {
        assert_eq!(
            r.call("parse_rfc3339", &[v_str(s.into())]).unwrap(),
            v_int(ms),
            "{s}"
        )
    }
    for (s, e) in [
        ("2021-06-02T15:30:45", "parse"),
        ("2021-06-02T15:30:45+", "parse"),
        ("2021-06-02T15:30:45+0", "parse"),
        ("2021-06-02T15:30:45+25:00", "parse"),
        ("10000-01-01T00:00:00Z", "parse"),
        ("-10000-01-01T00:00:00Z", "parse"),
        ("-10000-12-31T23:59:59.999Z", "parse"),
        ("-9999-01-01T00:00:00+00:01", "parse"),
        ("9999-12-31T23:59:59-00:01", "out_of_range"),
    ] {
        assert_eq!(
            r.call("parse_rfc3339", &[v_str(s.into())])
                .unwrap_err()
                .code,
            e,
            "{s}"
        );
    }
}
#[test]
fn strftime_golden_table() {
    let r = reg();
    for (p, e) in [
        ("%Y-%m-%d", "2021-06-02"),
        ("%H:%M:%S", "15:30:45"),
        ("%j", "153"),
        ("%s", "1622647845"),
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
        ("%V", "22"),
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
            r.call("format", &[ts(WED_MS + 123), v_str(p.into())])
                .unwrap(),
            v_str(e.into()),
            "{p}"
        )
    }
    for (p, e) in [
        ("%3f", "123"),
        ("%6f", "123000"),
        ("%9f", "123000000"),
        ("%.f", ".123"),
        ("%.3f", ".123"),
        ("%.6f", ".123000"),
        ("%.9f", ".123000000"),
    ] {
        assert_eq!(
            r.call("format", &[ts(WED_MS + 123), v_str(p.into())])
                .unwrap(),
            v_str(e.into())
        )
    }
    for p in [
        "%1d", "%2Y", "%03f", "%_3f", "%-3f", "%#3f", "%3d", "%9Y", "%_9f", "%#9f", "%-9f",
    ] {
        assert_eq!(
            r.call("format", &[ts(WED_MS), v_str(p.into())])
                .unwrap_err()
                .code,
            "parse",
            "{p}"
        )
    }
}
#[test]
fn rv_funclib_d45cf639_age_checked_subtraction() {
    assert_eq!(
        reg().call("age", &[ts(i64::MIN)]).unwrap_err().code,
        "out_of_range"
    )
}
