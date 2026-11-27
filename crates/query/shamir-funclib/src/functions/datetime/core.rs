//! `/datetime` scalar category — temporal functions over a canonical
//! **epoch-millis UTC `i64`** timestamp.
//!
//! Functions registered locally by `register` (plain names); `register_builtins`
//! dispatches them under the production `datetime/` folder prefix:
//! `now to_epoch_s to_epoch_ms from_epoch_s from_epoch_ms parse_rfc3339
//!  format_rfc3339 parse format year month day hour minute second weekday
//!  is_weekend add_secs add_days diff_secs start_of_day start_of_week
//!  start_of_month truncate age`.
//!
//! Conventions (mirroring `math.rs`):
//! - The canonical timestamp is `i64` epoch-millis UTC. Component extractors
//!   (`year`, `month`, …) and `weekday` return `Int`; `is_weekend` returns a
//!   `Bool`; `format_rfc3339` returns a `Str`; everything else returns the
//!   canonical `Int` epoch-millis.
//! - `now` and `age` read the wall clock and are therefore **non-deterministic
//!   and impure** (`pure:false, deterministic:false`); every other function is
//!   `pure + deterministic` but remains unvouched for indexing.
//! - Errors are machine codes only: `"out_of_range"` for un-representable
//!   timestamps, `"parse"` for an unparseable RFC-3339 string, a strftime
//!   pattern containing an invalid specifier, or an input string that does
//!   not match the given pattern, and `"bad_unit"` for an unknown `truncate`
//!   unit.

use crate::registry::{
    arg_i64, arg_str, v_bool, v_int, v_str, FnEntry, ScalarError, ScalarRegistry,
};
use jiff::{civil::DateTime as CivilDateTime, Timestamp};

/// Ensure the UTC civil year containing `ms` is in Jiff's -9999..=9999 range.
fn ensure_civil_year_range(ms: i64, code: &'static str) -> Result<(), ScalarError> {
    let days = ms.div_euclid(86_400_000);
    let (year, _, _) = civil_from_days(days);
    if (-9999..=9999).contains(&year) {
        Ok(())
    } else {
        Err(ScalarError::builtin(code))
    }
}

/// Gregorian calendar fields for a UTC epoch-millisecond timestamp.
struct CalendarFields {
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i32,
    millisecond: i32,
    /// Monday = 0 through Sunday = 6.
    weekday: i64,
}

impl CalendarFields {
    fn from_millis(ms: i64) -> Result<Self, ScalarError> {
        ensure_civil_year_range(ms, "out_of_range")?;
        const DAY_MILLIS: i64 = 86_400_000;
        let days = ms.div_euclid(DAY_MILLIS);
        let day_millis = ms.rem_euclid(DAY_MILLIS);
        let (year, month, day) = civil_from_days(days);
        Ok(Self {
            year: year as i32,
            month: month as i32,
            day: day as i32,
            hour: (day_millis / 3_600_000) as i32,
            minute: ((day_millis % 3_600_000) / 60_000) as i32,
            second: ((day_millis % 60_000) / 1_000) as i32,
            millisecond: (day_millis % 1_000) as i32,
            weekday: (days + 3).rem_euclid(7),
        })
    }
}

/// Convert a proleptic Gregorian civil date to days since 1970-01-01.
fn days_from_civil(year: i32, month: i32, day: i32) -> i64 {
    let mut y = i64::from(year);
    let m = i64::from(month);
    y -= if m <= 2 { 1 } else { 0 };
    let era = y.div_euclid(400);
    let year_of_era = y - era * 400;
    let shifted_month = m + if m > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Convert days since 1970-01-01 to a proleptic Gregorian civil date.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

fn checked_calendar_start(days: i64) -> Result<i64, ScalarError> {
    let millis = days
        .checked_mul(86_400_000)
        .ok_or_else(|| ScalarError::builtin("out_of_range"))?;
    ensure_civil_year_range(millis, "out_of_range")?;
    Ok(millis)
}

fn start_of_calendar_day(ms: i64) -> Result<i64, ScalarError> {
    ensure_civil_year_range(ms, "out_of_range")?;
    checked_calendar_start(ms.div_euclid(86_400_000))
}

fn start_of_calendar_week(ms: i64) -> Result<i64, ScalarError> {
    ensure_civil_year_range(ms, "out_of_range")?;
    let days = ms.div_euclid(86_400_000);
    checked_calendar_start(days - (days + 3).rem_euclid(7))
}

fn start_of_calendar_month(ms: i64) -> Result<i64, ScalarError> {
    let fields = CalendarFields::from_millis(ms)?;
    checked_calendar_start(days_from_civil(fields.year, fields.month, 1))
}

fn start_of_calendar_year(ms: i64, fields: CalendarFields) -> Result<i64, ScalarError> {
    ensure_civil_year_range(ms, "out_of_range")?;
    checked_calendar_start(days_from_civil(fields.year, 1, 1))
}

/// Pattern validation intentionally remains lexical and shared with formatting.
fn validate_pattern(pattern: &str) -> Result<(), ScalarError> {
    fn invalid() -> ScalarError {
        ScalarError::builtin("parse")
    }
    const NUMERIC: &str = "CdefGHIjklmMqsSuUVwWyYg";
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            continue;
        }
        let Some(mut spec) = chars.next() else {
            return Err(invalid());
        };
        if spec == '%' {
            continue;
        }
        let pad = matches!(spec, '-' | '0' | '_');
        let alternate = spec == '#';
        if pad || alternate {
            spec = chars.next().ok_or_else(invalid)?;
            if matches!(spec, '-' | '0' | '_' | '#') {
                return Err(invalid());
            }
        }
        if spec == ':' {
            if pad || alternate {
                return Err(invalid());
            }
            let mut colons = 1;
            while chars.peek() == Some(&':') {
                chars.next();
                colons += 1;
            }
            if colons > 3 || chars.next() != Some('z') {
                return Err(invalid());
            }
            continue;
        }
        // Numeric widths are supported only for the three fractional forms.
        // Consume all digits before deciding, so no digit-led directive can
        // fall through to the ordinary specifier set below.
        let mut width = 0usize;
        let mut has_width = false;
        let mut leading_zero = false;
        while spec.is_ascii_digit() {
            if !has_width {
                leading_zero = spec == '0';
            }
            has_width = true;
            width = width
                .saturating_mul(10)
                .saturating_add(usize::from(spec as u8 - b'0'));
            spec = chars.next().ok_or_else(invalid)?;
        }
        if has_width {
            if leading_zero || spec != 'f' || pad || alternate || !matches!(width, 3 | 6 | 9) {
                return Err(invalid());
            }
            continue;
        }
        if spec == '.' {
            if pad || alternate {
                return Err(invalid());
            }
            match chars.next() {
                Some('f') => continue,
                Some('3' | '6' | '9') if chars.next() == Some('f') => continue,
                _ => return Err(invalid()),
            }
        }
        if alternate && spec != 'z' {
            return Err(invalid());
        }
        if pad && !NUMERIC.contains(spec) {
            return Err(invalid());
        }
        if !"aAbBcCdDe fFgGhHIjklmMn pPqrRsStTuUvVwWxXyYzZ+"
            .replace(' ', "")
            .contains(spec)
        {
            return Err(invalid());
        }
    }
    Ok(())
}

fn compatible_format(pattern: &str, ms: i64) -> Result<String, ScalarError> {
    let f = CalendarFields::from_millis(ms)?;
    let year = f.year;
    let ordinal = days_from_civil(year, f.month, f.day) - days_from_civil(year, 1, 1) + 1;
    let sunday_weekday = (f.weekday + 1) % 7;
    let nanos = ms.rem_euclid(1000) * 1_000_000;
    let epoch_seconds = ms.div_euclid(1000);
    let names = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    let days = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ];
    let month_name = names[(f.month - 1) as usize];
    let weekday_name = days[f.weekday as usize];
    let year_string = |value: i32| -> String {
        if (0..=9999).contains(&value) {
            format!("{:04}", value)
        } else if value < 0 {
            format!("-{:04}", value.unsigned_abs())
        } else {
            format!("+{:04}", value)
        }
    };
    let iso_weekday = f.weekday + 1;
    let mut iso_year = year;
    let mut iso_week = (ordinal - iso_weekday + 10) / 7;
    let weeks_in_year = |y: i32| -> i64 {
        let jan1 = (days_from_civil(y, 1, 1) + 3).rem_euclid(7);
        if jan1 == 3 || (jan1 == 2 && (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0))) {
            53
        } else {
            52
        }
    };
    if iso_week < 1 {
        iso_year -= 1;
        iso_week = weeks_in_year(iso_year);
    } else if iso_week > weeks_in_year(year) {
        iso_year += 1;
        iso_week = 1;
    }
    let hour = format!("{:02}", f.hour);
    let minute = format!("{:02}", f.minute);
    let second = format!("{:02}", f.second);
    let clock = format!("{hour}:{minute}:{second}");
    let hour12 = if f.hour % 12 == 0 { 12 } else { f.hour % 12 };
    let hour12_padded = format!("{:02}", hour12);
    let ampm = if f.hour < 12 { "AM" } else { "PM" };
    let mut out = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        if chars.peek() == Some(&'%') {
            chars.next();
            out.push('%');
            continue;
        }
        let mut flag = None;
        let mut colons = 0;
        while matches!(chars.peek(), Some('-' | '_' | '0' | '#' | ':')) {
            let next = chars.next().ok_or_else(|| ScalarError::builtin("parse"))?;
            if next == ':' {
                colons += 1;
            } else {
                flag = Some(next);
            }
        }
        let mut width_text = String::new();
        while matches!(chars.peek(), Some(c) if c.is_ascii_digit()) {
            width_text.push(chars.next().ok_or_else(|| ScalarError::builtin("parse"))?);
        }
        let width = width_text.parse::<usize>().unwrap_or(0);
        if width > 9 {
            return Err(ScalarError::builtin("parse"));
        }
        let mut precision = None;
        let mut auto_precision = false;
        if chars.peek() == Some(&'.') {
            chars.next();
            let mut digits = String::new();
            while matches!(chars.peek(), Some(c) if c.is_ascii_digit()) {
                digits.push(chars.next().ok_or_else(|| ScalarError::builtin("parse"))?);
            }
            if digits.is_empty() {
                auto_precision = true;
            } else {
                precision = Some(digits.parse::<usize>().unwrap_or(0));
            }
        }
        let d = chars.next().ok_or_else(|| ScalarError::builtin("parse"))?;
        let pad_number = |value: i64, natural: usize| -> String {
            let text = value.to_string();
            let size = width.max(natural);
            if flag == Some('-') || text.len() >= size {
                return text;
            }
            let pad = if flag == Some('_') { ' ' } else { '0' };
            format!("{}{}", pad.to_string().repeat(size - text.len()), text)
        };
        let auto_fraction = || -> String {
            if nanos == 0 {
                String::new()
            } else if nanos % 1_000_000 == 0 {
                format!(".{:03}", nanos / 1_000_000)
            } else if nanos % 1_000 == 0 {
                format!(".{:06}", nanos / 1_000)
            } else {
                format!(".{:09}", nanos)
            }
        };
        let value = match d {
            'a' => weekday_name[..3].to_string(),
            'A' => weekday_name.to_string(),
            'b' | 'h' => month_name[..3].to_string(),
            'B' => month_name.to_string(),
            'c' => format!(
                "{} {} {:2} {} {}",
                &weekday_name[..3],
                &month_name[..3],
                f.day,
                clock,
                year_string(year)
            ),
            'C' => pad_number(i64::from(year.div_euclid(100)), 2),
            'd' => pad_number(i64::from(f.day), 2),
            'D' => format!("{:02}/{:02}/{:02}", f.month, f.day, year.rem_euclid(100)),
            'e' => format!("{:>2}", f.day),
            'F' => format!("{}-{:02}-{:02}", year_string(year), f.month, f.day),
            'f' => {
                if precision.is_none() && width == 0 {
                    match flag {
                        Some('-') => nanos.to_string(),
                        Some('_') => format!("{:9}", nanos),
                        _ => format!("{:09}", nanos),
                    }
                } else {
                    let digits = precision
                        .or(if width > 0 { Some(width) } else { None })
                        .unwrap_or(9)
                        .min(9);
                    format!("{:09}", nanos)
                        .chars()
                        .take(digits)
                        .collect::<String>()
                }
            }
            'g' => format!("{:02}", iso_year.rem_euclid(100)),
            'G' => year_string(iso_year),
            'H' => pad_number(i64::from(f.hour), 2),
            'I' => pad_number(i64::from(hour12), 2),
            'j' => format!("{:03}", ordinal),
            'k' => format!("{:>2}", f.hour),
            'l' => format!("{:>2}", hour12),
            'm' => pad_number(i64::from(f.month), 2),
            'M' => pad_number(i64::from(f.minute), 2),
            'n' => "\n".to_string(),
            'p' => ampm.to_string(),
            'P' => ampm.to_ascii_lowercase(),
            'q' => ((f.month - 1) / 3 + 1).to_string(),
            'r' => format!("{}:{}:{} {}", hour12_padded, minute, second, ampm),
            'R' => format!("{}:{}", hour, minute),
            's' => epoch_seconds.to_string(),
            'S' => pad_number(i64::from(f.second), 2),
            't' => "\t".to_string(),
            'T' | 'X' => format!("{}:{}:{}", hour, minute, second),
            'u' => (f.weekday + 1).to_string(),
            'U' => format!("{:02}", (ordinal + 6 - sunday_weekday) / 7),
            'V' => format!("{:02}", iso_week),
            'v' => format!("{:2}-{}-{}", f.day, &month_name[..3], year_string(year)),
            'W' => format!("{:02}", (ordinal + 6 - f.weekday) / 7),
            'w' => sunday_weekday.to_string(),
            'x' => format!("{:02}/{:02}/{:02}", f.month, f.day, year.rem_euclid(100)),
            'y' => format!("{:02}", year.rem_euclid(100)),
            'Y' => year_string(year),
            'z' => {
                if flag == Some('#') {
                    return Err(ScalarError::builtin("parse"));
                }
                match colons {
                    1 => "+00:00".to_string(),
                    2 => "+00:00:00".to_string(),
                    3 => "+00".to_string(),
                    _ => "+0000".to_string(),
                }
            }
            'Z' => "UTC".to_string(),
            '+' => format!(
                "{}-{:02}-{:02}T{}:{}:{}{}+00:00",
                year_string(year),
                f.month,
                f.day,
                hour,
                minute,
                second,
                auto_fraction()
            ),
            '%' => "%".to_string(),
            _ => return Err(ScalarError::builtin("parse")),
        };
        let value = if d == 'f' && auto_precision {
            auto_fraction()
        } else if d == 'f' && precision.is_some() {
            format!(".{}", value)
        } else if d == 'f' {
            value
        } else if let Some(p) = precision {
            value.chars().take(p).collect::<String>()
        } else {
            value
        };
        if width > value.len() && !matches!(d, 'n' | 't') {
            out.push_str(&format!("{:>width$}", value, width = width));
        } else {
            out.push_str(&value);
        }
    }
    Ok(out)
}

fn parse_rfc3339_millis(input: &str) -> Result<i64, ScalarError> {
    fn parse_error() -> ScalarError {
        ScalarError::builtin("parse")
    }
    fn digits(bytes: &[u8], start: usize, len: usize) -> Result<i64, ScalarError> {
        let end = start.checked_add(len).ok_or_else(parse_error)?;
        let part = bytes.get(start..end).ok_or_else(parse_error)?;
        if !part.iter().all(u8::is_ascii_digit) {
            return Err(parse_error());
        }
        Ok(part
            .iter()
            .fold(0i64, |value, digit| value * 10 + i64::from(*digit - b'0')))
    }

    let bytes = input.as_bytes();
    if bytes.len() < 20 {
        return Err(parse_error());
    }
    let year = digits(bytes, 0, 4)? as i32;
    if bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || !matches!(bytes.get(10), Some(b'T' | b't' | b' '))
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return Err(parse_error());
    }
    let month = digits(bytes, 5, 2)? as i32;
    let day = digits(bytes, 8, 2)? as i32;
    let hour = digits(bytes, 11, 2)?;
    let minute = digits(bytes, 14, 2)?;
    let second = digits(bytes, 17, 2)?;
    let leap = second == 60;
    if !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 60 {
        return Err(parse_error());
    }
    let leap_year = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = [
        31,
        if leap_year { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if day < 1 || day > month_days[(month - 1) as usize] {
        return Err(parse_error());
    }

    let mut pos = 19;
    let mut fraction_nanos = 0i64;
    if bytes.get(pos) == Some(&b'.') {
        pos += 1;
        let start = pos;
        let fractional = &bytes[start..];
        let digit_count = fractional
            .iter()
            .position(|byte| !byte.is_ascii_digit())
            .unwrap_or(fractional.len());
        if digit_count == 0 {
            return Err(parse_error());
        }
        for digit in &fractional[..digit_count.min(9)] {
            fraction_nanos = fraction_nanos * 10 + i64::from(*digit - b'0');
        }
        for _ in digit_count.min(9)..9 {
            fraction_nanos *= 10;
        }
        pos += digit_count;
    }
    let suffix = &input[pos..];
    let (offset_start, negative) = if suffix.starts_with('−') {
        (pos + '−'.len_utf8(), true)
    } else {
        match bytes.get(pos) {
            Some(b'Z' | b'z') if pos + 1 == bytes.len() => (bytes.len(), false),
            Some(b'+') => (pos + 1, false),
            Some(b'-') => (pos + 1, true),
            _ => return Err(parse_error()),
        }
    };
    let offset_seconds = if matches!(suffix, "+" | "-" | "−") {
        return Err(parse_error());
    } else if offset_start == bytes.len() {
        0
    } else {
        if offset_start + 5 != bytes.len() || bytes.get(offset_start + 2) != Some(&b':') {
            return Err(parse_error());
        }
        let offset_hour = digits(bytes, offset_start, 2)?;
        let offset_minute = digits(bytes, offset_start + 3, 2)?;
        if offset_hour > 23 || offset_minute > 59 {
            return Err(parse_error());
        }
        let magnitude = offset_hour * 3600 + offset_minute * 60;
        if negative {
            -magnitude
        } else {
            magnitude
        }
    };

    let days = days_from_civil(year, month, day);
    let local_seconds = days
        .checked_mul(86_400)
        .and_then(|v| v.checked_add(hour * 3_600 + minute * 60 + if leap { 59 } else { second }))
        .ok_or_else(|| ScalarError::builtin("out_of_range"))?;
    let utc_seconds = local_seconds
        .checked_sub(offset_seconds)
        .ok_or_else(|| ScalarError::builtin("out_of_range"))?;
    let millis = utc_seconds
        .checked_mul(1_000)
        .and_then(|v| v.checked_add(fraction_nanos / 1_000_000))
        .and_then(|v| if leap { v.checked_add(1_000) } else { Some(v) })
        .ok_or_else(|| ScalarError::builtin("out_of_range"))?;
    ensure_civil_year_range(millis, "out_of_range")?;
    Ok(millis)
}

/// Register the `/datetime` functions.
pub fn register(reg: &mut ScalarRegistry) {
    // ---- non-deterministic / impure: wall-clock readers --------------------
    reg.register(
        "now",
        FnEntry {
            f: std::sync::Arc::new(|_a: &[_]| Ok(v_int(Timestamp::now().as_millisecond()))),
            min_args: 0,
            max_args: Some(0),
            pure: false,
            deterministic: false,
            trusted_pure: false,
        },
    );
    reg.register(
        "age",
        FnEntry {
            f: std::sync::Arc::new(|a: &[_]| {
                let then = arg_i64(a, 0)?;
                let diff = Timestamp::now()
                    .as_millisecond()
                    .checked_sub(then)
                    .ok_or_else(|| ScalarError::builtin("out_of_range"))?;
                Ok(v_int(diff / 1000))
            }),
            min_args: 1,
            max_args: Some(1),
            pure: false,
            deterministic: false,
            trusted_pure: false,
        },
    );

    // ---- epoch conversions -------------------------------------------------
    reg.register(
        "to_epoch_s",
        FnEntry::pure(|a| Ok(v_int(div_floor(arg_i64(a, 0)?, 1000))), 1, Some(1)),
    );
    reg.register(
        "to_epoch_ms",
        FnEntry::pure(|a| Ok(v_int(arg_i64(a, 0)?)), 1, Some(1)),
    );
    reg.register(
        "from_epoch_s",
        FnEntry::pure(
            |a| {
                arg_i64(a, 0)?
                    .checked_mul(1000)
                    .map(v_int)
                    .ok_or_else(|| ScalarError::builtin("out_of_range"))
            },
            1,
            Some(1),
        ),
    );
    reg.register(
        "from_epoch_ms",
        FnEntry::pure(|a| Ok(v_int(arg_i64(a, 0)?)), 1, Some(1)),
    );

    // ---- RFC-3339 parse / format ------------------------------------------
    reg.register(
        "parse_rfc3339",
        FnEntry::pure(
            |a| {
                let s = arg_str(a, 0)?;
                let timestamp = parse_rfc3339_millis(s)?;
                Ok(v_int(timestamp))
            },
            1,
            Some(1),
        ),
    );
    reg.register(
        "format_rfc3339",
        FnEntry::pure(
            |a| {
                let fields = CalendarFields::from_millis(arg_i64(a, 0)?)?;
                let year = if (0..=9999).contains(&fields.year) {
                    format!("{:04}", fields.year)
                } else if fields.year < 0 {
                    format!("-{:04}", fields.year.unsigned_abs())
                } else {
                    format!("+{:04}", fields.year)
                };
                let fraction = if fields.millisecond == 0 {
                    String::new()
                } else {
                    format!(".{:03}", fields.millisecond)
                };
                Ok(v_str(format!(
                    "{}-{:02}-{:02}T{:02}:{:02}:{:02}{}+00:00",
                    year,
                    fields.month,
                    fields.day,
                    fields.hour,
                    fields.minute,
                    fields.second,
                    fraction
                )))
            },
            1,
            Some(1),
        ),
    );

    // ---- strftime-pattern parse / format ----------------------------------
    reg.register(
        "parse",
        FnEntry::pure(
            |a| {
                let s = arg_str(a, 0)?;
                let pattern = arg_str(a, 1)?;
                validate_pattern(pattern)?;
                let parsed = CivilDateTime::strptime(pattern, s).or_else(|_| {
                    jiff::civil::Date::strptime(pattern, s).map(|date| date.at(0, 0, 0, 0))
                });
                let parsed = match parsed {
                    Ok(value) => value,
                    Err(_) => {
                        let Some((input_prefix, pattern_prefix)) =
                            super::parse_rescue::rescue(s, pattern)
                        else {
                            return Err(ScalarError::builtin("parse"));
                        };
                        CivilDateTime::strptime(&pattern_prefix, &input_prefix)
                            .or_else(|_| {
                                jiff::civil::Date::strptime(&pattern_prefix, &input_prefix)
                                    .map(|date| date.at(0, 0, 0, 0))
                            })
                            .map_err(|_| ScalarError::builtin("parse"))?
                    }
                };
                let date = parsed.date();
                let time = parsed.time();
                let days = days_from_civil(
                    i32::from(date.year()),
                    i32::from(date.month()),
                    i32::from(date.day()),
                );
                let millis = days
                    .checked_mul(86_400_000)
                    .and_then(|value| value.checked_add(i64::from(time.hour()) * 3_600_000))
                    .and_then(|value| value.checked_add(i64::from(time.minute()) * 60_000))
                    .and_then(|value| value.checked_add(i64::from(time.second()) * 1_000))
                    .and_then(|value| {
                        value.checked_add(i64::from(time.subsec_nanosecond() / 1_000_000))
                    })
                    .ok_or_else(|| ScalarError::builtin("parse"))?;
                ensure_civil_year_range(millis, "parse")?;
                Ok(v_int(millis))
            },
            2,
            Some(2),
        ),
    );
    reg.register(
        "format",
        FnEntry::pure(
            |a| {
                let millis = arg_i64(a, 0)?;
                let pattern = arg_str(a, 1)?;
                ensure_civil_year_range(millis, "out_of_range")?;
                validate_pattern(pattern)?;
                compatible_format(pattern, millis).map(v_str)
            },
            2,
            Some(2),
        ),
    );

    // ---- component extractors ---------------------------------------------
    reg.register(
        "year",
        FnEntry::pure(
            |a| {
                Ok(v_int(
                    CalendarFields::from_millis(arg_i64(a, 0)?)?.year as i64,
                ))
            },
            1,
            Some(1),
        ),
    );
    reg.register(
        "month",
        FnEntry::pure(
            |a| {
                Ok(v_int(
                    CalendarFields::from_millis(arg_i64(a, 0)?)?.month as i64,
                ))
            },
            1,
            Some(1),
        ),
    );
    reg.register(
        "day",
        FnEntry::pure(
            |a| {
                Ok(v_int(
                    CalendarFields::from_millis(arg_i64(a, 0)?)?.day as i64,
                ))
            },
            1,
            Some(1),
        ),
    );
    reg.register(
        "hour",
        FnEntry::pure(
            |a| {
                Ok(v_int(
                    CalendarFields::from_millis(arg_i64(a, 0)?)?.hour as i64,
                ))
            },
            1,
            Some(1),
        ),
    );
    reg.register(
        "minute",
        FnEntry::pure(
            |a| {
                Ok(v_int(
                    CalendarFields::from_millis(arg_i64(a, 0)?)?.minute as i64,
                ))
            },
            1,
            Some(1),
        ),
    );
    reg.register(
        "second",
        FnEntry::pure(
            |a| {
                Ok(v_int(
                    CalendarFields::from_millis(arg_i64(a, 0)?)?.second as i64,
                ))
            },
            1,
            Some(1),
        ),
    );
    // weekday: 0 = Monday .. 6 = Sunday.
    reg.register(
        "weekday",
        FnEntry::pure(
            |a| Ok(v_int(CalendarFields::from_millis(arg_i64(a, 0)?)?.weekday)),
            1,
            Some(1),
        ),
    );
    reg.register(
        "is_weekend",
        FnEntry::pure(
            |a| {
                Ok(v_bool(
                    CalendarFields::from_millis(arg_i64(a, 0)?)?.weekday >= 5,
                ))
            },
            1,
            Some(1),
        ),
    );

    // ---- arithmetic --------------------------------------------------------
    reg.register(
        "add_secs",
        FnEntry::pure(
            |a| {
                let timestamp = arg_i64(a, 0)?;
                ensure_civil_year_range(timestamp, "out_of_range")?;
                let seconds = arg_i64(a, 1)?;
                let millis = seconds
                    .checked_mul(1000)
                    .and_then(|delta| timestamp.checked_add(delta))
                    .ok_or_else(|| ScalarError::builtin("out_of_range"))?;
                ensure_civil_year_range(millis, "out_of_range")?;
                Ok(v_int(millis))
            },
            2,
            Some(2),
        ),
    );
    reg.register(
        "add_days",
        FnEntry::pure(
            |a| {
                let timestamp = arg_i64(a, 0)?;
                ensure_civil_year_range(timestamp, "out_of_range")?;
                let days = arg_i64(a, 1)?;
                let millis = days
                    .checked_mul(86_400_000)
                    .and_then(|delta| timestamp.checked_add(delta))
                    .ok_or_else(|| ScalarError::builtin("out_of_range"))?;
                ensure_civil_year_range(millis, "out_of_range")?;
                Ok(v_int(millis))
            },
            2,
            Some(2),
        ),
    );
    reg.register(
        "diff_secs",
        FnEntry::pure(
            |a| {
                let x = arg_i64(a, 0)?;
                let y = arg_i64(a, 1)?;
                let diff = x
                    .checked_sub(y)
                    .ok_or_else(|| ScalarError::builtin("out_of_range"))?;
                Ok(v_int(div_floor(diff, 1000)))
            },
            2,
            Some(2),
        ),
    );

    // ---- truncation / period starts ---------------------------------------
    reg.register(
        "start_of_day",
        FnEntry::pure(
            |a| Ok(v_int(start_of_calendar_day(arg_i64(a, 0)?)?)),
            1,
            Some(1),
        ),
    );
    reg.register(
        "start_of_week",
        FnEntry::pure(
            |a| Ok(v_int(start_of_calendar_week(arg_i64(a, 0)?)?)),
            1,
            Some(1),
        ),
    );
    reg.register(
        "start_of_month",
        FnEntry::pure(
            |a| Ok(v_int(start_of_calendar_month(arg_i64(a, 0)?)?)),
            1,
            Some(1),
        ),
    );
    reg.register(
        "truncate",
        FnEntry::pure(
            |a| {
                let millis = arg_i64(a, 0)?;
                let fields = CalendarFields::from_millis(millis)?;
                let unit = arg_str(a, 1)?;
                let value = match unit {
                    "second" => Ok(millis.div_euclid(1_000) * 1_000),
                    "minute" => Ok(millis.div_euclid(60_000) * 60_000),
                    "hour" => Ok(millis.div_euclid(3_600_000) * 3_600_000),
                    "day" => start_of_calendar_day(millis),
                    "week" => start_of_calendar_week(millis),
                    "month" => start_of_calendar_month(millis),
                    "year" => start_of_calendar_year(millis, fields),
                    _ => return Err(ScalarError::builtin("bad_unit")),
                };
                Ok(v_int(value?))
            },
            2,
            Some(2),
        ),
    );
}

fn div_floor(n: i64, d: i64) -> i64 {
    n.div_euclid(d)
}
