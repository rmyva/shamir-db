//! Named format checks (`email` / `url` / `uuid` / `date`) for Phase B.
//!
//! Each [`FormatKind`] maps to a pure predicate over a `&str`.  Where a
//! funclib validator already exists (`is_email` / `is_url` / `is_uuid`),
//! the *same* compiled regex is reused via a `LazyLock` mirror so the two
//! paths cannot drift.  `date` accepts either a full RFC-3339 timestamp
//! (`2024-01-31T08:30:00Z`) or a bare calendar date (`2024-01-31`).

use regex::Regex;
use std::sync::LazyLock;

/// Named string format recognised by declarative schema rules.
///
/// The variant set is intentionally small and covers the four formats the
/// design doc (`09-builtin-checks.md`) names for Phase B.  Unknown / custom
/// formats go through the `scalar` escape-hatch instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatKind {
    /// `email` — RFC-ish email address (`is_email` pattern).
    Email,
    /// `url` — `http(s)://` URL (`is_url` pattern).
    Url,
    /// `uuid` — canonical 8-4-4-4-12 hex form (`is_uuid` pattern).
    Uuid,
    /// `date` — RFC-3339 timestamp or bare `YYYY-MM-DD` calendar date.
    Date,
}

impl FormatKind {
    /// Parse a format name from its string form (case-insensitive).
    ///
    /// Returns `None` for unknown names — callers should surface this as a
    /// schema-compilation error rather than silently accepting.
    pub fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "email" => Some(Self::Email),
            "url" => Some(Self::Url),
            "uuid" => Some(Self::Uuid),
            "date" => Some(Self::Date),
            _ => None,
        }
    }

    /// The string name of this format (lowercase, stable for error codes).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Email => "email",
            Self::Url => "url",
            Self::Uuid => "uuid",
            Self::Date => "date",
        }
    }

    /// Validate `value` against this format.  Returns `true` if the value
    /// matches the format's grammar.
    ///
    /// Reuses the same compiled regexes as the funclib `validate` category
    /// (`is_email` / `is_url` / `is_uuid`) so the two paths cannot drift;
    /// `date` accepts either a full RFC-3339 timestamp or a bare
    /// `YYYY-MM-DD` calendar date (a light regex plus a day-of-month
    /// range sanity check, sufficient for schema-level rejection).
    pub fn matches(self, value: &str) -> bool {
        match self {
            Self::Email => EMAIL_RE.is_match(value),
            Self::Url => URL_RE.is_match(value),
            Self::Uuid => UUID_RE.is_match(value),
            Self::Date => is_date_like(value),
        }
    }
}

// ── compiled patterns (mirrors funclib::validate) ───────────────────────────
//
// These are byte-identical to the patterns in `crates/query/shamir-funclib/src/
// validate.rs`.  We keep a private mirror (rather than reaching into the
// funclib crate's `LazyLock`) because (a) funclib does not expose them as
// public items, and (b) the patterns are tiny and stable — duplication is
// cheaper than a new public surface that would have to stay frozen.  A drift
// test in `format_tests.rs` cross-checks a sample of values against the
// funclib scalars to catch accidental divergence.

#[expect(
    clippy::unwrap_used,
    reason = "guard: these literals are fixed regex patterns; no runtime input is compiled"
)]
static EMAIL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$").unwrap());

#[expect(
    clippy::unwrap_used,
    reason = "guard: these literals are fixed regex patterns; no runtime input is compiled"
)]
static URL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^https?://[^\s/$.?#][^\s]*$").unwrap());

#[expect(
    clippy::unwrap_used,
    reason = "guard: these literals are fixed regex patterns; no runtime input is compiled"
)]
static UUID_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
        .unwrap()
});

/// RFC-3339 date-time OR bare `YYYY-MM-DD`.
///
/// The full-timestamp shape is matched structurally (not parsed) — a value
/// that matches the RFC-3339 date-time shape encoded here (date, `T`, time,
/// offset). The bare-date shape adds a day-of-month range sanity check so
/// `2024-02-31` is rejected even though the regex alone would accept it.
fn is_date_like(s: &str) -> bool {
    // Full RFC-3339 date-time: YYYY-MM-DDThh:mm:ss with optional fractional
    // seconds and a timezone (Z or ±hh:mm).
    if RFC3339_RE.is_match(s) {
        return true;
    }
    // Bare calendar date: YYYY-MM-DD with a plausible day-of-month.
    if let Some((y, m, d)) = parse_bare_date(s) {
        return day_is_valid(y, m, d);
    }
    false
}

#[expect(
    clippy::unwrap_used,
    reason = "guard: these literals are fixed regex patterns; no runtime input is compiled"
)]
static RFC3339_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$").unwrap()
});

#[expect(
    clippy::unwrap_used,
    reason = "guard: these literals are fixed regex patterns; no runtime input is compiled"
)]
static BARE_DATE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(\d{4})-(\d{2})-(\d{2})$").unwrap());

fn parse_bare_date(s: &str) -> Option<(i32, u32, u32)> {
    let caps = BARE_DATE_RE.captures(s)?;
    let y = caps[1].parse().ok()?;
    let m = caps[2].parse().ok()?;
    let d = caps[3].parse().ok()?;
    Some((y, m, d))
}

/// Whether `(year, month, day)` is a valid Gregorian calendar date.
///
/// A small in-process Gregorian date check keeps validation independent of
/// date/time dependencies: months 1..=12, days 1..=days_in_month(year, month).
fn day_is_valid(year: i32, month: u32, day: u32) -> bool {
    if month == 0 || month > 12 || day == 0 {
        return false;
    }
    day <= days_in_month(year, month)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn is_leap(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}
