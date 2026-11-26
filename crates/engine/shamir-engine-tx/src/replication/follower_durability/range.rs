use std::ops::RangeInclusive;

/// Conservative interval potentially lost after an unclean follower stop.
/// `None` horizon means legacy/unknown metadata and retains the full range.
pub fn in_flight_range(
    clean_through: Option<u64>,
    recovered_end: u64,
) -> Option<RangeInclusive<u64>> {
    match clean_through {
        Some(horizon) if recovered_end <= horizon => None,
        Some(horizon) if horizon == u64::MAX => None,
        Some(horizon) => Some((horizon + 1)..=recovered_end),
        None if recovered_end > 0 => Some(1..=recovered_end),
        None => None,
    }
}
