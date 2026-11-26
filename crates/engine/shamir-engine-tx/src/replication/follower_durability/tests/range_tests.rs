use super::super::in_flight_range;

#[test]
fn known_horizon_limits_uncertainty_to_later_versions() {
    assert_eq!(in_flight_range(Some(7), 10), Some(8..=10));
    assert_eq!(in_flight_range(Some(10), 10), None);
}

#[test]
fn unknown_horizon_preserves_conservative_range() {
    assert_eq!(in_flight_range(None, 10), Some(1..=10));
    assert_eq!(in_flight_range(None, 0), None);
}

#[test]
fn maximum_horizon_does_not_overflow() {
    assert_eq!(in_flight_range(Some(u64::MAX), u64::MAX), None);
}
