use crate::backend::{IndexQuery, IndexResult};
use crate::kind::IndexKind;
use std::mem::size_of;

/// Hot-path enums must not grow without explicit review.
/// If you legitimately need to bump a bound, update it here and
/// document why in the commit message.
#[test]
fn enum_sizes_under_limits() {
    assert!(
        size_of::<IndexKind>() <= 80,
        "IndexKind: {}",
        size_of::<IndexKind>()
    );
    // IndexQuery is created once per request (not per record),
    // so a slightly larger size is acceptable. Range carries two
    // `Bound<Vec<u8>>` (~80 bytes); Vector carries `Vec<f32>`.
    assert!(
        size_of::<IndexQuery>() <= 128,
        "IndexQuery: {}",
        size_of::<IndexQuery>()
    );
    assert!(
        size_of::<IndexResult>() <= 64,
        "IndexResult: {}",
        size_of::<IndexResult>()
    );
}
