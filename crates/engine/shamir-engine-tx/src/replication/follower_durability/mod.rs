mod range;
pub mod receipt;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub mod test_seam;

pub use range::in_flight_range;
