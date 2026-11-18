//! Formerly in-crate `shamir-client` tests that need a real server, moved to this
//! integration target (public-API assertions only; the fake-transport tests live
//! back in `shamir-client`).

mod auth_identity_tests;
mod rust_migration_tests;
mod v2_passthrough_tests;
