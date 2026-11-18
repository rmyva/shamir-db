//! Manifest for the application-schema migration client regression tests
//! (brief `48-r1c-3a-rust-client-migrations.md`, task item 7). `mod.rs` is
//! a re-export manifest only; every behavioural proof lives in a sibling
//! file named for what it proves.

pub(crate) mod harness;

pub mod lost_apply_tests;
pub mod refusal_tests;
pub mod set_tests;
pub mod wire_ops_tests;
