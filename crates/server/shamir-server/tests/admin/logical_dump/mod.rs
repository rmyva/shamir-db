//! Offline logical-dump CI tests (R3 step 4): shared fixtures and the
//! budget/pinned-property tests built on them, plus the ONLINE export job
//! tests (R3 step 2b). Manifest only — no logic.

#[path = "budget_tests.rs"]
mod budget_tests;
#[path = "fixture.rs"]
mod fixture;
#[path = "online_tests.rs"]
mod online_tests;
#[path = "pinned_properties.rs"]
mod pinned_properties;
#[path = "reserved_name_refusal_tests.rs"]
mod reserved_name_refusal_tests;
