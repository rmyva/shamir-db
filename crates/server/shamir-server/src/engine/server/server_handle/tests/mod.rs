//! Unit tests for the release-tail drain bound (EXPORT-ONLINE-TAILS round 2,
//! finding 3c): the drain must honour the STAGE deadline passed by the
//! caller, never start a fresh clock of its own.
mod release_tail_drain_tests;
