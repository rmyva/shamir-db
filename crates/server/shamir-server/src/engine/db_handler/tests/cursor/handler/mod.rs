//! FG-5b — behavioral tests for `ShamirDbHandler::{create_cursor, fetch_next,
//! cancel_cursor}`, exercised through the wire `RequestHandler::handle`
//! entry point (mirrors `node_mode_tests.rs`'s harness style: a real
//! in-memory `ShamirDb` with an owned table, wire msgpack round-trip).
//! Declarations/re-exports only; the fixtures live in `common`.

pub mod common;
pub mod data_shape;
pub mod lifecycle;
pub mod pagination;
pub mod terminal;
