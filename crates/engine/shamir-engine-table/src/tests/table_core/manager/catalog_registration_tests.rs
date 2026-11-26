//! `TableCatalog::table_registration` contract.
//!
//! Parent-commit defect: the accessor was a get-or-insert under TWO separate
//! map operations (`configs.contains_key`, then `insert_table_cell`). A drop
//! landing between them re-created an empty `tables` cell for an
//! already-unconfigured name, and a table that vanished between a caller's
//! earlier `table_config` read and this call answered "changed registration"
//! instead of "unknown table".
//!
//! The accessor now takes the `tables` entry FIRST and checks `configs` while
//! holding it, so `None` ⇔ "not configured" and nothing is inserted. These
//! tests pin that side of the contract with the cell-map size as the oracle
//! (`TableCatalog::table_cell_count`, which is compiled for this crate's own
//! unit tests).
//!
//! The shard acquisition of an ABSENT name is checked — not here, but by the
//! sibling test `catalog_registration_order_tests.rs
//! ::registration_of_an_absent_name_waits_for_the_tables_shard`,
//! which holds the `tables` dashmap entry guard from another thread through
//! the crate-private `cfg(test)` `tables_entry_guard_for_test` seam; the
//! config/entry boundary itself is not observed (this file uses only public
//! reads).

use shamir_types::types::common::{new_dash_map, THasher};
use tokio::sync::OnceCell;

use crate::{table_token_for, TableCatalog, TableConfig};

fn new_catalog() -> TableCatalog {
    TableCatalog::new(
        new_dash_map(),
        new_dash_map(),
        scc::HashMap::with_capacity_and_hasher(16, THasher::default()),
        scc::HashMap::with_capacity_and_hasher(16, THasher::default()),
        OnceCell::new(),
    )
}

/// An unconfigured name answers `None` and reserves NO `tables` cell — before
/// or after a second probe.
///
/// Kills: an accessor that `or_insert_with`s the entry BEFORE consulting
/// `configs` (the "insert unconditionally / check afterwards" mutation). It
/// would leave one cell per probed name, so the size assertion fails.
/// Does NOT fail on the parent commit: sequentially the parent also returns
/// `None` without inserting; only the drop-between-the-two-operations race
/// differs. This test is a contract guard.
#[test]
fn unconfigured_name_answers_none_and_reserves_no_cell() {
    let catalog = new_catalog();

    assert!(catalog.table_registration("ghost").is_none());
    assert_eq!(
        catalog.table_cell_count(),
        0,
        "an unconfigured name must not reserve a catalogue cell"
    );
    assert!(!catalog.has_table("ghost"));
    assert!(
        catalog.token_name_for(table_token_for("ghost")).is_none(),
        "an unconfigured name must not be inserted into the token index"
    );

    assert!(catalog.table_registration("ghost").is_none());
    assert_eq!(
        catalog.table_cell_count(),
        0,
        "repeated probes of an unconfigured name must not accumulate cells"
    );
}

/// A configured name answers `Some` twice with the SAME registration identity
/// (`same_registration`) and never instantiates the table.
///
/// Kills: an accessor that hands out a fresh token per call (e.g. wrapping a
/// newly built cell each time) — `same_registration` would be `false`. Also
/// pins the non-instantiating property: the cell stays dormant, so
/// `initialized_table` stays `None` after both probes.
/// Does NOT fail on the parent commit (the parent reserved from the same map
/// on demand); it pins the contract.
#[test]
fn configured_name_answers_one_stable_dormant_identity() {
    let catalog = new_catalog();
    catalog.register_table_with(TableConfig::new("users"), || {});

    let first = catalog
        .table_registration("users")
        .expect("a configured table has a registration");
    let second = catalog
        .table_registration("users")
        .expect("a configured table has a registration");

    assert!(
        first.same_registration(&second),
        "two reads of one registration must share the catalogue cell"
    );
    assert!(
        catalog.initialized_table("users").is_none(),
        "reading the registration must not instantiate the table"
    );
    assert_eq!(
        catalog.table_cell_count(),
        1,
        "the configured table owns exactly one catalogue cell"
    );
}

/// `remove_table_with` drops the config AND the cell, so the accessor then
/// answers `None` and does not resurrect an empty cell.
///
/// Kills: an accessor that inserts the cell before consulting `configs` (it
/// would resurrect a cell here and answer `Some` on a later probe). Fails on
/// the parent commit only through that mutation shape: the parent's
/// `contains_key` guard also answers `None` with no insert, so as written the
/// parent passes this test — it is a contract guard for the removal path.
#[test]
fn removed_table_answers_none_and_leaves_no_cell() {
    let catalog = new_catalog();
    catalog.register_table_with(TableConfig::new("orders"), || {});
    assert!(catalog.table_registration("orders").is_some());
    assert_eq!(catalog.table_cell_count(), 1);

    assert!(catalog.remove_table_with("orders", || {}));

    assert!(catalog.table_registration("orders").is_none());
    assert!(!catalog.has_table("orders"));
    assert_eq!(
        catalog.table_cell_count(),
        0,
        "a removed table must leave no catalogue cell behind"
    );
}
