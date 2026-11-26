//! Shard-acquisition check for `TableCatalog::table_registration` of an
//! ABSENT name (the sibling `catalog_registration_tests.rs` pins the
//! sequential contract).
//!
//! WHY the seam: the only rendezvous at the config/entry boundary is an
//! externally held `tables` dashmap entry guard (the shard write lock), and
//! the `tables` field is private to `support::storage::catalog`. The
//! `tables_entry_guard_for_test` seam (`cfg(test)`, crate-private) opens
//! exactly that item: the guard is the other side of the race, replacing the
//! unschedulable mid-accessor `DROP` of the old implementation.

use shamir_types::types::common::{new_dash_map, THasher};
use std::sync::mpsc;
use std::time::Duration;
use tokio::sync::OnceCell;

use crate::TableCatalog;

/// How long the negative check waits for a premature completion.
const NEGATIVE_WAIT: Duration = Duration::from_secs(2);
/// Absolute bound for the worker to finish after the entry is released.
const RELEASE_DEADLINE: Duration = Duration::from_secs(120);

fn new_catalog() -> TableCatalog {
    TableCatalog::new(
        new_dash_map(),
        new_dash_map(),
        scc::HashMap::with_capacity_and_hasher(16, THasher::default()),
        scc::HashMap::with_capacity_and_hasher(16, THasher::default()),
        OnceCell::new(),
    )
}

/// Kills the reverted split-order accessor (`configs.contains_key` probe
/// first, `tables` get-or-insert second, no entry guard held across the
/// two reads): with that shape, `table_registration` of an unconfigured
/// name answers `None` immediately without ever touching the `tables`
/// entry, so the worker below completes and signals WHILE this thread
/// still holds the `tables` entry for the same name — the bounded
/// receive fails and the test goes red.
///
/// With the fixed accessor (entry taken FIRST, config checked under it),
/// the worker blocks on the `tables` shard lock until this thread drops
/// its guard, so the signal can only arrive after the drop and the
/// negative wait always times out. The wait is a one-sided check: the fixed
/// accessor can never complete while the guard is held (no flake), a
/// reverted one is caught whenever its thread runs within the window.
///
/// What it does NOT kill: an accessor that reads `configs` first but still
/// takes the `tables` entry before answering (it blocks here too), whose
/// stale config answer for a CONFIGURED name could race a `DROP`. The
/// config/entry boundary itself is not observed; a rendezvous inside the
/// accessor would be needed for that.
#[test]
fn registration_of_an_absent_name_waits_for_the_tables_shard() {
    let catalog = new_catalog();
    // Hold the `tables` shard lock for "ghost" (a vacant entry still owns
    // the shard's write lock; dropping it without inserting leaves no
    // cell behind).
    let held_entry = catalog.tables_entry_guard_for_test("ghost");
    let worker_catalog = catalog.clone();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let worker = std::thread::spawn(move || {
        // Fixed accessor: blocks on the entry above, then answers `None`
        // for the unconfigured name. Reverted accessor: answers `None`
        // immediately, without waiting for the entry.
        let answer = worker_catalog.table_registration("ghost");
        assert!(answer.is_none(), "an unconfigured name answers `None`");
        done_tx
            .send(())
            .expect("worker completion channel stays open");
    });
    // Negative wait — no handshake can express "has not completed yet";
    // this mirrors the bounded negative waits used by the runtime
    // teardown tests. The reverted accessor completes here (red); the
    // fixed accessor stays parked until `held_entry` drops below.
    assert!(
        done_rx.recv_timeout(NEGATIVE_WAIT).is_err(),
        "table_registration must block while another thread holds the \
         tables entry for the same name (entry-first ordering)"
    );
    drop(held_entry);
    // Bounded completion handshake before the join: the released shard lets
    // the worker finish, so a hang here is a real deadlock, reported with
    // the test's own deadline instead of the runner's kill.
    done_rx
        .recv_timeout(RELEASE_DEADLINE)
        .expect("the worker must finish once the tables entry is released");
    worker
        .join()
        .expect("worker joins after the entry is released");
    // Typed absence survives the rendezvous: still `None`, and no cell
    // was reserved (or resurrected) for the unconfigured name.
    assert!(catalog.table_registration("ghost").is_none());
    assert_eq!(
        catalog.table_cell_count(),
        0,
        "the rendezvous probe must not reserve a catalogue cell"
    );
}
