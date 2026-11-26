//! `activity/` — R4b-1 in-flight request table and the `ListActivity`
//! serving path for the operational-views activity view.
//!
//! - [`requests`] — the in-flight table (`scc::HashIndex`: lock-free
//!   readers; writers take brief internal bucket locks), the RAII
//!   registration guard (which also owns the per-session in-flight
//!   request slot), the fixed-text `server_busy` rejection, the gauge
//!   mirror and the state word.
//! - [`list`] — the `ListActivity` paging implementation, as inherent
//!   methods on `ShamirDbHandler`.
//! - `cancel` — the `CancelRequest` / `CancelTx` / `CancelCursorAdmin`
//!   workers, their ownership/permission rule, their detached audit path
//!   and the per-server outcome counters.
//! - `shapes` — the R4b step 4 shape-statistics surface (`ListShapeStats` /
//!   `ResetShapeStats`), the second operational-views admin surface.
//! - `abort_release` — the Level-3 record-lock releaser the registry calls
//!   from `InteractiveTx::drop`.

mod abort_release;
mod cancel;
mod continuation;
mod requests;
// The two serving surfaces live in `serving/` (the folder entry limit); the
// `#[path]` attributes keep their module paths and every `super::` unchanged.
#[path = "serving/shapes.rs"]
mod shapes;

// The paging implementation (`list.rs`) implements `list_activity` as an
// method on `ShamirDbHandler`; no `pub` module re-export is needed.
#[path = "serving/list.rs"]
pub(in crate::engine::db_handler) mod list;

// The guard and entry are named directly only by registry tests.
pub(in crate::engine::db_handler) use abort_release::tx_abort_release;
pub(in crate::engine::db_handler) use cancel::CancelCounters;
#[cfg(test)]
pub(in crate::engine::db_handler) use cancel::CancelPauseTarget;
pub(in crate::engine::db_handler) use requests::server_busy_response;
pub(in crate::engine::db_handler) use requests::ActivityRequests;

// Unconditional (same `pub(in crate::engine::db_handler)` reach as the
// definitions): the export-activity projection in `handler/` also names
// these two types, not only registry tests.
pub(in crate::engine::db_handler) use requests::{InFlightRequest, RequestEntry};

#[cfg(test)]
mod tests;
