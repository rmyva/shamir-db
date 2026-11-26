//! P1d-2a — unit tests for the background [`Drainer`].
//!
//! These drive `drain_step` / `drain_all` DIRECTLY (the drainer is not
//! wired into the commit path in P1d-2a). They seed the inflight WAL tail
//! by hand and prime the gate so visibility leads durability — exactly the
//! state P1d-2b's cutover produces (publish on the ack-path, history write
//! deferred). The drainer must then replay the not-yet-durable visible
//! prefix into history, advance `durable_watermark`, and (A5-gated) truncate
//! the markers.

mod commit_wire_tests;
mod drain_tests;
mod index_batch_tests;
pub(crate) mod support;
mod ticket_tests;
mod window_tests;
