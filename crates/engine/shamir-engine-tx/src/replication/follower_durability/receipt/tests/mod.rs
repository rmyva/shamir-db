#![cfg(feature = "fjall")]

//! Receipt tests (S5c-X PR6b-2c, W5): apply pipeline, checkpoint-Uncertain
//! reconcile, caller-cancellation convergence, kill-subprocess crash matrix,
//! and the write-domain DDL interlock.

mod activation_routing_tests;
mod cancellation_tests;
mod checkpoint_uncertain_tests;
mod crash_subprocess_tests;
mod ddl_interlock_tests;
mod receipt_apply_tests;
