//! Shared close shape for the 1c gate tests: the old combined
//! close-and-wait call, rebuilt on the ungated production entry points
//! (`begin_close` + `wait_zero`) after R2a-1d split them.

// Test-only module: compiled empty when the crate is built with `test-util` alone.
#![cfg(any(test, feature = "test-util"))]

use std::sync::Arc;
use std::time::Duration;

use super::super::close_owner::CommitAdmissionCloseOwner;
use super::super::error::CommitAdmissionError;
use super::super::gate::CommitAdmissionGate;

/// Close and await zero pre-close-admitted leases (unbounded, the old
/// `close()` shape); the owner reopens on drop.
#[cfg(any(test, feature = "test-util"))]
pub async fn close(
    gate: &Arc<CommitAdmissionGate>,
) -> Result<CommitAdmissionCloseOwner, CommitAdmissionError> {
    let owner = gate.begin_close()?;
    owner.wait_zero(Duration::MAX).await?;
    Ok(owner)
}
