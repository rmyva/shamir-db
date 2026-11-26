use std::sync::Arc;

use crate::drainer::Drainer;
use shamir_tx::RepoTxGate;
use shamir_wal::WalEntryV2;

/// Re-scan the contiguous window prefix `[dur+1 ..= vis]` after a WAL reseed.
/// Synchronous on purpose: the non-`Send` scc guard must never be live across
/// the `.await` that follows in the drainer pass.
pub(in crate::drain::drainer) fn rescan_window_prefix(
    drainer: &Drainer,
    gate: &RepoTxGate,
    dur: u64,
    vis: u64,
    visible: u64,
    out: &mut Vec<Arc<WalEntryV2>>,
) {
    let guard = scc::Guard::new();
    for (expected, (k, v)) in (dur + 1..).zip(drainer.window.range(dur + 1..=vis, &guard)) {
        if *k != expected {
            break; // still a genuine gap after the one reseed
        }
        if gate.is_active_post_wal(*k) || (*k > visible && !gate.is_orphaned_post_wal(*k)) {
            break; // in-flight entry: stop, never reseed again
        }
        out.push(Arc::clone(v));
        if gate.is_orphaned_post_wal(*k) {
            break;
        }
    }
}
