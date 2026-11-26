#[cfg(test)]
use std::sync::Mutex;

/// One-shot probe fired immediately before the owner starts awaiting the
/// scheduled worker's completion signal. Test-only: a shutdown test that
/// parks the worker waits for this marker instead of guessing how long the
/// earlier teardown stages take, so "shutdown must not return while the
/// worker is parked" can never pass vacuously on a slow runner.
#[cfg(test)]
static SCHEDULED_JOIN_PROBE: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

/// Clears an unfired [`arm_scheduled_join_probe`] on drop; a probe that fires
/// removes itself from the slot.
#[cfg(test)]
pub(crate) struct ScheduledJoinProbeGuard;

#[cfg(test)]
impl Drop for ScheduledJoinProbeGuard {
    fn drop(&mut self) {
        *SCHEDULED_JOIN_PROBE.lock().expect("scheduled join probe") = None;
    }
}

#[cfg(test)]
pub(crate) fn arm_scheduled_join_probe(
    probe: impl FnOnce() + Send + 'static,
) -> ScheduledJoinProbeGuard {
    *SCHEDULED_JOIN_PROBE.lock().expect("scheduled join probe") = Some(Box::new(probe));
    ScheduledJoinProbeGuard
}

#[cfg(test)]
pub(in crate::engine::server) fn fire_scheduled_join_probe() {
    let probe = SCHEDULED_JOIN_PROBE
        .lock()
        .expect("scheduled join probe")
        .take();
    if let Some(probe) = probe {
        probe();
    }
}

/// One-shot marker for the post-deadline fallback join in `ShutdownOwner::drain`.
#[cfg(test)]
static FALLBACK_DRAIN_JOIN_PROBE: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

#[cfg(test)]
pub(crate) struct FallbackDrainJoinProbeGuard;

#[cfg(test)]
impl Drop for FallbackDrainJoinProbeGuard {
    fn drop(&mut self) {
        *FALLBACK_DRAIN_JOIN_PROBE
            .lock()
            .expect("fallback drain join probe") = None;
    }
}

#[cfg(test)]
pub(crate) fn arm_fallback_drain_join_probe(
    probe: impl FnOnce() + Send + 'static,
) -> FallbackDrainJoinProbeGuard {
    *FALLBACK_DRAIN_JOIN_PROBE
        .lock()
        .expect("fallback drain join probe") = Some(Box::new(probe));
    FallbackDrainJoinProbeGuard
}

#[cfg(test)]
fn fire_fallback_drain_join_probe() {
    let probe = FALLBACK_DRAIN_JOIN_PROBE
        .lock()
        .expect("fallback drain join probe")
        .take();
    if let Some(probe) = probe {
        probe();
    }
}

/// Mark entry into the post-expiry owner drain (test-only).
#[cfg(test)]
pub(crate) fn mark_fallback_drain_join() {
    fire_fallback_drain_join_probe();
}

/// One-shot probe fired immediately BEFORE the ordered shutdown's REAL
/// repository flush call (`ShamirDb::flush_all_report`) — never at the point
/// the flush would have started, so a SKIPPED flush leaves the probe unfired
/// and cannot look like an entered one. A shutdown test compares its order with
/// [`arm_scheduled_join_probe`]: the scheduled worker and its in-flight run are
/// joined BEFORE the flush, so the join marker must fire first (mutant: the
/// worker awaited after the flush).
#[cfg(test)]
static REPO_FLUSH_PROBE: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

/// Clears an unfired [`arm_repo_flush_probe`]; a probe that fires removes
/// itself.
#[cfg(test)]
pub(crate) struct RepoFlushProbeGuard;

#[cfg(test)]
impl Drop for RepoFlushProbeGuard {
    fn drop(&mut self) {
        *REPO_FLUSH_PROBE.lock().expect("repo flush probe") = None;
    }
}

/// Arm ONE repository-flush probe.
#[cfg(test)]
pub(crate) fn arm_repo_flush_probe(probe: impl FnOnce() + Send + 'static) -> RepoFlushProbeGuard {
    *REPO_FLUSH_PROBE.lock().expect("repo flush probe") = Some(Box::new(probe));
    RepoFlushProbeGuard
}

/// Fire and consume the repository-flush probe, if any.
#[cfg(test)]
pub(in crate::engine::server) fn fire_repo_flush_probe() {
    let probe = REPO_FLUSH_PROBE.lock().expect("repo flush probe").take();
    if let Some(probe) = probe {
        probe();
    }
}

/// One-shot probe fired in
/// [`release_source_stop::stop_release_sources_and_drain_tails`] immediately
/// AFTER the release sources are stopped and the registry cleared, BEFORE the
/// tail admission closes and (on the graceful path) BEFORE the repository
/// flush stage. A shutdown test compares its order with
/// [`arm_repo_flush_probe`]: the sources must stop before the flush, so this
/// marker must fire first (mutant: the helper moved after the flush).
#[cfg(test)]
static RELEASE_SOURCES_STOPPED_PROBE: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

/// Clears an unfired [`arm_release_sources_stopped_probe`]; a probe that
/// fires removes itself from the slot.
#[cfg(test)]
pub(crate) struct ReleaseSourcesStoppedProbeGuard;

#[cfg(test)]
impl Drop for ReleaseSourcesStoppedProbeGuard {
    fn drop(&mut self) {
        *RELEASE_SOURCES_STOPPED_PROBE
            .lock()
            .expect("release sources stopped probe") = None;
    }
}

/// Arm ONE release-sources-stopped probe.
#[cfg(test)]
pub(crate) fn arm_release_sources_stopped_probe(
    probe: impl FnOnce() + Send + 'static,
) -> ReleaseSourcesStoppedProbeGuard {
    *RELEASE_SOURCES_STOPPED_PROBE
        .lock()
        .expect("release sources stopped probe") = Some(Box::new(probe));
    ReleaseSourcesStoppedProbeGuard
}

/// Fire and consume the release-sources-stopped probe, if any.
#[cfg(test)]
pub(in crate::engine::server) fn fire_release_sources_stopped_probe() {
    let probe = RELEASE_SOURCES_STOPPED_PROBE
        .lock()
        .expect("release sources stopped probe")
        .take();
    if let Some(probe) = probe {
        probe();
    }
}

/// One-shot probe fired at the ENTRY of the store-release stage's zero-budget
/// branch (`server_handle/core.rs`, `release_budget.is_zero()`). Test-only: a
/// deadline test that exhausts every stage deadline at once must be able to
/// PROVE the zero-budget verdict came from this branch and not from the outer
/// future being abandoned before the stage was ever reached — the advance past
/// the outer deadline also expires the `timeout_at` in `drain_server`, so
/// without this rendezvous the branch entry is not guaranteed.
#[cfg(test)]
static RELEASE_ZERO_BUDGET_PROBE: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

/// Clears an unfired [`arm_release_zero_budget_probe`]; a probe that fires
/// removes itself from the slot.
#[cfg(test)]
pub(crate) struct ReleaseZeroBudgetProbeGuard;

#[cfg(test)]
impl Drop for ReleaseZeroBudgetProbeGuard {
    fn drop(&mut self) {
        *RELEASE_ZERO_BUDGET_PROBE
            .lock()
            .expect("release zero-budget probe") = None;
    }
}

/// Arm ONE zero-budget-branch-entry probe.
#[cfg(test)]
pub(crate) fn arm_release_zero_budget_probe(
    probe: impl FnOnce() + Send + 'static,
) -> ReleaseZeroBudgetProbeGuard {
    *RELEASE_ZERO_BUDGET_PROBE
        .lock()
        .expect("release zero-budget probe") = Some(Box::new(probe));
    ReleaseZeroBudgetProbeGuard
}

/// Fire and consume the zero-budget-branch probe, if any.
#[cfg(test)]
pub(in crate::engine::server) fn fire_release_zero_budget_probe() {
    let probe = RELEASE_ZERO_BUDGET_PROBE
        .lock()
        .expect("release zero-budget probe")
        .take();
    if let Some(probe) = probe {
        probe();
    }
}
