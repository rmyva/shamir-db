//! Shutdown barrier: waits until every fjall store under the data dir has
//! dropped its directory lock.
//!
//! After the handle's own references are gone, detached tasks (buffered-store
//! flushers, connection tasks) can keep a store alive for a short while, and
//! fjall releases its `lock` file only when the last handle drops. Probing the
//! lock is the one signal that does not depend on knowing every holder.
//!
//! A successful probe is an OBSERVATION, not a lock: it says the directory lock
//! was free at that instant, and another process (or a task that opens the store
//! later) can take it again immediately afterwards. The verdict therefore
//! narrows the relaunch race, it cannot close it — and a probe that could not be
//! taken is UNKNOWN state, never a release.

use shamir_collections::TSet;
use std::fs::{OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::time::Instant;

/// fjall's per-database marker files; a directory holding both is a store root.
const LOCK_FILE: &str = "lock";
const VERSION_MARKER: &str = "version";
/// Store roots sit directly under `data_dir` (or one level below it). A walk
/// that would have to go deeper cannot know the store set: that is a GAP.
const MAX_DEPTH: usize = 4;
const POLL_START: Duration = Duration::from_millis(1);
const POLL_MAX: Duration = Duration::from_millis(25);

/// A discovered store root: the name to REPORT plus the directory to PROBE.
///
/// The two are deliberately distinct. On Unix `data_dir/users` can be a
/// directory SYMLINK to the fjall directory the launcher opens by pathname
/// (`FjallUserDirectory::open(config.data_dir.join("users"))`), so the lock
/// lives on the link's TARGET: the probe must be taken there, while the report
/// must name the logical path the operator recognises.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoreRoot {
    /// The path reported to the caller (`data_dir/users`).
    logical: PathBuf,
    /// The canonical directory its lock file must be opened in.
    probe: PathBuf,
}

/// What ONE lock probe saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockState {
    /// Nobody holds the directory lock: the marker was opened and locked.
    Unlocked,
    /// Another handle — in this process or in another — holds the lock.
    Held,
    /// The lock could not be probed at all: the state is UNKNOWN.
    ProbeError,
    /// A lock marker was discovered but its target disappeared or could not
    /// be rechecked while opening it: discovery is no longer trustworthy.
    DiscoveryGap,
}

/// How far the discovery walk got.
#[derive(Debug)]
enum Discovery {
    /// Every directory under the root was classified: this IS the store set.
    Complete(Vec<StoreRoot>),
    /// The walk could not finish — an unreadable directory, a non-UTF8 entry, a
    /// dangling symlink, or directories below the depth cap — so the store set
    /// is UNKNOWN. A gap is never an empty (⇒ released) set.
    Gap,
}

/// What marker inspection established about a directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreClassification {
    NotStore,
    Complete,
    Gap,
}

/// Classify fjall's marker files without treating metadata failures as absence.
/// `version` must be a regular file, while `lock` only has to exist: a bad lock
/// entry must still be discovered so its probe can report UNKNOWN.
fn classify_store_root(dir: &Path) -> StoreClassification {
    let version_path = dir.join(VERSION_MARKER);
    match std::fs::metadata(&version_path) {
        Ok(meta) if meta.is_file() => {}
        Ok(_) => return StoreClassification::NotStore,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A dangling marker symlink exists but its target cannot be read.
            match std::fs::symlink_metadata(&version_path) {
                Ok(_) => return StoreClassification::Gap,
                Err(marker_error) if marker_error.kind() == std::io::ErrorKind::NotFound => {
                    return StoreClassification::NotStore;
                }
                Err(_) => return StoreClassification::Gap,
            }
        }
        Err(_) => return StoreClassification::Gap,
    }

    match std::fs::metadata(dir.join(LOCK_FILE)) {
        Ok(_) => StoreClassification::Complete,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A dangling lock symlink exists but cannot be opened. Distinguish
            // it from a genuinely absent marker before declaring NotStore.
            match std::fs::symlink_metadata(dir.join(LOCK_FILE)) {
                Ok(_) => StoreClassification::Gap,
                Err(marker_error) if marker_error.kind() == std::io::ErrorKind::NotFound => {
                    StoreClassification::NotStore
                }
                Err(_) => StoreClassification::Gap,
            }
        }
        Err(_) => StoreClassification::Gap,
    }
}

/// Walk `root` for fjall store roots, FOLLOWING directory symlinks.
///
/// The returned roots carry their logical name and their canonical probe path.
/// A cycle is impossible: a canonical path is walked at most once.
fn discover(root: &Path) -> Discovery {
    let mut found = Vec::new();
    // Canonical paths already walked: a symlink cycle, or a second name for a
    // directory already visited, must not be walked twice.
    let mut visited = TSet::default();
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    while let Some((logical, depth)) = stack.pop() {
        // Resolving the probe path is what follows a directory symlink. It can
        // fail on a dangling or unreadable link, which may have hidden a store
        // root: a GAP, not a skip.
        let Ok(probe) = std::fs::canonicalize(&logical) else {
            return Discovery::Gap;
        };
        if !visited.insert(probe.clone()) {
            continue;
        }
        match classify_store_root(&probe) {
            StoreClassification::Complete => {
                found.push(StoreRoot { logical, probe });
                continue;
            }
            StoreClassification::Gap => return Discovery::Gap,
            StoreClassification::NotStore => {}
        }
        let Ok(entries) = std::fs::read_dir(&probe) else {
            return Discovery::Gap;
        };
        let mut children = Vec::new();
        for entry in entries {
            let Ok(entry) = entry else {
                return Discovery::Gap;
            };
            let name = entry.file_name();
            if name.to_str().is_none() {
                // A store root under an unnameable path could not be reported.
                return Discovery::Gap;
            }
            let child = logical.join(&name);
            match entry.file_type() {
                Ok(kind) if kind.is_dir() => children.push(child),
                Ok(kind) if kind.is_symlink() => match std::fs::metadata(&child) {
                    // Follow only DIRECTORY links; a link to a file is not a
                    // store root and needs no further walk.
                    Ok(meta) if meta.is_dir() => children.push(child),
                    Ok(_) => {}
                    Err(_) => return Discovery::Gap,
                },
                Ok(_) => {}
                Err(_) => return Discovery::Gap,
            }
        }
        if children.is_empty() {
            continue;
        }
        if depth >= MAX_DEPTH {
            // Directories remain below the cap, so a store root may sit below
            // it: a GAP, never a silent truncation of the store set.
            return Discovery::Gap;
        }
        for child in children {
            stack.push((child, depth + 1));
        }
    }
    Discovery::Complete(found)
}

/// Open one lock marker of a DISCOVERED store. A marker that is gone now, or
/// that exists but cannot be followed (for example a dangling symlink), is a
/// discovery gap either way: on Unix an unlinked lock file can still be held
/// through an open descriptor, so its absence proves nothing. Classification
/// follows metadata; this recheck handles the race between that classification
/// and opening the marker.
fn open_lock(dir: &Path) -> Result<std::fs::File, LockState> {
    match OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join(LOCK_FILE))
    {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match std::fs::symlink_metadata(dir.join(LOCK_FILE)) {
                Ok(_) => Err(LockState::DiscoveryGap),
                // The marker itself vanished after discovery: UNKNOWN, not free.
                Err(marker_error) if marker_error.kind() == std::io::ErrorKind::NotFound => {
                    Err(LockState::DiscoveryGap)
                }
                Err(_) => Err(LockState::ProbeError),
            }
        }
        Err(_) => Err(LockState::ProbeError),
    }
}

/// One lock observation on a store's canonical directory.
fn probe(dir: &Path) -> LockState {
    let file = match open_lock(dir) {
        Ok(file) => file,
        Err(state) => return state,
    };
    match file.try_lock() {
        Ok(()) => {
            let _ = file.unlock();
            LockState::Unlocked
        }
        Err(TryLockError::WouldBlock) => LockState::Held,
        Err(TryLockError::Error(_)) => LockState::ProbeError,
    }
}

/// Probe every root; an error from any probe invalidates the observation,
/// because a partially observed set could under-report a held lock.
fn observe(roots: &[StoreRoot]) -> Result<Vec<StoreRoot>, LockState> {
    let mut held = Vec::new();
    for root in roots {
        match probe(&root.probe) {
            LockState::Unlocked => {}
            LockState::Held => held.push(root.clone()),
            state @ (LockState::ProbeError | LockState::DiscoveryGap) => return Err(state),
        }
    }
    Ok(held)
}

/// Discovery and the FIRST lock observation, in one blocking job: a starved
/// blocking pool then costs the budget once instead of twice.
struct FirstLook {
    discovery: Discovery,
    /// The first COMPLETED observation over the discovered set; `Err` when a
    /// probe errored or the set was never reached.
    observed: Result<Vec<StoreRoot>, LockState>,
}

/// The combined discovery + first probe job.
fn first_look(root: &Path) -> FirstLook {
    match discover(root) {
        Discovery::Complete(roots) => {
            let observed = observe(&roots);
            FirstLook {
                discovery: Discovery::Complete(roots),
                observed,
            }
        }
        // A gap carries no store set, so there is nothing to observe.
        other => FirstLook {
            discovery: other,
            observed: Err(LockState::DiscoveryGap),
        },
    }
}

/// The reportable (logical) names of these roots.
fn logical(roots: Vec<StoreRoot>) -> Vec<PathBuf> {
    roots.into_iter().map(|root| root.logical).collect()
}

/// The verdict of one bounded release wait.
///
/// The three cases are distinct in what they claim about the locks:
/// - [`Self::Released`]: a COMPLETE discovery found the store set and every
///   store in it answered the probe unlocked (an empty set counts — but only
///   after a discovery that finished).
/// - [`Self::StillLocked`]: at least one store root was OBSERVED holding its
///   directory lock and the budget did not allow another confirmation — the
///   last completed observation is the verdict, even if it is the last one that
///   fitted inside the budget. The paths are LOGICAL store roots.
/// - [`Self::Unverified`]: the store state is UNKNOWN — discovery never finished
///   (an unreadable directory, a symlink that does not resolve, a walk cut by
///   the depth cap, a starved blocking pool), or a lock probe errored. This is
///   never a release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnverifiedReason {
    /// The blocking job did not complete before the absolute deadline.
    PoolStarved,
    /// Discovery could not classify the complete store set.
    DiscoveryGap,
    /// A lock probe ran but could not determine the lock state.
    ProbeError,
    /// The caller's shutdown budget was already exhausted before probing.
    BudgetExpired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReleaseOutcome {
    /// No store under the root holds its directory lock.
    Released,
    /// These logical store roots were still holding their lock when the budget
    /// expired.
    StillLocked(Vec<PathBuf>),
    /// The store state is UNKNOWN, with the reason discovery or probing failed.
    Unverified(UnverifiedReason),
}

/// Wait until no fjall store under `root` is locked, bounded end to end.
///
/// Every probe await is bounded by the REMAINING budget, so neither discovery
/// nor a lock probe can delay the verdict past the deadline (a job already
/// running may still finish later); the verdict says which of the three cases
/// happened. An already-spent deadline never turns a known held
/// set into `Unverified`: an observed held lock remains `StillLocked`.
pub(crate) async fn release_stores(root: &Path, timeout: Duration) -> ReleaseOutcome {
    release_stores_until(root, Instant::now() + timeout).await
}

pub(crate) async fn release_stores_until(root: &Path, deadline: Instant) -> ReleaseOutcome {
    if Instant::now() >= deadline {
        return ReleaseOutcome::Unverified(UnverifiedReason::BudgetExpired);
    }
    let root = root.to_path_buf();
    // Discovery and the first observation are one job; the two states stay
    // apart afterwards: `discovery` is fixed by that one walk, while `observed`
    // is only ever replaced by a COMPLETED lock probe.
    let (discovery, observed) = match bounded_probe(deadline, move || first_look(&root)).await {
        Ok(FirstLook {
            discovery,
            observed,
        }) => (discovery, observed),
        Err(reason) => return ReleaseOutcome::Unverified(reason),
    };
    let Discovery::Complete(_roots) = discovery else {
        // Unknown or Gap: the store set is not knowable, so no release claim.
        return ReleaseOutcome::Unverified(UnverifiedReason::DiscoveryGap);
    };
    let mut observed = match observed {
        Ok(held) => Some(held),
        Err(LockState::ProbeError) => {
            return ReleaseOutcome::Unverified(UnverifiedReason::ProbeError);
        }
        Err(LockState::DiscoveryGap) => {
            return ReleaseOutcome::Unverified(UnverifiedReason::DiscoveryGap);
        }
        Err(LockState::Unlocked | LockState::Held) => unreachable!("observation error state"),
    };
    let mut delay = POLL_START;
    loop {
        // The verdict always comes from the last COMPLETED observation.
        let Some(held) = observed.take() else {
            // The only empty slot is an in-flight result handoff.
            return ReleaseOutcome::Unverified(UnverifiedReason::ProbeError);
        };
        if held.is_empty() {
            return ReleaseOutcome::Released;
        }
        if Instant::now() >= deadline {
            return ReleaseOutcome::StillLocked(logical(held));
        }
        // The probe closure owns the set, so keep a copy: a probe that cannot
        // RUN (a saturated pool, or a budget spent while scheduling) must not
        // discard locks already observed held.
        let fallback = held.clone();
        // Test seam: an installed gate parks the barrier here, after the
        // first COMPLETED observation and before the re-probe is submitted.
        #[cfg(test)]
        wait_at_re_probe_gate().await;
        // Counts reaching the statement that submits the re-probe job: a
        // barrier that answers from the first observation without getting
        // here leaves the counter untouched.
        #[cfg(test)]
        RE_PROBE_SUBMITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        observed = match bounded_probe(deadline, move || observe(&held)).await {
            Ok(Ok(observed)) => Some(observed),
            Ok(Err(LockState::ProbeError)) => {
                return ReleaseOutcome::Unverified(UnverifiedReason::ProbeError);
            }
            Ok(Err(LockState::DiscoveryGap)) => {
                return ReleaseOutcome::Unverified(UnverifiedReason::DiscoveryGap);
            }
            Ok(Err(LockState::Unlocked | LockState::Held)) => {
                unreachable!("observation error state")
            }
            Err(UnverifiedReason::PoolStarved) => {
                return ReleaseOutcome::StillLocked(logical(fallback));
            }
            Err(reason) => return ReleaseOutcome::Unverified(reason),
        };
        // A fresh observation is answered at once: an errored one is UNKNOWN,
        // and an empty one is a release — no sleep after either.
        match &observed {
            Some(held) if held.is_empty() => return ReleaseOutcome::Released,
            Some(_) => {}
            None => unreachable!("completed observation is handled above"),
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            // Re-check at the top of the loop: the verdict is `StillLocked`.
            continue;
        }
        // Capped by the remaining budget: the poll must never overshoot the
        // deadline it is bounded by.
        tokio::time::sleep(delay.min(remaining)).await;
        delay = (delay * 2).min(POLL_MAX);
    }
}

/// The blocking job did not complete before its deadline (it may have been
/// queued or still running), or its result channel closed unexpectedly.
///
/// The job stamps its own completion: `timeout_at` polls the receiver before
/// its timer, so a result that arrives after the deadline but before the async
/// side polls again would otherwise win. Such a late result is not an in-budget
/// observation and counts as the pool-starved case.
async fn bounded_probe<T: Send + 'static>(
    deadline: Instant,
    probe: impl FnOnce() -> T + Send + 'static,
) -> Result<T, UnverifiedReason> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    // Stamped on the timer's own clock (tokio's `Instant`, runtime-entered in
    // the blocking job), so a paused clock cannot make the two disagree.
    let job = tokio::task::spawn_blocking(move || {
        let value = probe();
        let _ = tx.send((value, Instant::now() < deadline));
    });
    match tokio::time::timeout_at(deadline, rx).await {
        Ok(Ok((value, true))) => Ok(value),
        Ok(Ok((_, false))) => Err(UnverifiedReason::PoolStarved),
        Ok(Err(_)) => Err(UnverifiedReason::ProbeError),
        Err(_) => {
            // The budget expired with the job still queued (or stuck): abort it.
            // That only stops a job that has not started; one already inside a
            // filesystem call can finish after the verdict.
            job.abort();
            Err(UnverifiedReason::PoolStarved)
        }
    }
}

/// Test seam for the completion stamp of [`bounded_probe`].
#[cfg(test)]
pub(crate) async fn bounded_probe_for_test<T: Send + 'static>(
    deadline: Instant,
    probe: impl FnOnce() -> T + Send + 'static,
) -> Result<T, UnverifiedReason> {
    bounded_probe(deadline, probe).await
}

/// Test seam for the RE-PROBE point of [`release_stores_until`]: while a gate
/// is installed, the barrier — at the moment it is about to submit a re-probe
/// AFTER a completed first observation — signals the test and then blocks
/// (bounded by 120 s) until the test drops the gate's release sender. The
/// gate is consumed once; production installs nothing, so the wait is never
/// taken outside tests.
/// LIMIT: one gate per process — the static is process-global, and under
/// nextest (one process per test) only the intended test can install it.
#[cfg(test)]
pub(crate) static RE_PROBE_GATE: std::sync::Mutex<
    Option<(
        tokio::sync::mpsc::Sender<()>,
        tokio::sync::mpsc::Receiver<()>,
    )>,
> = std::sync::Mutex::new(None);

/// Test-only count of re-probe jobs SUBMITTED to the blocking pool by
/// [`release_stores_until`]: incremented at exactly the submission point (the
/// `bounded_probe` call that follows the seam), so a mutant that answers from
/// the first observation without submitting a re-probe leaves it at zero.
/// LIMIT: one counter per process — under nextest (one process per test) only
/// the intended test observes it; assert the exact value after the verdict.
#[cfg(test)]
pub(crate) static RE_PROBE_SUBMITS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// An installed re-probe gate: `arrived` resolves when the barrier reached
/// the seam; dropping `release` (or the guard itself) lets the barrier
/// continue. A guard dropped while the gate is UNCONSUMED removes it, so a
/// failed assertion cannot strand a later test at the seam.
#[cfg(test)]
pub(crate) struct ReProbeGate {
    pub(crate) arrived: tokio::sync::mpsc::Receiver<()>,
    pub(crate) release: tokio::sync::mpsc::Sender<()>,
}

#[cfg(test)]
impl Drop for ReProbeGate {
    fn drop(&mut self) {
        drop(RE_PROBE_GATE.lock().expect("re-probe gate lock").take());
    }
}

#[cfg(test)]
pub(crate) fn install_re_probe_gate() -> ReProbeGate {
    let (arrived_tx, arrived_rx) = tokio::sync::mpsc::channel(1);
    let (release_tx, release_rx) = tokio::sync::mpsc::channel(1);
    *RE_PROBE_GATE.lock().expect("re-probe gate lock") = Some((arrived_tx, release_rx));
    ReProbeGate {
        arrived: arrived_rx,
        release: release_tx,
    }
}

/// The seam itself: consume the installed gate, signal arrival, and wait
/// (bounded) for the test's release.
#[cfg(test)]
async fn wait_at_re_probe_gate() {
    let Some((arrived, mut release)) = RE_PROBE_GATE.lock().expect("re-probe gate lock").take()
    else {
        return;
    };
    let _ = arrived.send(()).await;
    let _ = tokio::time::timeout(Duration::from_secs(120), release.recv()).await;
}

/// Test seam for the Unix lock-open classification: a marker that exists but
/// cannot be opened (a dangling link) is a discovery gap, never `Unlocked`.
#[cfg(all(test, unix))]
pub(crate) fn open_lock_is_discovery_gap(dir: &Path) -> bool {
    matches!(open_lock(dir), Err(LockState::DiscoveryGap))
}

/// Fail-closed wrapper for the callers that must not publish while the lock
/// state is UNKNOWN: `Unverified` is reported as a failure of the given root.
pub(crate) async fn wait_stores_released(
    root: &Path,
    timeout: Duration,
) -> Result<(), Vec<PathBuf>> {
    match release_stores(root, timeout).await {
        ReleaseOutcome::Released => Ok(()),
        ReleaseOutcome::StillLocked(locked) => Err(locked),
        ReleaseOutcome::Unverified(_) => Err(vec![root.to_path_buf()]),
    }
}
