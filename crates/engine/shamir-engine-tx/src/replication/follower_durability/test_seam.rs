use shamir_collections::TFxMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Prepared,
    Materialized,
    Checkpointed,
}

#[cfg(test)]
fn maybe_panic_after(hook: &Hook, phase: Phase) {
    if *hook
        .panic_after
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        == Some(phase)
    {
        hook.reached.notify_waiters();
        panic!("injected receipt panic after {phase:?}");
    }
}

#[derive(Default)]
pub(crate) struct Hook {
    pub park_at: Mutex<Option<Phase>>,
    pub abort_at: Mutex<Option<Phase>>,
    /// One-shot post-phase failure (S5c-X round 3): when the pipeline passes
    /// `phase` and this is `Some(phase)`, the pipeline returns
    /// `DbError::Storage("injected")`. Consumed on first fire — the flag does
    /// not fire again — UNLESS `fail_after_sticky` is set, in which case it
    /// stays armed (every retry fails too) until the test clears `fail_after`.
    pub fail_after: Mutex<Option<Phase>>,
    pub fail_after_sticky: AtomicBool,
    /// When set (with `fail_after` armed for the same phase), the injected
    /// error is `DbError::RepoRequiresReopen("injected")` instead of
    /// `DbError::Storage("injected")` — a NON-retryable kind, so the
    /// pipeline must park immediately instead of entering the 3-retry loop.
    pub fail_after_reopen: AtomicBool,
    /// Panic synchronously immediately after the selected phase boundary.
    #[cfg(test)]
    pub panic_after: Mutex<Option<Phase>>,
    pub reached: Notify,
    pub resume: Notify,
}

fn hooks() -> &'static Mutex<TFxMap<String, Arc<Hook>>> {
    static HOOKS: OnceLock<Mutex<TFxMap<String, Arc<Hook>>>> = OnceLock::new();
    HOOKS.get_or_init(|| Mutex::new(TFxMap::default()))
}

#[cfg(feature = "fjall")]
pub(crate) fn install(repo_name: &str, hook: Arc<Hook>) {
    hooks()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(repo_name.to_owned(), hook);
}

#[cfg(feature = "fjall")]
pub(crate) fn remove(repo_name: &str) {
    hooks()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(repo_name);
}

/// Consume the one-shot post-`phase` failure flag for `repo_name` (armed
/// only when it holds exactly this phase). Returns `true` exactly when the
/// caller must return the injected `DbError::Storage("injected")`. Sticky
/// mode leaves the flag armed so every retry attempt fails too.
pub(crate) fn take_fail_after(repo_name: &str, phase: Phase) -> bool {
    let hooks = hooks()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(hook) = hooks.get(repo_name) else {
        return false;
    };
    let mut slot = hook
        .fail_after
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if *slot != Some(phase) {
        return false;
    }
    if !hook.fail_after_sticky.load(Ordering::Acquire) {
        // One-shot: consumed by this firing.
        *slot = None;
    }
    true
}

/// Whether the armed `fail_after` for `repo_name` injects the NON-retryable
/// `RepoRequiresReopen` kind instead of `Storage`.
pub(crate) fn fail_after_reopen(repo_name: &str) -> bool {
    hooks()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(repo_name)
        .is_some_and(|hook| hook.fail_after_reopen.load(Ordering::Acquire))
}

pub(crate) async fn phase(repo_name: &str, phase: Phase) {
    let hook = hooks()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(repo_name)
        .cloned();
    if let Some(hook) = hook {
        let park = *hook
            .park_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == Some(phase);
        if park {
            let resumed = hook.resume.notified();
            tokio::pin!(resumed);
            resumed.as_mut().enable();
            hook.reached.notify_waiters();
            resumed.await;
        }
        #[cfg(test)]
        maybe_panic_after(&hook, phase);
        if *hook
            .abort_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == Some(phase)
        {
            // The notify lets an in-process observer proceed before the
            // abort (kill without graceful shutdown, for the crash harness).
            hook.reached.notify_waiters();
            // Distinctive stderr marker so the crash harness can prove the
            // abort hook fired (the child's death alone is ambiguous).
            eprintln!("SHAMIR_RECEIPT_ABORT_FIRED {phase:?}");
            std::process::abort();
        }
    }
}
