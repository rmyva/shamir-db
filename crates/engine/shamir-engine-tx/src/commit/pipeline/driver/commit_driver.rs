use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::FutureExt;
use shamir_collections::TFxMap;
use shamir_storage::types::RecordKey;
use shamir_tx::{IsolationLevel, MvccStore, TxContext};
use shamir_types::types::common::THasher;
use tokio::runtime::Handle;

use crate::commit::core::RepoCore;
use crate::commit::TxError;
use crate::TxOutcome;

type CommitFuture = Pin<Box<dyn Future<Output = Result<TxOutcome, TxError>> + Send + 'static>>;

/// Polls commits inline; cancellation transfers the exact suspended future.
/// Detachment and panic cleanup require the captured runtime to remain alive:
/// shutdown must stop admission and await `RepoInstance::quiesce_commits`.
#[must_use = "commit drivers must be awaited"]
pub struct CommitDriver {
    future: Option<CommitFuture>,
    in_poll: bool,
    handle: Handle,
    locked_keys: Option<LockSnapshot>,
}

impl CommitDriver {
    pub fn new<F, Make>(tx: TxContext, repo: &RepoCore, make: Make) -> Self
    where
        F: Future<Output = Result<TxOutcome, TxError>> + Send + 'static,
        Make: FnOnce(TxContext, RepoCore) -> F,
    {
        let handle = Handle::current();
        let locked_keys = LockSnapshot::capture(&tx, repo);
        let future = make(tx, repo.clone());
        Self {
            future: Some(Box::pin(future)),
            in_poll: false,
            handle,
            locked_keys,
        }
    }
}

impl Future for CommitDriver {
    type Output = Result<TxOutcome, TxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // A caller may catch an inline unwind while retaining the driver.
        // Such a future must never be polled again, even by that caller.
        assert!(!this.in_poll, "commit future polled after an inline panic");
        this.in_poll = true;
        // Pending delegates waker registration to the owned child future.
        // The future remains present until this poll completes.
        #[expect(
            clippy::expect_used,
            reason = "guard: CommitDriver removes its future only after a Ready poll"
        )]
        let result = this
            .future
            .as_mut()
            .expect("commit driver polled after completion")
            .as_mut()
            .poll(cx);
        this.in_poll = false;
        if result.is_ready() {
            drop(this.future.take());
            drop(this.locked_keys.take());
        }
        result
    }
}

impl Drop for CommitDriver {
    fn drop(&mut self) {
        let Some(future) = self.future.take() else {
            return;
        };
        let snapshot = self.locked_keys.take();
        if self.in_poll {
            // Unwind already left the child poll: drop, NEVER restart it.
            // VersionGuard's existing Drop remains the orphan fallback.
            drop(future);
            if let Some(snapshot) = snapshot {
                // fire-and-forget: best-effort panic cleanup, runtime-dependent.
                drop(self.handle.spawn(async move {
                    snapshot.release().await;
                }));
            }
        } else {
            // fire-and-forget: detached by design; the owned barrier guard
            // tracks completion for explicit quiescence, not runtime shutdown.
            drop(self.handle.spawn(async move {
                // AssertUnwindSafe is containment only: no panicked future is
                // reused; its guards drop and VersionGuard registers an orphan.
                match AssertUnwindSafe(future).catch_unwind().await {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => log::warn!("detached commit failed: {error}"),
                    Err(_panic) => {
                        if let Some(snapshot) = snapshot {
                            snapshot.release().await;
                        }
                        log::error!("detached commit panicked; orphan recovery remains active");
                    }
                }
            }));
        }
    }
}

/// Owns cleanup stores independently of the sole foreground repo clone.
struct LockSnapshot {
    tx_version: u64,
    tables: Vec<(Arc<MvccStore>, Vec<RecordKey>)>,
}

impl LockSnapshot {
    fn capture(tx: &TxContext, repo: &RepoCore) -> Option<Self> {
        if tx.isolation != IsolationLevel::Pessimistic {
            return None;
        }
        // Accepted O(number of locks) cost ONLY for Level-3's owned panic snapshot.
        // No scc length scan; no bucket guard survives into asynchronous cleanup.
        let mut by_table: TFxMap<u64, Vec<RecordKey>> = TFxMap::with_hasher(THasher::default());
        tx.locked_keys.iter_sync(|(token, key), _| {
            by_table.entry(*token).or_default().push(key.clone());
            true
        });
        let mut tables = Vec::with_capacity(by_table.len());
        for (token, keys) in by_table {
            let mvcc = repo
                .per_table_mvcc()
                .read_sync(&token, |_, mvcc| Arc::clone(mvcc));
            if let Some(mvcc) = mvcc {
                tables.push((mvcc, keys));
            }
        }
        Some(Self {
            tx_version: tx.tx_id.0,
            tables,
        })
    }

    async fn release(self) {
        for (mvcc, keys) in self.tables {
            mvcc.release_locks(self.tx_version, &keys).await;
        }
    }
}
