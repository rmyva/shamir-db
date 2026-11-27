//! `FjallRepo` — database connection, per-repo atomic batches, and the `Repo` surface.
#[cfg(test)]
use super::atomic_batch_test_seam;
use super::persist::persist_sync_all;
use super::store::FjallStore;
use crate::commit_window_park_seam;
use crate::error::{DbError, DbResult};
use crate::types::{AtomicBatch, AtomicCommitOutcome, KvOp, RecordKey, Repo, Store};
use async_trait::async_trait;
use bytes::Bytes;
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use std::path::Path;
use std::sync::Arc;
use tokio::task;

pub struct FjallRepo {
    db: Arc<Database>,
}

/// Mirrors fjall 3.1.10 `keyspace::name::is_valid_keyspace_name` (non-empty,
/// length fits u8): the check is on UTF-8 byte length, not character classes.
fn validate_store_name(name: &str) -> DbResult<()> {
    if name.is_empty() || name.len() > u8::MAX as usize {
        return Err(DbError::Validation(
            "store name must contain 1..=255 UTF-8 bytes".into(),
        ));
    }
    Ok(())
}

/// Pending repository-wide atomic batch for [`FjallRepo`]: ops are staged as
/// (store-name, op) pairs and applied in ONE `Database::batch()` commit +
/// `persist(SyncAll)` on commit — all-or-nothing across keyspaces.
struct FjallAtomicBatch {
    db: Arc<Database>,
    ops: Vec<(String, KvOp)>,
}

/// Execute a repository-wide atomic batch synchronously: ops are staged as
/// (store-name, op) pairs, resolved to keyspaces up-front (any resolution
/// failure is a plain `Err` — nothing applied), then committed in ONE
/// `Database::batch()` + `persist(SyncAll)`.
pub(super) fn exec_atomic_commit(
    db: &Arc<Database>,
    ops: Vec<(String, KvOp)>,
) -> DbResult<AtomicCommitOutcome> {
    if ops.is_empty() {
        return Ok(AtomicCommitOutcome::Committed);
    }

    // Fail fast BEFORE any write on an invalid store name.
    for (name, _) in &ops {
        validate_store_name(name)?;
    }

    // Test seam: injected failure BEFORE any write is staged into a batch —
    // a plain Err, nothing applied.
    #[cfg(test)]
    if atomic_batch_test_seam::FAIL_BEFORE_COMMIT.load(std::sync::atomic::Ordering::Acquire) {
        return Err(DbError::Storage(
            "injected atomic batch failure before commit".into(),
        ));
    }

    // Resolve each distinct store name to a keyspace before building the
    // batch (op counts are small — linear lookup dedupe is fine).
    let mut keyspaces: Vec<(String, Keyspace)> = Vec::new();
    for (name, _) in &ops {
        if keyspaces.iter().any(|(n, _)| n == name) {
            continue;
        }
        let ks = db
            .keyspace(name, KeyspaceCreateOptions::default)
            .map_err(|e| DbError::Storage(e.to_string()))?;
        keyspaces.push((name.clone(), ks));
    }

    let mut batch = db.batch();
    for (name, op) in ops {
        let keyspace = &keyspaces
            .iter()
            .find(|(n, _)| *n == name)
            .expect("keyspace resolved above")
            .1;
        match op {
            KvOp::Set(k, v) => batch.insert(keyspace, k.as_ref(), v.as_ref()),
            KvOp::Remove(k) => batch.remove(keyspace, k.as_ref()),
        }
    }

    // fjall's API cannot distinguish a pre-apply from a post-apply failure
    // here, so an error AFTER staging is reported UNCERTAIN — never assumed
    // absent.
    // Test-only commit-window park: parks this `spawn_blocking` thread
    // INSIDE the physical commit window (after staging, before the batch is
    // applied) so a test can abort the caller mid-commit. See the module
    // docs on `commit_window_park_seam` for the contention model.
    if commit_window_park_seam::ARMED.load(std::sync::atomic::Ordering::Relaxed) {
        commit_window_park_seam::park();
    }
    if let Err(e) = batch.commit() {
        return Ok(AtomicCommitOutcome::Uncertain {
            detail: format!("fjall batch commit: {e}"),
        });
    }

    #[cfg(test)]
    if atomic_batch_test_seam::FAIL_AFTER_COMMIT.load(std::sync::atomic::Ordering::Acquire) {
        return Ok(AtomicCommitOutcome::Uncertain {
            detail: "injected uncertain commit after batch application".into(),
        });
    }

    // The batch is applied; durability is per-backend policy. A persist
    // failure means applied-but-durability-unknown → Uncertain.
    match persist_sync_all(db) {
        Ok(()) => Ok(AtomicCommitOutcome::Committed),
        Err(e) => Ok(AtomicCommitOutcome::Uncertain {
            detail: format!("fjall persist after atomic batch: {e}"),
        }),
    }
}

impl FjallRepo {
    pub fn new(path: impl AsRef<Path>) -> DbResult<Self> {
        let db = Database::builder(path.as_ref())
            .open()
            .map_err(|e| DbError::Storage(e.to_string()))?;
        Ok(Self { db: Arc::new(db) })
    }

    /// cfg(test): flush the named store's active memtable into persisted
    /// segments (see `FjallStore::persist_memtables_for_test`) so tests can
    /// exercise the persisted-table read path of `iter_range_stream`. Lives
    /// on the repo because tests only hold `Arc<dyn Store>` handles (no
    /// downcast), which cannot reach the concrete store's seam.
    #[cfg(test)]
    pub(crate) async fn persist_memtables_for_test(&self, store: &str) -> DbResult<()> {
        validate_store_name(store)?;
        let db = self.db.clone();
        let name = store.to_string();
        let db_for_open = db.clone();
        let keyspace = task::spawn_blocking(move || -> DbResult<Keyspace> {
            db_for_open
                .keyspace(&name, KeyspaceCreateOptions::default)
                .map_err(|e| DbError::Storage(e.to_string()))
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))??;
        let store_handle = FjallStore {
            keyspace,
            db,
            worker: std::sync::OnceLock::new(),
            spawn_worker: None,
        };
        store_handle.persist_memtables_for_test();
        Ok(())
    }
}

#[async_trait]
impl Repo for FjallRepo {
    async fn store_get<S: AsRef<str> + Send>(&self, name: S) -> DbResult<Arc<dyn Store>> {
        validate_store_name(name.as_ref())?;
        let db = self.db.clone();
        let table_name = name.as_ref().to_string();

        let keyspace = task::spawn_blocking(move || -> DbResult<Keyspace> {
            db.keyspace(&table_name, KeyspaceCreateOptions::default)
                .map_err(|e| DbError::Storage(e.to_string()))
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))??;

        Ok(Arc::new(FjallStore {
            keyspace,
            db: self.db.clone(),
            worker: std::sync::OnceLock::new(),
            #[cfg(test)]
            spawn_worker: None,
        }))
    }

    async fn store_get_existing(&self, name: &str) -> DbResult<Option<Arc<dyn Store>>> {
        validate_store_name(name)?;
        let db = self.db.clone();
        let name = name.to_string();
        // Fjall 3.1.13 exposes `keyspace_exists`, but its `keyspace` open
        // is still create-or-open and the two calls aren't atomic. Callers in
        // the Shamir binding catalogue hold the replication-control lifecycle
        // interlock, which excludes normal Shamir create/delete for this store.
        // External writers using this Fjall database directly remain outside
        // that exclusion domain (TOCTOU caveat).
        let keyspace = task::spawn_blocking(move || -> DbResult<Option<Keyspace>> {
            if !db.keyspace_exists(&name) {
                return Ok(None);
            }
            db.keyspace(&name, KeyspaceCreateOptions::default)
                .map(Some)
                .map_err(|error| DbError::Storage(error.to_string()))
        })
        .await
        .map_err(|error| DbError::Internal(error.to_string()))??;
        Ok(keyspace.map(|keyspace| {
            Arc::new(FjallStore {
                keyspace,
                db: self.db.clone(),
                worker: std::sync::OnceLock::new(),
                #[cfg(test)]
                spawn_worker: None,
            }) as Arc<dyn Store>
        }))
    }

    async fn store_delete<S: AsRef<str> + Send>(&self, name: S) -> DbResult<bool> {
        validate_store_name(name.as_ref())?;
        let db = self.db.clone();
        let table_name = name.as_ref().to_string();

        task::spawn_blocking(move || -> DbResult<bool> {
            let keyspace = db
                .keyspace(&table_name, KeyspaceCreateOptions::default)
                .map_err(|e| DbError::Storage(e.to_string()))?;

            db.delete_keyspace(keyspace)
                .map_err(|e| DbError::Storage(e.to_string()))?;
            Ok(true)
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?
    }

    async fn stores_list(&self) -> DbResult<Vec<String>> {
        let db = self.db.clone();
        task::spawn_blocking(move || -> DbResult<Vec<String>> {
            let names: Vec<String> = db
                .list_keyspace_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect();
            Ok(names)
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?
    }

    /// `true` — the batch commits in ONE `OwnedWriteBatch::commit()` +
    /// `persist(SyncAll)` across all keyspaces (see `exec_atomic_commit`).
    fn supports_atomic_batch(&self) -> bool {
        true
    }

    async fn begin_atomic_batch(&self) -> DbResult<Box<dyn AtomicBatch>> {
        Ok(Box::new(FjallAtomicBatch {
            db: self.db.clone(),
            ops: Vec::new(),
        }))
    }

    /// ONE `persist(SyncAll)` covers every keyspace: fjall's journal is
    /// per-Database, not per-keyspace, so a single fsync after all stores
    /// have drained (via `Store::flush_drain`) is durability-identical to
    /// the old per-store `flush()` loop that fsynced the SAME journal N
    /// times (the N−1 redundant fsyncs are the Windows shutdown-stall
    /// multiplier this replaces).
    async fn persist_once(&self) -> DbResult<()> {
        let db = self.db.clone();
        task::spawn_blocking(move || {
            persist_sync_all(&db).map_err(|e| DbError::Storage(format!("Fjall persist: {}", e)))
        })
        .await
        .map_err(|e| DbError::Internal(e.to_string()))?
    }
}

#[async_trait]
impl AtomicBatch for FjallAtomicBatch {
    fn set(&mut self, store: &str, key: RecordKey, value: Bytes) -> DbResult<()> {
        validate_store_name(store)?;
        self.ops.push((store.to_string(), KvOp::Set(key, value)));
        Ok(())
    }

    fn delete(&mut self, store: &str, key: RecordKey) -> DbResult<()> {
        validate_store_name(store)?;
        self.ops.push((store.to_string(), KvOp::Remove(key)));
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> DbResult<AtomicCommitOutcome> {
        let db = self.db.clone();
        let ops = std::mem::take(&mut self.ops);
        task::spawn_blocking(move || exec_atomic_commit(&db, ops))
            .await
            .map_err(|e| DbError::Internal(e.to_string()))?
    }
}
