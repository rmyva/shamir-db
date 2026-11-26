//! Crash-image tests (acceptance test 10, PR6b-3): a legacy crash image —
//! floor marker plus v2/u64 bookmark bytes written directly into the store —
//! is REFUSED with the typed error on the cold open/apply path, and the
//! refusal REPEATS: nothing is rewritten, no v3 is minted, counters and
//! floor are unchanged. There is no runnable legacy executor to replay it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, Stream};
use shamir_storage::error::{DbError, DbResult, FollowerRefusalReason};
use shamir_storage::types::{RecordKey, Store};
use std::pin::Pin;

use crate::meta::recovery_marker::{load_last_committed, save_last_committed_flushed};
use crate::meta::repl::bookmark_v3::{
    classify_follower_bookmark, load_bookmark_v3, FollowerBookmarkState,
};
use crate::meta::repl_bookmark::{
    load_clean_horizon, load_replication_bookmark, save_fence_record_v3_aware, FenceRecord,
};
use crate::meta::{MetaEnvelope, MetaKey};

/// A durable crash image: an immutable byte snapshot of a metadata store.
#[derive(Default)]
struct CrashImage {
    records: Mutex<BTreeMap<RecordKey, Bytes>>,
}

impl CrashImage {
    fn set(&self, key: RecordKey, value: Bytes) {
        self.records.lock().unwrap().insert(key, value);
    }

    fn image(&self) -> Arc<ImageStore> {
        Arc::new(ImageStore {
            records: Mutex::new(self.records.lock().unwrap().clone()),
            writes: AtomicUsize::new(0),
            mutated_keys: Mutex::new(BTreeSet::new()),
        })
    }
}

/// Read/write view of a crash image. Scans iterate the REAL map. Mutating
/// writes are counted in a ledger (the refusal path must never rewrite
/// refused state) while the immutable snapshot itself is never rewritten.
struct ImageStore {
    records: Mutex<BTreeMap<RecordKey, Bytes>>,
    writes: AtomicUsize,
    mutated_keys: Mutex<BTreeSet<Vec<u8>>>,
}

impl ImageStore {
    fn record_mutation(&self, key: &[u8]) {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.mutated_keys.lock().unwrap().insert(key.to_vec());
    }

    fn mutations(&self) -> usize {
        self.writes.load(Ordering::SeqCst)
    }

    fn mutated_keys(&self) -> Vec<Vec<u8>> {
        self.mutated_keys.lock().unwrap().iter().cloned().collect()
    }
}

#[async_trait]
impl Store for ImageStore {
    async fn insert(&self, value: Bytes) -> DbResult<RecordKey> {
        let key = RecordKey::from_slice(&(self.records.lock().unwrap().len() as u64).to_be_bytes());
        // Mirror `set`: the generated key must land in the write ledger too.
        self.records.lock().unwrap().insert(key.clone(), value);
        self.record_mutation(key.as_ref());
        Ok(key)
    }
    async fn set(&self, key: RecordKey, value: Bytes) -> DbResult<bool> {
        let inserted = self
            .records
            .lock()
            .unwrap()
            .insert(key.clone(), value)
            .is_none();
        self.record_mutation(key.as_ref());
        Ok(inserted)
    }
    async fn get(&self, key: RecordKey) -> DbResult<Bytes> {
        self.records
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or_else(|| DbError::NotFound("crash image key".into()))
    }
    async fn remove(&self, key: RecordKey) -> DbResult<bool> {
        let removed = self.records.lock().unwrap().remove(&key).is_some();
        self.record_mutation(key.as_ref());
        Ok(removed)
    }
    // Scans iterate the REAL map in sorted batches — the crash image is a
    // metadata snapshot with real records, so an empty stream would lie.
    fn iter_stream(
        &self,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<(RecordKey, Bytes)>, DbError>> + Send>> {
        let snapshot: Vec<(RecordKey, Bytes)> = self
            .records
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let batch_size = batch_size.max(1);
        let batches: Vec<Vec<(RecordKey, Bytes)>> = snapshot
            .chunks(batch_size)
            .map(<[(RecordKey, Bytes)]>::to_vec)
            .collect();
        Box::pin(stream::iter(batches.into_iter().map(Ok)))
    }
    fn scan_prefix_stream(
        &self,
        prefix: Bytes,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<(RecordKey, Bytes)>, DbError>> + Send>> {
        let snapshot: Vec<(RecordKey, Bytes)> = self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.as_ref().starts_with(prefix.as_ref()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let batch_size = batch_size.max(1);
        let batches: Vec<Vec<(RecordKey, Bytes)>> = snapshot
            .chunks(batch_size)
            .map(<[(RecordKey, Bytes)]>::to_vec)
            .collect();
        Box::pin(stream::iter(batches.into_iter().map(Ok)))
    }
}

fn bookmark_key() -> RecordKey {
    RecordKey::from_slice(MetaKey::ReplicationBookmark.as_record_id().as_bytes())
}

fn fence(bookmark: u64) -> FenceRecord {
    FenceRecord {
        bookmark,
        lineage_id: [5; 16],
        history_epoch: 3,
        observed_identity: None,
    }
}

/// Hand-built legacy bookmark bytes: fixed-width u64 and v2 (fence +
/// independent clean horizon), including bookmark zero.
fn legacy_bookmark_fixtures() -> Vec<(Vec<u8>, &'static str)> {
    vec![
        (
            MetaEnvelope::new(10u64).encode().unwrap(),
            "fixed-width u64",
        ),
        (MetaEnvelope::new(0u64).encode().unwrap(), "u64 zero"),
        (
            MetaEnvelope::new((2u16, (Some(fence(10)), 10u64, Some(8u64))))
                .encode()
                .unwrap(),
            "v2",
        ),
    ]
}

/// One legacy crash image: floor marker + the legacy bookmark bytes.
fn legacy_crash_image(bookmark_bytes: &[u8]) -> Arc<ImageStore> {
    let image = CrashImage::default();
    image.set(bookmark_key(), Bytes::from(bookmark_bytes.to_vec()));
    image.image()
}

async fn assert_refused_repeatedly(
    image: &Arc<ImageStore>,
    expected_bytes: &[u8],
    reason: FollowerRefusalReason,
    label: &str,
) {
    let bookmark_bytes = bookmark_key().as_ref().to_vec();
    // The metadata APIs take `&Arc<dyn Store>`; coerce once up front.
    let store: Arc<dyn Store> = image.clone();
    for attempt in 0..2 {
        let writes_before = image.mutations();
        let check = |result: DbError| {
            assert!(
                matches!(result, DbError::FollowerStateRefused { reason: r } if r == reason),
                "attempt {attempt} for {label}: expected FollowerStateRefused({reason:?}), \
                 got {result:?}"
            );
        };
        check(load_bookmark_v3(&store).await.unwrap_err());
        check(classify_follower_bookmark(&store).await.unwrap_err());
        check(load_replication_bookmark(&store).await.unwrap_err());
        check(load_clean_horizon(&store).await.unwrap_err());
        // The refusal path must NEVER mint a v3 record over refused bytes.
        check(
            save_fence_record_v3_aware(&store, fence(99))
                .await
                .unwrap_err(),
        );
        // The stored bytes are untouched — no rewrite, no adoption.
        assert_eq!(
            store.get(bookmark_key()).await.unwrap().as_ref(),
            expected_bytes,
            "attempt {attempt} for {label}"
        );
        // No mutating write touched the image during the refusal calls, and
        // the refused bookmark key is not among the mutated keys.
        assert_eq!(
            image.mutations(),
            writes_before,
            "attempt {attempt} for {label}: refusal must not write"
        );
        assert!(
            !image.mutated_keys().contains(&bookmark_bytes),
            "attempt {attempt} for {label}: the refused key must stay unwritten"
        );
    }
}

/// A legacy bookmark record in a crash image refuses every loader and the
/// v3 writer, repeatedly, with `LegacyBookmarkFormat`; nothing is rewritten.
#[tokio::test]
async fn legacy_bookmark_crash_images_refuse_repeatedly_without_rewrite() {
    for (bytes, label) in legacy_bookmark_fixtures() {
        let image = legacy_crash_image(&bytes);
        assert_refused_repeatedly(
            &image,
            &bytes,
            FollowerRefusalReason::LegacyBookmarkFormat,
            label,
        )
        .await;
    }
}

/// A floor-without-bookmark crash image is NOT a repo open: classification
/// correctly yields `FloorWithoutBookmark` (a LEADER with this state must
/// still open — covered by
/// `leader_with_floor_marker_still_opens_and_preserves_floor`). The
/// FOLLOWER-ATTEMPT adoption over the image (`save_fence_record_v3_aware`)
/// refuses with `LegacyFollowerState`, repeatedly, with the floor preserved
/// verbatim and no v3 record minted.
#[tokio::test]
async fn floor_only_crash_image_classifies_unowned_and_refuses_adoption() {
    let image = CrashImage::default();
    let store = image.image();
    // The metadata APIs take `&Arc<dyn Store>`; coerce once up front.
    let dyn_store: Arc<dyn Store> = store.clone();
    // Floor marker WITHOUT any bookmark: unowned pre-receipt follower state.
    save_last_committed_flushed(&dyn_store, 7).await.unwrap();
    let writes_before = store.mutations();
    for attempt in 0..2 {
        assert!(
            matches!(
                classify_follower_bookmark(&dyn_store).await.unwrap(),
                FollowerBookmarkState::FloorWithoutBookmark { floor: 7 }
            ),
            "attempt {attempt}"
        );
        let error = save_fence_record_v3_aware(&dyn_store, fence(99))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                DbError::FollowerStateRefused {
                    reason: FollowerRefusalReason::LegacyFollowerState
                }
            ),
            "attempt {attempt}: {error:?}"
        );
        assert_eq!(load_last_committed(&dyn_store).await.unwrap(), Some(7));
        assert!(load_bookmark_v3(&dyn_store).await.unwrap().is_none());
        // No NEW mutations during the refusal loop — the refused adoption
        // wrote nothing, and the bookmark key was never touched.
        assert_eq!(store.mutations(), writes_before, "attempt {attempt}");
        assert!(!store
            .mutated_keys()
            .contains(&bookmark_key().as_ref().to_vec()));
    }
}

/// A crash image containing BOTH a valid v3 record and its floor keeps
/// opening as a native follower — the only supported durable state.
#[tokio::test]
async fn v3_crash_image_still_classifies_native() {
    let image = CrashImage::default();
    let store = image.image();
    // The metadata APIs take `&Arc<dyn Store>`; coerce once up front.
    let dyn_store: Arc<dyn Store> = store.clone();
    save_last_committed_flushed(&dyn_store, 2).await.unwrap();
    crate::meta::repl::bookmark_v3::save_bookmark_v3(
        &dyn_store,
        &crate::meta::repl::bookmark_v3::BookmarkV3 {
            fence: Some(fence(2)),
            bookmark: 2,
            clean_through: Some(2),
            outstanding_receipt: None,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        classify_follower_bookmark(&dyn_store).await.unwrap(),
        crate::meta::repl::bookmark_v3::FollowerBookmarkState::Native(_)
    ));
    assert_eq!(load_replication_bookmark(&dyn_store).await.unwrap(), 2);
    assert_eq!(load_clean_horizon(&dyn_store).await.unwrap(), Some(2));
}
