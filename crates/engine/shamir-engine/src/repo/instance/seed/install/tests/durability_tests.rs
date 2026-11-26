use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_storage::types::Repo;
use shamir_tx::MvccStore;

use crate::repo::SeedInstallState;
use crate::table::table_manager::table_token_for;

use super::flush_probe::FlushProbe;
use super::helpers::{populated, repo};

// Counterfactual: omitting history flush allows bookmark set to trip the
// predecessor assertion. Swallowing either flush error allows Complete.
#[tokio::test]
async fn real_history_and_bookmark_helpers_propagate_failed_barriers() {
    for (history_fails, bookmark_fails) in [(false, false), (true, false), (false, true)] {
        let image = repo(Arc::new(InMemoryRepo::new()), Vec::new());
        let mut writer = populated(image.clone()).await;
        let info = image.tx_info_store().await.unwrap();
        let history_name = image.mapped_table_store_names("items").unwrap().2;
        let history = Arc::new(FlushProbe {
            inner: image.core.repo().store_get(history_name).await.unwrap(),
            info: info.clone(),
            fail: AtomicBool::new(history_fails),
            flushed: AtomicBool::new(false),
            flushes: AtomicUsize::new(0),
            predecessor: None,
            is_bookmark: false,
        });
        let bookmark = Arc::new(FlushProbe {
            inner: info.clone(),
            info: info.clone(),
            fail: AtomicBool::new(bookmark_fails),
            flushed: AtomicBool::new(false),
            flushes: AtomicUsize::new(0),
            predecessor: Some(history.clone()),
            is_bookmark: true,
        });
        let token = table_token_for("items");
        assert!(image.per_table_mvcc().remove_sync(&token).is_some());
        assert!(image
            .per_table_mvcc()
            .insert_sync(
                token,
                Arc::new(MvccStore::new(
                    history.clone(),
                    image.tx_gate().await.unwrap()
                )),
            )
            .is_ok());
        writer.bookmark_store = Some(bookmark.clone());
        let result = writer.finish(&[]).await;
        assert_eq!(history.flushes.load(Ordering::Acquire), 1);
        assert_eq!(
            bookmark.flushes.load(Ordering::Acquire),
            usize::from(!history_fails)
        );
        let state = SeedInstallState::load(&info).await.unwrap();
        if history_fails || bookmark_fails {
            assert!(result.is_err());
            assert!(matches!(state, Some(SeedInstallState::Installing(_))));
            assert!(SeedInstallState::require_complete(&info, true)
                .await
                .is_err());
        } else {
            assert_eq!(result.unwrap().source_version, 73);
            assert!(matches!(state, Some(SeedInstallState::Complete(_))));
            assert!(bookmark.flushed.load(Ordering::Acquire));
        }
        if history_fails {
            assert_eq!(image.replication_bookmark().await.unwrap(), 0);
        }
    }
}
