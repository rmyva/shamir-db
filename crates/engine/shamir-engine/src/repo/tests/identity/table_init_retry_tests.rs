//! B09: setup failure/cancellation must not strand MVCC publication.
//! The hook belongs immediately before unwrap/create setup (after the old
//! registration point). Restoring early registration exposes an entry in
//! the failure and cancellation tests and prevents replacement in the race test.

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::table_manager::table_token_for;
use crate::table::{TableConfig, TableManager};
use crate::tx::test_seams::TableInitHook;
use shamir_storage::error::DbError;
use shamir_storage::storage_in_memory::{InMemoryRepo, InMemoryStore};
use shamir_storage::types::Store;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::timeout;

fn make_repo() -> RepoInstance {
    RepoInstance::new(
        "retryable".into(),
        BoxRepo::InMemory(Arc::new(InMemoryRepo::new())),
        vec![TableConfig::new("orders")],
    )
}

fn assert_coherent(repo: &RepoInstance, table: &TableManager) {
    let mine = table.mvcc_store().expect("opened table must own MVCC");
    let registered = repo
        .per_table_mvcc()
        .read_sync(&table_token_for("orders"), |_, mvcc| Arc::clone(mvcc))
        .expect("opened table must be registered");
    assert!(Arc::ptr_eq(&mine, &registered));
}

fn assert_unregistered(repo: &RepoInstance) {
    assert!(repo
        .per_table_mvcc()
        .read_sync(&table_token_for("orders"), |_, _| ())
        .is_none());
}

#[tokio::test]
async fn setup_failure_retries_with_coherent_mvcc() {
    let repo = make_repo();
    repo.core
        .table_init_hook
        .store(Some(TableInitHook::new(true)));
    let error = match repo.get_table("orders").await {
        Err(error) => error,
        Ok(_) => panic!("injected setup failure must propagate"),
    };
    assert!(
        matches!(&error, DbError::Internal(message) if message == "B09 injected setup failure")
    );
    assert_unregistered(&repo);
    let table = repo.get_table("orders").await.unwrap();
    assert_eq!(table.name(), "orders");
    assert_coherent(&repo, &table);
    let again = repo.get_table("orders").await.unwrap();
    assert!(Arc::ptr_eq(
        &table.mvcc_store().unwrap(),
        &again.mvcc_store().unwrap(),
    ));
}

#[tokio::test]
async fn cold_open_completes_through_a_blocking_sync_bridge() {
    // One runtime thread is blocked by the bridge: the open must not wait for
    // another task of this runtime to make progress.
    let repo = make_repo();
    let table = futures::executor::block_on(repo.get_table("orders")).unwrap();
    assert_coherent(&repo, &table);
    let unfenced = futures::executor::block_on(repo.core.get_table_unfenced("orders")).unwrap();
    assert!(Arc::ptr_eq(
        &table.mvcc_store().unwrap(),
        &unfenced.mvcc_store().unwrap(),
    ));
}

#[tokio::test]
async fn cancelled_caller_keeps_cold_open_admitted_until_publication() {
    const DEADLINE: Duration = Duration::from_secs(120);
    let repo = make_repo();
    let hook = TableInitHook::new(false);
    repo.core.table_init_hook.store(Some(Arc::clone(&hook)));
    let worker_repo = repo.clone();
    let worker = tokio::spawn(async move { worker_repo.get_table("orders").await });
    timeout(DEADLINE, hook.wait_entered()).await.unwrap();
    worker.abort();
    let outcome = timeout(DEADLINE, worker).await.unwrap();
    assert!(matches!(outcome, Err(error) if error.is_cancelled()));

    let mut next_admission = Box::pin(repo.write_domain().admit());
    assert!(
        futures::poll!(next_admission.as_mut()).is_pending(),
        "caller cancellation must not release the cold open's permit"
    );
    assert_unregistered(&repo);
    assert!(repo.core.catalog().initialized_table("orders").is_none());

    hook.resume();
    let permit = timeout(DEADLINE, next_admission).await.unwrap().unwrap();
    // No retry has run yet: the cancelled caller's open must have published
    // a coherent manager before releasing admission.
    let published = repo
        .core
        .catalog()
        .initialized_table("orders")
        .expect("cold open must publish before releasing admission");
    assert_coherent(&repo, &published);
    drop(permit);

    let table = timeout(DEADLINE, repo.get_table("orders"))
        .await
        .unwrap()
        .unwrap();
    assert_coherent(&repo, &table);
    assert!(Arc::ptr_eq(
        &published.mvcc_store().unwrap(),
        &table.mvcc_store().unwrap(),
    ));
}

#[tokio::test]
async fn retired_initializer_cannot_disrupt_replacement() {
    let repo = make_repo();
    let contended = Arc::new(Notify::new());
    repo.write_domain()
        .install_contended_admission_for_test(Arc::clone(&contended));

    let hook = TableInitHook::new(false);
    repo.core.table_init_hook.store(Some(Arc::clone(&hook)));
    let worker_repo = repo.clone();
    let worker = tokio::spawn(async move { worker_repo.get_table("orders").await });
    hook.wait_entered().await;
    assert!(repo.remove_table_for_test("orders"));
    repo.add_table_for_test(TableConfig::new("orders"));
    let replacement_repo = repo.clone();
    let mut replacement = Box::pin(replacement_repo.get_table("orders"));
    assert!(futures::poll!(replacement.as_mut()).is_pending());
    contended.notified().await;
    hook.resume();
    let result = worker.await.unwrap();
    assert!(matches!(
        result,
        Err(DbError::Internal(_)) | Err(DbError::NotFound(_))
    ));
    let replacement = replacement.await.unwrap();
    assert_coherent(&repo, &replacement);
    let current = repo.get_table("orders").await.unwrap();
    assert!(Arc::ptr_eq(
        &replacement.mvcc_store().unwrap(),
        &current.mvcc_store().unwrap(),
    ));
    assert!(repo.remove_table_for_test("orders"));
    assert_unregistered(&repo);
}

/// Red before B09: init resumes after DROP, returns a retired manager and
/// leaves its newly inserted MVCC registered despite missing config.
#[tokio::test]
async fn dropped_config_rolls_back_retired_publication() {
    let repo = make_repo();
    let hook = TableInitHook::new(false);
    repo.core.table_init_hook.store(Some(Arc::clone(&hook)));
    let worker_repo = repo.clone();
    let worker = tokio::spawn(async move { worker_repo.get_table("orders").await });
    hook.wait_entered().await;
    assert!(repo.remove_table_for_test("orders"));
    hook.resume();
    assert!(matches!(worker.await.unwrap(), Err(DbError::NotFound(_))));
    assert_unregistered(&repo);
    repo.add_table_for_test(TableConfig::new("orders"));
    let table = repo.get_table("orders").await.unwrap();
    assert_coherent(&repo, &table);
}

/// DROP of an uninitialized cell must not erase somebody else's attachment.
/// Red before B09: remove_table unconditionally removes the token's MVCC.
#[tokio::test]
async fn remove_empty_cell_preserves_foreign_mvcc() {
    let repo = make_repo();
    let gate = repo.tx_gate().await.unwrap();
    let history: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let foreign = Arc::new(shamir_tx::MvccStore::new(history, gate));
    let token = table_token_for("orders");
    assert!(repo
        .per_table_mvcc()
        .insert_sync(token, Arc::clone(&foreign))
        .is_ok());
    assert!(matches!(
        repo.get_table("orders").await,
        Err(DbError::Internal(_))
    ));
    assert!(repo.remove_table_for_test("orders"));
    let retained = repo
        .per_table_mvcc()
        .read_sync(&token, |_, mvcc| Arc::clone(mvcc))
        .expect("foreign registration must survive DROP");
    assert!(Arc::ptr_eq(&retained, &foreign));
}
