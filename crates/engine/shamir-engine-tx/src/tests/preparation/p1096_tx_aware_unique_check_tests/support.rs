//! #1096: Stage-time unique check must be tx-aware of the transaction's
//! own prior releases.
//!
//! Tests the fix for the bug where `insert_tx`'s stage-time unique check
//! rejects legitimate release-then-reclaim patterns (DELETE or UPDATE-off
//! of a durable record, followed by INSERT claiming the same unique key
//! in the same transaction).

use std::sync::Arc;

use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_types::core::interner::{InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::value::InnerValue;

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;
use crate::table::table_manager::TableManager;

pub(super) fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

pub(super) async fn key_id(tbl: &TableManager, name: &str) -> u64 {
    let interner = tbl.interner().get().await.unwrap();
    match interner.touch_ind(name).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    }
}

pub(super) fn record_with_str(key: u64, val: &str) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(InternerKey::new(key), InnerValue::Str(val.into()));
    InnerValue::Map(m)
}

pub(super) fn record_with_two_str(key1: u64, val1: &str, key2: u64, val2: &str) -> InnerValue {
    let mut m = new_map_wc(2);
    m.insert(InternerKey::new(key1), InnerValue::Str(val1.into()));
    m.insert(InternerKey::new(key2), InnerValue::Str(val2.into()));
    InnerValue::Map(m)
}
