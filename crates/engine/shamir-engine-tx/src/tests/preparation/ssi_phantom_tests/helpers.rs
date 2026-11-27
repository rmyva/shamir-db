use std::sync::Arc;

use bytes::Bytes;
use shamir_storage::storage_in_memory::InMemoryRepo;
use shamir_tx::predicate_set::{SORTED_PREFIX_LEN, SORTED_TAG};

use crate::repo::repo_instance::RepoInstance;
use crate::repo::repo_types::BoxRepo;

pub(super) fn make_repo() -> RepoInstance {
    let repo = Arc::new(InMemoryRepo::new());
    RepoInstance::new("test".into(), BoxRepo::InMemory(repo), Vec::new())
}

/// Deterministic stand-in for the interned index name id.
pub(super) fn test_index_id(name: &str) -> u64 {
    let mut h: u64 = 1_469_598_103_934_665_603;
    for b in name.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(1_099_511_628_211);
    }
    h
}

/// Build a posting key in the physical sorted-index format:
///   SORTED_TAG (1) || index_id BE8 (8) || encoded_value || dummy_rid (16)
pub(super) fn test_posting_key(index_id: u64, age: i64) -> Bytes {
    let mut k = Vec::with_capacity(1 + 8 + 9 + 16);
    k.push(SORTED_TAG);
    k.extend_from_slice(&index_id.to_be_bytes());
    shamir_types::core::sort_codec::encode_i64(&mut k, age);
    k.extend_from_slice(&[0u8; 16]); // dummy rid
    Bytes::from(k)
}

/// Build a bound key (same as posting key but without the trailing rid).
pub(super) fn test_bound_key(index_id: u64, age: i64) -> Bytes {
    let mut k = Vec::with_capacity(SORTED_PREFIX_LEN + 9);
    k.push(SORTED_TAG);
    k.extend_from_slice(&index_id.to_be_bytes());
    shamir_types::core::sort_codec::encode_i64(&mut k, age);
    Bytes::from(k)
}
