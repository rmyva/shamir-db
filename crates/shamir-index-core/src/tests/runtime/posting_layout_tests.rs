use crate::posting_layout::{build_posting_key, type_tag, PostingKeyRef, FIXED_OVERHEAD};
use shamir_types::types::record_id::RecordId;

fn rid_with(n: u8) -> RecordId {
    let mut a = [0u8; 16];
    a[15] = n;
    RecordId(a)
}

#[test]
fn round_trip_btree() {
    let val = [0xAAu8; 16];
    let rid = rid_with(7);
    let bytes = build_posting_key(123, type_tag::BTREE, &val, &rid);
    let r = PostingKeyRef::decode(&bytes).unwrap();
    assert_eq!(r.index_id, 123);
    assert_eq!(r.type_tag, type_tag::BTREE);
    assert_eq!(r.value_bytes, &val);
    assert_eq!(r.record_id, rid.as_bytes());
}

#[test]
fn round_trip_empty_value() {
    // FTS minimal posting: token-hash-only (8 bytes) is the
    // smallest realistic case, but the layout must also work
    // with empty value_bytes (e.g., Vector backends).
    let rid = rid_with(1);
    let bytes = build_posting_key(0, type_tag::VECTOR, &[], &rid);
    let r = PostingKeyRef::decode(&bytes).unwrap();
    assert_eq!(r.index_id, 0);
    assert_eq!(r.type_tag, type_tag::VECTOR);
    assert!(r.value_bytes.is_empty());
    assert_eq!(r.record_id, rid.as_bytes());
}

#[test]
fn round_trip_fts_token() {
    // FTS: 8-byte token hash.
    let token: [u8; 8] = 0x1234_5678_9abc_def0u64.to_le_bytes();
    let rid = rid_with(42);
    let bytes = build_posting_key(0xCAFE, type_tag::FTS, &token, &rid);
    let r = PostingKeyRef::decode(&bytes).unwrap();
    assert_eq!(r.index_id, 0xCAFE);
    assert_eq!(r.type_tag, type_tag::FTS);
    assert_eq!(r.value_bytes, &token);
    assert_eq!(r.record_id, rid.as_bytes());
}

#[test]
fn decode_rejects_truncated() {
    let too_short = [0u8; FIXED_OVERHEAD - 1];
    assert!(PostingKeyRef::decode(&too_short).is_none());
}

#[test]
fn record_id_owned_is_copy_of_borrowed() {
    let rid = rid_with(99);
    let bytes = build_posting_key(1, type_tag::BTREE, &[0; 16], &rid);
    let r = PostingKeyRef::decode(&bytes).unwrap();
    assert_eq!(r.record_id_owned(), rid);
}
