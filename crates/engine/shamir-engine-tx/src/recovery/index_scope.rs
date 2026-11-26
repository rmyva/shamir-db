//! Resolve a literal WAL posting key against the target table's loaded indexes.

use std::mem::size_of;

use shamir_index::base_index::index_record_key::posting_index_prefix;
use shamir_index::model::identity::markers::{REGULAR_V2, UNIQUE_V2};
use shamir_index::{type_tag, IndexKind, PostingKeyRef};
use shamir_storage::error::{DbError, DbResult};
use shamir_types::core::sorted_index_layout::{SORTED_PREFIX_LEN, SORTED_TAG};

use crate::table::TableManager;

// IndexRecordKey encodes [namespace][name: u64 LE]; v1 adds two u64 hashes
// and regular postings append a record ID. The legacy codec's sweep predicate
// only checks a minimum length, so use the exact persisted lengths here.
const BASE_PREFIX_LEN: usize = 1 + size_of::<u64>();
const LEGACY_UNIQUE_LEN: usize = BASE_PREFIX_LEN + 2 * size_of::<u64>();
const RECORD_ID_LEN: usize = 16;
const LEGACY_REGULAR_LEN: usize = LEGACY_UNIQUE_LEN + RECORD_ID_LEN;

struct PostingFormats<'a> {
    base: Option<(bool, u64)>,
    sorted: Option<u64>,
    index2: Option<PostingKeyRef<'a>>,
}

fn parse_base(key: &[u8]) -> Option<(bool, u64)> {
    let (&namespace, rest) = key.split_first()?;
    let unique = match namespace {
        0x00 if key.len() == LEGACY_REGULAR_LEN => false,
        0x01 if key.len() == LEGACY_UNIQUE_LEN => true,
        REGULAR_V2 | UNIQUE_V2
            if posting_index_prefix(key).is_some_and(|prefix| prefix.len() > BASE_PREFIX_LEN) =>
        {
            namespace == UNIQUE_V2
        }
        _ => return None,
    };
    let id = u64::from_le_bytes(rest.get(..BASE_PREFIX_LEN - 1)?.try_into().ok()?);
    Some((unique, id))
}

fn parse_sorted(key: &[u8]) -> Option<u64> {
    if key.first() != Some(&SORTED_TAG) || key.len() < SORTED_PREFIX_LEN + RECORD_ID_LEN {
        return None;
    }
    Some(u64::from_be_bytes(
        key[1..SORTED_PREFIX_LEN].try_into().ok()?,
    ))
}

fn parse_posting_key(key: &[u8]) -> PostingFormats<'_> {
    // Index2 starts with the ID, so keep overlapping interpretations for strict replay.
    PostingFormats {
        base: parse_base(key),
        sorted: parse_sorted(key),
        index2: PostingKeyRef::decode(key).filter(|posting| {
            matches!(
                posting.type_tag,
                type_tag::FTS | type_tag::FTS_V2 | type_tag::FUNCTIONAL | type_tag::FUNCTIONAL_V2
            )
        }),
    }
}

/// Accept a key matching any of the posting layouts recognized by strict replay:
/// - Legacy base: namespace `0x00` regular (41 bytes) or `0x01` unique (25 bytes).
/// - V2 base: regular/unique namespace, LE u64 name ID and nonempty identity;
///   regular also has a 16-byte record ID suffix (minimum 26/10 bytes, respectively).
/// - Sorted: sorted tag, BE u64 name ID, optional value bytes and a 16-byte record ID.
/// - Index2: LE u32 ID, FTS/FTS_V2/FUNCTIONAL/FUNCTIONAL_V2 tag, optional value
///   bytes and a 16-byte record ID.
///
/// Sharing the parser rejects unsupported shapes before opening stores without
/// duplicating format rules. Loaded definition identity is still checked after open.
pub(crate) fn is_supported_posting_key(key: &[u8]) -> bool {
    let formats = parse_posting_key(key);
    formats.base.is_some() || formats.sorted.is_some() || formats.index2.is_some()
}

fn count_base_matches(table: &TableManager, unique: bool, id: u64) -> usize {
    let definitions = if unique {
        table.unique_index_definitions()
    } else {
        table.index_definitions()
    };
    definitions
        .iter()
        .filter(|def| def.name_interned == id)
        .count()
}

fn count_sorted_matches(table: &TableManager, id: u64) -> usize {
    table
        .sorted_index_definitions()
        .iter()
        .filter(|def| def.name_interned == id)
        .count()
}

/// Accept only a key that resolves to exactly one loaded index definition.
pub(super) async fn validate_posting(table: &TableManager, key: &[u8]) -> DbResult<()> {
    let formats = parse_posting_key(key);
    let mut matches = formats
        .base
        .map_or(0, |(unique, id)| count_base_matches(table, unique, id))
        + formats
            .sorted
            .map_or(0, |id| count_sorted_matches(table, id));
    if let Some(posting) = formats.index2 {
        matches += table
            .index2_descriptors()
            .await
            .iter()
            .filter(|descriptor| {
                descriptor.id == posting.index_id
                    && matches!(
                        (posting.type_tag, &descriptor.kind),
                        (type_tag::FTS | type_tag::FTS_V2, IndexKind::Fts { .. })
                            | (
                                type_tag::FUNCTIONAL | type_tag::FUNCTIONAL_V2,
                                IndexKind::Functional(_)
                            )
                    )
            })
            .count();
    }
    if matches != 1 {
        return Err(DbError::Validation(format!(
            "strict WAL replay: index posting on table {} matches {matches} definitions (expected one)",
            table.name()
        )));
    }
    Ok(())
}
