//! Zero-copy posting-key encoding / decoding.
//!
//! Layout: `[index_id: u32 LE][type_tag: u8][value_bytes...][record_id: 16 bytes]`
//!
//! `index_id` is short (u32) and assigned by `IndexRegistry`. `type_tag`
//! lives in the key (not just metadata) so storage-level scans are
//! type-safe — a Btree scan can't accidentally read FTS postings.

use shamir_types::types::record_id::RecordId;

pub mod type_tag {
    pub const BTREE: u8 = 0;
    pub const FTS: u8 = 1;
    pub const FUNCTIONAL: u8 = 2;
    pub const VECTOR: u8 = 3;
    pub const FTS_V2: u8 = 0x11;
    pub const FUNCTIONAL_V2: u8 = 0x12;
}

/// `index_id` (4) + `type_tag` (1) + `record_id` (16). The variable
/// part is `value_bytes`.
pub(crate) const FIXED_OVERHEAD: usize = 4 + 1 + 16;

/// Borrowed view of a posting key. Decodes from `&[u8]` without
/// allocating — the caller keeps ownership of the underlying buffer.
#[derive(Debug)]
pub struct PostingKeyRef<'a> {
    pub index_id: u32,
    pub type_tag: u8,
    pub value_bytes: &'a [u8],
    pub record_id: &'a [u8; 16],
}

impl<'a> PostingKeyRef<'a> {
    pub fn decode(bytes: &'a [u8]) -> Option<Self> {
        if bytes.len() < FIXED_OVERHEAD {
            return None;
        }
        let index_id = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let type_tag = bytes[4];
        let value_len = bytes.len() - FIXED_OVERHEAD;
        let value_bytes = &bytes[5..5 + value_len];
        let rid_slice = &bytes[5 + value_len..];
        // Safe: rid_slice.len() == 16 by arithmetic above.
        let record_id: &[u8; 16] = rid_slice.try_into().ok()?;
        Some(Self {
            index_id,
            type_tag,
            value_bytes,
            record_id,
        })
    }

    pub fn record_id_owned(&self) -> RecordId {
        RecordId(*self.record_id)
    }
}

/// Build a posting key with a single capacity-correct allocation.
pub fn build_posting_key(
    index_id: u32,
    type_tag: u8,
    value_bytes: &[u8],
    rid: &RecordId,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(FIXED_OVERHEAD + value_bytes.len());
    out.extend_from_slice(&index_id.to_le_bytes());
    out.push(type_tag);
    out.extend_from_slice(value_bytes);
    out.extend_from_slice(rid.as_bytes());
    out
}
