//! Identity bytes `I` — the inline/digest split that goes into a posting
//! key (design P34-12 section (a)). Frozen: `INLINE_MAX = 256` is part of
//! the format.
//!
//! `|C| <= INLINE_MAX`: `I = C` (inline, no hash at all). `|C| > INLINE_MAX`:
//! `I = 0xFF || BLAKE3-256(C)` with `derive_key` context
//! [`DIGEST_CONTEXT`] (33 bytes). `0xFF` is never a leaf tag, so inline and
//! digest identities are disjoint and both self-delimiting. In the digest
//! form the posting VALUE carries `C` (after the owner `RecordId` for the
//! unique family), so equality is always decided on canonical bytes.

use shamir_types::types::record_id::RecordId;

use super::markers::DIGEST_MARKER;

/// Inline cap in bytes: `|C| <= INLINE_MAX` keeps `I = C`; above it the
/// digest form is used.
pub const INLINE_MAX: usize = 256;

/// `blake3` `derive_key` context for the posting digest.
pub const DIGEST_CONTEXT: &str = "shamir-db index identity v2 posting digest";

/// Total length of the digest form: `0xFF` marker + BLAKE3-256.
pub const DIGEST_LEN: usize = 33;

/// Owner `RecordId` prefix length of a unique-posting value.
const RECORD_ID_LEN: usize = 16;

/// BLAKE3-256 of `canonical` under the frozen `derive_key` context,
/// prefixed with the [`DIGEST_MARKER`] byte.
#[must_use]
pub fn digest_identity(canonical: &[u8]) -> [u8; DIGEST_LEN] {
    let hash = blake3::Hasher::new_derive_key(DIGEST_CONTEXT)
        .update(canonical)
        .finalize();
    let mut out = [0u8; DIGEST_LEN];
    out[0] = DIGEST_MARKER;
    out[1..].copy_from_slice(hash.as_bytes());
    out
}

/// Identity bytes `I` of canonical bytes `C`: inline when `|C| <=
/// [`INLINE_MAX`]`, digest form otherwise.
#[must_use]
pub fn identity_bytes(canonical: &[u8]) -> Vec<u8> {
    if canonical.len() <= INLINE_MAX {
        canonical.to_vec()
    } else {
        digest_identity(canonical).to_vec()
    }
}

/// The canonical-byte tail `C` carried in a posting value after the 16-byte
/// owner prefix (`None` for the inline form, whose value has no tail).
#[must_use]
pub fn posting_value_tail(posting_value: &[u8]) -> Option<&[u8]> {
    posting_value
        .get(RECORD_ID_LEN..)
        .filter(|tail| !tail.is_empty())
}

/// Tail-verification helper: does the posting value's `C` tail equal the
/// probe's `C`? (Inline postings have no tail and never match this way.)
#[must_use]
pub fn posting_tail_matches(posting_value: &[u8], probe_c: &[u8]) -> bool {
    posting_value_tail(posting_value) == Some(probe_c)
}

/// Encodes a unique-posting value: inline form is the owner `RecordId`
/// (16 B) only; digest form is `RecordId` + `C`.
#[must_use]
pub fn encode_unique_posting_value(owner: &RecordId, canonical: Option<&[u8]>) -> Vec<u8> {
    let mut out = Vec::with_capacity(RECORD_ID_LEN + canonical.map_or(0, <[u8]>::len));
    out.extend_from_slice(owner.as_bytes());
    if let Some(c) = canonical {
        out.extend_from_slice(c);
    }
    out
}

/// Decodes the owner of a unique posting: the first 16 bytes as a
/// [`RecordId`], whenever at least 16 bytes are present.
#[must_use]
pub fn unique_posting_owner(posting_value: &[u8]) -> Option<RecordId> {
    if posting_value.len() < RECORD_ID_LEN {
        return None;
    }
    RecordId::try_from_bytes(&posting_value[..RECORD_ID_LEN])
}
