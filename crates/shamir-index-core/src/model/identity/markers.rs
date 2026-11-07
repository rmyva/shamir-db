//! IDV2 marker constants — the key-namespace / type-tag bytes that carry
//! the identity version per posting (design P34-12 section (a), "Per-family
//! layouts (v2)"). Frozen: any change is an identity-version bump.

/// Base regular index namespace, identity v2 (`[0x02][name_interned LE][I][rid]`).
pub const REGULAR_V2: u8 = 0x02;

/// Base unique index namespace, identity v2 (`[0x03][name_interned LE][I]`).
pub const UNIQUE_V2: u8 = 0x03;

/// index2 FTS type tag, identity v2 (`posting_layout::type_tag`).
pub const FTS_V2: u8 = 0x11;

/// index2 functional type tag, identity v2 (`posting_layout::type_tag`).
pub const FUNCTIONAL_V2: u8 = 0x12;

/// First byte of the digest form of identity bytes `I`
/// (`0xFF || BLAKE3-256(C)`); never a leaf tag, so inline and digest
/// identities are disjoint.
pub const DIGEST_MARKER: u8 = 0xFF;
