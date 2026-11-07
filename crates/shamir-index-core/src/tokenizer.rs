//! Canonical IDV2 token identity.
//!
//! Lives in `shamir-index-core` because it is IDV2 identity (it delegates to
//! `model::identity::canonical`), not FTS; the FTS tokenizer in
//! `shamir-index` re-exports it, and `IndexBackend::tokenize_query`'s
//! default impl uses it.

use shamir_types::types::value::InnerValue;

/// Canonical IDV2 identity of a token string.
pub fn token_identity(token: &str) -> Vec<u8> {
    crate::model::identity::canonical::encode_inner_value(&InnerValue::Str(token.to_owned()))
        .expect("token strings fit the canonical length format")
}
