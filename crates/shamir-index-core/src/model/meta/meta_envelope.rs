//! Versioned envelope for engine metadata.
//!
//! Wraps every persisted `__meta__/*` payload in
//! `[magic="SDB2"][version: u16][written_at_nanos: u64][payload: T]`
//! so future migrations can dispatch on `version` without ambiguity.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub const ENVELOPE_MAGIC: [u8; 4] = *b"SDB2";
pub const ENVELOPE_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaEnvelope<T> {
    pub magic: [u8; 4],
    pub version: u16,
    pub written_at_nanos: u64,
    pub payload: T,
}

#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("bad magic: expected {expected:?}, got {got:?}")]
    BadMagic { expected: [u8; 4], got: [u8; 4] },
    #[error("unsupported envelope version: {0}")]
    UnsupportedVersion(u16),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("encode error: {0}")]
    Encode(String),
}

/// Leading header fields only — deserialized first so a future version
/// reports `UnsupportedVersion`, not a misleading payload decode error.
#[derive(Deserialize)]
struct MetaEnvelopeHeader {
    magic: [u8; 4],
    version: u16,
}

impl<T> MetaEnvelope<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    pub fn new(payload: T) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Self {
            magic: ENVELOPE_MAGIC,
            version: ENVELOPE_VERSION,
            written_at_nanos: nanos,
            payload,
        }
    }

    /// Like [`MetaEnvelope::new`] but with an explicit envelope version
    /// (identity v2 writes `_m.idx` with version 2; design P34-12 section
    /// (b), item 3).
    pub fn new_with_version(version: u16, payload: T) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Self {
            magic: ENVELOPE_MAGIC,
            version,
            written_at_nanos: nanos,
            payload,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, MetaError> {
        bincode::serialize(self).map_err(|e| MetaError::Encode(e.to_string()))
    }

    /// Header-first open that ACCEPTS an explicit set of envelope versions
    /// and returns the version with the payload (identity v2 reads
    /// `_m.idx` written with version 1 or 2). Strict [`MetaEnvelope::open`]
    /// is unchanged.
    pub fn open_accepting(bytes: &[u8], versions: &[u16]) -> Result<(u16, T), MetaError> {
        let header: MetaEnvelopeHeader =
            bincode::deserialize(bytes).map_err(|e| MetaError::Decode(e.to_string()))?;
        if header.magic != ENVELOPE_MAGIC {
            return Err(MetaError::BadMagic {
                expected: ENVELOPE_MAGIC,
                got: header.magic,
            });
        }
        if !versions.contains(&header.version) {
            return Err(MetaError::UnsupportedVersion(header.version));
        }
        let env: MetaEnvelope<T> =
            bincode::deserialize(bytes).map_err(|e| MetaError::Decode(e.to_string()))?;
        Ok((header.version, env.payload))
    }

    pub fn open(bytes: &[u8]) -> Result<T, MetaError> {
        // Header-first so a future version reports UnsupportedVersion,
        // not a misleading payload Decode under today's schema.
        let header: MetaEnvelopeHeader =
            bincode::deserialize(bytes).map_err(|e| MetaError::Decode(e.to_string()))?;
        if header.magic != ENVELOPE_MAGIC {
            return Err(MetaError::BadMagic {
                expected: ENVELOPE_MAGIC,
                got: header.magic,
            });
        }
        if header.version != ENVELOPE_VERSION {
            return Err(MetaError::UnsupportedVersion(header.version));
        }
        let env: MetaEnvelope<T> =
            bincode::deserialize(bytes).map_err(|e| MetaError::Decode(e.to_string()))?;
        Ok(env.payload)
    }
}
