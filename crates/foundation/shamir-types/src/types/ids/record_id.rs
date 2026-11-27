use crate::types::base::{self, Base58Error};
use bytes::Bytes;
use rand::RngCore;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::fmt;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

/// The custom epoch for our RecordId timestamps, set to 2026-01-31 00:00:00 UTC.
/// This makes the timestamp part of the ID smaller and more manageable.
/// Value: 1_769_817_600_000_000 microseconds since the Unix epoch.
const CUSTOM_EPOCH_MICROS: i64 = 1_769_817_600_000_000;

/// Timestamp IDs produced by the constructors reserve the first 2^32 relative
/// microseconds so their first four bytes avoid the legacy system-ID prefix.
/// Historical persisted timestamp IDs and IDs supplied from arbitrary bytes
/// may still have that prefix; the 16-byte encoding is unchanged.
const SYSTEM_RECORD_PREFIX: &[u8] = &[0, 0, 0, 0];
const SYSTEM_PREFIX_TIMESTAMP_BOUNDARY: i64 = 1i64 << 32;

/// Identifier, not a secret or capability: possession must not grant access.
/// Timestamp-generated IDs disclose their relative timestamp, and their random
/// tails use a non-cryptographic PRNG and must not be treated as unpredictable.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordId(pub [u8; 16]);

impl RecordId {
    pub fn new() -> Self {
        Self::from_ts(Self::now_micros())
    }

    /// Returns the current wall-clock timestamp in microseconds —
    /// the same scale used by `RecordId::new()` / `RecordId::from_ts`.
    /// Call once before a batch loop, then feed the result to `from_ts`
    /// per row.
    #[inline]
    pub fn now_micros() -> i64 {
        unix_micros_from_system_time(SystemTime::now())
    }

    /// Creates a `RecordId` with a caller-supplied `timestamp_micros` and a
    /// fresh random tail. Relative timestamps in `[0, 2^32)` microseconds are
    /// clamped to 2^32 to preserve the system-ID prefix.
    pub fn from_ts(timestamp_micros: i64) -> Self {
        let mut bytes = [0u8; 16];

        // Timestamp part, relative to the custom epoch.
        let relative_micros = timestamp_micros.saturating_sub(CUSTOM_EPOCH_MICROS);
        let relative_micros = if (0..SYSTEM_PREFIX_TIMESTAMP_BOUNDARY).contains(&relative_micros) {
            SYSTEM_PREFIX_TIMESTAMP_BOUNDARY
        } else {
            relative_micros
        };
        bytes[0..8].copy_from_slice(&relative_micros.to_be_bytes());

        // Random part — thread-local Xoshiro256++ seeded once from OsRng.
        // ~1-2 ns/call vs ~5 ns for ChaCha (thread_rng). Xoshiro256++ has
        // excellent statistical quality (passes BigCrush) and is sufficient
        // for collision-resistant IDs; CSPRNG is unnecessary here.
        Self::fill_random_tail(&mut bytes[8..16]);
        Self(bytes)
    }

    /// Creates a `RecordId` with a caller-supplied `timestamp_micros` and
    /// an ascending `seq` counter embedded in the tail's high 4 bytes.
    /// Relative timestamps in `[0, 2^32)` microseconds clamp to 2^32;
    /// Layout: `[0..8] BE relative_ts | [8..12] BE seq (u32) | [12..16] 4 random bytes`.
    ///
    /// Use in batch paths: one `now_micros()` call per batch, then
    /// `from_ts_seq(batch_ts, 0)`, `from_ts_seq(batch_ts, 1)`, ...
    /// This preserves intra-batch monotonicity (ascending byte order)
    /// while keeping the single clock-read hoist from L13.
    pub fn from_ts_seq(timestamp_micros: i64, seq: u32) -> Self {
        let mut bytes = [0u8; 16];

        let relative_micros = timestamp_micros.saturating_sub(CUSTOM_EPOCH_MICROS);
        let relative_micros = if (0..SYSTEM_PREFIX_TIMESTAMP_BOUNDARY).contains(&relative_micros) {
            SYSTEM_PREFIX_TIMESTAMP_BOUNDARY
        } else {
            relative_micros
        };
        bytes[0..8].copy_from_slice(&relative_micros.to_be_bytes());

        // Sequence counter in tail's high 4 bytes — gives lex-ascending
        // order within a batch sharing the same timestamp prefix.
        bytes[8..12].copy_from_slice(&seq.to_be_bytes());

        // 4 random bytes for collision resistance across concurrent batches
        // sharing the same ts + seq (practically impossible but defensive).
        Self::fill_random_tail(&mut bytes[12..16]);
        Self(bytes)
    }

    /// Fills the given slice with random bytes from a thread-local
    /// Xoshiro256++ PRNG seeded once from OsRng.
    fn fill_random_tail(dest: &mut [u8]) {
        thread_local! {
            static RNG: RefCell<Xoshiro256PlusPlus> = {
                RefCell::new(Xoshiro256PlusPlus::from_os_rng())
            };
        }
        RNG.with(|rng| rng.borrow_mut().fill_bytes(dest));
    }

    /// Creates a deterministic, system-level `RecordId` from a string name.
    /// These IDs are used for internal metadata and are distinguished by a zero-timestamp prefix.
    /// The name is directly copied into the ID, truncated to 12 bytes.
    pub fn system(name: &str) -> Self {
        let mut bytes = [0u8; 16];
        // The first 4 bytes are the system prefix (zeros).
        // The rest is filled with the name's bytes.
        let name_bytes = name.as_bytes();
        let len_to_copy = std::cmp::min(name_bytes.len(), 12);
        bytes[4..4 + len_to_copy].copy_from_slice(&name_bytes[..len_to_copy]);
        Self(bytes)
    }

    /// Checks the reserved zero prefix. Newly generated timestamp IDs clamp
    /// relative times in `[0, 2^32)` microseconds to 2^32. Historical persisted
    /// and externally supplied IDs may still have this prefix and are
    /// indistinguishable from system IDs in the existing encoding.
    pub fn is_system(&self) -> bool {
        self.0.starts_with(SYSTEM_RECORD_PREFIX)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.0)
    }

    /// Construct a `RecordId` from a byte slice. Returns `None` if the
    /// slice isn't exactly 16 bytes.
    pub fn try_from_bytes(b: &[u8]) -> Option<Self> {
        let arr: [u8; 16] = b.try_into().ok()?;
        Some(Self(arr))
    }
}

/// Converts system time to Unix microseconds using the existing floor/ceiling rounding rules.
pub(crate) fn unix_micros_from_system_time(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or_else(|error| {
            let duration = error.duration();
            let micros = i128::try_from(duration.as_micros()).unwrap_or(i128::MAX);
            let micros = micros
                .checked_add(if duration.subsec_nanos() % 1_000 != 0 {
                    1
                } else {
                    0
                })
                .unwrap_or(i128::MAX);
            i64::try_from(micros.checked_neg().unwrap_or(i128::MIN)).unwrap_or(i64::MIN)
        })
}

impl Default for RecordId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordId({})", self)
    }
}

impl fmt::Display for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", base::encode(&self.0))
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct RecordIdError(#[from] Base58Error);

impl FromStr for RecordId {
    type Err = RecordIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let arr = base::decode16(s)?;
        Ok(RecordId(arr))
    }
}

impl Serialize for RecordId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for RecordId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: &[u8] = serde::Deserialize::deserialize(deserializer)?;
        let arr: [u8; 16] = bytes
            .try_into()
            .map_err(|_| serde::de::Error::invalid_length(bytes.len(), &"16 bytes"))?;
        Ok(RecordId(arr))
    }
}
