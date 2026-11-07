//! Discriminated index variants.
//!
//! Stored in `IndexDescriptor.kind`; serialized through `MetaEnvelope`
//! to `__meta__/indexes`. Big variants are `Box`-ed so the enum stays
//! compact on the hot path.

use serde::{Deserialize, Serialize};

/// Bincode persists variant ordinals: never reorder or remove variants; append only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexKind {
    Btree {
        unique: bool,
    },
    Fts {
        tokenizer: TokenizerKind,
        language: Option<String>,
    },
    Functional(Box<FunctionalConfig>),
    Vector(Box<VectorConfig>),
}

/// Bincode persists variant ordinals: never reorder or remove variants; append only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TokenizerKind {
    Whitespace,
    Unicode,
    Ngram {
        n: u8,
    },
    /// Full pipeline: whitespace split → lowercase → optional stopwords
    /// → optional snowball stemming.  `language` selects both the
    /// stopword list and the stemmer algorithm.
    Full {
        language: StemLanguage,
        #[serde(default = "default_true")]
        stopwords: bool,
        #[serde(default = "default_true")]
        stem: bool,
    },
}

/// Language selector for [`TokenizerKind::Full`].
///
/// Maps 1:1 to `rust_stemmers::Algorithm` and to a built-in stopword
/// list (where available).  Serialises as a lowercase string
/// (`"english"` / `"russian"` / ...).
///
/// # Bincode ordinal stability: append only
///
/// This enum is persisted via bincode which encodes variants by their
/// ordinal position.  **Never** reorder or insert variants before
/// existing ones — only append new languages at the end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StemLanguage {
    // ordinal 0 — DO NOT MOVE
    English,
    // ordinal 1 — DO NOT MOVE
    Russian,
    // --- new languages appended below (ordinal 2+) ---
    Arabic,
    Danish,
    Dutch,
    Finnish,
    French,
    German,
    Greek,
    Hungarian,
    Italian,
    Norwegian,
    Portuguese,
    Romanian,
    Spanish,
    Swedish,
    Tamil,
    Turkish,
}

impl StemLanguage {
    /// Parse a DSL string into a [`StemLanguage`].
    ///
    /// Accepts both the full lowercase name and the ISO 639-1
    /// two-letter code:
    ///
    /// ```text
    /// "english" | "en"  → English
    /// "russian" | "ru"  → Russian
    /// "french"  | "fr"  → French
    ///   ...etc...
    /// ```
    ///
    /// Returns `None` for unrecognised strings.
    pub fn from_dsl(s: &str) -> Option<Self> {
        match s {
            "english" | "en" => Some(Self::English),
            "russian" | "ru" => Some(Self::Russian),
            "arabic" | "ar" => Some(Self::Arabic),
            "danish" | "da" => Some(Self::Danish),
            "dutch" | "nl" => Some(Self::Dutch),
            "finnish" | "fi" => Some(Self::Finnish),
            "french" | "fr" => Some(Self::French),
            "german" | "de" => Some(Self::German),
            "greek" | "el" => Some(Self::Greek),
            "hungarian" | "hu" => Some(Self::Hungarian),
            "italian" | "it" => Some(Self::Italian),
            "norwegian" | "no" => Some(Self::Norwegian),
            "portuguese" | "pt" => Some(Self::Portuguese),
            "romanian" | "ro" => Some(Self::Romanian),
            "spanish" | "es" => Some(Self::Spanish),
            "swedish" | "sv" => Some(Self::Swedish),
            "tamil" | "ta" => Some(Self::Tamil),
            "turkish" | "tr" => Some(Self::Turkish),
            _ => None,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionalConfig {
    pub expr: crate::expr::IndexExpr,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum VectorMetric {
    L2,
    Cosine,
    Dot,
}

/// Vector quantization strategy for an HNSW index (opt-in).
///
/// Descriptor persistence stores this intent as a stable string in the
/// versioned `IndexDescriptor::options` map, not as a bincode ordinal.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum VectorQuantization {
    // ordinal 0 — DO NOT MOVE
    Sq8,
}

impl VectorQuantization {
    /// Parse a wire/DDL string into [`VectorQuantization`].
    ///
    /// Returns `None` for unrecognised strings (the caller treats `None`
    /// as "no quantization" — the legacy f32 path).
    pub fn from_dsl(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "sq8" => Some(Self::Sq8),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorConfig {
    pub dim: u32,
    pub metric: VectorMetric,
    pub backend: VectorBackendRef,
    /// Opt-in SQ8 scalar quantization. `None` (default) = unquantized f32
    /// HNSW path, bit-for-bit identical to pre-#411 behaviour.
    ///
    /// # Serialization
    ///
    /// `#[serde(skip)]` excludes this field from BOTH the bincode-persisted
    /// `IndexDescriptor` (the `PersistedIndexes` envelope) AND any JSON/msgpack
    /// serialization. Rationale: bincode 1.3.3 does NOT honour
    /// `#[serde(default)]` for skipped fields on read (it tries to read the
    /// field bytes and fails with `UnexpectedEof`), so adding a non-`skip`
    /// field would break every pre-#411 persisted index descriptor. The
    /// persistence layer carries intent in `IndexDescriptor::options`; the
    /// wire operation carries it separately as a string.
    #[serde(skip)]
    pub quantization: Option<VectorQuantization>,
}

/// A persisted legacy External variant (ordinal 1) is rejected during decode;
/// only the database's in-process HNSW backend is supported.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VectorBackendRef {
    InProcessHnsw { ef_construct: u32, m: u32 },
}
