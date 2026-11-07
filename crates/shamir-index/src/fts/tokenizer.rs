//! Minimal tokenizers for FTS indexes.
//!
//! Zero-copy on ASCII paths (Cow::Borrowed), allocates only for
//! case-folding non-ASCII. No heavy deps — just std + manual
//! iteration.
//!
//! `TokenizerEnum` replaces `Box<dyn Tokenizer>` / `Arc<dyn Tokenizer>` on
//! hot paths — enum dispatch monomorphises the `tokenize` call and removes
//! vtable indirection.

use rustc_hash::FxHasher;
use shamir_collections::{TFxMap, TFxSet};
use std::borrow::Cow;
use std::hash::Hasher;
use std::sync::OnceLock;

use crate::backends::backend::IndexError;
use crate::fts::ngram_budget;
use crate::kind::StemLanguage;

pub trait Tokenizer: Send + Sync {
    /// The output may be unbounded: use only to clean up already-admitted values.
    /// New values and queries must use `try_tokenize`.
    fn tokenize<'a>(&self, text: &'a str) -> Vec<Cow<'a, str>>;
}

// ---------------------------------------------------------------------------
// Enum-dispatch wrapper — zero vtable overhead on hot tokenize() calls.
// ---------------------------------------------------------------------------

/// Owned enum that covers all built-in tokenizer variants.
/// Use this instead of `Box<dyn Tokenizer>` / `Arc<dyn Tokenizer>` where the
/// tokenizer is stored on a struct that lives on a hot path.
#[derive(Clone)]
pub enum TokenizerEnum {
    Whitespace(WhitespaceTokenizer),
    Unicode(UnicodeTokenizer),
    Ngram(NgramTokenizer),
    Full(FullTokenizer),
}

impl TokenizerEnum {
    /// Tokenize with per-value n-gram limits; returned tokens borrow only from `text`.
    pub fn try_tokenize<'a>(&self, text: &'a str) -> Result<Vec<Cow<'a, str>>, IndexError> {
        match self {
            Self::Ngram(t) => ngram_budget::try_tokenize(text, t.n),
            Self::Whitespace(_) | Self::Unicode(_) | Self::Full(_) => Ok(self.tokenize(text)),
        }
    }

    pub(crate) fn try_token_frequencies_with(
        &self,
        text: &str,
        mut encode: impl FnMut(&str) -> Vec<u8>,
    ) -> Result<(TFxMap<Vec<u8>, u32>, u32), IndexError> {
        let tokens = self.try_tokenize(text)?;
        let mut text_freq: TFxMap<String, u32> = TFxMap::default();
        for token in tokens {
            if let Some(count) = text_freq.get_mut(token.as_ref()) {
                *count += 1;
            } else {
                text_freq.insert(token.into_owned(), 1);
            }
        }
        let doc_len = text_freq.values().copied().sum();
        let mut freq: TFxMap<Vec<u8>, u32> = TFxMap::default();
        for (text, count) in text_freq {
            *freq.entry(encode(&text)).or_insert(0) += count;
        }
        Ok((freq, doc_len))
    }

    pub(crate) fn for_each_token_identity(&self, text: &str, visitor: impl FnMut(&[u8])) {
        self.for_each_token_identity_with(text, token_identity, visitor);
    }

    pub(crate) fn for_each_token_identity_with(
        &self,
        text: &str,
        mut encode: impl FnMut(&str) -> Vec<u8>,
        mut visitor: impl FnMut(&[u8]),
    ) {
        let mut identities: TFxMap<String, Vec<u8>> = TFxMap::default();
        let mut visit = |token: &str| {
            if let Some(identity) = identities.get(token) {
                visitor(identity);
            } else {
                let identity = encode(token);
                identities.insert(token.to_owned(), identity);
                visitor(identities.get(token).expect("identity inserted"));
            }
        };
        match self {
            Self::Ngram(tokenizer) => {
                ngram_budget::visit(text, tokenizer.n, |token| visit(token));
            }
            _ => {
                for token in self.tokenize(text) {
                    visit(&token);
                }
            }
        }
    }

    /// The output may be unbounded: use only to clean up already-admitted values.
    /// New values and queries must use `try_tokenize`.
    #[inline]
    pub fn tokenize<'a>(&self, text: &'a str) -> Vec<Cow<'a, str>> {
        match self {
            TokenizerEnum::Whitespace(t) => t.tokenize(text),
            TokenizerEnum::Unicode(t) => t.tokenize(text),
            TokenizerEnum::Ngram(t) => t.tokenize(text),
            TokenizerEnum::Full(t) => t.tokenize(text),
        }
    }
}

/// Split on whitespace, lowercase each token.
#[derive(Clone)]
pub struct WhitespaceTokenizer;

impl Tokenizer for WhitespaceTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<Cow<'a, str>> {
        text.split_whitespace()
            .map(|word| lowercase_cow(word))
            .collect()
    }
}

/// Split on non-alphanumeric boundaries (unicode-aware), lowercase.
#[derive(Clone)]
pub struct UnicodeTokenizer;

impl Tokenizer for UnicodeTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<Cow<'a, str>> {
        let mut tokens = Vec::new();
        let mut start = None;
        for (i, ch) in text.char_indices() {
            if ch.is_alphanumeric() {
                if start.is_none() {
                    start = Some(i);
                }
            } else if let Some(s) = start.take() {
                let word = &text[s..i];
                tokens.push(lowercase_cow(word));
            }
        }
        if let Some(s) = start {
            tokens.push(lowercase_cow(&text[s..]));
        }
        tokens
    }
}

fn lowercase_cow(word: &str) -> Cow<'_, str> {
    if word.chars().all(|c| !c.is_uppercase()) {
        Cow::Borrowed(word)
    } else {
        Cow::Owned(word.to_lowercase())
    }
}

// ---------------------------------------------------------------------------
// Character n-gram tokenizer
// ---------------------------------------------------------------------------

/// Character n-gram tokenizer: splits on non-alphanumeric (unicode-aware),
/// lowercases, then emits sliding character n-grams of length `n` per word.
/// A word shorter than `n` is emitted whole (so short words remain findable).
/// `n == 0` is treated as `1`. Enables substring / CJK / fuzzy matching where
/// word-boundary tokenization fails.
#[derive(Clone)]
pub struct NgramTokenizer {
    n: usize,
}

impl NgramTokenizer {
    pub fn new(n: u8) -> Self {
        Self {
            n: (n as usize).max(1),
        }
    }
}

impl Tokenizer for NgramTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<Cow<'a, str>> {
        let mut tokens = Vec::new();
        // Iterate words the same way UnicodeTokenizer does (alphanumeric runs).
        let mut start = None;
        for (i, ch) in text.char_indices() {
            if ch.is_alphanumeric() {
                if start.is_none() {
                    start = Some(i);
                }
            } else if let Some(s) = start.take() {
                ngram_budget::emit(&text[s..i], self.n, &mut tokens);
            }
        }
        if let Some(s) = start {
            ngram_budget::emit(&text[s..], self.n, &mut tokens);
        }
        tokens
    }
}

// ---------------------------------------------------------------------------
// Full pipeline tokenizer (stopwords + stemming)
// ---------------------------------------------------------------------------

/// Returns the curated stopword set for a language, if one exists.
///
/// Currently only English and Russian have curated stopword lists.
/// For all other Snowball languages the function returns `None`;
/// stemming still works — stopword filtering is simply skipped.
/// Non-EN/RU stopword lists are a future addition.
fn stopwords_for(lang: StemLanguage) -> Option<&'static TFxSet<&'static str>> {
    match lang {
        StemLanguage::English => Some(english_stopwords()),
        StemLanguage::Russian => Some(russian_stopwords()),
        StemLanguage::Arabic
        | StemLanguage::Danish
        | StemLanguage::Dutch
        | StemLanguage::Finnish
        | StemLanguage::French
        | StemLanguage::German
        | StemLanguage::Greek
        | StemLanguage::Hungarian
        | StemLanguage::Italian
        | StemLanguage::Norwegian
        | StemLanguage::Portuguese
        | StemLanguage::Romanian
        | StemLanguage::Spanish
        | StemLanguage::Swedish
        | StemLanguage::Tamil
        | StemLanguage::Turkish => None,
    }
}

/// Map [`StemLanguage`] → [`rust_stemmers::Algorithm`].
/// Exhaustive — no wildcard arm, so adding a new variant to
/// `StemLanguage` is a compile error until mapped here.
fn stem_algorithm(lang: StemLanguage) -> rust_stemmers::Algorithm {
    match lang {
        StemLanguage::English => rust_stemmers::Algorithm::English,
        StemLanguage::Russian => rust_stemmers::Algorithm::Russian,
        StemLanguage::Arabic => rust_stemmers::Algorithm::Arabic,
        StemLanguage::Danish => rust_stemmers::Algorithm::Danish,
        StemLanguage::Dutch => rust_stemmers::Algorithm::Dutch,
        StemLanguage::Finnish => rust_stemmers::Algorithm::Finnish,
        StemLanguage::French => rust_stemmers::Algorithm::French,
        StemLanguage::German => rust_stemmers::Algorithm::German,
        StemLanguage::Greek => rust_stemmers::Algorithm::Greek,
        StemLanguage::Hungarian => rust_stemmers::Algorithm::Hungarian,
        StemLanguage::Italian => rust_stemmers::Algorithm::Italian,
        StemLanguage::Norwegian => rust_stemmers::Algorithm::Norwegian,
        StemLanguage::Portuguese => rust_stemmers::Algorithm::Portuguese,
        StemLanguage::Romanian => rust_stemmers::Algorithm::Romanian,
        StemLanguage::Spanish => rust_stemmers::Algorithm::Spanish,
        StemLanguage::Swedish => rust_stemmers::Algorithm::Swedish,
        StemLanguage::Tamil => rust_stemmers::Algorithm::Tamil,
        StemLanguage::Turkish => rust_stemmers::Algorithm::Turkish,
    }
}

/// Full-pipeline FTS tokenizer: whitespace split → lowercase →
/// optional language-specific stopwords → optional snowball stemming.
///
/// Read-only after construction; safe to share across threads without
/// any mutex.
pub struct FullTokenizer {
    stopwords: Option<&'static TFxSet<&'static str>>,
    stemmer: Option<rust_stemmers::Stemmer>,
    /// Stored to allow `Clone` reconstruction — `rust_stemmers::Stemmer`
    /// holds a bare function pointer and doesn't implement `Clone`.
    stem_lang: Option<StemLanguage>,
}

impl Clone for FullTokenizer {
    fn clone(&self) -> Self {
        Self {
            stopwords: self.stopwords,
            stemmer: self
                .stem_lang
                .map(|lang| rust_stemmers::Stemmer::create(stem_algorithm(lang))),
            stem_lang: self.stem_lang,
        }
    }
}

impl FullTokenizer {
    pub fn new(language: StemLanguage, stopwords: bool, stem: bool) -> Self {
        let stop_set = if stopwords {
            stopwords_for(language)
        } else {
            None
        };
        let stem_lang = if stem { Some(language) } else { None };
        let stemmer = stem_lang.map(|lang| rust_stemmers::Stemmer::create(stem_algorithm(lang)));
        Self {
            stopwords: stop_set,
            stemmer,
            stem_lang,
        }
    }
}

impl Tokenizer for FullTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<Cow<'a, str>> {
        let mut out = Vec::with_capacity(4);
        for word in text.split_whitespace() {
            let lowered: Cow<'a, str> = lowercase_cow(word);
            if let Some(sw) = self.stopwords {
                if sw.contains(lowerd_str(&lowered)) {
                    continue;
                }
            }
            if let Some(ref stemmer) = self.stemmer {
                let stemmed = stemmer.stem(lowerd_str(&lowered)).into_owned();
                out.push(Cow::Owned(stemmed));
            } else {
                out.push(lowered);
            }
        }
        out
    }
}

fn lowerd_str<'a>(cow: &'a Cow<'a, str>) -> &'a str {
    cow.as_ref()
}

fn english_stopwords() -> &'static TFxSet<&'static str> {
    static SET: OnceLock<TFxSet<&'static str>> = OnceLock::new();
    SET.get_or_init(|| {
        [
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into",
            "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then",
            "there", "these", "they", "this", "to", "was", "will", "with",
        ]
        .iter()
        .copied()
        .collect()
    })
}

fn russian_stopwords() -> &'static TFxSet<&'static str> {
    static SET: OnceLock<TFxSet<&'static str>> = OnceLock::new();
    SET.get_or_init(|| {
        [
            "и",
            "в",
            "во",
            "не",
            "что",
            "он",
            "на",
            "я",
            "с",
            "со",
            "как",
            "а",
            "то",
            "все",
            "она",
            "так",
            "его",
            "но",
            "да",
            "ты",
            "к",
            "у",
            "же",
            "вы",
            "за",
            "бы",
            "по",
            "только",
            "её",
            "мне",
            "было",
            "вот",
            "от",
            "меня",
            "ещё",
            "нет",
            "о",
            "из",
            "ему",
            "теперь",
            "когда",
            "даже",
            "ну",
            "ли",
            "если",
            "уже",
            "или",
            "ни",
            "быть",
            "был",
            "него",
            "до",
            "вас",
            "нибудь",
            "опять",
            "уж",
            "вам",
            "ведь",
            "там",
            "потом",
            "себя",
            "ничего",
            "ей",
            "может",
            "они",
            "тут",
            "где",
            "есть",
            "надо",
            "ней",
            "для",
            "мы",
            "тебя",
            "их",
            "чем",
            "была",
            "сам",
            "чтоб",
            "без",
            "будто",
            "чего",
            "раз",
            "тоже",
            "себе",
            "под",
            "будет",
            "ж",
            "тогда",
            "кто",
            "этот",
            "того",
            "потому",
            "этого",
            "какой",
            "совсем",
            "ним",
            "здесь",
            "этом",
            "один",
            "почти",
            "мой",
            "тем",
            "чтобы",
            "нее",
            "сейчас",
            "были",
            "туда",
            "откуда",
            "этой",
            "перед",
            "иногда",
            "ведь",
            "тоже",
        ]
        .iter()
        .copied()
        .collect()
    })
}

/// Build a [`TokenizerEnum`] from a [`TokenizerKind`].
///
/// Returns an owned enum value — callers store it directly (or wrap in
/// `Arc`) instead of going through `Box<dyn Tokenizer>`.
pub fn build_tokenizer(kind: &crate::kind::TokenizerKind) -> TokenizerEnum {
    match kind {
        crate::kind::TokenizerKind::Whitespace => TokenizerEnum::Whitespace(WhitespaceTokenizer),
        crate::kind::TokenizerKind::Unicode => TokenizerEnum::Unicode(UnicodeTokenizer),
        crate::kind::TokenizerKind::Ngram { n } => TokenizerEnum::Ngram(NgramTokenizer::new(*n)),
        crate::kind::TokenizerKind::Full {
            language,
            stopwords,
            stem,
        } => TokenizerEnum::Full(FullTokenizer::new(*language, *stopwords, *stem)),
    }
}

/// Canonical IDV2 identity of a token string.
pub use shamir_index_core::tokenizer::token_identity;

/// Legacy token hash retained for decoding historical fixtures only.
pub fn token_hash(token: &str) -> u64 {
    let mut h = FxHasher::default();
    h.write(token.as_bytes());
    h.finish()
}
