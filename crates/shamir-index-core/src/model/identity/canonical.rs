//! Canonical identity codec `C` (IDV2, design P34-12 section (a)) — frozen
//! format; any change is an identity-version bump.
//!
//! Encodes a value's EQUALITY CLASS as self-delimiting big-endian bytes:
//! tags are one byte, lengths and counts are `u32` BE, no `usize`, no
//! `Hash` impl. The numeric classes come from
//! [`NumericKey`](shamir_types::types::numeric_key::NumericKey) — the ONE
//! equality rule — so `Int(1) == F64(1.0) == Dec(1) == Big(1)` encode
//! identically while `F64(0.5)` and `Dec(0.5)` keep separate classes.
//! Top-level NaN has no encoding (a NaN matches nothing: no posting); a
//! NaN nested inside a List/Set/Map keeps its raw bits (class `0x04`).
//!
//! Two entry points exist by design and must stay byte-identical per leaf
//! (pinned by a parity test): the write path
//! [`encode_scalar_ref`](crate::model::identity::canonical::encode_scalar_ref)
//! (`ScalarRef`, Null/Bool/Int/F64/Str/Bin) and the lookup path
//! [`encode_inner_value`](crate::model::identity::canonical::encode_inner_value)
//! (full `InnerValue` incl. List/Set/Map). Composite base keys are the
//! concatenation of leaf codes in index path order.

use shamir_types::record_view::ScalarRef;
use shamir_types::types::numeric_key::{decimal_exact_parts, NumericKey};
use shamir_types::types::value::InnerValue;

/// Leaf tag: Null (no payload).
const TAG_NULL: u8 = 0x00;
/// Leaf tag: Bool (1 byte `0x00`/`0x01`).
const TAG_BOOL: u8 = 0x01;
/// Leaf tag: integer class — i64 BE. Int, integral F64 in i64 range,
/// -0.0, Big/Dec fitting i64.
const TAG_INT: u8 = 0x02;
/// Leaf tag: big integer — sign byte (`0x00` non-negative, `0x01`
/// negative) + u32 len + minimal BE magnitude.
const TAG_BIG_BITS: u8 = 0x03;
/// Leaf tag: float bits — u64 BE bit pattern (non-integral finite, ±inf).
const TAG_F64_BITS: u8 = 0x04;
/// Leaf tag: exact decimal — i128 BE mantissa + u32 BE scale.
const TAG_DECIMAL: u8 = 0x05;
/// Leaf tag: Str — u32 len + UTF-8.
const TAG_STR: u8 = 0x06;
/// Leaf tag: Bin — u32 len + bytes.
const TAG_BIN: u8 = 0x07;
/// Leaf tag: List — u32 count + element codes in order.
const TAG_LIST: u8 = 0x08;
/// Leaf tag: Set — u32 count + element codes sorted bytewise ascending,
/// duplicate (equal) codes removed.
const TAG_SET: u8 = 0x09;
/// Leaf tag: Map — u32 count + entries sorted by interner key id
/// ascending: u64 BE key id + value code.
const TAG_MAP: u8 = 0x0A;

/// Typed codec error. A top-level NaN has no index identity (no posting); a
/// length or count above `u32::MAX` is refused, never truncated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    #[error("NaN has no index identity: a NaN matches nothing")]
    Nan,
    #[error("{what} of {len} exceeds u32::MAX; refusing to truncate")]
    TooLarge { what: &'static str, len: u64 },
}

/// Appends a u32 BE length/count, returning a typed error above `u32::MAX`.
fn put_len(buf: &mut Vec<u8>, len: usize, what: &'static str) -> Result<(), CodecError> {
    let len = u32::try_from(len).map_err(|_| CodecError::TooLarge {
        what,
        len: len as u64,
    })?;
    buf.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

/// Encodes one leaf of the shared [`NumericKey`] class. `F64Bits` callers
/// must have rejected NaN already (see [`encode_f64_leaf`]).
fn put_numeric_key(buf: &mut Vec<u8>, key: NumericKey) -> Result<(), CodecError> {
    match key {
        NumericKey::Int(i) => {
            buf.push(TAG_INT);
            buf.extend_from_slice(&i.to_be_bytes());
        }
        NumericKey::BigBits {
            negative,
            magnitude_be,
        } => {
            buf.push(TAG_BIG_BITS);
            buf.push(u8::from(negative));
            put_len(buf, magnitude_be.len(), "big integer magnitude")?;
            buf.extend_from_slice(&magnitude_be);
        }
        NumericKey::F64Bits(bits) => {
            buf.push(TAG_F64_BITS);
            buf.extend_from_slice(&bits.to_be_bytes());
        }
    }
    Ok(())
}

/// One `f64` leaf: a top-level NaN is refused (it matches nothing), a
/// `nested` NaN keeps its raw bits; an integral finite f64 lands in the
/// integer class (via [`NumericKey::from_f64`], which also routes
/// integral ≥ 2^63 through `BigBits`); everything else keeps raw bits.
fn put_f64_leaf(buf: &mut Vec<u8>, f: f64, nested: bool) -> Result<(), CodecError> {
    if f.is_nan() && !nested {
        return Err(CodecError::Nan);
    }
    put_numeric_key(buf, NumericKey::from_f64(f))
}

/// One non-integral `Dec` leaf: i128 BE mantissa + u32 BE scale from
/// [`decimal_exact_parts`]. Integral decimals route through
/// [`NumericKey::from_decimal`] instead (integer / big-integer class).
fn put_decimal_leaf(buf: &mut Vec<u8>, (mantissa, scale): &(i128, u32)) {
    buf.push(TAG_DECIMAL);
    buf.extend_from_slice(&mantissa.to_be_bytes());
    buf.extend_from_slice(&scale.to_be_bytes());
}

fn put_str(buf: &mut Vec<u8>, s: &str) -> Result<(), CodecError> {
    buf.push(TAG_STR);
    put_len(buf, s.len(), "string length")?;
    buf.extend_from_slice(s.as_bytes());
    Ok(())
}

fn put_bin(buf: &mut Vec<u8>, b: &[u8]) -> Result<(), CodecError> {
    buf.push(TAG_BIN);
    put_len(buf, b.len(), "binary length")?;
    buf.extend_from_slice(b);
    Ok(())
}

/// Encodes one write-path scalar leaf into `buf` (composite keys
/// concatenate these in index path order). Same bytes as the lookup-path
/// encoder for every representable class.
pub fn encode_scalar_ref_into(v: &ScalarRef<'_>, buf: &mut Vec<u8>) -> Result<(), CodecError> {
    match v {
        ScalarRef::Null => buf.push(TAG_NULL),
        ScalarRef::Bool(b) => {
            buf.push(TAG_BOOL);
            buf.push(u8::from(*b));
        }
        ScalarRef::Int(i) => {
            buf.push(TAG_INT);
            buf.extend_from_slice(&i.to_be_bytes());
        }
        ScalarRef::F64(f) => put_f64_leaf(buf, *f, false)?,
        ScalarRef::Str(s) => put_str(buf, s)?,
        ScalarRef::Bin(b) => put_bin(buf, b)?,
    }
    Ok(())
}

/// Canonical bytes `C` of one write-path scalar leaf.
pub fn encode_scalar_ref(v: &ScalarRef<'_>) -> Result<Vec<u8>, CodecError> {
    let mut buf = Vec::new();
    encode_scalar_ref_into(v, &mut buf)?;
    Ok(buf)
}

/// Encodes one full lookup-path value (incl. List/Set/Map) into `buf`.
/// Set elements are encoded, sorted bytewise and deduplicated (equal
/// canonical codes); Map entries are sorted by interner key id ascending.
/// A top-level NaN is refused; a NaN inside a List/Set/Map is encoded.
pub fn encode_inner_value_into(v: &InnerValue, buf: &mut Vec<u8>) -> Result<(), CodecError> {
    encode_value(v, buf, false)
}

/// Recursive encoder; `nested` is true below a List/Set/Map.
fn encode_value(v: &InnerValue, buf: &mut Vec<u8>, nested: bool) -> Result<(), CodecError> {
    match v {
        InnerValue::Null => {
            buf.push(TAG_NULL);
            Ok(())
        }
        InnerValue::Bool(b) => {
            buf.push(TAG_BOOL);
            buf.push(u8::from(*b));
            Ok(())
        }
        InnerValue::Int(i) => {
            buf.push(TAG_INT);
            buf.extend_from_slice(&i.to_be_bytes());
            Ok(())
        }
        InnerValue::F64(f) => put_f64_leaf(buf, *f, nested),
        InnerValue::Dec(d) => match NumericKey::from_decimal(d) {
            Some(key) => put_numeric_key(buf, key),
            None => {
                put_decimal_leaf(buf, &decimal_exact_parts(d));
                Ok(())
            }
        },
        InnerValue::Big(b) => put_numeric_key(buf, NumericKey::from_bigint(b)),
        InnerValue::Str(s) => put_str(buf, s),
        InnerValue::Bin(b) => put_bin(buf, b),
        InnerValue::List(items) => {
            buf.push(TAG_LIST);
            put_len(buf, items.len(), "list length")?;
            for item in items {
                encode_value(item, buf, true)?;
            }
            Ok(())
        }
        InnerValue::Set(items) => {
            let mut codes: Vec<Vec<u8>> = Vec::with_capacity(items.len());
            for item in items {
                let mut code = Vec::new();
                encode_value(item, &mut code, true)?;
                codes.push(code);
            }
            codes.sort();
            codes.dedup();
            buf.push(TAG_SET);
            put_len(buf, codes.len(), "set length")?;
            for code in &codes {
                buf.extend_from_slice(code);
            }
            Ok(())
        }
        InnerValue::Map(entries) => {
            let mut encoded: Vec<(u64, Vec<u8>)> = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                let mut code = Vec::new();
                encode_value(value, &mut code, true)?;
                encoded.push((key.id(), code));
            }
            encoded.sort_by_key(|(id, _)| *id);
            buf.push(TAG_MAP);
            put_len(buf, encoded.len(), "map length")?;
            for (id, code) in &encoded {
                buf.extend_from_slice(&id.to_be_bytes());
                buf.extend_from_slice(code);
            }
            Ok(())
        }
    }
}

/// Canonical bytes `C` of one full lookup-path value.
pub fn encode_inner_value(v: &InnerValue) -> Result<Vec<u8>, CodecError> {
    let mut buf = Vec::new();
    encode_inner_value_into(v, &mut buf)?;
    Ok(buf)
}
