//! Pure expression AST for functional indexes.
//!
//! `IndexExpr` is a closed whitelist of deterministic, side-effect-free
//! transforms over `InnerValue`. No I/O, no time-dependency, no WASM.
//! Each function is O(1) or O(n) in the size of the input string.
//!
//! §5b floor: `eval` takes a `RecordRef` lens (no input conversion); its
//! result is an OWNED COMPUTED value (a transform output, not a record
//! materialization), so it is irreducibly `InnerValue`. The single record
//! touch is `materialize_at` on the `Field` leaf — the `RecordRef` trait's
//! documented escape hatch.

use std::cell::{Cell, OnceCell};

use serde::{Deserialize, Serialize};
use shamir_funclib::registry::ScalarError;
use shamir_types::codecs::interned::common::intern_string_key;
use shamir_types::codecs::interned::{inner_value_to_query_value_with, query_value_to_inner_with};
use shamir_types::core::interner::{Interner, InternerKey};
use shamir_types::record_view::RecordRef;
use shamir_types::types::value::{InnerValue, QueryValue};
use smallvec::SmallVec;

/// A pure, deterministic expression that transforms a record's field
/// value into a computed index key.
/// Bincode persists variant ordinals: never reorder or remove variants; append only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexExpr {
    /// Extract a field by interned path.
    Field(Vec<u64>),
    /// Lowercase a string value.
    Lower(Box<IndexExpr>),
    /// Uppercase a string value.
    Upper(Box<IndexExpr>),
    /// Trim leading/trailing whitespace.
    Trim(Box<IndexExpr>),
    /// String or list length.
    Length(Box<IndexExpr>),
    /// Substring extraction.
    Substring {
        src: Box<IndexExpr>,
        start: u32,
        len: u32,
    },
    /// Traverse into a nested map by interned keys.
    NestedPath(Box<IndexExpr>, Vec<u64>),
    /// Concatenate N expressions' string results.
    Concat(Vec<IndexExpr>),
    /// Integer modulo (useful for shard/partition indexes).
    Mod(Box<IndexExpr>, i64),
    /// First non-null value (NULL handling).
    Coalesce(Vec<IndexExpr>),
    /// User-registered scalar function applied to a field. The `name`
    /// is resolved at eval time against a [`ScalarResolver`] by the
    /// `FunctionalBackend`. Only `.trusted_pure()`-vouched scalars are
    /// allowed here — the name is persisted, the callable is per-process.
    Scalar { name: String, inner: Box<IndexExpr> },
}

#[derive(Debug, thiserror::Error)]
pub enum ExprError {
    #[error("type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: &'static str, got: String },
    #[error("field not found")]
    FieldNotFound,
    #[error("division by zero")]
    DivisionByZero,
    #[error("remainder overflow")]
    RemainderOverflow,
    #[error("scalar function error: {0}")]
    ScalarError(String),
}

impl IndexExpr {
    /// Evaluate the expression against a record. The record must be
    /// a map at the top level (as stored by the engine).
    ///
    /// Generic over `RecordRef` so both `InnerValue` (tree) and
    /// `RecordView` (zero-copy lens) callers work without conversion.
    /// Returns an owned computed value (§5b floor — not a record materialization).
    pub fn eval(&self, rec: &(impl RecordRef + ?Sized)) -> Result<InnerValue, ExprError> {
        self.eval_with_scalars(rec, None)
    }

    /// Evaluate with an optional scalar resolver for `IndexExpr::Scalar` variants.
    /// When a `Scalar` variant is encountered and `resolver` is `None` or the
    /// name doesn't resolve, returns `ExprError::ScalarError`.
    pub fn eval_with_scalars(
        &self,
        rec: &(impl RecordRef + ?Sized),
        resolver: Option<&shamir_funclib::scalar_resolver::ScalarResolver>,
    ) -> Result<InnerValue, ExprError> {
        match self {
            IndexExpr::Field(path) => {
                let ipath: SmallVec<[InternerKey; 4]> =
                    path.iter().map(|&id| InternerKey::new(id)).collect();
                rec.materialize_at(&ipath).ok_or(ExprError::FieldNotFound)
            }

            IndexExpr::Lower(inner) => match inner.eval_with_scalars(rec, resolver)? {
                InnerValue::Str(s) => Ok(InnerValue::Str(s.to_lowercase())),
                other => Err(type_err("string", &other)),
            },

            IndexExpr::Upper(inner) => match inner.eval_with_scalars(rec, resolver)? {
                InnerValue::Str(s) => Ok(InnerValue::Str(s.to_uppercase())),
                other => Err(type_err("string", &other)),
            },

            IndexExpr::Trim(inner) => match inner.eval_with_scalars(rec, resolver)? {
                InnerValue::Str(s) => Ok(InnerValue::Str(s.trim().to_string())),
                other => Err(type_err("string", &other)),
            },

            IndexExpr::Length(inner) => match inner.eval_with_scalars(rec, resolver)? {
                InnerValue::Str(s) => Ok(InnerValue::Int(s.len() as i64)),
                InnerValue::List(v) => Ok(InnerValue::Int(v.len() as i64)),
                other => Err(type_err("string or list", &other)),
            },

            IndexExpr::Substring { src, start, len } => {
                match src.eval_with_scalars(rec, resolver)? {
                    InnerValue::Str(s) => {
                        let start = *start as usize;
                        let len = *len as usize;
                        let chars: Vec<char> = s.chars().skip(start).take(len).collect();
                        Ok(InnerValue::Str(chars.into_iter().collect()))
                    }
                    other => Err(type_err("string", &other)),
                }
            }

            IndexExpr::NestedPath(inner, segments) => {
                let val = inner.eval_with_scalars(rec, resolver)?;
                resolve_path(&val, segments)
            }

            IndexExpr::Concat(exprs) => {
                let mut out = String::new();
                for e in exprs {
                    match e.eval_with_scalars(rec, resolver)? {
                        InnerValue::Str(s) => out.push_str(&s),
                        InnerValue::Int(n) => out.push_str(&n.to_string()),
                        InnerValue::F64(f) => out.push_str(&f.to_string()),
                        InnerValue::Bool(b) => out.push_str(if b { "true" } else { "false" }),
                        InnerValue::Null => out.push_str("null"),
                        other => return Err(type_err("stringifiable", &other)),
                    }
                }
                Ok(InnerValue::Str(out))
            }

            IndexExpr::Mod(inner, divisor) => {
                if *divisor == 0 {
                    return Err(ExprError::DivisionByZero);
                }
                match inner.eval_with_scalars(rec, resolver)? {
                    InnerValue::Int(n) => n
                        .checked_rem(*divisor)
                        .map(InnerValue::Int)
                        .ok_or(ExprError::RemainderOverflow),
                    other => Err(type_err("int", &other)),
                }
            }

            IndexExpr::Coalesce(exprs) => {
                for e in exprs {
                    match e.eval_with_scalars(rec, resolver) {
                        Ok(InnerValue::Null) | Err(ExprError::FieldNotFound) => continue,
                        result => return result,
                    }
                }
                Ok(InnerValue::Null)
            }

            IndexExpr::Scalar { name, inner } => {
                let val = inner.eval_with_scalars(rec, resolver)?;
                // Each scalar invocation gets a fresh scratch namespace, just as
                // before; only map-key conversion initializes the interner.
                let scratch = OnceCell::new();
                let qv_arg = scalar_to_query(&val, &scratch)?;
                let resolver = resolver.ok_or_else(|| {
                    ExprError::ScalarError("no scalar resolver available".to_string())
                })?;
                let result_qv = resolver
                    .call_indexable(name, std::slice::from_ref(&qv_arg))
                    .map_err(|e: ScalarError| ExprError::ScalarError(e.code))?;
                scalar_from_query(&result_qv, &scratch)
            }
        }
    }
}

fn scalar_to_query(
    value: &InnerValue,
    scratch: &OnceCell<Interner>,
) -> Result<QueryValue, ExprError> {
    let missing = Cell::new(None);
    inner_value_to_query_value_with(value, &|id| {
        let resolved = scratch
            .get_or_init(Interner::new)
            .with_str(&InternerKey::new(id), str::to_owned);
        if resolved.is_none() {
            missing.set(Some(id));
        }
        resolved
    })
    .map_err(|error| match missing.get() {
        Some(id) => ExprError::ScalarError(format!(
            "Failed to decode data: Interned key not found: {:?}",
            InternerKey::new(id)
        )),
        None => ExprError::ScalarError(error.to_string()),
    })
}

fn scalar_from_query(
    value: &QueryValue,
    scratch: &OnceCell<Interner>,
) -> Result<InnerValue, ExprError> {
    query_value_to_inner_with(value, &|key| {
        intern_string_key(scratch.get_or_init(Interner::new), key)
    })
    .map_err(|error| ExprError::ScalarError(error.to_string()))
}

#[cfg(test)]
#[path = "../../tests/fts/scalar_lazy_tests.rs"]
mod scalar_lazy_tests;

fn resolve_path(val: &InnerValue, path: &[u64]) -> Result<InnerValue, ExprError> {
    let mut current = val;
    for &seg in path {
        match current {
            InnerValue::Map(m) => {
                let key = InternerKey::new(seg);
                match m.get(&key) {
                    Some(v) => current = v,
                    None => return Err(ExprError::FieldNotFound),
                }
            }
            _ => return Err(ExprError::FieldNotFound),
        }
    }
    Ok(current.clone())
}

fn type_err(expected: &'static str, got: &InnerValue) -> ExprError {
    ExprError::TypeMismatch {
        expected,
        got: format!("{:?}", std::mem::discriminant(got)),
    }
}
