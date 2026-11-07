use crate::expr::{ExprError, IndexExpr};
use shamir_funclib::registry::FnEntry;
use shamir_funclib::scalar_resolver::{ScalarResolver, UserScalarLayer};
use shamir_types::core::interner::{Interner, InternerKey, TouchInd};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::value::{InnerValue, QueryValue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn intern(i: &Interner, s: &str) -> u64 {
    match i.touch_ind(s).unwrap() {
        TouchInd::Exists(k) | TouchInd::New(k) => k.id(),
    }
}

fn make_rec(interner: &Interner) -> InnerValue {
    let mut m = new_map_wc(5);
    m.insert(
        InternerKey::new(intern(interner, "email")),
        InnerValue::Str("  Alice@Example.COM  ".into()),
    );
    m.insert(
        InternerKey::new(intern(interner, "age")),
        InnerValue::Int(30),
    );
    m.insert(
        InternerKey::new(intern(interner, "tags")),
        InnerValue::List(vec![
            InnerValue::Str("a".into()),
            InnerValue::Str("b".into()),
        ]),
    );
    m.insert(InternerKey::new(intern(interner, "name")), InnerValue::Null);
    InnerValue::Map(m)
}

#[test]
fn lower() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Lower(Box::new(IndexExpr::Field(vec![intern(&i, "email")])));
    let v = expr.eval(&rec).unwrap();
    assert_eq!(v, InnerValue::Str("  alice@example.com  ".into()));
}

#[test]
fn upper() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Upper(Box::new(IndexExpr::Field(vec![intern(&i, "email")])));
    let v = expr.eval(&rec).unwrap();
    assert_eq!(v, InnerValue::Str("  ALICE@EXAMPLE.COM  ".into()));
}

#[test]
fn trim() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Trim(Box::new(IndexExpr::Field(vec![intern(&i, "email")])));
    let v = expr.eval(&rec).unwrap();
    assert_eq!(v, InnerValue::Str("Alice@Example.COM".into()));
}

#[test]
fn lower_trim_compose() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Lower(Box::new(IndexExpr::Trim(Box::new(IndexExpr::Field(vec![
        intern(&i, "email"),
    ])))));
    let v = expr.eval(&rec).unwrap();
    assert_eq!(v, InnerValue::Str("alice@example.com".into()));
}

#[test]
fn length_string() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Length(Box::new(IndexExpr::Field(vec![intern(&i, "email")])));
    assert_eq!(expr.eval(&rec).unwrap(), InnerValue::Int(21));
}

#[test]
fn length_list() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Length(Box::new(IndexExpr::Field(vec![intern(&i, "tags")])));
    assert_eq!(expr.eval(&rec).unwrap(), InnerValue::Int(2));
}

#[test]
fn substring() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Substring {
        src: Box::new(IndexExpr::Trim(Box::new(IndexExpr::Field(vec![intern(
            &i, "email",
        )])))),
        start: 0,
        len: 5,
    };
    assert_eq!(expr.eval(&rec).unwrap(), InnerValue::Str("Alice".into()));
}

#[test]
fn modulo() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Mod(Box::new(IndexExpr::Field(vec![intern(&i, "age")])), 7);
    assert_eq!(expr.eval(&rec).unwrap(), InnerValue::Int(2)); // 30 % 7 = 2
}

#[test]
fn rv_index_215567c1_mod_min_by_negative_one_returns_error() {
    let i = Interner::new();
    let rec = make_num_rec(&i, i64::MIN);
    let expr = IndexExpr::Mod(Box::new(IndexExpr::Field(vec![intern(&i, "n")])), -1);
    assert!(matches!(expr.eval(&rec), Err(ExprError::RemainderOverflow)));
}

fn make_num_rec(interner: &Interner, n: i64) -> InnerValue {
    let mut m = new_map_wc(1);
    m.insert(InternerKey::new(intern(interner, "n")), InnerValue::Int(n));
    InnerValue::Map(m)
}

#[test]
fn rv_index_bcd7caf0_scalar_dispatch_rejects_untrusted_user_entry() {
    let interner = Interner::new();
    let rec = make_rec(&interner);
    let user = Arc::new(UserScalarLayer::new());
    let calls = Arc::new(AtomicUsize::new(0));
    user.register(
        "index_scalar",
        FnEntry::pure(|_| Ok(QueryValue::Int(7)), 1, Some(1)).trusted_pure(),
    );
    let resolver = ScalarResolver::new(Arc::clone(&user));
    let replacement_calls = Arc::clone(&calls);
    user.register(
        "index_scalar",
        FnEntry::pure(
            move |_| {
                replacement_calls.fetch_add(1, Ordering::Relaxed);
                Ok(QueryValue::Int(9))
            },
            1,
            Some(1),
        ),
    );
    let expr = IndexExpr::Scalar {
        name: "index_scalar".into(),
        inner: Box::new(IndexExpr::Field(vec![intern(&interner, "age")])),
    };
    let result = expr.eval_with_scalars(&rec, Some(&resolver));
    assert!(matches!(result, Err(ExprError::ScalarError(code)) if code == "not_indexable"));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[test]
fn rv_index_bcd7caf0_trusted_scalar_dispatch_preserves_result_and_arity() {
    let interner = Interner::new();
    let rec = make_rec(&interner);
    let user = Arc::new(UserScalarLayer::new());
    user.register(
        "index_scalar",
        FnEntry::pure(|args| Ok(args[0].clone()), 1, Some(1)).trusted_pure(),
    );
    let resolver = ScalarResolver::new(user);
    let expr = IndexExpr::Scalar {
        name: "index_scalar".into(),
        inner: Box::new(IndexExpr::Field(vec![intern(&interner, "age")])),
    };
    assert_eq!(
        expr.eval_with_scalars(&rec, Some(&resolver)).unwrap(),
        InnerValue::Int(30)
    );
    assert_eq!(
        resolver
            .call_indexable("index_scalar", &[])
            .unwrap_err()
            .code,
        "arity"
    );
}

#[test]
fn scalar_list_round_trip_without_map_keys() {
    let interner = Interner::new();
    let rec = make_rec(&interner);
    let user = Arc::new(UserScalarLayer::new());
    user.register(
        "identity",
        FnEntry::pure(|args| Ok(args[0].clone()), 1, Some(1)).trusted_pure(),
    );
    let resolver = ScalarResolver::new(user);
    let expr = IndexExpr::Scalar {
        name: "identity".into(),
        inner: Box::new(IndexExpr::Field(vec![intern(&interner, "tags")])),
    };
    assert_eq!(
        expr.eval_with_scalars(&rec, Some(&resolver)).unwrap(),
        InnerValue::List(vec![
            InnerValue::Str("a".into()),
            InnerValue::Str("b".into())
        ])
    );
}

#[test]
fn scalar_returned_map_uses_fresh_scratch_and_nested_scalar_rejects_unknown_key() {
    let interner = Interner::new();
    let rec = make_rec(&interner);
    let user = Arc::new(UserScalarLayer::new());
    user.register(
        "map",
        FnEntry::pure(
            |_| {
                let mut map = new_map_wc(1);
                map.insert("answer".to_string(), QueryValue::Int(42));
                Ok(QueryValue::Map(map))
            },
            1,
            Some(1),
        )
        .trusted_pure(),
    );
    user.register(
        "identity",
        FnEntry::pure(|args| Ok(args[0].clone()), 1, Some(1)).trusted_pure(),
    );
    let resolver = ScalarResolver::new(user);
    let inner = IndexExpr::Scalar {
        name: "map".into(),
        inner: Box::new(IndexExpr::Field(vec![intern(&interner, "age")])),
    };
    let result = inner.eval_with_scalars(&rec, Some(&resolver)).unwrap();
    let scratch = Interner::new();
    let mut expected = new_map_wc(1);
    expected.insert(
        InternerKey::new(intern(&scratch, "answer")),
        InnerValue::Int(42),
    );
    assert_eq!(result, InnerValue::Map(expected));
    let outer = IndexExpr::Scalar {
        name: "identity".into(),
        inner: Box::new(inner),
    };
    let old_error =
        shamir_types::codecs::interned::inner_value_to_query_value(&result, &Interner::new())
            .unwrap_err()
            .to_string();
    assert!(matches!(
        outer.eval_with_scalars(&rec, Some(&resolver)),
        Err(ExprError::ScalarError(error)) if error == old_error
    ));
}

#[test]
fn modulo_div_zero() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Mod(Box::new(IndexExpr::Field(vec![intern(&i, "age")])), 0);
    assert!(matches!(expr.eval(&rec), Err(ExprError::DivisionByZero)));
}

#[test]
fn coalesce_skips_null() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Coalesce(vec![
        IndexExpr::Field(vec![intern(&i, "name")]),        // Null
        IndexExpr::Field(vec![intern(&i, "nonexistent")]), // FieldNotFound
        IndexExpr::Field(vec![intern(&i, "email")]),       // actual
    ]);
    let v = expr.eval(&rec).unwrap();
    assert_eq!(v, InnerValue::Str("  Alice@Example.COM  ".into()));
}

#[test]
fn coalesce_all_null() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Coalesce(vec![
        IndexExpr::Field(vec![intern(&i, "name")]),
        IndexExpr::Field(vec![intern(&i, "nonexistent")]),
    ]);
    assert_eq!(expr.eval(&rec).unwrap(), InnerValue::Null);
}

#[test]
fn concat() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Concat(vec![
        IndexExpr::Trim(Box::new(IndexExpr::Field(vec![intern(&i, "email")]))),
        IndexExpr::Field(vec![intern(&i, "age")]),
    ]);
    assert_eq!(
        expr.eval(&rec).unwrap(),
        InnerValue::Str("Alice@Example.COM30".into())
    );
}

#[test]
fn type_mismatch_on_lower_int() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Lower(Box::new(IndexExpr::Field(vec![intern(&i, "age")])));
    assert!(matches!(
        expr.eval(&rec),
        Err(ExprError::TypeMismatch { .. })
    ));
}

#[test]
fn field_not_found() {
    let i = Interner::new();
    let rec = make_rec(&i);
    let expr = IndexExpr::Field(vec![intern(&i, "unknown")]);
    assert!(matches!(expr.eval(&rec), Err(ExprError::FieldNotFound)));
}

#[test]
fn serde_round_trip() {
    let expr = IndexExpr::Lower(Box::new(IndexExpr::Trim(Box::new(IndexExpr::Field(vec![
        42,
    ])))));
    let bytes = bincode::serialize(&expr).unwrap();
    let got: IndexExpr = bincode::deserialize(&bytes).unwrap();
    match got {
        IndexExpr::Lower(inner) => match *inner {
            IndexExpr::Trim(inner2) => match *inner2 {
                IndexExpr::Field(p) => assert_eq!(p, vec![42]),
                _ => panic!("wrong inner2"),
            },
            _ => panic!("wrong inner"),
        },
        _ => panic!("wrong outer"),
    }
}
