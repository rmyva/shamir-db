//! P34-12 codec parity / boundary / envelope tests (design section (e) S1).

use crate::model::identity::canonical::{encode_inner_value, encode_scalar_ref, CodecError};
use crate::model::identity::identity_bytes::{
    digest_identity, encode_unique_posting_value, identity_bytes, posting_tail_matches,
    posting_value_tail, unique_posting_owner, DIGEST_CONTEXT, DIGEST_LEN, INLINE_MAX,
};
use crate::model::identity::markers;
use crate::model::meta::meta_envelope::{MetaEnvelope, MetaError};
use shamir_types::core::interner::InternerKey;
use shamir_types::record_view::ScalarRef;
use shamir_types::types::common::{new_map, new_set};
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::{InnerValue, Value};

fn map_of(entries: &[(u64, InnerValue)]) -> InnerValue {
    let mut m = new_map::<InternerKey, InnerValue>();
    for (id, v) in entries {
        m.insert(InternerKey::new(*id), v.clone());
    }
    Value::Map(m)
}

fn set_of(items: &[InnerValue]) -> InnerValue {
    let mut s = new_set::<InnerValue>();
    for v in items {
        s.insert(v.clone());
    }
    Value::Set(s)
}

#[test]
fn p34_12_codec_map_reversed_insertion_parity() {
    let ab = map_of(&[(5, Value::Int(1)), (7, Value::Int(2))]);
    let ba = map_of(&[(7, Value::Int(2)), (5, Value::Int(1))]);
    assert_eq!(
        encode_inner_value(&ab).unwrap(),
        encode_inner_value(&ba).unwrap()
    );
}

#[test]
fn p34_12_codec_set_order_parity_and_dedup() {
    let one_two = set_of(&[Value::Int(1), Value::Int(2)]);
    let two_one = set_of(&[Value::Int(2), Value::Int(1)]);
    assert_eq!(
        encode_inner_value(&one_two).unwrap(),
        encode_inner_value(&two_one).unwrap()
    );

    // Numerically-equal elements of different types dedup to one code.
    let dup = set_of(&[Value::Int(1), Value::F64(1.0), Value::Int(1)]);
    let expected = [0x09, 0, 0, 0, 1].to_vec();
    let mut expected = expected;
    expected.extend_from_slice(&encode_inner_value(&Value::Int(1)).unwrap());
    assert_eq!(encode_inner_value(&dup).unwrap(), expected);
}

#[test]
fn p34_12_codec_numeric_class_parity() {
    let c = |v: &InnerValue| encode_inner_value(v).unwrap();
    // Int/F64/Big/Dec of 1 are one class.
    let one = c(&Value::Int(1));
    assert_eq!(c(&Value::F64(1.0)), one);
    assert_eq!(c(&Value::Big(num_bigint::BigInt::from(1))), one);
    assert_eq!(c(&Value::Dec(rust_decimal::Decimal::from(1))), one);
    // Dec(1.0) normalizes to integral -> same class too.
    assert_eq!(c(&Value::Dec(rust_decimal::Decimal::new(10, 1))), one);

    // Non-integral F64 and Dec are DISTINCT classes (0x04 bits vs 0x05).
    let f = c(&Value::F64(0.5));
    let d = c(&Value::Dec(rust_decimal::Decimal::new(5, 1)));
    assert_ne!(f, d);
    assert_eq!(f[0], 0x04);
    assert_eq!(d[0], 0x05);

    // -0.0 == 0.
    assert_eq!(c(&Value::F64(-0.0)), c(&Value::Int(0)));

    // F64(2^63) == Big(2^63): from_f64 routes integral >= 2^63 through
    // BigBits, so both land in tag 0x03 with identical bytes.
    let two63 = num_bigint::BigInt::from(1i8) << 63;
    let big = c(&Value::Big(two63));
    let fl = c(&Value::F64(9_223_372_036_854_775_808.0));
    assert_eq!(big, fl);
    assert_eq!(big[0], 0x03);

    // NaN is refused on both paths.
    assert_eq!(
        encode_inner_value(&Value::F64(f64::NAN)),
        Err(CodecError::Nan)
    );
    assert_eq!(
        encode_scalar_ref(&ScalarRef::F64(f64::NAN)),
        Err(CodecError::Nan)
    );
}

#[test]
fn p34_12_codec_prefix_free() {
    // Deterministic xorshift64 corpus: nested lists/sets/maps/strs/bins of
    // varying sizes, no new dev-dependency.
    struct XorShift(u64);
    impl XorShift {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
    }
    fn gen(rng: &mut XorShift, depth: u32) -> InnerValue {
        let pick = rng.next() % 10;
        // Below the nesting cap only leaves are generated, so the recursion
        // is bounded whatever the random draws are.
        if depth >= 3 {
            return match pick % 3 {
                0 => Value::Int(rng.next() as i64),
                1 => Value::Str("y".repeat((rng.next() % 40) as usize)),
                _ => Value::Bin((0..rng.next() % 40).map(|_| rng.next() as u8).collect()),
            };
        }
        match pick {
            0..=2 => Value::Int(rng.next() as i64),
            3 => Value::Str("x".repeat((rng.next() % 300) as usize)),
            4 => Value::Bin((0..rng.next() % 300).map(|_| rng.next() as u8).collect()),
            5 => {
                // A top-level NaN has no encoding; keep the corpus encodable.
                let f = f64::from_bits(rng.next());
                if f.is_nan() {
                    Value::Int(0)
                } else {
                    Value::F64(f)
                }
            }
            6 | 7 => {
                let n = (rng.next() % 5 + 1) as usize;
                Value::List((0..n).map(|_| gen(rng, depth + 1)).collect())
            }
            8 => {
                let n = (rng.next() % 4 + 1) as usize;
                let mut m = new_map::<InternerKey, InnerValue>();
                for _ in 0..n {
                    let id = rng.next();
                    m.insert(InternerKey::new(id), gen(rng, depth + 1));
                }
                Value::Map(m)
            }
            _ => {
                let n = (rng.next() % 4 + 1) as usize;
                set_of(&(0..n).map(|_| gen(rng, depth + 1)).collect::<Vec<_>>())
            }
        }
    }
    let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
    let mut codes: Vec<Vec<u8>> = (0..300)
        .map(|_| encode_inner_value(&gen(&mut rng, 0)).unwrap())
        .collect();
    codes.sort();
    for pair in codes.windows(2) {
        let (a, b) = (&pair[0], &pair[1]);
        let proper_prefix = a.len() < b.len() && b[..a.len()] == a[..];
        assert!(!proper_prefix, "encoding is a proper prefix of another");
    }
}

#[test]
fn p34_12_codec_scalar_ref_inner_value_parity() {
    let cases: Vec<(ScalarRef<'static>, InnerValue)> = vec![
        (ScalarRef::Null, Value::Null),
        (ScalarRef::Bool(true), Value::Bool(true)),
        (ScalarRef::Bool(false), Value::Bool(false)),
        (ScalarRef::Int(42), Value::Int(42)),
        (ScalarRef::Int(i64::MIN), Value::Int(i64::MIN)),
        (ScalarRef::F64(2.5), Value::F64(2.5)),
        // Integral f64 shares the integer class through NumericKey.
        (ScalarRef::F64(3.0), Value::Int(3)),
        (ScalarRef::F64(0.0), Value::Int(0)),
        (ScalarRef::Str("hi"), Value::Str("hi".to_string())),
        (ScalarRef::Bin(b"\x00\x01"), Value::Bin(vec![0x00, 0x01])),
    ];
    for (s, v) in &cases {
        assert_eq!(
            encode_scalar_ref(s).unwrap(),
            encode_inner_value(v).unwrap()
        );
    }
    // NaN errors identically on both paths.
    assert_eq!(
        encode_scalar_ref(&ScalarRef::F64(f64::NAN)),
        Err(CodecError::Nan)
    );
}

#[test]
fn p34_12_identity_inline_digest_boundary() {
    // 251-char Str => |C| = 1 + 4 + 251 = 256 exactly: inline.
    let c256 = encode_inner_value(&Value::Str("a".repeat(251))).unwrap();
    assert_eq!(c256.len(), INLINE_MAX);
    let i256 = identity_bytes(&c256);
    assert_eq!(i256, c256);
    assert_ne!(i256[0], markers::DIGEST_MARKER);

    // 252-char Str => |C| = 257: digest form, 33 bytes starting 0xFF.
    let c257 = encode_inner_value(&Value::Str("a".repeat(252))).unwrap();
    assert_eq!(c257.len(), INLINE_MAX + 1);
    let i257 = identity_bytes(&c257);
    assert_eq!(i257.len(), DIGEST_LEN);
    assert_eq!(i257[0], markers::DIGEST_MARKER);
    // Digest golden: a literal from an independent single-chunk BLAKE3
    // derive_key computation (context "shamir-db index identity v2 posting
    // digest", input C = 06 00 00 00 fc followed by 252 x 0x61), not from
    // this crate's own code, so a context, marker or algorithm drift fails.
    assert_eq!(DIGEST_CONTEXT, "shamir-db index identity v2 posting digest");
    let mut expected = vec![markers::DIGEST_MARKER];
    expected.extend_from_slice(&[
        0x82, 0xb1, 0x4c, 0x8a, 0x55, 0xb9, 0x22, 0xdb, 0x94, 0x8e, 0xb3, 0x6e, 0xb5, 0x53, 0xe6,
        0x70, 0xab, 0x77, 0xbc, 0xfc, 0xe7, 0xc2, 0x59, 0x5c, 0xb6, 0x24, 0xc9, 0x99, 0x11, 0x2d,
        0x68, 0xe6,
    ]);
    assert_eq!(i257, expected);
    assert_eq!(identity_bytes(&c257), i257);
    assert_eq!(digest_identity(&c257)[..], i257[..]);

    // Inline and digest forms are disjoint: an inline C never starts 0xFF
    // (0xFF is not a leaf tag).
    assert_ne!(c257[0], markers::DIGEST_MARKER);

    // Unique-posting values: inline = rid 16 B only; digest = rid + C.
    let rid = RecordId::try_from_bytes(&[7u8; 16]).unwrap();
    let inline_val = encode_unique_posting_value(&rid, None);
    assert_eq!(inline_val.len(), 16);
    assert_eq!(unique_posting_owner(&inline_val), Some(rid));
    let digest_val = encode_unique_posting_value(&rid, Some(&c257));
    assert_eq!(digest_val.len(), 16 + c257.len());
    assert_eq!(unique_posting_owner(&digest_val), Some(rid));
    assert!(posting_tail_matches(&digest_val, &c257));
    assert_eq!(posting_value_tail(&digest_val), Some(c257.as_slice()));
    // No tail on the inline form; short values decode to no owner.
    assert_eq!(posting_value_tail(&inline_val), None);
    assert!(!posting_tail_matches(&inline_val, &c257));
    assert_eq!(unique_posting_owner(&inline_val[..8]), None);
    assert_eq!(unique_posting_owner(&[]), None);
}

#[test]
fn p34_12_codec_9e472725_witness_distinct() {
    // rustc-hash 2.1.2 64-bit multiplier (`const K: usize` in its
    // src/lib.rs): the v1 FxHasher collision family (0, 0) vs (1, -(K*K)).
    const K: u64 = 0xf135_7aea_2e62_a9c5;
    let kk = K.wrapping_mul(K) as i64;
    let mut a = encode_scalar_ref(&ScalarRef::Int(0)).unwrap();
    a.extend_from_slice(&encode_scalar_ref(&ScalarRef::Int(0)).unwrap());
    let mut b = encode_scalar_ref(&ScalarRef::Int(1)).unwrap();
    b.extend_from_slice(&encode_scalar_ref(&ScalarRef::Int(kk.wrapping_neg())).unwrap());
    assert_ne!(a, b, "witness values must encode to distinct bytes");
}

#[test]
fn p34_12_envelope_strict_open_rejects_v2() {
    let bytes = MetaEnvelope::new_with_version(2, "x".to_string())
        .encode()
        .unwrap();
    assert!(matches!(
        MetaEnvelope::<String>::open(&bytes),
        Err(MetaError::UnsupportedVersion(2))
    ));
}

#[test]
fn p34_12_envelope_open_accepting() {
    let env = MetaEnvelope::new_with_version(2, "payload".to_string());
    let bytes = env.encode().unwrap();
    let (version, payload) = MetaEnvelope::<String>::open_accepting(&bytes, &[1, 2]).unwrap();
    assert_eq!(version, 2);
    assert_eq!(payload, "payload");
    // Strict open is unchanged: a v2 blob is unsupported there.
    assert!(matches!(
        MetaEnvelope::<String>::open(&bytes),
        Err(MetaError::UnsupportedVersion(2))
    ));
    // Versions outside the accepting set are refused.
    assert!(matches!(
        MetaEnvelope::<String>::open_accepting(&bytes, &[1]),
        Err(MetaError::UnsupportedVersion(2))
    ));
    // v1 round-trip still works through both paths.
    let v1 = MetaEnvelope::new("old".to_string());
    let v1_bytes = v1.encode().unwrap();
    let (version, payload) = MetaEnvelope::<String>::open_accepting(&v1_bytes, &[1, 2]).unwrap();
    assert_eq!((version, payload), (1, "old".to_string()));
    assert_eq!(MetaEnvelope::<String>::open(&v1_bytes).unwrap(), "old");
}
