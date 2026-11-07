//! P34-12 codec golden tests — literal-hex assertions of the frozen IDV2
//! canonical format (design section (a) example vectors). They must hold on
//! every target: no `target_pointer_width` gate.

use crate::model::identity::canonical::{encode_inner_value, encode_scalar_ref, CodecError};
use shamir_types::core::interner::InternerKey;
use shamir_types::types::common::new_map;
use shamir_types::types::value::{InnerValue, Value};

fn map_of(entries: &[(u64, InnerValue)]) -> InnerValue {
    let mut m = new_map::<InternerKey, InnerValue>();
    for (id, v) in entries {
        m.insert(InternerKey::new(*id), v.clone());
    }
    Value::Map(m)
}

#[test]
fn p34_12_codec_golden_vectors() {
    // Int(1) == F64(1.0) == Dec(1) == Big(1): 02 || i64 BE(1).
    let int_golden = [0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01];
    assert_eq!(
        encode_inner_value(&Value::Int(1)).unwrap(),
        int_golden.to_vec()
    );
    assert_eq!(
        encode_inner_value(&Value::F64(1.0)).unwrap(),
        int_golden.to_vec()
    );
    assert_eq!(
        encode_inner_value(&Value::Dec(rust_decimal::Decimal::from(1))).unwrap(),
        int_golden.to_vec()
    );
    assert_eq!(
        encode_inner_value(&Value::Big(num_bigint::BigInt::from(1))).unwrap(),
        int_golden.to_vec()
    );

    // Str("run"): 06 || u32 BE(3) || "run".
    assert_eq!(
        encode_inner_value(&Value::Str("run".to_string())).unwrap(),
        vec![0x06, 0x00, 0x00, 0x00, 0x03, 0x72, 0x75, 0x6E]
    );

    // Composite (Int 0, Int 0): concatenation of leaf codes, 18 bytes
    // (`02 00x8 02 00x8`), no container tag.
    let mut composite = Vec::new();
    composite.extend_from_slice(
        &encode_scalar_ref(&shamir_types::record_view::ScalarRef::Int(0)).unwrap(),
    );
    composite.extend_from_slice(
        &encode_scalar_ref(&shamir_types::record_view::ScalarRef::Int(0)).unwrap(),
    );
    assert_eq!(composite.len(), 18);
    assert_eq!(
        composite,
        vec![0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0x02, 0, 0, 0, 0, 0, 0, 0, 0]
    );

    // Map{k5: 1, k7: 2}: 0A || u32 BE(2) || sorted entries by key id.
    let map_golden: Vec<u8> = [
        0x0A, 0x00, 0x00, 0x00, 0x02, // tag + count
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // key id 5 BE
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // value Int(1)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // key id 7 BE
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // value Int(2)
    ]
    .to_vec();
    let got = encode_inner_value(&map_of(&[(5, Value::Int(1)), (7, Value::Int(2))])).unwrap();
    assert_eq!(got, map_golden);

    // Top-level NaN has no identity.
    assert_eq!(
        encode_inner_value(&Value::F64(f64::NAN)),
        Err(CodecError::Nan)
    );

    // A NaN nested in a List keeps its raw bits (class 0x04); only a
    // top-level NaN is refused.
    let nested_nan = Value::List(vec![Value::F64(f64::from_bits(0x7ff8_0000_0000_0000))]);
    assert_eq!(
        encode_inner_value(&nested_nan).unwrap(),
        vec![
            0x08, 0x00, 0x00, 0x00, 0x01, // List, count 1
            0x04, 0x7f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // F64Bits(NaN)
        ]
    );
}
