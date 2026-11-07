use super::{scalar_from_query, scalar_to_query};
use shamir_types::types::common::new_map_wc;
use shamir_types::types::value::{InnerValue, QueryValue};
use std::cell::OnceCell;

#[test]
fn scalar_conversion_initializes_scratch_only_for_map_keys() {
    let scratch = OnceCell::new();
    for value in [
        InnerValue::Int(42),
        InnerValue::List(vec![InnerValue::Str("value".into()), InnerValue::Int(7)]),
    ] {
        let query = scalar_to_query(&value, &scratch).unwrap();
        assert!(scratch.get().is_none());
        assert_eq!(scalar_from_query(&query, &scratch).unwrap(), value);
        assert!(scratch.get().is_none());
    }
    let mut map = new_map_wc(1);
    map.insert("answer".to_string(), QueryValue::Int(42));
    let returned = QueryValue::Map(map);
    let converted = scalar_from_query(&returned, &scratch).unwrap();
    assert!(scratch.get().is_some());
    assert_eq!(scalar_to_query(&converted, &scratch).unwrap(), returned);
    let independent = OnceCell::new();
    assert!(independent.get().is_none());
    assert!(scalar_to_query(&converted, &independent).is_err());
    assert!(independent.get().is_some());
}
