use crate::common::wire::ChallengeRotationWire;
use crate::common::Error;

#[derive(serde::Deserialize)]
struct LayoutVectorFile {
    rows: Vec<LayoutVectorRow>,
}

#[derive(serde::Deserialize, PartialEq, Debug)]
struct LayoutVectorRow {
    name: String,
    expected: String,
    hex: String,
}

#[test]
fn challenge_layout_v2_json_and_toml_vectors_agree_and_decode() {
    // Discriminates against marker-layout acceptance/rejection drift and divergent vector formats.
    let json_text = include_str!("../../../../test-vectors/challenge_layout_v2.json");
    let mut json_rows = Vec::new();
    let mut name = None;
    let mut expected = None;
    for line in json_text.lines() {
        let line = line.trim();
        if let Some(value) = line.strip_prefix("\"name\": \"") {
            name = Some(json_string_value(value));
        } else if let Some(value) = line.strip_prefix("\"expected\": \"") {
            expected = Some(json_string_value(value));
        } else if let Some(value) = line.strip_prefix("\"hex\": \"") {
            json_rows.push(LayoutVectorRow {
                name: name.take().unwrap(),
                expected: expected.take().unwrap(),
                hex: json_string_value(value),
            });
        }
    }
    let toml: LayoutVectorFile = toml::from_str(include_str!(
        "../../../../test-vectors/challenge_layout_v2.toml"
    ))
    .unwrap();
    assert_eq!(json_rows, toml.rows);
    for row in json_rows {
        let bytes = hex::decode(&row.hex).unwrap();
        let result = ChallengeRotationWire::decode(&bytes);
        match row.expected.as_str() {
            "accept" => assert!(result.is_ok(), "{}", row.name),
            "reject" => assert!(
                matches!(result, Err(Error::ChallengeLayout)),
                "{}",
                row.name
            ),
            other => panic!("unknown vector outcome: {other}"),
        }
    }
}

/// The caller already stripped the opening quote with the key prefix.
fn json_string_value(value: &str) -> String {
    value
        .trim_end_matches(',')
        .strip_suffix('"')
        .expect("a json string value ends with a quote")
        .to_string()
}

fn encoded_parts(previous: &[u8], deadline: u64, proof: &[u8]) -> Vec<u8> {
    rmp_serde::to_vec(&(
        serde_bytes::Bytes::new(previous),
        deadline,
        serde_bytes::Bytes::new(proof),
    ))
    .unwrap()
}

#[test]
fn challenge_rotation_valid_binary_fields_round_trip() {
    // Discriminates against changing the positional schema or losing u64 precision.
    let previous = [0x11; 32];
    let proof = [0x22; 64];
    let deadline = u64::MAX - 7;
    let wire = ChallengeRotationWire {
        previous_pub: previous.to_vec(),
        transition_until_ns: deadline,
        rotation_proof: proof.to_vec(),
    };
    let bytes = wire.encode().unwrap();
    let expected = format!(
        "93c420{}cffffffffffffffff8c440{}",
        "11".repeat(32),
        "22".repeat(64)
    );
    assert_eq!(hex::encode(&bytes), expected);
    let parsed = ChallengeRotationWire::decode(&bytes)
        .unwrap()
        .into_payload()
        .unwrap();
    assert_eq!(parsed.previous_pub, previous);
    assert_eq!(parsed.transition_until_ns, deadline);
    assert_eq!(parsed.rotation_proof, proof);
}

#[test]
fn challenge_rotation_rejects_binary_deadline() {
    // Discriminates against accepting a byte-string deadline in place of an unsigned integer.
    let bytes = rmp_serde::to_vec(&(
        serde_bytes::Bytes::new(&[1u8; 32]),
        serde_bytes::Bytes::new(&[7u8; 8]),
        serde_bytes::Bytes::new(&[2u8; 64]),
    ))
    .unwrap();
    assert!(matches!(
        ChallengeRotationWire::decode(&bytes),
        Err(Error::ChallengeLayout)
    ));
}

#[test]
fn challenge_rotation_rejects_negative_deadline() {
    // Discriminates against coercing a signed negative deadline to u64.
    let bytes = rmp_serde::to_vec(&(
        serde_bytes::Bytes::new(&[1u8; 32]),
        -1i64,
        serde_bytes::Bytes::new(&[2u8; 64]),
    ))
    .unwrap();
    assert!(matches!(
        ChallengeRotationWire::decode(&bytes),
        Err(Error::ChallengeLayout)
    ));
}

#[test]
fn challenge_rotation_rejects_short_array() {
    // Discriminates against accepting an incomplete positional payload.
    let bytes = rmp_serde::to_vec(&(serde_bytes::Bytes::new(&[1u8; 32]), 7u64)).unwrap();
    assert!(matches!(
        ChallengeRotationWire::decode(&bytes),
        Err(Error::ChallengeLayout)
    ));
}

#[test]
fn challenge_rotation_rejects_long_array() {
    // Discriminates against ignoring an extra positional field.
    let bytes = rmp_serde::to_vec(&(
        serde_bytes::Bytes::new(&[1u8; 32]),
        7u64,
        serde_bytes::Bytes::new(&[2u8; 64]),
        9u8,
    ))
    .unwrap();
    assert!(matches!(
        ChallengeRotationWire::decode(&bytes),
        Err(Error::ChallengeLayout)
    ));
}

#[test]
fn challenge_rotation_rejects_array_instead_of_binary_key() {
    // Discriminates against accepting MessagePack arrays of integers masquerading as bin.
    let bytes = rmp_serde::to_vec(&([1u8; 32], 7u64, serde_bytes::Bytes::new(&[2u8; 64]))).unwrap();
    assert!(matches!(
        ChallengeRotationWire::decode(&bytes),
        Err(Error::ChallengeLayout)
    ));
}

#[test]
fn challenge_rotation_rejects_wrong_key_length_at_decode() {
    assert!(matches!(
        ChallengeRotationWire::decode(&encoded_parts(&[1u8; 31], 7, &[2u8; 64])),
        Err(Error::ChallengeLayout)
    ));
}

#[test]
fn challenge_rotation_rejects_wrong_proof_length_at_decode() {
    assert!(matches!(
        ChallengeRotationWire::decode(&encoded_parts(&[1u8; 32], 7, &[2u8; 63])),
        Err(Error::ChallengeLayout)
    ));
}
