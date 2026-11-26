use crate::common::wire::ChallengeRotationWire;
use crate::common::Error;

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
    // Discriminates against coercing a signed negative integer to a u64 deadline.
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
    // Discriminates against accepting incomplete positional payloads.
    let bytes = rmp_serde::to_vec(&(serde_bytes::Bytes::new(&[1u8; 32]), 7u64)).unwrap();
    assert!(matches!(
        ChallengeRotationWire::decode(&bytes),
        Err(Error::ChallengeLayout)
    ));
}

#[test]
fn challenge_rotation_rejects_long_array() {
    // Discriminates against ignoring trailing positional fields.
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
    // Discriminates against MessagePack arrays of integers masquerading as bin.
    let bytes = rmp_serde::to_vec(&([1u8; 32], 7u64, serde_bytes::Bytes::new(&[2u8; 64]))).unwrap();
    assert!(matches!(
        ChallengeRotationWire::decode(&bytes),
        Err(Error::ChallengeLayout)
    ));
}

#[test]
fn challenge_rotation_rejects_wrong_key_length() {
    // Discriminates against accepting a 31-byte public key.
    let bytes = encoded_parts(&[1u8; 31], 7, &[2u8; 64]);
    assert!(matches!(
        ChallengeRotationWire::decode(&bytes)
            .unwrap()
            .into_payload(),
        Err(Error::ChallengeLayout)
    ));
}

#[test]
fn challenge_rotation_rejects_wrong_proof_length() {
    // Discriminates against accepting a 63-byte signature.
    let bytes = encoded_parts(&[1u8; 32], 7, &[2u8; 63]);
    assert!(matches!(
        ChallengeRotationWire::decode(&bytes)
            .unwrap()
            .into_payload(),
        Err(Error::ChallengeLayout)
    ));
}
