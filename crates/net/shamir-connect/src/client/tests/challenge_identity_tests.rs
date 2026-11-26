//! Client-side challenge identity and rotation-chain verification oracles.

use crate::client::handshake::{HandshakeBuilder, ServerChallenge, ServerChallengeV2};
use crate::common::crypto::{sha256, Ed25519Keypair};
use crate::common::identity::{build_challenge_identity_input, sign_identity};
use crate::common::kdf_params::KdfParams;
use crate::common::rotation::build_rotation_proof_input;
use crate::common::types::{BindingMode, TransportKind};
use crate::common::username::NormalizedUsername;
use crate::common::wire::ChallengeRotationPayload;

const NOW: u64 = 1_000_000_000;

fn client(pin: [u8; 32]) -> crate::client::handshake::ClientHandshake {
    HandshakeBuilder::new(
        NormalizedUsername::from_raw("alice").unwrap(),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
    )
    .pinned_hash(pin)
    .build()
    .unwrap()
}

fn challenge(
    client: &crate::client::handshake::ClientHandshake,
    signer: &Ed25519Keypair,
    chain: Option<ChallengeRotationPayload>,
) -> ServerChallengeV2 {
    let salt = [0x55; 16];
    let server_nonce = [0x66; 32];
    let input = build_challenge_identity_input(
        &signer.public_bytes(),
        client.client_nonce(),
        &server_nonce,
        &salt,
        KdfParams::DEFAULT.memory_kb,
        KdfParams::DEFAULT.time,
        KdfParams::DEFAULT.parallelism,
        KdfParams::DEFAULT.argon2_version,
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        &[0; 32],
    );
    ServerChallengeV2 {
        challenge: ServerChallenge {
            salt,
            kdf_params: KdfParams::DEFAULT,
            server_nonce,
        },
        server_pub_key: Some(signer.public_bytes()),
        challenge_sig: Some(sign_identity(signer, &input)),
        challenge_rotation: chain,
    }
}

fn tofu_client() -> crate::client::handshake::ClientHandshake {
    HandshakeBuilder::new(
        NormalizedUsername::from_raw("alice").unwrap(),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
    )
    .accept_new_host(true)
    .build()
    .unwrap()
}

#[test]
fn pinless_tofu_accepts_valid_challenge_and_returns_server_key() {
    // Discriminates against requiring a pre-existing pin for signature verification/TOFU key capture.
    let key = Ed25519Keypair::from_seed(&[7; 32]);
    let c = tofu_client();
    let ch = challenge(&c, &key, None);
    assert_eq!(
        c.verify_challenge_identity(&ch, NOW).unwrap(),
        key.public_bytes()
    );
}

#[test]
fn pinless_tofu_still_rejects_corrupted_signature() {
    // Discriminates against bypassing signature verification when there is no pin.
    let key = Ed25519Keypair::from_seed(&[8; 32]);
    let c = tofu_client();
    let mut ch = challenge(&c, &key, None);
    ch.challenge_sig.as_mut().unwrap()[0] ^= 1;
    assert!(matches!(
        c.verify_challenge_identity(&ch, NOW),
        Err(crate::common::Error::ServerIdentityChanged)
    ));
}

#[test]
fn pinned_client_rejects_challenge_with_no_identity_fields() {
    // Discriminates against treating an identity-less v1 challenge as acceptable to a pinned v2 verifier.
    let key = Ed25519Keypair::from_seed(&[9; 32]);
    let c = client(sha256(&key.public_bytes()));
    let ch = ServerChallengeV2 {
        challenge: ServerChallenge {
            salt: [0x55; 16],
            kdf_params: KdfParams::DEFAULT,
            server_nonce: [0x66; 32],
        },
        server_pub_key: None,
        challenge_sig: None,
        challenge_rotation: None,
    };
    assert!(matches!(
        c.verify_challenge_identity(&ch, NOW),
        Err(crate::common::Error::ServerIdentityChanged)
    ));
}

#[test]
fn valid_foreign_signer_without_chain_is_rejected() {
    // Discriminates against implementations that verify signatures but omit pin comparison.
    let pin_key = Ed25519Keypair::from_seed(&[1; 32]);
    let other = Ed25519Keypair::from_seed(&[2; 32]);
    let c = client(sha256(&pin_key.public_bytes()));
    let server_challenge = challenge(&c, &other, None);
    assert!(matches!(
        c.verify_challenge_identity(&server_challenge, NOW),
        Err(crate::common::Error::ServerIdentityChanged)
    ));
}

#[test]
fn correct_key_with_corrupted_challenge_signature_is_rejected() {
    // Discriminates against implementations that accept a matching pin without checking its transcript signature.
    let key = Ed25519Keypair::from_seed(&[3; 32]);
    let c = client(sha256(&key.public_bytes()));
    let mut ch = challenge(&c, &key, None);
    ch.challenge_sig.as_mut().unwrap()[0] ^= 1;
    assert!(matches!(
        c.verify_challenge_identity(&ch, NOW),
        Err(crate::common::Error::ServerIdentityChanged)
    ));
}

fn rotation_case(
    signer_seed: [u8; 32],
    chain_seed: [u8; 32],
    pin_seed: [u8; 32],
    deadline: u64,
) -> crate::common::Result<[u8; 32]> {
    let current = Ed25519Keypair::from_seed(&signer_seed);
    let previous = Ed25519Keypair::from_seed(&chain_seed);
    let c = client(sha256(&Ed25519Keypair::from_seed(&pin_seed).public_bytes()));
    let input =
        build_rotation_proof_input(&previous.public_bytes(), &current.public_bytes(), deadline);
    let chain = ChallengeRotationPayload {
        previous_pub: previous.public_bytes(),
        transition_until_ns: deadline,
        rotation_proof: previous.sign(&input),
    };
    let ch = challenge(&c, &current, Some(chain));
    c.verify_challenge_identity(&ch, NOW)
}

#[test]
fn pinned_previous_key_accepts_valid_chain_inside_window() {
    // Discriminates against dropping the authorized-rotation acceptance branch.
    assert!(rotation_case([5; 32], [4; 32], [4; 32], NOW + 10).is_ok());
}

#[test]
fn expired_rotation_deadline_is_rejected() {
    // Discriminates against omitting the lower deadline bound.
    assert!(rotation_case([5; 32], [4; 32], [4; 32], NOW).is_err());
}

#[test]
fn excessive_rotation_deadline_is_rejected() {
    // Discriminates against omitting the maximum-overlap deadline bound.
    assert!(rotation_case(
        [5; 32],
        [4; 32],
        [4; 32],
        NOW + crate::client::rotation::ROTATION_MAX_TRANSITION_NS + 1
    )
    .is_err());
}

#[test]
fn chain_signed_by_non_pinned_key_is_rejected() {
    // Discriminates against verifying the proof under an unrelated key or skipping previous-key pin matching.
    assert!(rotation_case([5; 32], [6; 32], [4; 32], NOW + 10).is_err());
}
