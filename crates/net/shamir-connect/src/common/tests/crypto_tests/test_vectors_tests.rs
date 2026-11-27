//! Fixed, cross-language test-vector suite (spec §16).
//!
//! Each test here loads a pinned vector from `test-vectors/*.toml`, feeds the
//! real production function the SAME fixed inputs, and asserts BYTE-FOR-BYTE
//! equality against the pinned `expected` hex. Unlike the round-trip tests in
//! `scram_tests.rs` / `fake_blob_tests.rs` / `identity_tests.rs` (which prove
//! Rust-to-Rust internal consistency with random inputs), these pin the exact
//! output bytes — so a domain-tag reorder, an HKDF info-string change, or any
//! drift in the composite constructions fails LOUDLY, and a second
//! implementation (browser/TS SDK) has concrete bytes to check against.
//!
//! Most `expected` values were captured by running the real function once;
//! the change-password and bootstrap vectors below are independently assembled.
//!
//! Coherent fixed scenario: all vectors share the same username="alice",
//! nonces, salt, kdf_params=DEFAULT, transport/binding/exporter as
//! `auth_message_default.json`, so they chain into one end-to-end story
//! (auth_message → Argon2id → SCRAM proofs → identity_sig → …).

use crate::common::auth_message::{AuthMessage, AuthMessageInputs};
use crate::common::bootstrap_message::build_bootstrap_input;
use crate::common::changepw::{build_auth_message_cp, ChangePwAuthMessageInputs};
#[cfg(feature = "server")]
use crate::common::crypto::{aes256gcm_decrypt, aes256gcm_encrypt};
use crate::common::crypto::{argon2id, ed25519_verify_strict, Ed25519Keypair};
#[cfg(feature = "server")]
use crate::common::domain_tags::TICKET_V1;
use crate::common::fake_blob::FakeBlob;
use crate::common::identity::{
    build_challenge_identity_input, build_identity_input, sign_identity, ChallengeIdentityTransport,
};
use crate::common::kdf_params::KdfParams;
use crate::common::rotation::build_rotate_event_input;
use crate::common::scram::{build_client_proof, build_server_signature, DerivedKeys};
use crate::common::types::{BindingMode, ProtocolVersion, TransportKind};
use crate::common::username::NormalizedUsername;
#[cfg(feature = "server")]
use crate::server::ticket::TicketPlain;

// Independent layouts assembled from the documented field widths; bootstrap's
// SHA-256 was computed with Python hashlib, not the production builder.
#[derive(serde::Deserialize)]
struct ChangePwVector {
    username: String,
    session_id_hex: String,
    client_nonce_hex: String,
    server_nonce_hex: String,
    salt_hex: String,
    channel_binding_hex: String,
    transport_kind: u8,
    binding_mode: u8,
    memory_kb: u32,
    time: u32,
    parallelism: u32,
    argon2_version: u8,
    expected_hex: String,
}

#[test]
fn change_password_complete_byte_vector() {
    let v: ChangePwVector = toml::from_str(include_str!(
        "../../../../test-vectors/changepw_message.toml"
    ))
    .unwrap();
    let user = NormalizedUsername::from_raw(&v.username).unwrap();
    let actual = build_auth_message_cp(ChangePwAuthMessageInputs {
        username: &user,
        session_id: &hex_to_array(&v.session_id_hex),
        client_nonce_cp: &hex_to_array(&v.client_nonce_hex),
        server_nonce_cp: &hex_to_array(&v.server_nonce_hex),
        salt: &hex_to_array(&v.salt_hex),
        kdf_params: KdfParams {
            memory_kb: v.memory_kb,
            time: v.time,
            parallelism: v.parallelism,
            argon2_version: v.argon2_version,
        },
        transport_kind: TransportKind::from_u8(v.transport_kind).unwrap(),
        binding_mode: BindingMode::from_u8(v.binding_mode).unwrap(),
        channel_binding_at_auth: &hex_to_array(&v.channel_binding_hex),
    })
    .unwrap();
    assert_eq!(hex::encode(actual), v.expected_hex);
}

#[derive(serde::Deserialize)]
struct BootstrapVector {
    server_pub_key_hex: String,
    transport_kind: u8,
    tls_exporter_hex: String,
    client_nonce_hex: String,
    server_time_ns: u64,
    expected_hex: String,
}

#[test]
fn bootstrap_complete_byte_vector() {
    let v: BootstrapVector = toml::from_str(include_str!(
        "../../../../test-vectors/bootstrap_message.toml"
    ))
    .unwrap();
    let actual = build_bootstrap_input(
        &hex_to_array(&v.server_pub_key_hex),
        TransportKind::from_u8(v.transport_kind).unwrap(),
        &hex_to_array(&v.tls_exporter_hex),
        &hex_to_array(&v.client_nonce_hex),
        v.server_time_ns,
    );
    assert_eq!(hex::encode(actual), v.expected_hex);
}

// --- shared deserialization helpers ---

const KDF_CANON_TOML: &str =
    include_str!("../../../../test-vectors/kdf_canonical_string_default.toml");
const ARGON2_TOML: &str = include_str!("../../../../test-vectors/argon2id_default.toml");
const SCRAM_TOML: &str = include_str!("../../../../test-vectors/scram_flow_default.toml");
const IDENTITY_TOML: &str = include_str!("../../../../test-vectors/identity_sig_default.toml");
const FAKE_BLOB_TOML: &str = include_str!("../../../../test-vectors/fake_blob_default.toml");
#[cfg(feature = "server")]
const TICKET_TOML: &str = include_str!("../../../../test-vectors/resumption_ticket_roundtrip.toml");
const ROTATION_TOML: &str =
    include_str!("../../../../test-vectors/identity_rotation_signed_by_old.toml");
const AUTH_MSG_TOML: &str = include_str!("../../../../test-vectors/auth_message_default.toml");
const CHALLENGE_ID_TOML: &str = include_str!("../../../../test-vectors/challenge_identity_v2.toml");

#[derive(serde::Deserialize)]
struct VecKdfParams {
    memory_kb: u32,
    time: u32,
    parallelism: u32,
    argon2_version: u8,
}

impl From<VecKdfParams> for KdfParams {
    fn from(v: VecKdfParams) -> Self {
        KdfParams {
            memory_kb: v.memory_kb,
            time: v.time,
            parallelism: v.parallelism,
            argon2_version: v.argon2_version,
        }
    }
}

/// Fields needed to rebuild the shared fixed `AuthMessage` scenario.
#[derive(serde::Deserialize)]
struct AuthMsgInputs {
    username: String,
    client_nonce_hex: String,
    server_nonce_hex: String,
    salt_hex: String,
    kdf_params: VecKdfParams,
    transport_kind: u8,
    binding_mode: u8,
    tls_exporter_hex: String,
    supported_version: u8,
}

#[derive(serde::Deserialize)]
struct AuthMsgVector {
    inputs: AuthMsgInputs,
}

fn hex_to_vec(s: &str) -> Vec<u8> {
    hex::decode(s).expect("valid hex in vector")
}

fn hex_to_array<const N: usize>(s: &str) -> [u8; N] {
    let v = hex_to_vec(s);
    assert_eq!(v.len(), N, "hex length mismatch");
    let mut a = [0u8; N];
    a.copy_from_slice(&v);
    a
}

/// Rebuild the shared fixed `AuthMessage` from `auth_message_default.toml`.
/// Used by the SCRAM + identity tests so their downstream signatures are
/// derived over the EXACT same auth_message bytes the spec §16 example pins.
fn rebuild_default_auth_message() -> AuthMessage {
    let v: AuthMsgVector = toml::from_str(AUTH_MSG_TOML).unwrap();
    let username = NormalizedUsername::from_raw(&v.inputs.username).unwrap();
    let client_nonce = hex_to_array::<32>(&v.inputs.client_nonce_hex);
    let server_nonce = hex_to_array::<32>(&v.inputs.server_nonce_hex);
    let salt = hex_to_array::<16>(&v.inputs.salt_hex);
    let tls_exporter = hex_to_array::<32>(&v.inputs.tls_exporter_hex);
    AuthMessage::build(AuthMessageInputs {
        username: &username,
        client_nonce: &client_nonce,
        server_nonce: &server_nonce,
        salt: &salt,
        kdf_params: v.inputs.kdf_params.into(),
        transport_kind: TransportKind::from_u8(v.inputs.transport_kind).unwrap(),
        binding_mode: BindingMode::from_u8(v.inputs.binding_mode).unwrap(),
        tls_exporter_or_zeros: &tls_exporter,
        supported_version: ProtocolVersion(v.inputs.supported_version),
    })
    .unwrap()
}

// ===========================================================================
// 1. kdf_canonical_string
// ===========================================================================

#[derive(serde::Deserialize)]
struct KdfCanonVector {
    inputs: KdfCanonInputs,
    expected: KdfCanonExpected,
}
#[derive(serde::Deserialize)]
struct KdfCanonInputs {
    memory_kb: u32,
    time: u32,
    parallelism: u32,
    argon2_version: u8,
}
#[derive(serde::Deserialize)]
struct KdfCanonExpected {
    total_bytes: usize,
    canonical_string_hex: String,
}

#[test]
fn kdf_canonical_string_matches_vector() {
    let v: KdfCanonVector = toml::from_str(KDF_CANON_TOML).unwrap();
    // Serialize the 4 fields exactly as auth_message.rs embeds them:
    // u32_be(memory_kb) || u32_be(time) || u32_be(parallelism) || u8(version).
    let mut canon = Vec::with_capacity(13);
    canon.extend_from_slice(&v.inputs.memory_kb.to_be_bytes());
    canon.extend_from_slice(&v.inputs.time.to_be_bytes());
    canon.extend_from_slice(&v.inputs.parallelism.to_be_bytes());
    canon.push(v.inputs.argon2_version);

    assert_eq!(canon.len(), v.expected.total_bytes);
    assert_eq!(
        hex::encode(&canon),
        v.expected.canonical_string_hex,
        "kdf_canonical_string drift: a cross-language impl would mis-encode KdfParams"
    );
}

// ===========================================================================
// 2. argon2id_default
// ===========================================================================

#[derive(serde::Deserialize)]
struct Argon2Vector {
    inputs: Argon2Inputs,
    expected: Argon2Expected,
}
#[derive(serde::Deserialize)]
struct Argon2Inputs {
    password: String,
    salt_hex: String,
    kdf_params: VecKdfParams,
}
#[derive(serde::Deserialize)]
struct Argon2Expected {
    salted_password_hex: String,
}

#[test]
fn argon2id_default_matches_vector() {
    let v: Argon2Vector = toml::from_str(ARGON2_TOML).unwrap();
    let salt = hex_to_vec(&v.inputs.salt_hex);
    let kdf: KdfParams = v.inputs.kdf_params.into();

    let salted = argon2id(v.inputs.password.as_bytes(), &salt, &kdf).unwrap();

    assert_eq!(
        hex::encode(&salted[..]),
        v.expected.salted_password_hex,
        "Argon2id output drift: browser SDK and Rust would disagree on salted_password"
    );
}

// ===========================================================================
// 3. scram_flow_default (chained: derive → client_proof → server_signature)
// ===========================================================================

#[derive(serde::Deserialize)]
struct ScramVector {
    inputs: ScramInputs,
    expected: ScramExpected,
}
#[derive(serde::Deserialize)]
struct ScramInputs {
    password: String,
    salt_hex: String,
    kdf_params: VecKdfParams,
    auth_message_hex: String,
}
#[derive(serde::Deserialize)]
struct ScramExpected {
    salted_password_hex: String,
    client_key_hex: String,
    server_key_hex: String,
    stored_key_hex: String,
    client_proof_hex: String,
    server_signature_hex: String,
}

#[test]
fn scram_flow_matches_vector() {
    let v: ScramVector = toml::from_str(SCRAM_TOML).unwrap();
    let salt = hex_to_vec(&v.inputs.salt_hex);
    let kdf: KdfParams = v.inputs.kdf_params.into();

    // Rebuild the shared auth_message and prove it equals the pinned hex.
    let am = rebuild_default_auth_message();
    assert_eq!(
        hex::encode(am.as_bytes()),
        v.inputs.auth_message_hex,
        "rebuilt auth_message does not match scram_flow vector's pinned auth_message_hex"
    );

    let derived = DerivedKeys::derive(v.inputs.password.as_bytes(), &salt, &kdf).unwrap();
    let client_proof = build_client_proof(&derived.client_key, &derived.stored_key, &am);
    let server_signature = build_server_signature(&derived.server_key, &am);

    assert_eq!(
        hex::encode(&derived.salted_password[..]),
        v.expected.salted_password_hex
    );
    assert_eq!(
        hex::encode(&derived.client_key[..]),
        v.expected.client_key_hex
    );
    assert_eq!(
        hex::encode(&derived.server_key[..]),
        v.expected.server_key_hex
    );
    assert_eq!(hex::encode(derived.stored_key.0), v.expected.stored_key_hex);
    assert_eq!(
        hex::encode(client_proof),
        v.expected.client_proof_hex,
        "client_proof drift: a domain-tag or HMAC-info change would break interop"
    );
    assert_eq!(
        hex::encode(server_signature),
        v.expected.server_signature_hex,
        "server_signature drift"
    );
}

// ===========================================================================
// 4. identity_sig_default
// ===========================================================================

#[derive(serde::Deserialize)]
struct IdentityVector {
    inputs: IdentityInputs,
    expected: IdentityExpected,
}
#[derive(serde::Deserialize)]
struct IdentityInputs {
    ed25519_seed_hex: String,
    transport_kind: u8,
    binding_mode: u8,
    tls_exporter_hex: String,
    auth_message_hex: String,
    session_id_hex: String,
    expires_at_ns: i64,
}
#[derive(serde::Deserialize)]
struct IdentityExpected {
    server_pub_key_hex: String,
    identity_input_hex: String,
    identity_sig_hex: String,
}

#[test]
fn identity_sig_matches_vector() {
    let v: IdentityVector = toml::from_str(IDENTITY_TOML).unwrap();
    let seed = hex_to_array::<32>(&v.inputs.ed25519_seed_hex);
    let kp = Ed25519Keypair::from_seed(&seed);
    let tls_exporter = hex_to_array::<32>(&v.inputs.tls_exporter_hex);
    let session_id = hex_to_array::<32>(&v.inputs.session_id_hex);

    let am = rebuild_default_auth_message();
    assert_eq!(
        hex::encode(am.as_bytes()),
        v.inputs.auth_message_hex,
        "rebuilt auth_message does not match identity_sig vector"
    );

    let identity_input = build_identity_input(
        &kp.public_bytes(),
        TransportKind::from_u8(v.inputs.transport_kind).unwrap(),
        BindingMode::from_u8(v.inputs.binding_mode).unwrap(),
        &tls_exporter,
        &am,
        &session_id,
        v.inputs.expires_at_ns as u64,
    );
    let sig = sign_identity(&kp, &identity_input);

    assert_eq!(
        hex::encode(kp.public_bytes()),
        v.expected.server_pub_key_hex,
        "Ed25519 pubkey drift for fixed seed"
    );
    assert_eq!(
        hex::encode(&identity_input),
        v.expected.identity_input_hex,
        "identity_input byte layout drift"
    );
    assert_eq!(
        hex::encode(sig),
        v.expected.identity_sig_hex,
        "identity_sig drift: browser SDK and Rust would disagree on server signature"
    );
    // Defense-in-depth: the pinned signature must actually verify.
    assert!(ed25519_verify_strict(
        &kp.public_bytes(),
        &identity_input,
        &sig
    ));
}

// ===========================================================================
// 5. fake_blob_default
// ===========================================================================

#[derive(serde::Deserialize)]
struct FakeBlobVector {
    inputs: FakeBlobInputs,
    expected: FakeBlobExpected,
}
#[derive(serde::Deserialize)]
struct FakeBlobInputs {
    server_secret_hex: String,
    username: String,
}
#[derive(serde::Deserialize)]
struct FakeBlobExpected {
    fake_blob_hex: String,
    fake_blob_total_bytes: usize,
    fake_salt_hex: String,
    fake_stored_key_hex: String,
    fake_server_key_hex: String,
}

#[test]
fn fake_blob_matches_vector() {
    let v: FakeBlobVector = toml::from_str(FAKE_BLOB_TOML).unwrap();
    let secret = hex_to_array::<32>(&v.inputs.server_secret_hex);
    let username = NormalizedUsername::from_raw(&v.inputs.username).unwrap();

    let blob = FakeBlob::derive(&secret, &username).unwrap();

    let mut full = Vec::with_capacity(80);
    full.extend_from_slice(&blob.salt);
    full.extend_from_slice(&blob.stored_key.0);
    full.extend_from_slice(&blob.server_key[..]);

    assert_eq!(full.len(), v.expected.fake_blob_total_bytes);
    assert_eq!(
        hex::encode(&full),
        v.expected.fake_blob_hex,
        "fake_blob drift: HKDF salt/info-string change would break anti-enumeration interop"
    );
    assert_eq!(hex::encode(blob.salt), v.expected.fake_salt_hex);
    assert_eq!(
        hex::encode(blob.stored_key.0),
        v.expected.fake_stored_key_hex
    );
    assert_eq!(
        hex::encode(&blob.server_key[..]),
        v.expected.fake_server_key_hex
    );
}

#[derive(serde::Deserialize)]
struct ChallengeIdentityVector {
    inputs: ChallengeIdentityInputs,
    expected: ChallengeIdentityExpected,
}
#[derive(serde::Deserialize)]
struct ChallengeIdentityInputs {
    ed25519_seed_hex: String,
    client_nonce_hex: String,
    server_nonce_hex: String,
    salt_hex: String,
    memory_kb: u32,
    time: u32,
    parallelism: u32,
    argon2_version: u8,
    transport_kind: u8,
    binding_mode: u8,
    tls_exporter_hex: String,
}
#[derive(serde::Deserialize)]
struct ChallengeIdentityExpected {
    server_pub_key_hex: String,
    server_pub_key_sha256_hex: String,
    challenge_identity_input_hex: String,
    challenge_identity_input_total_bytes: usize,
    challenge_sig_hex: String,
}

#[test]
fn challenge_identity_v2_matches_independent_vector() {
    let vector: ChallengeIdentityVector = toml::from_str(CHALLENGE_ID_TOML).unwrap();
    let input = &vector.inputs;
    let keypair = Ed25519Keypair::from_seed(&hex_to_array(&input.ed25519_seed_hex));
    let public_key = keypair.public_bytes();
    assert_eq!(hex::encode(public_key), vector.expected.server_pub_key_hex);
    assert_eq!(
        hex::encode(crate::common::crypto::sha256(&public_key)),
        vector.expected.server_pub_key_sha256_hex
    );
    let transcript = build_challenge_identity_input(
        &public_key,
        &hex_to_array(&input.client_nonce_hex),
        &hex_to_array(&input.server_nonce_hex),
        &hex_to_array(&input.salt_hex),
        KdfParams {
            memory_kb: input.memory_kb,
            time: input.time,
            parallelism: input.parallelism,
            argon2_version: input.argon2_version,
        },
        ChallengeIdentityTransport {
            transport_kind: TransportKind::from_u8(input.transport_kind).unwrap(),
            binding_mode: BindingMode::from_u8(input.binding_mode).unwrap(),
            tls_exporter_or_zeros: &hex_to_array(&input.tls_exporter_hex),
        },
    );
    assert_eq!(
        transcript.len(),
        vector.expected.challenge_identity_input_total_bytes
    );
    assert_eq!(
        hex::encode(&transcript),
        vector.expected.challenge_identity_input_hex
    );
    let signature = sign_identity(&keypair, &transcript);
    assert_eq!(hex::encode(signature), vector.expected.challenge_sig_hex);
}

// ==========================================================================
// 6. resumption_ticket_roundtrip (AES-256-GCM)
// ==========================================================================

#[cfg(feature = "server")]
#[derive(serde::Deserialize)]
struct TicketVector {
    inputs: TicketInputs,
    expected: TicketExpected,
}
#[cfg(feature = "server")]
#[derive(serde::Deserialize)]
struct TicketInputs {
    ticket_key_hex: String,
    ticket_nonce_hex: String,
    plaintext_msgpack_hex: String,
    aad_hex: String,
    ticket_plain_fields: TicketPlainFields,
}
#[cfg(feature = "server")]
#[derive(serde::Deserialize)]
struct TicketPlainFields {
    version: u8,
    user_id_hex: String,
    username_nfc: String,
    transport_kind_at_auth: u8,
    binding_mode_at_auth: u8,
    channel_binding_at_auth_hex: String,
    ticket_family_id_hex: String,
    original_auth_at_ns: i64,
    expires_at_ns: i64,
    family_counter: u64,
    identity_key_version: u64,
}
#[cfg(feature = "server")]
#[derive(serde::Deserialize)]
struct TicketExpected {
    ciphertext_and_tag_hex: String,
    ciphertext_and_tag_total_bytes: usize,
}

#[cfg(feature = "server")]
#[test]
fn resumption_ticket_roundtrip_matches_vector() {
    let v: TicketVector = toml::from_str(TICKET_TOML).unwrap();
    let key = hex_to_array::<32>(&v.inputs.ticket_key_hex);
    let nonce = hex_to_array::<12>(&v.inputs.ticket_nonce_hex);
    let expected_aad = hex_to_vec(&v.inputs.aad_hex);

    // Reconstruct the realistic TicketPlain and re-serialize to msgpack so the
    // vector pins the REAL ticket plaintext shape (SESSION_RESUMPTION §2).
    let user_id = hex_to_array::<16>(&v.inputs.ticket_plain_fields.user_id_hex);
    let channel_binding =
        hex_to_array::<32>(&v.inputs.ticket_plain_fields.channel_binding_at_auth_hex);
    let family_id = hex_to_array::<16>(&v.inputs.ticket_plain_fields.ticket_family_id_hex);
    let plain = TicketPlain {
        version: v.inputs.ticket_plain_fields.version,
        user_id: serde_bytes::ByteArray::from(user_id),
        username_nfc: v.inputs.ticket_plain_fields.username_nfc.clone(),
        transport_kind_at_auth: v.inputs.ticket_plain_fields.transport_kind_at_auth,
        binding_mode_at_auth: v.inputs.ticket_plain_fields.binding_mode_at_auth,
        channel_binding_at_auth: serde_bytes::ByteArray::from(channel_binding),
        ticket_family_id: serde_bytes::ByteArray::from(family_id),
        original_auth_at_ns: v.inputs.ticket_plain_fields.original_auth_at_ns as u64,
        expires_at_ns: v.inputs.ticket_plain_fields.expires_at_ns as u64,
        family_counter: v.inputs.ticket_plain_fields.family_counter,
        identity_key_version: v.inputs.ticket_plain_fields.identity_key_version,
    };
    let plaintext = rmp_serde::to_vec_named(&plain).unwrap();

    // Cross-check: the re-serialized msgpack must equal the pinned hex.
    assert_eq!(
        hex::encode(&plaintext),
        v.inputs.plaintext_msgpack_hex,
        "TicketPlain msgpack serialization drift"
    );

    // AAD is the envelope-visible "SHAMIR-TICKET-v1" || u8(version).
    let mut aad = Vec::with_capacity(TICKET_V1.len() + 1);
    aad.extend_from_slice(TICKET_V1);
    aad.push(plain.version);
    assert_eq!(aad, expected_aad, "AAD construction drift");

    let ct_and_tag = aes256gcm_encrypt(&key, &nonce, &plaintext, &aad).unwrap();
    assert_eq!(ct_and_tag.len(), v.expected.ciphertext_and_tag_total_bytes);
    assert_eq!(
        hex::encode(&ct_and_tag),
        v.expected.ciphertext_and_tag_hex,
        "AES-256-GCM ciphertext drift for fixed key+nonce+plaintext+aad"
    );

    // Round-trip: decrypt must yield the original plaintext.
    let recovered = aes256gcm_decrypt(&key, &nonce, &ct_and_tag, &aad).unwrap();
    assert_eq!(recovered, plaintext, "AES-GCM round-trip failed");
}

// ===========================================================================
// 7. identity_rotation_signed_by_old
// ===========================================================================

#[derive(serde::Deserialize)]
struct RotationVector {
    inputs: RotationInputs,
    expected: RotationExpected,
}
#[derive(serde::Deserialize)]
struct RotationInputs {
    old_ed25519_seed_hex: String,
    new_ed25519_seed_hex: String,
    transition_until_ns: i64,
    recipient_session_id_hex: String,
}
#[derive(serde::Deserialize)]
struct RotationExpected {
    old_pub_hex: String,
    new_pub_hex: String,
    rotate_event_input_hex: String,
    signed_by_old_hex: String,
}

#[test]
fn identity_rotation_signed_by_old_matches_vector() {
    let v: RotationVector = toml::from_str(ROTATION_TOML).unwrap();
    let old_seed = hex_to_array::<32>(&v.inputs.old_ed25519_seed_hex);
    let new_seed = hex_to_array::<32>(&v.inputs.new_ed25519_seed_hex);
    let old_kp = Ed25519Keypair::from_seed(&old_seed);
    let new_kp = Ed25519Keypair::from_seed(&new_seed);
    let recipient = hex_to_array::<32>(&v.inputs.recipient_session_id_hex);

    assert_eq!(hex::encode(old_kp.public_bytes()), v.expected.old_pub_hex);
    assert_eq!(hex::encode(new_kp.public_bytes()), v.expected.new_pub_hex);

    let rotate_input = build_rotate_event_input(
        &old_kp.public_bytes(),
        &new_kp.public_bytes(),
        v.inputs.transition_until_ns as u64,
        &recipient,
    );
    assert_eq!(
        hex::encode(&rotate_input),
        v.expected.rotate_event_input_hex,
        "rotate_event_input byte layout drift"
    );

    let signed = old_kp.sign(&rotate_input);
    assert_eq!(
        hex::encode(signed),
        v.expected.signed_by_old_hex,
        "signed_by_old drift: rotation chain attestation would break interop"
    );

    // Defense-in-depth: the pinned signature must verify under old_pub.
    assert!(ed25519_verify_strict(
        &old_kp.public_bytes(),
        &rotate_input,
        &signed
    ));
}

// ===========================================================================
// Coherence: the chained scenario is internally consistent.
// ===========================================================================

#[test]
fn argon2id_and_scram_vectors_share_salted_password() {
    // argon2id_default and scram_flow_default use the same password/salt/kdf,
    // so their salted_password MUST be identical — proves the two vectors
    // describe one coherent scenario, not two disconnected ones.
    let a: Argon2Vector = toml::from_str(ARGON2_TOML).unwrap();
    let s: ScramVector = toml::from_str(SCRAM_TOML).unwrap();
    assert_eq!(
        a.inputs.password, s.inputs.password,
        "argon2id and scram vectors must share the password"
    );
    assert_eq!(
        a.inputs.salt_hex, s.inputs.salt_hex,
        "argon2id and scram vectors must share the salt"
    );
    assert_eq!(
        a.expected.salted_password_hex, s.expected.salted_password_hex,
        "salted_password must be identical across the two vectors"
    );
}
