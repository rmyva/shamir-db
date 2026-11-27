//! Integration tests: full client+server SCRAM handshake in one process.
//!
//! Validates the complete protocol surface from `auth_init` through
//! `auth_ok` verification, plus orthogonal scenarios (wrong password,
//! unknown user, binding_mode policy mismatch, anti-downgrade).

use shamir_connect::client::{HandshakeBuilder, ServerAuthOk, ServerChallenge};
use shamir_connect::common::crypto::{sha256, Ed25519Keypair};
use shamir_connect::common::kdf_params::KdfParams;
use shamir_connect::common::scram::DerivedKeys;
use shamir_connect::common::time::UnixNanos;
use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::common::username::NormalizedUsername;
use shamir_connect::server::rotation::{build_rotation_in_progress_payload, ServerIdentityState};
use shamir_connect::server::{
    AuthInitView, ListenerPolicy, ProofOutcome, ServerHandshake, ServerSecrets, UserRecord,
    SESSION_MAX_AGE_NS,
};

#[test]
fn legacy_challenge_path_keeps_original_fields_and_no_v2_identity() {
    // Discriminates against populating v2 fields on the legacy v1 path or changing existing challenge values.
    let secrets = fixed_secrets();
    let policy = ListenerPolicy::new(BindingMode::TlsExporter);
    let params = fast_kdf();
    let auth_init = AuthInitView {
        user: NormalizedUsername::from_raw("alice").unwrap(),
        client_nonce: [0x31; 32],
        binding_mode: BindingMode::TlsExporter,
        version: 1,
    };
    let user = UserRecord {
        salt: [0x42; 16],
        stored_key: shamir_connect::common::crypto::StoredKey([0x53; 32]),
        server_key: [0x64; 32].into(),
        kdf_params: params,
        tickets_invalid_before_ns: 0,
    };
    let server = ServerHandshake::new(
        policy,
        TransportKind::Tcp,
        &secrets,
        auth_init,
        [0x75; 32],
        KdfParams::DEFAULT,
        |_user| Some(user),
    )
    .unwrap();

    let baseline = server.challenge();
    let challenge = server.challenge();
    assert_eq!(challenge.salt, baseline.salt);
    assert_eq!(challenge.kdf_params, baseline.kdf_params);
    assert_eq!(challenge.server_nonce, baseline.server_nonce);
    assert_eq!(challenge.salt, [0x42; 16]);
    assert_eq!(challenge.kdf_params, params);
    assert!(challenge.server_pub_key.is_none());
    assert!(challenge.challenge_sig.is_none());
    assert!(challenge.challenge_rotation.is_none());
}

/// Fast Argon2id params (real defaults take ~2s; tests use minimum).
fn fast_kdf() -> KdfParams {
    KdfParams {
        memory_kb: 19_456, // OWASP minimum
        time: 2,
        parallelism: 1,
        argon2_version: 0x13,
    }
}

fn fixed_secrets() -> ServerSecrets {
    ServerSecrets {
        server_secret: [0x42u8; 32],
        lockout_secret: [0xa5u8; 32],
    }
}

/// Create a UserRecord by deriving from a known password.
/// Mirrors what the server stores at createUser/bootstrap (spec §3.5).
fn make_user_record(password: &[u8], salt: [u8; 16], params: KdfParams) -> UserRecord {
    let derived = DerivedKeys::derive(password, &salt, &params).unwrap();
    UserRecord {
        salt,
        stored_key: derived.stored_key,
        server_key: derived.server_key,
        kdf_params: params,
        tickets_invalid_before_ns: 0,
    }
}

#[test]
fn happy_path_full_round_trip() {
    let username = "alice";
    let password = b"correct horse battery staple";

    // Server side state
    let secrets = fixed_secrets();
    let policy = ListenerPolicy::new(BindingMode::TlsExporter);
    let identity_kp = Ed25519Keypair::generate();
    let salt = [0x55u8; 16];
    let params = fast_kdf();

    // Server "stores" the user record (i.e., what's in __system__/users)
    let server_user_db = make_user_record(password, salt, params);
    let stored_pin = sha256(&identity_kp.public_bytes());

    // Client side
    let user_norm = NormalizedUsername::from_raw(username).unwrap();
    let exporter = [0x77u8; 32];
    let client = HandshakeBuilder::new(
        user_norm.clone(),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
    )
    .tls_exporter(exporter)
    .pinned_hash(stored_pin)
    .build()
    .unwrap();

    // Step 1: client → server: auth_init
    let auth_init = client.auth_init();

    // Server: parse + look up user
    let server_view = AuthInitView {
        user: NormalizedUsername::from_raw(&auth_init.user).unwrap(),
        client_nonce: auth_init.client_nonce,
        binding_mode: BindingMode::from_u8(auth_init.binding_mode).unwrap(),
        version: auth_init.version,
    };
    let server = ServerHandshake::new(
        policy,
        TransportKind::Tcp,
        &secrets,
        server_view,
        exporter,
        params, // kdf_params_current
        |_user| Some(server_user_db.clone()),
    )
    .unwrap();

    // Step 2: server → client: challenge
    let server_challenge = server.challenge();
    let client_challenge = ServerChallenge {
        salt: server_challenge.salt,
        kdf_params: server_challenge.kdf_params,
        server_nonce: server_challenge.server_nonce,
    };

    // Step 3: client computes proof
    let mut password_buf = password.to_vec();
    let (proof, derived, am_client) = client
        .process_challenge(&client_challenge, &mut password_buf)
        .unwrap();

    // Step 4: server verifies proof
    let outcome = server
        .verify_proof(&proof, &identity_kp, SESSION_MAX_AGE_NS)
        .unwrap();
    let auth_ok_server = match outcome {
        ProofOutcome::Accepted(ok) => *ok,
        ProofOutcome::Rejected => panic!("server rejected legitimate proof"),
    };

    // Step 5: client receives auth_ok and verifies all three
    let auth_ok_client = ServerAuthOk {
        server_signature: auth_ok_server.server_signature,
        server_pub_key: auth_ok_server.server_pub_key,
        identity_sig: auth_ok_server.identity_sig,
        session_id: auth_ok_server.session_id,
        expires_at_ns: auth_ok_server.expires_at_ns,
        resumption_ticket: None,
        resumption_expires_at_ns: None,
        rotation_in_progress: None,
        kdf_upgrade_required: None,
    };
    let success = client
        .process_auth_ok(&auth_ok_client, &derived, &am_client, |_pin| {
            panic!("pin already known — TOFU callback should not fire");
        })
        .unwrap();

    assert_eq!(success.session_id, auth_ok_server.session_id);
    assert_eq!(success.expires_at_ns, auth_ok_server.expires_at_ns);
    assert!(success.expires_at_ns > 0);
}

#[test]
fn rejects_wrong_password() {
    let username = "alice";
    let real_password = b"correct password";
    let wrong_password = b"wrong password";

    let secrets = fixed_secrets();
    let policy = ListenerPolicy::new(BindingMode::TlsExporter);
    let identity_kp = Ed25519Keypair::generate();
    let salt = [0x55u8; 16];
    let params = fast_kdf();

    let server_user_db = make_user_record(real_password, salt, params);

    let user_norm = NormalizedUsername::from_raw(username).unwrap();
    let exporter = [0x77u8; 32];
    let client = HandshakeBuilder::new(user_norm, TransportKind::Tcp, BindingMode::TlsExporter)
        .tls_exporter(exporter)
        .pinned_hash(sha256(&identity_kp.public_bytes()))
        .build()
        .unwrap();

    let auth_init = client.auth_init();
    let server_view = AuthInitView {
        user: NormalizedUsername::from_raw(&auth_init.user).unwrap(),
        client_nonce: auth_init.client_nonce,
        binding_mode: BindingMode::from_u8(auth_init.binding_mode).unwrap(),
        version: auth_init.version,
    };
    let server = ServerHandshake::new(
        policy,
        TransportKind::Tcp,
        &secrets,
        server_view,
        exporter,
        params,
        |_| Some(server_user_db.clone()),
    )
    .unwrap();

    let cc = server.challenge();
    let client_challenge = ServerChallenge {
        salt: cc.salt,
        kdf_params: cc.kdf_params,
        server_nonce: cc.server_nonce,
    };

    let mut buf = wrong_password.to_vec();
    let (proof, _derived, _am) = client
        .process_challenge(&client_challenge, &mut buf)
        .unwrap();

    let outcome = server
        .verify_proof(&proof, &identity_kp, SESSION_MAX_AGE_NS)
        .unwrap();
    assert!(matches!(outcome, ProofOutcome::Rejected));
}

#[test]
fn unknown_user_path_constant_time_returns_rejected() {
    // Client tries to log in for "ghost" — server has no record.
    let username = "ghost";
    let password = b"any password";

    let secrets = fixed_secrets();
    let policy = ListenerPolicy::new(BindingMode::TlsExporter);
    let identity_kp = Ed25519Keypair::generate();
    let params = fast_kdf();
    let exporter = [0x77u8; 32];

    let user_norm = NormalizedUsername::from_raw(username).unwrap();
    let client = HandshakeBuilder::new(user_norm, TransportKind::Tcp, BindingMode::TlsExporter)
        .tls_exporter(exporter)
        .pinned_hash(sha256(&identity_kp.public_bytes()))
        .build()
        .unwrap();

    let auth_init = client.auth_init();
    let server_view = AuthInitView {
        user: NormalizedUsername::from_raw(&auth_init.user).unwrap(),
        client_nonce: auth_init.client_nonce,
        binding_mode: BindingMode::from_u8(auth_init.binding_mode).unwrap(),
        version: auth_init.version,
    };
    let server = ServerHandshake::new(
        policy,
        TransportKind::Tcp,
        &secrets,
        server_view,
        exporter,
        params,
        |_| None, // user not found
    )
    .unwrap();

    // Client gets a (fake) challenge that looks structurally identical.
    let cc = server.challenge();
    assert_eq!(cc.salt.len(), 16);
    assert_eq!(cc.kdf_params, params);

    let client_challenge = ServerChallenge {
        salt: cc.salt,
        kdf_params: cc.kdf_params,
        server_nonce: cc.server_nonce,
    };

    let mut buf = password.to_vec();
    let (proof, _derived, _am) = client
        .process_challenge(&client_challenge, &mut buf)
        .unwrap();

    let outcome = server
        .verify_proof(&proof, &identity_kp, SESSION_MAX_AGE_NS)
        .unwrap();
    // Server STILL goes through full crypto pipeline for unknown users
    // (HKDF fake_blob → HMAC → SHA256) and emits Rejected — same outcome
    // as wrong-password case.
    assert!(matches!(outcome, ProofOutcome::Rejected));
}

#[test]
fn rejects_binding_mode_policy_mismatch_pre_argon2id() {
    // Listener accepts only TlsExporter; client claims None (plain).
    let username = "alice";
    let secrets = fixed_secrets();
    let policy = ListenerPolicy::new(BindingMode::TlsExporter);
    let exporter = [0x77u8; 32];

    let user_norm = NormalizedUsername::from_raw(username).unwrap();
    let auth_init_view = AuthInitView {
        user: user_norm,
        client_nonce: [0xabu8; 32],
        binding_mode: BindingMode::None, // mismatch with policy
        version: 1,
    };

    let result = ServerHandshake::new(
        policy,
        TransportKind::Tcp,
        &secrets,
        auth_init_view,
        exporter,
        fast_kdf(),
        |_| panic!("user lookup must NOT happen — pre-Argon policy check should fire first"),
    );

    assert!(result.is_err());
}

#[test]
fn rejects_unsupported_protocol_version() {
    let secrets = fixed_secrets();
    let policy = ListenerPolicy::new(BindingMode::TlsExporter);
    let auth_init_view = AuthInitView {
        user: NormalizedUsername::from_raw("alice").unwrap(),
        client_nonce: [1u8; 32],
        binding_mode: BindingMode::TlsExporter,
        version: 99, // future protocol
    };

    let result = ServerHandshake::new(
        policy,
        TransportKind::Tcp,
        &secrets,
        auth_init_view,
        [0u8; 32],
        fast_kdf(),
        |_| panic!("user lookup must NOT happen"),
    );
    assert!(result.is_err());
}

#[test]
fn rejects_all_zero_client_nonce() {
    let secrets = fixed_secrets();
    let policy = ListenerPolicy::new(BindingMode::TlsExporter);
    let auth_init_view = AuthInitView {
        user: NormalizedUsername::from_raw("alice").unwrap(),
        client_nonce: [0u8; 32], // all-zero
        binding_mode: BindingMode::TlsExporter,
        version: 1,
    };

    let result = ServerHandshake::new(
        policy,
        TransportKind::Tcp,
        &secrets,
        auth_init_view,
        [0u8; 32],
        fast_kdf(),
        |_| None,
    );
    assert!(result.is_err());
}

#[test]
fn verify_nonces_distinct_rejects_equal_nonces() {
    // M-tier audit M7 defense-in-depth: equal client/server nonces must
    // be rejected to avoid weakening the SCRAM anti-replay invariant.
    use shamir_connect::server::handshake::verify_nonces_distinct;
    let nonce = [0x33u8; 32];
    let res = verify_nonces_distinct(&nonce, &nonce);
    assert!(res.is_err(), "equal nonces must be rejected");
}

#[test]
fn verify_nonces_distinct_accepts_different_nonces() {
    use shamir_connect::server::handshake::verify_nonces_distinct;
    let cn = [0x33u8; 32];
    let sn = [0x44u8; 32];
    verify_nonces_distinct(&cn, &sn).expect("differing nonces must be accepted");
}

#[test]
fn rv_client_f20c09bf_rotation_hint_never_auto_repins() {
    let username = "alice";
    let password = b"secret password";
    let secrets = fixed_secrets();
    let policy = ListenerPolicy::new(BindingMode::TlsExporter);
    let now_ns = UnixNanos::now().as_u64();
    let transition_until_ns = now_ns + 1_000_000_000;
    let identity = ServerIdentityState::from_material(
        &[0x31; 32],
        Some(&[0x42; 32]),
        Some(transition_until_ns),
        1,
    );
    let wrong_pin = sha256(&identity.previous_pub().expect("previous rotation key"));
    let params = fast_kdf();
    let exporter = [0x77u8; 32];
    let server_user_db = make_user_record(password, [0x55u8; 16], params);
    let current_key = Ed25519Keypair::from_seed(&[0x31; 32]);
    let client = HandshakeBuilder::new(
        NormalizedUsername::from_raw(username).unwrap(),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
    )
    .tls_exporter(exporter)
    .pinned_hash(wrong_pin)
    .build()
    .unwrap();
    let auth_init = client.auth_init();
    let server = ServerHandshake::new(
        policy,
        TransportKind::Tcp,
        &secrets,
        AuthInitView {
            user: NormalizedUsername::from_raw(&auth_init.user).unwrap(),
            client_nonce: auth_init.client_nonce,
            binding_mode: BindingMode::from_u8(auth_init.binding_mode).unwrap(),
            version: auth_init.version,
        },
        exporter,
        params,
        |_| Some(server_user_db.clone()),
    )
    .unwrap();
    let challenge = server.challenge();
    let (proof, derived, auth_message) = client
        .process_challenge(
            &ServerChallenge {
                salt: challenge.salt,
                kdf_params: challenge.kdf_params,
                server_nonce: challenge.server_nonce,
            },
            &mut password.to_vec(),
        )
        .unwrap();
    let accepted = match server
        .verify_proof(&proof, &current_key, SESSION_MAX_AGE_NS)
        .unwrap()
    {
        ProofOutcome::Accepted(ok) => ok,
        ProofOutcome::Rejected => panic!("expected valid password"),
    };
    let identity_input = shamir_connect::common::identity::build_identity_input(
        &accepted.server_pub_key,
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        &exporter,
        &auth_message,
        &accepted.session_id,
        accepted.expires_at_ns,
    );
    let rotation = build_rotation_in_progress_payload(&identity, &identity_input).unwrap();
    let auth_ok = ServerAuthOk {
        server_signature: accepted.server_signature,
        server_pub_key: accepted.server_pub_key,
        identity_sig: accepted.identity_sig,
        session_id: accepted.session_id,
        expires_at_ns: accepted.expires_at_ns,
        resumption_ticket: None,
        resumption_expires_at_ns: None,
        rotation_in_progress: Some(rotation),
        kdf_upgrade_required: None,
    };
    assert!(matches!(
        client.process_auth_ok(&auth_ok, &derived, &auth_message, |_| {
            panic!("pin must not auto-repin")
        }),
        Err(shamir_connect::Error::ServerIdentityChanged)
    ));
}

#[test]
fn client_pin_mismatch_after_auth_ok_aborts() {
    // Server signs with one Ed25519 key; client has a different pin.
    let username = "alice";
    let password = b"secret password";

    let secrets = fixed_secrets();
    let policy = ListenerPolicy::new(BindingMode::TlsExporter);
    let real_kp = Ed25519Keypair::generate();
    let salt = [0x55u8; 16];
    let params = fast_kdf();
    let exporter = [0x77u8; 32];

    let server_user_db = make_user_record(password, salt, params);

    // CLIENT PINS A DIFFERENT KEY than what server uses
    let attacker_kp = Ed25519Keypair::generate();
    let wrong_pin = sha256(&attacker_kp.public_bytes());

    let user_norm = NormalizedUsername::from_raw(username).unwrap();
    let client = HandshakeBuilder::new(user_norm, TransportKind::Tcp, BindingMode::TlsExporter)
        .tls_exporter(exporter)
        .pinned_hash(wrong_pin)
        .build()
        .unwrap();

    let auth_init = client.auth_init();
    let server_view = AuthInitView {
        user: NormalizedUsername::from_raw(&auth_init.user).unwrap(),
        client_nonce: auth_init.client_nonce,
        binding_mode: BindingMode::from_u8(auth_init.binding_mode).unwrap(),
        version: auth_init.version,
    };
    let server = ServerHandshake::new(
        policy,
        TransportKind::Tcp,
        &secrets,
        server_view,
        exporter,
        params,
        |_| Some(server_user_db.clone()),
    )
    .unwrap();

    let cc = server.challenge();
    let client_challenge = ServerChallenge {
        salt: cc.salt,
        kdf_params: cc.kdf_params,
        server_nonce: cc.server_nonce,
    };

    let mut buf = password.to_vec();
    let (proof, derived, am_client) = client
        .process_challenge(&client_challenge, &mut buf)
        .unwrap();

    let outcome = server
        .verify_proof(&proof, &real_kp, SESSION_MAX_AGE_NS)
        .unwrap();
    let ok = match outcome {
        ProofOutcome::Accepted(ok) => ok,
        ProofOutcome::Rejected => panic!("expected accept (pw is right; pin is just different)"),
    };

    let server_auth_ok = ServerAuthOk {
        server_signature: ok.server_signature,
        server_pub_key: ok.server_pub_key,
        identity_sig: ok.identity_sig,
        session_id: ok.session_id,
        expires_at_ns: ok.expires_at_ns,
        resumption_ticket: None,
        resumption_expires_at_ns: None,
        rotation_in_progress: None,
        kdf_upgrade_required: None,
    };

    // Client should reject with ServerIdentityChanged (pin mismatch).
    let result = client.process_auth_ok(&server_auth_ok, &derived, &am_client, |_| {
        panic!("TOFU should not fire when pin is set");
    });
    assert!(matches!(
        result,
        Err(shamir_connect::Error::ServerIdentityChanged)
    ));
}

#[test]
fn tofu_first_connect_invokes_pin_callback() {
    let username = "alice";
    let password = b"secret password";

    let secrets = fixed_secrets();
    let policy = ListenerPolicy::new(BindingMode::TlsExporter);
    let real_kp = Ed25519Keypair::generate();
    let salt = [0x55u8; 16];
    let params = fast_kdf();
    let exporter = [0x77u8; 32];

    let server_user_db = make_user_record(password, salt, params);

    let user_norm = NormalizedUsername::from_raw(username).unwrap();
    // No pinned_hash — accept_new_host enabled (TOFU).
    let client = HandshakeBuilder::new(user_norm, TransportKind::Tcp, BindingMode::TlsExporter)
        .tls_exporter(exporter)
        .accept_new_host(true)
        .build()
        .unwrap();

    let auth_init = client.auth_init();
    let server_view = AuthInitView {
        user: NormalizedUsername::from_raw(&auth_init.user).unwrap(),
        client_nonce: auth_init.client_nonce,
        binding_mode: BindingMode::from_u8(auth_init.binding_mode).unwrap(),
        version: auth_init.version,
    };
    let server = ServerHandshake::new(
        policy,
        TransportKind::Tcp,
        &secrets,
        server_view,
        exporter,
        params,
        |_| Some(server_user_db.clone()),
    )
    .unwrap();

    let cc = server.challenge();
    let cc_view = ServerChallenge {
        salt: cc.salt,
        kdf_params: cc.kdf_params,
        server_nonce: cc.server_nonce,
    };

    let mut buf = password.to_vec();
    let (proof, derived, am_client) = client.process_challenge(&cc_view, &mut buf).unwrap();
    let outcome = server
        .verify_proof(&proof, &real_kp, SESSION_MAX_AGE_NS)
        .unwrap();
    let ok = match outcome {
        ProofOutcome::Accepted(ok) => ok,
        _ => panic!(),
    };

    let server_auth_ok = ServerAuthOk {
        server_signature: ok.server_signature,
        server_pub_key: ok.server_pub_key,
        identity_sig: ok.identity_sig,
        session_id: ok.session_id,
        expires_at_ns: ok.expires_at_ns,
        resumption_ticket: None,
        resumption_expires_at_ns: None,
        rotation_in_progress: None,
        kdf_upgrade_required: None,
    };

    let mut tofu_fired = false;
    let _ = client
        .process_auth_ok(&server_auth_ok, &derived, &am_client, |hash| {
            assert_eq!(*hash, sha256(&real_kp.public_bytes()));
            tofu_fired = true;
        })
        .unwrap();

    assert!(tofu_fired, "TOFU pin callback must fire on first connect");
}

#[test]
fn tampered_identity_sig_aborts_client() {
    let username = "alice";
    let password = b"secret password";

    let secrets = fixed_secrets();
    let policy = ListenerPolicy::new(BindingMode::TlsExporter);
    let real_kp = Ed25519Keypair::generate();
    let salt = [0x55u8; 16];
    let params = fast_kdf();
    let exporter = [0x77u8; 32];

    let server_user_db = make_user_record(password, salt, params);

    let user_norm = NormalizedUsername::from_raw(username).unwrap();
    let client = HandshakeBuilder::new(user_norm, TransportKind::Tcp, BindingMode::TlsExporter)
        .tls_exporter(exporter)
        .pinned_hash(sha256(&real_kp.public_bytes()))
        .build()
        .unwrap();

    let auth_init = client.auth_init();
    let server_view = AuthInitView {
        user: NormalizedUsername::from_raw(&auth_init.user).unwrap(),
        client_nonce: auth_init.client_nonce,
        binding_mode: BindingMode::from_u8(auth_init.binding_mode).unwrap(),
        version: auth_init.version,
    };
    let server = ServerHandshake::new(
        policy,
        TransportKind::Tcp,
        &secrets,
        server_view,
        exporter,
        params,
        |_| Some(server_user_db.clone()),
    )
    .unwrap();

    let cc = server.challenge();
    let cc_view = ServerChallenge {
        salt: cc.salt,
        kdf_params: cc.kdf_params,
        server_nonce: cc.server_nonce,
    };

    let mut buf = password.to_vec();
    let (proof, derived, am_client) = client.process_challenge(&cc_view, &mut buf).unwrap();
    let outcome = server
        .verify_proof(&proof, &real_kp, SESSION_MAX_AGE_NS)
        .unwrap();
    let ok = match outcome {
        ProofOutcome::Accepted(ok) => ok,
        _ => panic!(),
    };

    // TAMPER WITH THE SIG
    let mut bad_sig = ok.identity_sig;
    bad_sig[0] ^= 0xff;

    let server_auth_ok = ServerAuthOk {
        server_signature: ok.server_signature,
        server_pub_key: ok.server_pub_key,
        identity_sig: bad_sig,
        session_id: ok.session_id,
        expires_at_ns: ok.expires_at_ns,
        resumption_ticket: None,
        resumption_expires_at_ns: None,
        rotation_in_progress: None,
        kdf_upgrade_required: None,
    };

    let result = client.process_auth_ok(&server_auth_ok, &derived, &am_client, |_| {});
    assert!(matches!(
        result,
        Err(shamir_connect::Error::ServerSignatureInvalid)
    ));
}

// ----------------------------------------------------------------------------
// complete_auth_ok helper tests (integration-helper task)
// ----------------------------------------------------------------------------

/// Helper test: complete_auth_ok preserves base fields and applies all
/// three optional extensions when supplied.
#[test]
fn complete_auth_ok_attaches_all_three_optional_fields() {
    use shamir_connect::server::handshake::{complete_auth_ok, AuthOkView};
    use shamir_connect::server::rotation::{
        build_rotation_in_progress_payload, ServerIdentityState,
    };

    // Build a base AuthOkView with deterministic content.
    let base = AuthOkView {
        server_signature: [0xa1u8; 32],
        server_pub_key: [0xb2u8; 32],
        identity_sig: [0xc3u8; 64],
        session_id: [0xd4u8; 32],
        expires_at_ns: 1_234_567_890,
        resumption_ticket: None,
        resumption_expires_at_ns: None,
        rotation_in_progress: None,
        kdf_upgrade_required: None,
    };

    // Build a real rotation_in_progress payload via the canonical builder.
    let identity = ServerIdentityState::fresh();
    identity.rotate(1_000_000).unwrap();
    let identity_input: &[u8] = b"test-identity-input-bytes-for-helper";
    let rotation = build_rotation_in_progress_payload(&identity, identity_input).unwrap();

    let ticket_bytes = vec![0x77u8; 80];
    let ticket_expires = 9_999_999_999u64;

    let view = complete_auth_ok(
        base.clone(),
        Some((ticket_bytes.clone(), ticket_expires)),
        Some(rotation.clone()),
        true,
    );

    // Base fields preserved.
    assert_eq!(view.server_signature, base.server_signature);
    assert_eq!(view.server_pub_key, base.server_pub_key);
    assert_eq!(view.identity_sig, base.identity_sig);
    assert_eq!(view.session_id, base.session_id);
    assert_eq!(view.expires_at_ns, base.expires_at_ns);

    // Extensions populated.
    assert_eq!(
        view.resumption_ticket.as_deref(),
        Some(ticket_bytes.as_slice())
    );
    assert_eq!(view.resumption_expires_at_ns, Some(ticket_expires));
    assert!(view.rotation_in_progress.is_some());
    let r = view.rotation_in_progress.unwrap();
    assert_eq!(r.previous_pub, rotation.previous_pub);
    assert_eq!(r.identity_sig_previous, rotation.identity_sig_previous);
    assert_eq!(view.kdf_upgrade_required, Some(true));
}

/// Helper test: complete_auth_ok leaves all extensions None when no inputs
/// supplied.
#[test]
fn complete_auth_ok_no_extensions_when_none_supplied() {
    use shamir_connect::server::handshake::{complete_auth_ok, AuthOkView};

    let base = AuthOkView {
        server_signature: [0u8; 32],
        server_pub_key: [0u8; 32],
        identity_sig: [0u8; 64],
        session_id: [0u8; 32],
        expires_at_ns: 0,
        resumption_ticket: None,
        resumption_expires_at_ns: None,
        rotation_in_progress: None,
        kdf_upgrade_required: None,
    };
    let view = complete_auth_ok(base, None, None, false);
    assert!(view.resumption_ticket.is_none());
    assert!(view.resumption_expires_at_ns.is_none());
    assert!(view.rotation_in_progress.is_none());
    assert!(view.kdf_upgrade_required.is_none());
}

/// needs_kdf_upgrade returns true iff user_params is weaker on ANY axis.
#[test]
fn needs_kdf_upgrade_detects_weaker_axes() {
    use shamir_connect::server::handshake::needs_kdf_upgrade;

    let current = KdfParams {
        memory_kb: 131_072,
        time: 4,
        parallelism: 1,
        argon2_version: 0x13,
    };

    // Same → no upgrade.
    assert!(!needs_kdf_upgrade(current, current));

    // Weaker memory.
    let weaker_mem = KdfParams {
        memory_kb: 65_536,
        ..current
    };
    assert!(needs_kdf_upgrade(weaker_mem, current));

    // Weaker time.
    let weaker_time = KdfParams { time: 2, ..current };
    assert!(needs_kdf_upgrade(weaker_time, current));

    // Stronger user is fine.
    let stronger = KdfParams {
        memory_kb: 262_144,
        time: 8,
        parallelism: 2,
        argon2_version: 0x13,
    };
    assert!(!needs_kdf_upgrade(stronger, current));
}

/// AuthOkView builder methods chain correctly.
#[test]
fn auth_ok_view_with_methods_chain() {
    use shamir_connect::server::handshake::AuthOkView;

    let view = AuthOkView {
        server_signature: [0u8; 32],
        server_pub_key: [0u8; 32],
        identity_sig: [0u8; 64],
        session_id: [0u8; 32],
        expires_at_ns: 100,
        resumption_ticket: None,
        resumption_expires_at_ns: None,
        rotation_in_progress: None,
        kdf_upgrade_required: None,
    }
    .with_resumption_ticket(vec![1, 2, 3], 200)
    .with_kdf_upgrade_required();

    assert_eq!(
        view.resumption_ticket.as_deref(),
        Some([1u8, 2, 3].as_slice())
    );
    assert_eq!(view.resumption_expires_at_ns, Some(200));
    assert_eq!(view.kdf_upgrade_required, Some(true));
}
