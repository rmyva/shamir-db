//! Client-side full SCRAM handshake state machine.
//!
//! Per spec §5: client knows `pinned_hash`, password, and what binding_mode
//! it can support. Walks four wire stages:
//!
//! ```text
//! 1. send auth_init { user, client_nonce, binding_mode, version }
//! 2. recv challenge   { salt, kdf_params, server_nonce }
//! 3. send client_proof
//! 4. recv auth_ok / error
//! ```
//!
//! After `auth_ok`, performs three independent verifications (any failure ⇒
//! disconnect, per spec §5.3):
//!  - **SCRAM mutual auth** — `HMAC(server_key, auth_message) == server_signature`
//!  - **TOFU pin check** — `SHA256(server_pub_key) == pinned_hash`
//!  - **Ed25519 strict verify** — `verify_strict(server_pub, identity_input, identity_sig)`

use crate::client::rotation::{
    verify_rotation_in_progress, verify_rotation_window, RotationRecovery,
};
use crate::common::auth_message::{AuthMessage, AuthMessageInputs};
use crate::common::crypto::{constant_time_eq, sha256, try_random_array};
use crate::common::error::{Error, Result};
use crate::common::identity::{
    build_challenge_identity_input, build_identity_input,
    verify_challenge_identity as verify_signature, verify_identity, ChallengeIdentityTransport,
};
use crate::common::kdf_params::{validate_client_kdf_safe, KdfParams};
use crate::common::password::WipeOnDrop;
use crate::common::rotation::build_rotation_proof_input;
use crate::common::scram::{build_client_proof, build_server_signature, ClientProof, DerivedKeys};
use crate::common::types::{limits, BindingMode, ProtocolVersion, TransportKind};
use crate::common::username::NormalizedUsername;
use crate::common::wire::{ChallengeRotationPayload, RotationInProgressPayload};

/// Server-supplied challenge fields.
#[derive(Debug, Clone)]
pub struct ServerChallenge {
    /// Per-user Argon2id salt.
    pub salt: [u8; limits::SALT_BYTES],
    /// KDF parameters for this user.
    pub kdf_params: KdfParams,
    /// Server-side per-handshake CSPRNG nonce.
    pub server_nonce: [u8; limits::SERVER_NONCE_BYTES],
}

/// v2 challenge identity data. Kept separate so existing v1 `ServerChallenge`
/// struct literals and call sites remain source-compatible.
#[derive(Debug, Clone)]
pub struct ServerChallengeV2 {
    /// The original challenge fields.
    pub challenge: ServerChallenge,
    /// v2 signer public key.
    pub server_pub_key: Option<[u8; 32]>,
    /// v2 signature over challenge identity transcript.
    pub challenge_sig: Option<[u8; 64]>,
    /// Optional v2 rotation chain.
    pub challenge_rotation: Option<ChallengeRotationPayload>,
}

/// Server-supplied auth_ok fields the client needs to verify.
///
/// Spec §2.4 / diagram 01 step 16 lists three optional extensions that
/// transport bindings populate as needed:
///
/// - `resumption_ticket` (+ `resumption_expires_at_ns`): server-issued
///   resumption token; client persists for later resume.
/// - `rotation_in_progress`: orphan-recovery payload (spec §6.5 / diagram 05
///   Part B). Client invokes
///   [`crate::client::rotation::verify_rotation_in_progress`] when present.
/// - `kdf_upgrade_required`: server requests changePassword to upgrade KDF
///   params (spec §13).
#[derive(Debug, Clone)]
pub struct ServerAuthOk {
    /// SCRAM proof of the server (mutual auth).
    pub server_signature: [u8; 32],
    /// Server's Ed25519 public key (current keypair).
    pub server_pub_key: [u8; 32],
    /// Ed25519 signature of `identity_input`.
    pub identity_sig: [u8; 64],
    /// Session id assigned by the server.
    pub session_id: [u8; limits::SESSION_ID_BYTES],
    /// Absolute session expiry (unix nanos).
    pub expires_at_ns: u64,
    /// Optional encrypted resumption ticket bytes.
    pub resumption_ticket: Option<Vec<u8>>,
    /// Optional resumption expiry (paired with `resumption_ticket`).
    pub resumption_expires_at_ns: Option<u64>,
    /// Optional orphan-recovery payload — see
    /// [`crate::common::wire::RotationInProgressPayload`].
    pub rotation_in_progress: Option<RotationInProgressPayload>,
    /// Optional flag asking client to run changePassword to upgrade KDF.
    pub kdf_upgrade_required: Option<bool>,
}

/// Successful handshake outcome — ready for session use.
#[derive(Debug, Clone)]
pub struct HandshakeSuccess {
    /// Session id assigned by the server (use as `sid` in subsequent requests).
    pub session_id: [u8; limits::SESSION_ID_BYTES],
    /// Absolute session expiry.
    pub expires_at_ns: u64,
}

/// Client-side handshake state holder.
///
/// Construct via [`HandshakeBuilder`]. Walks: `WaitingChallenge` →
/// `WaitingAuthOk` → success/error.
pub struct ClientHandshake {
    username: NormalizedUsername,
    binding_mode: BindingMode,
    transport_kind: TransportKind,
    tls_exporter_or_zeros: [u8; 32],
    pinned_hash: Option<[u8; 32]>,
    accept_new_host: bool,
    /// Generated when [`Self::start`] is called.
    client_nonce: [u8; limits::CLIENT_NONCE_BYTES],
}

/// Builder for [`ClientHandshake`].
pub struct HandshakeBuilder {
    username: NormalizedUsername,
    binding_mode: BindingMode,
    transport_kind: TransportKind,
    tls_exporter_or_zeros: [u8; 32],
    pinned_hash: Option<[u8; 32]>,
    accept_new_host: bool,
}

impl HandshakeBuilder {
    /// Create a builder. `username` MUST be already normalized (see [`NormalizedUsername`]).
    pub fn new(
        username: NormalizedUsername,
        transport_kind: TransportKind,
        binding_mode: BindingMode,
    ) -> Self {
        Self {
            username,
            binding_mode,
            transport_kind,
            tls_exporter_or_zeros: [0u8; 32],
            pinned_hash: None,
            accept_new_host: false,
        }
    }

    /// Set the TLS exporter (for `binding_mode == TlsExporter`).
    /// For other modes leave at default (zeros).
    pub fn tls_exporter(mut self, exporter: [u8; 32]) -> Self {
        self.tls_exporter_or_zeros = exporter;
        self
    }

    /// Set the pinned `SHA256(server_pub_key)` (out-of-band or known_hosts).
    pub fn pinned_hash(mut self, hash: [u8; 32]) -> Self {
        self.pinned_hash = Some(hash);
        self
    }

    /// Permit TOFU first-use (no pin yet). When set AND `pinned_hash` is `None`,
    /// `auth_ok` will accept any server pub. Per spec §6.3 dev-only.
    pub fn accept_new_host(mut self, allow: bool) -> Self {
        self.accept_new_host = allow;
        self
    }

    /// Finalize. Generates the per-handshake `client_nonce`.
    pub fn build(self) -> Result<ClientHandshake> {
        if self.pinned_hash.is_none() && !self.accept_new_host {
            return Err(Error::InvalidInput(
                "no pinned_hash AND --accept-new-host not set",
            ));
        }
        let mut client_nonce = try_random_array::<{ limits::CLIENT_NONCE_BYTES }>()?;
        if client_nonce.iter().all(|&b| b == 0) {
            // Astronomically improbable, but spec §3.2 forbids — re-roll.
            client_nonce = try_random_array::<{ limits::CLIENT_NONCE_BYTES }>()?;
        }
        Ok(ClientHandshake {
            username: self.username,
            binding_mode: self.binding_mode,
            transport_kind: self.transport_kind,
            tls_exporter_or_zeros: self.tls_exporter_or_zeros,
            pinned_hash: self.pinned_hash,
            accept_new_host: self.accept_new_host,
            client_nonce,
        })
    }
}

/// `auth_init` payload bytes — what the client transmits first.
#[derive(Debug, Clone)]
pub struct AuthInit {
    /// Username (post-normalization, UTF-8).
    pub user: String,
    /// CSPRNG client nonce.
    pub client_nonce: [u8; limits::CLIENT_NONCE_BYTES],
    /// Wire byte for binding mode.
    pub binding_mode: u8,
    /// Protocol version.
    pub version: u8,
}

impl ClientHandshake {
    /// Produce the `auth_init` message.
    pub fn auth_init(&self) -> AuthInit {
        AuthInit {
            user: self.username.as_str().to_string(),
            client_nonce: self.client_nonce,
            binding_mode: self.binding_mode.as_u8(),
            version: ProtocolVersion::V1.as_u8(),
        }
    }

    /// Verify signed challenge identity before any password derivation.
    pub fn verify_challenge_identity(
        &self,
        challenge: &ServerChallengeV2,
        now_ns: u64,
    ) -> Result<[u8; 32]> {
        let key = challenge
            .server_pub_key
            .ok_or(Error::ServerIdentityChanged)?;
        let sig = challenge
            .challenge_sig
            .ok_or(Error::ServerIdentityChanged)?;
        let input = build_challenge_identity_input(
            &key,
            &self.client_nonce,
            &challenge.challenge.server_nonce,
            &challenge.challenge.salt,
            challenge.challenge.kdf_params,
            ChallengeIdentityTransport {
                transport_kind: self.transport_kind,
                binding_mode: self.binding_mode,
                tls_exporter_or_zeros: &self.tls_exporter_or_zeros,
            },
        );
        if !verify_signature(&key, &input, &sig) {
            return Err(Error::ServerIdentityChanged);
        }
        let key_hash = sha256(&key);
        if let Some(pin) = self.pinned_hash {
            if constant_time_eq(&pin, &key_hash) {
                return Ok(key);
            }
            let chain = challenge
                .challenge_rotation
                .as_ref()
                .ok_or(Error::ServerIdentityChanged)?;
            if !constant_time_eq(&pin, &sha256(&chain.previous_pub)) {
                return Err(Error::ServerIdentityChanged);
            }
            let proof_input =
                build_rotation_proof_input(&chain.previous_pub, &key, chain.transition_until_ns);
            if !crate::common::crypto::ed25519_verify_strict(
                &chain.previous_pub,
                &proof_input,
                &chain.rotation_proof,
            ) {
                return Err(Error::ServerIdentityChanged);
            }
            verify_rotation_window(chain.transition_until_ns, now_ns)?;
        }
        Ok(key)
    }

    /// Process the server's `challenge`, derive Argon2id material, build proof.
    ///
    /// Returns `(client_proof, derived)`. The caller must store `derived` to
    /// verify the server's `auth_ok` (it carries `server_key` for mutual auth)
    /// and forward `client_proof` on the wire.
    ///
    /// `password` is consumed and zeroized on return.
    pub fn process_challenge(
        &self,
        challenge: &ServerChallenge,
        password: &mut [u8],
    ) -> Result<(ClientProof, DerivedKeys, AuthMessage)> {
        // Wipe on every exit path, including early-error returns
        // (RV-CONNECT-226DEDF4).
        let pw = WipeOnDrop::new(password);

        // (1) Validate KDF params per spec §5.1.1, then the outer
        //     defense-in-depth client safety cap (M-tier audit M1).
        challenge.kdf_params.validate_client_limits()?;
        if let Err(_msg) = validate_client_kdf_safe(&challenge.kdf_params) {
            return Err(Error::KdfParamsRejected);
        }
        if challenge.server_nonce.iter().all(|&b| b == 0) {
            return Err(Error::InvalidInput("server_nonce all-zero"));
        }

        // (2) Build canonical auth_message
        let am = AuthMessage::build(AuthMessageInputs {
            username: &self.username,
            client_nonce: &self.client_nonce,
            server_nonce: &challenge.server_nonce,
            salt: &challenge.salt,
            kdf_params: challenge.kdf_params,
            transport_kind: self.transport_kind,
            binding_mode: self.binding_mode,
            tls_exporter_or_zeros: &self.tls_exporter_or_zeros,
            supported_version: ProtocolVersion::V1,
        })?;

        // (3) Argon2id derive (~2s) + HMAC + SHA-256
        let derived = DerivedKeys::derive(pw.as_slice(), &challenge.salt, &challenge.kdf_params)?;

        // (4) client_proof
        let proof = build_client_proof(&derived.client_key, &derived.stored_key, &am);

        Ok((proof, derived, am))
    }

    /// Process the server's `auth_ok` — performs all three checks per spec §5.3.
    ///
    /// On `Ok(_)`: pin saved (if TOFU), session ready for use.
    /// On `Err(_)`: caller MUST disconnect.
    ///
    /// `pin_callback` is invoked exactly when this is the first connection
    /// to this host (TOFU): caller decides whether to persist the pin (e.g.,
    /// write to known_hosts). Receives `SHA256(server_pub_key)`.
    pub fn process_auth_ok<F>(
        &self,
        auth_ok: &ServerAuthOk,
        derived: &DerivedKeys,
        auth_message: &AuthMessage,
        mut pin_callback: F,
    ) -> Result<HandshakeSuccess>
    where
        F: FnMut(&[u8; 32]),
    {
        // (1) SCRAM mutual auth
        let expected = build_server_signature(&derived.server_key, auth_message);
        if !constant_time_eq(&expected, &auth_ok.server_signature) {
            return Err(Error::ServerAuthFailed);
        }

        // (2) Pin check (TOFU or out-of-band) — mismatch is fatal.
        // The TOFU callback itself is deferred to step (4): a pin is only
        // offered for persistence after ALL verification steps pass
        // (RV-CONNECT-0FC58D93).
        let received_hash = sha256(&auth_ok.server_pub_key);
        if let Some(pinned) = self.pinned_hash {
            if !constant_time_eq(&pinned, &received_hash) {
                return Err(Error::ServerIdentityChanged);
            }
        }

        // (3) Ed25519 strict verify of identity_sig
        let identity_input = build_identity_input(
            &auth_ok.server_pub_key,
            self.transport_kind,
            self.binding_mode,
            &self.tls_exporter_or_zeros,
            auth_message,
            &auth_ok.session_id,
            auth_ok.expires_at_ns,
        );
        if !verify_identity(
            &auth_ok.server_pub_key,
            &identity_input,
            &auth_ok.identity_sig,
        ) {
            return Err(Error::ServerSignatureInvalid);
        }

        // (4) First connection (TOFU): all checks passed — only now offer
        // the pin for persistence.
        if self.pinned_hash.is_none() {
            debug_assert!(self.accept_new_host, "builder enforces this");
            pin_callback(&received_hash);
        }

        Ok(HandshakeSuccess {
            session_id: auth_ok.session_id,
            expires_at_ns: auth_ok.expires_at_ns,
        })
    }

    /// Verify a changed server identity through the explicit orphan-recovery chain.
    /// This method never updates or persists the configured pin.
    pub fn verify_rotation_recovery(
        &self,
        ok: &ServerAuthOk,
        keys: &DerivedKeys,
        auth_message: &AuthMessage,
        now_ns: u64,
    ) -> Result<RotationRecovery> {
        let expected = build_server_signature(&keys.server_key, auth_message);
        if !constant_time_eq(&expected, &ok.server_signature) {
            return Err(Error::ServerAuthFailed);
        }

        let pinned_hash = self.pinned_hash.ok_or(Error::ServerIdentityChanged)?;
        let new_pin = sha256(&ok.server_pub_key);
        if constant_time_eq(&pinned_hash, &new_pin) {
            return Err(Error::ServerIdentityChanged);
        }

        let payload = ok
            .rotation_in_progress
            .as_ref()
            .ok_or(Error::ServerIdentityChanged)?;
        let input = build_identity_input(
            &ok.server_pub_key,
            self.transport_kind,
            self.binding_mode,
            &self.tls_exporter_or_zeros,
            auth_message,
            &ok.session_id,
            ok.expires_at_ns,
        );
        if !verify_identity(&ok.server_pub_key, &input, &ok.identity_sig) {
            return Err(Error::ServerSignatureInvalid);
        }
        let previous_pin = sha256(&payload.previous_pub);
        let _ = verify_rotation_in_progress(
            payload,
            &ok.server_pub_key,
            &input,
            &pinned_hash,
            now_ns,
            true,
        )?;
        Ok(RotationRecovery {
            previous_pin,
            new_pin,
            transition_until_ns: payload.transition_until_ns,
        })
    }

    /// Borrow the username (post-normalization).
    pub fn username(&self) -> &NormalizedUsername {
        &self.username
    }

    /// Borrow the per-handshake client nonce.
    pub fn client_nonce(&self) -> &[u8; limits::CLIENT_NONCE_BYTES] {
        &self.client_nonce
    }
}
