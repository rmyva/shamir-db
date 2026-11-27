//! Server-side full SCRAM handshake state machine.
//!
//! Implements the verifier path of spec §5.2 with **constant-time discipline**:
//!
//! - `binding_mode` policy check happens **before** any Argon2id (spec §4.3).
//! - Real-vs-fake user paths run identical operations: HMAC-SHA256 with either
//!   the real `stored_key` or HKDF-derived `fake_stored_key`.
//! - `server_signature`, `session_id`, and `identity_sig` are computed
//!   ALWAYS, even when verification will fail — preventing branch-timing leaks
//!   on the wire (spec §5.2.4).
//!
//! Wall-clock latency padding (spec §8.5) is **not** applied here — this is a
//! library and the runtime/transport layer is responsible for sleeping the
//! response by `target_constant_time_ms` (=`max(50ms, kdf_time*1000)`).

use crate::common::auth_message::{AuthMessage, AuthMessageInputs};
use crate::common::crypto::{try_random_array, Ed25519Keypair, HmacTag};
use crate::common::error::{Error, Result};
use crate::common::fake_blob::FakeBlob;
use crate::common::identity::{
    build_challenge_identity_input, build_identity_input, sign_identity, ChallengeIdentityTransport,
};
use crate::common::kdf_params::KdfParams;
use crate::common::scram::{build_server_signature, verify_client_proof, ClientProof};
use crate::common::time::{ns, UnixNanos};
use crate::common::types::{limits, BindingMode, ProtocolVersion, TransportKind};
use crate::common::username::NormalizedUsername;
use crate::common::wire::ChallengeRotationPayload;
use crate::server::config::{ListenerPolicy, ServerSecrets};
use crate::server::rotation::{
    build_rotation_in_progress_payload, RotationInProgressPayload, ServerIdentityState,
};
use crate::server::user_record::UserRecord;

/// Server's `auth_init` view — what arrived from the wire.
#[derive(Debug, Clone)]
pub struct AuthInitView {
    /// Username (post NFC + UsernameCaseMapped).
    pub user: NormalizedUsername,
    /// Client's CSPRNG nonce.
    pub client_nonce: [u8; limits::CLIENT_NONCE_BYTES],
    /// Wire-byte binding mode (parsed before this struct is built).
    pub binding_mode: BindingMode,
    /// Protocol version asserted by the client.
    pub version: u8,
}

/// One identity view captured for a single hello; step 2 consumes this snapshot.
#[derive(Clone)]
pub struct IdentitySnapshot {
    /// Currently signing identity keypair.
    pub current_signer: Ed25519Keypair,
    /// Previous signer during an overlap window.
    pub previous_signer: Option<Ed25519Keypair>,
    /// Rotation overlap deadline.
    pub transition_until_ns: Option<u64>,
    /// Prepared proof signed by the previous signer.
    pub rotation_proof: Option<[u8; 64]>,
}

/// Server-side per-handshake state, built from `auth_init` + listener policy.
/// Holds derived material across `auth_init → challenge → client_proof →
/// auth_ok` round trips. GC after `HANDSHAKE_TIMEOUT` (spec §8.2).
pub struct ServerHandshake<'a> {
    listener_policy: ListenerPolicy,
    transport_kind: TransportKind,
    secrets: &'a ServerSecrets,
    /// Either real persisted record or `None` (synthesized via fake_blob).
    user_record: Option<UserRecord>,
    fake_blob: FakeBlob,
    auth_init: AuthInitView,
    server_nonce: [u8; limits::SERVER_NONCE_BYTES],
    tls_exporter_or_zeros: [u8; 32],
    /// Effective KDF params surfaced to client — real user's OR server defaults.
    effective_kdf: KdfParams,
}

/// Server's `challenge` view — what gets sent on the wire.
#[derive(Debug, Clone)]
pub struct ChallengeView {
    /// Either user's real salt OR fake_salt for unknown user (constant-time).
    pub salt: [u8; limits::SALT_BYTES],
    /// KDF parameters.
    pub kdf_params: KdfParams,
    /// Per-handshake CSPRNG.
    pub server_nonce: [u8; limits::SERVER_NONCE_BYTES],
    /// Public key signing the challenge (v2); absent on v1 view.
    pub server_pub_key: Option<[u8; 32]>,
    /// Signature over the challenge-identity transcript (v2).
    pub challenge_sig: Option<[u8; 64]>,
    /// Optional in-window rotation authorization chain (v2).
    pub challenge_rotation: Option<ChallengeRotationPayload>,
}

/// Server's `auth_ok` view — bundles everything the client needs to verify.
///
/// Optional extension fields per spec §2.4 / diagram 01 step 16:
/// - `resumption_ticket` / `resumption_expires_at_ns`: when the server wants
///   to issue an initial resumption token (typical happy path).
/// - `rotation_in_progress`: when the server is in identity rotation overlap
///   AND this connection is from an orphan client still pinning the previous
///   key (spec §6.5 / diagram 05 Part B).
/// - `kdf_upgrade_required`: when the user's stored Argon2id parameters are
///   below the current server defaults (spec §13).
#[derive(Debug, Clone)]
pub struct AuthOkView {
    /// SCRAM mutual auth proof.
    pub server_signature: HmacTag,
    /// Server's current Ed25519 public key.
    pub server_pub_key: [u8; 32],
    /// Ed25519 over `identity_input`.
    pub identity_sig: [u8; 64],
    /// Session id — fresh CSPRNG.
    pub session_id: [u8; limits::SESSION_ID_BYTES],
    /// Absolute session expiry (unix nanos).
    pub expires_at_ns: u64,
    /// Optional resumption ticket bytes (encrypted blob from `issue_initial_ticket`).
    pub resumption_ticket: Option<Vec<u8>>,
    /// Optional ticket expiry (only meaningful with `resumption_ticket`).
    pub resumption_expires_at_ns: Option<u64>,
    /// Optional orphan-recovery payload (spec §6.5).
    pub rotation_in_progress: Option<RotationInProgressPayload>,
    /// Optional flag: set to `true` to ask the client to run `changePassword`
    /// soon to upgrade their stored Argon2id parameters (spec §13).
    pub kdf_upgrade_required: Option<bool>,
}

/// Outcome of [`ServerHandshake::verify_proof`].
#[derive(Debug)]
pub enum ProofOutcome {
    /// Proof was valid → emit `auth_ok`.
    Accepted(Box<AuthOkView>),
    /// Proof invalid OR user unknown → emit generic `authentication_failed`.
    /// Caller must apply latency padding before responding.
    Rejected,
}

impl<'a> ServerHandshake<'a> {
    /// Construct a handshake state, enforcing pre-Argon2id `binding_mode` policy.
    ///
    /// `lookup_user` returns `Some(record)` for known users, `None` for unknown.
    /// We synthesize fake material via [`FakeBlob`] to keep the rest of the
    /// path identical (constant-time discipline, spec §5.2.1–5.2.4).
    pub fn new<F>(
        listener_policy: ListenerPolicy,
        transport_kind: TransportKind,
        secrets: &'a ServerSecrets,
        auth_init: AuthInitView,
        tls_exporter_or_zeros: [u8; 32],
        kdf_params_current: KdfParams,
        lookup_user: F,
    ) -> Result<Self>
    where
        F: FnOnce(&NormalizedUsername) -> Option<UserRecord>,
    {
        // PRE-ARGON2ID POLICY CHECK (spec §4.3 NORMATIVE).
        if auth_init.binding_mode != listener_policy.binding_mode {
            return Err(Error::InvalidInput("binding_mode not in listener policy"));
        }
        if auth_init.version != ProtocolVersion::V1.as_u8() {
            return Err(Error::UnsupportedVersion);
        }
        if auth_init.client_nonce.iter().all(|&b| b == 0) {
            return Err(Error::InvalidInput("client_nonce all-zero"));
        }

        let user_record = lookup_user(&auth_init.user);
        let fake_blob = FakeBlob::derive(&secrets.server_secret, &auth_init.user)?;

        let server_nonce = try_random_array::<{ limits::SERVER_NONCE_BYTES }>()?;

        // M-tier audit M7: defense-in-depth — reject when client and
        // server nonces coincide. See [`verify_nonces_distinct`].
        verify_nonces_distinct(&auth_init.client_nonce, &server_nonce)?;

        // Effective params: real user's stored params (so SCRAM math works for them);
        // for unknown user → current defaults (spec §13.5 anti-enumeration trade-off).
        let effective_kdf = match &user_record {
            Some(r) => r.kdf_params,
            None => kdf_params_current,
        };

        Ok(Self {
            listener_policy,
            transport_kind,
            secrets,
            user_record,
            fake_blob,
            auth_init,
            server_nonce,
            tls_exporter_or_zeros,
            effective_kdf,
        })
    }

    /// Effective KDF params for this challenge — a known user's STORED
    /// params when present, otherwise the server default (spec §13.5).
    /// The handshake budget (spec §8 `HANDSHAKE_TIMEOUT = max(15s,
    /// kdf_time × 5)`) is derived from the `time` field of THESE params,
    /// not from the listener default, so the deadline always covers the
    /// proof window the client was actually told to run.
    pub fn effective_kdf_params(&self) -> KdfParams {
        self.effective_kdf
    }

    /// Wire view of the `challenge` sent to the client.
    pub fn challenge(&self) -> ChallengeView {
        let salt = match &self.user_record {
            Some(r) => r.salt,
            None => self.fake_blob.salt,
        };
        ChallengeView {
            salt,
            kdf_params: self.effective_kdf,
            server_nonce: self.server_nonce,
            server_pub_key: None,
            challenge_sig: None,
            challenge_rotation: None,
        }
    }

    /// Build an identity-signed challenge from one captured identity snapshot.
    pub fn challenge_signed(&self, snapshot: &IdentitySnapshot) -> Result<ChallengeView> {
        let mut view = self.challenge();
        let key = snapshot.current_signer.public_bytes();
        let input = build_challenge_identity_input(
            &key,
            &self.auth_init.client_nonce,
            &self.server_nonce,
            &view.salt,
            view.kdf_params,
            ChallengeIdentityTransport {
                transport_kind: self.transport_kind,
                binding_mode: self.auth_init.binding_mode,
                tls_exporter_or_zeros: &self.tls_exporter_or_zeros,
            },
        );
        view.server_pub_key = Some(key);
        view.challenge_sig = Some(sign_identity(&snapshot.current_signer, &input));
        if let (Some(previous), Some(until), Some(proof)) = (
            snapshot.previous_signer.as_ref(),
            snapshot.transition_until_ns,
            snapshot.rotation_proof,
        ) {
            view.challenge_rotation = Some(ChallengeRotationPayload {
                previous_pub: previous.public_bytes(),
                transition_until_ns: until,
                rotation_proof: proof,
            });
        }
        Ok(view)
    }

    /// Verify the client's SCRAM proof and (if valid) build `auth_ok`.
    ///
    /// **Constant-time discipline:** all three crypto outputs (server_signature,
    /// session_id, identity_sig) are computed regardless of verification
    /// result. Only the final `Accepted` vs `Rejected` branch differs — caller
    /// is responsible for padding the negative path latency (spec §8.5).
    pub fn verify_proof(
        &self,
        client_proof: &ClientProof,
        identity_keypair: &Ed25519Keypair,
        session_max_age: u64,
    ) -> Result<ProofOutcome> {
        // Reconstruct `auth_message` from server-side components.
        let am = self.build_auth_message()?;

        // Pick stored_key + server_key (real OR fake), constant-time access pattern.
        let (stored_key_ref, server_key_ref) = match &self.user_record {
            Some(r) => (&r.stored_key, &r.server_key[..]),
            None => (&self.fake_blob.stored_key, &self.fake_blob.server_key[..]),
        };

        // ALWAYS compute everything — no branch on success/failure (§5.2.4).
        let mut server_key_arr = [0u8; 32];
        server_key_arr.copy_from_slice(server_key_ref);
        let server_signature = build_server_signature(&server_key_arr, &am);
        let session_id = try_random_array::<{ limits::SESSION_ID_BYTES }>()?;
        let now_ns = UnixNanos::try_now()?.as_u64();
        let expires_at_ns = now_ns.saturating_add(session_max_age);

        let identity_input = build_identity_input(
            &identity_keypair.public_bytes(),
            self.transport_kind,
            self.auth_init.binding_mode,
            &self.tls_exporter_or_zeros,
            &am,
            &session_id,
            expires_at_ns,
        );
        let identity_sig = sign_identity(identity_keypair, &identity_input);

        // Verify proof.
        let verified = verify_client_proof(client_proof, stored_key_ref, &am);

        // Branch ONLY on the accept/reject decision. Both paths have already
        // computed the same three operations.

        if verified && self.user_record.is_some() {
            Ok(ProofOutcome::Accepted(Box::new(AuthOkView {
                server_signature,
                server_pub_key: identity_keypair.public_bytes(),
                identity_sig,
                session_id,
                expires_at_ns,
                resumption_ticket: None,
                resumption_expires_at_ns: None,
                rotation_in_progress: None,
                kdf_upgrade_required: None,
            })))
        } else {
            Ok(ProofOutcome::Rejected)
        }
    }

    /// Build the optional orphan-recovery payload matching this auth_ok identity.
    pub fn rotation_in_progress_payload(
        &self,
        view: &AuthOkView,
        state: &ServerIdentityState,
    ) -> Result<Option<RotationInProgressPayload>> {
        let now_ns = UnixNanos::try_now()?.as_u64();
        if !state.rotation_in_progress(now_ns) || state.current_pub() != view.server_pub_key {
            return Ok(None);
        }
        let auth_message = self.build_auth_message()?;
        let identity_input = build_identity_input(
            &view.server_pub_key,
            self.transport_kind,
            self.auth_init.binding_mode,
            &self.tls_exporter_or_zeros,
            &auth_message,
            &view.session_id,
            view.expires_at_ns,
        );
        build_rotation_in_progress_payload(state, &identity_input).map(Some)
    }

    /// Build the canonical `auth_message` from accumulated handshake state.
    fn build_auth_message(&self) -> Result<AuthMessage> {
        let salt_ref = match &self.user_record {
            Some(r) => &r.salt,
            None => &self.fake_blob.salt,
        };
        AuthMessage::build(AuthMessageInputs {
            username: &self.auth_init.user,
            client_nonce: &self.auth_init.client_nonce,
            server_nonce: &self.server_nonce,
            salt: salt_ref,
            kdf_params: self.effective_kdf,
            transport_kind: self.transport_kind,
            binding_mode: self.auth_init.binding_mode,
            tls_exporter_or_zeros: &self.tls_exporter_or_zeros,
            supported_version: ProtocolVersion::V1,
        })
    }

    /// Borrow listener policy.
    pub fn listener_policy(&self) -> ListenerPolicy {
        self.listener_policy
    }

    /// Borrow per-handshake secrets reference.
    pub fn secrets(&self) -> &ServerSecrets {
        self.secrets
    }
}

/// Default `SESSION_MAX_AGE` per spec §7.4: 24 hours.
pub const SESSION_MAX_AGE_NS: u64 = 24 * ns::HOUR;

/// Defense-in-depth check that `client_nonce != server_nonce` (M-tier
/// audit M7).
///
/// The SCRAM `auth_message` construction concatenates both nonces and
/// both feed the HMAC inputs (`client_proof`, `server_signature`). An
/// attacker who could force a collision — CSPRNG break, replay of a
/// previously-observed server_nonce, or a malicious client sending its
/// captured prior server_nonce as the new client_nonce — would weaken
/// one layer of anti-replay.
///
/// Returns the same `InvalidInput` error variant that the existing
/// all-zero nonce check produces, so call sites can treat the two
/// defenses identically.
pub fn verify_nonces_distinct(
    client_nonce: &[u8; limits::CLIENT_NONCE_BYTES],
    server_nonce: &[u8; limits::SERVER_NONCE_BYTES],
) -> Result<()> {
    if client_nonce[..] == server_nonce[..] {
        return Err(Error::InvalidInput(
            "client_nonce equals server_nonce — anti-replay defense rejected",
        ));
    }
    Ok(())
}

/// Returns true iff `user_params` is weaker than `current_defaults` along any
/// axis (memory_kb, time, parallelism). Used to drive the
/// `kdf_upgrade_required` flag on `auth_ok` per spec §13.
pub fn needs_kdf_upgrade(user_params: KdfParams, current_defaults: KdfParams) -> bool {
    user_params.memory_kb < current_defaults.memory_kb
        || user_params.time < current_defaults.time
        || user_params.parallelism < current_defaults.parallelism
}

impl AuthOkView {
    /// Attach a server-issued resumption ticket bytes + its expiry.
    ///
    /// Typical integration: after `verify_proof` returns
    /// `ProofOutcome::Accepted`, call
    /// [`crate::server::resume::issue_initial_ticket`] (or
    /// [`crate::server::ticket::encrypt_ticket_with_cipher`] for finer
    /// control), then chain `.with_resumption_ticket(bytes, expires)`
    /// before serializing.
    pub fn with_resumption_ticket(mut self, bytes: Vec<u8>, expires_at_ns: u64) -> Self {
        self.resumption_ticket = Some(bytes);
        self.resumption_expires_at_ns = Some(expires_at_ns);
        self
    }

    /// Attach an orphan-recovery `rotation_in_progress` payload (spec §6.5
    /// / diagram 05 Part B).
    ///
    /// The caller MUST build the payload via
    /// [`crate::server::rotation::build_rotation_in_progress_payload`]
    /// using the **same byte-exact** `identity_input` that the current
    /// `identity_sig` in this `AuthOkView` was signed over. The library
    /// does not auto-rebuild that input here because doing so would
    /// require threading additional state through every `verify_proof`
    /// call site.
    pub fn with_rotation_in_progress(mut self, payload: RotationInProgressPayload) -> Self {
        self.rotation_in_progress = Some(payload);
        self
    }

    /// Set `kdf_upgrade_required = Some(true)` (spec §13). Use
    /// [`needs_kdf_upgrade`] to decide.
    pub fn with_kdf_upgrade_required(mut self) -> Self {
        self.kdf_upgrade_required = Some(true);
        self
    }
}

/// **Helper sketch for integrators** — combine the three attachers above into
/// a single call given precomputed pieces.
///
/// This deliberately does NOT compute `rotation_in_progress` for the caller:
/// the orphan-recovery payload requires the byte-exact `identity_input` that
/// the current `identity_sig` covers, and that input is not stored on
/// `AuthOkView`. Callers wanting orphan-recovery emission MUST build the
/// payload themselves via [`build_rotation_in_progress_payload`] right after
/// `verify_proof` (where they still have the auth_message in scope) and pass
/// the result here as `rotation`.
///
/// `ticket: Option<(bytes, expires_at_ns)>` and `kdf_upgrade: bool` round
/// out the three optional fields. Returns the populated `AuthOkView`.
pub fn complete_auth_ok(
    base: AuthOkView,
    ticket: Option<(Vec<u8>, u64)>,
    rotation: Option<RotationInProgressPayload>,
    kdf_upgrade: bool,
) -> AuthOkView {
    let mut view = base;
    if let Some((bytes, exp)) = ticket {
        view = view.with_resumption_ticket(bytes, exp);
    }
    if let Some(p) = rotation {
        view = view.with_rotation_in_progress(p);
    }
    if kdf_upgrade {
        view = view.with_kdf_upgrade_required();
    }
    view
}
