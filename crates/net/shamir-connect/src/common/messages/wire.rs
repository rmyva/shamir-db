//! Shared wire views used by BOTH the client and server flows.
//!
//! These live in `common` (not behind the `server` feature) so the
//! standalone `client` feature builds without server code
//! (RV-CONNECT-FED6910E). The `server` modules re-export them so all
//! existing `server::<module>::` paths keep working.

use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::common::bootstrap_message::build_bootstrap_input;
use crate::common::crypto::Ed25519Keypair;
use crate::common::error::Error;
use crate::common::kdf_params::KdfParams;
use crate::common::time::UnixNanos;
use crate::common::types::{limits, TransportKind};
use crate::common::username::NormalizedUsername;

/// Wire view: client → server `bootstrap_hello`.
#[derive(Debug, Clone)]
pub struct BootstrapHello {
    /// 32-byte CSPRNG nonce — anti-replay for the challenge.
    pub client_nonce: [u8; 32],
}

/// Wire view: server → client `bootstrap_challenge`.
#[derive(Debug, Clone)]
pub struct BootstrapChallenge {
    /// Current Ed25519 server public key.
    pub server_pub_key: [u8; 32],
    /// Server wall-clock unix nanos (client validates `abs(now - server_time) ≤ 60s`).
    pub server_time_ns: u64,
    /// Ed25519 signature over `build_bootstrap_input(...)`.
    pub identity_sig_bootstrap: [u8; 64],
}

/// Wire view: client → server `bootstrap` (the actual create-superuser).
///
/// Custom [`Debug`] impl redacts `token` and `server_key` — both are raw
/// crypto secrets (the operator-issued bootstrap token and the SCRAM server
/// key) and must never appear in `{:?}`-style logging.
pub struct BootstrapRequest {
    /// 32-byte token from operator (out-of-band channel).
    pub token: Zeroizing<[u8; 32]>,
    /// Username (post-NFC + UsernameCaseMapped).
    pub user: NormalizedUsername,
    /// Per-user salt.
    pub salt: [u8; limits::SALT_BYTES],
    /// Stored key (= SHA256(client_key)).
    pub stored_key: [u8; 32],
    /// Server key (= HMAC(salted_password, "Server Key")).
    pub server_key: Zeroizing<[u8; 32]>,
    /// KDF parameters used.
    pub kdf_params: KdfParams,
}

impl core::fmt::Debug for BootstrapRequest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BootstrapRequest")
            .field("token", &"<REDACTED>")
            .field("user", &self.user)
            .field("salt", &self.salt)
            .field("stored_key", &self.stored_key)
            .field("server_key", &"<REDACTED>")
            .field("kdf_params", &self.kdf_params)
            .finish()
    }
}

/// Server-side compatibility wrapper; prefer [`try_make_bootstrap_challenge`].
///
/// # Panics
/// Panics if the system clock is invalid.
pub fn make_bootstrap_challenge(
    server_keypair: &Ed25519Keypair,
    transport_kind: TransportKind,
    tls_exporter: &[u8; 32],
    hello: &BootstrapHello,
) -> BootstrapChallenge {
    try_make_bootstrap_challenge(server_keypair, transport_kind, tls_exporter, hello)
        .expect("system clock failure")
}

/// Build a bootstrap challenge, propagating system clock failure.
pub fn try_make_bootstrap_challenge(
    server_keypair: &Ed25519Keypair,
    transport_kind: TransportKind,
    tls_exporter: &[u8; 32],
    hello: &BootstrapHello,
) -> crate::common::error::Result<BootstrapChallenge> {
    let server_pub = server_keypair.public_bytes();
    let server_time_ns = UnixNanos::try_now()?.as_u64();
    let payload = build_bootstrap_input(
        &server_pub,
        transport_kind,
        tls_exporter,
        &hello.client_nonce,
        server_time_ns,
    );
    let sig = server_keypair.sign(&payload);
    Ok(BootstrapChallenge {
        server_pub_key: server_pub,
        server_time_ns,
        identity_sig_bootstrap: sig,
    })
}

/// Wire view: client → server `changePassword` body.
#[derive(Debug, Clone)]
pub struct ChangePwRequest {
    /// SCRAM proof recovered from the OLD password.
    pub client_proof_old: [u8; 32],
    /// New per-user salt (CSPRNG).
    pub new_salt: [u8; limits::SALT_BYTES],
    /// New stored_key = SHA256(HMAC(new_salted_pw, "Client Key")).
    pub new_stored_key: [u8; 32],
    /// New server_key = HMAC(new_salted_pw, "Server Key").
    pub new_server_key: [u8; 32],
}

/// Wire view: `identity_rotation` event sent over the active session bus.
#[derive(Debug, Clone)]
pub struct IdentityRotationEvent {
    /// Old Ed25519 public key.
    pub old_pub: [u8; 32],
    /// New Ed25519 public key.
    pub new_pub: [u8; 32],
    /// End of overlap.
    pub transition_until_ns: u64,
    /// Full session id of the recipient (spec HIGH-5 fix — full 32 bytes).
    pub recipient_session_id: [u8; 32],
    /// Ed25519 signature by previous priv over the canonical input.
    pub signed_by_old: [u8; 64],
}

/// Positional challenge-identity rotation chain (exactly three MessagePack elements).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChallengeRotationWire {
    /// Previously pinned public key (32-byte MessagePack bin).
    #[serde(with = "serde_bytes")]
    pub previous_pub: Vec<u8>,
    /// End of the overlap in Unix nanoseconds.
    pub transition_until_ns: u64,
    /// Proof by the previous key (64-byte MessagePack bin).
    #[serde(with = "serde_bytes")]
    pub rotation_proof: Vec<u8>,
}

impl ChallengeRotationWire {
    /// Encode as a positional three-element MessagePack array.
    pub fn encode(&self) -> crate::common::error::Result<Vec<u8>> {
        if self.previous_pub.len() != 32 || self.rotation_proof.len() != 64 {
            return Err(crate::common::error::Error::ChallengeLayout);
        }
        rmp_serde::to_vec(self).map_err(|_| crate::common::error::Error::ChallengeLayout)
    }

    /// Decode only the exact marker-level positional wire layout.
    pub fn decode(bytes: &[u8]) -> crate::common::error::Result<Self> {
        fn byte(input: &[u8], pos: &mut usize) -> Option<u8> {
            let value = *input.get(*pos)?;
            *pos = pos.checked_add(1)?;
            Some(value)
        }
        fn take<'a>(input: &'a [u8], pos: &mut usize, len: usize) -> Option<&'a [u8]> {
            let end = pos.checked_add(len)?;
            let value = input.get(*pos..end)?;
            *pos = end;
            Some(value)
        }
        fn bin<'a>(input: &'a [u8], pos: &mut usize, expected: usize) -> Option<&'a [u8]> {
            let marker = byte(input, pos)?;
            let len = match marker {
                0xc4 => usize::from(byte(input, pos)?),
                0xc5 => usize::from(u16::from_be_bytes(take(input, pos, 2)?.try_into().ok()?)),
                0xc6 => usize::try_from(u32::from_be_bytes(take(input, pos, 4)?.try_into().ok()?))
                    .ok()?,
                _ => return None,
            };
            if len != expected {
                return None;
            }
            take(input, pos, len)
        }
        fn uint(input: &[u8], pos: &mut usize) -> Option<u64> {
            match byte(input, pos)? {
                n @ 0x00..=0x7f => Some(u64::from(n)),
                0xcc => Some(u64::from(byte(input, pos)?)),
                0xcd => Some(u64::from(u16::from_be_bytes(
                    take(input, pos, 2)?.try_into().ok()?,
                ))),
                0xce => Some(u64::from(u32::from_be_bytes(
                    take(input, pos, 4)?.try_into().ok()?,
                ))),
                0xcf => Some(u64::from_be_bytes(take(input, pos, 8)?.try_into().ok()?)),
                _ => None,
            }
        }

        let invalid = || Error::ChallengeLayout;
        let mut pos = 0usize;
        let marker = byte(bytes, &mut pos).ok_or_else(invalid)?;
        let count = match marker {
            0x90..=0x9f => usize::from(marker & 0x0f),
            0xdc => usize::from(u16::from_be_bytes(
                take(bytes, &mut pos, 2)
                    .ok_or_else(invalid)?
                    .try_into()
                    .map_err(|_| invalid())?,
            )),
            0xdd => usize::try_from(u32::from_be_bytes(
                take(bytes, &mut pos, 4)
                    .ok_or_else(invalid)?
                    .try_into()
                    .map_err(|_| invalid())?,
            ))
            .map_err(|_| invalid())?,
            _ => return Err(invalid()),
        };
        if count != 3 {
            return Err(invalid());
        }
        let previous_pub = bin(bytes, &mut pos, 32).ok_or_else(invalid)?.to_vec();
        let transition_until_ns = uint(bytes, &mut pos).ok_or_else(invalid)?;
        let rotation_proof = bin(bytes, &mut pos, 64).ok_or_else(invalid)?.to_vec();
        if pos != bytes.len() {
            return Err(invalid());
        }
        Ok(Self {
            previous_pub,
            transition_until_ns,
            rotation_proof,
        })
    }

    /// Check byte field lengths and convert to the validated payload.
    pub fn into_payload(self) -> crate::common::error::Result<ChallengeRotationPayload> {
        Ok(ChallengeRotationPayload {
            previous_pub: self
                .previous_pub
                .try_into()
                .map_err(|_| crate::common::error::Error::ChallengeLayout)?,
            transition_until_ns: self.transition_until_ns,
            rotation_proof: self
                .rotation_proof
                .try_into()
                .map_err(|_| crate::common::error::Error::ChallengeLayout)?,
        })
    }
}

/// Validated challenge rotation chain payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChallengeRotationPayload {
    /// Previous public identity key.
    pub previous_pub: [u8; 32],
    /// Overlap deadline in Unix nanoseconds.
    pub transition_until_ns: u64,
    /// Previous-key signature over the rotation chain.
    pub rotation_proof: [u8; 64],
}

/// Wire view: `rotation_in_progress` payload embedded in `auth_ok` for
/// offline-client orphan recovery (spec §6.5 + diagram 05 Part B step 70).
///
/// Spec §6.5 NORMATIVE 4-field schema:
/// ```text
/// rotation_in_progress: {
///     previous_pub,
///     identity_sig_previous,  // Ed25519 sig by previous_priv over SAME identity_input
///     transition_until_ns,
///     rotation_proof          // Ed25519 sig by previous_priv over rotation chain
/// }
/// ```
#[derive(Debug, Clone)]
pub struct RotationInProgressPayload {
    /// Old Ed25519 pub.
    pub previous_pub: [u8; 32],
    /// Ed25519 signature by previous_priv over the **same byte-exact**
    /// `identity_input` that current_priv signed for `identity_sig`. Allows
    /// orphan client to verify mutual auth against its old pin (spec §6.5
    /// step 3 / diagram 05 Part B step 75).
    pub identity_sig_previous: [u8; 64],
    /// End of overlap.
    pub transition_until_ns: u64,
    /// Ed25519 signature by previous priv over the
    /// `(previous_pub, current_pub, transition_until_ns)` rotation chain.
    pub rotation_proof: [u8; 64],
}

/// Serde wire form of [`RotationInProgressPayload`]: fixed-size arrays travel
/// as `Vec<u8>` (serde has no `Deserialize` for `[u8; 64]`) and are
/// length-checked by [`RotationInProgressWire::into_payload`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotationInProgressWire {
    /// Previous server public identity key (32 bytes).
    #[serde(with = "serde_bytes")]
    pub previous_pub: Vec<u8>,
    /// Previous identity signature (64 bytes).
    #[serde(with = "serde_bytes")]
    pub identity_sig_previous: Vec<u8>,
    /// Unix-nanosecond end of the identity transition window.
    pub transition_until_ns: u64,
    /// Rotation proof signature (64 bytes).
    #[serde(with = "serde_bytes")]
    pub rotation_proof: Vec<u8>,
}

impl From<&RotationInProgressPayload> for RotationInProgressWire {
    fn from(p: &RotationInProgressPayload) -> Self {
        Self {
            previous_pub: p.previous_pub.to_vec(),
            identity_sig_previous: p.identity_sig_previous.to_vec(),
            transition_until_ns: p.transition_until_ns,
            rotation_proof: p.rotation_proof.to_vec(),
        }
    }
}

impl RotationInProgressWire {
    /// `None` when any fixed-size field has the wrong length.
    pub fn into_payload(self) -> Option<RotationInProgressPayload> {
        Some(RotationInProgressPayload {
            previous_pub: self.previous_pub.try_into().ok()?,
            identity_sig_previous: self.identity_sig_previous.try_into().ok()?,
            transition_until_ns: self.transition_until_ns,
            rotation_proof: self.rotation_proof.try_into().ok()?,
        })
    }
}
