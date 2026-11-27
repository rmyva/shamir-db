//! Client-side identity rotation handling (spec §6.5, §12.2).
//!
//! Two paths:
//! - **Active session** receives `identity_rotation` event → verify
//!   `signed_by_old` against currently-pinned `old_pub` + `recipient_session_id`
//!   == `my_session_id` + `transition_until_ns` bounds. On success: prompt
//!   user (interactive) OR `--accept-rotation` flag (non-interactive) →
//!   update pin to `SHA256(new_pub)`.
//!
//! - **Orphan client** doing fresh SCRAM — `auth_ok.rotation_in_progress` →
//!   verify `rotation_proof` (signed by `previous_pub`) → upgrade pin.
//!
//! Either path is **fail-closed by default**: never auto-update pin without
//! explicit user/operator consent (spec §6.5 critical security caveat).

use crate::common::crypto::{constant_time_eq, ed25519_verify_strict, sha256};
use crate::common::error::{Error, Result};
use crate::common::rotation::{build_rotate_event_input, build_rotation_proof_input};
use crate::common::time::ns;
use crate::common::wire::{IdentityRotationEvent, RotationInProgressPayload};

/// Verified orphan-recovery outcome. Applying `new_pin` requires explicit caller approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RotationRecovery {
    /// Hash of the previously pinned public key.
    pub previous_pin: [u8; 32],
    /// Hash of the newly verified public key.
    pub new_pin: [u8; 32],
    /// End of the overlap window.
    pub transition_until_ns: u64,
}

/// Maximum allowed overlap-window length to avoid leaked-priv-key attack
/// with far-future timestamps (spec §6.5 HIGH-2 fix).
pub const ROTATION_MAX_TRANSITION_NS: u64 = 7 * ns::DAY + ns::HOUR; // 7d + 1h skew

/// Check the shared rotation deadline bounds. Narrowly exposed for challenge verification.
pub(super) fn verify_rotation_window(transition_until_ns: u64, now_ns: u64) -> Result<()> {
    if transition_until_ns <= now_ns
        || transition_until_ns > now_ns.saturating_add(ROTATION_MAX_TRANSITION_NS)
    {
        return Err(Error::ServerIdentityChanged);
    }
    Ok(())
}

/// Verify a broadcast `identity_rotation` event received over an active
/// session (spec §12.2 step 5).
///
/// Returns the new pin if the event is valid AND the caller has confirmed.
/// `accept_rotation` MUST be `true` for the function to actually return the
/// new hash — caller wires this to interactive user prompt or
/// `--accept-rotation` CLI flag.
pub fn verify_identity_rotation_event(
    event: &IdentityRotationEvent,
    pinned_hash: &[u8; 32],
    my_session_id: &[u8; 32],
    now_ns: u64,
    accept_rotation: bool,
) -> Result<[u8; 32]> {
    // (a) old_pub matches what we currently trust.
    let old_hash = sha256(&event.old_pub);
    if !constant_time_eq(pinned_hash, &old_hash) {
        return Err(Error::ServerIdentityChanged);
    }

    // (b) Event is for THIS recipient.
    if !constant_time_eq(&event.recipient_session_id, my_session_id) {
        return Err(Error::ServerIdentityChanged);
    }

    // (c) Signature valid against old_pub.
    let payload = build_rotate_event_input(
        &event.old_pub,
        &event.new_pub,
        event.transition_until_ns,
        &event.recipient_session_id,
    );
    if !ed25519_verify_strict(&event.old_pub, &payload, &event.signed_by_old) {
        return Err(Error::ServerSignatureInvalid);
    }

    // (d, e) Window sanity: lower + upper bound (HIGH-2).
    if event.transition_until_ns <= now_ns + 60 * ns::SECOND {
        return Err(Error::ServerSignatureInvalid);
    }
    if event.transition_until_ns > now_ns + ROTATION_MAX_TRANSITION_NS {
        return Err(Error::ServerSignatureInvalid);
    }

    // Final pin update gate — must be explicit.
    if !accept_rotation {
        return Err(Error::ServerIdentityChanged);
    }

    Ok(sha256(&event.new_pub))
}

/// Verify the orphan-recovery `rotation_in_progress` payload from
/// `auth_ok.rotation_in_progress` (spec §6.5 + diagram 05 Part B steps 73-75).
///
/// Performs FOUR cryptographic checks (in order):
///
/// 1. `SHA256(previous_pub) == pinned_hash` — confirms we actually pin the old key.
/// 2. `Ed25519::verify_strict(previous_pub, identity_input, identity_sig_previous)` —
///    diagram 05 Part B step 75 mutual-auth anchor over the **same byte-exact**
///    `identity_input` that the current `identity_sig` covers.
/// 3. `Ed25519::verify_strict(previous_pub, rotation_proof_payload, rotation_proof)` —
///    chain attestation that current_pub was authorized by previous_priv.
/// 4. `transition_until_ns` window: `> now_ns` AND `≤ now_ns + 7d + 1h` (HIGH-2).
///
/// `identity_input` MUST be the bytes the caller already used to verify the
/// standard `identity_sig` (computed against `current_pub`).
///
/// Returns the new pin (= `SHA256(current_pub)`) only if every check passes
/// AND `accept_rotation = true`. Default fail-closed.
pub fn verify_rotation_in_progress(
    payload: &RotationInProgressPayload,
    current_pub: &[u8; 32],
    identity_input: &[u8],
    pinned_hash: &[u8; 32],
    now_ns: u64,
    accept_rotation: bool,
) -> Result<[u8; 32]> {
    // 1. We currently pin the OLD pub.
    let old_hash = sha256(&payload.previous_pub);
    if !constant_time_eq(pinned_hash, &old_hash) {
        return Err(Error::ServerIdentityChanged);
    }

    // 2. identity_sig_previous over the SAME identity_input — diagram 05
    //    Part B step 75. Anchors mutual auth to the previously-pinned key.
    if !ed25519_verify_strict(
        &payload.previous_pub,
        identity_input,
        &payload.identity_sig_previous,
    ) {
        return Err(Error::ServerSignatureInvalid);
    }

    // 3. rotation_proof over the rotation chain.
    let proof_payload = build_rotation_proof_input(
        &payload.previous_pub,
        current_pub,
        payload.transition_until_ns,
    );
    if !ed25519_verify_strict(
        &payload.previous_pub,
        &proof_payload,
        &payload.rotation_proof,
    ) {
        return Err(Error::ServerSignatureInvalid);
    }

    // 4. Window sanity, shared with pre-proof challenge verification.
    verify_rotation_window(payload.transition_until_ns, now_ns)
        .map_err(|_| Error::ServerSignatureInvalid)?;

    if !accept_rotation {
        return Err(Error::ServerIdentityChanged);
    }

    Ok(sha256(current_pub))
}
