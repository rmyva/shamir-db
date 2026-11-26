//! Server-side identity rotation state machine (spec §6.4, §6.5, §12.2).

use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::common::crypto::{try_random_bytes, Ed25519Keypair};
use crate::common::error::{Error, Result};
use crate::common::rotation::{build_rotate_event_input, build_rotation_proof_input};
use crate::common::time::ns;
use zeroize::Zeroizing;

/// Rotation overlap window — fixed 7 days per spec §12.2.
pub const ROTATION_OVERLAP_NS: u64 = 7 * ns::DAY;

/// Server identity state — current keypair plus optional previous
/// (during the 7-day overlap).
pub struct ServerIdentityState {
    inner: ArcSwap<ServerIdentityInner>,
}

#[derive(Clone)]
struct ServerIdentityInner {
    current: Ed25519Keypair,
    previous: Option<Ed25519Keypair>,
    rotation_until_ns: Option<u64>,
    /// Monotonically increasing version of the current Ed25519 keypair.
    /// Starts at 0 for `fresh()`; `rotate()` increments by 1. Allows tickets
    /// to record which keypair they were issued under so that during a
    /// rotation overlap window the server can reject pre-rotation tickets
    /// (spec §5.7 NORMATIVE / diagram 12).
    current_version: u64,
}

impl ServerIdentityState {
    /// Construct from a freshly-generated keypair (`current_version = 0`).
    ///
    /// Legacy wrapper; prefer drawing the seed via [`Ed25519Keypair::try_generate`]
    /// and [`Self::from_material`].
    ///
    /// # Panics
    /// Panics on OS entropy failure.
    pub fn fresh() -> Self {
        Self {
            inner: ArcSwap::from_pointee(ServerIdentityInner {
                current: Ed25519Keypair::generate(),
                previous: None,
                rotation_until_ns: None,
                current_version: 0,
            }),
        }
    }

    /// Construct from explicit material (for rehydration from `__system__/server_meta`).
    ///
    /// `current_version` MUST match the persisted version counter so that
    /// post-restart resumes correctly accept/reject in-flight tickets.
    pub fn from_material(
        current_seed: &[u8; 32],
        previous_seed: Option<&[u8; 32]>,
        rotation_until_ns: Option<u64>,
        current_version: u64,
    ) -> Self {
        Self {
            inner: ArcSwap::from_pointee(ServerIdentityInner {
                current: Ed25519Keypair::from_seed(current_seed),
                previous: previous_seed.map(Ed25519Keypair::from_seed),
                rotation_until_ns,
                current_version,
            }),
        }
    }

    /// Current identity-key version.
    ///
    pub fn current_version(&self) -> u64 {
        self.inner.load().current_version
    }

    /// Whether a ticket carrying `ticket_identity_key_version` is acceptable
    /// for resumption right now (spec §5.7 NORMATIVE / diagram 12).
    ///
    /// Rules:
    /// - Outside overlap (no `previous`): accept ONLY tickets at `current_version`.
    ///   (Older tickets predate at least one rotation that already finalized;
    ///   their issuing keypair is gone — force re-auth.)
    /// - Inside overlap: ticket version MUST equal `current_version`. Tickets
    ///   issued under `previous` (i.e., `current_version - 1`) are rejected
    ///   so orphan clients are forced into full re-auth and thus pick up the
    ///   `rotation_in_progress` payload (spec §6.5 / diagram 05 Part B).
    ///
    #[inline]
    pub fn is_ticket_version_acceptable(&self, ticket_version: u64) -> bool {
        ticket_version == self.inner.load().current_version
    }

    /// Current public key.
    pub fn current_pub(&self) -> [u8; 32] {
        self.inner.load().current.public_bytes()
    }

    /// Previous public key during overlap (if any).
    pub fn previous_pub(&self) -> Option<[u8; 32]> {
        self.inner
            .load()
            .previous
            .as_ref()
            .map(|kp: &Ed25519Keypair| kp.public_bytes())
    }

    /// `transition_until_ns` if currently in overlap.
    pub fn rotation_until_ns(&self) -> Option<u64> {
        self.inner.load().rotation_until_ns
    }

    /// Whether overlap window is currently active.
    pub fn rotation_in_progress(&self, now_ns: u64) -> bool {
        match self.inner.load().rotation_until_ns {
            Some(t) => now_ns < t,
            None => false,
        }
    }

    /// Sign a message with the **current** priv key.
    pub fn sign_with_current(&self, msg: &[u8]) -> [u8; 64] {
        self.inner.load().current.sign(msg)
    }

    /// Sign a message with the **previous** priv key (if in overlap).
    pub fn sign_with_previous(&self, msg: &[u8]) -> Option<[u8; 64]> {
        self.inner
            .load()
            .previous
            .as_ref()
            .map(|kp: &Ed25519Keypair| kp.sign(msg))
    }

    /// Execute `rotateServerIdentity` (spec §12.2):
    /// - Pre-condition: not already in overlap (HIGH-5 fix).
    /// - Atomic (RV-CONNECT-F8A5E821): check-then-act runs inside
    ///   `ArcSwap::rcu`, so a concurrent rotation cannot interleave between
    ///   the overlap check and the publish — the closure is retried against
    ///   the freshest snapshot whenever another writer wins. Whether the
    ///   call was rejected is derived from the outcome after `rcu`, so no
    ///   per-attempt out-flag can leak a stale `rejected` state into a
    ///   retried winning attempt. Ticket acceptance reads the committed snapshot
    ///   directly, so the key and version publish as one authoritative state.
    ///
    /// Entropy (P2-10 second tranche, RV-CONNECT-387EAA05): the candidate
    /// seed is drawn ONCE, before the `rcu`, through the fallible
    /// [`try_random_bytes`] path into a zeroizing buffer; on failure the call returns
    /// [`Error::Environment`] with keys, `rotation_until_ns` and the version
    /// counter unchanged — no partial publication.
    ///
    /// Known remaining panicking-clock/entropy call sites (documented,
    /// compat-additive limitation): `persist_snapshot` paths run inside the
    /// scheduler's `safe_run` wrapper, and `default_node_id` is a boot-time
    /// fail-fast; both are outside this fallible path.
    pub fn rotate(&self, now_ns: u64) -> Result<RotationOutcome> {
        // No per-attempt out-flag: whether the call was rejected is derived
        // from `outcome` after `rcu`, so a retry that wins can never be
        // mistaken for a rejection (the old `rejected` flag was never reset
        // on a successful retry).
        // RV-CONNECT-387EAA05: draw the candidate seed ONCE, before the rcu,
        // through the fallible path. On entropy failure we return before any
        // state mutation — keys, rotation_until_ns and version all stay put.
        let mut candidate_seed = Zeroizing::new([0u8; 32]);
        try_random_bytes(candidate_seed.as_mut())?;
        let mut outcome: Option<RotationOutcome> = None;
        self.inner.rcu(|current: &Arc<ServerIdentityInner>| {
            // Per-attempt state: a retry that ends in rejection must not keep
            // the candidate built by an earlier, losing attempt.
            outcome = None;
            if let Some(t) = current.rotation_until_ns {
                if now_ns < t {
                    return Arc::clone(current);
                }
            }
            let old_pub = current.current.public_bytes();
            let new_keypair = Ed25519Keypair::from_seed(&candidate_seed);
            let new_pub = new_keypair.public_bytes();
            let transition_until_ns = now_ns.saturating_add(ROTATION_OVERLAP_NS);
            let new_version = current.current_version.saturating_add(1);
            outcome = Some(RotationOutcome {
                old_pub,
                new_pub,
                transition_until_ns,
                new_version,
            });
            Arc::new(ServerIdentityInner {
                current: new_keypair,
                previous: Some(current.current.clone()),
                rotation_until_ns: Some(transition_until_ns),
                current_version: new_version,
            })
        });
        if outcome.is_none() {
            return Err(Error::InvalidInput("rotation_in_progress_already"));
        }
        Ok(outcome.expect("checked above"))
    }

    /// Background task: when `now_ns >= rotation_until_ns`, zeroize previous
    /// and clear the overlap state. Returns `true` if cleanup happened.
    /// Atomic via `rcu` (RV-CONNECT-F8A5E821): competes safely with a
    /// concurrent `rotate` — whichever CAS wins last is based on the freshest
    /// snapshot, and the overlap check is re-evaluated on every retry.
    pub fn try_finalize(&self, now_ns: u64) -> bool {
        let mut finalized = false;
        self.inner.rcu(
            |current: &Arc<ServerIdentityInner>| match current.rotation_until_ns {
                Some(t) if now_ns >= t => {
                    finalized = true;
                    Arc::new(ServerIdentityInner {
                        current: current.current.clone(),
                        previous: None,
                        rotation_until_ns: None,
                        current_version: current.current_version,
                    })
                }
                _ => {
                    finalized = false;
                    Arc::clone(current)
                }
            },
        );
        finalized
    }
}

/// Result of a successful [`ServerIdentityState::rotate`] call.
#[derive(Debug, Clone)]
pub struct RotationOutcome {
    /// Old (now-previous) Ed25519 public key.
    pub old_pub: [u8; 32],
    /// New (now-current) Ed25519 public key.
    pub new_pub: [u8; 32],
    /// Wall-clock end of overlap window.
    pub transition_until_ns: u64,
    /// New `current_version` after rotation. Used by callers issuing fresh
    /// tickets immediately so they tag the ticket with the just-incremented
    /// version (spec §5.7 / diagram 12).
    pub new_version: u64,
}

/// Build the per-recipient `identity_rotation` event signed by the previous
/// priv (spec §12.2 step 4).
///
/// MUST be called on every active session — uniqueness via
/// `recipient_session_id` is what defeats replay between recipients.
pub fn build_identity_rotation_event(
    state: &ServerIdentityState,
    recipient_session_id: &[u8; 32],
) -> Result<IdentityRotationEvent> {
    let inner = state.inner.load();
    let previous = inner
        .previous
        .as_ref()
        .ok_or(Error::InvalidInput("no rotation in progress"))?;
    let transition_until_ns = inner
        .rotation_until_ns
        .ok_or(Error::InvalidInput("no rotation in progress"))?;
    let old_pub = previous.public_bytes();
    let new_pub = inner.current.public_bytes();

    let payload = build_rotate_event_input(
        &old_pub,
        &new_pub,
        transition_until_ns,
        recipient_session_id,
    );
    let signed_by_old = previous.sign(&payload);

    Ok(IdentityRotationEvent {
        old_pub,
        new_pub,
        transition_until_ns,
        recipient_session_id: *recipient_session_id,
        signed_by_old,
    })
}

/// Build the orphan-recovery payload for embedding into `auth_ok`
/// (spec §6.5 + diagram 05 Part B step 67).
///
/// Two signatures by previous_priv are produced:
///
/// 1. `identity_sig_previous` — over the **same byte-exact** `identity_input`
///    that the current keypair signed for the standard `identity_sig` field.
///    Diagram 05 Part B step 75 ("Verify identity_sig_previous против
///    previous_pub ✓") relies on this.
/// 2. `rotation_proof` — over the `(previous_pub, current_pub,
///    transition_until_ns)` chain so the client can attest that current_pub
///    was authorized by the previously-pinned key.
///
/// `identity_input` MUST be the bytes returned by
/// [`crate::common::identity::build_identity_input`] for the current handshake
/// — i.e. computed against `current_pub`, the same `auth_message`, the same
/// `session_id`, and the same `expires_at_ns` that the corresponding
/// `identity_sig` is signed over.
pub fn build_rotation_in_progress_payload(
    state: &ServerIdentityState,
    identity_input: &[u8],
) -> Result<RotationInProgressPayload> {
    let inner = state.inner.load();
    let previous = inner
        .previous
        .as_ref()
        .ok_or(Error::InvalidInput("no rotation in progress"))?;
    let transition_until_ns = inner
        .rotation_until_ns
        .ok_or(Error::InvalidInput("no rotation in progress"))?;
    let previous_pub = previous.public_bytes();
    let current_pub = inner.current.public_bytes();

    let identity_sig_previous = previous.sign(identity_input);

    let proof_payload =
        build_rotation_proof_input(&previous_pub, &current_pub, transition_until_ns);
    let rotation_proof = previous.sign(&proof_payload);

    Ok(RotationInProgressPayload {
        previous_pub,
        identity_sig_previous,
        transition_until_ns,
        rotation_proof,
    })
}

/// Build the challenge rotation chain from existing rotation-proof material.
/// The transcript is connection-independent and exactly reuses the canonical constructor.
pub fn build_challenge_rotation_payload(
    previous_signer: &Ed25519Keypair,
    current_pub: &[u8; 32],
    transition_until_ns: u64,
) -> crate::common::wire::ChallengeRotationPayload {
    let previous_pub = previous_signer.public_bytes();
    let input = build_rotation_proof_input(&previous_pub, current_pub, transition_until_ns);
    crate::common::wire::ChallengeRotationPayload {
        previous_pub,
        transition_until_ns,
        rotation_proof: previous_signer.sign(&input),
    }
}

// Wire views moved to common so the standalone `client` feature builds
// without server code (RV-CONNECT-FED6910E); paths preserved via re-export.
pub use crate::common::wire::{IdentityRotationEvent, RotationInProgressPayload};
