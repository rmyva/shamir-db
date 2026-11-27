//! Per-principal fairness of the refusal-audit queue.
//!
//! The share is charged per principal NAME, so two distinct principals can
//! never share one counter: `colliding_names` returns a pair that DID share a
//! counter under the removed hash-bucket scheme, and the test asserts that one
//! of them reaching its share cannot deny the other while global slots are
//! free.
//!
//! No assertion runs before the appender is released and every accepted job is
//! joined: a panic in the middle of the fill loop would otherwise leave up to
//! 32 blocking jobs parked until their own deadline.

use std::hash::BuildHasher;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use shamir_collections::TMap;
use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::audit_chain::{
    AuditAppender, AuditChain, AuditChainWriter, AuditEntry,
};
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_query_types::admin::activity::{CancelOutcome, RequestExecId};

use crate::db_handler::repo_audit::{
    enqueue_cancel_refusal_audit, enqueue_refused_read_audit, enqueue_typed_read_audit,
    CancelRefusalAudit, CANCEL_AUDIT_OBSERVABILITY_CAPACITY,
    CANCEL_AUDIT_OBSERVABILITY_PRINCIPAL_SHARE, CANCEL_AUDIT_PRINCIPAL_SHARE,
};

const TIMEOUT: Duration = Duration::from_secs(120);

fn session(name: &str, sid: u8) -> Session {
    let mut session = Session::new(
        [sid; 16],
        name.to_owned(),
        SessionPermissions::new(false, false, Vec::new()),
        TransportKind::Tcp,
        BindingMode::None,
        [0; 32],
        1_000,
    );
    session.session_id = [sid; 32];
    session
}

/// Two DISTINCT principal names that shared ONE counter under the removed
/// hash-bucket scheme (`THasher::hash_one(name) % 32`). With exact per-name
/// accounting they can no longer deny each other, and using such a pair keeps
/// a regression to bucket accounting visible.
fn colliding_names() -> (String, String) {
    // 33 names cannot fit 32 buckets (pigeonhole), so the loop always finds a
    // collision; `REMOVED_BUCKETS` is the width the old scheme used.
    const REMOVED_BUCKETS: u64 = 32;
    let mut seen: TMap<u64, String> = TMap::default();
    for index in 0..=REMOVED_BUCKETS as u32 {
        let name = format!("principal-{index}");
        let bucket =
            shamir_collections::THasher::default().hash_one(name.as_str()) as u64 % REMOVED_BUCKETS;
        if let Some(other) = seen.get(&bucket) {
            return (other.clone(), name);
        }
        seen.insert(bucket, name);
    }
    unreachable!("33 names cannot fit 32 buckets")
}

#[derive(Default)]
struct GatedAppender {
    gate: Mutex<GateState>,
    release: Condvar,
    recorded: Mutex<Vec<AuditEntry>>,
}

#[derive(Default)]
struct GateState {
    entered: usize,
    released: bool,
}

impl GatedAppender {
    /// Unblock every parked `append_entry`, including future calls.
    fn release_all(&self) {
        self.gate.lock().unwrap().released = true;
        self.release.notify_all();
    }

    fn entered(&self) -> usize {
        self.gate.lock().unwrap().entered
    }

    fn recorded(&self) -> Vec<AuditEntry> {
        self.recorded.lock().unwrap().clone()
    }
}

impl AuditAppender for GatedAppender {
    fn append_entry(&self, entry: &AuditEntry) -> Result<(), std::io::Error> {
        // Synchronous by trait: this runs inside the blocking audit job, so
        // the guard never crosses an `.await`.
        let mut gate = self.gate.lock().unwrap();
        gate.entered += 1;
        self.release.notify_all();
        while !gate.released {
            let (next, waited) = self
                .release
                .wait_timeout(gate, TIMEOUT)
                .map_err(|_| std::io::Error::other("gated appender mutex poisoned"))?;
            gate = next;
            if waited.timed_out() && !gate.released {
                return Err(std::io::Error::other("gated appender release timed out"));
            }
        }
        drop(gate);
        self.recorded.lock().unwrap().push(entry.clone());
        Ok(())
    }

    fn checkpoint(&self, _: u64, _: &[u8; 32]) -> Result<(), std::io::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn one_principal_cannot_exceed_its_share_while_another_is_recorded() {
    let (name_a, name_b) = colliding_names();
    let appender = Arc::new(GatedAppender::default());
    let writer = Arc::new(AuditChainWriter::new_with_shared(
        Arc::new(AuditChain::new([7; 32])),
        appender.clone(),
    ));
    let queue = Arc::new(CancelRefusalAudit::default());
    let principal_a = session(&name_a, 1);
    let principal_b = session(&name_b, 2);

    // A fill loop that never asserts: the release below must be reachable on
    // every path.
    let mut accepted = Vec::with_capacity(CANCEL_AUDIT_PRINCIPAL_SHARE + 2);
    let mut admitted_a = 0;
    for target in 0..CANCEL_AUDIT_PRINCIPAL_SHARE as u64 {
        let job = enqueue_cancel_refusal_audit(
            &queue,
            &writer,
            &principal_a,
            RequestExecId(target),
            &CancelOutcome::NotCancellable,
        );
        if let Some(job) = job {
            admitted_a += 1;
            accepted.push(job);
        }
    }
    let over_share = enqueue_cancel_refusal_audit(
        &queue,
        &writer,
        &principal_a,
        RequestExecId(u64::MAX),
        &CancelOutcome::NotCancellable,
    );
    let over_share_refused = over_share.is_none();
    if let Some(job) = over_share {
        accepted.push(job);
    }
    let from_other = enqueue_cancel_refusal_audit(
        &queue,
        &writer,
        &principal_b,
        RequestExecId(u64::MAX),
        &CancelOutcome::NotCancellable,
    );
    let other_accepted = from_other.is_some();
    if let Some(job) = from_other {
        accepted.push(job);
    }
    let drops = queue.dropped_count();

    // Release unconditionally: an assertion must never be able to leave a
    // blocking append parked until its own deadline.
    appender.release_all();
    let accepted_count = accepted.len();
    for job in accepted {
        tokio::time::timeout(TIMEOUT, job)
            .await
            .expect("accepted refusal audit job timed out")
            .unwrap();
    }
    let entered = appender.entered();

    assert_eq!(
        admitted_a, CANCEL_AUDIT_PRINCIPAL_SHARE,
        "every refusal inside the principal share must be admitted"
    );
    assert!(
        over_share_refused,
        "principal A exceeded its share of the audit queue"
    );
    assert!(
        other_accepted,
        "principal B was denied by principal A's share: these two names shared one counter under \
         the removed bucket scheme, and exact per-name accounting must not"
    );
    assert_eq!(
        drops, 1,
        "exactly the over-share refusal is counted as a dropped event"
    );
    assert_eq!(accepted_count, CANCEL_AUDIT_PRINCIPAL_SHARE + 1);
    assert_eq!(entered, CANCEL_AUDIT_PRINCIPAL_SHARE + 1);

    let recorded = appender.recorded();
    assert_eq!(recorded.len(), CANCEL_AUDIT_PRINCIPAL_SHARE + 1);
    let count_for = |principal: &Session| {
        recorded
            .iter()
            .filter(|entry| entry.user == principal.username)
            .count()
    };
    assert_eq!(count_for(&principal_a), CANCEL_AUDIT_PRINCIPAL_SHARE);
    assert_eq!(count_for(&principal_b), 1);
    let recorded_principals: shamir_collections::TSet<&str> =
        recorded.iter().map(|entry| entry.user.as_str()).collect();
    assert_eq!(recorded_principals.len(), 2);
    assert!(recorded_principals.contains(principal_a.username.as_str()));
    assert!(recorded_principals.contains(principal_b.username.as_str()));

    // Every slot above was released when its job finished, so principal A is
    // admissible again. A counter that never decrements (or that keeps A's
    // share charged forever) refuses here.
    let again = enqueue_cancel_refusal_audit(
        &queue,
        &writer,
        &principal_a,
        RequestExecId(u64::MAX - 1),
        &CancelOutcome::NotCancellable,
    );
    let re_admitted = again.is_some();
    if let Some(job) = again {
        let _ = tokio::time::timeout(TIMEOUT, job).await;
    }
    assert!(
        re_admitted,
        "a finished refusal must release its share slot for the same principal"
    );
}

/// BLOCKED-APPENDER FLOOD, per principal.
///
/// The per-principal observability allowance (three quarters of the share) is
/// not the principal's whole share: after a principal's read audits fill their
/// allowance, a security event for that SAME principal must still be admitted
/// and appended once the barrier is released. The extra observability probe
/// pins the reserved boundary: pre-change the queue had one counter per
/// principal and admitted it, so `further_refused` fails.
#[tokio::test]
async fn observability_flood_cannot_drop_a_security_event_for_the_same_principal() {
    let appender = Arc::new(GatedAppender::default());
    let writer = Arc::new(AuditChainWriter::new_with_shared(
        Arc::new(AuditChain::new([13; 32])),
        appender.clone(),
    ));
    let queue = Arc::new(CancelRefusalAudit::default());
    let principal = session("principal-shared-reads", 4);

    let mut accepted = Vec::new();
    let mut admitted_observability = 0usize;
    for _ in 0..CANCEL_AUDIT_OBSERVABILITY_PRINCIPAL_SHARE as u64 {
        let job = enqueue_typed_read_audit(
            &queue,
            &writer,
            &principal,
            "status",
            "app",
            Some(("repo", "table")),
        );
        if let Some(job) = job {
            admitted_observability += 1;
            accepted.push(job);
        }
    }

    let security = enqueue_cancel_refusal_audit(
        &queue,
        &writer,
        &principal,
        RequestExecId(u64::MAX),
        &CancelOutcome::NotCancellable,
    );
    let security_admitted = security.is_some();
    if let Some(job) = security {
        accepted.push(job);
    }

    // One more read audit for the SAME principal must be refused while the
    // principal is still below its FULL share: the observability allowance is
    // the reserved boundary. Pre-change this submission was admitted.
    let further = enqueue_typed_read_audit(&queue, &writer, &principal, "status", "app", None);
    let further_refused = further.is_none();
    if let Some(job) = further {
        accepted.push(job);
    }

    // Release unconditionally before asserting: a panic must not leave a
    // blocking append parked until its own deadline.
    appender.release_all();
    for job in accepted {
        tokio::time::timeout(TIMEOUT, job)
            .await
            .expect("accepted audit job timed out")
            .unwrap();
    }

    assert_eq!(
        admitted_observability, CANCEL_AUDIT_OBSERVABILITY_PRINCIPAL_SHARE,
        "every observability job inside the per-principal allowance must be admitted"
    );
    assert!(
        security_admitted,
        "a security event for the flooded principal must use the reserved headroom"
    );
    assert!(
        further_refused,
        "the observability allowance must not grow into the security headroom"
    );
    let recorded = appender.recorded();
    assert_eq!(
        recorded
            .iter()
            .filter(|entry| entry.user == principal.username && entry.event == "cancel_request")
            .count(),
        1,
        "the same principal's security event is appended exactly once after the barrier"
    );
}

/// BLOCKED-APPENDER FLOOD, global.
///
/// Read audits from many distinct principals fill only the observability part
/// of the global cap (three quarters). A security event from ANOTHER principal
/// must still be admitted, and one further observability submission must be
/// refused once that part is exhausted. Pre-change the global counter had no
/// class split, so the extra observability submission was admitted and
/// `extra_refused` fails.
#[tokio::test]
async fn observability_flood_cannot_drop_a_security_event_from_another_principal() {
    let appender = Arc::new(GatedAppender::default());
    let writer = Arc::new(AuditChainWriter::new_with_shared(
        Arc::new(AuditChain::new([17; 32])),
        appender.clone(),
    ));
    let queue = Arc::new(CancelRefusalAudit::default());

    let mut accepted = Vec::new();
    let mut admitted_observability = 0usize;
    for index in 0..CANCEL_AUDIT_OBSERVABILITY_CAPACITY {
        let name = format!("observability-{index}");
        let principal = session(&name, (index % 251) as u8);
        let job = enqueue_typed_read_audit(&queue, &writer, &principal, "get_record", "app", None);
        if let Some(job) = job {
            admitted_observability += 1;
            accepted.push(job);
        }
    }

    let security_principal = session("security-outsider", 250);
    let security = enqueue_cancel_refusal_audit(
        &queue,
        &writer,
        &security_principal,
        RequestExecId(1),
        &CancelOutcome::NotCancellable,
    );
    let security_admitted = security.is_some();
    if let Some(job) = security {
        accepted.push(job);
    }

    let extra_principal = session("observability-overflow", 249);
    let extra = enqueue_typed_read_audit(&queue, &writer, &extra_principal, "status", "app", None);
    let extra_refused = extra.is_none();
    if let Some(job) = extra {
        accepted.push(job);
    }

    appender.release_all();
    for job in accepted {
        tokio::time::timeout(TIMEOUT, job)
            .await
            .expect("accepted audit job timed out")
            .unwrap();
    }

    assert_eq!(
        admitted_observability, CANCEL_AUDIT_OBSERVABILITY_CAPACITY,
        "every observability job inside the global allowance must be admitted"
    );
    assert!(
        security_admitted,
        "a security event from another principal must use the reserved global headroom"
    );
    assert!(
        extra_refused,
        "the global observability allowance must be exhausted before the full cap"
    );
    let recorded = appender.recorded();
    assert_eq!(
        recorded
            .iter()
            .filter(|entry| entry.user == security_principal.username)
            .count(),
        1,
        "the other principal's security event is appended exactly once"
    );
}

/// DROP ACCOUNTING.
///
/// A typed-read job whose clock cannot be read never reserves a slot and must
/// still increment the queue's drop counter. The clock is driven through the
/// `fail_clock` test seam (`UnixNanos::try_now` cannot be made to fail without
/// one); the encoding branch records the same drop and is not separately
/// exercised because the names-only payload cannot fail to encode.
#[tokio::test]
async fn typed_read_clock_failure_is_counted_as_a_dropped_job() {
    let appender = Arc::new(GatedAppender::default());
    let writer = Arc::new(AuditChainWriter::new_with_shared(
        Arc::new(AuditChain::new([19; 32])),
        appender.clone(),
    ));
    let queue = Arc::new(CancelRefusalAudit::default());
    let principal = session("clock-failure", 6);

    queue.fail_clock.store(true, Ordering::SeqCst);
    let dropped = enqueue_typed_read_audit(&queue, &writer, &principal, "status", "app", None);
    assert!(
        dropped.is_none(),
        "a clock failure must not admit an observability job"
    );
    assert_eq!(
        queue.dropped_count(),
        1,
        "a clock-failed observability job must be counted as a dropped event"
    );

    // Clock failure precedes reservation. After clearing the seam, fill
    // precisely the per-principal allowance while the appender is gated.
    queue.fail_clock.store(false, Ordering::SeqCst);
    let mut accepted = Vec::new();
    let mut admitted = 0usize;
    for _ in 0..CANCEL_AUDIT_OBSERVABILITY_PRINCIPAL_SHARE {
        let job = enqueue_typed_read_audit(&queue, &writer, &principal, "status", "app", None);
        if let Some(job) = job {
            admitted += 1;
            accepted.push(job);
        }
    }
    let over_principal_share =
        enqueue_typed_read_audit(&queue, &writer, &principal, "status", "app", None);
    let principal_refused = over_principal_share.is_none();
    if let Some(job) = over_principal_share {
        accepted.push(job);
    }

    // Use the full remaining per-principal allowance, then enough distinct
    // principals to reach the exact global observability boundary as well.
    let mut global_accepted = Vec::new();
    let global_slots_needed = CANCEL_AUDIT_OBSERVABILITY_CAPACITY - admitted;
    for index in 0..global_slots_needed {
        let other = session(&format!("clock-global-{index}"), (index % 251) as u8);
        let job = enqueue_typed_read_audit(&queue, &writer, &other, "status", "app", None);
        if let Some(job) = job {
            global_accepted.push(job);
        }
    }
    let global_overflow = session("clock-global-overflow", 251);
    let global_refused =
        enqueue_typed_read_audit(&queue, &writer, &global_overflow, "status", "app", None)
            .is_none();

    let global_accepted_count = global_accepted.len();
    appender.release_all();
    for job in accepted.into_iter().chain(global_accepted) {
        tokio::time::timeout(TIMEOUT, job)
            .await
            .expect("admitted quota audit job timed out")
            .unwrap();
    }
    assert_eq!(admitted, CANCEL_AUDIT_OBSERVABILITY_PRINCIPAL_SHARE);
    assert!(
        principal_refused,
        "per-principal observability quota must stop at 24"
    );
    assert_eq!(global_accepted_count, global_slots_needed);
    assert!(
        global_refused,
        "global observability quota must stop at 192"
    );
    assert_eq!(
        queue.dropped_count(),
        3,
        "clock failure plus both quota refusals are the only drops"
    );
}

/// BLOCKED-APPENDER FLOOD, refused-read security events.
///
/// A single principal can fill only its own share: another principal's
/// refused read must still be admitted while global queue capacity remains.
/// This discriminates per-principal accounting from sharing another
/// principal's Security-event share.
#[tokio::test]
async fn refused_read_flood_cannot_consume_another_principals_share() {
    let appender = Arc::new(GatedAppender::default());
    let writer = Arc::new(AuditChainWriter::new_with_shared(
        Arc::new(AuditChain::new([23; 32])),
        appender.clone(),
    ));
    let queue = Arc::new(CancelRefusalAudit::default());
    let principal_a = session("refused-read-flood", 8);
    let principal_b = session("refused-read-outsider", 9);

    let mut accepted = Vec::new();
    let mut admitted_a = 0usize;
    for _ in 0..CANCEL_AUDIT_PRINCIPAL_SHARE {
        let job = enqueue_refused_read_audit(
            &queue,
            &writer,
            &principal_a,
            "get_record",
            ("app", "repo", "table"),
            "not_found",
        );
        if let Some(job) = job {
            admitted_a += 1;
            accepted.push(job);
        }
    }
    let over_share = enqueue_refused_read_audit(
        &queue,
        &writer,
        &principal_a,
        "get_record",
        ("app", "repo", "table"),
        "not_found",
    );
    let over_share_refused = over_share.is_none();
    if let Some(job) = over_share {
        accepted.push(job);
    }
    let from_other = enqueue_refused_read_audit(
        &queue,
        &writer,
        &principal_b,
        "get_record",
        ("app", "repo", "table"),
        "not_found",
    );
    let other_accepted = from_other.is_some();
    if let Some(job) = from_other {
        accepted.push(job);
    }
    let drops = queue.dropped_count();

    appender.release_all();
    for job in accepted {
        tokio::time::timeout(TIMEOUT, job)
            .await
            .expect("accepted refused-read audit job timed out")
            .unwrap();
    }

    assert_eq!(admitted_a, CANCEL_AUDIT_PRINCIPAL_SHARE);
    assert!(over_share_refused, "principal A exceeded its queue share");
    assert!(
        other_accepted,
        "principal A must not consume principal B's Security-event share"
    );
    assert_eq!(
        drops, 1,
        "the over-share refusal is counted as a dropped event"
    );
    let recorded = appender.recorded();
    assert_eq!(recorded.len(), CANCEL_AUDIT_PRINCIPAL_SHARE + 1);
    assert_eq!(
        recorded
            .iter()
            .filter(|entry| entry.user == principal_a.username)
            .count(),
        CANCEL_AUDIT_PRINCIPAL_SHARE
    );
    assert_eq!(
        recorded
            .iter()
            .filter(|entry| entry.user == principal_b.username)
            .count(),
        1
    );
}
