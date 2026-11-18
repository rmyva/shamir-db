//! §5.3 lost-response procedure on the REAL wire: the scripted transport
//! answers the status read, receives the single apply send, then drops the
//! response side (EOF). The client must report the outcome as typed
//! UNKNOWN carrying the attempt token — and must NOT resend the apply on
//! that connection (proven by the drained, empty peer buffer). A second
//! scripted client then answers exactly ONE status query for the same
//! migration id, and the journal's applied head resolves the attempt as
//! `Applied` — no replay ever happened.

use crate::schema_migration::{MigrationError, UnknownApplyResolution};

use super::harness::{
    bound, client_on, empty_journal_value, in_flight_value, is_status, migration_set,
    populated_status_value, read_request, respond_admin_payload, single_admin_op, with_in_flight,
    DB,
};
use shamir_query_types::admin::types::schema_migration::SchemaMigrationAdminOp;
// The three scripted-peer `.shutdown()` calls need the write-ext trait in
// scope.
use tokio::io::AsyncWriteExt;

/// The scripted peer: status answered, apply received, response side shut
/// (EOF), then a drained-buffer probe proving nothing else was ever sent.
#[tokio::test]
async fn rust_migration_lost_apply_response_queries_status_without_replay() {
    // The compiled set stays at ONE entry: the resolver must be able to
    // confirm the attempted entry from the reported prefix even when the
    // JOURNAL carries a later migration whose body the set never contained
    // (remote prefix proof is separate from whole-local-prefix readiness).
    let set = migration_set(&["mig_items"]);
    let checksums: Vec<[u8; 32]> = set.checksums().to_vec();
    let target = super::harness::target();
    let applied_checksum = checksums[0];

    let (client_to_server, mut server_reader) = tokio::io::duplex(64 * 1024);
    let (mut server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);

    let peer = tokio::spawn(async move {
        let mut seen = Vec::new();
        // 1. The preflight status read: answer with the EMPTY journal
        //    (generation-zero baseline).
        let (rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("peer reads the preflight status request");
        let (alias, op) = single_admin_op(&request);
        assert!(is_status(&op), "preflight is the typed status operation");
        seen.push(op);
        respond_admin_payload(&mut server_writer, rid, &alias, empty_journal_value()).await;
        // 2. The single apply send: receive it, then deterministically DROP
        //    the response side without answering.
        let (_rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("peer reads the apply request");
        let (_alias, op) = single_admin_op(&request);
        let SchemaMigrationAdminOp::ApplyApplicationSchemaMigration(apply_request) = &op else {
            panic!("expected the typed apply operation, got {op:?}")
        };
        assert_eq!(apply_request.migration.metadata.id, "mig_items");
        assert_eq!(apply_request.migration.metadata.ordinal, 1);
        assert_eq!(apply_request.migration.checksum, applied_checksum);
        assert_eq!(apply_request.expected_fence_generation, 0);
        seen.push(op);
        // EOF to the client: no response ever arrives.
        server_writer
            .shutdown()
            .await
            .expect("shut the response side");
        // Return the peer's remaining read half so the test can prove no
        // replayed (or otherwise buffered) send arrives after the EOF.
        (seen, server_reader)
    });

    let error = bound(client.apply_next(DB, target, &set))
        .await
        .expect_err("the lost apply response is a typed UNKNOWN outcome, never a retry");
    let MigrationError::UnknownOutcome { attempt, detail } = error else {
        panic!("typed unknown-outcome error, got {error:?}")
    };
    assert!(!detail.is_empty());
    assert_eq!(attempt.db(), DB);
    assert_eq!(attempt.target(), target);
    assert_eq!(attempt.migration_id(), "mig_items");
    assert_eq!(attempt.ordinal(), 1);
    assert_eq!(attempt.checksum(), applied_checksum);

    // The connection is finished (EOF): drop the client so its write half
    // closes, then prove the peer's buffer holds NO replayed apply (or any
    // other send): the read returns EOF, not another frame.
    drop(client);
    let (seen, mut server_reader) = bound(peer).await.expect("peer task");
    assert_eq!(
        seen.len(),
        2,
        "exactly one status + exactly one apply, in order"
    );
    assert!(
        bound(read_request(&mut server_reader)).await.is_none(),
        "no replayed apply and no unexpected buffered send after the EOF"
    );

    // 3. Resolution on a SECOND scripted client (the reconnect): exactly
    //    ONE status query for the same migration id, answered with a
    //    journal where a LATER migration (ordinal 2) is already applied —
    //    the attempt's entry is a NON-HEAD member of the reported applied
    //    prefix, and the new resolver resolves from that exact entry. The
    //    peer's read half is returned so the test can drain it after the
    //    caller returns: EOF with an empty buffer proves the resolver sent
    //    nothing beyond the single status query.
    let (resolve_to_server, mut resolve_reader) = tokio::io::duplex(64 * 1024);
    let (mut resolve_writer, resolve_client_reader) = tokio::io::duplex(64 * 1024);
    let resolver = client_on(resolve_client_reader, resolve_to_server);
    let later_applied = populated_status_value(
        &[
            ("mig_items", 1, applied_checksum),
            // A LATER migration applied by another client: the local set
            // has no body for it, only the reported remote entry.
            ("mig_audit", 2, [8u8; 32]),
        ],
        4,
        "valid",
    );
    let resolve_peer = tokio::spawn(async move {
        let mut seen = Vec::new();
        let (rid, request) = bound(read_request(&mut resolve_reader))
            .await
            .expect("resolver peer reads the status query");
        let (alias, op) = single_admin_op(&request);
        assert!(is_status(&op), "resolution is a read-only status query");
        seen.push(op);
        respond_admin_payload(&mut resolve_writer, rid, &alias, later_applied).await;
        (seen, resolve_reader)
    });

    let resolution = bound(resolver.resolve_unknown_apply(&attempt, &set))
        .await
        .expect("resolution succeeds");
    assert_eq!(
        resolution,
        UnknownApplyResolution::Applied {
            migration_id: String::from("mig_items"),
            ordinal: 1,
            checksum: applied_checksum,
        },
        "the attempt's EXACT applied entry resolves it as APPLIED, even as a non-head member"
    );
    let (seen, mut resolve_reader) = bound(resolve_peer).await.expect("resolver peer task");
    assert_eq!(seen.len(), 1, "exactly ONE status query for the resolution");
    drop(resolver);
    assert!(
        bound(read_request(&mut resolve_reader)).await.is_none(),
        "the resolver sent nothing beyond the single status query"
    );
}

/// A `ReadyToApply` resolution after a lost response: the journal head is
/// still BELOW the attempt's ordinal with no unresolved event, so the
/// attempt is provably absent and it is safe to call `apply_next` again
/// (the identical-body resume path).
#[tokio::test]
async fn rust_migration_lost_apply_resolves_ready_to_apply_from_absence() {
    let set = migration_set(&["mig_items"]);
    let target = super::harness::target();

    let (client_to_server, mut server_reader) = tokio::io::duplex(64 * 1024);
    let (mut server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let peer = tokio::spawn(async move {
        // Status answered (empty journal), apply received, response side
        // dropped without answering.
        let (rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("preflight status read");
        let (alias, op) = single_admin_op(&request);
        assert!(is_status(&op));
        respond_admin_payload(&mut server_writer, rid, &alias, empty_journal_value()).await;
        let (_rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("apply request");
        let (_alias, op) = single_admin_op(&request);
        assert!(matches!(
            op,
            SchemaMigrationAdminOp::ApplyApplicationSchemaMigration(_)
        ));
        server_writer
            .shutdown()
            .await
            .expect("drop the response side");
    });

    let error = bound(client.apply_next(DB, target, &set))
        .await
        .expect_err("unknown outcome after the dropped apply response");
    let MigrationError::UnknownOutcome { attempt, .. } = error else {
        panic!("typed unknown outcome, got {error:?}")
    };
    drop(client);
    bound(peer).await.expect("peer task");

    let (resolve_to_server, mut resolve_reader) = tokio::io::duplex(64 * 1024);
    let (mut resolve_writer, resolve_client_reader) = tokio::io::duplex(64 * 1024);
    let resolver = client_on(resolve_client_reader, resolve_to_server);
    let resolve_peer = tokio::spawn(async move {
        let (rid, request) = bound(read_request(&mut resolve_reader))
            .await
            .expect("resolution status read");
        let (alias, op) = single_admin_op(&request);
        assert!(is_status(&op), "resolution sends exactly one status query");
        respond_admin_payload(&mut resolve_writer, rid, &alias, empty_journal_value()).await;
    });
    let resolution = bound(resolver.resolve_unknown_apply(&attempt, &set))
        .await
        .expect("resolution succeeds");
    assert_eq!(resolution, UnknownApplyResolution::ReadyToApply);
    bound(resolve_peer).await.expect("resolver peer task");
    drop(resolver);
}

/// The resolver's in-doubt branch: after a lost apply response, the status
/// answer carries an unresolved event that MATCHES the attempt exactly
/// (same id, same ordinal) and is `in_doubt` — the typed resolution
/// carries the server's recorded reason and pending operation index,
/// after exactly ONE read-only status query (the apply is never resent).
#[tokio::test]
async fn rust_migration_resolver_reports_in_doubt_with_reason_for_matching_in_flight() {
    let set = migration_set(&["mig_items"]);
    let target = super::harness::target();

    // The attempt token comes from a real lost apply response.
    let (client_to_server, mut server_reader) = tokio::io::duplex(64 * 1024);
    let (mut server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let peer = tokio::spawn(async move {
        let (rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("preflight status read");
        let (alias, op) = single_admin_op(&request);
        assert!(is_status(&op));
        respond_admin_payload(&mut server_writer, rid, &alias, empty_journal_value()).await;
        let (_rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("apply request");
        let (_alias, op) = single_admin_op(&request);
        assert!(matches!(
            op,
            SchemaMigrationAdminOp::ApplyApplicationSchemaMigration(_)
        ));
        server_writer
            .shutdown()
            .await
            .expect("drop the response side");
    });
    let error = bound(client.apply_next(DB, target, &set))
        .await
        .expect_err("unknown outcome after the dropped apply response");
    let MigrationError::UnknownOutcome { attempt, .. } = error else {
        panic!("typed unknown outcome, got {error:?}")
    };
    drop(client);
    bound(peer).await.expect("peer task");

    // Resolution: the matching in-doubt event with reason and index.
    let (resolve_to_server, mut resolve_reader) = tokio::io::duplex(64 * 1024);
    let (mut resolve_writer, resolve_client_reader) = tokio::io::duplex(64 * 1024);
    let resolver = client_on(resolve_client_reader, resolve_to_server);
    let in_doubt_head = with_in_flight(
        empty_journal_value(),
        in_flight_value(
            "mig_items",
            1,
            Some(1),
            3,
            "in_doubt",
            Some("durability flush rejected"),
        ),
    );
    let resolve_peer = tokio::spawn(async move {
        let mut seen = Vec::new();
        let (rid, request) = bound(read_request(&mut resolve_reader))
            .await
            .expect("resolver peer reads the status query");
        let (alias, op) = single_admin_op(&request);
        assert!(is_status(&op), "resolution is a read-only status query");
        seen.push(op);
        respond_admin_payload(&mut resolve_writer, rid, &alias, in_doubt_head).await;
        seen
    });

    let resolution = bound(resolver.resolve_unknown_apply(&attempt, &set))
        .await
        .expect("resolution succeeds");
    assert_eq!(
        resolution,
        UnknownApplyResolution::InDoubt {
            operation_index: Some(1),
            reason: Some(String::from("durability flush rejected")),
            journal_status: "in_doubt".into(),
            fence_generation: 3,
        },
        "the matching in-doubt event resolves with reason and index"
    );
    let seen = bound(resolve_peer).await.expect("resolver peer task");
    assert_eq!(seen.len(), 1, "exactly ONE status query, nothing else sent");
    drop(resolver);
}
