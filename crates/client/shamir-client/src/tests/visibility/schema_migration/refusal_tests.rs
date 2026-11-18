//! Refusals that must happen BEFORE any write reaches the wire, and the
//! old-server unsupported-operation mapping on the real framed path.
//!
//! A changed checksum of an applied body (DRIFT), a journal head beyond
//! the local set (the set is SHORTER than the journal) and an unverified
//! persisted checksum each fail `apply_next` after exactly one read-only
//! status send — zero apply or plan sends. An invalid LOCAL set (a gap)
//! fails at construction, before any client exists, emitting zero bytes.

use shamir_query_types::admin::types::schema_migration::SchemaMigrationChecksumValidation;
use tokio::io::AsyncWriteExt;

use crate::schema_migration::{MigrationError, MigrationStatus, PrefixCoverage};

use super::harness::{
    body, bound, client_on, empty_journal_value, head_value, in_flight_value, is_status,
    migration_set, populated_status_value, read_request, respond_admin_payload, respond_db_error,
    respond_old_server_decoder_refusal, single_admin_op, with_in_flight, DB,
};
use shamir_query_types::admin::types::schema_migration::SchemaMigrationAdminOp;

/// The status fixture chain every refusal scenario answers with. After
/// the answer the peer drops nothing but probes its read half for EOF:
/// the test drops the client (closing the client's write half), and the
/// EOF-with-zero-buffered-bytes probe proves NO plan or apply was ever
/// sent after the mismatch.
async fn scripted_status_peer(
    mut server_writer: impl tokio::io::AsyncWrite + Unpin,
    mut server_reader: impl tokio::io::AsyncRead + Unpin,
    payload: shamir_types::types::value::QueryValue,
) {
    let (rid, request) = bound(read_request(&mut server_reader))
        .await
        .expect("peer reads the preflight status request");
    let (alias, op) = single_admin_op(&request);
    assert!(
        is_status(&op),
        "the only send is the typed status operation"
    );
    respond_admin_payload(&mut server_writer, rid, &alias, payload).await;
    // The client has been dropped by the test (its refusal happened before
    // any send): the peer must observe EOF with an EMPTY buffer — no
    // replayed, retried or otherwise unexpected send.
    assert!(
        bound(read_request(&mut server_reader)).await.is_none(),
        "no apply or plan reached the wire after the mismatch"
    );
}

/// Lowercase-hex of the drifted journal checksum the typed error carries.
fn drifted_hex() -> String {
    let mut hex = String::new();
    for byte in [9u8; 32] {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

/// A changed canonical checksum of the applied head body (DRIFT), a head
/// beyond the local set (shorter set), and an unverifiable persisted
/// checksum all refuse typed BEFORE any apply send; each consumed exactly
/// one status read and nothing else reached the wire.
#[tokio::test]
async fn rust_migration_changed_checksum_or_gap_fails_before_apply() {
    let set = migration_set(&["mig_items", "mig_audit"]);
    let local_checksum = set.checksums()[0];
    let target = super::harness::target();

    // Case 1 — changed checksum of the applied body: the journal head at
    // ordinal 1 carries the right id but a DIFFERENT checksum.
    let (client_to_server, server_reader) = tokio::io::duplex(64 * 1024);
    let (server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let drifted = head_value("mig_items", 1, [9u8; 32]);
    let peer = tokio::spawn(scripted_status_peer(server_writer, server_reader, drifted));

    let error = bound(client.apply_next(DB, target, &set))
        .await
        .expect_err("a changed checksum refuses before the apply");
    let MigrationError::Drift {
        ordinal,
        journal_id,
        journal_checksum,
        local_id,
    } = &error
    else {
        panic!("typed drift refusal, got {error:?}")
    };
    assert_eq!(*ordinal, 1);
    assert_eq!(journal_id, "mig_items");
    assert_eq!(journal_checksum, &drifted_hex());
    assert_eq!(local_id, "mig_items");
    drop(client);
    bound(peer)
        .await
        .expect("peer task proves nothing else was sent");

    // Case 2 — the journal head is BEYOND the local set (the compiled set
    // is shorter than the journal).
    let (client_to_server, server_reader) = tokio::io::duplex(64 * 1024);
    let (server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let beyond = populated_status_value(
        &[
            ("j1", 1, [1u8; 32]),
            ("j2", 2, [2u8; 32]),
            ("j3", 3, [3u8; 32]),
            ("j4", 4, [5u8; 32]),
            ("elsewhere", 5, [4u8; 32]),
        ],
        6,
        "valid",
    );
    let peer = tokio::spawn(scripted_status_peer(server_writer, server_reader, beyond));

    let error = bound(client.apply_next(DB, target, &set))
        .await
        .expect_err("a journal beyond the set refuses before the apply");
    let MigrationError::JournalBeyondSet { ordinal } = &error else {
        panic!("typed journal-beyond-set refusal, got {error:?}")
    };
    assert_eq!(*ordinal, 5);
    drop(client);
    bound(peer)
        .await
        .expect("peer task proves nothing else was sent");

    // Case 3 — the journal head matches locally, but the server could NOT
    // verify the persisted checksum (`checksum_validation: mismatch`).
    let (client_to_server, server_reader) = tokio::io::duplex(64 * 1024);
    let (server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    // Case 3 — the journal head matches locally, but the server could NOT
    // verify the persisted checksum (`checksum_validation: mismatch`); the
    // R1c step 3a fields still travel as a real server would emit them.
    let unverified = populated_status_value(&[("mig_items", 1, local_checksum)], 2, "mismatch");
    let peer = tokio::spawn(scripted_status_peer(
        server_writer,
        server_reader,
        unverified,
    ));

    let error = bound(client.apply_next(DB, target, &set))
        .await
        .expect_err("an unverifiable journal refuses before the apply");
    let MigrationError::JournalUnverified { validation } = &error else {
        panic!("typed journal-unverified refusal, got {error:?}")
    };
    assert_eq!(*validation, SchemaMigrationChecksumValidation::Mismatch);
    drop(client);
    bound(peer)
        .await
        .expect("peer task proves nothing else was sent");

    // Case 4 — an invalid LOCAL set (a gap) is rejected at construction:
    // no client, no connection, zero outbound bytes by construction.
    let gapped = vec![body("first", 1), body("third", 3)];
    assert!(
        crate::schema_migration::ApplicationMigrationSet::new(gapped).is_err(),
        "the gapped set never constructs, so nothing can ever be sent for it"
    );
}

/// The read-only `status` view of the drift, checked on the wire before
/// any write path runs: the journal head differs from the local body and
/// is reported as typed head drift; the unresolved-event evidence key
/// (`in_flight` with reason) decodes alongside when the server reports it.
#[tokio::test]
async fn rust_migration_status_reports_head_drift_and_in_flight_evidence() {
    let set = migration_set(&["mig_items", "mig_audit"]);
    let target = super::harness::target();
    let (client_to_server, server_reader) = tokio::io::duplex(64 * 1024);
    let (server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let drifted = with_in_flight(
        head_value("mig_items", 1, [9u8; 32]),
        in_flight_value("mig_audit", 2, Some(0), 3, "started", None),
    );
    let peer = tokio::spawn(scripted_status_peer(server_writer, server_reader, drifted));

    let status: MigrationStatus = bound(client.status(DB, target, &set))
        .await
        .expect("status succeeds");
    assert_eq!(
        status.coverage,
        PrefixCoverage::Drift {
            ordinal: 1,
            journal_id: String::from("mig_items"),
            journal_checksum: [9u8; 32],
            local_id: String::from("mig_items"),
        },
        "typed head drift with the journal checksum preserved as raw bytes"
    );
    // The SERVER verified its persisted checksum (`valid`); the drift is
    // the LOCAL body differing from it, not an unverifiable journal.
    assert!(status.journal_checksum_verified());
    let flight = status.in_flight.expect("the unresolved event decodes");
    assert_eq!(flight.migration_id, "mig_audit");
    assert_eq!(flight.ordinal, 2);
    assert_eq!(flight.operation_index, Some(0));
    assert_eq!(flight.fence_generation, 3);
    assert_eq!(flight.status, "started");
    assert_eq!(status.server_error, None);
    drop(client);
    bound(peer)
        .await
        .expect("peer task proves nothing else was sent");
}

/// Drift BELOW the head, found in the reported `applied` prefix: ordinal 1
/// carries a changed checksum while the head (ordinal 2) still matches and
/// ordinal 3 is locally available. `status` reports the typed drift at
/// ordinal 1 (the earliest divergence, not the head) and `apply_next`
/// refuses with the same ordinal after EXACTLY one read-only status send —
/// the drained peer buffer proves zero apply frames ever reached the wire.
#[tokio::test]
async fn rust_migration_prefix_drift_below_matching_head_refuses_with_zero_apply_frames() {
    let set = migration_set(&["mig_items", "mig_audit", "mig_log"]);
    let checksums: Vec<[u8; 32]> = set.checksums().to_vec();
    let target = super::harness::target();
    let drifted_prefix = populated_status_value(
        &[("mig_items", 1, [9u8; 32]), ("mig_audit", 2, checksums[1])],
        4,
        "valid",
    );

    // The read-only view: drift is reported at ordinal 1.
    let (client_to_server, server_reader) = tokio::io::duplex(64 * 1024);
    let (server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let peer = tokio::spawn(scripted_status_peer(
        server_writer,
        server_reader,
        drifted_prefix.clone(),
    ));
    let status: MigrationStatus = bound(client.status(DB, target, &set))
        .await
        .expect("status succeeds");
    assert_eq!(
        status.coverage,
        PrefixCoverage::Drift {
            ordinal: 1,
            journal_id: String::from("mig_items"),
            journal_checksum: [9u8; 32],
            local_id: String::from("mig_items"),
        },
        "the EARLIEST divergence is reported, even though the head matches"
    );
    assert_eq!(status.fence_generation, 4);
    assert_eq!(status.applied.len(), 2);
    drop(client);
    bound(peer)
        .await
        .expect("peer task proves nothing else was sent");

    // The write path refuses on the SAME ordinal, and the drained buffer
    // proves ZERO apply frames after the single preflight STATUS.
    let (client_to_server, server_reader) = tokio::io::duplex(64 * 1024);
    let (server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let peer = tokio::spawn(scripted_status_peer(
        server_writer,
        server_reader,
        drifted_prefix,
    ));
    let error = bound(client.apply_next(DB, target, &set))
        .await
        .expect_err("a prefix drift at ordinal 1 refuses before any apply");
    let MigrationError::Drift {
        ordinal,
        journal_id,
        journal_checksum,
        local_id,
    } = &error
    else {
        panic!("typed drift refusal, got {error:?}")
    };
    assert_eq!(*ordinal, 1);
    assert_eq!(journal_id, "mig_items");
    assert_eq!(journal_checksum, &drifted_hex());
    assert_eq!(local_id, "mig_items");
    drop(client);
    bound(peer)
        .await
        .expect("peer task proves zero apply frames were sent");
}

/// A MALFORMED `applied` list — a GAP in the reported ordinals (1 then 3,
/// ordinal 2 missing) whose head still matches the local set — is a shape
/// this client refuses typed before anything else is sent; the drained
/// peer buffer proves only the preflight STATUS reached the wire.
#[tokio::test]
async fn rust_migration_gapped_applied_list_is_a_typed_shape_refusal() {
    let set = migration_set(&["mig_items", "mig_audit", "mig_log"]);
    let checksums: Vec<[u8; 32]> = set.checksums().to_vec();
    let target = super::harness::target();
    let (client_to_server, server_reader) = tokio::io::duplex(64 * 1024);
    let (server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let gapped = populated_status_value(
        &[("mig_items", 1, checksums[0]), ("mig_log", 3, checksums[2])],
        8,
        "valid",
    );
    let peer = tokio::spawn(scripted_status_peer(
        server_writer,
        server_reader,
        gapped.clone(),
    ));

    // The read-only view classifies the gap EXACTLY: expected ordinal 2
    // (the first missing one after the proven prefix), actual ordinal 3.
    let status: MigrationStatus = bound(client.status(DB, target, &set))
        .await
        .expect("status succeeds");
    assert_eq!(
        status.coverage,
        PrefixCoverage::Gap {
            expected: 2,
            actual: 3,
        },
        "the gap is typed, never partially trusted"
    );
    drop(client);
    bound(peer)
        .await
        .expect("peer task proves the read-only view sent nothing else");

    // The write path refuses with the SAME typed branch, and the drained
    // buffer proves zero apply frames after the single preflight STATUS.
    let (client_to_server, server_reader) = tokio::io::duplex(64 * 1024);
    let (server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let peer = tokio::spawn(scripted_status_peer(server_writer, server_reader, gapped));
    let error = bound(client.apply_next(DB, target, &set))
        .await
        .expect_err("a gapped applied list is malformed, never partially trusted");
    let MigrationError::Gap { message } = &error else {
        panic!("typed gap refusal for the malformed prefix, got {error:?}")
    };
    assert!(!message.is_empty());
    drop(client);
    bound(peer)
        .await
        .expect("peer task proves only the preflight status was sent");
}

/// The old-server path on the REAL wire: the peer predates the typed
/// application-schema operation and its decoder refuses with the
/// `invalid_request` / `Unknown operation type` pair. The client surfaces
/// the distinct unsupported-server error — it never falls back to
/// unjournaled DDL and never retries. Each half runs a SPAWNED scripted
/// peer (read-then-answer is concurrent with the client call, never
/// sequenced before it) so the exchange cannot stall.
#[tokio::test]
async fn rust_migration_old_server_unknown_operation_maps_to_unsupported_server() {
    let set = migration_set(&["mig_items"]);
    let target = super::harness::target();

    // READ path: status is what the old peer refuses.
    let (client_to_server, mut server_reader) = tokio::io::duplex(64 * 1024);
    let (mut server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let peer = tokio::spawn(async move {
        let (rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("peer reads the status request");
        let (_alias, op) = single_admin_op(&request);
        assert!(
            is_status(&op),
            "the status operation is what the old peer refuses"
        );
        respond_old_server_decoder_refusal(&mut server_writer, rid).await;
    });

    let error = bound(client.status(DB, target, &set))
        .await
        .expect_err("the old server's refusal surfaces typed");
    let MigrationError::UnsupportedServer { message } = &error else {
        panic!("typed unsupported-server error, got {error:?}")
    };
    assert_eq!(
        message, "Unknown operation type",
        "decoder text preserved verbatim"
    );
    drop(client);
    bound(peer).await.expect("peer task");

    // WRITE path: apply_next's preflight status hits the same old-peer
    // refusal; unsupported-server is reported before any outcome could be
    // attributed to a send.
    let (client_to_server, mut server_reader) = tokio::io::duplex(64 * 1024);
    let (mut server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let peer = tokio::spawn(async move {
        let (rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("peer reads the apply path's first request");
        let (_alias, op) = single_admin_op(&request);
        assert!(is_status(&op));
        respond_old_server_decoder_refusal(&mut server_writer, rid).await;
    });

    let error = bound(client.apply_next(DB, target, &set))
        .await
        .expect_err("the old server's refusal surfaces typed on the apply path too");
    let MigrationError::UnsupportedServer { message } = &error else {
        panic!("typed unsupported-server error, got {error:?}")
    };
    assert_eq!(message, "Unknown operation type");
    drop(client);
    bound(peer).await.expect("peer task");
}

/// The REAL deferred response shape: a deferred admission inside a batch
/// surfaces as a `BatchError::QueryError` whose Display is
/// `Query '<alias>' failed: schema_migration_operation_deferred: <reason>`
/// carried with the coarse `query` code. The client recognizes the exact
/// wrapped prefix (code `query`), preserves the FULL server message in the
/// typed `DeferredOperation` and never treats it as an uncertain outcome
/// of a write.
#[tokio::test]
async fn rust_migration_wrapped_deferred_query_error_maps_to_deferred_operation() {
    let set = migration_set(&["mig_items"]);
    let target = super::harness::target();
    let (client_to_server, mut server_reader) = tokio::io::duplex(64 * 1024);
    let (mut server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let wrapped: &str = "Query 'application_schema_status' failed: \
         schema_migration_operation_deferred: admission backlog";
    let peer = tokio::spawn(async move {
        let (rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("peer reads the status request");
        let (_alias, op) = single_admin_op(&request);
        assert!(is_status(&op));
        respond_db_error(&mut server_writer, rid, "query", wrapped).await;
    });

    let error = bound(client.status(DB, target, &set))
        .await
        .expect_err("the deferred admission surfaces typed");
    let MigrationError::DeferredOperation { message } = &error else {
        panic!("typed deferred-operation error, got {error:?}")
    };
    assert_eq!(
        message, wrapped,
        "the FULL server message is preserved verbatim"
    );
    drop(client);
    bound(peer).await.expect("peer task");
}

/// The RAW deferred shape (no batch wrapper): the message itself starts
/// with the exact `schema_migration_operation_deferred:` prefix. A
/// `query`-coded message without that exact prefix is NOT deferred — it
/// stays a generic typed `Server` error.
#[tokio::test]
async fn rust_migration_deferred_prefix_recognized_raw_and_not_over_broad() {
    let set = migration_set(&["mig_items"]);
    let target = super::harness::target();

    // RAW prefix: no wrapper.
    let (client_to_server, mut server_reader) = tokio::io::duplex(64 * 1024);
    let (mut server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let raw: &str = "schema_migration_operation_deferred: admission backlog";
    let peer = tokio::spawn(async move {
        let (rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("peer reads the status request");
        let (_alias, op) = single_admin_op(&request);
        assert!(is_status(&op));
        respond_db_error(&mut server_writer, rid, "query", raw).await;
    });
    let error = bound(client.status(DB, target, &set))
        .await
        .expect_err("the raw deferred prefix surfaces typed");
    let MigrationError::DeferredOperation { message } = &error else {
        panic!("typed deferred-operation error, got {error:?}")
    };
    assert_eq!(message, raw);
    drop(client);
    bound(peer).await.expect("peer task");

    // NOT over-broad: a `query` code with the deferred text NOT in the
    // exact wrapped position stays a generic typed server error.
    let (client_to_server, mut server_reader) = tokio::io::duplex(64 * 1024);
    let (mut server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let unrelated: &str = "Query 'x' failed: unrelated failure mentioning deferred text";
    let peer = tokio::spawn(async move {
        let (rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("peer reads the status request");
        let (_alias, op) = single_admin_op(&request);
        assert!(is_status(&op));
        respond_db_error(&mut server_writer, rid, "query", unrelated).await;
    });
    let error = bound(client.status(DB, target, &set))
        .await
        .expect_err("a query-coded unrelated failure surfaces typed");
    let MigrationError::Server { code, message } = &error else {
        panic!("generic typed server error, got {error:?}")
    };
    assert_eq!(code, "query");
    assert_eq!(message, unrelated);
    drop(client);
    bound(peer).await.expect("peer task");
}

/// A post-apply `result_too_large` replacement response (the server can
/// replace an ALREADY-EXECUTED final response with this error) leaves the
/// apply outcome UNKNOWN: typed UNKNOWN carrying the intact error display,
/// and the peer's drained buffer proves NO replay was ever sent.
#[tokio::test]
async fn rust_migration_result_too_large_after_apply_is_unknown_outcome_without_replay() {
    let set = migration_set(&["mig_items"]);
    let checksums: Vec<[u8; 32]> = set.checksums().to_vec();
    let target = super::harness::target();
    let (client_to_server, mut server_reader) = tokio::io::duplex(64 * 1024);
    let (mut server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let peer = tokio::spawn(async move {
        // Preflight status: empty journal (generation-zero baseline).
        let (rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("peer reads the preflight status request");
        let (alias, op) = single_admin_op(&request);
        assert!(is_status(&op));
        respond_admin_payload(&mut server_writer, rid, &alias, empty_journal_value()).await;
        // The apply was RECEIVED and EXECUTED, but the final response was
        // replaced by the response-budget rejection.
        let (rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("peer reads the apply request");
        let (_alias, op) = single_admin_op(&request);
        assert!(matches!(
            op,
            SchemaMigrationAdminOp::ApplyApplicationSchemaMigration(_)
        ));
        respond_db_error(
            &mut server_writer,
            rid,
            "result_too_large",
            "encoded response is 1048577 bytes; maximum is 1048576",
        )
        .await;
        server_writer
            .shutdown()
            .await
            .expect("drop the response side");
        // Return the read half so the test can prove NO replay was ever
        // buffered: after the client is dropped the read must hit EOF.
        server_reader
    });

    let error = bound(client.apply_next(DB, target, &set))
        .await
        .expect_err("a replaced post-apply response leaves the outcome UNKNOWN");
    let MigrationError::UnknownOutcome { attempt, detail } = error else {
        panic!("typed unknown outcome, got {error:?}")
    };
    assert_eq!(attempt.migration_id(), "mig_items");
    assert_eq!(attempt.ordinal(), 1);
    assert_eq!(attempt.checksum(), checksums[0]);
    assert!(
        detail.contains("result_too_large"),
        "intact error display: {detail}"
    );
    drop(client);
    let mut server_reader = bound(peer).await.expect("peer task");
    assert!(
        bound(read_request(&mut server_reader)).await.is_none(),
        "no replayed apply and no unexpected buffered send after the EOF"
    );
}
