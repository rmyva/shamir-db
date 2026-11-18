//! The status / plan / apply_next operations send the TYPED admin batch
//! operations of `shamir-query-builder` through the real framed duplex and
//! decode the ACTUAL emitted variant / metadata / ordinal / predecessor /
//! fence generation from the outbound frames. Initial generation zero is
//! covered (empty journal, reported fence 0, baseline expected fence 0)
//! and the multi-entry verified prefix feeds the FRESH apply: the typed
//! request pins the status-REPORTED top-level fence generation and the
//! last applied entry as predecessor. The resume territory is covered
//! from a matching in-doubt status (one applied head, typed in-doubt
//! error record, reported fence generation pinned on the single matching
//! apply).

use shamir_query_types::admin::types::schema_migration::{
    SchemaMigrationAdminOp, SchemaMigrationAppliedEntry,
};

use crate::schema_migration::{MigrationError, PrefixCoverage};

use super::harness::{
    apply_result_value, body, bound, client_on, empty_journal_value, head_value, in_flight_value,
    is_status, migration_set, old_head_value, plan_result_value, populated_status_value,
    predecessor_value, read_request, respond_admin_payload, single_admin_op, with_error,
    with_in_flight, DB,
};

/// One scripted peer: reads exactly the expected number of requests,
/// checks each decoded operation and answers with the fixture closure.
/// Returns the decoded operations in arrival order (an unexpected extra
/// request or a missing one fails the bounded join).
macro_rules! script_peer {
    ($s2c:expr, $c2s:expr, $($expect:pat => $answer:expr),* $(,)?) => {{
        tokio::spawn(async move {
            let mut writer = $s2c;
            let mut reader = $c2s;
            let mut seen = Vec::new();
            $(
                let (rid, request) = bound(read_request(&mut reader))
                    .await
                    .expect("scripted peer reads the next request");
                let (alias, op) = single_admin_op(&request);
                seen.push(op.clone());
                if let $expect = &op {
                    respond_admin_payload(&mut writer, rid, &alias, $answer).await;
                } else {
                    panic!("unexpected operation on the wire: {op:?}");
                }
            )*
            (seen, writer, reader)
        })
    }};
}

/// Happy path: explicit status, status then plan (predecessor guard),
/// status then apply — each call's typed request is decoded from the real
/// frame and each typed outcome decoded from the real response record.
/// The initial apply runs from an EMPTY journal: generation zero, the
/// baseline fence expectation — never a guessed generation.
#[tokio::test]
async fn rust_migration_status_plan_apply_send_typed_admin_operations() {
    let set = migration_set(&["mig_items", "mig_audit"]);
    let checksums: Vec<[u8; 32]> = set.checksums().to_vec();
    // Copy the needed checksums out of the Vec: spawned peer tasks need
    // 'static captures, and the Vec is asserted on after every peer join.
    let head_checksum = checksums[0];
    let next_checksum = checksums[1];
    let target = super::harness::target();
    let (client_to_server, server_reader) = tokio::io::duplex(64 * 1024);
    let (server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);

    // req1: the explicit status call.
    let peer = script_peer!(server_writer, server_reader,
        SchemaMigrationAdminOp::ApplicationSchemaStatus(_) => empty_journal_value(),
    );

    let status = bound(client.status(DB, target, &set))
        .await
        .expect("status succeeds");
    assert_eq!(status.reported_target, target);
    assert_eq!(status.last_applied_ordinal, 0);
    assert_eq!(
        status.coverage,
        crate::schema_migration::PrefixCoverage::Empty
    );
    assert!(status.in_flight.is_none());
    assert!(status.journal_checksum_verified());
    // The R1c step 3a additive fields on the EMPTY journal: the reported
    // fence generation is the baseline 0 and the (omitted) applied list
    // decodes as empty.
    assert_eq!(status.fence_generation, 0);
    assert!(status.applied.is_empty());

    let (seen, _, _) = bound(peer).await.expect("peer task");
    assert_eq!(seen.len(), 1);
    assert!(
        is_status(&seen[0]),
        "status sends the typed status operation"
    );

    // req2+req3: plan issues its own status read first, then the typed
    // plan operation with the empty-journal predecessor.
    let (client_to_server, server_reader2) = tokio::io::duplex(64 * 1024);
    let (server_writer2, client_reader2) = tokio::io::duplex(64 * 1024);
    let client2 = client_on(client_reader2, client_to_server);
    let next = body("mig_items", 1);
    let peer2 = script_peer!(
        server_writer2, server_reader2,
        SchemaMigrationAdminOp::ApplicationSchemaStatus(_) => empty_journal_value(),
        SchemaMigrationAdminOp::ApplicationSchemaPlan(_) => plan_result_value(
            &next, head_checksum, predecessor_value(None, 0, [0; 32]),
        ),
    );

    let plan = bound(client2.plan(DB, target, &set))
        .await
        .expect("plan succeeds");
    let (seen, server_writer2, server_reader2) = bound(peer2).await.expect("peer task");
    assert_eq!(seen.len(), 2);
    assert!(is_status(&seen[0]), "plan first reads the journal");
    let SchemaMigrationAdminOp::ApplicationSchemaPlan(plan_request) = &seen[1] else {
        panic!("second operation is the typed plan, got {:?}", seen[1])
    };
    assert_eq!(plan_request.protocol_version, 1);
    assert_eq!(plan_request.target, target);
    // Predecessor guard: the EMPTY journal's baseline predecessor.
    assert_eq!(plan_request.expected_predecessor.ordinal, 0);
    assert_eq!(plan_request.expected_predecessor.migration_id, None);
    assert_eq!(plan_request.expected_predecessor.checksum, [0; 32]);
    // The unapplied local bodies are submitted with their CACHED checksums.
    assert_eq!(plan_request.migrations.len(), 2);
    assert_eq!(plan_request.migrations[0].metadata.id, "mig_items");
    assert_eq!(plan_request.migrations[0].metadata.ordinal, 1);
    assert_eq!(plan_request.migrations[0].checksum, head_checksum);
    assert_eq!(plan_request.migrations[1].metadata.id, "mig_audit");
    assert_eq!(plan_request.migrations[1].checksum, next_checksum);
    // The decoded typed plan result.
    assert_eq!(plan.checksum, head_checksum);
    assert_eq!(plan.predecessor.ordinal, 0);
    assert_eq!(plan.operations.len(), 1);
    assert_eq!(plan.operations[0].operation_kind, "create_index");
    assert!(plan.warnings.is_empty());
    drop(server_writer2);
    drop(server_reader2);

    // req4+req5: apply_next issues its own status read first, then exactly
    // ONE apply with the baseline fence generation 0.
    let (client_to_server, server_reader3) = tokio::io::duplex(64 * 1024);
    let (server_writer3, client_reader3) = tokio::io::duplex(64 * 1024);
    let client3 = client_on(client_reader3, client_to_server);
    let applied_checksum = checksums[0];
    let peer3 = script_peer!(
        server_writer3, server_reader3,
        SchemaMigrationAdminOp::ApplicationSchemaStatus(_) => empty_journal_value(),
        SchemaMigrationAdminOp::ApplyApplicationSchemaMigration(_) => apply_result_value(
            "mig_items", 1, applied_checksum, "applied", Some(1),
        ),
    );

    let applied = bound(client3.apply_next(DB, target, &set))
        .await
        .expect("apply_next succeeds");
    let (seen, server_writer3, server_reader3) = bound(peer3).await.expect("peer task");
    assert_eq!(seen.len(), 2, "status read + exactly one apply");
    assert!(is_status(&seen[0]));
    let SchemaMigrationAdminOp::ApplyApplicationSchemaMigration(apply_request) = &seen[1] else {
        panic!("second operation is the typed apply, got {:?}", seen[1])
    };
    assert_eq!(apply_request.protocol_version, 1);
    assert_eq!(apply_request.target, target);
    // Initial generation zero: the empty journal's baseline expectation.
    assert_eq!(apply_request.expected_fence_generation, 0);
    assert_eq!(apply_request.expected_predecessor.ordinal, 0);
    assert_eq!(apply_request.expected_predecessor.migration_id, None);
    // The submitted body is the LOCAL body at the next ordinal with its
    // cached canonical checksum.
    assert_eq!(apply_request.migration.metadata.id, "mig_items");
    assert_eq!(apply_request.migration.metadata.ordinal, 1);
    assert_eq!(apply_request.migration.operations.len(), 1);
    assert_eq!(apply_request.migration.checksum, head_checksum);
    // The decoded typed outcome: the server reports the ACQUIRED fence
    // generation (the fresh first acquire), never the baseline 0.
    assert_eq!(applied.migration_id, "mig_items");
    assert_eq!(applied.ordinal, 1);
    assert_eq!(applied.checksum, head_checksum);
    assert_eq!(applied.status, "applied");
    assert_eq!(applied.fence_generation, Some(1));
    drop(server_writer3);
    drop(server_reader3);
    drop(client);
    drop(client2);
    drop(client3);
}

/// RESUME territory: the journal carries ONE applied head (ordinal 1,
/// verified) and the typed in-doubt error record for the matching
/// unresolved event at ordinal 2 with the REPORTED fence generation 3-ish.
/// `apply_next` is designed to resume: after exactly one status read it
/// sends exactly ONE matching apply that pins the reported fence
/// generation, the applied head as predecessor and the local body's cached
/// canonical checksum. The fixture reply reports the ACQUIRED fence (one
/// past the reported 3-ish), not the baseline.
#[tokio::test]
async fn rust_migration_resume_from_in_doubt_status_sends_one_matching_apply() {
    let set = migration_set(&["mig_items", "mig_audit"]);
    let checksums: Vec<[u8; 32]> = set.checksums().to_vec();
    let target = super::harness::target();
    let in_doubt_head = with_in_flight(
        with_error(
            head_value("mig_items", 1, checksums[0]),
            "schema_migration_in_doubt",
        ),
        in_flight_value(
            "mig_audit",
            2,
            Some(0),
            3,
            "in_doubt",
            Some("durability flush rejected"),
        ),
    );

    let (client_to_server, server_reader) = tokio::io::duplex(64 * 1024);
    let (server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let applied_checksum = checksums[1];
    let peer = script_peer!(
        server_writer, server_reader,
        SchemaMigrationAdminOp::ApplicationSchemaStatus(_) => in_doubt_head,
        SchemaMigrationAdminOp::ApplyApplicationSchemaMigration(_) => apply_result_value(
            "mig_audit", 2, applied_checksum, "applied", Some(4),
        ),
    );

    let applied = bound(client.apply_next(DB, target, &set))
        .await
        .expect("the resume apply succeeds");
    let (seen, server_writer, server_reader) = bound(peer).await.expect("peer task");
    assert_eq!(
        seen.len(),
        2,
        "one status read + exactly ONE matching apply"
    );
    assert!(is_status(&seen[0]));
    let SchemaMigrationAdminOp::ApplyApplicationSchemaMigration(apply_request) = &seen[1] else {
        panic!("second operation is the typed apply, got {:?}", seen[1])
    };
    assert_eq!(apply_request.protocol_version, 1);
    assert_eq!(apply_request.target, target);
    // The REPORTED fence generation of the unresolved event is pinned on
    // the resume apply — never guessed.
    assert_eq!(apply_request.expected_fence_generation, 3);
    // The applied head is the predecessor.
    assert_eq!(
        apply_request.expected_predecessor.migration_id.as_deref(),
        Some("mig_items")
    );
    assert_eq!(apply_request.expected_predecessor.ordinal, 1);
    assert_eq!(apply_request.expected_predecessor.checksum, checksums[0]);
    // The submitted body is the LOCAL body at the unresolved ordinal with
    // its cached canonical checksum (identical-body resume).
    assert_eq!(apply_request.migration.metadata.id, "mig_audit");
    assert_eq!(apply_request.migration.metadata.ordinal, 2);
    assert_eq!(apply_request.migration.checksum, checksums[1]);
    // The decoded typed outcome: the ACQUIRED fence (4), not the baseline.
    assert_eq!(applied.migration_id, "mig_audit");
    assert_eq!(applied.ordinal, 2);
    assert_eq!(applied.checksum, checksums[1]);
    assert_eq!(applied.status, "applied");
    assert_eq!(applied.fence_generation, Some(4));
    drop(server_writer);
    drop(server_reader);
    drop(client);
}

/// The multi-entry verified prefix: two applied entries in ordinal order
/// decode EXACTLY (typed id/ordinal/`Bin` checksum each), the coverage is
/// `Verified` at the head ordinal and the reported fence generation is
/// the exact top-level wire value.
#[tokio::test]
async fn rust_migration_multi_entry_verified_status_reports_exact_entries_and_generation() {
    let set = migration_set(&["mig_items", "mig_audit"]);
    let checksums: Vec<[u8; 32]> = set.checksums().to_vec();
    let target = super::harness::target();
    let (client_to_server, server_reader) = tokio::io::duplex(64 * 1024);
    let (server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let verified = populated_status_value(
        &[
            ("mig_items", 1, checksums[0]),
            ("mig_audit", 2, checksums[1]),
        ],
        4,
        "valid",
    );
    let peer = script_peer!(server_writer, server_reader,
        SchemaMigrationAdminOp::ApplicationSchemaStatus(_) => verified,
    );

    let status = bound(client.status(DB, target, &set))
        .await
        .expect("status succeeds");
    let (seen, server_writer, server_reader) = bound(peer).await.expect("peer task");
    assert_eq!(seen.len(), 1);
    assert_eq!(status.last_applied_ordinal, 2);
    assert_eq!(status.last_applied_id.as_deref(), Some("mig_audit"));
    assert_eq!(
        status.coverage,
        PrefixCoverage::Verified { ordinal: 2 },
        "the whole reported prefix matches the local set"
    );
    // Two applied entries realistically carry a durable fence of at least
    // 4 (each acquire and release advances it); the fixture reports 4.
    assert_eq!(status.fence_generation, 4);
    assert_eq!(
        status.applied,
        vec![
            SchemaMigrationAppliedEntry {
                id: String::from("mig_items"),
                ordinal: 1,
                checksum: checksums[0],
            },
            SchemaMigrationAppliedEntry {
                id: String::from("mig_audit"),
                ordinal: 2,
                checksum: checksums[1],
            },
        ],
        "every entry decodes exactly: id, ordinal and raw checksum bytes"
    );
    drop(server_writer);
    drop(server_reader);
    drop(client);
}

/// A FRESH next apply on top of a verified two-entry prefix uses the
/// status-REPORTED top-level fence generation verbatim (distinctive 8 —
/// never derived from the ordinal or the head), with the last applied
/// entry as the typed predecessor. The reply's ACQUIRED generation (9)
/// decodes as the typed outcome.
#[tokio::test]
async fn rust_migration_fresh_apply_pins_reported_fence_generation_and_last_entry_predecessor() {
    let set = migration_set(&["mig_items", "mig_audit", "mig_log"]);
    let checksums: Vec<[u8; 32]> = set.checksums().to_vec();
    // Copy the needed checksums out of the Vec: the spawned peer task needs
    // 'static captures, and the Vec is asserted on after the peer join.
    let next_checksum = checksums[2];
    let target = super::harness::target();
    let (client_to_server, server_reader) = tokio::io::duplex(64 * 1024);
    let (server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let verified = populated_status_value(
        &[
            ("mig_items", 1, checksums[0]),
            ("mig_audit", 2, checksums[1]),
        ],
        8,
        "valid",
    );
    let peer = script_peer!(
        server_writer, server_reader,
        SchemaMigrationAdminOp::ApplicationSchemaStatus(_) => verified,
        SchemaMigrationAdminOp::ApplyApplicationSchemaMigration(_) => apply_result_value(
            "mig_log", 3, next_checksum, "applied", Some(9),
        ),
    );

    let applied = bound(client.apply_next(DB, target, &set))
        .await
        .expect("the fresh apply succeeds");
    let (seen, server_writer, server_reader) = bound(peer).await.expect("peer task");
    assert_eq!(seen.len(), 2, "one status read + exactly one apply");
    let SchemaMigrationAdminOp::ApplyApplicationSchemaMigration(apply_request) = &seen[1] else {
        panic!("second operation is the typed apply, got {:?}", seen[1])
    };
    // The REPORTED top-level generation is used verbatim on the wire.
    assert_eq!(apply_request.expected_fence_generation, 8);
    // The predecessor is the last APPLIED entry, not the head-only guess.
    assert_eq!(
        apply_request.expected_predecessor.migration_id.as_deref(),
        Some("mig_audit")
    );
    assert_eq!(apply_request.expected_predecessor.ordinal, 2);
    assert_eq!(apply_request.expected_predecessor.checksum, checksums[1]);
    assert_eq!(apply_request.migration.metadata.id, "mig_log");
    assert_eq!(apply_request.migration.metadata.ordinal, 3);
    assert_eq!(apply_request.migration.checksum, next_checksum);
    assert_eq!(applied.fence_generation, Some(9), "the ACQUIRED generation");
    drop(server_writer);
    drop(server_reader);
    drop(client);
}

/// A populated journal whose head predates R1c step 3a (the explicit
/// compatibility fixture: `last_applied_*` present, BOTH `fence_generation`
/// and `applied` absent) is refused typed as unsupported-server — the client
/// can neither verify the full applied prefix nor start the next migration
/// without a reported generation, and it never guesses. Only the preflight
/// STATUS was ever sent: after dropping the client the peer's read half must
/// hit EOF, proving zero apply bytes (not merely zero scripted reads).
#[tokio::test]
async fn rust_migration_old_populated_head_without_new_fields_refuses_unsupported_server() {
    let set = migration_set(&["mig_items", "mig_audit"]);
    let target = super::harness::target();
    let (client_to_server, mut server_reader) = tokio::io::duplex(64 * 1024);
    let (mut server_writer, client_reader) = tokio::io::duplex(64 * 1024);
    let client = client_on(client_reader, client_to_server);
    let head = old_head_value("mig_items", 1, set.checksums()[0]);
    let peer = tokio::spawn(async move {
        let mut seen = Vec::new();
        let (rid, request) = bound(read_request(&mut server_reader))
            .await
            .expect("peer reads the preflight status request");
        let (alias, op) = single_admin_op(&request);
        assert!(
            is_status(&op),
            "the only send is the typed status operation"
        );
        seen.push(op);
        respond_admin_payload(&mut server_writer, rid, &alias, head).await;
        (seen, server_reader)
    });
    let error = bound(client.apply_next(DB, target, &set))
        .await
        .expect_err("an old populated journal cannot support this client");
    let MigrationError::UnsupportedServer { message } = &error else {
        panic!("typed unsupported-server refusal, got {error:?}")
    };
    assert!(
        message.contains("too old") || message.contains("full applied prefix"),
        "the refusal names the old-server cause: {message}"
    );
    // Drain AFTER the caller returned: dropping the client closes its write
    // half; the peer must observe EOF with NO extra buffered frame — zero
    // apply bytes ever reached the wire.
    drop(client);
    let (seen, mut server_reader) = bound(peer).await.expect("peer task");
    assert_eq!(seen.len(), 1, "one status read, no scripted apply");
    assert!(
        bound(read_request(&mut server_reader)).await.is_none(),
        "no apply or plan reached the wire after the unsupported-server refusal"
    );
    // `server_writer` was moved into the spawned peer and dropped there.
    drop(server_reader);
}
