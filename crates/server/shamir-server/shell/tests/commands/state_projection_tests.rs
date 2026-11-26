//! Unit regression for the `data head` / `data count` / `replication
//! subscriptions` projection seams (admin-shell PR3, §4.2, §4.4): the
//! projected-row BYTE cap, the shared MONOTONIC deadline, the count-path
//! statistics propagation and the typed subscription identity shapes are
//! exercised through the deterministic seam — `project_head` takes its
//! deadline as a parameter, so the wall-time refusal is driven by an
//! ALREADY-EXPIRED deadline and the passing path by one far in the
//! future. No sleeps, no tokio runtime, no wall-clock thresholds: the
//! only clock read is the monotonic `tokio::time::Instant::now` that
//! builds the seam deadlines.

use std::time::Duration;

use num_bigint::BigInt;
use shamir_query_types::read::{PaginationInfo, QueryRecord, QueryResult, QueryStats};
use shamir_types::types::value::QueryValue;

use super::super::state::{
    head_error, head_tail, head_tail_with_now, is_optional_version, project_count, project_head,
    project_subscriptions,
};
use crate::shell::output::ShellError;

/// A direct map record with one large string field of `chars` characters.
fn big_row(chars: usize) -> QueryRecord {
    let mut fields = shamir_collections::new_map();
    fields.insert(String::from("payload"), QueryValue::Str("x".repeat(chars)));
    QueryRecord::Direct(QueryValue::Map(fields))
}

fn result_with(records: Vec<QueryRecord>) -> QueryResult {
    QueryResult {
        records,
        ..QueryResult::default()
    }
}

fn pagination(has_next: bool) -> PaginationInfo {
    PaginationInfo {
        total_count: None,
        total_pages: None,
        current_page: None,
        page_size: None,
        has_next,
        has_prev: false,
    }
}

/// The FIRST row that would overflow the byte cap stops the projection
/// and sets `bytes_truncated`; earlier rows stay in `rows` and the
/// row-truncation markers stay untouched by the byte cap.
#[test]
fn head_byte_cap_stops_projection_with_marker() {
    // Two ~150 KiB rows: the first fits, the second cannot (256 KiB cap).
    let result = result_with(vec![big_row(150 * 1024), big_row(150 * 1024)]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let out = project_head(&result, 2, 2, deadline).expect("projection succeeds");
    assert_eq!(
        out["rows"].as_array().map(Vec::len),
        Some(1),
        "exactly the rows that fit must be projected"
    );
    assert_eq!(out["bytes_truncated"].as_bool(), Some(true));
    assert_eq!(out["rows_truncated"].as_bool(), Some(false));
    assert!(out.get("rows_capped").is_none());
    assert_eq!(out["rows_returned"].as_str(), Some("1"));
}

/// An expired deadline (deterministic seam, no sleep) is the typed
/// `head_wall_time` outcome-unknown refusal — even with zero rows, the
/// projection never emits a partial success.
#[test]
fn head_expired_deadline_is_head_wall_time() {
    let result = result_with(vec![big_row(16)]);
    let expired = tokio::time::Instant::now() - Duration::from_secs(1);
    let error = project_head(&result, 1, 1, expired).expect_err("expired deadline refuses");
    assert_eq!(error, ShellError::HeadWallTime);
}

/// A row nested deeper than the projection depth bound (`output.rs`
/// `MAX_PROJECTION_DEPTH`) — the deterministic per-row PROJECTION error.
fn too_deep_row() -> QueryRecord {
    let mut nested = QueryValue::Null;
    for _ in 0..40 {
        nested = QueryValue::List(vec![nested]);
    }
    QueryRecord::Direct(nested)
}

/// An already-elapsed `data head` deadline wins over a per-row projection
/// refusal: the same row that refuses `projection_limit` under a live
/// budget reports the typed `head_wall_time` (exit 7) under an elapsed one,
/// never a misleading exit 5.
#[test]
fn truncated_head_reports_the_deadline_not_the_projection_error() {
    let result = result_with(vec![too_deep_row()]);
    // Control: the row really does force a projection refusal with a live
    // budget, so the assertion below is about the deadline winning.
    let live = tokio::time::Instant::now() + Duration::from_secs(60);
    assert_eq!(
        project_head(&result, 1, 1, live).expect_err("depth overrun is refused"),
        ShellError::ProjectionLimit
    );
    let expired = tokio::time::Instant::now() - Duration::from_secs(1);
    assert_eq!(
        project_head(&result, 1, 1, expired).expect_err("expired deadline refuses"),
        ShellError::HeadWallTime,
        "an elapsed budget must win over the per-row projection error"
    );
}

/// The discriminating rule of [`head_error`]: an elapsed deadline maps ANY
/// per-row error to `HeadWallTime`, while a live one keeps the original
/// refusal.
#[test]
fn head_error_prefers_an_elapsed_deadline() {
    assert_eq!(
        head_error(tokio::time::Instant::now(), ShellError::Server),
        ShellError::HeadWallTime
    );
    let live = tokio::time::Instant::now() + Duration::from_secs(5);
    assert_eq!(
        head_error(live, ShellError::ProjectionLimit),
        ShellError::ProjectionLimit
    );
}

/// The `data head` tail runs the SYNCHRONOUS credential scan and only then
/// consults the shared deadline: an already-elapsed budget is
/// `HeadWallTime` whether the scan came back clean or refused, and it also
/// wins over the projection result itself; a live budget preserves both
/// the clean payload and the scan refusal.
#[test]
fn head_tail_checks_the_deadline_after_the_credential_scan() {
    let expired = tokio::time::Instant::now() - Duration::from_secs(1);
    let live = tokio::time::Instant::now() + Duration::from_secs(60);
    let payload = serde_json::json!({ "rows": [] });

    assert_eq!(
        head_tail(expired, Ok(payload.clone()), |_| Ok(())),
        Err(ShellError::HeadWallTime),
        "an elapsed budget refuses even a clean scan"
    );
    assert_eq!(
        head_tail(expired, Ok(payload.clone()), |_| Err(ShellError::Server)),
        Err(ShellError::HeadWallTime),
        "an elapsed budget wins over a refusing scan"
    );
    assert_eq!(
        head_tail(expired, Err(ShellError::Server), |_| Ok(())),
        Err(ShellError::HeadWallTime),
        "an elapsed budget wins over the projection result"
    );
    assert_eq!(
        head_tail(live, Ok(payload.clone()), |_| Ok(())),
        Ok(payload.clone())
    );
    assert_eq!(
        head_tail(live, Ok(payload), |_| Err(ShellError::Server)),
        Err(ShellError::Server),
        "a live budget keeps the scan refusal"
    );
}

/// The oracle for the ORDERING, driven by an INJECTED clock instead of a
/// sleep. The clock is a `Cell` that only the SCAN flips: before the scan
/// `now()` returns `t0` (live 10 ms budget), after it `t0 + 40ms` (past the
/// budget), so a clean scan must still refuse with the typed
/// `head_wall_time`. Kills two mutations: "post-scan check removed" — the
/// refusal is gone and the payload comes back; and "check moved before the
/// scan" — the only read happens pre-scan, sees a live budget, and the
/// payload comes back. The negative control below (a scan that does NOT
/// flip the cell must return the payload) keeps the seam honest, so the
/// test cannot pass by refusing unconditionally.
#[test]
fn head_tail_rechecks_the_deadline_after_the_scan() {
    let t0 = tokio::time::Instant::now();
    let deadline = t0 + Duration::from_millis(10);
    let payload = serde_json::json!({ "rows": [] });
    let expired = std::cell::Cell::new(false);
    let now = || {
        if expired.get() {
            t0 + Duration::from_millis(40)
        } else {
            t0
        }
    };
    let out = head_tail_with_now(
        deadline,
        Ok(payload.clone()),
        |_| {
            expired.set(true);
            Ok(())
        },
        &now,
    );
    assert_eq!(
        out,
        Err(ShellError::HeadWallTime),
        "an expiry crossed by the scan must beat a clean scan result"
    );
    // Negative control: same seam, but the scan never flips the cell, so
    // the budget stays live and the payload must come back.
    let expired = std::cell::Cell::new(false);
    let now = || {
        if expired.get() {
            t0 + Duration::from_millis(40)
        } else {
            t0
        }
    };
    let out = head_tail_with_now(deadline, Ok(payload.clone()), |_| Ok(()), &now);
    assert_eq!(out, Ok(payload), "a live budget must return the payload");
}

/// A deadline far in the future (deterministic seam) lets a normal small
/// result complete with both budget markers false.
#[test]
fn head_future_deadline_completes_with_clean_markers() {
    let result = result_with(vec![big_row(16)]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3600);
    let out = project_head(&result, 1, 1, deadline).expect("projection succeeds");
    assert_eq!(out["rows"].as_array().map(Vec::len), Some(1));
    assert_eq!(out["bytes_truncated"].as_bool(), Some(false));
    assert_eq!(out["rows_truncated"].as_bool(), Some(false));
}

/// `rows_truncated` requires BOTH a page that says more rows exist AND a
/// request above the hard row cap; `rows_capped` records pure clamping.
/// A dishonest peer's extra rows are dropped by the limit take.
#[test]
fn head_row_markers_follow_request_cap_and_pagination() {
    // Peer returns MORE records than the limit; the projection takes
    // exactly `limit` and never marks row truncation without the cap.
    let result = result_with((0..7).map(|_| big_row(4)).collect());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let out = project_head(&result, 5, 5, deadline).expect("projection succeeds");
    assert_eq!(out["rows"].as_array().map(Vec::len), Some(5));
    assert_eq!(out["rows_truncated"].as_bool(), Some(false));
    assert!(out.get("rows_capped").is_none());

    // Above the hard cap with a `has_next` page: both markers fire.
    let capped = QueryResult {
        pagination: Some(pagination(true)),
        ..result_with((0..100).map(|_| big_row(4)).collect())
    };
    let capped_out = project_head(&capped, 150, 100, deadline).expect("projection succeeds");
    assert_eq!(capped_out["rows_truncated"].as_bool(), Some(true));
    assert_eq!(capped_out["rows_capped"].as_bool(), Some(true));

    // Above the hard cap but a FINAL page (`has_next == false`): the
    // clamp is visible, row truncation is not claimed.
    let final_page = QueryResult {
        pagination: Some(pagination(false)),
        ..result_with((0..100).map(|_| big_row(4)).collect())
    };
    let final_out = project_head(&final_page, 150, 100, deadline).expect("projection succeeds");
    assert_eq!(final_out["rows_truncated"].as_bool(), Some(false));
    assert_eq!(final_out["rows_capped"].as_bool(), Some(true));
}

/// Count-path statistics propagation (engine dispatch ground truth):
/// the `__record_counter__` index marker is the O(1) counter, a scanned
/// statistic is a scan, and missing/foreign statistics stay `unknown`
/// WITH the fixed caveat — the shell never invents a path.
#[test]
fn count_path_follows_query_stats() {
    let row = || {
        let mut fields = shamir_collections::new_map();
        fields.insert(String::from("result"), QueryValue::Int(7));
        result_with(vec![QueryRecord::Direct(QueryValue::Map(fields))])
    };

    let mut counter = row();
    counter.stats = Some(QueryStats {
        index_used: Some(String::from("__record_counter__")),
        records_scanned: 0,
        records_returned: 1,
        execution_time_us: 1,
    });
    let out = project_count(&counter).expect("counter row projects");
    assert_eq!(out["count"].as_str(), Some("7"));
    assert_eq!(out["count_path"].as_str(), Some("counter"));
    assert!(out.get("caveat").is_none());

    let mut scan = row();
    scan.stats = Some(QueryStats {
        index_used: None,
        records_scanned: 7,
        records_returned: 1,
        execution_time_us: 2,
    });
    let out = project_count(&scan).expect("scan row projects");
    assert_eq!(out["count_path"].as_str(), Some("scan"));
    assert!(out.get("caveat").is_none());

    let out = project_count(&row()).expect("stats-free row projects");
    assert_eq!(out["count_path"].as_str(), Some("unknown"));
    assert!(
        out["caveat"]
            .as_str()
            .is_some_and(|text| text.contains("O(1) counter")),
        "an unknown count path must carry the fixed caveat"
    );
}

// ── `replication subscriptions`: the typed identity shapes ───────────

/// A lineage id value of `count` bytes, as the engine's `[u8; 16]`
/// serializes through `to_qv` (a list of non-negative integers).
fn lineage(count: usize) -> QueryValue {
    QueryValue::List((0..count).map(|_| QueryValue::Int(0x5A)).collect())
}

/// The engine's `RepoIdentity` map shape: exactly `lineage_id` +
/// `history_epoch`.
fn identity(lineage_id: QueryValue) -> QueryValue {
    let mut fields = shamir_collections::new_map();
    fields.insert(String::from("lineage_id"), lineage_id);
    fields.insert(String::from("history_epoch"), QueryValue::Int(3));
    QueryValue::Map(fields)
}

/// A one-entry `subscriptions` result whose `recorded_identity` is the
/// caller's value; every other allowlisted field carries its engine shape.
fn subscriptions_result(recorded_identity: QueryValue) -> QueryResult {
    let mut entry = shamir_collections::new_map();
    for (name, value) in [
        ("db", QueryValue::Str(String::from("main"))),
        ("repo", QueryValue::Str(String::from("orders"))),
        ("name", QueryValue::Str(String::from("sub"))),
        ("state", QueryValue::Str(String::from("paused"))),
        (
            "installation_state",
            QueryValue::Str(String::from("ordinary")),
        ),
        ("recorded_identity", recorded_identity),
        ("observed_identity", QueryValue::Null),
        ("upstream_bookmark", QueryValue::Int(7)),
        ("next_pull_version", QueryValue::Int(8)),
        ("local_consistent_version", QueryValue::Int(7)),
        ("certified_gap_first", QueryValue::Null),
        ("certified_gap_through", QueryValue::Null),
        ("artifact_digest", QueryValue::Null),
        ("source_lineage_id", QueryValue::Null),
        ("source_history_epoch", QueryValue::Null),
        ("seed_source_version", QueryValue::Null),
        ("local_baseline_version", QueryValue::Null),
    ] {
        entry.insert(String::from(name), value);
    }
    let mut root = shamir_collections::new_map();
    root.insert(
        String::from("subscriptions"),
        QueryValue::List(vec![QueryValue::Map(entry)]),
    );
    result_with(vec![QueryRecord::Direct(QueryValue::Map(root))])
}

/// Nonconforming nested identity shapes are the typed `Server` refusal —
/// never rendered as data: a string where the identity map belongs, an
/// EXTRA nested field, and a 15-element `lineage_id`. The well-formed
/// control still renders.
#[test]
fn subscriptions_refuse_a_nonconforming_identity_shape() {
    let as_text = subscriptions_result(QueryValue::Str(String::from("0xdeadbeef")));
    assert_eq!(
        project_subscriptions(&as_text).expect_err("a text identity is refused"),
        ShellError::Server
    );

    let mut extra = identity(lineage(16));
    let QueryValue::Map(extra_fields) = &mut extra else {
        panic!("identity is a map");
    };
    extra_fields.insert(String::from("injected"), QueryValue::Int(1));
    assert_eq!(
        project_subscriptions(&subscriptions_result(extra))
            .expect_err("an extra nested field is refused"),
        ShellError::Server
    );

    assert_eq!(
        project_subscriptions(&subscriptions_result(identity(lineage(15))))
            .expect_err("a 15-element lineage id is refused"),
        ShellError::Server
    );

    let out = project_subscriptions(&subscriptions_result(identity(lineage(16))))
        .expect("the well-formed identity renders");
    let recorded = &out["subscriptions"][0]["recorded_identity"];
    assert_eq!(
        recorded["lineage_id"].as_array().map(Vec::len),
        Some(16),
        "every lineage byte must be projected"
    );
    assert_eq!(recorded["history_epoch"].as_str(), Some("3"));
    assert_eq!(out["subscriptions"][0]["name"].as_str(), Some("sub"));
}

/// A present-but-`Null` identity stays `null` — the absent marker is
/// rendered, not dropped and not refused.
#[test]
fn subscriptions_render_a_present_but_empty_identity() {
    let out = project_subscriptions(&subscriptions_result(QueryValue::Null))
        .expect("a null identity is well-formed");
    let entry = &out["subscriptions"][0];
    assert!(entry.get("recorded_identity").is_some(), "key must survive");
    assert!(entry["recorded_identity"].is_null(), "null must stay null");
    assert!(entry["observed_identity"].is_null());
    assert_eq!(entry["upstream_bookmark"].as_str(), Some("7"));
}

/// A `u64` identity epoch above `i64::MAX` is `QueryValue::Big` in-process
/// and canonical decimal TEXT on the wire; the real named codec round trip
/// must keep it projectable and exact (no float, no wraparound).
#[test]
fn subscriptions_project_a_large_unsigned_identity_through_the_wire_codec() {
    // The SAME two calls the engine's `to_qv` runs
    // (`execute/core/helpers.rs`): a REAL `RepoIdentity` named-msgpack
    // encode, then a `QueryValue` decode. The epoch leaf is never
    // hand-built: `u64::MAX` sits above `i64::MAX`, so the decode must
    // promote it to `Big` instead of wrapping it to `Int(-1)`.
    let real_identity = shamir_db::engine::meta::repo_identity::RepoIdentity {
        lineage_id: [0x5A; 16],
        history_epoch: u64::MAX,
    };
    let identity_bytes = rmp_serde::to_vec_named(&real_identity).expect("encode identity");
    let recorded_identity: QueryValue =
        rmp_serde::from_slice(&identity_bytes).expect("decode identity");
    let QueryValue::Map(identity_fields) = &recorded_identity else {
        panic!("identity is a map");
    };
    assert_eq!(
        identity_fields.get("history_epoch"),
        Some(&QueryValue::Big(BigInt::from(u64::MAX))),
        "the decoded epoch leaf must be Big, not a wrapped Int"
    );
    let result = subscriptions_result(recorded_identity);

    let record = result.records.into_iter().next().expect("one record");
    let bytes = rmp_serde::to_vec_named(&record).expect("encode");
    let decoded: QueryRecord = rmp_serde::from_slice(&bytes).expect("decode");
    let out = project_subscriptions(&result_with(vec![decoded]))
        .expect("the wire-decoded identity renders");
    let recorded = &out["subscriptions"][0]["recorded_identity"];
    assert_eq!(
        recorded["history_epoch"].as_str(),
        Some("18446744073709551615"),
        "u64::MAX must survive as its exact canonical decimal text"
    );
    assert_eq!(
        recorded["lineage_id"].as_array().map(Vec::len),
        Some(16),
        "the lineage bytes are untouched by the epoch swap"
    );
}

/// The typed subscription shapes refuse what the engine can never emit: a
/// lineage byte above 255 (not a `[u8; 16]` byte) and NON-canonical decimal
/// text in a version leaf; the canonical `u64::MAX` text still renders
/// exactly.
#[test]
fn subscriptions_refuse_an_impossible_lineage_byte_and_non_canonical_text() {
    let impossible = QueryValue::List((0..16).map(|_| QueryValue::Int(256)).collect());
    assert_eq!(
        project_subscriptions(&subscriptions_result(identity(impossible)))
            .expect_err("a lineage byte above 255 is refused"),
        ShellError::Server
    );

    let with_bookmark = |text: &str| {
        let mut result = subscriptions_result(identity(lineage(16)));
        let QueryRecord::Direct(QueryValue::Map(root)) = &mut result.records[0] else {
            panic!("subscription result is one direct map record");
        };
        let QueryValue::List(entries) = root.get_mut("subscriptions").expect("subscriptions list")
        else {
            panic!("subscriptions is a list");
        };
        let QueryValue::Map(entry) = &mut entries[0] else {
            panic!("one subscription entry is a map");
        };
        entry.insert(
            String::from("upstream_bookmark"),
            QueryValue::Str(String::from(text)),
        );
        result
    };

    for text in ["abc", "007", "-1"] {
        assert_eq!(
            project_subscriptions(&with_bookmark(text))
                .expect_err("non-canonical version text is refused"),
            ShellError::Server,
            "{text:?} is not a canonical u64"
        );
    }

    let out = project_subscriptions(&with_bookmark("18446744073709551615"))
        .expect("canonical u64::MAX text renders");
    assert_eq!(
        out["subscriptions"][0]["upstream_bookmark"].as_str(),
        Some("18446744073709551615")
    );
}

/// A version / epoch / bookmark is a `u64`, so only the shapes the engine
/// can emit are accepted: small non-negative values as `Int`, the
/// in-process engine shape as a `Big` that fits in a `u64`, and canonical
/// decimal text ONLY above `i64::MAX` (the encoder emits anything smaller
/// as `Int`). `Str("7")` and a `Big` above `u64::MAX` are not that shape.
#[test]
fn optional_version_accepts_only_encoder_shapes() {
    assert!(
        is_optional_version(&QueryValue::Int(7)),
        "the engine emits a small non-negative value as Int"
    );
    assert!(
        !is_optional_version(&QueryValue::Int(-1)),
        "a negative Int is not a u64 version"
    );

    assert!(
        is_optional_version(&QueryValue::Big(BigInt::from(7))),
        "a Big that fits in a u64 is the in-process shape"
    );
    assert!(
        is_optional_version(&QueryValue::Big(BigInt::from(u64::MAX))),
        "u64::MAX is the largest version"
    );
    assert!(
        !is_optional_version(&QueryValue::Big(BigInt::from(u64::MAX) + BigInt::from(1))),
        "a Big above u64::MAX can never be a version"
    );
    assert!(
        !is_optional_version(&QueryValue::Big(BigInt::from(-1))),
        "a negative Big is not a version"
    );

    assert!(
        !is_optional_version(&QueryValue::Str(String::from("7"))),
        "the encoder emits a small u64 as Int, never as text"
    );
    assert!(
        !is_optional_version(&QueryValue::Str(String::from("007"))),
        "non-canonical decimal text is refused"
    );
    assert!(
        !is_optional_version(&QueryValue::Str(String::from("-1"))),
        "negative text is not a u64"
    );

    let above_i64 = (i64::MAX as u64 + 1).to_string();
    assert!(
        is_optional_version(&QueryValue::Str(above_i64)),
        "the encoder emits a u64 above i64::MAX as canonical decimal text"
    );
    let max = u64::MAX.to_string();
    assert!(
        is_optional_version(&QueryValue::Str(max)),
        "u64::MAX text is the largest wire encoding"
    );
    assert!(
        !is_optional_version(&QueryValue::Str(String::from("18446744073709551616"))),
        "text above u64::MAX is not a version"
    );
}
