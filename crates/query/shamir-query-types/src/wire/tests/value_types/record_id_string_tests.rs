//! RU6 golden/contract tests — `RecordId` on the wire is a canonical base58
//! string (RV-QUERY-TYPES-A984DFED), applied to `QueryResult::op_id`,
//! `DdlOpStatus.op_id`, `DropIndexOp.request_id` and `RenameIndexOp.request_id`.
//!
//! The byte oracle below is derived transparently from the rmp_serde
//! named-map encoding rules (fixmap header `0x80 + n`, fixstr
//! `0xa0 + len`) — no runtime capture was possible (tests are CI-only per
//! the repo's compile/build policy), so the expected byte strings are
//! built field-by-field with local helpers instead of hard-coded hex.

use shamir_types::types::record_id::RecordId;
use std::str::FromStr;

use crate::admin::DropIndexOp;
use crate::admin::RenameIndexOp;
use crate::read::DdlOpKind;
use crate::read::DdlOpState;
use crate::read::DdlOpStatus;
use crate::read::QueryResult;
use crate::wire::DbRequest;
use crate::wire::DbResponse;

/// Fixed RecordId: raw bytes `0x01..=0x10`; base58btc → `8DfbjXLth7APvt3qQPgtf`.
fn fixed_id() -> RecordId {
    RecordId::try_from_bytes(&(0x01u8..=0x10).collect::<Vec<u8>>()).expect("16 raw bytes")
}

fn fixstr(s: &str) -> Vec<u8> {
    let mut out = vec![0xa0 + s.len() as u8];
    out.extend_from_slice(s.as_bytes());
    out
}

fn fixmap(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut out = vec![0x80 + entries.len() as u8];
    for (key, value) in entries {
        out.extend_from_slice(&fixstr(key));
        out.extend_from_slice(value);
    }
    out
}

fn encoded<T: serde::Serialize>(value: &T) -> Vec<u8> {
    rmp_serde::to_vec_named(value).expect("encode")
}

fn bin8(payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0xc4, payload.len() as u8];
    out.extend_from_slice(payload);
    out
}

#[test]
fn rv_query_types_a984dfed_query_result_op_id_is_base58_str() {
    let result = QueryResult {
        op_id: Some(fixed_id()),
        ..Default::default()
    };
    let expected = fixmap(&[
        ("records", vec![0x90]),
        ("op_id", fixstr(&fixed_id().to_string())),
    ]);
    assert_eq!(encoded(&result), expected);
    // The value marker preceding the id bytes is a str marker, never bin8 (0xc4).
    let bytes = encoded(&result);
    let canonical = fixed_id().to_string();
    let marker = bytes[bytes.len() - canonical.len() - 1];
    assert!(
        (0xa0..=0xbf).contains(&marker),
        "expected fixstr, got {marker:#04x}"
    );
}

#[test]
fn rv_query_types_a984dfed_ddl_op_status_envelope_is_base58_str() {
    let status = DdlOpStatus {
        op_id: fixed_id(),
        kind: DdlOpKind::Other {
            description: "x".to_string(),
        },
        state: DdlOpState::InProgress,
    };
    let response = DbResponse::DdlOpStatus {
        status: Some(status),
    };
    let expected = fixmap(&[
        ("kind", fixstr("ddl_op_status")),
        (
            "status",
            fixmap(&[
                ("op_id", fixstr(&fixed_id().to_string())),
                (
                    "kind",
                    fixmap(&[("other", fixmap(&[("description", fixstr("x"))]))]),
                ),
                ("state", fixstr("in_progress")),
            ]),
        ),
    ]);
    assert_eq!(encoded(&response), expected);
}

#[test]
fn rv_query_types_a984dfed_drop_index_request_id_is_base58_str() {
    let op = DropIndexOp {
        drop_index: "idx".to_string(),
        table: "t".to_string(),
        repo: "main".to_string(),
        hmac: None,
        if_exists: false,
        request_id: Some(fixed_id()),
    };
    let expected = fixmap(&[
        ("drop_index", fixstr("idx")),
        ("table", fixstr("t")),
        ("repo", fixstr("main")),
        ("request_id", fixstr(&fixed_id().to_string())),
    ]);
    assert_eq!(encoded(&op), expected);
}

#[test]
fn rv_query_types_a984dfed_rename_index_request_id_is_base58_str() {
    let op = RenameIndexOp {
        rename_index: "old".to_string(),
        to: "new".to_string(),
        table: "t".to_string(),
        repo: "main".to_string(),
        if_exists: false,
        request_id: Some(fixed_id()),
    };
    let expected = fixmap(&[
        ("rename_index", fixstr("old")),
        ("to", fixstr("new")),
        ("table", fixstr("t")),
        ("repo", fixstr("main")),
        ("request_id", fixstr(&fixed_id().to_string())),
    ]);
    assert_eq!(encoded(&op), expected);
}

#[test]
fn rv_query_types_a984dfed_decode_rejects_bin_and_non_canonical() {
    // bin8 payload where a str is required — must be an error. `records`
    // (required on QueryResult) is present so a failure isolates the
    // identifier rejection, not a missing-field error.
    let bytes = fixmap(&[
        ("records", vec![0x90]),
        ("op_id", bin8(&(0x01u8..=0x10).collect::<Vec<u8>>())),
    ]);
    assert!(rmp_serde::from_slice::<QueryResult>(&bytes).is_err());

    // A string that is not canonical base58 (uppercase-ineligible 'I') must
    // be rejected even if `RecordId::from_str` happened to tolerate it.
    let id = fixed_id();
    let canonical = id.to_string();
    let mut tampered: Vec<char> = canonical.chars().collect();
    tampered[0] = 'I';
    let tampered: String = tampered.into_iter().collect();
    if tampered != canonical {
        let bytes = fixmap(&[("records", vec![0x90]), ("op_id", fixstr(&tampered))]);
        assert!(rmp_serde::from_slice::<QueryResult>(&bytes).is_err());
    }

    // Strict-bin: a msgpack bin whose bytes are the ASCII of the CANONICAL
    // token is still an error — the adapter accepts str, never bin.
    let canonical_bytes = canonical.as_bytes();
    let bytes = fixmap(&[("records", vec![0x90]), ("op_id", bin8(canonical_bytes))]);
    assert!(rmp_serde::from_slice::<QueryResult>(&bytes).is_err());

    // Same strictness through the Option adapter: DropIndexOp.request_id.
    let bytes = fixmap(&[
        ("drop_index", fixstr("idx")),
        ("table", fixstr("t")),
        ("repo", fixstr("main")),
        ("request_id", bin8(canonical_bytes)),
    ]);
    assert!(rmp_serde::from_slice::<DropIndexOp>(&bytes).is_err());

    // Same strictness through the internally tagged DbResponse envelope
    // (hand-built: rmp_serde::to_vec_named cannot emit a bin8 here). The
    // envelope decodes kind first, then DdlOpStatus.op_id hits the strict
    // visitor and the whole DbResponse decode errs.
    let bytes = fixmap(&[
        ("kind", fixstr("ddl_op_status")),
        (
            "status",
            fixmap(&[
                ("op_id", bin8(canonical_bytes)),
                (
                    "kind",
                    fixmap(&[("other", fixmap(&[("description", fixstr("x"))]))]),
                ),
                ("state", fixstr("in_progress")),
            ]),
        ),
    ]);
    assert!(rmp_serde::from_slice::<DbResponse>(&bytes).is_err());

    // Canonical string round-trips: from_str(s).to_string() == s.
    assert_eq!(
        RecordId::from_str(&canonical).unwrap().to_string(),
        canonical
    );
    let bytes = encoded(&QueryResult {
        op_id: Some(id),
        ..Default::default()
    });
    let back: QueryResult = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(back.op_id, Some(fixed_id()));
}

#[test]
fn rv_query_types_a984dfed_request_id_wire_mirror_round_trip() {
    // The "send a base58 request_id, poll by the same string" wire analog:
    // encode a real DropIndexOp, decode it through a plain-String mirror,
    // rebuild the request from that string, and require byte-equal output.
    #[derive(serde::Deserialize)]
    struct RequestIdMirror {
        request_id: String,
    }
    let op = DropIndexOp {
        drop_index: "idx".to_string(),
        table: "t".to_string(),
        repo: "main".to_string(),
        hmac: None,
        if_exists: false,
        request_id: Some(fixed_id()),
    };
    let bytes = encoded(&op);
    let mirror: RequestIdMirror = rmp_serde::from_slice(&bytes).expect("mirror decode");
    assert_eq!(mirror.request_id, fixed_id().to_string());

    let rebuilt = DropIndexOp {
        request_id: Some(RecordId::from_str(&mirror.request_id).expect("canonical base58")),
        ..op.clone()
    };
    assert_eq!(encoded(&rebuilt), bytes, "wire mirror must be byte-stable");
    assert_eq!(rebuilt, op);
}

#[test]
fn rv_query_types_a984dfed_poll_by_response_string_round_trip() {
    // Mirror of the TS client: decode the response as an untyped
    // `{ op_id: String }`, then feed that string unchanged into the poll
    // request and decode it back as a `DbRequest`.
    #[derive(serde::Deserialize)]
    struct OpIdMirror {
        op_id: String,
    }
    let bytes = encoded(&QueryResult {
        op_id: Some(fixed_id()),
        ..Default::default()
    });
    let mirror: OpIdMirror = rmp_serde::from_slice(&bytes).unwrap();
    let request = DbRequest::GetDdlOpStatus {
        db: "db".to_string(),
        repo: "main".to_string(),
        table: "t".to_string(),
        op_id: mirror.op_id,
    };
    let request_bytes = encoded(&request);
    let back: DbRequest = rmp_serde::from_slice(&request_bytes).unwrap();
    match back {
        DbRequest::GetDdlOpStatus { op_id, .. } => assert_eq!(op_id, fixed_id().to_string()),
        other => panic!("expected GetDdlOpStatus, got {other:?}"),
    }
}
