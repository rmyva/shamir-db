use shamir_client::{ExportFailure, ExportPhase, ExportStatusReceipt};
use shamir_collections::new_map;
use shamir_types::types::value::QueryValue;

use super::{
    failure_reason, opaque_label, optional_timestamp, phase, project_schedule, project_status,
    status_envelope,
};

#[test]
fn schedule_timestamps_are_iso8601_and_absent_stays_null() {
    assert_eq!(
        optional_timestamp(Some(&QueryValue::Str("0".into()))).expect("epoch timestamp"),
        Some("1970-01-01T00:00:00.000000000Z".into())
    );
    assert_eq!(optional_timestamp(None).expect("missing timestamp"), None);
    let mut schedule = new_map();
    schedule.insert("enabled".into(), QueryValue::Bool(true));
    schedule.insert("interval_seconds".into(), QueryValue::Int(60));
    schedule.insert("window".into(), QueryValue::Null);
    schedule.insert("keep_last".into(), QueryValue::Int(7));
    schedule.insert("run_on_start".into(), QueryValue::Bool(false));
    schedule.insert("next_run_unix_ns".into(), QueryValue::Str("0".into()));
    let mut runs = new_map();
    for name in [
        "scheduled_started",
        "scheduled_skipped_busy",
        "scheduled_skipped_window",
        "scheduled_failed",
        "scheduled_completed",
        "failure_streak",
        "retention_deleted",
        "retention_failed",
    ] {
        runs.insert(name.into(), QueryValue::Int(0));
    }
    schedule.insert("runs".into(), QueryValue::Map(runs));
    let projected = project_schedule(Some(&QueryValue::Map(schedule))).expect("schedule projects");
    assert_eq!(projected["next_run"], "1970-01-01T00:00:00.000000000Z");
    assert!(projected.get("next_run_unix_ns").is_none());
}

#[test]
fn export_terminal_status_projection_keeps_decimal_counters_and_opaque_labels() {
    let status = project_status(&ExportStatusReceipt {
        export_id: String::from("ex-1730000000000-000001"),
        phase: ExportPhase::Done,
        sections: u64::MAX,
        items: 42,
        bytes: 900,
        artifact_name: Some(String::from("export-1730000000000-000002")),
        failure: Some(ExportFailure::Internal),
    })
    .expect("valid server-generated labels project");
    assert_eq!(status["export_id"], "ex-1730000000000-000001");
    assert_eq!(status["artifact_name"], "export-1730000000000-000002");
    assert_eq!(status["sections"], u64::MAX.to_string());
    assert_eq!(status["items"], "42");
    assert_eq!(status["bytes"], "900");
    assert_eq!(status["failure"], "internal");

    for (export_id, artifact_name) in [
        ("../escape-secret", Some("export-1730000000000-000002")),
        ("ex-1730000000000-000001", Some("/etc/passwd")),
        ("ex-1730000000000-000001", Some("C:\\temp\\x")),
        ("ex-1730000000000-000001", Some("export-secret-value")),
        ("ex-secret-canary", Some("export-1730000000000-000002")),
        ("ex-1730000000000-000001", Some("export-secret-canary")),
    ] {
        let err = project_status(&ExportStatusReceipt {
            export_id: export_id.to_owned(),
            phase: ExportPhase::Done,
            sections: 1,
            items: 2,
            bytes: 3,
            artifact_name: artifact_name.map(str::to_owned),
            failure: None,
        })
        .expect_err("malformed opaque label must fail closed");
        assert_eq!(err.code(), "server_error");
        let serialized = serde_json::to_string(&err.code()).expect("stable code serializes");
        assert!(!serialized.contains("/") && !serialized.contains('\\'));
        assert!(!serialized.contains("escape") && !serialized.contains("secret"));
        assert!(!err.message().contains("escape") && !err.message().contains("canary"));
    }
    assert!(opaque_label("export-1730000000000-000002", "export").is_ok());
    assert!(opaque_label("export-1730000000000-00000x", "export").is_err());
}

/// Every terminal failure reason must keep the server contract's snake_case
/// spelling — `Debug`-lowercasing would fuse `TimedOut` into "timedout" and
/// `UnsupportedContent` into "unsupportedcontent".
#[test]
fn failure_reasons_use_the_documented_snake_case_spellings() {
    for (reason, spelling) in [
        (ExportFailure::TimedOut, "timed_out"),
        (ExportFailure::UnsupportedContent, "unsupported_content"),
        (ExportFailure::Io, "io"),
        (ExportFailure::Verification, "verification"),
        (ExportFailure::Internal, "internal"),
    ] {
        assert_eq!(failure_reason(reason), spelling);
        assert!(!spelling.contains("timedout"));
        assert!(!spelling.contains("unsupportedcontent"));
    }
}

/// Every wire phase label is pinned: a rename silently breaks the guide, the
/// guide-example checks and every interactive driver that matches on phases.
#[test]
fn phase_labels_are_pinned() {
    for (phase_value, label) in [
        (ExportPhase::WaitingForGate, "waiting_for_gate"),
        (ExportPhase::Exporting, "exporting"),
        (ExportPhase::Verifying, "verifying"),
        (ExportPhase::Done, "done"),
        (ExportPhase::Failed, "failed"),
        (ExportPhase::Cancelled, "cancelled"),
    ] {
        assert_eq!(phase(phase_value), label);
    }
}

/// The envelope database of a later interactive `status`/`cancel`: the
/// database of the job this session started WINS over the profile's routing
/// database; with neither, the command must be a usage error. Mutants: any
/// precedence swap or a fabricated default would flip one arm here.
#[test]
fn status_envelope_prefers_the_job_database() {
    let job = String::from("jobdb");
    assert_eq!(
        status_envelope(Some(job.as_str()), Some("routing")),
        Some("jobdb")
    );
    assert_eq!(status_envelope(Some(job.as_str()), None), Some("jobdb"));
    assert_eq!(status_envelope(None, Some("routing")), Some("routing"));
    assert_eq!(status_envelope(None, None), None);
}
