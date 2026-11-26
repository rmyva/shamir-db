//! The `when`/`after` refusal of the online-export dispatch form, driven
//! through the QUERY BUILDER (never raw JSON): a batch entry carrying a
//! `when` guard or an `after` ordering edge must be refused atomically
//! with the stable `online_export_invalid_form` code, because the direct
//! export dispatch bypasses the planner that would otherwise honour them.

use shamir_connect::common::types::{BindingMode, TransportKind};
use shamir_connect::server::session::{Session, SessionPermissions};
use shamir_query_builder::batch::Batch;
use shamir_query_builder::ddl;
use shamir_query_types::batch::BatchOp;
use shamir_query_types::filter::{FieldPath, Filter, FilterValue};
use shamir_query_types::wire::DbResponse;

use crate::db_handler::export_gate::{export_dispatch, ExportDispatch};

/// A SCRAM superuser session: the form check only runs after the
/// superuser gate, so the refusal below is the FORM refusal, not the
/// privilege one.
fn superuser_session() -> Session {
    Session::new(
        [83; 16],
        "owner".into(),
        SessionPermissions::new(true, false, vec![]),
        TransportKind::Tcp,
        BindingMode::TlsExporter,
        [0; 32],
        0,
    )
}

/// The builder-built `StartExport` op for the request envelope `appdb`
/// (`ddl::start_export(..).build()` already produces the `BatchOp`).
fn export_op() -> BatchOp {
    ddl::start_export("appdb").repo("main").build()
}

/// The fixed atomic form refusal, matched field by field.
fn assert_form_refusal(refusal: Box<DbResponse>) {
    match *refusal {
        DbResponse::Error { code, message } => {
            assert_eq!(
                code, "online_export_invalid_form",
                "the fixed refusal code must not change: {message}"
            );
            assert!(
                message.contains("standalone")
                    && message.contains("nontransactional")
                    && message.contains("top-level single-operation batch"),
                "the fixed refusal message must not change: {message}"
            );
        }
        other => panic!("the form refusal must be a DbResponse::Error: {other:?}"),
    }
}

/// Control: the builder's plain standalone export entry is a valid dispatch.
///
/// Mutant killed by the pair: a validator that refuses EVERY export batch
/// (or accepts everything) fails this control or the two refusal tests.
#[test]
fn a_plain_builder_export_entry_is_a_valid_dispatch() {
    let mut batch = Batch::new();
    batch.op("export", export_op());
    let request = batch.try_build().expect("the plain batch must build");
    let session = superuser_session();
    let dispatch = export_dispatch(&session, &request, "appdb")
        .expect("a plain standalone export entry must dispatch");
    assert!(
        matches!(dispatch, Some(ExportDispatch::Start(_))),
        "the plain entry must dispatch as an export start: {dispatch:?}"
    );
}

/// A `when` guard on the export entry is refused: the direct dispatch
/// bypasses the planner, so the condition would be ignored, not honoured.
///
/// Mutant: removing the `visit.entry.when.is_some()` arm of
/// `export_dispatch_form` (`repo_admin/support/export_gate.rs`) admits the
/// guarded entry and the refusal assertion fails.
#[test]
fn a_when_guard_on_the_export_entry_is_refused() {
    let mut batch = Batch::new();
    let export = batch.op("export", export_op());
    batch.when(
        &export,
        Filter::Eq {
            field: FieldPath::from(vec!["flag".to_string()]),
            value: FilterValue::Bool(true),
        },
    );
    let request = batch.try_build().expect("the guarded batch must build");
    let session = superuser_session();
    let refusal = export_dispatch(&session, &request, "appdb")
        .expect_err("a `when` guard must be refused on the export form");
    assert_form_refusal(refusal);
}

/// An `after` ordering edge onto the export entry is refused for the same
/// reason. The fixture is a SINGLE entry carrying the edge: a second
/// entry would hit the mixed-batch rejection with the identical response,
/// so removing only the `after` check would leave the test green. The
/// builder refuses a self-referencing `after` edge at validation time
/// (`BuildError::SelfReference`), so the single-entry batch is built
/// normally and the edge is set on the BUILT request — the only validator
/// this test exercises beyond the builder is the export form gate.
///
/// Mutant: removing the `!visit.entry.after.is_empty()` arm of
/// `export_dispatch_form` (`repo_admin/support/export_gate.rs`) admits the
/// edged entry and the refusal assertion fails.
#[test]
fn an_after_edge_onto_the_export_entry_is_refused() {
    let mut batch = Batch::new();
    batch.op("export", export_op());
    let mut request = batch.try_build().expect("the plain batch must build");
    request
        .queries
        .get_mut("export")
        .expect("the single export entry is present")
        .after = vec!["export".to_string()];
    let session = superuser_session();
    let refusal = export_dispatch(&session, &request, "appdb")
        .expect_err("an `after` edge must be refused on the export form");
    assert_form_refusal(refusal);
}
