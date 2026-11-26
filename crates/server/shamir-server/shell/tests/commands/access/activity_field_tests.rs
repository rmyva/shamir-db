//! Free-text activity fields are projected through an allowlist, never
//! echoed as-is (design `admin-shell.md` §4.5, the privacy rule): `user`
//! must be a principal name the server accepts — the shell delegates to
//! `NormalizedUsername::from_raw`
//! (`crates/net/shamir-connect/src/common/user/username.rs`, RFC 8265
//! PRECIS `UsernameCaseMapped`) and keeps the value only when it is already
//! canonical, and never a token shape — `db` must match the engine's own
//! component-name rule, `transport` must be one of the three labels the
//! server emits, `op_kind` must be a wire operation discriminant. Anything
//! else is the fixed `<invalid>` placeholder, so a peer cannot smuggle a
//! terminal escape or an over-long blob into the operator's terminal.
//!
//! Every assertion below names the value it forbids, so deleting the
//! allowlist (or widening it to `output::sanitize`) fails the test instead
//! of merely changing a string's shape.

use serde_json::Value;
use shamir_query_types::admin::activity::{ActivityRequestState, ActivityRow, RequestExecId};

use super::{project_request, project_session};

/// The fixed placeholder every out-of-shape field renders as.
const INVALID: &str = "<invalid>";

/// A credential-looking blob: a token shape the shell must never echo, in
/// any field (for `user` it is redundant with the login rule, and that
/// over-redaction is deliberate).
const TOKEN_SHAPE: &str = "shk1.7f3a9c.4b1d2e6f8a0c";

/// One session row with the four free-text fields under test.
fn session_row(user: &str, transport: &str) -> ActivityRow {
    ActivityRow::Session {
        session_handle: 7,
        user: user.to_owned(),
        transport: transport.to_owned(),
        created_at_ns: 1,
        idle_ns: 2,
        open_transactions: 3,
        open_cursors: 4,
    }
}

/// One request row with the free-text fields under test.
fn request_row(user: &str, db: &str, op_kind: &str) -> ActivityRow {
    ActivityRow::Request {
        request_exec_id: RequestExecId(9),
        session_handle: Some(7),
        user: user.to_owned(),
        db: db.to_owned(),
        op_kind: op_kind.to_owned(),
        state: ActivityRequestState::WaitingLock,
        elapsed_ns: 5,
    }
}

/// Well-shaped fields pass through unchanged: the allowlist is a filter, not
/// a rewrite (mutant: replacing every field with the placeholder).
#[test]
fn documented_values_pass_through() {
    let session = project_session(&session_row("admin", "tcp")).expect("session projects");
    assert_eq!(session["user"], "admin");
    assert_eq!(session["transport"], "tcp");
    for transport in ["ws", "ipc"] {
        let row = project_session(&session_row("u-1_2", transport)).expect("projects");
        assert_eq!(row["transport"], transport, "documented transport");
        assert_eq!(row["user"], "u-1_2", "the principal rule passes");
    }
    let request = project_request(&request_row("admin", "shelldb", "write")).expect("projects");
    assert_eq!(request["user"], "admin");
    assert_eq!(request["db"], "shelldb");
    assert_eq!(request["op_kind"], "write");
    // The server's own admin labels carry a colon: they must not become
    // `<invalid>` (mutant: an op_kind rule without the colon).
    let admin =
        project_request(&request_row("admin", "shelldb", "admin:list_activity")).expect("projects");
    assert_eq!(admin["op_kind"], "admin:list_activity");
}

/// A control/escape sequence, a whitespace, an empty value and an over-long
/// value in ANY free-text field render as the one fixed placeholder (mutant:
/// echoing `sanitize`d wire text — an escape-only sanitiser still lets
/// whitespace and the long blob through). A credential shape is hostile in
/// every field, `user` included (see [`user_field_rejects_token_shaped_names`]);
/// a login name is legitimately any Unicode text the server accepts (see
/// [`user_field_accepts_precis_names`]).
#[test]
fn hostile_values_render_the_placeholder() {
    let escape = "\u{1b}[2Jshamir";
    let long = "a".repeat(300);
    for user in [escape, &long, "", "with space"] {
        let session = project_session(&session_row(user, "tcp")).expect("projects");
        assert_eq!(session["user"], INVALID, "hostile session user");
        let request = project_request(&request_row(user, "shelldb", "write")).expect("projects");
        assert_eq!(request["user"], INVALID, "hostile request user");
    }
    for db in [TOKEN_SHAPE, escape, &long, "", "db/../etc"] {
        let request = project_request(&request_row("admin", db, "write")).expect("projects");
        assert_eq!(request["db"], INVALID, "hostile request db");
    }
    for transport in ["quic", "Tcp", escape, "", TOKEN_SHAPE] {
        let session = project_session(&session_row("admin", transport)).expect("projects");
        assert_eq!(session["transport"], INVALID, "undocumented transport");
    }
    for op_kind in ["write\n", "Write", escape, "", TOKEN_SHAPE, &"x".repeat(65)] {
        let request = project_request(&request_row("admin", "shelldb", op_kind)).expect("projects");
        assert_eq!(request["op_kind"], INVALID, "undocumented op_kind");
    }
}

/// The placeholder is the ONLY substitution: every other leaf of a row whose
/// free-text fields are hostile still projects normally (mutant: refusing the
/// whole row, which would hide a real request from the operator).
#[test]
fn hostile_text_does_not_drop_the_row() {
    let escape = "\u{1b}[2Jshamir";
    let projected: Value = project_request(&request_row(escape, TOKEN_SHAPE, TOKEN_SHAPE))
        .expect("a hostile row still projects");
    assert_eq!(projected["kind"], "request");
    assert_eq!(projected["request_exec_id"], "9");
    assert_eq!(projected["state"], "waiting_lock");
    assert_eq!(projected["user"], INVALID);
    assert_eq!(projected["db"], INVALID);
    assert_eq!(projected["op_kind"], INVALID);
}

/// Every static `op_kind` label the server can register, copied from
/// `crates/query/shamir-query-types/src/admin/activity/op_name.rs`
/// (`BatchOp::activity_name`, the exhaustive match) plus the `tx_open` label
/// the server's transaction path mints for an interactive transaction
/// (`crates/server/shamir-server/src/engine/db_handler/repo_admin/tx_handlers.rs`).
/// The list is pinned here on purpose: the labels are `&'static str` literals
/// in that match, not exported constants.
const OP_LABELS: [&str; 81] = [
    "admin:access_tree",
    "admin:add_group_member",
    "admin:add_schema_rule",
    "admin:alter_buffer_config",
    "admin:alter_function",
    "admin:alter_repo",
    "admin:alter_subscription",
    "admin:application_schema_plan",
    "admin:application_schema_status",
    "admin:apply_application_schema_migration",
    "admin:backup_status",
    "admin:bind_validator",
    "admin:cancel_backup",
    "admin:cancel_request",
    "admin:changes_since",
    "admin:check_function",
    "admin:chgrp",
    "admin:chmod",
    "admin:chown",
    "admin:commit_migration",
    "admin:create_db",
    "admin:create_function",
    "admin:create_function_folder",
    "admin:create_group",
    "admin:create_index",
    "admin:create_publication",
    "admin:create_replication_profile",
    "admin:create_repo",
    "admin:create_subscription",
    "admin:create_table",
    "admin:create_user",
    "admin:create_validator",
    "admin:describe_function",
    "admin:describe_repo",
    "admin:describe_table",
    "admin:drop_db",
    "admin:drop_function",
    "admin:drop_group",
    "admin:drop_index",
    "admin:drop_publication",
    "admin:drop_replication_profile",
    "admin:drop_repo",
    "admin:drop_subscription",
    "admin:drop_table",
    "admin:drop_user",
    "admin:drop_validator",
    "admin:export_function",
    "admin:get_buffer_config",
    "admin:get_table_schema",
    "admin:grant_role",
    "admin:interner_dump",
    "admin:interner_touch",
    "admin:list",
    "admin:list_activity",
    "admin:list_publications",
    "admin:list_subscriptions",
    "admin:list_validators",
    "admin:migration_status",
    "admin:purge_history",
    "admin:remove_group_member",
    "admin:remove_schema_rule",
    "admin:rename_db",
    "admin:rename_function",
    "admin:rename_function_folder",
    "admin:rename_group",
    "admin:rename_index",
    "admin:rename_repo",
    "admin:rename_table",
    "admin:rename_validator",
    "admin:replication_status",
    "admin:revoke_role",
    "admin:rollback_migration",
    "admin:set_buffer_config",
    "admin:set_retention",
    "admin:set_table_schema",
    "admin:start_backup",
    "admin:start_migration",
    "admin:unbind_validator",
    "read",
    "tx_open",
    "write",
];

/// EVERY server label passes the projection: the rule is a shape filter, never
/// a vocabulary cap. Mutant: the previous 32-byte cap, which turned the healthy
/// 40-byte `admin:apply_application_schema_migration` row into `<invalid>` in a
/// real listing.
#[test]
fn every_server_op_label_passes() {
    for label in OP_LABELS {
        let row = project_request(&request_row("admin", "shelldb", label)).expect("projects");
        assert_eq!(
            row["op_kind"], label,
            "the server's own label must never become the placeholder"
        );
    }
    let longest = OP_LABELS
        .iter()
        .max_by_key(|label| label.len())
        .expect("the list is not empty");
    assert_eq!(longest.len(), 40, "the longest static label is 40 bytes");
}

/// The `op_kind` cap is a bound with headroom over the longest real label: one
/// byte over it is the placeholder, the cap itself still passes.
#[test]
fn op_kind_cap_boundary() {
    let at_cap = "a".repeat(super::MAX_OP_KIND_BYTES);
    let row = project_request(&request_row("admin", "shelldb", &at_cap)).expect("projects");
    assert_eq!(row["op_kind"], at_cap);
    let over = "a".repeat(super::MAX_OP_KIND_BYTES + 1);
    let row = project_request(&request_row("admin", "shelldb", &over)).expect("projects");
    assert_eq!(row["op_kind"], INVALID);
}

/// The user and database display caps are the ones the server applies, not a
/// shell guess: a 65-byte ASCII login (accepted and published by the server
/// under its 255-byte `USERNAME_MAX_BYTES` limit) is printed as-is, the cap
/// itself passes, and one byte over it is the placeholder. The cap is BYTES on
/// both sides — the server caps the PRECIS-normalised byte length — so a
/// 127-char Cyrillic login (254 bytes) passes and its 128-char brother
/// (256 bytes) does not. Mutant: the previous 64-byte cap applied to both
/// fields; mutant: counting chars instead of bytes.
#[test]
fn name_caps_follow_the_engine_limits() {
    let login = "u".repeat(65);
    let session = project_session(&session_row(&login, "tcp")).expect("projects");
    assert_eq!(session["user"], login, "a 65-byte login must not be hidden");

    let at_user_cap = "u".repeat(255);
    let session = project_session(&session_row(&at_user_cap, "tcp")).expect("projects");
    assert_eq!(
        session["user"], at_user_cap,
        "the engine's login limit passes"
    );
    let over_user_cap = "u".repeat(256);
    let session = project_session(&session_row(&over_user_cap, "tcp")).expect("projects");
    assert_eq!(session["user"], INVALID, "one byte over the login limit");

    let wide_at_cap = "и".repeat(127);
    let session = project_session(&session_row(&wide_at_cap, "tcp")).expect("projects");
    assert_eq!(
        session["user"], wide_at_cap,
        "254 bytes of Cyrillic is inside the limit"
    );
    let wide_over_cap = "и".repeat(128);
    let session = project_session(&session_row(&wide_over_cap, "tcp")).expect("projects");
    assert_eq!(
        session["user"], INVALID,
        "256 bytes of Cyrillic is over the limit, even though it is 128 chars"
    );

    let long_db = "d".repeat(65);
    let request = project_request(&request_row("admin", &long_db, "write")).expect("projects");
    assert_eq!(request["db"], long_db, "a 65-byte database name passes");
    let at_db_cap = "d".repeat(255);
    let request = project_request(&request_row("admin", &at_db_cap, "write")).expect("projects");
    assert_eq!(request["db"], at_db_cap);
    let over_db_cap = "d".repeat(256);
    let request = project_request(&request_row("admin", &over_db_cap, "write")).expect("projects");
    assert_eq!(request["db"], INVALID);

    // `db` keeps the engine's ASCII-only component rule, so a non-ASCII or
    // dotted value is the placeholder for a database even where a login of
    // the same text is printable (see `db_field_keeps_the_component_rule`).
    for raw in ["ünïcode", "mixed_case.with.dot"] {
        let request = project_request(&request_row("admin", raw, "write")).expect("projects");
        assert_eq!(request["db"], INVALID, "the db charset stays ASCII-only");
    }
}

/// The `user` field follows the login profile the SERVER enforces, not the
/// engine's ASCII component rule: `crates/net/shamir-connect/src/common/user/
/// username.rs` accepts a PRECIS `UsernameCaseMapped` login in any script, so
/// a legitimate canonical non-ASCII login must be printed, never hidden.
/// Mutant: the component rule `[A-Za-z0-9_-]` applied to a principal, which
/// turned every non-ASCII login into `<invalid>` in a real `activity
/// sessions` / `activity requests` listing.
#[test]
fn user_field_accepts_precis_names() {
    for user in [
        "владимир",
        "иван",
        "ünal",
        "用户",
        "café",
        "u-1_2",
        "user42",
    ] {
        let session = project_session(&session_row(user, "tcp")).expect("projects");
        assert_eq!(session["user"], user, "a server-accepted login is printed");
        let request = project_request(&request_row(user, "shelldb", "write")).expect("projects");
        assert_eq!(
            request["user"], user,
            "a server-accepted login is printed in a request row too"
        );
    }
}

/// `db` keeps the engine's own component rule, which is ASCII-only: the very
/// text the `user` field prints is the placeholder in `db`. Mutant: routing
/// `db` through the principal rule, which would echo any printable Unicode as
/// a database name.
#[test]
fn db_field_keeps_the_component_rule() {
    for db in [
        "владимир",
        "иван",
        "shelldb.prod",
        "with space",
        "shelldb/prod",
    ] {
        let request = project_request(&request_row("admin", db, "write")).expect("projects");
        assert_eq!(request["db"], INVALID, "a db is not a principal name");
    }
    for db in ["shelldb", "db-1_2", "db42"] {
        let request = project_request(&request_row("admin", db, "write")).expect("projects");
        assert_eq!(request["db"], db, "the component rule still passes");
    }
}

/// A `user` value the server would reject or normalise, or that is not
/// terminal-safe, is the placeholder: a private-use code point, a soft
/// hyphen, and a decomposed NFC `cafe\u{0301}` all fail the server's own
/// validator or its canonical-text requirement, and an uppercase (case-mapped)
/// name is not the wire text either. Mutant: trusting a shell-side character
/// allowlist instead of the server's validator, which would print text the
/// server never stores.
#[test]
fn user_field_rejects_server_rejected_names() {
    for user in [
        "a\u{E000}",
        "a\u{00AD}b",
        "cafe\u{0301}",
        "Владимир",
        "Ünal",
    ] {
        let session = project_session(&session_row(user, "tcp")).expect("projects");
        assert_eq!(session["user"], INVALID, "non-canonical session user");
        let request = project_request(&request_row(user, "shelldb", "write")).expect("projects");
        assert_eq!(request["user"], INVALID, "non-canonical request user");
    }
}

/// A credential-shaped login is redacted even though the PRECIS profile
/// accepts it as a name: the `shk1.<id>.<secret>` shape
/// `crate::shell::connect::token` refuses to echo is mirrored in
/// `user_field`, and over-redacting a token-shaped value is the owner's
/// accepted choice. Mutant: a login rule without the token check, which would
/// print a pasted API token in the operator's terminal.
#[test]
fn user_field_rejects_token_shaped_names() {
    let session = project_session(&session_row(TOKEN_SHAPE, "tcp")).expect("projects");
    assert_eq!(session["user"], INVALID, "a token-shaped session user");
    let request = project_request(&request_row(TOKEN_SHAPE, "shelldb", "write")).expect("projects");
    assert_eq!(request["user"], INVALID, "a token-shaped request user");
    // The shape is redacted anywhere in the cell, not only at its start.
    let embedded = format!("bob.{TOKEN_SHAPE}");
    let session = project_session(&session_row(&embedded, "tcp")).expect("projects");
    assert_eq!(
        session["user"], INVALID,
        "an embedded token in a session user"
    );
    let request = project_request(&request_row(&embedded, "shelldb", "write")).expect("projects");
    assert_eq!(
        request["user"], INVALID,
        "an embedded token in a request user"
    );
}

/// A `user` value that is not terminal-safe is still the placeholder, even
/// though the profile is otherwise permissive: a C0 control, a C1 control, a
/// bidi override, a bidi isolate, a zero-width character, a BOM, a Unicode
/// line separator and a whitespace each make the whole cell `<invalid>`.
/// Mutant: accepting every character (e.g. checking only the length), which
/// would put a bidi override or a line break in the cell.
#[test]
fn user_field_rejects_terminal_control_characters() {
    let hostile = [
        "ivan\u{7}",
        "ivan\u{85}",
        "ivan\u{202E}",
        "\u{2066}ivan",
        "ivan\u{200B}",
        "\u{FEFF}ivan",
        "ivan\u{2028}next",
        "ivan\tname",
        "\u{3000}ivan",
    ];
    for user in hostile {
        let session = project_session(&session_row(user, "tcp")).expect("projects");
        assert_eq!(session["user"], INVALID, "terminal control in user");
        let request = project_request(&request_row(user, "shelldb", "write")).expect("projects");
        assert_eq!(request["user"], INVALID, "terminal control in request user");
    }
}
