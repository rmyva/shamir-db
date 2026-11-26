//! Unit tests of [`request_error`] in `connect/scram.rs` — the exact fixed
//! local SDK error values for the R5-2b misclassification fixes. Real token
//! channel/path behaviour is covered by the shell integration worker.

use shamir_client::ClientError;
use shamir_connect::Error as ConnectError;
use tokio_tungstenite::tungstenite::http::Response;
use tokio_tungstenite::tungstenite::Error as WsError;

use super::{request_error, server_code};
use crate::shell::output::ShellError;

/// Native WSS upgrade rejected before SCRAM (404 from a listener on
/// another path): connection failure, not authentication — the exact wrap
/// produced by `upgrade_error` in `client/transport/dial_ws.rs`.
#[test]
fn wss_upgrade_failure_is_connection_not_authentication() {
    let response = Response::builder()
        .status(404)
        .body(Some(Vec::new()))
        .expect("static 404 response");
    let error = ClientError::Handshake {
        context: "websocket upgrade: HTTP error: 404 Not Found".to_string(),
        source: Some(Box::new(WsError::Http(Box::new(response)))),
    };
    assert_eq!(request_error(error), ShellError::Connection);
}

/// A real SCRAM authentication failure (typed `ConnectError::AuthFailed`
/// wrapped under `Handshake.source` by auth_ok.rs) stays Authentication.
#[test]
fn scram_auth_failure_stays_authentication() {
    let error = ClientError::Handshake {
        context: "authentication failed".to_string(),
        source: Some(Box::new(ConnectError::AuthFailed)),
    };
    assert_eq!(request_error(error), ShellError::Authentication);
}

/// REST server answered 408 after the request was dispatched: unknown
/// outcome (§6), mapped to the Timeout refusal — not generic exit 5.
#[test]
fn http_408_after_dispatch_is_timeout() {
    let error = ClientError::Http {
        status: 408,
        code: None,
        retry_after: None,
    };
    assert_eq!(request_error(error), ShellError::Timeout);
}

/// Typed `token_channel_denied` (403, REST-only token on the token WSS
/// upgrade) decoded by the SDK as `Db`: permission refusal (exit 4).
#[test]
fn token_channel_denied_is_denied() {
    let error = ClientError::Db {
        code: "token_channel_denied".to_string(),
        message: "server text is dropped".to_string(),
    };
    assert_eq!(request_error(error), ShellError::Denied);
}

#[test]
fn online_export_server_codes_have_exact_shell_outcomes() {
    for (code, expected, stable_code, exit) in [
        (
            "online_export_already_running",
            ShellError::ExportBusy,
            "online_export_already_running",
            5,
        ),
        (
            "export_scope_denied",
            ShellError::Denied,
            "permission_denied",
            4,
        ),
        (
            "online_export_invalid_scope",
            ShellError::Server,
            "server_error",
            5,
        ),
        (
            "online_export_unsupported",
            ShellError::Server,
            "server_error",
            5,
        ),
        (
            "online_export_scope_unavailable",
            ShellError::Server,
            "server_error",
            5,
        ),
        (
            "online_export_invalid_form",
            ShellError::Server,
            "server_error",
            5,
        ),
        ("internal_error", ShellError::Server, "server_error", 5),
        (
            "server_shutting_down",
            ShellError::Server,
            "server_error",
            5,
        ),
        (
            "online_export_not_found",
            ShellError::ExportNotFound,
            "online_export_not_found",
            5,
        ),
        (
            "online_export_unavailable",
            ShellError::ExportUnavailable,
            "online_export_unavailable",
            5,
        ),
        (
            "online_export_not_configured",
            ShellError::ExportUnavailable,
            "online_export_unavailable",
            5,
        ),
    ] {
        let mapped = server_code(code);
        assert_eq!(mapped, expected, "wrong mapping for {code}");
        assert_eq!(
            mapped.code(),
            stable_code,
            "wrong stable shell code for {code}"
        );
        assert_eq!(mapped.exit(), exit, "wrong exit class for {code}");
        assert_eq!(
            mapped.message(),
            match expected {
                ShellError::Denied => "permission denied by the server",
                ShellError::ExportBusy => "an online export job is already running on this server; wait for it to finish or cancel it",
                ShellError::ExportNotFound => "no online export job is visible to this session",
                ShellError::ExportUnavailable => "online export is not configured or unavailable on this node",
                _ => "the operation failed on the server",
            },
            "wrong fixed local message for {code}"
        );
    }
}

/// The ACL-mutation refusals keep their OWN typed error (and therefore their
/// own stable code and exit) instead of collapsing into `Server`; an
/// unlisted code still maps to `Server`, so the allowlist stays closed.
#[test]
fn acl_mutation_codes_are_typed() {
    assert_eq!(server_code("invalid_owner"), ShellError::InvalidOwner);
    assert_eq!(server_code("hmac_mismatch"), ShellError::HmacMismatch);
    assert_eq!(server_code("hmac_required"), ShellError::HmacRequired);
    assert_eq!(server_code("unknown_db"), ShellError::UnknownDb);
    assert_eq!(
        server_code("online_export_already_running"),
        ShellError::ExportBusy
    );
    assert_eq!(server_code("export_scope_denied"), ShellError::Denied);
    assert_eq!(
        server_code("online_export_not_found"),
        ShellError::ExportNotFound
    );
    assert_eq!(
        server_code("not_an_allowlisted_code"),
        ShellError::Server,
        "the allowlist must stay closed"
    );
}

/// A read-only follower's stable write refusal keeps its OWN typed error (the
/// design §6 follower diagnostic is surfaced, never retried or redirected), and
/// its exit code (design §5: a server-side refusal is exit 5). Mutant: falling
/// through to the generic `server_error` arm.
#[test]
fn read_only_replica_is_its_own_refusal() {
    for error in [
        ClientError::Db {
            code: "read_only_replica".to_string(),
            message: "server text is dropped".to_string(),
        },
        ClientError::Server {
            code: "read_only_replica".to_string(),
        },
    ] {
        let mapped = request_error(error);
        assert_eq!(mapped, ShellError::ReadOnlyReplica);
        assert_eq!(mapped.code(), "read_only_replica");
        assert_eq!(mapped.exit(), 5, "a server-side refusal is exit 5");
        assert!(
            !mapped.message().contains("dropped"),
            "the server text is never reproduced"
        );
    }
}

/// The VICTIM's own wire code `cancelled` (this session's in-flight batch was
/// cancelled by another operator) is its own typed refusal with exit 5, not a
/// generic server error — the fix must not confuse it with the control
/// command's `CancelOutcome::Cancelled` rendering.
#[test]
fn cancelled_batch_is_its_own_refusal() {
    let mapped = request_error(ClientError::Db {
        code: "cancelled".to_string(),
        message: "request cancelled".to_string(),
    });
    assert_eq!(mapped, ShellError::Cancelled);
    assert_eq!(mapped.code(), "cancelled");
    assert_eq!(mapped.exit(), 5);
    assert!(
        !mapped.message().contains("request cancelled"),
        "the server text is never reproduced"
    );
}

/// The legacy `not_found` fallback classifies a database, a store, a table and
/// a function alike, so its fixed text must name a resource — not a record or
/// a table. Mutant: the previous "the addressed record or table was not found".
#[test]
fn not_found_text_names_a_resource() {
    let mapped = request_error(ClientError::Db {
        code: "not_found".to_string(),
        message: "server text is dropped".to_string(),
    });
    assert_eq!(mapped, ShellError::NotFound);
    assert_eq!(mapped.exit(), 5);
    assert_eq!(mapped.message(), "the addressed resource was not found");
}

/// A node-local online-copy I/O failure (`backup_io_error`) keeps its OWN typed
/// error: its stable code, the server-error class (exit 5), and a fixed local
/// message — the server's own `io::Error` text is never reproduced. Mutant:
/// falling through to the generic `server_error` arm.
#[test]
fn backup_io_error_maps_to_the_typed_io_refusal() {
    let mapped = request_error(ClientError::Db {
        code: "backup_io_error".to_string(),
        message: "hostile server text: /srv/secret path denied".to_string(),
    });
    assert_eq!(mapped, ShellError::BackupIo);
    assert_eq!(mapped.code(), "backup_io_error");
    assert_eq!(mapped.exit(), 5, "a server-side failure is exit 5");
    assert_eq!(
        mapped.message(),
        "the online copy failed on an I/O error on the server node; its own message text is never reproduced"
    );
    assert!(
        !mapped.message().contains("hostile"),
        "the server text is never reproduced"
    );
}
