//! `DbRequest` `Debug` redaction (RV-QUERY-TYPES-1C72378C, review P2/P3):
//! `ChangePasswordVerify`'s SCRAM material must never reach `Debug` output
//! (it rides on logs/crash reports), while non-secret variants keep real
//! diagnostics.

use crate::wire::{ChangePasswordSecrets, DbRequest};

fn redacted_request() -> DbRequest {
    DbRequest::ChangePasswordVerify(ChangePasswordSecrets {
        client_proof_old: vec![0xDE, 0xAD, 0xBE, 0xEF, 0x2A],
        new_salt: vec![0x11, 0x22, 0x33, 0x44],
        new_stored_key: vec![0x55, 0x66, 0x77, 0x88],
        new_server_key: vec![0x99, 0xAA, 0xBB, 0xCC],
    })
}

/// Reverting the redaction — i.e. going back to plain
/// `#[derive(Debug)]`-style printing of the raw `Vec<u8>` fields — makes
/// this FAIL: derived Debug prints the bytes as decimal lists
/// (`[222, 173, ...]`), the field names, and `ChangePasswordSecrets`' own
/// derived shape would print byte lists too.
#[test]
fn rv_query_types_1c72378c_debug_redacts_scram_material() {
    let rendered = format!("{:?}", redacted_request());

    // No field name of the secret struct may appear.
    for name in [
        "client_proof_old",
        "new_salt",
        "new_stored_key",
        "new_server_key",
    ] {
        assert!(
            !rendered.contains(name),
            "Debug output leaks secret field name {name}: {rendered}"
        );
    }

    // No secret byte may appear in any rendering. Decimal lists (derived
    // Debug's `Vec<u8>` form) always contain digits, and the redacted output
    // has none; hex substrings are not checked because short hex pairs
    // collide with the words in the redaction marker and field names.
    assert!(
        !rendered.chars().any(|c| c.is_ascii_digit()),
        "Debug output contains digits (possible secret bytes): {rendered}"
    );

    // The redaction marker is present.
    assert!(rendered.contains("[REDACTED]"), "got: {rendered}");
}

/// The anti-P3 assertion: redacting the secret variant must NOT cost the
/// other variants their diagnostics — `Debug` on a non-secret variant
/// still carries its data. Fails if anyone swaps `DbRequest`'s derived
/// `Debug` back for an opaque `DbRequest(..)` fallback.
#[test]
fn rv_query_types_1c72378c_non_secret_variants_keep_diagnostics() {
    let rendered = format!(
        "{:?}",
        DbRequest::GetDdlOpStatus {
            db: "app".to_string(),
            repo: "main".to_string(),
            table: "t".to_string(),
            op_id: "tok".to_string(),
        }
    );
    assert!(rendered.contains("app"), "got: {rendered}");
    assert!(rendered.contains("tok"), "got: {rendered}");
}
