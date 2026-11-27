use shamir_types::types::value::QueryValue;

use crate::{DbError, DbResult};

/// Names owned at the data-root level: the internal system database, Fjall system catalogue directory, Fjall default repository directory, user directory, server metadata, private cache, counters, audit checkpoint store, token store, active audit log, bootstrap token, format stamp and its atomic-write temporary, import receipt, restore marker, maintenance lock, wire tables registry, and the two system-store WAL siblings. Leading-dot names are policy entries because wire DDL cannot admit them; they also protect non-wire admission paths. This fixed inventory is separate from generated-shape rules below; generated seed repositories and audit rotation archives are reserved by shape rather than enumerated here.
pub const RESERVED_DB_NAMES: [&str; 20] = [
    "__system__",
    "shamir_db_meta.redb",
    "users",
    "server_meta",
    "cache",
    "counters",
    "audit",
    "audit_checkpoint",
    "api_tokens",
    "audit.log",
    "shamir_db_default_main.redb",
    "bootstrap_token.txt",
    "format.stamp",
    ".logical-import-receipt.json",
    ".shamir.lock",
    "wire_tables.mpack",
    "format.stamp.tmp",
    "shamir_db_meta.redb.shamirwal",
    "shamir_db_default_main.redb.shamirwal",
    ".shamir_restore_epoch",
];

fn is_generated_seed_shape(name: &str) -> bool {
    let Some(hash_and_attempt) = name.strip_prefix("seed-") else {
        return false;
    };
    let (hash, attempt) = match hash_and_attempt.split_once("-attempt-") {
        Some((hash, attempt)) => (hash, Some(attempt)),
        None => (hash_and_attempt, None),
    };
    let attempt_is_generated = attempt.is_none_or(|digits| {
        !digits.is_empty()
            && digits.bytes().all(|byte| byte.is_ascii_digit())
            && digits.parse::<u64>().is_ok_and(|value| value < u64::MAX)
    });
    hash.len() == 64
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && attempt_is_generated
}

fn is_audit_rotation_shape(name: &str) -> bool {
    name.strip_prefix("audit.log.").is_some_and(|suffix| {
        suffix.len() == 20 && suffix.bytes().all(|byte| byte.is_ascii_digit())
    })
}

/// Checks a database name by stripping trailing ASCII dots and spaces on every OS and folding ASCII case for the fixed inventory. Generated-shape rules are deliberately separate from that fixed list and match emitted lowercase/ASCII spellings exactly: `seed-` plus 64 lowercase hexadecimal digits, optionally `-attempt-` plus one or more decimal digits below `u64::MAX`, and `audit.log.` plus exactly 20 decimal digits. Unicode full case folding is deliberately not used because reserved names are ASCII and folding could equate distinct non-ASCII names; Unicode NFC/NFD forms are not normalized because path joins compare names byte-wise. 8.3 short aliases are not normalized because aliases depend on volume state and 8.3 generation is commonly disabled. Windows device names such as `CON` and `NUL` are not normalized. This is not a general filesystem-name validator: wire DDL name validation accepts a narrower alphabet, and admission requires passing both layers.
pub fn is_reserved_db_name(name: &str) -> bool {
    let normalized = name.trim_end_matches(['.', ' ']).to_ascii_lowercase();
    RESERVED_DB_NAMES.contains(&normalized.as_str())
        || is_generated_seed_shape(name)
        || is_audit_rotation_shape(name)
}

pub(crate) fn reserved_db_name_refusal(name: &str) -> DbError {
    DbError::Validation(format!("database name '{name}' is reserved by the server"))
}

pub(crate) fn validate_catalogue_db_names(records: &[QueryValue]) -> DbResult<()> {
    for record in records {
        let Some(name) = record["name"].as_str() else {
            continue;
        };
        if name == "__system__" {
            continue;
        }
        if is_reserved_db_name(name) {
            return Err(DbError::Validation(format!(
                "database name '{name}' is reserved by the server; recover by exporting the existing reserved name with the previous release's offline logical-export (for example, --db users), importing with this release into an absent target with --as-db <non-reserved name>, and starting the new server there. A catalogue rename alone does not restore the physical repository path; if export refuses unsupported content, stay on the previous release"
            )));
        }
    }
    Ok(())
}
