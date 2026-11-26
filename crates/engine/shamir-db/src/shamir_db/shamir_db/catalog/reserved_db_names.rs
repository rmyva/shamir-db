use shamir_types::types::value::QueryValue;

use crate::{DbError, DbResult};

/// Names owned at the data-root level: the internal system database, Fjall system catalogue directory, default repository file, user directory, server metadata, private cache, counters, audit checkpoint store, token store, active audit log, bootstrap token, format stamp, logical-import receipt, and audit data.
pub const RESERVED_DB_NAMES: [&str; 14] = [
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
];

/// Checks a database name by stripping trailing ASCII dots and spaces on every OS and folding ASCII case. Unicode full case folding is deliberately not used because reserved names are ASCII and folding could equate distinct non-ASCII names; Unicode NFC/NFD forms are not normalized because path joins compare names byte-wise. 8.3 short aliases are not normalized because aliases depend on volume state and 8.3 generation is commonly disabled. Windows device names such as `CON` and `NUL` are not normalized. This is not a general filesystem-name validator: wire DDL name validation accepts a narrower alphabet, and admission requires passing both layers.
pub fn is_reserved_db_name(name: &str) -> bool {
    let normalized = name.trim_end_matches(['.', ' ']).to_ascii_lowercase();
    RESERVED_DB_NAMES.contains(&normalized.as_str())
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
                "database name '{name}' is reserved by the server; a catalogue rename alone does not restore the physical repository path. Export with the previous release's offline logical-export and import with this release under a non-reserved name into an absent target (see the operations guide); if export refuses unsupported content, stay on the previous release"
            )));
        }
    }
    Ok(())
}
