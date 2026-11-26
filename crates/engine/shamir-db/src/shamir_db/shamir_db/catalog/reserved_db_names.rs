use shamir_types::types::value::QueryValue;

use crate::{DbError, DbResult};

/// Names owned at the data-root level: the internal system database, system catalogue file, user directory, server metadata, private cache, counters, and audit data.
pub const RESERVED_DB_NAMES: [&str; 7] = [
    "__system__",
    "shamir_db_meta.redb",
    "users",
    "server_meta",
    "cache",
    "counters",
    "audit",
];

/// Checks a database name using ASCII-only lowercase after stripping trailing dots/spaces, matching Windows (and case-insensitive macOS) path resolution. Unicode full case folding is deliberately not used because reserved names are ASCII and folding could equate distinct non-ASCII names. 8.3 aliases such as `USERS~1` are not normalized because aliases depend on volume state and 8.3 generation is commonly disabled; Unicode NFC/NFD forms are not normalized because server path joins compare names byte-wise and every reserved name is ASCII; interior dots/spaces are not stripped because Win32 resolves away only trailing characters.
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
                "database name '{name}' is reserved by the server; start the previous release's server binary, rename it to a non-reserved name with RENAME DATABASE (admin rename), or drop it, then upgrade"
            )));
        }
    }
    Ok(())
}
