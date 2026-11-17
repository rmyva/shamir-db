use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use shamir_types::types::common::new_map;
use shamir_types::types::record_id::RecordId;
use shamir_types::types::value::QueryValue;

use crate::query::filter::{Filter, FilterValue};
use crate::query::write::UpdateOp;
use crate::query::TableRef;
use crate::shamir_db::system_store::store::TABLE_DATABASES;
use crate::shamir_db::system_store::SystemStore;
use crate::{DbError, DbResult};

/// The string-encoded per-database identity for R1c step 2a.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DatabaseIdentity(pub RecordId);

impl DatabaseIdentity {
    /// Generate a new database identity using the standard `RecordId` generator.
    pub fn new() -> Self {
        Self(RecordId::new())
    }
}

impl Default for DatabaseIdentity {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for DatabaseIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for DatabaseIdentity {
    type Err = DbError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        RecordId::from_str(value)
            .map(Self)
            .map_err(|error| DbError::Validation(format!("invalid database identity: {error}")))
    }
}

impl Serialize for DatabaseIdentity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DatabaseIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

/// Ensure the named database has a durable identity, backfilling legacy rows.
pub async fn ensure_database_identity(
    store: &SystemStore,
    name: &str,
) -> DbResult<DatabaseIdentity> {
    let (row, record_id, version) = store.database_row_with_version(name).await?;
    let Some(row) = row else {
        return Err(DbError::NotFound(format!("Database '{name}' not found")));
    };
    let record_id = record_id.ok_or_else(|| {
        DbError::Internal(format!(
            "corrupt database '{name}' catalogue row has no physical id"
        ))
    })?;
    let identity_result = identity_from_record(&row, name, record_id)?;
    if let Some(identity) = identity_result {
        if database_identity_in_use(store, name, identity).await? {
            return Err(DbError::Internal(format!(
                "database '{name}' identity {identity} is already assigned to another database record"
            )));
        }
        let table = store.table(TABLE_DATABASES).await?;
        store.persist_table_interner(&table, None).await?;
        store.flush_system_repo_durable(&table).await?;
        return Ok(identity);
    }

    let table = store.table(TABLE_DATABASES).await?;
    store.catalogue_write_fault_gate()?;
    let identity = DatabaseIdentity::new();
    let mut changes = new_map();
    changes.insert(
        "identity".to_string(),
        QueryValue::Str(identity.to_string()),
    );
    let changes = QueryValue::Map(changes);
    let update = UpdateOp {
        update: TableRef::new(TABLE_DATABASES),
        where_clause: Some(Filter::Eq {
            field: vec!["name".to_string()],
            value: FilterValue::String(name.to_string()),
        }),
        set: changes.clone(),
        select: None,
        expected_version: Some(version),
    };

    match store.update_via_implicit_tx(&table, &update).await {
        Ok(result) if result.affected == 1 => {
            store.persist_table_interner(&table, Some(&changes)).await?;
            store.flush_system_repo_durable(&table).await?;
            Ok(identity)
        }
        Ok(result) => Err(DbError::Internal(format!(
            "database '{name}' identity update affected {} records; expected exactly one",
            result.affected
        ))),
        Err(error) if is_version_conflict(&error) => {
            let (winner, winner_id, _) = store.database_row_with_version(name).await?;
            match winner
                .as_ref()
                .zip(winner_id)
                .map(|(record, id)| identity_from_record(record, name, id))
                .transpose()?
            {
                Some(Some(identity)) => {
                    if database_identity_in_use(store, name, identity).await? {
                        return Err(DbError::Internal(format!(
                            "database '{name}' identity {identity} is already assigned to another database record"
                        )));
                    }
                    store.flush_system_repo_durable(&table).await?;
                    Ok(identity)
                }
                _ => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

async fn database_identity_in_use(
    store: &SystemStore,
    database: &str,
    identity: DatabaseIdentity,
) -> DbResult<bool> {
    let databases = store.load_databases().await?;
    for record in databases {
        let Some(name) = record.get("name").and_then(QueryValue::as_str) else {
            return Err(DbError::Internal(
                "corrupt database catalogue row has no name".into(),
            ));
        };
        if name == database {
            continue;
        }
        let Some(record_identity) = record.get("identity") else {
            continue;
        };
        let record_identity = record_identity.as_str().ok_or_else(|| {
            DbError::Internal(format!(
                "corrupt database '{name}' catalogue identity is not a string"
            ))
        })?;
        if record_identity
            .parse::<DatabaseIdentity>()
            .map_err(|error| {
                DbError::Internal(format!(
                    "corrupt database '{name}' catalogue identity: {error}"
                ))
            })?
            == identity
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn identity_from_record(
    record: &QueryValue,
    database: &str,
    record_id: RecordId,
) -> DbResult<Option<DatabaseIdentity>> {
    let Some(value) = record.get("identity") else {
        return Ok(None);
    };
    let value = value.as_str().ok_or_else(|| {
        DbError::Internal(format!(
            "corrupt database '{database}' record {record_id}: identity field is not a string"
        ))
    })?;
    DatabaseIdentity::from_str(value)
        .map(Some)
        .map_err(|error| {
            DbError::Internal(format!(
                "corrupt database '{database}' record {record_id}: invalid identity: {error}"
            ))
        })
}

fn is_version_conflict(error: &DbError) -> bool {
    error.code() == Some("version_conflict")
        || matches!(error, DbError::Internal(message) if message.contains("version_conflict"))
}
