use crate::engine::table::TableManager;
use crate::shamir_db::system_store::{FormatEpoch, SchemaMigrationContext};
use crate::types::value::{InnerValue, QueryValue};
use futures::StreamExt;
use shamir_query_builder::{ddl, SchemaMigration};
use shamir_query_types::admin::types::schema_migration::{
    canonical_body_checksum, SchemaMigrationBody,
};
use shamir_types::codecs::interned::query_value_to_inner;
use shamir_types::types::record_id::RecordId;

pub(super) fn context() -> SchemaMigrationContext {
    SchemaMigrationContext::new(crate::access::Actor::System, FormatEpoch::Known(7))
}

pub(super) fn body(id: &str, ordinal: u64) -> SchemaMigrationBody {
    SchemaMigration::new(id, ordinal, format!("migration {ordinal}"))
        .op(ddl::create_table(format!("items_{ordinal}")).build())
        .expect("typed create-table operation is admitted")
        .build()
        .expect("test migration body is valid")
}

pub(super) fn checksum(body: &SchemaMigrationBody) -> [u8; 32] {
    canonical_body_checksum(body).expect("test migration body canonicalizes")
}

pub(super) async fn rows(table: &TableManager) -> Vec<(RecordId, QueryValue)> {
    let interner = table
        .info_store_read_view()
        .interner_read_view()
        .await
        .expect("journal interner is readable");
    let mut stream = Box::pin(table.list_stream(128));
    let mut records = Vec::new();
    while let Some(batch) = stream.next().await {
        for (id, record) in batch.expect("journal scan succeeds") {
            let value = match record {
                crate::engine::table::record_cow::RecordCow::Borrowed(bytes) => interner
                    .record_bytes_to_query_value(&bytes)
                    .expect("journal record materialization succeeds")
                    .expect("journal row is a supported value"),
                crate::engine::table::record_cow::RecordCow::Owned(value) => interner
                    .inner_to_query_value(&value)
                    .expect("journal record materializes"),
            };
            records.push((id, value));
        }
    }
    records
}

pub(super) async fn overwrite_row(table: &TableManager, id: RecordId, value: QueryValue) {
    let interner = table
        .interner_for_test()
        .get()
        .await
        .expect("test table interner is loaded");
    let inner: InnerValue =
        query_value_to_inner(&value, interner).expect("raw fixture value interns");
    table
        .set(id, &inner)
        .await
        .expect("raw fixture write succeeds");
}
