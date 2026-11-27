use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize};
use serde_bytes::ByteBuf;
use shamir_tx::{ChangeOp, ChangelogEvent, RecordChange};
use shamir_types::access::Actor;

/// Pull-only envelope; the durable journal and live changefeed stay unchanged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReplicatedEvent {
    #[serde(flatten)]
    pub event: ChangelogEvent,
    pub field_bindings: Vec<(u64, String)>,
}

impl<'de> Deserialize<'de> for ReplicatedEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // ChangelogEvent's key adapter asks for Vec<u8> (deserialize_seq).
        // Flatten buffers binary keys as serde Content::Bytes/ByteBuf, which
        // reject deserialize_seq. Keep this binary-aware adapter Pull-local.
        #[derive(Deserialize)]
        struct WireChange {
            table: String,
            key: ByteBuf,
            op: ChangeOp,
            value: Option<ByteBuf>,
        }

        #[derive(Deserialize)]
        struct WireEvent {
            repo: String,
            commit_version: u64,
            tx_id: u64,
            actor: Actor,
            timestamp_ns: u64,
            changes: Vec<WireChange>,
            // Legacy envelopes default to empty; referenced ids are checked
            // by apply preflight, not by rejecting scalar Puts or Deletes.
            #[serde(default)]
            field_bindings: Vec<(u64, String)>,
        }

        let wire = WireEvent::deserialize(deserializer)?;
        Ok(Self {
            event: ChangelogEvent {
                repo: wire.repo,
                commit_version: wire.commit_version,
                tx_id: wire.tx_id,
                actor: wire.actor,
                timestamp_ns: wire.timestamp_ns,
                changes: wire
                    .changes
                    .into_iter()
                    .map(|change| RecordChange {
                        table: change.table,
                        key: Bytes::from(change.key.into_vec()),
                        op: change.op,
                        value: change.value.map(|value| Bytes::from(value.into_vec())),
                    })
                    .collect(),
            },
            field_bindings: wire.field_bindings,
        })
    }
}
