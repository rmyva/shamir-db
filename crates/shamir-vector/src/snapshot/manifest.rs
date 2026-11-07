use super::SnapshotError;
use bytes::Bytes;
use futures::StreamExt;
use shamir_storage::types::{KvOp, RecordKey, Store};

const ORPHAN_SWEEP_BATCH: usize = 128;

/// Remove chunk and sidecar records for generations other than the verified active one.
pub(super) async fn sweep_orphan_generations(
    store: &std::sync::Arc<dyn Store>,
    keyspace: &str,
    active_gen: u32,
) -> Result<(), SnapshotError> {
    let prefix = Bytes::from(format!("{keyspace}.g"));
    let mut stream = store.scan_prefix_stream(prefix, ORPHAN_SWEEP_BATCH);
    let mut removals = Vec::with_capacity(ORPHAN_SWEEP_BATCH);
    while let Some(batch) = stream.next().await {
        for (key, _) in batch? {
            let raw = key.as_ref();
            let text = std::str::from_utf8(raw).map_err(|error| {
                SnapshotError::Corrupt(format!("snapshot key not utf8: {error}"))
            })?;
            let Some(rest) = text.strip_prefix(&format!("{keyspace}.g")) else {
                continue;
            };
            let Some((gen_text, suffix)) = rest.split_once('.') else {
                continue;
            };
            let Ok(gen) = gen_text.parse::<u32>() else {
                continue;
            };
            if gen == active_gen || !is_generation_record(suffix) {
                continue;
            }
            removals.push(KvOp::Remove(key));
            if removals.len() == ORPHAN_SWEEP_BATCH {
                store.transact(std::mem::take(&mut removals)).await?;
            }
        }
    }
    if !removals.is_empty() {
        store.transact(removals).await?;
    }
    Ok(())
}

fn is_generation_record(suffix: &str) -> bool {
    if suffix == "sidecar" {
        return true;
    }
    let Some((section, index)) = suffix.rsplit_once('.') else {
        return false;
    };
    matches!(section, "graph" | "data" | "qgraph" | "qdata")
        && index.len() == 6
        && index.bytes().all(|byte| byte.is_ascii_digit())
}

// CRC detects accidental corruption, not malicious modification.
pub(super) fn integrity_key(keyspace: &str) -> RecordKey {
    Bytes::from(format!("{keyspace}.integrity")).into()
}

pub(super) fn integrity_bytes(manifest: &[u8], sidecar: &[u8]) -> Bytes {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&(manifest.len() as u64).to_le_bytes());
    hasher.update(manifest);
    hasher.update(sidecar);
    let mut result = b"SVI\x01".to_vec();
    result.extend_from_slice(&hasher.finalize().to_le_bytes());
    result.into()
}

pub(super) fn verify_integrity(
    bytes: &[u8],
    manifest: &[u8],
    sidecar: &[u8],
) -> Result<(), SnapshotError> {
    if bytes != integrity_bytes(manifest, sidecar).as_ref() {
        return Err(SnapshotError::Corrupt(
            "snapshot metadata integrity mismatch".into(),
        ));
    }
    Ok(())
}

/// Reject names that are not a portable single filename component, including
/// Windows device names (also reserved when followed by an extension).
pub(super) fn validate_basename(name: &str) -> Result<(), SnapshotError> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.ends_with(['.', ' '])
        || name.chars().any(|c| {
            c.is_control() || matches!(c, '/' | '\\' | ':' | '<' | '>' | '"' | '|' | '?' | '*')
        })
    {
        return Err(SnapshotError::Corrupt("invalid graph basename".into()));
    }
    let stem = name.split('.').next().unwrap_or("").trim_end_matches(' ');
    let upper = stem.to_ascii_uppercase();
    let reserved = matches!(
        upper.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) || ["COM", "LPT"].iter().any(|prefix| {
        upper.strip_prefix(prefix).is_some_and(|suffix| {
            matches!(
                suffix,
                "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
            )
        })
    });
    if reserved {
        return Err(SnapshotError::Corrupt("reserved graph basename".into()));
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub struct OldGenerationChunks {
    pub graph: u32,
    pub data: u32,
    pub qgraph: u32,
    pub qdata: u32,
}
