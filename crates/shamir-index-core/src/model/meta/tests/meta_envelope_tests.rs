//! Regression test RV-STORAGE-CCF2090E — header validation must precede
//! payload decode in `MetaEnvelope::open`.

use crate::model::meta::meta_envelope::{MetaEnvelope, MetaError};

#[test]
fn rv_storage_ccf2090e_open_reports_unsupported_version_before_payload_decode() {
    let mut bytes = MetaEnvelope::<String>::new("payload".to_string())
        .encode()
        .expect("encode");

    // Corrupt payload region (tail) so full-struct decode cannot succeed
    // under the current schema: bincode String is u64 len + bytes.
    let tail_start = bytes.len() - 3;
    for b in &mut bytes[tail_start..] {
        *b = 0xFF;
    }

    // Flip version bytes (offset: magic 4 + version 2, little-endian u16).
    bytes[4] = 0xFF;
    bytes[5] = 0xFF;

    match MetaEnvelope::<String>::open(&bytes) {
        Err(MetaError::UnsupportedVersion(0xFFFF)) => {}
        other => panic!("expected UnsupportedVersion(0xFFFF), got {:?}", other.err()),
    }

    // Sanity: version-only mutation (intact payload) also reports UnsupportedVersion.
    let mut vbytes = MetaEnvelope::<String>::new("payload".to_string())
        .encode()
        .expect("encode");
    vbytes[4] = 0xFF;
    vbytes[5] = 0xFF;
    match MetaEnvelope::<String>::open(&vbytes) {
        Err(MetaError::UnsupportedVersion(0xFFFF)) => {}
        other => panic!(
            "expected UnsupportedVersion(0xFFFF) for intact payload, got {:?}",
            other.err()
        ),
    }
}
