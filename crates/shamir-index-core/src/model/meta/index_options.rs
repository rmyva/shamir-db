//! Versioned descriptor options; a future `api_key_ref` will live in this map.
//! The descriptor layout stays unchanged and unknown versions remain opaque.

use crate::kind::VectorQuantization;
use serde::{Deserialize, Serialize};
use shamir_storage::error::DbError;

const VERSION: u8 = 1;

#[derive(Default, Serialize, Deserialize)]
struct IndexOptionsV1 {
    #[serde(default)]
    vector_quantization: Option<String>,
}

pub(super) fn decode(bytes: &[u8]) -> Result<Option<VectorQuantization>, DbError> {
    let Some((&version, payload)) = bytes.split_first() else {
        return Ok(None);
    };
    if version != VERSION {
        log::warn!("unknown index options version {version}; preserving opaque bytes");
        return Ok(None);
    }
    if !matches!(payload.first(), Some(0x80..=0x8f | 0xde | 0xdf)) {
        return Err(DbError::Codec("index options must be a map".into()));
    }
    let mut cursor = std::io::Cursor::new(payload);
    let options = IndexOptionsV1::deserialize(&mut rmp_serde::Deserializer::new(&mut cursor))
        .map_err(|e| DbError::Codec(format!("index options decode failed: {e}")))?;
    if cursor.position() != payload.len() as u64 {
        return Err(DbError::Codec("trailing index options bytes".into()));
    }
    match options.vector_quantization.as_deref() {
        None => Ok(None),
        Some("sq8") => Ok(Some(VectorQuantization::Sq8)),
        Some(value) => {
            log::warn!("unknown index vector quantization {value}");
            Ok(None)
        }
    }
}

pub(super) fn write(
    bytes: &mut Vec<u8>,
    quantization: Option<VectorQuantization>,
) -> Result<(), DbError> {
    if bytes.first().is_some_and(|version| *version != VERSION) {
        log::warn!(
            "unknown index options version {}; preserving opaque bytes",
            bytes[0]
        );
        return Ok(());
    }
    if bytes.is_empty() && quantization.is_none() {
        return Ok(());
    }
    // Decode first so malformed existing data is never silently replaced.
    if !bytes.is_empty() && decode(bytes)? == quantization {
        return Ok(());
    }
    if let Some(VectorQuantization::Sq8) = quantization {
        if bytes.is_empty() {
            let options = IndexOptionsV1 {
                vector_quantization: Some("sq8".into()),
            };
            let payload = rmp_serde::to_vec_named(&options)
                .map_err(|e| DbError::Codec(format!("index options encode failed: {e}")))?;
            *bytes = std::iter::once(VERSION).chain(payload).collect();
        } else {
            update_map(bytes)?;
        }
    }
    Ok(())
}

// Preserve future fields as their original MessagePack bytes, even when the
// quantization field must change. Decode each value to locate its boundary.
fn update_map(bytes: &mut Vec<u8>) -> Result<(), DbError> {
    let payload = &bytes[1..];
    let (count, header_len) = match payload.first().copied() {
        Some(tag @ 0x80..=0x8f) => (u32::from(tag & 15), 1),
        Some(0xde) if payload.len() >= 3 => {
            (u32::from(u16::from_be_bytes([payload[1], payload[2]])), 3)
        }
        Some(0xdf) if payload.len() >= 5 => (
            u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]),
            5,
        ),
        _ => return Err(DbError::Codec("index options must be a map".into())),
    };
    let mut cursor = std::io::Cursor::new(&payload[header_len..]);
    let mut entries = Vec::new();
    let mut replaced = false;
    for _ in 0..count {
        let start = cursor.position() as usize;
        let key = String::deserialize(&mut rmp_serde::Deserializer::new(&mut cursor))
            .map_err(|e| DbError::Codec(format!("index options key: {e}")))?;
        let value_start = cursor.position() as usize;
        serde::de::IgnoredAny::deserialize(&mut rmp_serde::Deserializer::new(&mut cursor))
            .map_err(|e| DbError::Codec(format!("index options value: {e}")))?;
        let end = cursor.position() as usize;
        let body = &payload[header_len..];
        entries.extend_from_slice(&body[start..value_start]);
        if key == "vector_quantization" {
            entries.extend_from_slice(&[0xa3, b's', b'q', b'8']);
            replaced = true;
        } else {
            entries.extend_from_slice(&body[value_start..end]);
        }
    }
    if cursor.position() as usize != payload.len() - header_len {
        return Err(DbError::Codec("trailing index options bytes".into()));
    }
    let count = if replaced {
        count
    } else {
        entries.extend(
            rmp_serde::to_vec("vector_quantization").map_err(|e| DbError::Codec(e.to_string()))?,
        );
        entries.extend_from_slice(&[0xa3, b's', b'q', b'8']);
        count
            .checked_add(1)
            .ok_or_else(|| DbError::Codec("index options map too large".into()))?
    };
    let mut output = vec![VERSION];
    if count <= 15 {
        output.push(0x80 | count as u8);
    } else if count <= u16::MAX.into() {
        output.push(0xde);
        output.extend_from_slice(&(count as u16).to_be_bytes());
    } else {
        output.push(0xdf);
        output.extend_from_slice(&count.to_be_bytes());
    }
    output.extend(entries);
    *bytes = output;
    Ok(())
}
