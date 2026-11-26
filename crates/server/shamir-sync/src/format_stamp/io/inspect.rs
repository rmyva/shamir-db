//! Read-only classification of the stamp location (design 2.5).

use std::fs;
use std::io::Read;
use std::path::Path;

use crate::format_stamp::codec::{decode, STAMP_FILE_NAME, STAMP_TMP_NAME};
use crate::format_stamp::model::{CorruptKind, FormatStamp, StampError};

/// State of the stamp location, computed without writing anything.
#[derive(Debug, Clone, PartialEq)]
pub enum StampState {
    /// No `format.stamp`; `leftover_tmp` marks an interrupted first write
    /// (the tmp is never read or trusted).
    Absent { leftover_tmp: bool },
    /// The stamp exists but does not decode.
    Corrupt { kind: CorruptKind },
    /// A frame this binary cannot fully read (design 2.5: the epoch comes
    /// from the header; the checksum is not verified).
    NewerLayout { layout: u16, epoch: u64 },
    /// A valid stamp.
    Valid(FormatStamp),
}

/// Classify `<dir>/format.stamp`. An unreadable stamp file is classified
/// corrupt (fail closed: the epoch cannot be determined).
pub fn inspect(dir: &Path) -> StampState {
    let stamp_path = dir.join(STAMP_FILE_NAME);
    let Ok(mut file) = crate::sync::durability::backup::open_plain_snapshot_file(&stamp_path)
    else {
        return match fs::symlink_metadata(&stamp_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => StampState::Absent {
                leftover_tmp: dir.join(STAMP_TMP_NAME).exists(),
            },
            _ => StampState::Corrupt {
                kind: CorruptKind::ShortFile,
            },
        };
    };
    let mut bytes = Vec::new();
    if file.read_to_end(&mut bytes).is_err() {
        return StampState::Corrupt {
            kind: CorruptKind::ShortFile,
        };
    }
    match decode(&bytes, dir) {
        Ok(stamp) => StampState::Valid(stamp),
        Err(StampError::Corrupt { kind, .. }) => StampState::Corrupt { kind },
        Err(StampError::NewerLayout { layout, epoch, .. }) => {
            StampState::NewerLayout { layout, epoch }
        }
        // decode maps every other failure to Corrupt; unreachable.
        Err(_) => StampState::Corrupt {
            kind: CorruptKind::BadBody,
        },
    }
}
