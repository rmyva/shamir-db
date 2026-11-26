//! No-follow, plain-regular-file-only open of snapshot control and payload
//! names, shared by verify, restore staging, stamp inspection and the copy
//! walk.

use std::fs::{self, File};
use std::io;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

// errno an `open(O_NOFOLLOW)` of a symbolic-link leaf reports: ELOOP on Linux
// and Android (40) and on macOS, iOS and the BSDs (62); FreeBSD reports EMLINK
// (31) for it. It is turned into the helper's own `InvalidData` refusal below,
// so every caller sees the same typed "not a regular file" shape on Unix and
// Windows.
#[cfg(all(unix, any(target_os = "linux", target_os = "android")))]
const SYMLINK_LEAF_ERRNO: i32 = 40;
#[cfg(all(unix, target_os = "freebsd"))]
const SYMLINK_LEAF_ERRNO: i32 = 31;
#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_os = "freebsd"))
))]
const SYMLINK_LEAF_ERRNO: i32 = 62;

// O_NOFOLLOW values for the platforms whose libc headers fix them; a
// platform neither listed nor Windows gets NO no-follow flag and must be
// added here before relying on this helper there.
#[cfg(unix)]
fn no_follow_flags(options: &mut fs::OpenOptions) {
    // O_NONBLOCK as well: a FIFO leaf with no writer must not block the open;
    // it opens at once and is then refused by the handle-type check below. It
    // has no effect on reads of regular files.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    options.custom_flags(0x20000 | 0x800); // O_NOFOLLOW | O_NONBLOCK
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    options.custom_flags(0x100 | 0x4); // O_NOFOLLOW | O_NONBLOCK
}

/// A writer into the snapshot directory can replace control or payload names
/// between checks: the same-hole class, not a new escalation. Open the leaf
/// once, without following links, and validate the opened object.
///
/// Windows: no `O_NOFOLLOW`, so the leaf is opened with
/// `FILE_FLAG_OPEN_REPARSE_POINT` — the reparse point itself is opened, never
/// its target — and the resulting handle's metadata is then checked for
/// `FILE_ATTRIBUTE_REPARSE_POINT` (symlinks, mount points, AppExecLinks and
/// any other reparse-tagged object are all refused). `is_file()` on that
/// handle metadata rejects directories and devices; the handle (not the
/// path) is the object everything downstream reads, hashes and copies.
pub(crate) fn open_plain_file(path: &Path) -> io::Result<File> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    no_follow_flags(&mut options);
    #[cfg(windows)]
    {
        options.custom_flags(0x0020_0000); // FILE_FLAG_OPEN_REPARSE_POINT
    }
    let file = match options.open(path) {
        Ok(file) => file,
        #[cfg(unix)]
        Err(error) if error.raw_os_error() == Some(SYMLINK_LEAF_ERRNO) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot control/data file is a symbolic link",
            ));
        }
        Err(error) => return Err(error),
    };
    let metadata = file.metadata()?;
    #[cfg(windows)]
    {
        // FILE_ATTRIBUTE_REPARSE_POINT
        if metadata.file_attributes() & 0x0400 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot control/data file is a reparse point",
            ));
        }
    }
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot control/data file is not regular",
        ));
    }
    Ok(file)
}

/// Re-exported through `backup` so `restore.rs`, `staged_listing.rs` and
/// `format_stamp::inspect` share one mechanism without widening the public
/// surface.
pub(crate) fn open_plain_snapshot_file(path: &Path) -> io::Result<File> {
    open_plain_file(path)
}
