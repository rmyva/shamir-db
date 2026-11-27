use std::fs::{self, File, Metadata};
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
use std::path::Path;

use crate::sync::durability::backup::online::progress::CancelFlag;

pub(super) const ROOT_ENTRIES: usize = 4_096;
const TREE_ENTRIES: usize = 100_000;
const PASS_ENTRIES: usize = 1_000_000;
const PASS_BYTES: u64 = 1 << 40;

#[derive(Debug)]
pub(super) enum Refusal {
    Unverifiable,
    Incomplete,
    Budget,
    Cancelled,
}

pub(super) struct Budget<'a> {
    cancel: &'a CancelFlag,
    entries_left: usize,
    bytes_left: u64,
    #[cfg(test)]
    arm_on_probe: bool,
}

impl<'a> Budget<'a> {
    pub(super) fn new(cancel: &'a CancelFlag) -> Self {
        Self::with_bytes(cancel, PASS_BYTES)
    }

    pub(super) fn with_bytes(cancel: &'a CancelFlag, bytes_left: u64) -> Self {
        Self {
            cancel,
            entries_left: PASS_ENTRIES,
            bytes_left,
            #[cfg(test)]
            arm_on_probe: false,
        }
    }

    /// Test-only: the cancellation probe arms the flag the first time it is
    /// consulted, so the pass's checkpoints see an uncancelled token and only
    /// the consumer of the probe (the verifier's per-chunk hash loop) can
    /// produce a cancellation.
    #[cfg(test)]
    pub(super) fn with_probe_arming(cancel: &'a CancelFlag, bytes_left: u64) -> Self {
        Self {
            cancel,
            entries_left: PASS_ENTRIES,
            bytes_left,
            arm_on_probe: true,
        }
    }

    pub(super) fn checkpoint(&self) -> Result<(), Refusal> {
        if self.cancel.is_cancelled() {
            return Err(Refusal::Cancelled);
        }
        Ok(())
    }

    /// The same flag [`Self::checkpoint`] tests, exposed as a plain probe so
    /// a long streaming hash can poll it between chunks instead of only at
    /// the pass's coarse checkpoints.
    pub(super) fn cancelled(&self) -> bool {
        #[cfg(test)]
        if self.arm_on_probe {
            self.cancel.cancel();
        }
        self.cancel.is_cancelled()
    }

    pub(super) fn entry(&mut self, count: usize) -> Result<(), Refusal> {
        self.checkpoint()?;
        if count >= TREE_ENTRIES {
            return Err(Refusal::Budget);
        }
        self.entries_left = self.entries_left.checked_sub(1).ok_or(Refusal::Budget)?;
        Ok(())
    }

    pub(super) fn bytes(&mut self, size: u64) -> Result<(), Refusal> {
        self.bytes_left = self.bytes_left.checked_sub(size).ok_or(Refusal::Budget)?;
        Ok(())
    }

    pub(super) fn read(
        &mut self,
        path: &Path,
        root_meta: &Metadata,
        limit: u64,
    ) -> Result<Vec<u8>, Refusal> {
        self.checkpoint()?;
        let meta = metadata(path)?;
        if !meta.is_file() || !plain_on_root(&meta, root_meta) {
            return Err(Refusal::Unverifiable);
        }
        if meta.len() > limit {
            return Err(Refusal::Budget);
        }
        // Reserve one extra byte to detect a size change without an unbounded read.
        self.bytes(meta.len() + 1)?;
        let mut bytes = Vec::new();
        File::open(path)
            .and_then(|file| file.take(meta.len() + 1).read_to_end(&mut bytes))
            .map_err(|_| Refusal::Incomplete)?;
        if bytes.len() as u64 != meta.len() {
            return Err(Refusal::Incomplete);
        }
        Ok(bytes)
    }
}

pub(super) fn metadata(path: &Path) -> Result<Metadata, Refusal> {
    fs::symlink_metadata(path).map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => Refusal::Unverifiable,
        _ => Refusal::Incomplete,
    })
}

pub(super) fn plain_on_root(meta: &Metadata, root_meta: &Metadata) -> bool {
    if !meta.is_file() && !meta.is_dir() {
        return false;
    }
    #[cfg(unix)]
    {
        // A same-device bind mount is indistinguishable here; operators
        // must not mount anything inside the managed root.
        meta.dev() == root_meta.dev()
    }
    #[cfg(windows)]
    {
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        (meta.file_attributes() | root_meta.file_attributes()) & FILE_ATTRIBUTE_REPARSE_POINT == 0
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = root_meta;
        false
    }
}
