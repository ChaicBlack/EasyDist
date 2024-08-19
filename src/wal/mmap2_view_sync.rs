use memmap2::{MmapMut, MmapOptions};
use std::cell::UnsafeCell;
use std::fmt;
use std::fs::File;
use std::io::Result;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

/// ported from https://github.com/danburkert/memmap-rs in version 0.5.2

/// A thread-safe view of a memory map.
///
/// The view may be split into disjoint ranges, each of which will share the
/// underlying memory map.
pub struct MmapViewSync {
    inner: Arc<UnsafeCell<MmapMut>>,
    offset: usize,
    len: usize,
}

impl MmapViewSync {
    /// Create a memory map from "file". The map start at byte "offset"
    /// from the beginning of the file.
    /// The length is indicated by "capacity".
    ///
    /// If there is any underlying system call fails, this will return an error.
    pub(crate) fn from_file(file: &File, offset: usize, capacity: usize) -> Result<MmapViewSync> {
        let mmap = unsafe {
            MmapOptions::new()
                .offset(offset as u64)
                .len(capacity)
                .map_mut(file)?
        };

        Ok(mmap.into())
    }

    /// Create an anonymous memory map.
    ///
    /// Return an error if any underlying system call fails.
    #[allow(dead_code)]
    pub(crate) fn anonymous(capacity: usize) -> Result<MmapViewSync> {
        let mmap = MmapOptions::new().len(capacity).map_anon()?;

        Ok(mmap.into())
    }

    /// Split the view into disjoint pieces at the specified offset.
    ///
    /// The provided offset must be less than the view's length.
    #[allow(dead_code)]
    pub(crate) fn split_at(self, offset: usize) -> Result<(MmapViewSync, MmapViewSync)> {
        if self.len < offset {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "mmap view split offset must be less than the view length",
            ));
        }
        let MmapViewSync {
            inner,
            offset: self_offset,
            len: self_len,
        } = self;
        Ok((
            MmapViewSync {
                inner: inner.clone(),
                offset: self_offset,
                len: offset,
            },
            MmapViewSync {
                inner,
                offset: self_offset + offset,
                len: self_len - offset,
            },
        ))
    }

    /// Restricts the range of this view to the provided offset and length based on original offset
    /// and view.
    ///
    /// New offset will be self.offset + offset(provided) and the new len is the provided len.
    ///
    /// The provided range must be a subset of the current range (`offset + len < view.len()`).
    pub(crate) fn restrict(&mut self, offset: usize, len: usize) -> Result<()> {
        if offset + len > self.len {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "mmap view may only be restricted to a subrange \
                                       of the current view",
            ));
        }
        self.offset += offset;
        self.len = len;
        Ok(())
    }

    /// Get a reference to the inner mmap.
    ///
    /// ## Unsafety
    ///
    /// The caller must ensure that the file is not concurrently modified.
    /// The caller must ensure that memory outside the `offset`/`len` range is not accessed.
    fn inner(&self) -> &MmapMut {
        unsafe { &*self.inner.get() }
    }

    /// Get a mutable reference to the inner mmap.
    ///
    /// ## Unsafety
    ///
    /// The caller must ensure that the file is not concurrently modified.
    /// The caller must ensure that memory outside the `offset`/`len` range is not accessed.
    fn inner_mut(&mut self) -> &mut MmapMut {
        unsafe { &mut *self.inner.get() }
    }

    /// Flushes outstanding view modifications to disk.
    ///
    /// When this returns with a non-error result, all outstanding changes to a file-backed memory
    /// map view are guaranteed to be durably stored. The file's metadata (including last
    /// modification timestamp) may not be updated.
    pub(crate) fn flush(&self) -> Result<()> {
        self.inner().flush_range(self.offset, self.len)
    }

    /// Returns the length of the memory map view.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Returns the memory mapped file as an immutable slice.
    ///
    /// ## Unsafety
    ///
    /// The caller must ensure that the file is not concurrently modified.
    pub(crate) unsafe fn as_slice(&self) -> &[u8] {
        &self.inner()[self.offset..self.offset + self.len]
    }

    /// Returns the memory mapped file as a mutable slice.
    ///
    /// ## Unsafety
    ///
    /// The caller must ensure that the file is not concurrently accessed.
    pub(crate) unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        let (offset, len) = (self.offset, self.len);
        &mut self.inner_mut()[offset..offset + len]
    }

    /// Clones the view of the memory map.
    ///
    /// The underlying memory map is shared, and thus the caller must ensure that the memory
    /// underlying the view is not illegally aliased.
    pub(crate) unsafe fn clone(&self) -> MmapViewSync {
        MmapViewSync {
            inner: self.inner.clone(),
            offset: self.offset,
            len: self.len,
        }
    }
}

impl From<MmapMut> for MmapViewSync {
    fn from(mmap: MmapMut) -> MmapViewSync {
        let len = mmap.len();
        MmapViewSync {
            #[allow(clippy::arc_with_non_send_sync)]
            inner: Arc::new(UnsafeCell::new(mmap)),
            offset: 0,
            len,
        }
    }
}

impl fmt::Debug for MmapViewSync {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MmapViewSync {{ offset: {}, len: {} }}",
            self.offset, self.len
        )
    }
}

#[cfg(test)]
unsafe impl Sync for MmapViewSync {}
unsafe impl Send for MmapViewSync {}
