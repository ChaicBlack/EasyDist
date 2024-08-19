use log::{debug, error, log_enabled, trace};
use std::cmp::Ordering;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::ptr;
use std::thread;
#[cfg(target_os = "windows")]
use std::time::Duration;

use super::MmapViewSync;
use byteorder::{ByteOrder, LittleEndian};
#[cfg(not(unix))]
use fs4::FileExt;

/// The magic bytes of the segment header.
const SEGMENT_MAGIC: &[u8; 3] = b"wal";
/// The version tag of the segment header.
const SEGMENT_VERSION: u8 = 0;

/// The length of both the segment and entry header.
const HEADER_LEN: usize = 8;

/// The length of a CRC value.
const CRC_LEN: usize = 4;

pub struct Entry {
    view: MmapViewSync,
}

impl Deref for Entry {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        unsafe { self.view.as_slice() }
    }
}

impl fmt::Debug for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Entry {{ len: {} }}", self.view.len())
    }
}

/// An append-only, fixed-length, durable container of entries.
///
/// The segment on-disk format is as simple as possible, while providing
/// backwards compatibility, protection against corruption, and alignment
/// guarantees.
///
/// A version tag allows the internal format to be updated, while still
/// maintaining compatibility with previously written files. CRCs are used
/// to ensure that entries are not corrupted. Padding is used to ensure that
/// entries are always aligned to 8-byte boundaries.
///
/// ## On Disk Format
///
/// All version, length, and CRC integers are serialized in little-endian format.
///
/// All CRC values are computed using
/// [CRC32-C](https://en.wikipedia.org/wiki/Cyclic_redundancy_check).
///
/// ### Segment Header Format
///
/// | component              | type    |
/// | ---------------------- | ------- |
/// | magic bytes ("wal")    | 3 bytes |
/// | segment format version | u8      |
/// | random CRC seed        | u32     |
///
/// The segment header is 8 bytes long: three magic bytes ("wal") followed by a
/// segment format version `u8`, followed by a random `u32` CRC seed. The CRC
/// seed ensures that if a segment file is reused for a new segment, the old
/// entries will be ignored (since the CRC will not match).
///
/// ### Entry Format
///
/// | component                    | type |
/// | ---------------------------- | ---- |
/// | length                       | u64  |
/// | data                         |      |
/// | padding                      |      |
/// | CRC(length + data + padding) | u32  |
///
/// Entries are serialized to the log with a fixed-length header, followed by
/// the data itself, and finally a variable length footer. The header includes
/// the length of the entry as a u64. The footer includes between 0 and 7 bytes
/// of padding to extend the total length of the entry to a multiple of 8,
/// followed by the CRC code of the length, data, and padding.
///
/// ### Logging
///
/// Segment modifications are logged through the standard Rust [logging
/// facade](https://crates.io/crates/log/). Metadata operations (create, open,
/// resize, file rename) are logged at `info` level, flush events are logged at
/// `debug` level, and entry events (append and truncate) are logged at `trace`
/// level. Long-running or multi-step operations will log a message at a lower
/// level when beginning, with a final completion message at a higher level.
pub struct Segment {
    /// The segment file buffer.
    mmap: MmapViewSync,
    /// The segment file path.
    path: PathBuf,
    /// Index of entry offset and lengths.
    index: Vec<(usize, usize)>,
    /// The crc of the last appended entry.
    crc: u32,
    /// Offset of last flush.
    flush_offset: usize,
}

impl Segment {
    pub fn create<P>(path: P, capacity: usize) -> Result<Segment>
    where
        P: AsRef<Path>,
    {
        // Get the &str from 'path'.
        let file_name = path
            .as_ref()
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .expect("Path to WAL segment file provided");

        // Create a path of temporary file which is in the same directory of path.
        let tmp_file_path = match path.as_ref().parent() {
            Some(parent) => parent.join(format!("tmp-{file_name}")),
            None => PathBuf::from(format!("tmp-{file_name}")),
        };

        // Round the capacity to nearest 8-byte alignment, because the segment would
        // not use any space lower than 8 bytes.
        let capacity = capacity & !7;
        // Check the capacity.
        if capacity < HEADER_LEN {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("invalid segment capacity: {capacity}"),
            ));
        }

        let seed = rand::random();

        {
            // Prepare properly formatted segment in a temporary file, so in case of failure it
            // won't be corrupted.
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                // Don't truncate now, we do it manually later to allocate the right size directly
                .truncate(false)
                .open(&tmp_file_path)?;

            // fs4 prevides some cross-platform bindings which help for Windows.
            #[cfg(not(unix))]
            {
                file.allocate(capacity as u64)?;
            }
            // For all linux system WAL can just use ftruncate directly.
            #[cfg(unix)]
            {
                rustix::fs::ftruncate(&file, capacity as u64)?;
            }

            // Create the mmap and set the header(magic number, version and seed)
            let mut mmap = MmapViewSync::from_file(&file, 0, capacity)?;
            {
                let segment = unsafe { &mut mmap.as_mut_slice() };
                copy_memory(SEGMENT_MAGIC, segment);
                segment[3] = SEGMENT_VERSION;
                LittleEndian::write_u32(&mut segment[4..], seed);
            }

            // A successful close does not guarantee that the data has been successfully saved to disk, as the kernel defers writes.
            // So we need to flush magic header manually to ensure that it is written to disk.
            mmap.flush()?;

            #[cfg(target_os = "windows")]
            {
                file.sync_all()?;
            }
        };

        // Rename the temporary file to the final file.
        fs::rename(&tmp_file_path, &path)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)?;

        let mmap = MmapViewSync::from_file(&file, 0, capacity)?;

        let segment = Segment {
            mmap,
            path: path.as_ref().to_path_buf(),
            index: Vec::new(),
            crc: seed,
            flush_offset: 0,
        };

        debug!("{:?}: created", segment);

        Ok(segment)
    }

    pub fn open<P>(path: P) -> Result<Segment>
    where
        P: AsRef<Path>,
    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)?;
        let capacity = file.metadata()?.len();
        if capacity > usize::MAX as u64 || capacity < HEADER_LEN as u64 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("invalid segment capacity: {capacity}"),
            ));
        }

        let capacity = capacity as usize & !7;
        let mmap = MmapViewSync::from_file(&file, 0, capacity)?;

        let mut index = Vec::new();
        let mut crc;

        {
            // Parse the segment, filling out the index containning the offset
            // and length of each entry, as well as the latest CRC value.
            //
            // If the CRC of any entry does not match, then parsing stops and
            // the remainder of the file is considered empty.
            let segment = unsafe { mmap.as_slice() };

            if &segment[0..3] != SEGMENT_MAGIC {
                return Err(Error::new(ErrorKind::InvalidData, "Iilegal segment header"));
            }

            if segment[3] != 0 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Segment version unsupported: {}", segment[3]),
                ));
            }

            crc = LittleEndian::read_u32(&segment[4..]);
            let mut offset = HEADER_LEN;

            while offset + HEADER_LEN + CRC_LEN < capacity {
                let len = LittleEndian::read_u64(&segment[offset..]) as usize;
                let padding = padding(len);
                let padding_len = len + padding;
                if offset + HEADER_LEN + padding_len + CRC_LEN > capacity {
                    break;
                }
                // Calculate the crc for check.
                let entry_crc = crc32c::crc32c_append(
                    !crc.reverse_bits(),
                    &segment[offset..offset + HEADER_LEN + padding_len],
                );
                // Read the stored crc.
                let stored_crc =
                    LittleEndian::read_u32(&segment[offset + HEADER_LEN + padding_len..]);
                if entry_crc != stored_crc {
                    if stored_crc != 0 {
                        log::warn!(
                            "CRC mismatch at offset {}: {} != {}",
                            offset,
                            entry_crc,
                            stored_crc
                        );
                    }
                    break;
                }

                crc = entry_crc;
                index.push((offset + HEADER_LEN, len));
                offset += HEADER_LEN + padding_len + CRC_LEN;
            }
        }

        let segment = Segment {
            mmap,
            path: path.as_ref().to_path_buf(),
            index,
            crc,
            flush_offset: 0,
        };

        debug!("{:?}: opened", segment);
        Ok(segment)
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { self.mmap.as_slice() }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { self.mmap.as_mut_slice() }
    }

    /// Return the segment entry at specific index, or "None" if no such
    /// entry exists.
    pub fn entry(&self, index: usize) -> Option<Entry> {
        self.index.get(index).map(|&(offset, len)| {
            let mut view = unsafe { self.mmap.clone() };
            // restrict only fails on bounds errors, but an invariant of
            // Segment is that the index always holds valid offset and
            // length bounds.
            view.restrict(offset, len)
                .expect("illegal segment offset or length");
            Entry { view }
        })
    }

    pub fn append<T>(&mut self, entry: &T) -> Option<usize>
    where
        T: Deref<Target = [u8]>,
    {
        // Check if there is enough space.
        if !self.sufficient_capacity(entry.len()) {
            return None;
        }
        trace!("{:?}: appending {} byte entry", self, entry.len());

        // Calculate padding
        let padding = padding(entry.len());
        let padded_len = entry.len() + padding;

        // Get offset and crc
        let offset = self.size();
        let mut crc = self.crc;

        // Write the length to the header of entry
        LittleEndian::write_u64(&mut self.as_mut_slice()[offset..], entry.len() as u64);
        // Apply the entry to the segment
        copy_memory(
            entry.deref(),
            &mut self.as_mut_slice()[offset + HEADER_LEN..],
        );

        // Apply the padding to segment
        if padding > 0 {
            let zeroes: [u8; 8] = [0; 8];
            copy_memory(
                &zeroes[..padding],
                &mut self.as_mut_slice()[offset + HEADER_LEN + entry.len()..],
            );
        }

        // Calculate and apply the crc to segment
        crc = crc32c::crc32c_append(
            !crc.reverse_bits(),
            &self.as_slice()[offset..offset + HEADER_LEN + padded_len],
        );
        LittleEndian::write_u32(
            &mut self.as_mut_slice()[offset + HEADER_LEN + padded_len..],
            crc,
        );

        self.crc = crc;
        self.index.push((offset + HEADER_LEN, entry.len()));
        Some(self.index.len() - 1)
    }

    fn _read_seed_crc(&self) -> u32 {
        LittleEndian::read_u32(&self.as_slice()[4..])
    }

    fn _read_entry_crc(&self, entry_id: usize) -> u32 {
        let (offset, len) = self.index[entry_id];
        let padding = padding(len);
        let padded_len = len + padding;
        LittleEndian::read_u32(&self.as_slice()[offset + padded_len..])
    }

    /// Truncate the entries in the segment beginning with `from`.
    ///
    /// The entries are not guarenteed to be removed until the segment is
    /// flushed.
    pub fn truncate(&mut self, from: usize) {
        if from >= self.index.len() {
            return;
        }
        trace!("{:?}: truncating from position {}", self, from);

        let deleted = self.index.drain(from..).count();
        trace!("{:?}: truncated {} entries", self, deleted);

        // Update the crc
        if self.index.is_empty() {
            self.crc = self._read_seed_crc();
        } else {
            // read crc from last entry
            self.crc = self._read_entry_crc(self.index.len() - 1);
        }

        // And overwrite the existing data so that we will not read the data back after a crash.
        let size = self.size();
        let zeroes: [u8; 16] = [0; 16];
        copy_memory(&zeroes, &mut self.as_mut_slice()[size..]);
    }

    /// Flushes recently written entries to durable storage
    pub fn flush(&mut self) -> Result<()> {
        trace!("{:?}: flushing", self);
        let start = self.flush_offset;
        let end = self.size();

        match start.cmp(&end) {
            Ordering::Equal => {
                trace!("{:?}: nothing to flush", self);
                Ok(())
            } // nothing to flush
            Ordering::Less => {
                // flush new elements added since last flush
                trace!("{:?}: flushing byte range[{}, {}]", self, start, end);
                let mut view = unsafe { self.mmap.clone() };
                self.flush_offset = end;
                view.restrict(start, start - end)?;
                view.flush()
            }
            Ordering::Greater => {
                // most likely truncated in between flushes
                // register new flush offset & flush the whole segment
                trace!("{:?}: flushing after truncation", self);
                let view = unsafe { self.mmap.clone() };
                self.flush_offset = end;
                view.flush()
            }
        }
    }

    /// Flushes recently written entries to durable storage.
    pub fn flush_async(&mut self) -> thread::JoinHandle<Result<()>> {
        trace!("{:?}: async flushing", self);
        let start = self.flush_offset;
        let end = self.size();

        match start.cmp(&end) {
            Ordering::Equal => thread::spawn(move || Ok(())), // nothing to flush
            Ordering::Less => {
                // flush new elements added since last flush
                let mut view = unsafe { self.mmap.clone() };
                self.flush_offset = end;

                let log_msg = if log_enabled!(log::Level::Trace) {
                    format!(
                        "{:?}: async flushing byte range [{}, {})",
                        &self, start, end
                    )
                } else {
                    String::new()
                };

                thread::spawn(move || {
                    trace!("{}", log_msg);
                    view.restrict(start, end - start).and_then(|_| view.flush())
                })
            }
            Ordering::Greater => {
                // most likely truncated in between flushes
                // register new flush offset & flush the whole segment
                let view = unsafe { self.mmap.clone() };
                self.flush_offset = end;

                let log_msg = if log_enabled!(log::Level::Trace) {
                    format!("{:?}: async flushing after truncation", &self)
                } else {
                    String::new()
                };

                thread::spawn(move || {
                    trace!("{}", log_msg);
                    view.flush()
                })
            }
        }
    }

    /// Ensure that the segment can store an entry of the provided size.
    ///
    /// If the current segment length is insufficient then it is resized. This
    /// is potentially a very slow operation.
    pub fn ensure_capacity(&mut self, entry_size: usize) -> Result<()> {
        let required_capacity =
            entry_size + padding(entry_size) + HEADER_LEN + CRC_LEN + self.size();
        // Sanity check the 8-byte alignment invariant.
        assert_eq!(required_capacity & !7, required_capacity);
        if required_capacity > self.capacity() {
            debug!("{:?}: resizing to {} bytes", self, required_capacity);
            self.flush()?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .open(&self.path)?;
            // fs4 provides some cross-platform bindings which help for Windows.
            #[cfg(not(unix))]
            file.allocate(required_capacity as u64)?;
            // For all unix systems WAL can just use ftruncate directly
            #[cfg(unix)]
            {
                rustix::fs::ftruncate(&file, required_capacity as u64)?;
            }

            let mut mmap = MmapViewSync::from_file(&file, 0, required_capacity)?;
            mem::swap(&mut mmap, &mut self.mmap);
        }
        Ok(())
    }

    /// Returns the number of entries in the segment.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Returns true if the segment has no entries.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Returns the capacity (file-size) of the segment in bytes
    ///
    /// Each entry is stored with a header and padding, so the entire capacity will not
    /// be available for entry data.
    pub fn capacity(&self) -> usize {
        self.mmap.len()
    }

    /// Return the total number of bytes used to store existing entries,
    /// including header and padding overhead.
    pub fn size(&self) -> usize {
        self.index.last().map_or(HEADER_LEN, |&(offset, len)| {
            offset + len + padding(len) + CRC_LEN
        })
    }

    /// Returns 'true' if the segment has sufficient remaining capacity to
    /// append an entry of size `entry_len`.
    pub fn sufficient_capacity(&self, entry_len: usize) -> bool {
        (self.capacity() - self.size())
            .checked_sub(HEADER_LEN + CRC_LEN)
            .map_or(false, |rem| rem >= entry_len + padding(entry_len))
    }

    /// Returns the path to the segment file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Renames the segment file.
    ///
    /// The caller is responsible for syncing the directory in order to
    /// guarantee that the rename is durable in the event of a crash.
    pub fn rename<P>(&mut self, path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        debug!("{:?}: renaming file to {:?}", self, path.as_ref());
        fs::rename(&self.path, &path).map_err(|e| {
            error!("{:?}: failed to rename segment {}", self.path, e);
            e
        })?;
        self.path = path.as_ref().to_path_buf();
        Ok(())
    }

    /// Deletes the segment file.
    pub fn delete(self) -> Result<()> {
        debug!("{:?}: deleting file", self);

        #[cfg(not(target_os = "windows"))]
        {
            self.delete_unix()
        }

        #[cfg(target_os = "windows")]
        {
            self.delete_windows()
        }
    }

    #[cfg(not(target_os = "windows"))]
    fn delete_unix(self) -> Result<()> {
        fs::remove_file(&self.path).map_err(|e| {
            error!("{:?}: failed to delete segment {}", self, e);
            e
        })
    }

    #[cfg(target_os = "windows")]
    fn delete_windows(self) -> Result<()> {
        const DELETE_TRIES: u32 = 3;

        let Segment {
            mmap,
            path,
            index,
            flush_offset,
            ..
        } = self;
        let mmap_len = mmap.len();

        // Unmaps the file before `fs::remove_file` else access will be denied
        mmap.flush()?;
        std::mem::drop(mmap);

        let mut tries = 0;
        loop {
            tries += 1;
            match fs::remove_file(&path) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if tries >= DELETE_TRIES {
                        error!(
                            "{:?}: failed to delete segment {}",
                            // `self` was destructured when mmap was yoinked out so `fmt::Debug`
                            // cannot be used
                            format_args!(
                                "Segment {{ path: {:?}, flush_offset: {}, entries: {}, \
                                space: ({}/{}) }}",
                                path,
                                flush_offset,
                                index.len(),
                                index.last().map_or(HEADER_LEN, |&(offset, len)| {
                                    offset + len + padding(len) + CRC_LEN
                                }),
                                mmap_len
                            ),
                            e
                        );
                        return Err(e);
                    } else {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        }
    }

    pub(crate) fn copy_to_path<P>(&self, path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        if path.as_ref().exists() {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("Path {:?} already exists", path.as_ref()),
            ));
        }

        let mut other = Self::create(path, self.capacity())?;
        unsafe {
            other
                .mmap
                .as_mut_slice()
                .copy_from_slice(self.mmap.as_slice());
        }
        Ok(())
    }
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Segment: {{ path: {:?}, flush_offset: {}, entries: {}, space: ({}/{}) }}",
            &self.path,
            self.flush_offset,
            self.len(),
            self.size(),
            self.capacity()
        )
    }
}

/// Copies data from `src` to `dst`
///
/// Panics if the length of `dst` is less than the length of `src`.
pub fn copy_memory(src: &[u8], dst: &mut [u8]) {
    let len_src = src.len();
    assert!(dst.len() >= len_src);
    unsafe {
        ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), len_src);
    }
}

/// Returns the number of padding bytes to add to a buffer to ensure 8-byte alignment.
///
/// CRC_LEN = 4, which is always used with this.
fn padding(len: usize) -> usize {
    4usize.wrapping_sub(len) & 7
}

/// Returns the overhead of storing an entry of length `len`.
pub fn entry_overhead(len: usize) -> usize {
    padding(len) + HEADER_LEN + CRC_LEN
}

/// Returns the fixed-overhead of segment metadata.
pub fn segment_overhead() -> usize {
    HEADER_LEN
}
