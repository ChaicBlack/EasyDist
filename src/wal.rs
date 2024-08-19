use crossbeam_channel::{Receiver, Sender};
use fs4::fs_std::FileExt;
use log::{debug, info, trace, warn};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File};
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::ops;
use std::path::{Path, PathBuf};
use std::result;
use std::str::FromStr;
use std::thread;

pub use segment::{Entry, Segment};

mod mmap2_view_sync;
mod segment;
pub use mmap2_view_sync::MmapViewSync;

#[derive(Debug)]
pub struct WalOptions {
    /// The segment capacity. Defaults to 32MiB.
    pub segment_capacity: usize,

    /// The number of segments to create ahead of time, so that appends never
    /// need to wait on creating a new segment.
    pub segment_queue_len: usize,
}

impl Default for WalOptions {
    fn default() -> WalOptions {
        WalOptions {
            segment_capacity: 32 * 1024 * 1024,
            segment_queue_len: 0,
        }
    }
}

/// An open segment and its ID.
#[derive(Debug)]
struct OpenSegment {
    pub id: u64,
    pub segment: Segment,
}

/// A closed segment, and the associated start and stop indices.
#[derive(Debug)]
struct ClosedSegment {
    pub start_index: u64,
    pub segment: Segment,
}

enum WalSegment {
    Open(OpenSegment),
    Closed(ClosedSegment),
}

/// A write ahead log.
///
/// ### Logging
///
/// Wal operations are logged. Metadata operations (open) are logged at `info`
/// level. Segment operations (create, close, delete) are logged at `debug`
/// level. Flush operations are logged at `debug` level. Entry operations
/// (append, truncate) are logged at `trace` level. Long-running or multi-step
/// operations will log a message at a lower level when beginning, and a final
/// completion message.
pub struct Wal {
    /// The segment currently being appended to.
    open_segment: OpenSegment,
    closed_segments: Vec<ClosedSegment>,
    creator: SegmentCreator,

    /// The directory which contains the write ahead log. Used to hold an open
    /// file lock for the lifetime of the log.
    #[allow(dead_code)]
    dir: File,

    /// The directory path.
    path: PathBuf,

    /// Tracks the flush status of recently closed segments between user calls
    /// to `Wal::flush`.
    flush: Option<thread::JoinHandle<Result<()>>>,
}

impl Wal {
    pub fn open<P>(path: P) -> Result<Wal>
    where
        P: AsRef<Path>,
    {
        Wal::with_options(path, &WalOptions::default())
    }

    pub fn generate_empty_wal_starting_at_index(
        path: impl Into<PathBuf>,
        options: &WalOptions,
        index: u64,
    ) -> Result<()> {
        let open_id = 0;
        let mut path_buf = path.into();
        path_buf.push(format!("open-{open_id}"));
        let segment = OpenSegment {
            id: index + 1,
            segment: Segment::create(&path_buf, options.segment_capacity)?,
        };

        let mut close_segment = close_segment(segment, index + 1)?;

        close_segment.segment.flush()
    }

    pub fn with_options<P>(path: P, options: &WalOptions) -> Result<Wal>
    where
        P: AsRef<Path>,
    {
        debug!("Wal {{ path: {:?} }}: opening", path.as_ref());

        #[cfg(not(target_os = "windows"))]
        let path = path.as_ref().to_path_buf();
        #[cfg(not(target_os = "windows"))]
        let dir = File::open(&path)?;

        // Windows workaround. Directories cannot be exclusively held so we create a proxy file
        // inside the tmp directory which is used for locking. This is done because:
        // - A Windows directory is not a file unlike in Linux, so we cannot open it with
        //   `File::open` nor lock it with `try_lock_exclusive`
        // - We want this to be auto-deleted together with the `TempDir`
        #[cfg(target_os = "windows")]
        let mut path = path.as_ref().to_path_buf();
        #[cfg(target_os = "windows")]
        let dir = {
            path.push(".wal");
            let dir = File::options()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)?;
            path.pop();
            dir
        };

        dir.try_lock_exclusive()?;

        // Holds open segments in the directory.
        let mut open_segments: Vec<OpenSegment> = Vec::new();
        let mut closed_segments: Vec<ClosedSegment> = Vec::new();

        for entry in fs::read_dir(&path)? {
            match open_dir_entry(entry?)? {
                Some(WalSegment::Open(open_segment)) => open_segments.push(open_segment),
                Some(WalSegment::Closed(closed_segment)) => closed_segments.push(closed_segment),
                None => {}
            }
        }

        // Validate the closed segments. They must be non-overlapping, and contiguous.
        closed_segments.sort_by(|a, b| a.start_index.cmp(&b.start_index));
        let mut next_start_index = closed_segments
            .first()
            .map_or(0, |segment| segment.start_index);
        for &ClosedSegment {
            start_index,
            ref segment,
            ..
        } in &closed_segments
        {
            match start_index.cmp(&next_start_index) {
                Ordering::Less => {
                    // TODO: figure out what to do here.
                    // Current thinking is the previous segment should be truncated.
                    unimplemented!()
                }
                Ordering::Equal => {
                    next_start_index = start_index + segment.len() as u64;
                }
                Ordering::Greater => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "missing segment(s) containing wal entries {next_start_index} to {start_index}"
                        ),
                    ));
                }
            }
        }

        // Validate the open segments.
        open_segments.sort_by(|a, b| a.id.cmp(&b.id));

        // The latest open segment, may already have segments.
        let mut open_segment: Option<OpenSegment> = None;
        // Unused open segments.
        let mut unused_segments: Vec<OpenSegment> = Vec::new();

        for segment in open_segments {
            if !segment.segment.is_empty() {
                // This segment has already been written to. If a previous open
                // segment has also already been written to, we close it out and
                // replace it with this new one. This may happen because when a
                // segment is closed it is renamed, but the directory is not
                // sync'd, so the operation is not guaranteed to be durable.
                let stranded_segment = open_segment.take();
                open_segment = Some(segment);
                if let Some(segment) = stranded_segment {
                    let closed_segment = close_segment(segment, next_start_index)?;
                    next_start_index += closed_segment.segment.len() as u64;
                    closed_segments.push(closed_segment);
                }
            } else if open_segment.is_none() {
                open_segment = Some(segment);
            } else {
                unused_segments.push(segment);
            }
        }

        let mut creator = SegmentCreator::new(
            &path,
            unused_segments,
            options.segment_capacity,
            options.segment_queue_len,
        );

        let open_segment = match open_segment {
            Some(segment) => segment,
            None => creator.next()?,
        };

        let wal = Wal {
            open_segment,
            closed_segments,
            creator,
            dir,
            path,
            flush: None,
        };
        info!("{:?}: opened", wal);
        Ok(wal)
    }

    fn retire_open_segment(&mut self) -> Result<()> {
        trace!("{:?}: retiring open segment", self);
        let mut segment = self.creator.next()?;
        mem::swap(&mut self.open_segment, &mut segment);

        if let Some(flush) = self.flush.take() {
            flush.join().map_err(|err| {
                Error::new(
                    ErrorKind::Other,
                    format!("wal flush thread panicked: {err:?}"),
                )
            })??;
        };

        self.flush = Some(segment.segment.flush_async());

        let start_index = self.open_segment_start_index();

        // If there is an empty closed segment, remove it before adding the new one.
        if let Some(last_closed) = self.closed_segments.last() {
            if last_closed.segment.is_empty() {
                let empty_segment = self.closed_segments.pop().unwrap();
                empty_segment.segment.delete()?;
            }
        }

        self.closed_segments
            .push(close_segment(segment, start_index)?);
        debug!("{self:?}: open segment retired. start_index: {start_index}");
        Ok(())
    }

    pub fn append<T>(&mut self, entry: &T) -> Result<u64>
    where
        T: ops::Deref<Target = [u8]>,
    {
        trace!("{:?}: appending entry of length {}", self, entry.len());
        if !self.open_segment.segment.sufficient_capacity(entry.len()) {
            if !self.open_segment.segment.is_empty() {
                self.retire_open_segment()?;
            }
            self.open_segment.segment.ensure_capacity(entry.len())?;
        }

        Ok(self.open_segment_start_index()
            + self.open_segment.segment.append(entry).unwrap() as u64)
    }

    pub fn flush_open_segment(&mut self) -> Result<()> {
        trace!("{:?}: flushing open segments", self);
        self.open_segment.segment.flush()?;
        Ok(())
    }

    pub fn flush_open_segment_async(&mut self) -> thread::JoinHandle<Result<()>> {
        trace!("{:?}: flushing open segments", self);
        self.open_segment.segment.flush_async()
    }

    /// Retrieve the entry with the provided index from the log.
    pub fn entry(&self, index: u64) -> Option<Entry> {
        let open_start_index = self.open_segment_start_index();
        if index >= open_start_index {
            return self
                .open_segment
                .segment
                .entry((index - open_start_index) as usize);
        }

        match self.find_closed_segment(index) {
            Ok(segment_index) => {
                let segment = &self.closed_segments[segment_index];
                segment
                    .segment
                    .entry((index - segment.start_index) as usize)
            }
            Err(i) => {
                // Sanity check that the missing index is less than the start of the log.
                assert_eq!(0, i);
                None
            }
        }
    }

    /// Truncates entries in the log beginning with `from`.
    ///
    /// Entries can be immediately appended to the log once this method returns,
    /// but the truncated entries are not guaranteed to be removed until the
    /// wal is flushed.
    pub fn truncate(&mut self, from: u64) -> Result<()> {
        trace!("{:?}: truncate from entry {}", self, from);
        let open_start_index = self.open_segment_start_index();
        if from >= open_start_index {
            self.open_segment
                .segment
                .truncate((from - open_start_index) as usize);
        } else {
            // Truncate the open segment completely.
            self.open_segment.segment.truncate(0);

            match self.find_closed_segment(from) {
                Ok(index) => {
                    if from == self.closed_segments[index].start_index {
                        for segment in self.closed_segments.drain(index..) {
                            // TODO: this should be async
                            segment.segment.delete()?;
                        }
                    } else {
                        {
                            let segment = &mut self.closed_segments[index];
                            segment
                                .segment
                                .truncate((from - segment.start_index) as usize);
                            // flushing closed segment after truncation
                            segment.segment.flush()?;
                        }
                        if index + 1 < self.closed_segments.len() {
                            for segment in self.closed_segments.drain(index + 1..) {
                                // TODO: this should be async
                                segment.segment.delete()?;
                            }
                        }
                    }
                }
                Err(index) => {
                    // The truncate index is before the first entry of the wal
                    assert!(
                        from <= self
                            .closed_segments
                            .get(index)
                            .map_or(0, |segment| segment.start_index)
                    );
                    for segment in self.closed_segments.drain(..) {
                        // TODO: this should be async
                        segment.segment.delete()?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Possibly removes entries from the beginning of the log before the given index.
    ///
    /// After calling this method, the `first_index` will be between the current
    /// `first_index` (inclusive), and `until` (exclusive).
    ///
    /// This always keeps at least one closed segment.
    pub fn prefix_truncate(&mut self, until: u64) -> Result<()> {
        trace!("{self:?}: prefix_truncate until entry {until}");

        // Return early if everything up to `until` has already been truncated
        if until
            <= self
                .closed_segments
                .first()
                .map_or(0, |segment| segment.start_index)
        {
            return Ok(());
        }

        // If `until` goes into or above our open segment, delete all but the last closed segments
        if until >= self.open_segment_start_index() {
            for segment in self
                .closed_segments
                .drain(..self.closed_segments.len().saturating_sub(1))
            {
                segment.segment.delete()?
            }
            return Ok(());
        }

        // Delete all closed segments before the one `until` is in
        let index = self.find_closed_segment(until).unwrap();
        trace!("{self:?}: prefix truncating until segment {index}");
        for segment in self.closed_segments.drain(..index) {
            segment.segment.delete()?
        }
        Ok(())
    }

    /// Returns the start index of the open segment.
    fn open_segment_start_index(&self) -> u64 {
        self.closed_segments.last().map_or(0, |segment| {
            segment.start_index + segment.segment.len() as u64
        })
    }

    fn find_closed_segment(&self, index: u64) -> result::Result<usize, usize> {
        self.closed_segments.binary_search_by(|segment| {
            if index < segment.start_index {
                Ordering::Greater
            } else if index >= segment.start_index + segment.segment.len() as u64 {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn num_segments(&self) -> usize {
        self.closed_segments.len() + 1
    }

    pub fn num_entries(&self) -> u64 {
        self.open_segment_start_index()
            - self
                .closed_segments
                .first()
                .map_or(0, |segment| segment.start_index)
            + self.open_segment.segment.len() as u64
    }

    /// The index of the first entry.
    pub fn first_index(&self) -> u64 {
        self.closed_segments
            .first()
            .map_or(0, |segment| segment.start_index)
    }

    /// The index of the last entry
    pub fn last_index(&self) -> u64 {
        let num_entries = self.num_entries();
        self.first_index() + num_entries.saturating_sub(1)
    }

    /// Remove all entries
    pub fn clear(&mut self) -> Result<()> {
        self.truncate(self.first_index())
    }

    /// Copy all files to the given path directory. directory should exist and be empty
    pub fn copy_to_path<P>(&self, path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        if fs::read_dir(path.as_ref())?.next().is_some() {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("path {:?} not empty", path.as_ref()),
            ));
        };

        let open_segment_file = self.open_segment.segment.path().file_name().unwrap();
        let close_segment_files: HashMap<_, _> = self
            .closed_segments
            .iter()
            .map(|segment| {
                (
                    segment.segment.path().file_name().unwrap(),
                    &segment.segment,
                )
            })
            .collect();

        for entry in fs::read_dir(self.path())? {
            let entry = entry?;
            if !entry.metadata()?.is_file() {
                continue;
            }

            // if file is locked by any Segment, call copy_to_path on it
            let entry_file_name = entry.file_name();
            let dst_path = path.as_ref().to_owned().join(entry_file_name.clone());
            if entry_file_name == open_segment_file {
                self.open_segment.segment.copy_to_path(&dst_path)?;
            } else if let Some(segment) = close_segment_files.get(entry_file_name.as_os_str()) {
                segment.copy_to_path(&dst_path)?;
            } else {
                // if file is not locked by any Segment, just copy it
                fs::copy(&entry.path(), &dst_path)?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for Wal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let start_index = self
            .closed_segments
            .first()
            .map_or(0, |segment| segment.start_index);
        let end_index = self.open_segment_start_index() + self.open_segment.segment.len() as u64;
        write!(
            f,
            "Wal {{ path: {:?}, segment-count: {}, entries: [{}, {})  }}",
            &self.path,
            self.closed_segments.len() + 1,
            start_index,
            end_index
        )
    }
}

fn close_segment(mut segment: OpenSegment, start_index: u64) -> Result<ClosedSegment> {
    let new_path = segment
        .segment
        .path()
        .with_file_name(format!("closed-{start_index}"));
    segment.segment.rename(new_path)?;
    Ok(ClosedSegment {
        start_index,
        segment: segment.segment,
    })
}

fn open_dir_entry(entry: fs::DirEntry) -> Result<Option<WalSegment>> {
    let metadata = entry.metadata()?;

    let error = || {
        Error::new(
            ErrorKind::InvalidData,
            format!("unexpected entry in wal directory: {:?}", entry.path()),
        )
    };

    if !metadata.is_file() {
        return Ok(None); // ignore non-files
    }

    let filename = entry.file_name().into_string().map_err(|_| error())?;
    match filename.split_once('-') {
        Some(("tmp", _)) => {
            // remove temporary files.
            fs::remove_file(entry.path())?;
            Ok(None)
        }
        Some(("open", id)) => {
            let id = u64::from_str(id).map_err(|_| error())?;
            let segment = Segment::open(entry.path())?;
            Ok(Some(WalSegment::Open(OpenSegment { segment, id })))
        }
        Some(("closed", start)) => {
            let start = u64::from_str(start).map_err(|_| error())?;
            let segment = Segment::open(entry.path())?;
            Ok(Some(WalSegment::Closed(ClosedSegment {
                start_index: start,
                segment,
            })))
        }
        _ => Ok(None), // Ignore other files.
    }
}

struct SegmentCreator {
    /// Receive channel for new segments.
    rx: Option<Receiver<OpenSegment>>,
    /// The segment creator thread.
    ///
    /// Used for retrieving error upon failure.
    thread: Option<thread::JoinHandle<Result<()>>>,
}

impl SegmentCreator {
    /// Creates a new segment creator.
    ///
    /// The segment creator must be started before new segments will be created.
    pub fn new<P>(
        dir: P,
        existing: Vec<OpenSegment>,
        segment_capacity: usize,
        segment_queue_len: usize,
    ) -> SegmentCreator
    where
        P: AsRef<Path>,
    {
        let (tx, rx) = crossbeam_channel::bounded(segment_queue_len);

        let dir = dir.as_ref().to_path_buf();
        let thread = thread::spawn(move || create_loop(tx, dir, segment_capacity, existing));
        SegmentCreator {
            rx: Some(rx),
            thread: Some(thread),
        }
    }

    /// Retrieves the next segment.
    pub fn next(&mut self) -> Result<OpenSegment> {
        self.rx.as_mut().unwrap().recv().map_err(|_| {
            match self.thread.take().map(|join_handle| join_handle.join()) {
                Some(Ok(Err(error))) => error,
                None => Error::new(ErrorKind::Other, "segment creator thread already failed"),
                Some(Ok(Ok(()))) => unreachable!(
                    "segment creator thread finished without an error,
                                                  but the segment creator is still live"
                ),
                Some(Err(_)) => unreachable!("segment creator thread panicked"),
            }
        })
    }
}

impl Drop for SegmentCreator {
    fn drop(&mut self) {
        drop(self.rx.take());
        if let Some(join_handle) = self.thread.take() {
            if let Err(error) = join_handle.join() {
                warn!("Error while shutting down segment creator: {:?}", error);
            }
        }
    }
}

fn create_loop(
    tx: Sender<OpenSegment>,
    mut path: PathBuf,
    capacity: usize,
    mut existing_segments: Vec<OpenSegment>,
) -> Result<()> {
    // Ensure the existing segments are in ID order.
    existing_segments.sort_by(|a, b| a.id.cmp(&b.id));

    let mut cont = true;
    let mut id = 0;

    for segment in existing_segments {
        id = segment.id;
        if tx.send(segment).is_err() {
            cont = false;
            break;
        }
    }

    // Directory being a file only applies to Linux
    #[cfg(not(target_os = "windows"))]
    let dir = File::open(&path)?;

    while cont {
        id += 1;
        path.push(format!("open-{id}"));
        let segment = OpenSegment {
            id,
            segment: Segment::create(&path, capacity)?,
        };
        path.pop();
        // Sync the directory, guaranteeing that the segment file is durably
        // stored on the filesystem.
        #[cfg(not(target_os = "windows"))]
        dir.sync_all()?;
        cont = tx.send(segment).is_ok();
    }

    info!("segment creator shutting down");
    Ok(())
}
