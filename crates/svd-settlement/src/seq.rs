//! The persistent batch-sequence counter: a 4 KiB memory-mapped file holding
//! one `AtomicUsize`. The core thread assigns one sequence per closed taker
//! group and the submitters assign one per split child, so the
//! `SettlementResult.batch_seq` values stay unique across restarts — the SQL
//! tables key on them (`UNIQUE(batch_seq)` with `INSERT IGNORE`).
//!
//! This is NOT a journal: no records, no replay. The mapping is never
//! flushed (the page cache is the persistence, like the SPSC queues) and the
//! counter resumes from the persisted value on every open. The mapping is
//! opened once and shared through [`Arc`] — the atomic operations of every
//! thread must address the same mapping.

use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use memmap2::{MmapMut, MmapOptions};

/// The size of the sequence file.
const FILE_SIZE: usize = 4096;

/// A persistent batch-sequence counter on a memory-mapped file.
pub struct SeqFile {
    /// The shared mapping holding the counter.
    mmap: Arc<MmapMut>,
}

impl SeqFile {
    /// Opens the sequence file at `path`, creating it with a zero counter
    /// when missing; an existing file resumes from the persisted value.
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file =
            OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        if file.metadata()?.len() == 0 {
            file.set_len(FILE_SIZE as u64)?;
        }
        let mmap = unsafe { MmapOptions::new().len(FILE_SIZE).map_mut(&file)? };
        Ok(Self { mmap: Arc::new(mmap) })
    }

    /// The next sequence: a relaxed fetch-add through the shared mapping.
    /// Monotonic across every thread and every restart.
    pub fn next(&self) -> u64 {
        // SAFETY: the file is at least FILE_SIZE bytes and the mapping lives
        // for the life of the process; the counter is a plain AtomicUsize
        // over the mapped page, the same pattern as the queue headers.
        let counter = unsafe { &*(self.mmap.as_ptr() as *const AtomicUsize) };
        counter.fetch_add(1, Ordering::Relaxed) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    /// Monotonic counter so tests running in parallel never collide on
    /// temp paths.
    static SEQ: AtomicUsize = AtomicUsize::new(0);

    fn temp_path(tag: &str) -> PathBuf {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dex_stl_seq_{}_{}_{}", std::process::id(), tag, seq))
    }

    struct TempFile(PathBuf);

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    #[test]
    fn test_open_creates_the_file_and_starts_at_zero() {
        let path = temp_path("fresh");
        let _guard = TempFile(path.clone());
        let seq = SeqFile::open(&path).unwrap();
        assert_eq!(seq.next(), 0);
        assert_eq!(seq.next(), 1);
    }

    #[test]
    fn test_reopen_resumes_the_persisted_value() {
        let path = temp_path("resume");
        let _guard = TempFile(path.clone());
        {
            let seq = SeqFile::open(&path).unwrap();
            assert_eq!(seq.next(), 0);
            assert_eq!(seq.next(), 1);
            assert_eq!(seq.next(), 2);
        }
        let seq = SeqFile::open(&path).unwrap();
        assert_eq!(seq.next(), 3, "the counter resumes past the previous run");
    }

    #[test]
    fn test_concurrent_next_from_two_threads_is_distinct() {
        let path = temp_path("concurrent");
        let _guard = TempFile(path.clone());
        let seq = Arc::new(SeqFile::open(&path).unwrap());

        let handles: Vec<_> = (0..2)
            .map(|_| {
                let seq = Arc::clone(&seq);
                thread::spawn(move || (0..100).map(|_| seq.next()).collect::<Vec<_>>())
            })
            .collect();
        let mut values: Vec<u64> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
        values.sort_unstable();
        values.dedup();
        assert_eq!(values.len(), 200, "every fetch-add is a distinct sequence");
    }
}
