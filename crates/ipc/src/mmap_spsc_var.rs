//! A SPSC queue of variable-size byte payloads on share memory, the queue's structure looks
//! like:
//! |---------------|-------|---------|-------|---------|
//! |     Header    | len0  |  pld0   | len1  |  pld1   |
//! |---------------|-------|---------|-------|---------|
//!
//! Every message is a frame: a fixed 4-byte little-endian length header followed by the payload
//! bytes, so the consumer always knows how many bytes one message occupies. The write and read
//! indices are byte offsets into the data region; a frame may straddle the end of the region (the
//! length header included) and is written / read in up to two segments. A push is all-or-nothing:
//! the producer writes the whole frame before it publishes the write index, so a torn frame is
//! never visible to the consumer, and a frame that does not fit is never partially written —
//! the producer retries.
//!
//! Like [`crate::mmap_spsc_fixed::SpscQueue`] the queue is a ring with one spare byte (a full queue
//! never wraps onto the consumer's read index), the mapping is never flushed (the page cache is
//! the persistence), and the consumer advances the read index explicitly with [`ByteSpscQueue::ack`]
//! once the message is fully processed or delivered to the next stage — the unacked messages stay
//! in the queue and survive a crash of the consumer.

use memmap2::{MmapMut, MmapOptions};
use std::fmt;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Modern CPU cache line length.
const CACHE_LINE_SIZE: usize = 64;

/// Cache line alignment Usize for the index in the queue's header. It prevents the false sharing
/// of the index in the SPSC queue header.
#[repr(C, align(64))]
struct PaddedUsize {
    value: AtomicUsize,
}

impl PaddedUsize {
    fn new(v: usize) -> PaddedUsize {
        Self { value: AtomicUsize::new(v) }
    }
}

/// Header of the byte queue in the share memory.
#[repr(C)]
struct Header {
    /// capacity of the data region in bytes, it is a read only field within a cache line.
    capacity: usize,
    /// magic of the byte queue file format.
    magic: u64,
    /// padding for capacity and magic to occupy a single cache line.
    _pad: [u8; CACHE_LINE_SIZE - size_of::<usize>() - size_of::<u64>()],
    /// write index in an individual cache line, a byte offset into the data region.
    write_idx: PaddedUsize,
    /// read index in an individual cache line, a byte offset into the data region.
    read_idx: PaddedUsize,
}

impl Header {
    fn new(capacity: usize) -> Header {
        Self {
            capacity,
            magic: MAGIC,
            _pad: [0u8; CACHE_LINE_SIZE - size_of::<usize>() - size_of::<u64>()],
            write_idx: PaddedUsize::new(0),
            read_idx: PaddedUsize::new(0),
        }
    }
}

/// The magic of a byte queue file: distinguishes it from the fixed-size queue files.
const MAGIC: u64 = u64::from_le_bytes(*b"DEXBSQ1\0");
/// Header size.
const HEADER_SIZE: usize = size_of::<Header>();
/// Size of one frame's length header. The frame size of a payload of `len`
/// bytes is `FRAME_HEADER_SIZE + len` — the value an ack passes.
pub const FRAME_HEADER_SIZE: usize = size_of::<u32>();

/// Errors of the byte queue.
#[derive(Debug)]
pub enum ByteQueueError {
    /// A committed frame is malformed: its length is zero, points outside the committed region,
    /// or the frame would exceed the ring. A well-behaved producer never writes one — the file is
    /// corrupted or a different version wrote it.
    InvalidFrame { detail: String },
}

impl fmt::Display for ByteQueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ByteQueueError::InvalidFrame { detail } => {
                write!(f, "invalid byte queue frame: {detail}")
            }
        }
    }
}

impl std::error::Error for ByteQueueError {}

/// A SPSC queue of variable-size byte payloads on top of share memory.
pub struct ByteSpscQueue {
    /// the share memory map to a file.
    mmap: MmapMut,
    /// the capacity of the data region in bytes.
    capacity: usize,
}

impl ByteSpscQueue {
    /// Create or open a shared memory byte queue backed by the file at `path`.
    ///
    /// With `create` set, the file is (re)initialized with a fresh header. Otherwise the file
    /// must exist, must be a byte queue (magic match) and must have been created with the same
    /// `capacity`, or an error is returned.
    pub fn open<P: AsRef<Path>>(path: P, capacity: usize, create: bool) -> std::io::Result<Self> {
        assert!(capacity >= 16, "capacity must be at least 16 bytes");
        let total = HEADER_SIZE.checked_add(capacity).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "capacity is too large")
        })?;

        // memory mapped file
        let file = OpenOptions::new().read(true).write(true).create(create).open(path)?;

        if create {
            file.set_len(total as u64)?;
        }

        // map the share memory to the file.
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };

        // write the header on create.
        if create {
            let header_ptr = mmap.as_mut_ptr() as *mut Header;
            unsafe {
                std::ptr::write(header_ptr, Header::new(capacity));
            }
        } else {
            // the file was created by an earlier opener; a magic or capacity mismatch means the
            // indices can run past the mapped region or address a different file format, so fail
            // instead of corrupting memory.
            let header = unsafe { &*(mmap.as_ptr() as *const Header) };
            if header.magic != MAGIC {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "the file is not a byte queue (magic mismatch)",
                ));
            }
            if header.capacity != capacity {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "queue capacity {capacity} does not match file header capacity {}",
                        header.capacity
                    ),
                ));
            }
        }

        Ok(Self { mmap, capacity })
    }

    /// header returns the read only reference of the header.
    #[inline]
    fn header(&self) -> &Header {
        unsafe { &*(self.mmap.as_ptr() as *const Header) }
    }

    /// data pointer returns the pointer to the 1st byte of the data region.
    #[inline]
    fn data_ptr(&self) -> *mut u8 {
        unsafe { self.mmap.as_ptr().add(HEADER_SIZE) as *mut u8 }
    }

    /// The capacity of the data region in bytes.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// available returns the free bytes of the queue, the spare byte never counted.
    #[inline]
    pub fn available(&self) -> usize {
        let w = self.header().write_idx.value.load(Ordering::Relaxed);
        let r = self.header().read_idx.value.load(Ordering::Acquire);
        let committed = if w >= r { w - r } else { self.capacity - r + w };
        self.capacity - 1 - committed
    }

    /// Pushes one frame of the payload. Returns false when the frame does not fit (nothing is
    /// written — never a partial frame) or the payload is empty or exceeds u32::MAX: the caller
    /// can decide a retrying or an upstream propagation.
    pub fn push(&mut self, payload: &[u8]) -> bool {
        if payload.is_empty() || payload.len() > u32::MAX as usize {
            return false;
        }
        let len_total = FRAME_HEADER_SIZE + payload.len();

        let header = self.header();
        let write = header.write_idx.value.load(Ordering::Relaxed);
        let read = header.read_idx.value.load(Ordering::Acquire);
        let committed = if write >= read { write - read } else { self.capacity - read + write };
        let free = self.capacity - 1 - committed;
        if len_total > free {
            return false;
        }

        // The whole frame is written before the write index is published: the length header at
        // the frame start (it may straddle the end like the payload), then the payload in up to
        // two segments. A crash in between leaves a torn frame past the committed write index,
        // which the consumer never reads.
        let base = self.data_ptr();
        let len_bytes = (payload.len() as u32).to_le_bytes();
        let first_h = (self.capacity - write).min(FRAME_HEADER_SIZE);
        unsafe {
            std::ptr::copy_nonoverlapping(len_bytes.as_ptr(), base.add(write), first_h);
            if FRAME_HEADER_SIZE > first_h {
                std::ptr::copy_nonoverlapping(
                    len_bytes.as_ptr().add(first_h),
                    base,
                    FRAME_HEADER_SIZE - first_h,
                );
            }
            let payload_pos = (write + FRAME_HEADER_SIZE) % self.capacity;
            let first_p = (self.capacity - payload_pos).min(payload.len());
            std::ptr::copy_nonoverlapping(payload.as_ptr(), base.add(payload_pos), first_p);
            if payload.len() > first_p {
                std::ptr::copy_nonoverlapping(
                    payload.as_ptr().add(first_p),
                    base,
                    payload.len() - first_p,
                );
            }
        }

        let next = (write + len_total) % self.capacity;
        header.write_idx.value.store(next, Ordering::Release);
        true
    }

    /// Peeks the next frame without advancing the read index: the payload is copied into the out
    /// buffer (cleared first), the function returns the payload length. Re-peeking before the ack
    /// returns the same payload. The caller advances the read index explicitly with
    /// [`ByteSpscQueue::ack`] by the frame size of the last peek (`FRAME_HEADER_SIZE + len`) once
    /// the message is fully processed or delivered to the next stage.
    ///
    /// A committed frame whose length is zero or runs outside the committed region is
    /// [`ByteQueueError::InvalidFrame`]: the file is corrupted or a different version wrote it.
    pub fn peek(&self, out: &mut Vec<u8>) -> Result<Option<usize>, ByteQueueError> {
        let header = self.header();
        // `write` belongs to the producer: Acquire publishes its data writes before they are
        // copied out. `read` is the consumer's own index, so Relaxed is enough.
        let write = header.write_idx.value.load(Ordering::Acquire);
        let read = header.read_idx.value.load(Ordering::Relaxed);
        let committed = if write >= read { write - read } else { self.capacity - read + write };
        if committed == 0 {
            return Ok(None);
        }

        // the length header of the frame at the read index, it may straddle the end.
        let base = self.data_ptr();
        let mut len_bytes = [0u8; FRAME_HEADER_SIZE];
        let first_h = (self.capacity - read).min(FRAME_HEADER_SIZE);
        unsafe {
            std::ptr::copy_nonoverlapping(base.add(read), len_bytes.as_mut_ptr(), first_h);
            if FRAME_HEADER_SIZE > first_h {
                std::ptr::copy_nonoverlapping(
                    base,
                    len_bytes.as_mut_ptr().add(first_h),
                    FRAME_HEADER_SIZE - first_h,
                );
            }
        }
        let len = u32::from_le_bytes(len_bytes) as usize;
        let len_total = FRAME_HEADER_SIZE + len;
        if len == 0 || len_total > committed || len_total > self.capacity - 1 {
            return Err(ByteQueueError::InvalidFrame {
                detail: format!("the frame length {len} exceeds the committed region"),
            });
        }

        out.clear();
        out.resize(len, 0);
        let payload_pos = (read + FRAME_HEADER_SIZE) % self.capacity;
        let first_p = (self.capacity - payload_pos).min(len);
        unsafe {
            std::ptr::copy_nonoverlapping(base.add(payload_pos), out.as_mut_ptr(), first_p);
            if len > first_p {
                std::ptr::copy_nonoverlapping(base, out.as_mut_ptr().add(first_p), len - first_p);
            }
        }
        Ok(Some(len))
    }

    /// Advances the read index by `frame_bytes`, releasing the frame to the producer. The caller
    /// must pass exactly the frame size of its last successful [`ByteSpscQueue::peek`]
    /// (`FRAME_HEADER_SIZE + len`) — anything else desynchronizes the queue.
    pub fn ack(&mut self, frame_bytes: usize) -> Result<(), ByteQueueError> {
        let header = self.header();
        // `write` belongs to the producer: Acquire publishes its data writes before the ack
        // releases them. `read` is the consumer's own index, so Relaxed is enough.
        let write = header.write_idx.value.load(Ordering::Acquire);
        let read = header.read_idx.value.load(Ordering::Relaxed);
        let committed = if write >= read { write - read } else { self.capacity - read + write };
        if frame_bytes <= FRAME_HEADER_SIZE || frame_bytes > committed {
            return Err(ByteQueueError::InvalidFrame {
                detail: format!(
                    "the ack of {frame_bytes} does not address a committed frame of {committed} bytes"
                ),
            });
        }
        let next = (read + frame_bytes) % self.capacity;
        header.read_idx.value.store(next, Ordering::Release);
        Ok(())
    }

    /// Pops the next frame: a [`ByteSpscQueue::peek`] followed by the ack of the frame size.
    pub fn pop(&mut self, out: &mut Vec<u8>) -> Result<Option<usize>, ByteQueueError> {
        let Some(len) = self.peek(out)? else { return Ok(None) };
        self.ack(FRAME_HEADER_SIZE + len)?;
        Ok(Some(len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    /// Monotonic counter so tests running in parallel never collide on temp paths.
    static SEQ: AtomicUsize = AtomicUsize::new(0);

    /// A unique path under the system temp dir.
    fn temp_path(tag: &str) -> PathBuf {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dex_ipc_bytes_{}_{}_{}", std::process::id(), tag, seq))
    }

    /// Removes the queue file when dropped.
    struct TempFile(PathBuf);

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    /// Opens a fresh byte queue plus a guard that removes its file.
    fn open_queue(tag: &str, capacity: usize) -> (ByteSpscQueue, TempFile) {
        let path = temp_path(tag);
        let queue = ByteSpscQueue::open(&path, capacity, true).unwrap();
        (queue, TempFile(path))
    }

    fn payload(tag: u8, len: usize) -> Vec<u8> {
        vec![tag; len]
    }

    #[test]
    fn test_header_layout() {
        assert_eq!(size_of::<Header>(), 192);
        assert_eq!(std::mem::offset_of!(Header, capacity), 0);
        assert_eq!(std::mem::offset_of!(Header, magic), 8);
        assert_eq!(std::mem::offset_of!(Header, write_idx), 64);
        assert_eq!(std::mem::offset_of!(Header, read_idx), 128);
    }

    #[test]
    fn test_open_and_available_on_fresh_queue() {
        for capacity in [16usize, 32, 64, 1024] {
            let (queue, _guard) = open_queue("fresh", capacity);
            assert_eq!(queue.capacity(), capacity);
            assert_eq!(queue.available(), capacity - 1);
        }
    }

    #[test]
    fn test_push_peek_ack_roundtrip() {
        let (mut queue, _guard) = open_queue("roundtrip", 1024);
        for (tag, len) in [(1u8, 3usize), (2, 100), (3, 1), (4, 500)] {
            assert!(queue.push(&payload(tag, len)));
        }
        let mut out = Vec::new();
        for (tag, len) in [(1u8, 3usize), (2, 100), (3, 1), (4, 500)] {
            assert_eq!(queue.peek(&mut out).unwrap(), Some(len));
            assert_eq!(out, payload(tag, len));
            queue.ack(FRAME_HEADER_SIZE + len).unwrap();
        }
        assert_eq!(queue.peek(&mut out).unwrap(), None);
        assert_eq!(queue.available(), 1023);
    }

    #[test]
    fn test_peek_twice_without_ack_is_idempotent() {
        let (mut queue, _guard) = open_queue("peektwice", 1024);
        assert!(queue.push(&payload(0xab, 64)));
        let mut out = Vec::new();
        assert_eq!(queue.peek(&mut out).unwrap(), Some(64));
        let first = out.clone();
        assert_eq!(queue.peek(&mut out).unwrap(), Some(64));
        assert_eq!(out, first);
        // the frame stays committed for the producer.
        assert_eq!(queue.available(), 1024 - 1 - (64 + FRAME_HEADER_SIZE));
    }

    #[test]
    fn test_pop_equals_peek_then_ack() {
        let (mut queue, _guard) = open_queue("pop", 1024);
        assert!(queue.push(&payload(0x11, 32)));
        assert!(queue.push(&payload(0x22, 32)));
        let mut out = Vec::new();
        assert_eq!(queue.pop(&mut out).unwrap(), Some(32));
        assert_eq!(out, payload(0x11, 32));
        assert_eq!(queue.peek(&mut out).unwrap(), Some(32));
        assert_eq!(out, payload(0x22, 32));
        queue.ack(FRAME_HEADER_SIZE + 32).unwrap();
        assert_eq!(queue.pop(&mut out).unwrap(), None);
    }

    #[test]
    fn test_frame_straddling_the_ring_end() {
        // A 200-byte region: the first frame fills it up to the spare byte
        // exactly, the second straddles the end (its length header starts
        // at the region tail, the payload wraps to the head).
        let (mut queue, _guard) = open_queue("straddle", 200);
        assert!(queue.push(&payload(0x11, 199 - FRAME_HEADER_SIZE)));
        assert_eq!(queue.available(), 0);
        // Pop and ack it, then a frame of 100 bytes straddles the end.
        let mut out = Vec::new();
        assert_eq!(queue.pop(&mut out).unwrap(), Some(199 - FRAME_HEADER_SIZE));
        assert!(queue.push(&payload(0x22, 100)));
        assert_eq!(queue.peek(&mut out).unwrap(), Some(100));
        assert_eq!(out, payload(0x22, 100));
        queue.ack(FRAME_HEADER_SIZE + 100).unwrap();
        assert_eq!(queue.peek(&mut out).unwrap(), None);
    }

    #[test]
    fn test_push_of_an_oversized_frame_writes_nothing() {
        let (mut queue, _guard) = open_queue("oversize", 200);
        assert!(queue.push(&payload(0x11, 64)));
        // The frame does not fit and nothing is written: the queue state is
        // unchanged and the previous frame is intact.
        assert!(!queue.push(&payload(0x22, 200)));
        let mut out = Vec::new();
        assert_eq!(queue.peek(&mut out).unwrap(), Some(64));
        assert_eq!(out, payload(0x11, 64));
        assert_eq!(queue.available(), 200 - 1 - (64 + FRAME_HEADER_SIZE));
    }

    #[test]
    fn test_push_of_an_empty_payload_is_rejected() {
        let (mut queue, _guard) = open_queue("emptypayload", 200);
        assert!(!queue.push(&[]));
        let mut out = Vec::new();
        assert_eq!(queue.peek(&mut out).unwrap(), None);
    }

    #[test]
    fn test_exact_fit_frame() {
        // The frame fills the region minus the spare byte exactly.
        let (mut queue, _guard) = open_queue("exactfit", 200);
        assert!(queue.push(&payload(0x11, 199 - FRAME_HEADER_SIZE)));
        assert_eq!(queue.available(), 0);
        assert!(!queue.push(&payload(0x22, 1)));
        let mut out = Vec::new();
        assert_eq!(queue.peek(&mut out).unwrap(), Some(199 - FRAME_HEADER_SIZE));
    }

    #[test]
    fn test_ack_validation() {
        let (mut queue, _guard) = open_queue("ackvalid", 200);
        assert!(queue.push(&payload(0x11, 32)));
        // a bare header or an ack past the committed frame desynchronizes.
        assert!(queue.ack(FRAME_HEADER_SIZE).is_err());
        assert!(queue.ack(FRAME_HEADER_SIZE + 33).is_err());
        assert!(queue.ack(FRAME_HEADER_SIZE + 32).is_ok());
        let mut out = Vec::new();
        assert_eq!(queue.peek(&mut out).unwrap(), None);
    }

    #[test]
    fn test_corrupt_committed_length_is_invalid() {
        let (mut queue, _guard) = open_queue("corruptlen", 200);
        assert!(queue.push(&payload(0x11, 64)));
        // Clobber the committed frame's length header through the mapping.
        let data_ptr = queue.data_ptr() as *mut u32;
        unsafe { std::ptr::write_volatile(data_ptr, 0u32) };
        let mut out = Vec::new();
        assert!(matches!(queue.peek(&mut out), Err(ByteQueueError::InvalidFrame { .. })));
        // The read index is unchanged: a re-open with a valid frame still
        // reads it (the clobbered bytes are the frame's own).
        queue.ack(FRAME_HEADER_SIZE + 64).unwrap();
        assert!(queue.push(&payload(0x22, 32)));
        assert_eq!(queue.peek(&mut out).unwrap(), Some(32));
        assert_eq!(out, payload(0x22, 32));
    }

    #[test]
    fn test_reopen_reads_back_pending_frames() {
        let path = temp_path("reopen");
        let _guard = TempFile(path.clone());
        {
            let mut queue = ByteSpscQueue::open(&path, 1024, true).unwrap();
            assert!(queue.push(&payload(0x11, 64)));
            assert!(queue.push(&payload(0x22, 64)));
        }
        let mut queue = ByteSpscQueue::open(&path, 1024, false).unwrap();
        let mut out = Vec::new();
        assert_eq!(queue.peek(&mut out).unwrap(), Some(64));
        assert_eq!(out, payload(0x11, 64));
        queue.ack(FRAME_HEADER_SIZE + 64).unwrap();
        assert_eq!(queue.peek(&mut out).unwrap(), Some(64));
        assert_eq!(out, payload(0x22, 64));
    }

    #[test]
    fn test_create_truncates_an_existing_file() {
        let path = temp_path("truncate");
        let _guard = TempFile(path.clone());
        {
            let mut queue = ByteSpscQueue::open(&path, 1024, true).unwrap();
            assert!(queue.push(&payload(0x11, 64)));
        }
        // create=true re-initializes: the frames are gone.
        let queue = ByteSpscQueue::open(&path, 1024, true).unwrap();
        let mut out = Vec::new();
        assert_eq!(queue.peek(&mut out).unwrap(), None);
    }

    #[test]
    fn test_open_with_mismatched_capacity_fails() {
        let path = temp_path("capmismatch");
        let _guard = TempFile(path.clone());
        ByteSpscQueue::open(&path, 1024, true).unwrap();
        assert!(ByteSpscQueue::open(&path, 2048, false).is_err());
    }

    #[test]
    fn test_open_of_a_non_byte_queue_fails() {
        let path = temp_path("wrongmagic");
        let _guard = TempFile(path.clone());
        crate::mmap_spsc_fixed::SpscQueue::<u32>::open(&path, 64, true).unwrap();
        assert!(ByteSpscQueue::open(&path, 64, false).is_err());
    }

    #[test]
    fn test_open_missing_file_without_create_fails() {
        let path = temp_path("missing");
        let _guard = TempFile(path.clone());
        assert!(ByteSpscQueue::open(&path, 1024, false).is_err());
    }

    #[test]
    fn test_byte_queue_producer_consumer_threads() {
        const CAP: usize = 4096;
        const N: usize = 200;
        let path = temp_path("threads");
        let _guard = TempFile(path.clone());
        ByteSpscQueue::open(&path, CAP, true).unwrap(); // initialize, then drop

        let producer = {
            let path = path.clone();
            thread::spawn(move || {
                let mut queue = ByteSpscQueue::open(&path, CAP, false).unwrap();
                for i in 0..N {
                    let bytes = (i as u64).to_le_bytes();
                    while !queue.push(&bytes) {
                        thread::yield_now();
                    }
                }
            })
        };

        let consumer = thread::spawn(move || {
            let mut queue = ByteSpscQueue::open(&path, CAP, false).unwrap();
            let mut out = Vec::new();
            for want in 0..N {
                // the consumer acks only after it "processed" the frame.
                loop {
                    match queue.peek(&mut out).unwrap() {
                        Some(8) => {
                            let got = u64::from_le_bytes(out.as_slice().try_into().unwrap());
                            assert_eq!(got, want as u64);
                            queue.ack(FRAME_HEADER_SIZE + 8).unwrap();
                            break;
                        }
                        Some(len) => panic!("unexpected frame length {len}"),
                        None => thread::yield_now(),
                    }
                }
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    }
}
