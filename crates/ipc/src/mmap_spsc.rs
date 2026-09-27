//! A SPSC queue base on share memory, it works as a data pipeline between services, it could
//! reduce the latency introduced by the heavy wire protocols (kafka or redpanda) in the hot path.
//! The queue's structure looks like:
//! |---------------|-------|--------|-------|
//! |     Header    |   T   |    T   |   T   |
//! |---------------|-------|--------|-------|
//!
use memmap2::{MmapMut, MmapOptions};
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

/// Header of the queue in the share memory.
#[repr(C)]
struct Header {
    /// capacity of the queue, it is read only field within a cache line.
    capacity: usize,
    /// padding for capacity to occupy a single cache line.
    _pad: [u8; CACHE_LINE_SIZE - size_of::<usize>()],
    /// write index in an individual cache line.
    write_idx: PaddedUsize,
    /// read index in an individual cache line.
    read_idx: PaddedUsize,
}

impl Header {
    fn new(capacity: usize) -> Header {
        Self {
            capacity,
            _pad: [0u8; CACHE_LINE_SIZE - size_of::<usize>()],
            write_idx: PaddedUsize::new(0),
            read_idx: PaddedUsize::new(0),
        }
    }
}

/// Header size.
const HEADER_SIZE: usize = size_of::<Header>();

/// A SPSC queue on top of share memory.
pub struct SpscQueue<T: Copy> {
    /// the share memory map to a file.
    mmap: MmapMut,
    /// the capacity of the queue.
    capacity: usize,
    /// marker to tell the compiler: the queue owns data of T.
    _marker: std::marker::PhantomData<T>,
}

impl<T: Copy> SpscQueue<T> {
    /// Create or open a shared memory queue backed by the file at `path`.
    ///
    /// With `create` set, the file is (re)initialized with a fresh header. Otherwise the file
    /// must exist and must have been created with the same `capacity`, or an error is returned.
    pub fn open<P: AsRef<Path>>(path: P, capacity: usize, create: bool) -> std::io::Result<Self> {
        assert!(capacity >= 2, "capacity must be at least 2");
        let elem_size = size_of::<T>();
        let align = align_of::<T>().max(1);
        // align up the data field with T's alignment.
        let data_offset = align_up(HEADER_SIZE, align);
        let total = capacity
            .checked_mul(elem_size)
            .and_then(|data_len| data_offset.checked_add(data_len))
            .ok_or_else(|| {
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
            // the file was created by an earlier opener; a capacity mismatch means the
            // indices can run past the mapped region, so fail instead of corrupting memory.
            let header = unsafe { &*(mmap.as_ptr() as *const Header) };
            if header.capacity != capacity {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "queue capacity {} does not match file header capacity {}",
                        capacity, header.capacity
                    ),
                ));
            }
        }

        Ok(Self { mmap, capacity, _marker: std::marker::PhantomData })
    }

    /// header returns the read only reference of the header.
    #[inline]
    fn header(&self) -> &Header {
        unsafe { &*(self.mmap.as_ptr() as *const Header) }
    }

    /// data pointer returns the pointer to the 1st elem slot.
    #[inline]
    fn data_ptr(&self) -> *mut T {
        let align = align_of::<T>().max(1);
        let offset = align_up(HEADER_SIZE, align);
        unsafe { self.mmap.as_ptr().add(offset) as *mut T }
    }

    /// available returns the free slots of the queue.
    #[inline]
    pub fn available(&self) -> usize {
        let w = self.header().write_idx.value.load(Ordering::Relaxed);
        let r = self.header().read_idx.value.load(Ordering::Acquire);
        if w >= r { self.capacity - 1 - (w - r) } else { r - w - 1 }
    }

    /// Push an element into the queue, returns error which carries the input if the queue is full.
    /// The caller can decide a retrying or an upstream propagation.
    #[inline]
    pub fn push(&mut self, value: T) -> Result<(), T> {
        let header = self.header();
        let write = header.write_idx.value.load(Ordering::Relaxed);
        let next = if write + 1 == self.capacity { 0 } else { write + 1 };

        if next == header.read_idx.value.load(Ordering::Acquire) {
            return Err(value);
        }

        unsafe {
            std::ptr::write_volatile(self.data_ptr().add(write), value);
        }

        header.write_idx.value.store(next, Ordering::Release);
        Ok(())
    }

    /// Pop an element from the queue, return None if the queue is empty.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        let header = self.header();
        let read = header.read_idx.value.load(Ordering::Relaxed);

        if read == header.write_idx.value.load(Ordering::Acquire) {
            return None;
        }

        let value = unsafe { std::ptr::read_volatile(self.data_ptr().add(read)) };
        let next = if read + 1 == self.capacity { 0 } else { read + 1 };
        header.read_idx.value.store(next, Ordering::Release);
        Some(value)
    }

    /// Push a batch of elements, return the number of elements that are append successfully.
    /// When the queue is full, the appending of the rest elements will be skipped.
    pub fn push_batch(&mut self, values: &[T]) -> usize {
        if values.is_empty() {
            return 0;
        }

        let header = self.header();
        let write = header.write_idx.value.load(Ordering::Relaxed);
        let read = header.read_idx.value.load(Ordering::Acquire);

        let free =
            if write >= read { self.capacity - 1 - (write - read) } else { read - write - 1 };

        let n = free.min(values.len());
        if n == 0 {
            return 0;
        }

        let base = self.data_ptr();
        // try to append from write position to the end of the queue, and then append
        // from head position 0 until finish.
        let first = (self.capacity - write).min(n);
        unsafe {
            // memory copying.
            std::ptr::copy_nonoverlapping(values.as_ptr(), base.add(write), first);
            if n > first {
                std::ptr::copy_nonoverlapping(values.as_ptr().add(first), base, n - first);
            }
        }

        let next = (write + n) % self.capacity;
        header.write_idx.value.store(next, Ordering::Release);
        n
    }

    /// Pop a batch of elements, the data elements are copied into the out buffer, the function
    /// returns the number of elements are popped.
    pub fn pop_batch(&mut self, out: &mut [T]) -> usize {
        if out.is_empty() {
            return 0;
        }

        let header = self.header();
        // `write` belongs to the producer: Acquire publishes its data writes before they are
        // copied out. `read` is the consumer's own index, so Relaxed is enough.
        let write = header.write_idx.value.load(Ordering::Acquire);
        let read = header.read_idx.value.load(Ordering::Relaxed);

        let avail = if write >= read { write - read } else { self.capacity - read + write };

        let n = avail.min(out.len());
        if n == 0 {
            return 0;
        }

        let base = self.data_ptr();

        let first = (self.capacity - read).min(n);
        unsafe {
            std::ptr::copy_nonoverlapping(base.add(read), out.as_mut_ptr(), first);
            if n > first {
                std::ptr::copy_nonoverlapping(base, out.as_mut_ptr().add(first), n - first);
            }
        }

        let next = (read + n) % self.capacity;
        header.read_idx.value.store(next, Ordering::Release);
        n
    }
}

/// align_up rounds the beginning index of data elements with the alignment of T, the data element.
/// It would help to reduce the possibility of cross line data element.
#[inline]
fn align_up(v: usize, align: usize) -> usize {
    (v + align - 1) & !(align - 1)
}

unsafe impl<T: Copy + Send> Send for SpscQueue<T> {}
unsafe impl<T: Copy + Send> Sync for SpscQueue<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    /// Monotonic counter so tests running in parallel never collide on temp paths.
    static SEQ: AtomicUsize = AtomicUsize::new(0);

    /// A unique path under the system temp dir; the file is created by the queue's `open`.
    fn temp_path(tag: &str) -> PathBuf {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dex_ipc_spsc_{}_{}_{}", std::process::id(), tag, seq))
    }

    /// Removes the backing file when dropped, so failed tests don't litter the temp dir.
    struct TempFile(PathBuf);

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    /// Opens a freshly created queue together with a guard that removes its file.
    fn open_queue<T: Copy>(tag: &str, capacity: usize) -> (SpscQueue<T>, TempFile) {
        let path = temp_path(tag);
        let queue = SpscQueue::open(&path, capacity, true).unwrap();
        (queue, TempFile(path))
    }

    #[test]
    fn test_header_layout() {
        use std::mem::{align_of, offset_of, size_of};

        // `capacity` plus padding occupies one cache line; each index gets its own line.
        assert_eq!(size_of::<PaddedUsize>(), 64);
        assert_eq!(align_of::<PaddedUsize>(), 64);
        assert_eq!(size_of::<Header>(), 192);
        assert_eq!(align_of::<Header>(), 64);
        assert_eq!(HEADER_SIZE, 192);
        assert_eq!(offset_of!(Header, capacity), 0);
        assert_eq!(offset_of!(Header, write_idx), 64);
        assert_eq!(offset_of!(Header, read_idx), 128);
    }

    #[test]
    fn test_align_up() {
        assert_eq!(align_up(0, 64), 0);
        assert_eq!(align_up(1, 64), 64);
        assert_eq!(align_up(63, 64), 64);
        assert_eq!(align_up(64, 64), 64);
        assert_eq!(align_up(65, 64), 128);
        assert_eq!(align_up(HEADER_SIZE, 64), HEADER_SIZE);
        assert_eq!(align_up(HEADER_SIZE + 1, 64), 256);
        assert_eq!(align_up(7, 8), 8);
        assert_eq!(align_up(8, 8), 8);
        assert_eq!(align_up(10, 1), 10);
    }

    #[test]
    fn test_available_on_fresh_queue() {
        // one slot is always wasted to distinguish full from empty
        for capacity in [2usize, 3, 8, 64, 1024] {
            let (queue, _guard) = open_queue::<u64>("available", capacity);
            assert_eq!(queue.available(), capacity - 1, "capacity {capacity}");
        }
    }

    #[test]
    fn test_capacity_two_holds_one_element() {
        let (mut queue, _guard) = open_queue::<u32>("cap2", 2);
        assert_eq!(queue.available(), 1);
        assert_eq!(queue.push(1), Ok(()));
        assert_eq!(queue.available(), 0);
        assert_eq!(queue.push(2), Err(2));
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.available(), 1);
    }

    #[test]
    fn test_push_pop_roundtrip_fifo() {
        const CAP: usize = 64;
        const N: usize = CAP - 1;
        let (mut queue, _guard) = open_queue::<u64>("fifo", CAP);

        for v in 0..N as u64 {
            assert_eq!(queue.push(v), Ok(()));
        }
        assert_eq!(queue.available(), 0);
        for v in 0..N as u64 {
            assert_eq!(queue.pop(), Some(v));
        }
        assert_eq!(queue.available(), CAP - 1);
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_push_on_full_returns_value() {
        let (mut queue, _guard) = open_queue::<u32>("full", 8);
        for v in 0..7u32 {
            assert_eq!(queue.push(v), Ok(()));
        }
        // the rejected element is handed back, not dropped
        assert_eq!(queue.push(99), Err(99));
        assert_eq!(queue.available(), 0);
        for v in 0..7u32 {
            assert_eq!(queue.pop(), Some(v));
        }
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_pop_on_empty_returns_none() {
        let (mut queue, _guard) = open_queue::<u32>("empty", 8);
        assert_eq!(queue.pop(), None);
        assert_eq!(queue.push(1), Ok(()));
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), None);
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_wraparound() {
        const CAP: usize = 8;
        const N: usize = CAP - 1;
        let (mut queue, _guard) = open_queue::<u32>("wrap", CAP);

        // fill and drain so the indices wrap around the ring
        for v in 0..N as u32 {
            assert_eq!(queue.push(v), Ok(()));
        }
        for v in 0..N as u32 {
            assert_eq!(queue.pop(), Some(v));
        }

        // a second full cycle works from the wrapped positions
        for v in N as u32..2 * N as u32 {
            assert_eq!(queue.push(v), Ok(()));
        }
        for v in N as u32..2 * N as u32 {
            assert_eq!(queue.pop(), Some(v));
        }

        // drain the head, refill, and read the remaining elements in order
        for v in 0..4u32 {
            assert_eq!(queue.push(v), Ok(()));
        }
        assert_eq!(queue.pop(), Some(0));
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.push(100), Ok(()));
        assert_eq!(queue.push(101), Ok(()));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), Some(3));
        assert_eq!(queue.pop(), Some(100));
        assert_eq!(queue.pop(), Some(101));
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_reopen_reads_back_pending_items() {
        let path = temp_path("reopen");
        let _guard = TempFile(path.clone());
        {
            let mut queue: SpscQueue<u64> = SpscQueue::open(&path, 16, true).unwrap();
            for v in 0..5u64 {
                assert_eq!(queue.push(v), Ok(()));
            }
        }
        // a second instance of the same file sees the shared header and data
        let mut queue: SpscQueue<u64> = SpscQueue::open(&path, 16, false).unwrap();
        assert_eq!(queue.available(), 10);
        for v in 0..5u64 {
            assert_eq!(queue.pop(), Some(v));
        }
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_open_with_mismatched_capacity_fails() {
        let path = temp_path("wrongcap");
        let _guard = TempFile(path.clone());
        SpscQueue::<u64>::open(&path, 8, true).unwrap();

        let err = match SpscQueue::<u64>::open(&path, 16, false) {
            Ok(_) => panic!("opening with a mismatched capacity should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_open_missing_file_without_create_fails() {
        let path = temp_path("missing");
        let err = match SpscQueue::<u64>::open(&path, 8, false) {
            Ok(_) => panic!("opening a missing file without create should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_create_truncates_existing_file() {
        let path = temp_path("recreate");
        let _guard = TempFile(path.clone());
        {
            let mut queue: SpscQueue<u32> = SpscQueue::open(&path, 8, true).unwrap();
            assert_eq!(queue.push(42), Ok(()));
        }
        // re-creating reinitializes the header: stale data is gone
        let mut queue: SpscQueue<u32> = SpscQueue::open(&path, 8, true).unwrap();
        assert_eq!(queue.available(), 7);
        assert_eq!(queue.pop(), None);
    }

    /// An element type with a larger alignment than the header, to exercise the
    /// alignment-aware data offset.
    #[derive(Copy, Clone, PartialEq, Debug)]
    #[repr(align(128))]
    struct OverAligned(u64);

    #[test]
    fn test_over_aligned_element_type() {
        let (mut queue, _guard) = open_queue::<OverAligned>("overaligned", 4);
        assert_eq!(queue.push(OverAligned(7)), Ok(()));
        assert_eq!(queue.pop(), Some(OverAligned(7)));

        let values = [OverAligned(1), OverAligned(2)];
        assert_eq!(queue.push_batch(&values), 2);
        let mut out = [OverAligned(0); 2];
        assert_eq!(queue.pop_batch(&mut out), 2);
        assert_eq!(out, values);
    }

    #[test]
    fn test_push_batch_pop_batch_roundtrip() {
        let (mut queue, _guard) = open_queue::<u32>("batch", 16);
        let values: Vec<u32> = (0..15).collect();
        assert_eq!(queue.push_batch(&values), 15);
        assert_eq!(queue.available(), 0);
        let mut out = vec![0u32; 15];
        assert_eq!(queue.pop_batch(&mut out), 15);
        assert_eq!(out, values);
    }

    #[test]
    fn test_push_batch_wraparound() {
        let (mut queue, _guard) = open_queue::<u32>("batchwrap", 8);
        // drain cycle then refill so the batch copy crosses the tail
        assert_eq!(queue.push_batch(&[0, 1, 2, 3, 4]), 5);
        let mut out = [0u32; 5];
        assert_eq!(queue.pop_batch(&mut out), 5);
        assert_eq!(out, [0, 1, 2, 3, 4]);
        // write == 5: three slots at 5, 6, 7, then wraps to 0 and 1
        assert_eq!(queue.push_batch(&[10, 11, 12, 13, 14]), 5);
        let mut out = [0u32; 5];
        assert_eq!(queue.pop_batch(&mut out), 5);
        assert_eq!(out, [10, 11, 12, 13, 14]);
    }

    #[test]
    fn test_push_batch_partial_when_full() {
        let (mut queue, _guard) = open_queue::<u32>("batchpartial", 8);
        let values: Vec<u32> = (0..10).collect();
        assert_eq!(queue.push_batch(&values), 7); // only 7 free slots
        let mut out = vec![0u32; 7];
        assert_eq!(queue.pop_batch(&mut out), 7);
        assert_eq!(out, values[..7]);
        // the rest fits after the queue drains
        assert_eq!(queue.push_batch(&values[7..]), 3);
        let mut out = vec![0u32; 3];
        assert_eq!(queue.pop_batch(&mut out), 3);
        assert_eq!(out, values[7..]);
    }

    #[test]
    fn test_pop_batch_partial_when_buffer_smaller() {
        let (mut queue, _guard) = open_queue::<u32>("poppartial", 8);
        assert_eq!(queue.push_batch(&[0, 1, 2, 3, 4]), 5);
        let mut out = [0u32; 3];
        assert_eq!(queue.pop_batch(&mut out), 3);
        assert_eq!(out, [0, 1, 2]);
        assert_eq!(queue.available(), 5);
        let mut out = [0u32; 2];
        assert_eq!(queue.pop_batch(&mut out), 2);
        assert_eq!(out, [3, 4]);
        assert_eq!(queue.available(), 7);
    }

    #[test]
    fn test_empty_batches() {
        let (mut queue, _guard) = open_queue::<u32>("emptybatch", 8);
        assert_eq!(queue.push_batch(&[]), 0);
        let mut out: [u32; 0] = [];
        assert_eq!(queue.pop_batch(&mut out), 0);
    }

    #[test]
    fn test_mixed_single_and_batch_ops() {
        let (mut queue, _guard) = open_queue::<u64>("mixed", 8);
        assert_eq!(queue.push(1), Ok(()));
        assert_eq!(queue.push_batch(&[2, 3, 4]), 3);
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.push(5), Ok(()));
        assert_eq!(queue.push_batch(&[6, 7]), 2);
        assert_eq!(queue.available(), 1);
        let mut out = [0u64; 6];
        assert_eq!(queue.pop_batch(&mut out), 6);
        assert_eq!(out, [2, 3, 4, 5, 6, 7]);
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_spsc_producer_consumer_threads() {
        const CAP: usize = 1024;
        const N: u64 = 10_000;
        let path = temp_path("threads");
        let _guard = TempFile(path.clone());
        SpscQueue::<u64>::open(&path, CAP, true).unwrap(); // initialize, then drop

        let producer = {
            let path = path.clone();
            thread::spawn(move || {
                let mut queue: SpscQueue<u64> = SpscQueue::open(&path, CAP, false).unwrap();
                for v in 0..N {
                    // retry until the slot is free
                    let mut value = v;
                    loop {
                        match queue.push(value) {
                            Ok(()) => break,
                            Err(v) => {
                                value = v;
                                thread::yield_now();
                            }
                        }
                    }
                }
            })
        };
        let consumer = {
            let path = path.clone();
            thread::spawn(move || {
                let mut queue: SpscQueue<u64> = SpscQueue::open(&path, CAP, false).unwrap();
                let mut values = Vec::with_capacity(N as usize);
                while values.len() < N as usize {
                    while let Some(v) = queue.pop() {
                        values.push(v);
                    }
                    thread::yield_now();
                }
                values
            })
        };

        producer.join().unwrap();
        let values = consumer.join().unwrap();
        assert_eq!(values, (0..N).collect::<Vec<_>>());
    }

    #[test]
    fn test_spsc_batch_threads() {
        const CAP: usize = 1024;
        const N: usize = 10_000;
        const CHUNK: usize = 64;
        let path = temp_path("batchthreads");
        let _guard = TempFile(path.clone());
        SpscQueue::<u64>::open(&path, CAP, true).unwrap(); // initialize, then drop

        let producer = {
            let path = path.clone();
            thread::spawn(move || {
                let mut queue: SpscQueue<u64> = SpscQueue::open(&path, CAP, false).unwrap();
                let mut chunk = [0u64; CHUNK];
                let mut sent = 0usize;
                while sent < N {
                    let n = CHUNK.min(N - sent);
                    for (i, item) in chunk.iter_mut().take(n).enumerate() {
                        *item = (sent + i) as u64;
                    }
                    let mut pushed = 0;
                    while pushed < n {
                        pushed += queue.push_batch(&chunk[pushed..n]);
                        if pushed < n {
                            thread::yield_now();
                        }
                    }
                    sent += n;
                }
            })
        };
        let consumer = {
            let path = path.clone();
            thread::spawn(move || {
                let mut queue: SpscQueue<u64> = SpscQueue::open(&path, CAP, false).unwrap();
                let mut values = Vec::with_capacity(N);
                let mut buf = [0u64; CHUNK];
                while values.len() < N {
                    let mut got = queue.pop_batch(&mut buf);
                    while got > 0 {
                        values.extend_from_slice(&buf[..got]);
                        got = queue.pop_batch(&mut buf);
                    }
                    if values.len() < N {
                        thread::yield_now();
                    }
                }
                values
            })
        };

        producer.join().unwrap();
        let values = consumer.join().unwrap();
        assert_eq!(values.len(), N);
        assert_eq!(values, (0..N as u64).collect::<Vec<_>>());
    }
}
