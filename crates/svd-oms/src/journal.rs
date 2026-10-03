//! The journal: a local persistent storage of the book snapshots.
//!
//! The journal is a **memory-mapped file**: a fixed header followed by a
//! payload area used as a ring buffer. The mapping removes the read and
//! write syscalls from the snapshot path — a write copies the payload into
//! the mapped pages and flushes only the touched ranges to the storage
//! device. The header holds **dual instances** of the metadata of the last
//! two writes; each metadata stores the monotonic write sequence number,
//! the offset and the length of the payload, and its CRC32.
//!
//! A write copies the payload and then replaces the metadata of the *older*
//! of the two slots, which turns the new write into the latest version while
//! the previous one stays as the fallback. A crash between the payload flush
//! and the metadata update leaves the fallback slot intact; a torn metadata
//! update fails the payload CRC check, so recovery detects the corruption by
//! checking the two writes and selects the lower, valid version.
//!
//! The new payload never overlaps the latest payload's region: the latest
//! payload is the fallback of the next write and must survive it. The older
//! payload may be overwritten — its metadata is replaced next anyway.

use std::fs::{File, OpenOptions};
use std::path::Path;

use memmap2::{MmapMut, MmapOptions};

/// Magic of the journal header.
const MAGIC: [u8; 8] = *b"SVDJRN01";
/// Version of the journal format.
const VERSION: u32 = 1;
/// Fixed size of the journal header.
pub const HEADER_SIZE: u64 = 512;
/// Offset of the first metadata slot in the header.
const SLOT0_OFFSET: u64 = 16;
/// Offset of the second metadata slot in the header.
const SLOT1_OFFSET: u64 = 48;
/// Size of one metadata slot.
const SLOT_SIZE: usize = 32;
/// Offset of the version field in the header.
const VERSION_OFFSET: u64 = 8;

/// Errors of the journal.
#[derive(Debug)]
pub enum JournalError {
    /// The file operation failed.
    Io(std::io::Error),
    /// The payload does not fit into the payload area without clobbering
    /// the latest valid snapshot.
    Full,
    /// The payload is larger than the whole payload area.
    TooLarge,
    /// The file at the path is not a journal.
    BadFormat(String),
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalError::Io(err) => write!(f, "journal io: {err}"),
            JournalError::Full => write!(f, "journal full"),
            JournalError::TooLarge => write!(f, "payload larger than the journal"),
            JournalError::BadFormat(what) => write!(f, "bad journal format: {what}"),
        }
    }
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            JournalError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for JournalError {
    fn from(err: std::io::Error) -> Self {
        JournalError::Io(err)
    }
}

/// The metadata of one write, stored in one of the two header slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Slot {
    /// Monotonic sequence number of the write.
    seq: u64,
    /// Offset of the payload in the payload area.
    offset: u64,
    /// Length of the payload.
    len: u32,
    /// CRC32 of the payload.
    crc: u32,
}

impl Slot {
    /// A slot that never received a write.
    const EMPTY: Slot = Slot { seq: 0, offset: 0, len: 0, crc: 0 };

    /// Encodes the slot metadata, little-endian.
    fn encode(self) -> [u8; SLOT_SIZE] {
        let mut bytes = [0u8; SLOT_SIZE];
        bytes[0..8].copy_from_slice(&self.seq.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.offset.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.len.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.crc.to_le_bytes());
        bytes
    }

    /// Decodes the slot metadata, little-endian.
    fn decode(bytes: &[u8; SLOT_SIZE]) -> Slot {
        Slot {
            seq: u64::from_le_bytes(bytes[0..8].try_into().expect("8 bytes")),
            offset: u64::from_le_bytes(bytes[8..16].try_into().expect("8 bytes")),
            len: u32::from_le_bytes(bytes[16..20].try_into().expect("4 bytes")),
            crc: u32::from_le_bytes(bytes[20..24].try_into().expect("4 bytes")),
        }
    }

    /// Whether the slot received a write at all.
    fn is_empty(self) -> bool {
        self.seq == 0 && self.len == 0
    }

    /// Whether the slot addresses a region inside the payload area.
    fn region_in_area(self, capacity: u64) -> bool {
        self.len > 0 && self.offset.saturating_add(self.len as u64) <= capacity
    }
}

/// The dual-instance journal of the book snapshots, backed by a
/// memory-mapped file.
pub struct Journal {
    /// The memory-mapped journal file: the header and the payload area.
    mmap: MmapMut,
    /// Size of the payload area.
    capacity: u64,
    /// Sequence number of the next write.
    next_seq: u64,
    /// The two metadata slots.
    slots: [Slot; 2],
    /// Index of the slot holding the latest valid write; `None` on a fresh
    /// journal.
    latest: Option<usize>,
}

impl Journal {
    /// Opens (creating when missing) the journal file of the given total
    /// size. A fresh or zero-length file is initialized with an empty
    /// header; an existing file is validated and its state recovered. The
    /// file is mapped whole: all the journal I/O operates on the mapping.
    pub fn open<P: AsRef<Path>>(path: P, size: u64) -> Result<Self, JournalError> {
        if size <= HEADER_SIZE {
            return Err(JournalError::BadFormat(format!(
                "the journal size {size} must exceed the {HEADER_SIZE}-byte header"
            )));
        }
        let len = usize::try_from(size).map_err(|_| {
            JournalError::BadFormat("the journal size does not fit the address space".to_string())
        })?;
        let capacity = size - HEADER_SIZE;
        let file =
            OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        let file_len = file.metadata()?.len();
        if file_len == 0 {
            return Self::init(file, capacity, len);
        }
        // A file shorter than the configured size is grown to it; the ring
        // never addresses beyond the configured area, so the growth is
        // safe. A longer file is mapped only up to the configured size,
        // matching the area the slots may address.
        if file_len < size {
            file.set_len(size)?;
        }
        let mmap = unsafe { MmapOptions::new().len(len).map_mut(&file)? };
        Self::open_existing(mmap, capacity)
    }

    /// Initializes a fresh journal file with an empty header.
    fn init(file: File, capacity: u64, len: usize) -> Result<Self, JournalError> {
        file.set_len(len as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(len).map_mut(&file)? };
        mmap[0..8].copy_from_slice(&MAGIC);
        mmap[VERSION_OFFSET as usize..VERSION_OFFSET as usize + 4]
            .copy_from_slice(&VERSION.to_le_bytes());
        mmap.flush_range(0, HEADER_SIZE as usize)?;
        Ok(Self { mmap, capacity, next_seq: 1, slots: [Slot::EMPTY; 2], latest: None })
    }

    /// Validates the header of an existing journal and recovers its state.
    fn open_existing(mmap: MmapMut, capacity: u64) -> Result<Self, JournalError> {
        let header = &mmap[0..HEADER_SIZE as usize];
        if header[0..8] != MAGIC {
            return Err(JournalError::BadFormat("the magic does not match".to_string()));
        }
        let version = u32::from_le_bytes(
            header[VERSION_OFFSET as usize..VERSION_OFFSET as usize + 4]
                .try_into()
                .expect("4 bytes"),
        );
        if version != VERSION {
            return Err(JournalError::BadFormat(format!("unsupported journal version {version}")));
        }
        let slots = [Self::read_slot(header, SLOT0_OFFSET), Self::read_slot(header, SLOT1_OFFSET)];
        // The write sequence continues after the newest slot, so a reopened
        // journal never reuses a sequence number.
        let next_seq = slots.iter().map(|slot| slot.seq).max().unwrap_or(0).saturating_add(1);
        // The latest valid write, once the payloads are checked.
        let latest = valid_slot(&mmap, &slots, capacity).map(|(index, _)| index);
        Ok(Self { mmap, capacity, next_seq, slots, latest })
    }

    /// Reads one metadata slot from the header.
    fn read_slot(header: &[u8], offset: u64) -> Slot {
        let bytes: [u8; SLOT_SIZE] =
            header[offset as usize..offset as usize + SLOT_SIZE].try_into().expect("32 bytes");
        Slot::decode(&bytes)
    }

    /// Writes one metadata slot into the mapped header and flushes it.
    fn write_slot(&mut self, index: usize) -> Result<(), JournalError> {
        let offset = if index == 0 { SLOT0_OFFSET } else { SLOT1_OFFSET };
        self.mmap[offset as usize..offset as usize + SLOT_SIZE]
            .copy_from_slice(&self.slots[index].encode());
        self.mmap.flush_range(offset as usize, SLOT_SIZE)?;
        Ok(())
    }

    /// Recovers the latest valid snapshot payload, if any. Each slot is
    /// validated by the CRC of its payload; the valid slot with the highest
    /// sequence wins. A torn latest write therefore falls back to the
    /// previous one, and a fully corrupt journal recovers nothing.
    pub fn recover(&mut self) -> Result<Option<Vec<u8>>, JournalError> {
        Ok(valid_slot(&self.mmap, &self.slots, self.capacity).map(|(_, payload)| payload))
    }

    /// Appends a snapshot payload, replacing the older metadata slot with
    /// the new write. Returns the sequence number of the write.
    pub fn write(&mut self, payload: &[u8]) -> Result<u64, JournalError> {
        if payload.len() as u64 > self.capacity {
            return Err(JournalError::TooLarge);
        }
        if payload.is_empty() {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "the journal payload must not be empty",
            )));
        }
        let len = payload.len() as u64;

        // The target slot is the older one: the latest write keeps its
        // metadata and serves as the fallback of this write. On a fresh
        // journal the first write goes into slot 0.
        let (target, latest_slot) = match self.latest {
            Some(latest) => ((latest + 1) % 2, self.slots[latest]),
            None => (0, Slot::EMPTY),
        };

        // The ring position: right after the latest payload, wrapping to the
        // head of the area. The region must not clobber the latest payload,
        // which is the fallback of this very write.
        let start = (latest_slot.offset + latest_slot.len as u64) % self.capacity;
        let available = if latest_slot.is_empty() {
            self.capacity
        } else if start <= latest_slot.offset {
            latest_slot.offset - start
        } else {
            self.capacity - start + latest_slot.offset
        };
        if len > available {
            return Err(JournalError::Full);
        }
        let pos = if start + len <= self.capacity { start } else { 0 };

        // Payload first, metadata second: a crash in between leaves the
        // latest slot untouched. The copy lands in the mapped pages and
        // only the touched ranges are flushed to the storage device.
        let offset = HEADER_SIZE + pos;
        self.mmap[offset as usize..offset as usize + payload.len()].copy_from_slice(payload);
        self.mmap.flush_range(offset as usize, payload.len())?;

        let seq = self.next_seq;
        self.slots[target] =
            Slot { seq, offset: pos, len: payload.len() as u32, crc: crc32fast::hash(payload) };
        self.write_slot(target)?;
        self.latest = Some(target);
        self.next_seq = self.next_seq.saturating_add(1);
        Ok(seq)
    }

    /// The sequence number of the next write.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

/// Finds the valid slot with the highest sequence: reads the payload each
/// slot addresses and verifies its CRC.
fn valid_slot(mmap: &MmapMut, slots: &[Slot; 2], capacity: u64) -> Option<(usize, Vec<u8>)> {
    let mut best: Option<(u64, usize, Vec<u8>)> = None;
    for (index, slot) in slots.iter().enumerate() {
        if slot.is_empty() || !slot.region_in_area(capacity) {
            continue;
        }
        let offset = HEADER_SIZE as usize + slot.offset as usize;
        let payload = mmap[offset..offset + slot.len as usize].to_vec();
        if crc32fast::hash(&payload) == slot.crc
            && best.as_ref().is_none_or(|(seq, _, _)| slot.seq > *seq)
        {
            best = Some((slot.seq, index, payload));
        }
    }
    best.map(|(_, index, payload)| (index, payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Monotonic counter so tests running in parallel never collide on
    /// temp paths.
    static SEQ: AtomicUsize = AtomicUsize::new(0);

    /// A unique path under the system temp dir.
    fn temp_path(tag: &str) -> PathBuf {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dex_oms_journal_{}_{}_{}", std::process::id(), tag, seq))
    }

    /// Removes the journal file when dropped.
    struct TempFile(PathBuf);

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    /// Opens a fresh journal plus a guard that removes its file.
    fn open_journal(tag: &str, size: u64) -> (Journal, TempFile) {
        let path = temp_path(tag);
        let journal = Journal::open(&path, size).unwrap();
        (journal, TempFile(path))
    }

    fn payload(tag: u8, len: usize) -> Vec<u8> {
        vec![tag; len]
    }

    /// Corrupts the payload region a slot addresses, as a torn write would.
    fn corrupt_payload(journal: &mut Journal, slot: Slot) {
        let offset = HEADER_SIZE as usize + slot.offset as usize;
        journal.mmap[offset..offset + slot.len as usize].fill(0xff);
    }

    #[test]
    fn test_fresh_journal_recovers_nothing() {
        let (mut journal, _guard) = open_journal("fresh", 4096);
        assert_eq!(journal.recover().unwrap(), None);
        assert_eq!(journal.next_seq(), 1);
    }

    #[test]
    fn test_write_recover_roundtrip() {
        let (mut journal, _guard) = open_journal("roundtrip", 4096);
        assert_eq!(journal.write(&payload(0xab, 100)).unwrap(), 1);
        assert_eq!(journal.recover().unwrap().unwrap(), payload(0xab, 100));
    }

    #[test]
    fn test_slots_alternate_and_sequences_increase() {
        let (mut journal, _guard) = open_journal("alternate", 4096);
        for (tag, seq) in [(0x01u8, 1u64), (0x02, 2), (0x03, 3), (0x04, 4)] {
            assert_eq!(journal.write(&payload(tag, 16)).unwrap(), seq);
        }
        assert_eq!(journal.recover().unwrap().unwrap(), payload(0x04, 16));
        assert_eq!(journal.next_seq(), 5);
    }

    #[test]
    fn test_corrupt_latest_falls_back_to_previous() {
        let (mut journal, _guard) = open_journal("fallback", 4096);
        journal.write(&payload(0x11, 32)).unwrap();
        journal.write(&payload(0x22, 32)).unwrap();

        // Corrupt the latest payload (written at the second slot's region).
        let slot = journal.slots[journal.latest.unwrap()];
        corrupt_payload(&mut journal, slot);

        assert_eq!(journal.recover().unwrap().unwrap(), payload(0x11, 32));
    }

    #[test]
    fn test_corrupt_both_slots_recovers_nothing() {
        let (mut journal, _guard) = open_journal("bothcorrupt", 4096);
        journal.write(&payload(0x11, 32)).unwrap();
        journal.write(&payload(0x22, 32)).unwrap();
        for index in [0usize, 1] {
            let slot = journal.slots[index];
            corrupt_payload(&mut journal, slot);
        }
        assert_eq!(journal.recover().unwrap(), None);
    }

    #[test]
    fn test_torn_metadata_falls_back_to_previous() {
        let (mut journal, _guard) = open_journal("torn", 4096);
        journal.write(&payload(0x11, 32)).unwrap();
        journal.write(&payload(0x22, 32)).unwrap();

        // Simulate a torn metadata update of the latest slot: garbage
        // offset and length address outside the payload area.
        let latest = journal.latest.unwrap();
        journal.slots[latest].offset = u64::MAX;
        journal.write_slot(latest).unwrap();

        assert_eq!(journal.recover().unwrap().unwrap(), payload(0x11, 32));
    }

    #[test]
    fn test_reopen_persists_state() {
        let path = temp_path("reopen");
        let _guard = TempFile(path.clone());
        {
            let mut journal = Journal::open(&path, 4096).unwrap();
            journal.write(&payload(0x33, 64)).unwrap();
            journal.write(&payload(0x44, 64)).unwrap();
        }
        let mut journal = Journal::open(&path, 4096).unwrap();
        assert_eq!(journal.recover().unwrap().unwrap(), payload(0x44, 64));
        // The sequence continues after the reopened journal's newest write.
        assert_eq!(journal.write(&payload(0x55, 64)).unwrap(), 3);
    }

    #[test]
    fn test_wrap_reuses_the_payload_area() {
        // A small area of 200 bytes: writes wrap around the ring.
        let (mut journal, _guard) = open_journal("wrap", HEADER_SIZE + 200);
        for tag in 1u8..=10 {
            let seq = journal.write(&payload(tag, 30)).unwrap();
            assert_eq!(seq, tag as u64);
        }
        assert_eq!(journal.recover().unwrap().unwrap(), payload(10, 30));
    }

    #[test]
    fn test_journal_full_when_payload_does_not_fit() {
        // The first write spans [0, 120); the second would need 120 bytes
        // but only 80 are free between its start (120) and the latest
        // payload's head (0), even wrapping — the journal is full.
        let (mut journal, _guard) = open_journal("full", HEADER_SIZE + 200);
        journal.write(&payload(0x11, 120)).unwrap();
        assert!(matches!(journal.write(&payload(0x22, 120)), Err(JournalError::Full)));
        // The latest snapshot is still recoverable.
        assert_eq!(journal.recover().unwrap().unwrap(), payload(0x11, 120));
    }

    #[test]
    fn test_payload_larger_than_area_is_rejected() {
        let (mut journal, _guard) = open_journal("toolarge", HEADER_SIZE + 64);
        assert!(matches!(journal.write(&payload(0x11, 128)), Err(JournalError::TooLarge)));
    }

    #[test]
    fn test_empty_payload_is_rejected() {
        let (mut journal, _guard) = open_journal("empty", 4096);
        assert!(journal.write(&[]).is_err());
    }

    #[test]
    fn test_bad_magic_is_rejected() {
        let path = temp_path("badmagic");
        let _guard = TempFile(path.clone());
        Journal::open(&path, 4096).unwrap();
        // Clobber the magic.
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all(&[0u8; 8]).unwrap();
        file.sync_all().unwrap();
        assert!(matches!(Journal::open(&path, 4096), Err(JournalError::BadFormat(_))));
    }

    #[test]
    fn test_size_at_most_header_is_rejected() {
        let path = temp_path("small");
        let _guard = TempFile(path.clone());
        assert!(Journal::open(&path, HEADER_SIZE).is_err());
    }

    #[test]
    fn test_write_is_visible_via_the_file() {
        // The flushed pages are visible to a reader of the file itself,
        // which is what the next process opening the journal sees.
        let (mut journal, guard) = open_journal("filevisible", 4096);
        journal.write(&payload(0x5a, 64)).unwrap();
        let slot = journal.slots[journal.latest.unwrap()];
        let mut file = File::open(&guard.0).unwrap();
        file.seek(SeekFrom::Start(HEADER_SIZE + slot.offset)).unwrap();
        let mut read = [0u8; 64];
        file.read_exact(&mut read).unwrap();
        assert_eq!(payload(0x5a, 64), read);
    }

    #[test]
    fn test_reopen_with_a_resized_journal() {
        let path = temp_path("resize");
        let _guard = TempFile(path.clone());
        {
            let mut journal = Journal::open(&path, 2048).unwrap();
            journal.write(&payload(0x11, 64)).unwrap();
        }
        // A larger configured size grows the file; the snapshot survives.
        let mut journal = Journal::open(&path, 4096).unwrap();
        assert_eq!(journal.recover().unwrap().unwrap(), payload(0x11, 64));
        journal.write(&payload(0x22, 64)).unwrap();
        drop(journal);
        // A smaller configured size maps only the head of the file; the
        // snapshot at offset 64 still recovers.
        let mut journal = Journal::open(&path, 2048).unwrap();
        assert_eq!(journal.recover().unwrap().unwrap(), payload(0x22, 64));
    }
}
