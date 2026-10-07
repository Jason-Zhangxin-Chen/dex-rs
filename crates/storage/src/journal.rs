//! The journals: local persistent storage on memory-mapped files.
//!
//! A journal is a **memory-mapped file**: a fixed header followed by a
//! payload area used as a ring buffer. The mapping removes the read and
//! write syscalls from the hot paths — a write copies the payload into the
//! mapped pages and flushes only the touched ranges to the storage device.
//! The header holds **dual instances** of the metadata of the last two
//! writes; each metadata stores the monotonic write sequence number, the
//! offset and the length of the payload, and its CRC32.
//!
//! A write copies the payload and then replaces the metadata of the *older*
//! of the two slots, which turns the new write into the latest version while
//! the previous one stays as the fallback. A crash between the payload flush
//! and the metadata update leaves the fallback slot intact; a torn metadata
//! update fails the payload CRC check, so recovery detects the corruption by
//! checking the two writes and selects the lower, valid version.
//!
//! Two journal kinds share the header machinery:
//!
//! - [`Journal`]: a **snapshot** journal — each write replaces the latest
//!   snapshot; the previous one survives as the fallback. Used by the
//!   [SVD_OMS_Slave] for the book snapshots.
//! - [`Log`]: a **record stream** — every write appends one record to a
//!   linked chain (each record header carries the offset of its
//!   predecessor); a crash loses at most the in-flight tail record, and
//!   [`Log::replay`] walks the chain back to the oldest live record. Used by
//!   the [SVD_Settlement] for the trade log and the batch states.

use std::fs::OpenOptions;
use std::path::Path;

use memmap2::{MmapMut, MmapOptions};

/// Magic of the snapshot journal header.
const JOURNAL_MAGIC: [u8; 8] = *b"SVDJRN01";
/// Magic of the record log header.
const LOG_MAGIC: [u8; 8] = *b"SVDLOG01";
/// Version of the journal formats.
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
/// Size of one record header of the [`Log`].
pub const RECORD_HEADER_SIZE: u64 = 24;
/// The `prev` link of the first record: there is no predecessor.
const NIL: u64 = u64::MAX;

/// Errors of the journals.
#[derive(Debug)]
pub enum JournalError {
    /// The file operation failed.
    Io(std::io::Error),
    /// The payload does not fit into the payload area without clobbering
    /// the live records.
    Full,
    /// The payload is larger than the whole payload area.
    TooLarge,
    /// The file at the path is not a journal of the expected kind.
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
    /// Offset of the payload (or record) in the payload area.
    offset: u64,
    /// Length of the payload (or record) in bytes.
    len: u32,
    /// CRC32 over the slot's region.
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

/// Opens (creating when missing) the journal file of the given total size.
/// A fresh or zero-length file is initialized with the given magic; an
/// existing file is grown to the configured size when shorter and mapped
/// only up to it when longer.
fn open_mmap(path: &Path, size: u64, magic: [u8; 8]) -> Result<(MmapMut, u64), JournalError> {
    if size <= HEADER_SIZE {
        return Err(JournalError::BadFormat(format!(
            "the journal size {size} must exceed the {HEADER_SIZE}-byte header"
        )));
    }
    let len = usize::try_from(size).map_err(|_| {
        JournalError::BadFormat("the journal size does not fit the address space".to_string())
    })?;
    let capacity = size - HEADER_SIZE;
    let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
    let file_len = file.metadata()?.len();
    if file_len == 0 {
        file.set_len(len as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(len).map_mut(&file)? };
        mmap[0..8].copy_from_slice(&magic);
        mmap[VERSION_OFFSET as usize..VERSION_OFFSET as usize + 4]
            .copy_from_slice(&VERSION.to_le_bytes());
        mmap.flush_range(0, HEADER_SIZE as usize)?;
        return Ok((mmap, capacity));
    }
    // A file shorter than the configured size is grown to it; the ring
    // never addresses beyond the configured area, so the growth is safe.
    // A longer file is mapped only up to the configured size, matching the
    // area the slots may address.
    if file_len < size {
        file.set_len(size)?;
    }
    let mmap = unsafe { MmapOptions::new().len(len).map_mut(&file)? };
    validate_header(&mmap, magic)?;
    Ok((mmap, capacity))
}

/// Validates the magic and the version of an existing journal header.
fn validate_header(mmap: &MmapMut, magic: [u8; 8]) -> Result<(), JournalError> {
    let header = &mmap[0..HEADER_SIZE as usize];
    if header[0..8] != magic {
        return Err(JournalError::BadFormat("the magic does not match".to_string()));
    }
    let version = u32::from_le_bytes(
        header[VERSION_OFFSET as usize..VERSION_OFFSET as usize + 4].try_into().expect("4 bytes"),
    );
    if version != VERSION {
        return Err(JournalError::BadFormat(format!("unsupported journal version {version}")));
    }
    Ok(())
}

/// Reads one metadata slot from the header.
fn read_slot(header: &[u8], offset: u64) -> Slot {
    let bytes: [u8; SLOT_SIZE] =
        header[offset as usize..offset as usize + SLOT_SIZE].try_into().expect("32 bytes");
    Slot::decode(&bytes)
}

/// Writes one metadata slot into the mapped header and flushes it.
fn write_slot(mmap: &mut MmapMut, slot: Slot, index: usize) -> Result<(), JournalError> {
    let offset = if index == 0 { SLOT0_OFFSET } else { SLOT1_OFFSET };
    mmap[offset as usize..offset as usize + SLOT_SIZE].copy_from_slice(&slot.encode());
    mmap.flush_range(offset as usize, SLOT_SIZE)?;
    Ok(())
}

/// The sequence number after the newest slot: a reopened journal never
/// reuses a sequence number.
fn next_seq_of(slots: &[Slot; 2]) -> u64 {
    slots.iter().map(|slot| slot.seq).max().unwrap_or(0).saturating_add(1)
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

/// The dual-instance snapshot journal, backed by a memory-mapped file.
/// Each write replaces the latest snapshot; the previous one survives as
/// the fallback of the next write.
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
    /// header; an existing file is validated and its state recovered.
    pub fn open<P: AsRef<Path>>(path: P, size: u64) -> Result<Self, JournalError> {
        let (mmap, capacity) = open_mmap(path.as_ref(), size, JOURNAL_MAGIC)?;
        let slots = [
            read_slot(&mmap[0..HEADER_SIZE as usize], SLOT0_OFFSET),
            read_slot(&mmap[0..HEADER_SIZE as usize], SLOT1_OFFSET),
        ];
        let next_seq = next_seq_of(&slots);
        let latest = valid_slot(&mmap, &slots, capacity).map(|(index, _)| index);
        Ok(Self { mmap, capacity, next_seq, slots, latest })
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
        write_slot(&mut self.mmap, self.slots[target], target)?;
        self.latest = Some(target);
        self.next_seq = self.next_seq.saturating_add(1);
        Ok(seq)
    }

    /// The sequence number of the next write.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

/// The header of one [`Log`] record, stored in the payload area right
/// before the record payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordHeader {
    /// Sequence number of the record.
    seq: u64,
    /// Length of the record payload.
    len: u32,
    /// CRC32 of the record payload.
    crc: u32,
    /// Offset of the previous record in the payload area, [`NIL`] for the
    /// first record of the log.
    prev: u64,
}

impl RecordHeader {
    /// Encodes the record header, little-endian.
    fn encode(self) -> [u8; RECORD_HEADER_SIZE as usize] {
        let mut bytes = [0u8; RECORD_HEADER_SIZE as usize];
        bytes[0..8].copy_from_slice(&self.seq.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.len.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.crc.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.prev.to_le_bytes());
        bytes
    }

    /// Decodes the record header, little-endian.
    fn decode(bytes: &[u8; RECORD_HEADER_SIZE as usize]) -> RecordHeader {
        RecordHeader {
            seq: u64::from_le_bytes(bytes[0..8].try_into().expect("8 bytes")),
            len: u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes")),
            crc: u32::from_le_bytes(bytes[12..16].try_into().expect("4 bytes")),
            prev: u64::from_le_bytes(bytes[16..24].try_into().expect("8 bytes")),
        }
    }
}

/// The record stream journal, backed by a memory-mapped file. Every write
/// appends one record to a singly linked chain over the ring: the latest
/// write commits through the dual metadata slots like the [`Journal`], and
/// each record header links its predecessor, so the whole live log is
/// reachable from the latest slot.
pub struct Log {
    /// The memory-mapped log file: the header and the payload area.
    mmap: MmapMut,
    /// Size of the payload area.
    capacity: u64,
    /// Sequence number of the next write.
    next_seq: u64,
    /// The two metadata slots.
    slots: [Slot; 2],
    /// Index of the slot holding the latest valid write; `None` on a fresh
    /// log.
    latest: Option<usize>,
    /// Offset of the oldest live record; `None` on a fresh log.
    oldest: Option<u64>,
}

impl Log {
    /// Opens (creating when missing) the log file of the given total size.
    /// A fresh or zero-length file is initialized with an empty header; an
    /// existing file is validated and its state recovered.
    pub fn open<P: AsRef<Path>>(path: P, size: u64) -> Result<Self, JournalError> {
        let (mmap, capacity) = open_mmap(path.as_ref(), size, LOG_MAGIC)?;
        let slots = [
            read_slot(&mmap[0..HEADER_SIZE as usize], SLOT0_OFFSET),
            read_slot(&mmap[0..HEADER_SIZE as usize], SLOT1_OFFSET),
        ];
        let next_seq = next_seq_of(&slots);
        let latest = valid_log_slot(&mmap, &slots, capacity).map(|(index, _)| index);
        let oldest = match latest {
            Some(index) => oldest_offset(&mmap, capacity, slots[index].offset),
            None => None,
        };
        Ok(Self { mmap, capacity, next_seq, slots, latest, oldest })
    }

    /// Appends one record, committing it through the older metadata slot.
    /// Returns the sequence number of the record. The write never clobbers
    /// a live record: a record that does not fit returns [`JournalError::Full`].
    pub fn write(&mut self, payload: &[u8]) -> Result<u64, JournalError> {
        if payload.len() as u64 > self.capacity - RECORD_HEADER_SIZE {
            return Err(JournalError::TooLarge);
        }
        if payload.is_empty() {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "the log payload must not be empty",
            )));
        }
        let len_total = RECORD_HEADER_SIZE + payload.len() as u64;

        // The target slot is the older one, like the snapshot journal; on a
        // fresh log the first record goes into slot 0.
        let (target, latest_slot) = match self.latest {
            Some(latest) => ((latest + 1) % 2, self.slots[latest]),
            None => (0, Slot::EMPTY),
        };

        // The ring position: right after the latest record. The record must
        // fit contiguously without clobbering the oldest live record (the
        // whole log stays replayable) — in the free region before the
        // oldest record, or wrapped at the head of the area. Until a
        // compaction advances the oldest record, the log fills linearly and
        // then reports `Full`.
        let (pos, prev) = if latest_slot.is_empty() {
            (0, NIL)
        } else {
            let start = (latest_slot.offset + latest_slot.len as u64) % self.capacity;
            let oldest = self.oldest.expect("a live log has an oldest record");
            let pos = if start <= oldest {
                // The free region is [start, oldest).
                if start + len_total > oldest {
                    return Err(JournalError::Full);
                }
                start
            } else if start + len_total <= self.capacity {
                // The free region is [start, capacity) ∪ [0, oldest); the
                // record fits at the tail.
                start
            } else if len_total <= oldest {
                // The record wraps to the head of the area.
                0
            } else {
                return Err(JournalError::Full);
            };
            (pos, latest_slot.offset)
        };

        // Payload first, record header second, metadata last: a crash in
        // between leaves the previous slot untouched and the torn record
        // unreachable through the chain (its header either lacks a valid
        // CRC or is never linked).
        let record_offset = HEADER_SIZE + pos;
        let payload_offset = record_offset + RECORD_HEADER_SIZE;
        self.mmap[payload_offset as usize..payload_offset as usize + payload.len()]
            .copy_from_slice(payload);
        self.mmap.flush_range(payload_offset as usize, payload.len())?;
        let seq = self.next_seq;
        let header =
            RecordHeader { seq, len: payload.len() as u32, crc: crc32fast::hash(payload), prev };
        self.mmap[record_offset as usize..payload_offset as usize]
            .copy_from_slice(&header.encode());
        self.mmap.flush_range(record_offset as usize, len_total as usize)?;

        self.slots[target] = Slot {
            seq,
            offset: pos,
            len: len_total as u32,
            crc: crc32fast::hash(
                &self.mmap[record_offset as usize..payload_offset as usize + payload.len()],
            ),
        };
        write_slot(&mut self.mmap, self.slots[target], target)?;
        self.latest = Some(target);
        self.oldest.get_or_insert(pos);
        self.next_seq = self.next_seq.saturating_add(1);
        Ok(seq)
    }

    /// Replays the live records in ascending sequence order: walks the
    /// chain from the latest valid record back to the oldest and returns
    /// `(sequence, payload)` pairs, oldest first. A torn tail record (or a
    /// broken chain link) ends the walk — the valid prefix is returned.
    pub fn replay(&self) -> Result<Vec<(u64, Vec<u8>)>, JournalError> {
        let Some(latest) = self.latest else { return Ok(Vec::new()) };
        let mut records = Vec::new();
        let mut offset = self.slots[latest].offset;
        loop {
            let Some((seq, payload, prev)) = read_record(&self.mmap, self.capacity, offset) else {
                break;
            };
            records.push((seq, payload));
            if prev == NIL {
                break;
            }
            offset = prev;
        }
        records.reverse();
        Ok(records)
    }

    /// The sequence number of the next write.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

/// Finds the valid log slot with the highest sequence: reads the record
/// each slot addresses and verifies its CRC over the record bytes.
fn valid_log_slot(mmap: &MmapMut, slots: &[Slot; 2], capacity: u64) -> Option<(usize, u64)> {
    let mut best: Option<(u64, usize)> = None;
    for (index, slot) in slots.iter().enumerate() {
        if slot.is_empty()
            || !slot.region_in_area(capacity)
            || slot.len as u64 <= RECORD_HEADER_SIZE
        {
            continue;
        }
        let offset = HEADER_SIZE as usize + slot.offset as usize;
        let record = &mmap[offset..offset + slot.len as usize];
        if crc32fast::hash(record) == slot.crc
            && best.as_ref().is_none_or(|(seq, _)| slot.seq > *seq)
        {
            best = Some((slot.seq, index));
        }
    }
    best.map(|(_, index)| (index, slots[index].offset))
}

/// Walks the chain from the latest record offset back to the oldest live
/// record offset. Stops at the first invalid link (a torn record never got
/// linked, so the chain only traverses committed records).
fn oldest_offset(mmap: &MmapMut, capacity: u64, latest: u64) -> Option<u64> {
    let mut offset = latest;
    loop {
        let (_, _, prev) = read_record(mmap, capacity, offset)?;
        if prev == NIL {
            return Some(offset);
        }
        offset = prev;
    }
}

/// Reads and validates the record at `offset`: returns its sequence, the
/// payload and the previous link. `None` on a torn or out-of-area record.
fn read_record(mmap: &MmapMut, capacity: u64, offset: u64) -> Option<(u64, Vec<u8>, u64)> {
    let header_offset = HEADER_SIZE + offset;
    let bytes: [u8; RECORD_HEADER_SIZE as usize] = mmap
        .get(header_offset as usize..header_offset as usize + RECORD_HEADER_SIZE as usize)?
        .try_into()
        .expect("record header size");
    let header = RecordHeader::decode(&bytes);
    if header.len == 0 || header.offset_overflow(capacity) {
        return None;
    }
    let payload_offset = header_offset + RECORD_HEADER_SIZE;
    let payload =
        mmap.get(payload_offset as usize..payload_offset as usize + header.len as usize)?.to_vec();
    if crc32fast::hash(&payload) != header.crc {
        return None;
    }
    Some((header.seq, payload, header.prev))
}

impl RecordHeader {
    /// Whether the record addresses a region inside the payload area.
    fn offset_overflow(self, capacity: u64) -> bool {
        u64::from(self.len) > capacity
    }
}

#[cfg(test)]
mod journal_tests {
    use super::*;
    use std::fs;
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Monotonic counter so tests running in parallel never collide on
    /// temp paths.
    static SEQ: AtomicUsize = AtomicUsize::new(0);

    /// A unique path under the system temp dir.
    fn temp_path(tag: &str) -> PathBuf {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "dex_storage_journal_{}_{}_{}",
            std::process::id(),
            tag,
            seq
        ))
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
    fn corrupt_slot_region(journal: &mut Journal, slot: Slot) {
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
        corrupt_slot_region(&mut journal, slot);

        assert_eq!(journal.recover().unwrap().unwrap(), payload(0x11, 32));
    }

    #[test]
    fn test_corrupt_both_slots_recovers_nothing() {
        let (mut journal, _guard) = open_journal("bothcorrupt", 4096);
        journal.write(&payload(0x11, 32)).unwrap();
        journal.write(&payload(0x22, 32)).unwrap();
        for index in [0usize, 1] {
            let slot = journal.slots[index];
            corrupt_slot_region(&mut journal, slot);
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
        write_slot(&mut journal.mmap, journal.slots[latest], latest).unwrap();

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

#[cfg(test)]
mod log_tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Monotonic counter so tests running in parallel never collide on
    /// temp paths.
    static SEQ: AtomicUsize = AtomicUsize::new(0);

    fn temp_path(tag: &str) -> PathBuf {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dex_storage_log_{}_{}_{}", std::process::id(), tag, seq))
    }

    struct TempFile(PathBuf);

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn open_log(tag: &str, size: u64) -> (Log, TempFile) {
        let path = temp_path(tag);
        let log = Log::open(&path, size).unwrap();
        (log, TempFile(path))
    }

    fn payload(tag: u8, len: usize) -> Vec<u8> {
        vec![tag; len]
    }

    #[test]
    fn test_fresh_log_replays_nothing() {
        let (log, _guard) = open_log("fresh", 4096);
        assert_eq!(log.replay().unwrap(), Vec::new());
        assert_eq!(log.next_seq(), 1);
    }

    #[test]
    fn test_write_replay_roundtrip_in_sequence_order() {
        let (mut log, _guard) = open_log("roundtrip", 4096);
        for tag in 1u8..=5 {
            assert_eq!(log.write(&payload(tag, 32)).unwrap(), tag as u64);
        }
        let records = log.replay().unwrap();
        assert_eq!(
            records,
            (1u8..=5).map(|tag| (tag as u64, payload(tag, 32))).collect::<Vec<_>>()
        );
        assert_eq!(log.next_seq(), 6);
    }

    #[test]
    fn test_reopen_persists_the_whole_log() {
        let path = temp_path("reopen");
        let _guard = TempFile(path.clone());
        {
            let mut log = Log::open(&path, 4096).unwrap();
            log.write(&payload(0x11, 32)).unwrap();
            log.write(&payload(0x22, 32)).unwrap();
        }
        let log = Log::open(&path, 4096).unwrap();
        assert_eq!(log.replay().unwrap(), vec![(1, payload(0x11, 32)), (2, payload(0x22, 32))]);
        assert_eq!(log.next_seq(), 3);
    }

    #[test]
    fn test_log_fills_until_full_and_replays_whole() {
        // A 200-byte area holds four 48-byte records ([0, 192)); the fifth
        // does not fit — the log never clobbers live records, it fills up.
        let (mut log, _guard) = open_log("fill", HEADER_SIZE + 200);
        for tag in 1u8..=4 {
            assert_eq!(log.write(&payload(tag, 24)).unwrap(), tag as u64);
        }
        assert!(matches!(log.write(&payload(5, 24)), Err(JournalError::Full)));
        let records = log.replay().unwrap();
        assert_eq!(records.len(), 4);
        assert_eq!(records[0], (1, payload(1, 24)));
        assert_eq!(records[3], (4, payload(4, 24)));
    }

    #[test]
    fn test_full_when_the_oldest_record_would_be_clobbered() {
        // A 320-byte area holds exactly two 160-byte records; the third
        // would clobber the first one.
        let (mut log, _guard) = open_log("full", HEADER_SIZE + 320);
        log.write(&payload(0x11, 136)).unwrap();
        log.write(&payload(0x22, 136)).unwrap();
        assert!(matches!(log.write(&payload(0x33, 136)), Err(JournalError::Full)));
        // The log is still replayable whole.
        assert_eq!(log.replay().unwrap(), vec![(1, payload(0x11, 136)), (2, payload(0x22, 136))]);
    }

    #[test]
    fn test_torn_tail_falls_back_to_the_previous_record() {
        let path = temp_path("torn");
        let _guard = TempFile(path.clone());
        {
            let mut log = Log::open(&path, 4096).unwrap();
            log.write(&payload(0x11, 32)).unwrap();
            log.write(&payload(0x22, 32)).unwrap();
        }
        // Corrupt the latest record region (torn write): the replay ends at
        // the previous record.
        let mut mmap = unsafe {
            let file = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
            memmap2::MmapOptions::new().map_mut(&file).unwrap()
        };
        // Reopen to learn the latest slot, then clobber its region.
        let log = Log::open(&path, 4096).unwrap();
        let slot = log.slots[log.latest.unwrap()];
        let offset = HEADER_SIZE as usize + slot.offset as usize;
        mmap[offset..offset + slot.len as usize].fill(0xff);
        mmap.flush_range(offset, slot.len as usize).unwrap();
        drop(mmap);
        let log = Log::open(&path, 4096).unwrap();
        assert_eq!(log.replay().unwrap(), vec![(1, payload(0x11, 32))]);
    }

    #[test]
    fn test_torn_metadata_keeps_the_previous_record() {
        let path = temp_path("tornmeta");
        let _guard = TempFile(path.clone());
        {
            let mut log = Log::open(&path, 4096).unwrap();
            log.write(&payload(0x11, 32)).unwrap();
            log.write(&payload(0x22, 32)).unwrap();
        }
        // Garbage metadata of the latest slot: the valid older slot wins.
        let mut log = Log::open(&path, 4096).unwrap();
        let latest = log.latest.unwrap();
        log.slots[latest].offset = u64::MAX;
        write_slot(&mut log.mmap, log.slots[latest], latest).unwrap();
        drop(log);
        let log = Log::open(&path, 4096).unwrap();
        assert_eq!(log.replay().unwrap(), vec![(1, payload(0x11, 32))]);
    }

    #[test]
    fn test_payload_larger_than_area_is_rejected() {
        let (mut log, _guard) = open_log("toolarge", HEADER_SIZE + 64);
        assert!(matches!(log.write(&payload(0x11, 128)), Err(JournalError::TooLarge)));
    }

    #[test]
    fn test_empty_payload_is_rejected() {
        let (mut log, _guard) = open_log("empty", 4096);
        assert!(log.write(&[]).is_err());
    }

    #[test]
    fn test_wrong_magic_is_rejected() {
        // A snapshot journal file is not a log.
        let path = temp_path("wrongmagic");
        let _guard = TempFile(path.clone());
        Journal::open(&path, 4096).unwrap();
        assert!(matches!(Log::open(&path, 4096), Err(JournalError::BadFormat(_))));
    }

    #[test]
    fn test_size_at_most_header_is_rejected() {
        let path = temp_path("small");
        let _guard = TempFile(path.clone());
        assert!(Log::open(&path, HEADER_SIZE).is_err());
    }
}
