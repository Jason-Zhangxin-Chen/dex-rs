//! The settlement journal: the framed records over the shared
//! [`storage::journal::Log`].
//!
//! One journal file holds three record kinds, tagged and MessagePack
//! encoded:
//!
//! - `TradeBatch` — written only by the core thread at drain time: the
//!   hot-path durability boundary. A trade is journaled before it is
//!   handed to the submitter, so a crash never loses a drained trade.
//! - `BatchState` — written only by the submitter thread: one transition
//!   of one batch. The `Received` state carries the batch's trades; the
//!   sequence number of that record IS the batch sequence.
//! - `Outcome` — written only by the submitter thread after the terminal
//!   state and before the publication: the journaled result re-publishes
//!   after a crash.
//!
//! The replay walks the records in sequence order and rebuilds the
//! pre-crash state: the trades journaled but not yet consumed into a
//! `Received` batch, the last-write-wins state of every batch, and the
//! published outcomes.

use std::sync::{Arc, Mutex, MutexGuard};

use primitives::message::hot_path::Trade;
use primitives::message::settlement::SettlementResult;
use storage::journal::{JournalError, Log};

use crate::batch::BatchState;

/// The tag byte of a `TradeBatch` record.
const TAG_TRADE_BATCH: u8 = 1;
/// The tag byte of a `BatchState` record.
const TAG_BATCH_STATE: u8 = 2;
/// The tag byte of an `Outcome` record.
const TAG_OUTCOME: u8 = 3;

/// One journaled settlement record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettlementRecord {
    /// A drained trade group (the core thread only).
    TradeBatch {
        /// The trades, in drain order.
        trades: Vec<Trade>,
    },
    /// One transition of one batch (the submitter only).
    BatchState {
        /// The batch sequence — for the `Received` state, the sequence of
        /// this very record.
        batch_seq: u64,
        /// The transition.
        state: BatchState,
    },
    /// The published result of a terminal batch (the submitter only).
    Outcome {
        /// The batch sequence the result belongs to.
        batch_seq: u64,
        /// The published result.
        result: SettlementResult,
    },
}

/// Errors of the settlement journal.
#[derive(Debug)]
pub enum SettlementJournalError {
    /// The underlying log failed.
    Log(JournalError),
    /// A record could not be decoded (corruption or a format change).
    Codec(String),
}

impl std::fmt::Display for SettlementJournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SettlementJournalError::Log(err) => write!(f, "settlement journal: {err}"),
            SettlementJournalError::Codec(what) => {
                write!(f, "settlement journal codec: {what}")
            }
        }
    }
}

impl std::error::Error for SettlementJournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SettlementJournalError::Log(err) => Some(err),
            SettlementJournalError::Codec(_) => None,
        }
    }
}

impl From<JournalError> for SettlementJournalError {
    fn from(err: JournalError) -> Self {
        SettlementJournalError::Log(err)
    }
}

/// The framed settlement journal. Two writers share it — the core thread
/// (trade batches) and the submitter (batch states and outcomes) — so the
/// writes serialize behind a mutex; the encode happens outside the lock
/// and the lock is held only across the memory-mapped append.
#[derive(Clone)]
pub struct SettlementJournal {
    /// The shared record log.
    log: Arc<Mutex<Log>>,
}

impl SettlementJournal {
    /// Opens (creating when missing) the journal file of the given total
    /// size.
    pub fn open(path: &str, size: u64) -> Result<Self, SettlementJournalError> {
        Ok(Self { log: Arc::new(Mutex::new(Log::open(path, size)?)) })
    }

    /// Appends one record. Returns the sequence number of the record — the
    /// batch sequence of a `Received` `BatchState` record (a `batch_seq` of
    /// zero in the record is replaced with the record's own sequence, under
    /// the same lock that writes it, so the embedded value always matches
    /// the returned one even while the core thread appends trade batches).
    pub fn write(&self, record: &SettlementRecord) -> Result<u64, SettlementJournalError> {
        match record {
            // The core thread's path: encode outside the lock (no sequence
            // fixup needed), hold the lock only across the mapped append.
            SettlementRecord::TradeBatch { trades } => {
                let mut bytes = Vec::with_capacity(256);
                encode_trade_batch(trades, &mut bytes).map_err(SettlementJournalError::Codec)?;
                self.write_encoded(&bytes)
            }
            // The submitter's path: the sequence fixup of a `Received`
            // record must be atomic with the write, so the encode happens
            // under the lock (side path, allocation is fine here).
            SettlementRecord::BatchState { batch_seq, state } => {
                let mut record =
                    SettlementRecord::BatchState { batch_seq: *batch_seq, state: state.clone() };
                let mut log = self.lock();
                if let SettlementRecord::BatchState {
                    batch_seq,
                    state: BatchState::Received { .. },
                } = &mut record
                    && *batch_seq == 0
                {
                    *batch_seq = log.next_seq();
                }
                let mut bytes = Vec::with_capacity(256);
                encode_record(&record, &mut bytes).map_err(SettlementJournalError::Codec)?;
                Ok(log.write(&bytes)?)
            }
            SettlementRecord::Outcome { .. } => {
                let mut bytes = Vec::with_capacity(256);
                encode_record(record, &mut bytes).map_err(SettlementJournalError::Codec)?;
                self.write_encoded(&bytes)
            }
        }
    }

    /// Appends already-framed bytes (the core thread's path): the caller
    /// encodes outside the lock into its reused buffer, so the encode
    /// performs no allocation in the steady state and the lock is held
    /// only across the mapped append.
    pub fn write_encoded(&self, bytes: &[u8]) -> Result<u64, SettlementJournalError> {
        let mut log = self.lock();
        Ok(log.write(bytes)?)
    }

    /// Replays the live records in ascending sequence order, oldest first.
    pub fn replay(&self) -> Result<Vec<(u64, SettlementRecord)>, SettlementJournalError> {
        let log = self.lock();
        let records = log.replay()?;
        drop(log);
        records
            .into_iter()
            .map(|(seq, bytes)| {
                decode_record(&bytes)
                    .map(|record| (seq, record))
                    .map_err(SettlementJournalError::Codec)
            })
            .collect()
    }

    /// Locks the log, poisoning-tolerant.
    fn lock(&self) -> MutexGuard<'_, Log> {
        self.log.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Encodes one record into `bytes` (cleared first): one tag byte followed
/// by the MessagePack payload.
fn encode_record(record: &SettlementRecord, bytes: &mut Vec<u8>) -> Result<(), String> {
    bytes.clear();
    match record {
        SettlementRecord::TradeBatch { trades } => {
            bytes.push(TAG_TRADE_BATCH);
            rmp_serde::encode::write(bytes, trades).map_err(|err| err.to_string())?;
        }
        SettlementRecord::BatchState { batch_seq, state } => {
            bytes.push(TAG_BATCH_STATE);
            rmp_serde::encode::write(bytes, &(batch_seq, state)).map_err(|err| err.to_string())?;
        }
        SettlementRecord::Outcome { batch_seq, result } => {
            bytes.push(TAG_OUTCOME);
            rmp_serde::encode::write(bytes, &(batch_seq, result)).map_err(|err| err.to_string())?;
        }
    }
    Ok(())
}

/// Decodes one record from its framed bytes.
fn decode_record(bytes: &[u8]) -> Result<SettlementRecord, String> {
    let Some((&tag, payload)) = bytes.split_first() else {
        return Err("the record is empty".to_string());
    };
    match tag {
        TAG_TRADE_BATCH => rmp_serde::from_slice::<Vec<Trade>>(payload)
            .map(|trades| SettlementRecord::TradeBatch { trades })
            .map_err(|err| err.to_string()),
        TAG_BATCH_STATE => {
            let (batch_seq, state): (u64, BatchState) =
                rmp_serde::from_slice(payload).map_err(|err| err.to_string())?;
            Ok(SettlementRecord::BatchState { batch_seq, state })
        }
        TAG_OUTCOME => {
            let (batch_seq, result): (u64, SettlementResult) =
                rmp_serde::from_slice(payload).map_err(|err| err.to_string())?;
            Ok(SettlementRecord::Outcome { batch_seq, result })
        }
        other => Err(format!("unknown record tag {other}")),
    }
}

/// Encodes one drained trade group into `bytes` (cleared first): the tag
/// byte followed by the MessagePack payload. The core thread reuses the
/// buffer and the encode runs straight over the slice, so it performs no
/// allocation in the steady state.
pub fn encode_trade_batch(trades: &[Trade], bytes: &mut Vec<u8>) -> Result<(), String> {
    bytes.clear();
    bytes.push(TAG_TRADE_BATCH);
    rmp_serde::encode::write(bytes, trades).map_err(|err| err.to_string())
}

/// The conservative byte budget of one trade-batch record: the tag byte
/// plus twice the in-memory size of the trades (MessagePack of these
/// integer-heavy structs stays well below that). Used by the startup check
/// that one drain batch always fits one record.
pub fn trade_batch_budget(batch_size: usize) -> usize {
    1 + std::mem::size_of::<Trade>().saturating_mul(batch_size).saturating_mul(2)
}

/// The state rebuilt from the journal replay.
#[derive(Debug, Default)]
pub struct Reconstructed {
    /// The trades journaled by the core thread but not yet consumed into a
    /// `Received` batch, in submission order. The submitter re-assembles
    /// them into batches after a restart.
    pub pending_trades: Vec<Trade>,
    /// The last-write-wins state per batch sequence, in ascending sequence
    /// order.
    pub batch_states: Vec<(u64, BatchState)>,
    /// The trades of every batch, captured from its `Received` record: the
    /// later states overwrite the batch state without the trades, so the
    /// replayed batch re-submits with these.
    pub batch_trades: std::collections::BTreeMap<u64, Vec<Trade>>,
    /// The published outcomes, in journal order.
    pub outcomes: Vec<(u64, SettlementResult)>,
}

/// Rebuilds the pre-crash state from the replayed records, walking them in
/// sequence order: a `TradeBatch` appends to the pending trade stream, a
/// `Received` batch state consumes the first `trades.len()` pending trades
/// (the assembler fills batches from the stream in order) and captures the
/// batch's trades, every later state overwrites the batch's state, and
/// every outcome is collected.
pub fn reconstruct(records: Vec<(u64, SettlementRecord)>) -> Reconstructed {
    let mut rebuilt = Reconstructed::default();
    let mut states: std::collections::BTreeMap<u64, BatchState> = std::collections::BTreeMap::new();
    for (seq, record) in records {
        match record {
            SettlementRecord::TradeBatch { trades } => {
                rebuilt.pending_trades.extend(trades);
            }
            SettlementRecord::BatchState { batch_seq, state } => {
                if let BatchState::Received { trades } = &state {
                    // The batch consumed the first `trades.len()` pending
                    // trades of the stream.
                    let consumed = trades.len().min(rebuilt.pending_trades.len());
                    rebuilt.pending_trades.drain(..consumed);
                    rebuilt.batch_trades.insert(batch_seq, trades.clone());
                }
                // The first BatchState record of a batch (the Received
                // record) carries the batch sequence; later transitions
                // overwrite the state.
                states.insert(batch_seq, state);
            }
            SettlementRecord::Outcome { batch_seq, result } => {
                // Sanity: the outcome must reference a known batch.
                debug_assert!(states.contains_key(&batch_seq), "outcome seq {batch_seq} unknown");
                rebuilt.outcomes.push((seq, result));
            }
        }
    }
    rebuilt.batch_states = states.into_iter().collect();
    rebuilt
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::address::Address;
    use primitives::base::{Hash32, Nonce, Side, Symbol};
    use primitives::message::settlement::SettlementOutcome;
    use primitives::order::{Order, OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use primitives::signature::Signature;
    use primitives::time_in_force::TimeInForce;
    use primitives::value::{Price, Quantity, TimestampMs};

    fn order(user: u8, nonce: u64) -> Order {
        Order::new(
            OrderHot {
                user: Address([user; 20]),
                nonce: Nonce(nonce),
                price: Price(100),
                quantity: Quantity(10),
                time_in_force: TimeInForce::Gtc,
                side: Side::Buy,
            },
            OrderCold::new(
                OrderColdCommon::new(
                    Hash32([0; 32]),
                    Symbol([0; 32]),
                    Signature::default(),
                    TimestampMs(0),
                ),
                OrderKind::Standard,
            ),
        )
    }

    fn trade(user: u8, nonce: u64) -> Trade {
        Trade::new(order(user, nonce), Quantity(9), order(user, nonce + 1), Price(100), Quantity(1))
    }

    #[test]
    fn test_encode_decode_trade_batch_roundtrip() {
        let trades = vec![trade(1, 1), trade(2, 2)];
        let mut bytes = Vec::new();
        encode_trade_batch(&trades, &mut bytes).unwrap();
        assert_eq!(decode_record(&bytes).unwrap(), SettlementRecord::TradeBatch { trades });
    }

    #[test]
    fn test_unknown_tag_is_rejected() {
        assert!(decode_record(&[9, 1, 2, 3]).is_err());
        assert!(decode_record(&[]).is_err());
    }

    #[test]
    fn test_reconstruct_rebuilds_the_pending_trade_stream() {
        let records = vec![
            (1, SettlementRecord::TradeBatch { trades: vec![trade(1, 1), trade(2, 1)] }),
            (2, SettlementRecord::TradeBatch { trades: vec![trade(3, 1)] }),
        ];
        let rebuilt = reconstruct(records);
        assert_eq!(rebuilt.pending_trades.len(), 3);
        assert!(rebuilt.batch_states.is_empty());
        assert!(rebuilt.outcomes.is_empty());
    }

    #[test]
    fn test_reconstruct_consumes_received_trades_in_order() {
        let records = vec![
            (1, SettlementRecord::TradeBatch { trades: vec![trade(1, 1), trade(2, 1)] }),
            (
                2,
                SettlementRecord::BatchState {
                    batch_seq: 2,
                    state: BatchState::Received { trades: vec![trade(1, 1)] },
                },
            ),
        ];
        let rebuilt = reconstruct(records);
        // The first pending trade went into the received batch; one stays.
        assert_eq!(rebuilt.pending_trades, vec![trade(2, 1)]);
        assert_eq!(
            rebuilt.batch_states,
            vec![(2, BatchState::Received { trades: vec![trade(1, 1)] })]
        );
    }

    #[test]
    fn test_reconstruct_last_write_wins_and_outcomes() {
        let result = SettlementResult {
            batch_seq: 3,
            symbol: Symbol([0; 32]),
            outcome: SettlementOutcome::Settled,
            tx_hash: Some(Hash32([1; 32])),
            block: Some(7),
            trades: vec![trade(1, 1)],
        };
        let records = vec![
            (
                3,
                SettlementRecord::BatchState {
                    batch_seq: 3,
                    state: BatchState::Received { trades: vec![trade(1, 1)] },
                },
            ),
            (
                4,
                SettlementRecord::BatchState {
                    batch_seq: 3,
                    state: BatchState::Submitting { tx: Hash32([2; 32]) },
                },
            ),
            (5, SettlementRecord::Outcome { batch_seq: 3, result: result.clone() }),
        ];
        let rebuilt = reconstruct(records);
        assert_eq!(rebuilt.batch_states, vec![(3, BatchState::Submitting { tx: Hash32([2; 32]) })]);
        assert_eq!(rebuilt.outcomes, vec![(5, result)]);
        assert!(rebuilt.pending_trades.is_empty());
    }

    #[test]
    fn test_roundtrip_through_the_log() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let seq = SEQ.fetch_add(1, AtomicOrdering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("dex_stl_journal_{}_{}", std::process::id(), seq));
        struct Guard(std::path::PathBuf);
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let _guard = Guard(path.clone());

        let journal = SettlementJournal::open(&path.to_string_lossy(), 4096).unwrap();
        let batch_seq = journal
            .write(&SettlementRecord::BatchState {
                batch_seq: 0, // overwritten by the returned sequence
                state: BatchState::Received { trades: vec![trade(1, 1)] },
            })
            .unwrap();
        assert_eq!(batch_seq, 1);

        // Reopen through a fresh handle (like a restarted process).
        let journal = SettlementJournal::open(&path.to_string_lossy(), 4096).unwrap();
        let records = journal.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0],
            (
                1,
                SettlementRecord::BatchState {
                    batch_seq: 1,
                    state: BatchState::Received { trades: vec![trade(1, 1)] }
                }
            )
        );
    }
}
