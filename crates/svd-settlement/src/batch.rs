//! The settlement batches: the journaled state machine of the on-chain
//! `settleBatch` calls and the assembly of the drained trades into them.
//!
//! Every transition of a batch is appended to the settlement journal
//! before the submitter acts on it — the journal lags reality, never leads
//! it — so a crash may repeat an already-taken action (a duplicate submit)
//! but never skips one. The assembler and the crash replay live in this
//! module; the drive loop that executes the transitions lives in
//! [`crate::submitter`].

use std::time::Duration;

use primitives::base::{Hash32, Symbol};
use primitives::message::hot_path::Trade;
use primitives::message::settlement::{
    FaultSide, SettlementFailure, SettlementOutcome, SettlementResult,
};
use serde::{Deserialize, Serialize};

use crate::journal::Reconstructed;

/// The journaled state machine of one batch. Every variant except the
/// initial creation is appended as a `BatchState` record by the submitter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchState {
    /// Assembled. Carries the trades so the crash replay is self-contained
    /// (the submitter never joins the `TradeBatch` records against the
    /// batches). The sequence of the journal record holding this state IS
    /// the batch sequence.
    Received {
        /// The trades, in submission order (position == on-chain trade
        /// index).
        trades: Vec<Trade>,
    },
    /// `submit()` returned this transaction; monitoring by its hash. The
    /// transaction may or may not be in the mempool yet (crash window).
    Submitting {
        /// The transaction hash.
        tx: Hash32,
    },
    /// Observed pending in the node's view; the nonce is known.
    Submitted {
        /// The transaction hash.
        tx: Hash32,
        /// The operator nonce the transaction was sent with.
        nonce: u64,
    },
    /// Terminal: mined at or beyond the confirmation depth.
    Confirmed {
        /// The settled transaction hash.
        tx: Hash32,
        /// The block the transaction mined in (0 = unknown, the idempotent
        /// double-submission path).
        block: u64,
    },
    /// Terminal: a singleton poison trade was singled out and classified.
    Reverted {
        /// The reverted transaction hash.
        tx: Hash32,
        /// The index of the failing trade in the trades.
        failed_trade: usize,
        /// Which side of the failing cross is at fault.
        at_fault: FaultSide,
        /// The decoded failure.
        reason: SettlementFailure,
    },
    /// Terminal for this batch: it reverted on-chain, was binary-split, and
    /// its trades now live in child batches (their `Received` records were
    /// journaled BEFORE this record). Carries the revert data for the
    /// audit trail.
    Split {
        /// The reverted transaction hash.
        tx: Hash32,
        /// The decoded revert code.
        code: u8,
        /// The decoded revert index.
        index: usize,
        /// The decoded revert side.
        side: u8,
    },
}

impl BatchState {
    /// Whether the state is terminal: no further transition of this batch
    /// exists.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            BatchState::Confirmed { .. } | BatchState::Reverted { .. } | BatchState::Split { .. }
        )
    }

    /// The transaction hash the state carries, if any.
    pub fn tx(&self) -> Option<Hash32> {
        match self {
            BatchState::Submitting { tx }
            | BatchState::Submitted { tx, .. }
            | BatchState::Confirmed { tx, .. }
            | BatchState::Reverted { tx, .. }
            | BatchState::Split { tx, .. } => Some(*tx),
            BatchState::Received { .. } => None,
        }
    }
}

/// One settlement batch: the trades that travel together in one
/// `settleBatch` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// == the seq of this batch's own `Received` journal record ==
    /// `SettlementResult.batch_seq`. Assigned by the journal at creation,
    /// never reused.
    pub seq: u64,
    /// The trades, in submission order.
    pub trades: Vec<Trade>,
    /// The current state; last-write-wins on the replay.
    pub state: BatchState,
    /// Retry bookkeeping (NOT journaled — recomputed on the replay).
    pub attempts: u32,
    /// The earliest instant of the next chain attempt.
    pub next_attempt_at: Option<std::time::Instant>,
    /// The operator nonce of the in-flight transaction. In-memory only:
    /// a replayed `Submitting` batch lacks it until a re-submit assigns a
    /// fresh one.
    pub nonce: Option<u64>,
    /// When the current transaction was submitted. In-memory only: drives
    /// the lost-transaction grace of a `Submitting` batch.
    pub submitted_at: Option<std::time::Instant>,
}

impl Batch {
    /// Builds the terminal outcome of a `Confirmed` / `Reverted` batch.
    /// Used both live (before journaling the `Outcome` record) and by the
    /// replay for a terminal batch whose outcome record is missing (crash
    /// between the two writes).
    pub fn outcome(&self, symbol: primitives::base::Symbol) -> Option<SettlementResult> {
        match &self.state {
            BatchState::Confirmed { tx, block } => Some(SettlementResult {
                batch_seq: self.seq,
                symbol,
                outcome: SettlementOutcome::Settled,
                tx_hash: Some(*tx),
                block: (*block != 0).then_some(*block),
                trades: self.trades.clone(),
            }),
            BatchState::Reverted { tx, failed_trade, at_fault, reason } => Some(SettlementResult {
                batch_seq: self.seq,
                symbol,
                outcome: SettlementOutcome::Reverted {
                    failed_trade: *failed_trade,
                    at_fault: *at_fault,
                    reason: *reason,
                },
                tx_hash: Some(*tx),
                block: None,
                trades: self.trades.clone(),
            }),
            _ => None,
        }
    }

    /// Whether the batch is terminal.
    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
}

/// Pure aggregation of the drained trades into the settlement batches. The
/// submitter drives it: a full batch (max_trades trades) is emitted
/// immediately, a partial batch is held until the flush window elapses
/// since its first trade.
#[derive(Debug, Clone)]
pub struct BatchAssembler {
    /// The bound of one batch.
    max_trades: usize,
    /// The flush window of a partial batch.
    window: Duration,
    /// The current partial batch.
    current: Vec<Trade>,
    /// When the current partial batch received its first trade.
    first_arrival: Option<std::time::Instant>,
}

impl BatchAssembler {
    /// Creates an assembler with the given bound and flush window.
    pub fn new(max_trades: usize, window: Duration) -> Self {
        Self { max_trades: max_trades.max(1), window, current: Vec::new(), first_arrival: None }
    }

    /// Consumes one drained trade group; returns the full batches to
    /// submit, in order.
    pub fn push(&mut self, trades: &[Trade], now: std::time::Instant) -> Vec<Vec<Trade>> {
        let mut ready = Vec::new();
        let mut rest = trades;
        while !rest.is_empty() {
            let missing = self.max_trades - self.current.len();
            if missing == 0 {
                ready.push(std::mem::take(&mut self.current));
                self.first_arrival = None;
                continue;
            }
            let take = missing.min(rest.len());
            self.current.extend_from_slice(&rest[..take]);
            self.first_arrival.get_or_insert(now);
            rest = &rest[take..];
        }
        if self.current.len() == self.max_trades {
            ready.push(std::mem::take(&mut self.current));
            self.first_arrival = None;
        }
        ready
    }

    /// Emits the partial batch once its window elapsed. An empty batch
    /// returns `None`.
    pub fn flush(&mut self, now: std::time::Instant) -> Option<Vec<Trade>> {
        let due = self.first_arrival.is_some_and(|first| now.duration_since(first) >= self.window);
        if due && !self.current.is_empty() {
            self.first_arrival = None;
            return Some(std::mem::take(&mut self.current));
        }
        None
    }

    /// Whether no partial batch is held.
    pub fn is_empty(&self) -> bool {
        self.current.is_empty()
    }
}

/// The state a restart resumes from: the batches in flight, the trades not
/// yet assembled, and the outcomes to (re-)publish.
#[derive(Debug, Default)]
pub struct ReplayState {
    /// The trades journaled by the core thread but not yet consumed into a
    /// `Received` batch, in submission order. The submitter re-assembles
    /// them into batches.
    pub pending_trades: Vec<Trade>,
    /// The batches in flight (`Received` / `Submitting` / `Submitted`),
    /// ascending by sequence. Their in-memory bookkeeping (`attempts`,
    /// `next_attempt_at`, `nonce`, `submitted_at`) is reset — it is
    /// deliberately not journaled.
    pub pending: Vec<Batch>,
    /// The outcomes to (re-)publish: every journaled `Outcome` record, in
    /// journal order, followed by a synthesized one for every terminal batch
    /// whose `Outcome` record is missing (the crash window between the
    /// terminal state write and the outcome write).
    pub outcomes: Vec<SettlementResult>,
}

/// Rebuilds the resumable state from the journal replay:
///
/// - one [`Batch`] per batch sequence, its trades taken from the `Received`
///   state, the last-write-wins state as the current one;
/// - a `Split` batch is dropped — its children are separate batches whose
///   `Received` records precede it;
/// - the in-flight batches (a non-terminal state) enter `pending`, ascending
///   by sequence, with the retry bookkeeping reset;
/// - a terminal batch (`Confirmed` / `Reverted`) with no `Outcome` record
///   gets its result synthesized by [`Batch::outcome`], so a crash between
///   the two writes still publishes;
/// - every `Outcome` record is republished, in journal order, before the
///   synthesized ones;
/// - the journaled-but-unassembled trades are handed back for re-assembly.
///
/// # The trades of a resumed batch
///
/// The rebuild keeps the last-write-wins state per batch (see
/// [`crate::journal::reconstruct`]) and the trades travel in the `Received`
/// state only: a batch whose last journaled state is any later one comes
/// back with an empty [`Batch::trades`]. Such a batch still resumes its
/// drive — a `Submitted` transaction is re-monitored by its hash, a
/// `Submitting` one is re-submitted. The trades travel in the `Received`
/// state and survive the later transitions through the rebuilt
/// [`Reconstructed::batch_trades`] map, so a re-submission always has its
/// trades and a synthesized outcome publishes with them.
pub fn replay_batches(rebuilt: Reconstructed, symbol: Symbol) -> ReplayState {
    let Reconstructed { pending_trades, batch_states, batch_trades, outcomes } = rebuilt;
    // The `u64` of a rebuilt outcome is the sequence of its journal record,
    // not the batch sequence: the batch is identified by the result's own
    // `batch_seq` (the sequence of the batch's `Received` record).
    let recorded: std::collections::BTreeSet<u64> =
        outcomes.iter().map(|(_, result)| result.batch_seq).collect();
    let mut pending = Vec::new();
    let mut synthesized = Vec::new();
    for (batch_seq, batch_state) in batch_states {
        // The trades travel in the `Received` state; the later states of the
        // same batch overwrite it, so the captured trades carry the batch
        // forward.
        let trades = match &batch_state {
            BatchState::Received { trades } => trades.clone(),
            _ => batch_trades.get(&batch_seq).cloned().unwrap_or_default(),
        };
        let batch = Batch {
            seq: batch_seq,
            trades,
            state: batch_state,
            attempts: 0,
            next_attempt_at: None,
            // Not journaled: a resumed `Submitting` batch re-submits under a
            // fresh nonce and the lost-transaction grace restarts.
            nonce: None,
            submitted_at: None,
        };
        match &batch.state {
            // Terminal for the batch itself: its children are the live
            // batches, the parent has nothing left to resume.
            BatchState::Split { .. } => {}
            // Terminal: a journaled outcome is the published truth; the
            // crash between the terminal state and the outcome leaves the
            // latter to synthesize.
            BatchState::Confirmed { .. } | BatchState::Reverted { .. } => {
                if !recorded.contains(&batch_seq)
                    && let Some(result) = batch.outcome(symbol)
                {
                    synthesized.push(result);
                }
            }
            // In flight: resume it.
            BatchState::Received { .. }
            | BatchState::Submitting { .. }
            | BatchState::Submitted { .. } => pending.push(batch),
        }
    }
    let mut outcomes: Vec<SettlementResult> =
        outcomes.into_iter().map(|(_, result)| result).collect();
    outcomes.extend(synthesized);
    ReplayState { pending_trades, pending, outcomes }
}

/// The exponential backoff of a retry: `base_ms * 2^attempt`, saturating,
/// capped at `max_ms`. `attempt` is zero-based, so the first retry waits
/// `base_ms`. Pure and deterministic — a replayed batch lands on the same
/// schedule.
pub fn backoff_for(attempt: u32, base_ms: u64, max_ms: u64) -> u64 {
    // Saturate rather than wrap: a shift of 64 or more, and a product past
    // `u64::MAX`, both mean "far beyond any cap".
    let factor = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
    base_ms.saturating_mul(factor).min(max_ms)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::{Duration, Instant};

    use primitives::address::Address;
    use primitives::base::{Nonce, Side};
    use primitives::message::settlement::SettlementOutcome;
    use primitives::order::{Order, OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use primitives::signature::Signature;
    use primitives::time_in_force::TimeInForce;
    use primitives::value::{Price, Quantity, TimestampMs};

    use super::*;
    use crate::journal::{SettlementJournal, SettlementRecord, reconstruct};

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
        Trade::new(
            order(user, nonce),
            Quantity(9),
            order(user.wrapping_add(1), nonce),
            Price(100),
            Quantity(1),
        )
    }

    /// `n` distinct trades, taker user 1, nonces `0..n`.
    fn trades(n: usize) -> Vec<Trade> {
        (0..n as u64).map(|nonce| trade(1, nonce)).collect()
    }

    /// The size of the temporary journal files.
    const JOURNAL_SIZE: u64 = 64 * 1024;

    static SEQ: AtomicUsize = AtomicUsize::new(0);

    /// A temporary settlement journal file, removed on drop.
    struct TempJournal {
        path: std::path::PathBuf,
        journal: SettlementJournal,
    }

    impl TempJournal {
        fn new() -> Self {
            let seq = SEQ.fetch_add(1, AtomicOrdering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("dex_stl_batch_{}_{}", std::process::id(), seq));
            let journal = SettlementJournal::open(&path.to_string_lossy(), JOURNAL_SIZE).unwrap();
            Self { path, journal }
        }

        /// Appends one record, returning its sequence.
        fn write(&self, record: &SettlementRecord) -> u64 {
            self.journal.write(record).unwrap()
        }

        /// Restarts over the same file: a fresh handle, replayed and
        /// reconstructed into the resumable state.
        fn restart(&self, symbol: Symbol) -> ReplayState {
            let journal =
                SettlementJournal::open(&self.path.to_string_lossy(), JOURNAL_SIZE).unwrap();
            let records = journal.replay().unwrap();
            replay_batches(reconstruct(records), symbol)
        }
    }

    impl Drop for TempJournal {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    #[test]
    fn test_assembler_emits_a_full_batch_immediately() {
        let window = Duration::from_millis(500);
        let now = Instant::now();

        let mut assembler = BatchAssembler::new(4, window);
        let full = trades(4);
        assert_eq!(assembler.push(&full, now), vec![full]);
        assert!(assembler.is_empty());
        // Nothing is left to flush after the emission.
        assert!(assembler.flush(now).is_none());
        assert!(assembler.flush(now + window).is_none());
    }

    #[test]
    fn test_assembler_emits_the_partial_batch_on_the_window() {
        let window = Duration::from_millis(500);
        let mut assembler = BatchAssembler::new(8, window);
        let first = Instant::now();
        assert!(assembler.push(&trades(2), first).is_empty());
        // The window runs from the first trade of the partial batch.
        assert!(assembler.flush(first + window - Duration::from_millis(1)).is_none());
        assert_eq!(assembler.flush(first + window), Some(trades(2)));
        assert!(assembler.is_empty());

        // The next trade restarts the window.
        let second = first + Duration::from_millis(600);
        assert!(assembler.push(&trades(1), second).is_empty());
        assert!(assembler.flush(second + window - Duration::from_millis(1)).is_none());
        assert_eq!(assembler.flush(second + window), Some(trades(1)));
    }

    #[test]
    fn test_assembler_exact_max_boundary() {
        let window = Duration::from_millis(500);
        let now = Instant::now();

        // `max_trades - 1` is held, the trade that fills the batch emits it
        // exactly at the bound.
        let mut assembler = BatchAssembler::new(4, window);
        let full = trades(4);
        assert!(assembler.push(&full[..3], now).is_empty());
        assert_eq!(assembler.push(&full[3..], now), vec![full.clone()]);
        assert!(assembler.is_empty());

        // A group larger than the bound emits one full batch immediately and
        // holds the overflow for the window.
        let mut assembler = BatchAssembler::new(4, window);
        let six = trades(6);
        assert_eq!(assembler.push(&six, now), vec![six[..4].to_vec()]);
        assert!(!assembler.is_empty());
        assert!(assembler.flush(now).is_none());
        assert_eq!(assembler.flush(now + window), Some(six[4..].to_vec()));
        assert!(assembler.is_empty());
    }

    #[test]
    fn test_backoff_for_grows_exponentially_and_caps() {
        assert_eq!(backoff_for(0, 1_000, 60_000), 1_000);
        assert_eq!(backoff_for(1, 1_000, 60_000), 2_000);
        assert_eq!(backoff_for(2, 1_000, 60_000), 4_000);
        assert_eq!(backoff_for(5, 1_000, 60_000), 32_000);
        assert_eq!(backoff_for(6, 1_000, 60_000), 60_000);
        assert_eq!(backoff_for(30, 1_000, 60_000), 60_000);
        // The cap below the base wins.
        assert_eq!(backoff_for(0, 10_000, 5_000), 5_000);
        // A zero base stays zero.
        assert_eq!(backoff_for(9, 0, 60_000), 0);
    }

    #[test]
    fn test_backoff_for_saturates_instead_of_wrapping() {
        // A shift past the width and a product past `u64::MAX` both saturate:
        // neither may wrap back to a small (or zero) wait, which would turn
        // the retry into a busy loop.
        assert_eq!(backoff_for(64, 1, 60_000), 60_000);
        assert_eq!(backoff_for(u32::MAX, 100, 10_000), 10_000);
        assert_eq!(backoff_for(54, 1_024, 60_000), 60_000);
        assert_eq!(backoff_for(63, u64::MAX, u64::MAX), u64::MAX);
    }

    #[test]
    fn test_replay_of_a_settled_batch() {
        let symbol = Symbol([7; 32]);
        let journal = TempJournal::new();
        let batch_trades = trades(2);
        let tx = Hash32([9; 32]);
        assert_eq!(
            journal.write(&SettlementRecord::TradeBatch { trades: batch_trades.clone() }),
            1
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Received { trades: batch_trades.clone() },
            }),
            2
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Submitting { tx },
            }),
            3
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Submitted { tx, nonce: 5 },
            }),
            4
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Confirmed { tx: Hash32([1; 32]), block: 44 },
            }),
            5
        );
        let result = SettlementResult {
            batch_seq: 2,
            symbol,
            outcome: SettlementOutcome::Settled,
            tx_hash: Some(tx),
            block: Some(44),
            trades: batch_trades,
        };
        assert_eq!(
            journal.write(&SettlementRecord::Outcome { batch_seq: 2, result: result.clone() }),
            6
        );

        let state = journal.restart(symbol);
        assert!(state.pending.is_empty());
        assert!(state.pending_trades.is_empty());
        // The journaled outcome, not a synthesized second copy of it.
        assert_eq!(state.outcomes, vec![result]);
    }

    #[test]
    fn test_replay_of_a_received_batch_keeps_the_trades() {
        let symbol = Symbol([7; 32]);
        let journal = TempJournal::new();
        let batch_trades = trades(2);
        assert_eq!(
            journal.write(&SettlementRecord::TradeBatch { trades: batch_trades.clone() }),
            1
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Received { trades: batch_trades.clone() },
            }),
            2
        );

        let state = journal.restart(symbol);
        assert_eq!(state.pending.len(), 1);
        let batch = &state.pending[0];
        assert_eq!(batch.seq, 2);
        // The `Received` state carries the trades, so a batch that never
        // left `Received` resumes whole.
        assert_eq!(batch.trades, batch_trades);
        assert_eq!(batch.state, BatchState::Received { trades: batch_trades });
        // The retry bookkeeping is reset (it is not journaled).
        assert_eq!(batch.attempts, 0);
        assert!(batch.next_attempt_at.is_none());
        // The `Received` record consumed the journaled trade stream.
        assert!(state.pending_trades.is_empty());
        assert!(state.outcomes.is_empty());
    }

    #[test]
    fn test_replay_of_a_submitting_batch() {
        let symbol = Symbol([7; 32]);
        let journal = TempJournal::new();
        let tx = Hash32([3; 32]);
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 1,
                state: BatchState::Submitting { tx },
            }),
            1
        );

        let state = journal.restart(symbol);
        assert_eq!(state.pending.len(), 1);
        assert_eq!(state.pending[0].seq, 1);
        assert_eq!(state.pending[0].state, BatchState::Submitting { tx });
        assert_eq!(state.pending[0].attempts, 0);
        assert!(state.pending[0].next_attempt_at.is_none());
        assert!(state.outcomes.is_empty());
    }

    #[test]
    fn test_replay_synthesizes_a_missing_settled_outcome() {
        let symbol = Symbol([7; 32]);
        let journal = TempJournal::new();
        let batch_trades = trades(2);
        let tx = Hash32([9; 32]);
        assert_eq!(
            journal.write(&SettlementRecord::TradeBatch { trades: batch_trades.clone() }),
            1
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Received { trades: batch_trades },
            }),
            2
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Submitting { tx },
            }),
            3
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Submitted { tx, nonce: 5 },
            }),
            4
        );
        // The crash hit between the terminal state and the outcome record.
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Confirmed { tx: Hash32([1; 32]), block: 44 },
            }),
            5
        );

        let state = journal.restart(symbol);
        assert!(state.pending.is_empty());
        assert_eq!(state.outcomes.len(), 1);
        let result = &state.outcomes[0];
        assert_eq!(result.batch_seq, 2);
        assert_eq!(result.symbol, symbol);
        assert_eq!(result.outcome, SettlementOutcome::Settled);
        assert_eq!(result.block, Some(44));
    }

    #[test]
    fn test_replay_synthesizes_a_missing_reverted_outcome() {
        let symbol = Symbol([7; 32]);
        let journal = TempJournal::new();
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 1,
                state: BatchState::Received { trades: trades(1) },
            }),
            1
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 1,
                state: BatchState::Reverted {
                    tx: Hash32([1; 32]),
                    failed_trade: 0,
                    at_fault: FaultSide::Taker,
                    reason: SettlementFailure::Protocol(2),
                },
            }),
            2
        );

        let state = journal.restart(symbol);
        assert!(state.pending.is_empty());
        assert_eq!(
            state.outcomes[0].outcome,
            SettlementOutcome::Reverted {
                failed_trade: 0,
                at_fault: FaultSide::Taker,
                reason: SettlementFailure::Protocol(2),
            }
        );
    }

    #[test]
    fn test_replay_orders_the_recorded_outcomes_before_the_synthesized_ones() {
        let symbol = Symbol([7; 32]);
        let journal = TempJournal::new();
        // Batch 1 settled and published.
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 1,
                state: BatchState::Received { trades: trades(1) },
            }),
            1
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 1,
                state: BatchState::Confirmed { tx: Hash32([1; 32]), block: 10 },
            }),
            2
        );
        let recorded = SettlementResult {
            batch_seq: 1,
            symbol,
            outcome: SettlementOutcome::Settled,
            tx_hash: None,
            block: Some(10),
            trades: trades(1),
        };
        assert_eq!(
            journal.write(&SettlementRecord::Outcome { batch_seq: 1, result: recorded.clone() }),
            3
        );
        // Batch 4 confirmed, but the crash hit before its outcome record.
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 4,
                state: BatchState::Received { trades: trades(1) },
            }),
            4
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 4,
                state: BatchState::Confirmed { tx: Hash32([1; 32]), block: 11 },
            }),
            5
        );

        let state = journal.restart(symbol);
        assert_eq!(state.outcomes.len(), 2);
        assert_eq!(state.outcomes[0], recorded);
        assert_eq!(state.outcomes[1].batch_seq, 4);
        assert_eq!(state.outcomes[1].outcome, SettlementOutcome::Settled);
        assert_eq!(state.outcomes[1].block, Some(11));
    }

    #[test]
    fn test_replay_drops_the_split_parent_and_keeps_its_children() {
        let symbol = Symbol([7; 32]);
        let journal = TempJournal::new();
        let all = trades(4);
        let tx = Hash32([4; 32]);
        assert_eq!(journal.write(&SettlementRecord::TradeBatch { trades: all.clone() }), 1);
        // The parent received the four trades, then the split journaled the
        // children (in submission order) and closed the parent.
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Received { trades: all.clone() },
            }),
            2
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 3,
                state: BatchState::Received { trades: all[..2].to_vec() },
            }),
            3
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 4,
                state: BatchState::Received { trades: all[2..].to_vec() },
            }),
            4
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Split { tx, code: 2, index: 1, side: 1 },
            }),
            5
        );

        let state = journal.restart(symbol);
        // Ascending by sequence, the parent absent.
        assert_eq!(state.pending.len(), 2);
        assert_eq!(state.pending.iter().map(|batch| batch.seq).collect::<Vec<_>>(), vec![3, 4]);
        assert_eq!(state.pending[0].trades, all[..2]);
        assert_eq!(state.pending[1].trades, all[2..]);
        assert!(state.outcomes.is_empty());
        assert!(state.pending_trades.is_empty());
    }

    #[test]
    fn test_replay_of_a_crash_before_the_split_record_resumes_the_parent() {
        let symbol = Symbol([7; 32]);
        let journal = TempJournal::new();
        let all = trades(4);
        let tx = Hash32([4; 32]);
        assert_eq!(journal.write(&SettlementRecord::TradeBatch { trades: all.clone() }), 1);
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Received { trades: all.clone() },
            }),
            2
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Submitting { tx },
            }),
            3
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Submitted { tx, nonce: 5 },
            }),
            4
        );
        // The children are journaled, then the crash hits before the parent's
        // `Split` record.
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 5,
                state: BatchState::Received { trades: all[..2].to_vec() },
            }),
            5
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 6,
                state: BatchState::Received { trades: all[2..].to_vec() },
            }),
            6
        );

        let state = journal.restart(symbol);
        // The parent re-enters the pending set with its pre-split state: it
        // is re-submitted as a whole, and the split repeats harmlessly
        // because the children settle as a subset.
        assert_eq!(state.pending.iter().map(|batch| batch.seq).collect::<Vec<_>>(), vec![2, 5, 6]);
        assert_eq!(state.pending[0].state, BatchState::Submitted { tx, nonce: 5 });
        assert!(state.outcomes.is_empty());
    }

    #[test]
    fn test_replay_of_a_batch_past_received_resumes_with_its_trades() {
        // A batch that progressed past `Received` comes back with the
        // trades captured from its `Received` record, so a re-submission
        // after a lost transaction has its calldata.
        let symbol = Symbol([7; 32]);
        let journal = TempJournal::new();
        let batch_trades = trades(2);
        let tx = Hash32([3; 32]);
        assert_eq!(
            journal.write(&SettlementRecord::TradeBatch { trades: batch_trades.clone() }),
            1
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Received { trades: batch_trades.clone() },
            }),
            2
        );
        assert_eq!(
            journal.write(&SettlementRecord::BatchState {
                batch_seq: 2,
                state: BatchState::Submitting { tx },
            }),
            3
        );

        let state = journal.restart(symbol);
        assert_eq!(state.pending.len(), 1);
        assert_eq!(state.pending[0].state, BatchState::Submitting { tx });
        assert_eq!(state.pending[0].trades, batch_trades);
        assert!(state.pending_trades.is_empty());
        assert!(state.outcomes.is_empty());
    }
}
