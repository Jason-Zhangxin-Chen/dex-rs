//! The settlement batches: the state machine of one on-chain `settleBatch`
//! call.
//!
//! A batch is the crosses of one taker order, assembled by the core thread
//! into a submitter-queue frame. The submitter drives the batch through the
//! states below in memory — there is no journal: the durability lives in
//! the file-mapped queues (a frame is acked only after every batch of it is
//! terminal and published), and the drive loop that executes the transitions
//! lives in [`crate::submitter`].

use primitives::base::{Hash32, Symbol};
use primitives::message::hot_path::Trade;
use primitives::message::settlement::{
    FaultSide, SettlementFailure, SettlementOutcome, SettlementResult,
};
use serde::{Deserialize, Serialize};

/// The state machine of one batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchState {
    /// Popped from the submitter queue. Carries the trades so the batch is
    /// self-contained. The sequence of the frame holding this batch IS the
    /// batch sequence.
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
        /// The block the transaction mined in (0 = unknown).
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
    /// its trades now live in the child batches. Carries the revert data for
    /// the audit trail.
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
    /// == the sequence of the frame holding this batch ==
    /// `SettlementResult.batch_seq`. Assigned by the core thread (the
    /// persistent sequence file), never reused; the split children get
    /// their own.
    pub seq: u64,
    /// The trades, in submission order.
    pub trades: Vec<Trade>,
    /// The current state.
    pub state: BatchState,
    /// Retry bookkeeping (in-memory only).
    pub attempts: u32,
    /// The earliest instant of the next chain attempt.
    pub next_attempt_at: Option<std::time::Instant>,
    /// The operator nonce of the in-flight transaction.
    pub nonce: Option<u64>,
    /// When the current transaction was submitted. Drives the
    /// lost-transaction grace of a `Submitting` batch.
    pub submitted_at: Option<std::time::Instant>,
}

impl Batch {
    /// Builds the terminal outcome of a `Confirmed` / `Reverted` batch.
    pub fn outcome(&self, symbol: Symbol) -> Option<SettlementResult> {
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

/// The exponential backoff of a retry: `base_ms * 2^attempt`, saturating,
/// capped at `max_ms`. `attempt` is zero-based, so the first retry waits
/// `base_ms`.
pub fn backoff_for(attempt: u32, base_ms: u64, max_ms: u64) -> u64 {
    // Saturate rather than wrap: a shift of 64 or more, and a product past
    // `u64::MAX`, both mean "far beyond any cap".
    let factor = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
    base_ms.saturating_mul(factor).min(max_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::address::Address;
    use primitives::base::{Nonce, Side};
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

    fn batch(state: BatchState) -> Batch {
        Batch {
            seq: 7,
            trades: trades(2),
            state,
            attempts: 0,
            next_attempt_at: None,
            nonce: None,
            submitted_at: None,
        }
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
    fn test_outcome_of_a_confirmed_batch_is_settled() {
        let tx = Hash32([9; 32]);
        let symbol = Symbol([7; 32]);
        let result = batch(BatchState::Confirmed { tx, block: 42 }).outcome(symbol).unwrap();
        assert_eq!(result.batch_seq, 7);
        assert_eq!(result.outcome, SettlementOutcome::Settled);
        assert_eq!(result.tx_hash, Some(tx));
        assert_eq!(result.block, Some(42));
        assert_eq!(result.trades, trades(2));
        // A zero block (the unknown-block path) publishes no block.
        let result = batch(BatchState::Confirmed { tx, block: 0 }).outcome(symbol).unwrap();
        assert_eq!(result.block, None);
    }

    #[test]
    fn test_outcome_of_a_reverted_batch() {
        let tx = Hash32([9; 32]);
        let symbol = Symbol([7; 32]);
        let state = BatchState::Reverted {
            tx,
            failed_trade: 1,
            at_fault: FaultSide::Maker,
            reason: SettlementFailure::Protocol(2),
        };
        let result = batch(state).outcome(symbol).unwrap();
        assert_eq!(
            result.outcome,
            SettlementOutcome::Reverted {
                failed_trade: 1,
                at_fault: FaultSide::Maker,
                reason: SettlementFailure::Protocol(2),
            }
        );
        assert_eq!(result.tx_hash, Some(tx));
        assert_eq!(result.block, None);
    }

    #[test]
    fn test_outcome_is_none_for_the_non_terminal_states() {
        let symbol = Symbol([7; 32]);
        assert!(batch(BatchState::Received { trades: trades(2) }).outcome(symbol).is_none());
        assert!(batch(BatchState::Submitting { tx: Hash32([9; 32]) }).outcome(symbol).is_none());
        assert!(
            batch(BatchState::Submitted { tx: Hash32([9; 32]), nonce: 1 })
                .outcome(symbol)
                .is_none()
        );
    }

    #[test]
    fn test_split_state_is_terminal() {
        assert!(
            BatchState::Split { tx: Hash32([9; 32]), code: 1, index: 0, side: 1 }.is_terminal()
        );
        assert!(
            batch(BatchState::Split { tx: Hash32([9; 32]), code: 1, index: 0, side: 1 })
                .is_terminal()
        );
    }
}
