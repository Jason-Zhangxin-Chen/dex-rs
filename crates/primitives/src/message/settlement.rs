//! Settlement messages between the [SVD_Settlement], the [SVD_OMS_Master]
//! and the storage: the reversals of the failed settlements and the
//! settlement results the downstream services consume.

use crate::address::Address;
use crate::base::{Hash32, Nonce, Symbol};
use crate::message::hot_path::Trade;
use crate::message::side_path::CancelReason;
use crate::order::Order;
use crate::value::Quantity;
use serde::{Deserialize, Serialize};

/// Messages sent from [SVD_Settlement] to [SVD_OMS_Master] on the reversal
/// queue. The master drains the queue on its core loop, so the reversals
/// and the ingress requests are applied by the single owner of the book.
// The size gap between the restore variant and the cancel variants is
// deliberate: the message is a fixed-size `Copy` value pre-allocated in the
// share memory queue, so the order payload stays inline (no boxing).
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementMsg {
    /// Rolls one failed trade back (the batch rollback path — the per-trade
    /// innocent-side restores go through the pre-trade pipeline instead):
    /// re-inserts `quantity` of the order into the book (the failed cross).
    /// The [SVD_OMS_Master] merges the quantity into the resting order when
    /// `(user, nonce)` is still in the book, and re-inserts the order at the
    /// tail of its price level when it is gone.
    RestoreOrder {
        /// The order of the failed cross.
        order: Order,
        /// The crossed quantity of the failed trade.
        quantity: Quantity,
    },
    /// Removes one order of the book: a deterministic settlement failure of
    /// that order (forged signature, expired, bad price).
    CancelOrder {
        /// The user of the order.
        user: Address,
        /// The nonce of the order.
        nonce: Nonce,
        /// Why the order is removed.
        reason: CancelReason,
    },
    /// Removes every resting order of an account: the account's margin is
    /// exhausted on-chain and it must stop trading.
    MassCancelByUser {
        /// The account to remove.
        user: Address,
        /// Why the account's orders are removed.
        reason: CancelReason,
    },
}

/// The result of one settlement batch, published by [SVD_Settlement] to the
/// storage channel `svd:stl:{symbol.hex()}:settlements`. The [SVD_Pretrade]
/// consumes the outcome twice: it re-injects the innocent side's crossed
/// quantity of a reverted trade into the pre-trade pipeline, and it blocks
/// the at-fault account when the failure is an insufficient margin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementResult {
    /// The batch sequence of the settlement journal — the sequence of the
    /// [SVD_Settlement]'s own at-most-once publication; the consumers do not
    /// bookkeep it: applying a result is idempotent (setting a block twice
    /// is a no-op).
    pub batch_seq: u64,
    /// The symbol of the batch.
    pub symbol: Symbol,
    /// The outcome of the batch.
    pub outcome: SettlementOutcome,
    /// The transaction hash when one was submitted.
    pub tx_hash: Option<Hash32>,
    /// The block number when the transaction was confirmed.
    pub block: Option<u64>,
    /// The trades of the batch — the [SVD_Pretrade] derives the innocent
    /// side and the at-fault account of a failed trade from them.
    pub trades: Vec<Trade>,
}

/// The outcome of one settlement batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementOutcome {
    /// The batch settled on-chain.
    Settled,
    /// One failing trade was singled out of the batch: the at-fault side's
    /// order is removed from the book and the innocent side's crossed
    /// quantity is re-injected into the pre-trade pipeline by the
    /// [SVD_Pretrade].
    Reverted {
        /// The index of the failing trade in the trades.
        failed_trade: usize,
        /// Which side of the failing cross is at fault.
        at_fault: FaultSide,
        /// The decoded failure.
        reason: SettlementFailure,
    },
    /// The batch was rolled back: the trades' orders were re-inserted into
    /// the book by the reversal messages.
    RolledBack {
        /// Why the batch was rolled back.
        reason: SettlementFailure,
    },
}

/// Which side of a failing cross is at fault. The revert data of the
/// protocol names the side (see doc/settlement-protocol.md); the settlement
/// derives it for the off-chain failure reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FaultSide {
    /// The taker's order caused the revert — the maker is innocent.
    Taker,
    /// The maker's order caused the revert — the taker is innocent.
    Maker,
}

/// The failure a settlement outcome carries: the decoded `SettlementError`
/// code of doc/settlement-protocol.md, or an off-chain cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementFailure {
    /// The decoded on-chain `SettlementError` code.
    Protocol(u8),
    /// The retry deadline expired before the batch settled.
    DeadlineExceeded,
    /// The transaction failed for an off-chain reason (transport, gas,
    /// nonce) and was not resubmitted in time.
    Submission,
    /// The revert could not be classified.
    Unclassified,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::Side;
    use crate::message::side_path::CancelReason;
    use crate::order::{OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use crate::signature::Signature;
    use crate::time_in_force::TimeInForce;
    use crate::value::{Price, TimestampMs};
    use rmp_serde::{from_slice, to_vec};

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
    fn test_settlement_msg_roundtrip() {
        let msg = SettlementMsg::CancelOrder {
            user: Address([1; 20]),
            nonce: Nonce(3),
            reason: CancelReason::SettlementFailed,
        };
        let bytes = to_vec(&msg).unwrap();
        let restored: SettlementMsg = from_slice(&bytes).unwrap();
        assert_eq!(msg, restored);

        let msg = SettlementMsg::MassCancelByUser {
            user: Address([2; 20]),
            reason: CancelReason::SettlementFailed,
        };
        let bytes = to_vec(&msg).unwrap();
        let restored: SettlementMsg = from_slice(&bytes).unwrap();
        assert_eq!(msg, restored);
    }

    #[test]
    fn test_settlement_result_roundtrip() {
        let result = SettlementResult {
            batch_seq: 7,
            symbol: Symbol([1; 32]),
            outcome: SettlementOutcome::Settled,
            tx_hash: Some(Hash32([2; 32])),
            block: Some(99),
            trades: vec![trade(1, 1), trade(2, 2)],
        };
        let bytes = to_vec(&result).unwrap();
        let restored: SettlementResult = from_slice(&bytes).unwrap();
        assert_eq!(result, restored);
    }

    #[test]
    fn test_settlement_result_reverted_roundtrip() {
        let result = SettlementResult {
            batch_seq: 8,
            symbol: Symbol([1; 32]),
            outcome: SettlementOutcome::Reverted {
                failed_trade: 1,
                at_fault: FaultSide::Taker,
                reason: SettlementFailure::Protocol(2),
            },
            tx_hash: Some(Hash32([2; 32])),
            block: Some(100),
            trades: vec![trade(1, 1), trade(2, 2)],
        };
        let bytes = to_vec(&result).unwrap();
        let restored: SettlementResult = from_slice(&bytes).unwrap();
        assert_eq!(result, restored);
    }

    #[test]
    fn test_settlement_failure_roundtrip() {
        for failure in [
            SettlementFailure::Protocol(1),
            SettlementFailure::Protocol(2),
            SettlementFailure::DeadlineExceeded,
            SettlementFailure::Submission,
            SettlementFailure::Unclassified,
        ] {
            let bytes = to_vec(&failure).unwrap();
            let restored: SettlementFailure = from_slice(&bytes).unwrap();
            assert_eq!(failure, restored);
        }
    }
}
