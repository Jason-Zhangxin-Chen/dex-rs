//! The settlement results the [SVD_Settlement] publishes to the storage and
//! the downstream services consume: the [SVD_Pretrade] restores the innocent
//! side of a failed trade and removes the at-fault side's orders, both
//! through the pre-trade pipeline.

use crate::base::{Hash32, Symbol};
use crate::message::hot_path::Trade;
use serde::{Deserialize, Serialize};

/// The result of one settlement batch, published by [SVD_Settlement] to the
/// storage channel `svd:stl:{symbol.hex()}:settlements`. The [SVD_Pretrade]
/// consumes the outcome: it re-injects the innocent side's crossed quantity
/// of a reverted trade into the pre-trade pipeline, removes the at-fault
/// side's orders from the book through the same pipeline, and blocks the
/// at-fault account when the failure is an insufficient margin.
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

/// The failure a reverted settlement outcome carries: the decoded
/// `SettlementError` code of doc/settlement-protocol.md, or an unclassifiable
/// cause. A transient failure (a paused protocol, a down chain) never
/// publishes an outcome — the [SVD_Settlement] retries it until it resolves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementFailure {
    /// The decoded on-chain `SettlementError` code.
    Protocol(u8),
    /// The revert could not be classified.
    Unclassified,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address::Address;
    use crate::base::{Hash32, Nonce, Side, Symbol};
    use crate::order::{Order, OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use crate::signature::Signature;
    use crate::time_in_force::TimeInForce;
    use crate::value::{Price, Quantity, TimestampMs};
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
            SettlementFailure::Unclassified,
        ] {
            let bytes = to_vec(&failure).unwrap();
            let restored: SettlementFailure = from_slice(&bytes).unwrap();
            assert_eq!(failure, restored);
        }
    }
}
