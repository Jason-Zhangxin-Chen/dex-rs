//! The failure classification of the decoded settlement reverts and the
//! binary-split isolation of the failing trades. Pure logic, no I/O, no
//! logging — the caller owns the alarm policy (an `Unclassified` retry is
//! the pageable state, a `Paused` retry is routine).
//!
//! A reverted `settleBatch` call carries the `SettlementError(code, index,
//! side)` revert data of doc/settlement-protocol.md. [`classify`] decodes it
//! into the action the submitter takes; when the action is [`Action::Revert`],
//! [`plan_isolation`] binary-splits the batch until the failing trade stands
//! alone, so the innocent trades settle and only the poison trade is
//! published as reverted.

use primitives::message::hot_path::Trade;
use primitives::message::settlement::{FaultSide, SettlementFailure};

/// The action the submitter takes on a reverted settlement batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// The failure is deterministic and names one failing trade: isolate it
    /// (see [`plan_isolation`]) and publish the `Reverted` outcome — the
    /// [SVD_Pretrade] removes the at-fault order and restores the innocent
    /// side's crossed quantity. Never retry.
    Revert {
        /// The index of the failing trade in the batch the action was
        /// classified for.
        failed_trade: usize,
        /// Which side of the failing cross is at fault.
        at_fault: FaultSide,
        /// The decoded failure to publish.
        reason: SettlementFailure,
    },
    /// The trade was already settled on-chain (a duplicated submission): the
    /// batch is final and its outcome is `Settled`.
    TreatAsSettled,
    /// The failure is transient or cannot be explained: the batch stays
    /// pending and is retried with backoff, and no outcome is published
    /// while it is pending.
    Retry {
        /// Why the batch is retried rather than resolved.
        reason: RetryReason,
    },
}

/// Why a batch is retried instead of resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryReason {
    /// The protocol or the symbol is not accepting settlement (codes 5 / 6):
    /// retry until the pause lifts.
    Paused,
    /// A transaction-level failure (an RPC error, gas, a nonce gap, a
    /// reorg): the transaction never settled, so the batch is re-submitted.
    /// Never produced by [`classify`], which only sees decoded revert data —
    /// the submitter raises it when the submission itself fails.
    TxError,
    /// The revert cannot be explained (an unknown code, an out-of-range
    /// index, a missing side): retry and page the human — the symbol's
    /// settlement stalls behind the batch until it resolves.
    Unclassified,
}

/// Maps the decoded revert data of an on-chain `settleBatch` call to the
/// action the submitter takes:
///
/// | code | index / side | action |
/// | --- | --- | --- |
/// | 1, 2, 4, 7, 8 | `index < trades_len` and `side` is 1 or 2 | [`Action::Revert`] |
/// | 1, 2, 4, 7, 8 | `index >= trades_len` or `side` is neither 1 nor 2 | [`Action::Retry`] with [`RetryReason::Unclassified`] |
/// | 3 | any | [`Action::TreatAsSettled`] |
/// | 5, 6 | any | [`Action::Retry`] with [`RetryReason::Paused`] |
/// | anything else | any | [`Action::Retry`] with [`RetryReason::Unclassified`] |
///
/// The index is relative to the trades of the batch the revert came from —
/// the caller passes that batch's length as `trades_len`.
pub fn classify(code: u8, index: usize, side: u8, trades_len: usize) -> Action {
    match code {
        // The idempotent double-submission path: the trade is already
        // settled, so the batch is final regardless of index and side.
        3 => Action::TreatAsSettled,
        // Transient: the protocol is not accepting settlement.
        5 | 6 => Action::Retry { reason: RetryReason::Paused },
        // Deterministic: the code names one failing trade, and the index and
        // the side say which. A code the protocol never pairs with them is
        // unclassifiable rather than a guess.
        1 | 2 | 4 | 7 | 8 => match (index < trades_len, fault_side(side)) {
            (true, Some(at_fault)) => Action::Revert {
                failed_trade: index,
                at_fault,
                reason: SettlementFailure::Protocol(code),
            },
            _ => Action::Retry { reason: RetryReason::Unclassified },
        },
        // An unknown code: the revert is unexplained.
        _ => Action::Retry { reason: RetryReason::Unclassified },
    }
}

/// Decodes the at-fault side byte of the revert data: 1 = taker, 2 = maker
/// (doc/settlement-protocol.md). Anything else — including 0, "neither" — is
/// unknown. `None` makes the revert unclassifiable rather than a guess.
pub fn fault_side(side: u8) -> Option<FaultSide> {
    match side {
        1 => Some(FaultSide::Taker),
        2 => Some(FaultSide::Maker),
        _ => None,
    }
}

/// Cuts a batch in half at `len / 2`, so both halves are non-empty. Returns
/// `None` when there is nothing to split — fewer than 2 trades, or an index
/// outside the batch (nothing to isolate).
///
/// The half covering `index` is the poison half; which of the two that is
/// decides which half [`plan_isolation`] queues to settle, so this function
/// validates the index and leaves the choice to its caller.
pub fn split(trades: &[Trade], index: usize) -> Option<(&[Trade], &[Trade])> {
    if trades.len() < 2 || index >= trades.len() {
        return None;
    }
    Some(trades.split_at(trades.len() / 2))
}

/// The plan of isolating one failing trade out of a batch: the clean halves
/// to settle, the poison trade to resolve, and the action the failing trade
/// takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsolationPlan {
    /// The clean halves to submit, in submission order (the clean half of
    /// the outermost split first, then the halves split off deeper). Their
    /// union is the batch minus the poison trade.
    pub settle: Vec<Vec<Trade>>,
    /// The poison trade, isolated down to a singleton — the one the action
    /// applies to.
    pub poison: Vec<Trade>,
    /// The action of the poison trade, classified as a one-trade batch.
    pub action: Action,
}

/// Plans the isolation of the failing trade at `index` by binary-splitting:
/// the batch is cut in half at `len / 2`, the clean half is queued to
/// settle, and the poison half is split again until only the failing trade
/// remains. The recursion depth is `ceil(log2 n)` — the number of extra
/// submissions the on-chain all-or-nothing revert costs.
///
/// The clean halves are queued outermost-first, so a submission of
/// `settle[0]`, `settle[1]`, …, `poison` never depends on the deeper splits.
/// A batch of one trade isolates nothing: `settle` is empty and the poison
/// is the whole batch.
///
/// An out-of-range `index` (which [`classify`] maps to a retry, so the
/// submitter does not plan an isolation for it) leaves the input whole as
/// the poison and nothing to settle.
pub fn plan_isolation(trades: &[Trade], code: u8, index: usize, side: u8) -> IsolationPlan {
    let mut settle = Vec::new();
    let mut poison = trades;
    let mut at = index;
    while let Some((left, right)) = split(poison, at) {
        if at < left.len() {
            // The poison is in the left half: the clean right half settles.
            settle.push(right.to_vec());
            poison = left;
        } else {
            // The poison is in the right half: the clean left half settles,
            // and the index is rebased onto the right half.
            settle.push(left.to_vec());
            poison = right;
            at -= left.len();
        }
    }
    IsolationPlan { settle, poison: poison.to_vec(), action: classify(code, 0, side, 1) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::address::Address;
    use primitives::base::{Hash32, Nonce, Side, Symbol};
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

    /// A trade identified by its position: the taker's address carries the
    /// position, so a trade can be addressed after it was split off.
    fn trade(position: usize) -> Trade {
        let user = position as u8;
        let pos = position as u64;
        Trade::new(order(user, pos), Quantity(9), order(user + 1, pos), Price(100), Quantity(1))
    }

    /// The position of a trade, see [`trade`].
    fn position(trade: &Trade) -> usize {
        usize::from(trade.taker.hot.user.0[0])
    }

    /// The decision table of doc/settlement-protocol.md, transcribed
    /// independently of the implementation.
    fn expected(code: u8, index: usize, side: u8, trades_len: usize) -> Action {
        let at_fault = match side {
            1 => Some(FaultSide::Taker),
            2 => Some(FaultSide::Maker),
            _ => None,
        };
        if code == 3 {
            return Action::TreatAsSettled;
        }
        if code == 5 || code == 6 {
            return Action::Retry { reason: RetryReason::Paused };
        }
        let deterministic = matches!(code, 1 | 2 | 4 | 7 | 8);
        match (deterministic && index < trades_len, at_fault) {
            (true, Some(at_fault)) => Action::Revert {
                failed_trade: index,
                at_fault,
                reason: SettlementFailure::Protocol(code),
            },
            _ => Action::Retry { reason: RetryReason::Unclassified },
        }
    }

    #[test]
    fn test_classify_is_exhaustive_over_the_table() {
        for trades_len in [1usize, 2, 4] {
            let indices =
                [0usize, trades_len / 2, trades_len - 1, trades_len, trades_len + 1, usize::MAX];
            for code in 0..=9u8 {
                for side in 0..=3u8 {
                    for index in indices {
                        let action = classify(code, index, side, trades_len);
                        assert_eq!(
                            action,
                            expected(code, index, side, trades_len),
                            "code {code}, index {index}, side {side}, len {trades_len}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_classify_canonical_rows() {
        // A deterministic failure with a valid index and side reverts.
        for code in [1u8, 2, 4, 7, 8] {
            assert_eq!(
                classify(code, 1, 1, 2),
                Action::Revert {
                    failed_trade: 1,
                    at_fault: FaultSide::Taker,
                    reason: SettlementFailure::Protocol(code),
                }
            );
            assert_eq!(
                classify(code, 0, 2, 1),
                Action::Revert {
                    failed_trade: 0,
                    at_fault: FaultSide::Maker,
                    reason: SettlementFailure::Protocol(code),
                }
            );
            // The index is out of the batch: never a guess.
            for index in [1usize, 2, usize::MAX] {
                assert_eq!(
                    classify(code, index, 1, 1),
                    Action::Retry { reason: RetryReason::Unclassified }
                );
            }
            // Side 0 ("neither") and unknown sides are unclassifiable.
            for side in [0u8, 3, 255] {
                assert_eq!(
                    classify(code, 0, side, 1),
                    Action::Retry { reason: RetryReason::Unclassified }
                );
            }
        }
        // The double submission is settled whatever the data says.
        for index in [0usize, 7, usize::MAX] {
            for side in 0..=3u8 {
                assert_eq!(classify(3, index, side, 0), Action::TreatAsSettled);
            }
        }
        // A pause is transient whatever the data says.
        for code in [5u8, 6] {
            for index in [0usize, usize::MAX] {
                assert_eq!(
                    classify(code, index, 0, 0),
                    Action::Retry { reason: RetryReason::Paused }
                );
            }
        }
        // An unknown code is unclassifiable whatever the data says.
        for code in [0u8, 9, 10, 255] {
            assert_eq!(
                classify(code, 0, 1, 1),
                Action::Retry { reason: RetryReason::Unclassified }
            );
        }
    }

    #[test]
    fn test_fault_side_decodes_taker_and_maker() {
        assert_eq!(fault_side(1), Some(FaultSide::Taker));
        assert_eq!(fault_side(2), Some(FaultSide::Maker));
        for side in [0u8, 3, 4, 127, 255] {
            assert_eq!(fault_side(side), None, "side {side}");
        }
    }

    #[test]
    fn test_split_properties() {
        for len in 0..=8usize {
            let trades: Vec<Trade> = (0..len).map(trade).collect();
            for index in 0..=len + 1 {
                let halves = split(&trades, index);
                if len < 2 || index >= len {
                    assert!(halves.is_none(), "len {len}, index {index}");
                    continue;
                }
                let (left, right) = halves.unwrap();
                // The halves are the ordered, contiguous halves of the input.
                assert_eq!(left.len(), len / 2, "len {len}, index {index}");
                assert!(!left.is_empty() && !right.is_empty());
                assert_eq!(left, &trades[..left.len()]);
                assert_eq!(right, &trades[left.len()..]);
                assert_eq!(left.len() + right.len(), len);
                // The poison index falls into one of the two halves.
                assert!(index < left.len() || index - left.len() < right.len());
            }
        }
    }

    #[test]
    fn test_plan_isolation_properties() {
        for len in 1..=8usize {
            let trades: Vec<Trade> = (0..len).map(trade).collect();
            for index in 0..len {
                for (code, side) in [(2u8, 1u8), (1, 2), (4, 1), (8, 2)] {
                    let plan = plan_isolation(&trades, code, index, side);
                    // The poison is the singleton failing trade.
                    assert_eq!(plan.poison, vec![trades[index]], "len {len}, index {index}");
                    assert_eq!(plan.action, classify(code, 0, side, 1));
                    // The clean halves are a partition of the input minus
                    // the poison: contiguous slices of the input, no
                    // duplicates, none missing.
                    let mut settled: Vec<usize> =
                        plan.settle.iter().flatten().map(position).collect();
                    let mut want: Vec<usize> = (0..len).filter(|pos| *pos != index).collect();
                    assert_eq!(settled.len(), len - 1);
                    settled.sort_unstable();
                    want.sort_unstable();
                    assert_eq!(settled, want);
                    for half in &plan.settle {
                        assert!(!half.is_empty());
                        assert!(
                            trades.windows(half.len()).any(|window| window == half),
                            "len {len}, index {index}: the half is not a contiguous slice"
                        );
                    }
                    // The isolation costs at most ceil(log2 n) extra splits.
                    let bound = usize::try_from(len.next_power_of_two().trailing_zeros()).unwrap();
                    assert!(plan.settle.len() <= bound, "len {len}, index {index}");
                }
            }
        }
    }

    #[test]
    fn test_plan_isolation_orders_the_clean_halves_outermost_first() {
        let trades: Vec<Trade> = (0..4).map(trade).collect();
        let plan = plan_isolation(&trades, 2, 1, 1);
        // The poison sits in the left half of the outer split, so the outer
        // clean half (the right one) is submitted first, then the inner one.
        assert_eq!(plan.settle, vec![vec![trades[2], trades[3]], vec![trades[0]]]);
        assert_eq!(plan.poison, vec![trades[1]]);
        assert_eq!(
            plan.action,
            Action::Revert {
                failed_trade: 0,
                at_fault: FaultSide::Taker,
                reason: SettlementFailure::Protocol(2),
            }
        );
    }

    #[test]
    fn test_plan_isolation_of_a_single_trade_batch() {
        let trades = vec![trade(0)];
        let plan = plan_isolation(&trades, 2, 0, 2);
        assert!(plan.settle.is_empty());
        assert_eq!(plan.poison, trades);
        assert_eq!(
            plan.action,
            Action::Revert {
                failed_trade: 0,
                at_fault: FaultSide::Maker,
                reason: SettlementFailure::Protocol(2),
            }
        );
    }

    #[test]
    fn test_plan_isolation_of_an_out_of_range_index_keeps_the_batch_whole() {
        // `classify` maps an out-of-range index to a retry, so the submitter
        // never plans an isolation for it; the plan stays total and settles
        // nothing rather than truncating the batch.
        let trades: Vec<Trade> = (0..4).map(trade).collect();
        for index in [4usize, 9, usize::MAX] {
            let plan = plan_isolation(&trades, 1, index, 1);
            assert!(plan.settle.is_empty());
            assert_eq!(plan.poison, trades);
        }
    }

    #[test]
    fn test_plan_isolation_action_follows_the_table() {
        // The action is classified for the isolated singleton: the transient
        // and idempotent codes stay retry / settled even when a Revert row
        // would have matched a batch-level classify.
        let trades: Vec<Trade> = (0..4).map(trade).collect();
        assert_eq!(plan_isolation(&trades, 3, 1, 1).action, Action::TreatAsSettled);
        assert_eq!(
            plan_isolation(&trades, 6, 1, 1).action,
            Action::Retry { reason: RetryReason::Paused }
        );
        assert_eq!(
            plan_isolation(&trades, 8, 1, 0).action,
            Action::Retry { reason: RetryReason::Unclassified }
        );
    }
}
