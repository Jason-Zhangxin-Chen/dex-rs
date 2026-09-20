//! Trade represents the exchange between maker and taker orders.

use crate::address::Address;
use crate::base::{Hash32, Quote, Side, Symbol};
use crate::value::{Price, Quantity, TimestampMs};
use serde::{Deserialize, Serialize};

// todo: impl builders for below types.

/// Enhanced trade result that includes symbol information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeResult {
    /// The symbol this trade result belongs to
    symbol: Symbol,
    /// The underlying match result.
    match_result: MatchResult,
    /// Total quote-asset notional consumed by this trade, computed as
    /// `Σ price × quantity` across every transaction. Populated for both
    /// base-quantity (`match_market_order`) and quote-notional
    /// (`match_market_order_by_amount`) market-order paths so consumers
    /// have the field uniformly available without recomputing per-trade.
    ///
    /// Defaults to `0` when deserializing payloads from format versions
    /// that pre-date `quote_notional` so existing consumers keep parsing.
    quote_notional: Quote,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchResult {
    /// The taker order ID.
    taker_order_id: Hash32,

    /// The taker address.
    taker_address: Address,

    /// The taker side.
    taker_side: Side,

    /// List of trades that resulted from teh match
    trades: Vec<Trade>,
    /// Remaining quantity of the taker order after matching.
    remaining_quantity: Quantity,
    /// Any maker orders that were completely filled and removed from the book.
    filled_order_ids: Vec<Hash32>,
    /// Match outcome.
    out_come: MatchOutcome,
}

/// Represents a completed trade between two orders.
///
/// All fields are private to enforce immutability after construction.
/// Use the provided accessor methods to read trade data.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Trade {
    /// Unique trade ID
    trade_id: Hash32,

    /// ID of the passive order that was in the book
    maker_order_id: Hash32,

    /// The maker address.
    maker_address: Address,

    /// Price at which the trade occurred
    price: Price,

    /// Quantity traded
    quantity: Quantity,

    /// Timestamp when the trade occurred in milliseconds since epoch
    timestamp: TimestampMs,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum MatchOutcome {
    /// The incoming order was completely filled (`remaining_quantity == 0`).
    Filled,

    /// The incoming order was partially filled: at least one trade occurred but
    /// some quantity remains. For a `Gtc` / `Gtd` / `Day` taker the order book
    /// rests the remainder; for an `Ioc` / market-to-limit taker it is
    /// discarded / converted by the caller.
    PartiallyFilled,

    /// No trade occurred and quantity remains because the level had nothing to
    /// fill the taker with (empty or fully consumed by an earlier sweep). This
    /// is the benign "no liquidity here" outcome — distinct from a kill or a
    /// rejection.
    #[default]
    NotFilled,

    /// A fill-or-kill (`Fok`) taker could not be filled in full at this level,
    /// so it was killed: zero trades, full remaining quantity, resting queue
    /// left untouched.
    Killed,

    /// A post-only taker would have taken liquidity (the level could fill some
    /// of it), so it was rejected: zero trades, full remaining quantity,
    /// resting queue left untouched.
    Rejected,
}

#[cfg(test)]
mod tests {
    use alloy::signers::local::PrivateKeySigner;
    use rmp_serde::{from_slice, to_vec};
    use solana_sdk::signer::{Signer, keypair::Keypair};

    use crate::address::{Address, EthAddress, SolAddress};
    use crate::base::{Hash32, Quote, Side, Symbol};
    use crate::value::{Price, Quantity, TimestampMs};

    use super::{MatchOutcome, MatchResult, Trade, TradeResult};

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    fn hash32(seed: u8) -> Hash32 {
        Hash32([seed; 32])
    }

    fn symbol(seed: u8) -> Symbol {
        Symbol([seed; 32])
    }

    fn eth_address() -> Address {
        let signer = PrivateKeySigner::random();
        Address::Ethereum(EthAddress(signer.address().0.0))
    }

    fn sol_address() -> Address {
        let keypair = Keypair::new();
        Address::Solana(SolAddress(keypair.pubkey().to_bytes()))
    }

    // ---------------------------------------------------------------
    // Trade (private fields — construct in-module)
    // ---------------------------------------------------------------

    fn make_trade(seed: u8) -> Trade {
        Trade {
            trade_id: hash32(seed),
            maker_order_id: hash32(seed.wrapping_add(100)),
            maker_address: if seed.is_multiple_of(2) { eth_address() } else { sol_address() },
            price: Price(1_000 * seed as u128),
            quantity: Quantity(10 * seed as u64),
            timestamp: TimestampMs(1_700_000_000_000 + seed as u64),
        }
    }

    #[test]
    fn test_trade_roundtrip() {
        let trade = make_trade(1);
        let bytes = to_vec(&trade).unwrap();
        let restored: Trade = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_trade_encoding_is_deterministic() {
        let trade = make_trade(2);
        assert_eq!(to_vec(&trade).unwrap(), to_vec(&trade).unwrap());
    }

    #[test]
    fn test_trade_roundtrip_is_stable() {
        let trade = make_trade(3);
        let bytes = to_vec(&trade).unwrap();
        let restored: Trade = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_trade_different_ids_are_distinct() {
        let a = make_trade(1);
        let b = make_trade(2);
        assert_ne!(to_vec(&a).unwrap(), to_vec(&b).unwrap());
    }

    #[test]
    fn test_trade_roundtrip_many_values() {
        for seed in 1..50u8 {
            let trade = make_trade(seed);
            let restored: Trade = from_slice(&to_vec(&trade).unwrap()).unwrap();
            assert_eq!(to_vec(&trade).unwrap(), to_vec(&restored).unwrap());
        }
    }

    #[test]
    fn test_trade_with_eth_and_sol_addresses() {
        let eth_trade = Trade {
            trade_id: hash32(1),
            maker_order_id: hash32(2),
            maker_address: eth_address(),
            price: Price(100),
            quantity: Quantity(1),
            timestamp: TimestampMs(1),
        };
        let sol_trade = Trade {
            trade_id: hash32(3),
            maker_order_id: hash32(4),
            maker_address: sol_address(),
            price: Price(100),
            quantity: Quantity(1),
            timestamp: TimestampMs(1),
        };

        let eth_restored: Trade = from_slice(&to_vec(&eth_trade).unwrap()).unwrap();
        let sol_restored: Trade = from_slice(&to_vec(&sol_trade).unwrap()).unwrap();

        assert!(matches!(eth_restored.maker_address, Address::Ethereum(_)));
        assert!(matches!(sol_restored.maker_address, Address::Solana(_)));
    }

    // ---------------------------------------------------------------
    // MatchOutcome (easiest — derives PartialEq / Eq / Default)
    // ---------------------------------------------------------------

    #[test]
    fn test_match_outcome_roundtrip() {
        for outcome in [
            MatchOutcome::Filled,
            MatchOutcome::PartiallyFilled,
            MatchOutcome::NotFilled,
            MatchOutcome::Killed,
            MatchOutcome::Rejected,
        ] {
            let bytes = to_vec(&outcome).unwrap();
            let restored: MatchOutcome = from_slice(&bytes).unwrap();
            assert_eq!(outcome, restored);
        }
    }

    #[test]
    fn test_match_outcome_all_variants_are_distinct() {
        let encoded: Vec<Vec<u8>> = [
            MatchOutcome::Filled,
            MatchOutcome::PartiallyFilled,
            MatchOutcome::NotFilled,
            MatchOutcome::Killed,
            MatchOutcome::Rejected,
        ]
        .iter()
        .map(|o| to_vec(o).unwrap())
        .collect();

        for i in 0..encoded.len() {
            for j in (i + 1)..encoded.len() {
                assert_ne!(encoded[i], encoded[j], "variants {i} and {j} collide");
            }
        }
    }

    #[test]
    fn test_match_outcome_default_is_not_filled() {
        assert_eq!(MatchOutcome::default(), MatchOutcome::NotFilled);
    }

    #[test]
    fn test_match_outcome_encoding_is_deterministic() {
        for outcome in [
            MatchOutcome::Filled,
            MatchOutcome::PartiallyFilled,
            MatchOutcome::NotFilled,
            MatchOutcome::Killed,
            MatchOutcome::Rejected,
        ] {
            assert_eq!(to_vec(&outcome).unwrap(), to_vec(&outcome).unwrap());
        }
    }

    // ---------------------------------------------------------------
    // MatchResult (private fields)
    // ---------------------------------------------------------------

    fn make_match_result(seed: u8) -> MatchResult {
        MatchResult {
            taker_order_id: hash32(seed),
            taker_address: if seed.is_multiple_of(2) { eth_address() } else { sol_address() },
            taker_side: if seed.is_multiple_of(2) { Side::Buy } else { Side::Sell },
            trades: vec![make_trade(seed), make_trade(seed.wrapping_add(1))],
            remaining_quantity: Quantity(0),
            filled_order_ids: vec![hash32(seed.wrapping_add(200))],
            out_come: MatchOutcome::Filled,
        }
    }

    #[test]
    fn test_match_result_roundtrip() {
        let result = make_match_result(1);
        let bytes = to_vec(&result).unwrap();
        let restored: MatchResult = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_match_result_encoding_is_deterministic() {
        let result = make_match_result(2);
        assert_eq!(to_vec(&result).unwrap(), to_vec(&result).unwrap());
    }

    #[test]
    fn test_match_result_roundtrip_is_stable() {
        let result = make_match_result(3);
        let bytes = to_vec(&result).unwrap();
        let restored: MatchResult = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_match_result_with_each_outcome() {
        for outcome in [
            MatchOutcome::Filled,
            MatchOutcome::PartiallyFilled,
            MatchOutcome::NotFilled,
            MatchOutcome::Killed,
            MatchOutcome::Rejected,
        ] {
            let result = MatchResult {
                taker_order_id: hash32(1),
                taker_address: eth_address(),
                taker_side: Side::Buy,
                trades: vec![],
                remaining_quantity: Quantity(100),
                filled_order_ids: vec![],
                out_come: outcome,
            };

            let restored: MatchResult = from_slice(&to_vec(&result).unwrap()).unwrap();
            assert_eq!(restored.out_come, outcome);
        }
    }

    #[test]
    fn test_match_result_empty_trades() {
        let result = MatchResult {
            taker_order_id: hash32(1),
            taker_address: eth_address(),
            taker_side: Side::Sell,
            trades: vec![],
            remaining_quantity: Quantity(100),
            filled_order_ids: vec![],
            out_come: MatchOutcome::NotFilled,
        };

        let restored: MatchResult = from_slice(&to_vec(&result).unwrap()).unwrap();
        assert_eq!(restored.trades.len(), 0);
        assert_eq!(restored.filled_order_ids.len(), 0);
    }

    #[test]
    fn test_match_result_many_trades() {
        let trades: Vec<Trade> = (1..=100u8).map(make_trade).collect();
        let result = MatchResult {
            taker_order_id: hash32(1),
            taker_address: eth_address(),
            taker_side: Side::Buy,
            trades,
            remaining_quantity: Quantity(0),
            filled_order_ids: vec![],
            out_come: MatchOutcome::Filled,
        };

        let restored: MatchResult = from_slice(&to_vec(&result).unwrap()).unwrap();
        assert_eq!(restored.trades.len(), 100);
        assert_eq!(to_vec(&result).unwrap(), to_vec(&restored).unwrap());
    }

    #[test]
    fn test_match_result_taker_side_preserved() {
        for side in [Side::Buy, Side::Sell] {
            let result = MatchResult {
                taker_order_id: hash32(1),
                taker_address: eth_address(),
                taker_side: side,
                trades: vec![],
                remaining_quantity: Quantity(0),
                filled_order_ids: vec![],
                out_come: MatchOutcome::NotFilled,
            };
            let restored: MatchResult = from_slice(&to_vec(&result).unwrap()).unwrap();
            assert_eq!(restored.taker_side, side);
        }
    }

    #[test]
    fn test_match_result_remaining_quantity_boundaries() {
        for qty in [0u64, 1, u64::MAX, u64::MAX - 1] {
            let result = MatchResult {
                taker_order_id: hash32(1),
                taker_address: eth_address(),
                taker_side: Side::Buy,
                trades: vec![],
                remaining_quantity: Quantity(qty),
                filled_order_ids: vec![],
                out_come: MatchOutcome::PartiallyFilled,
            };
            let restored: MatchResult = from_slice(&to_vec(&result).unwrap()).unwrap();
            assert_eq!(restored.remaining_quantity, Quantity(qty));
        }
    }

    // ---------------------------------------------------------------
    // TradeResult (public fields)
    // ---------------------------------------------------------------

    fn make_trade_result(seed: u8) -> TradeResult {
        TradeResult {
            symbol: symbol(seed),
            match_result: make_match_result(seed),
            quote_notional: Quote(1_000_000 * seed as u128),
        }
    }

    #[test]
    fn test_trade_result_roundtrip() {
        let tr = make_trade_result(1);
        let bytes = to_vec(&tr).unwrap();
        let restored: TradeResult = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_trade_result_field_survival() {
        let tr = make_trade_result(2);
        let restored: TradeResult = from_slice(&to_vec(&tr).unwrap()).unwrap();

        assert_eq!(restored.symbol, tr.symbol);
        assert_eq!(restored.quote_notional.0, tr.quote_notional.0);
    }

    #[test]
    fn test_trade_result_encoding_is_deterministic() {
        let tr = make_trade_result(3);
        assert_eq!(to_vec(&tr).unwrap(), to_vec(&tr).unwrap());
    }

    #[test]
    fn test_trade_result_roundtrip_is_stable() {
        let tr = make_trade_result(4);
        let bytes = to_vec(&tr).unwrap();
        let restored: TradeResult = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_trade_result_quote_notional_boundaries() {
        for value in [0u128, 1, u64::MAX as u128, u128::MAX, u128::MAX - 1] {
            let tr = TradeResult {
                symbol: symbol(1),
                match_result: make_match_result(1),
                quote_notional: Quote(value),
            };
            let restored: TradeResult = from_slice(&to_vec(&tr).unwrap()).unwrap();
            assert_eq!(restored.quote_notional.0, value);
        }
    }

    #[test]
    fn test_trade_result_with_each_outcome() {
        for outcome in [
            MatchOutcome::Filled,
            MatchOutcome::PartiallyFilled,
            MatchOutcome::NotFilled,
            MatchOutcome::Killed,
            MatchOutcome::Rejected,
        ] {
            let mut mr = make_match_result(1);
            mr.out_come = outcome;

            let tr = TradeResult { symbol: symbol(1), match_result: mr, quote_notional: Quote(0) };

            let restored: TradeResult = from_slice(&to_vec(&tr).unwrap()).unwrap();
            assert_eq!(restored.match_result.out_come, outcome);
        }
    }

    #[test]
    fn test_trade_result_different_symbols_are_distinct() {
        let a = make_trade_result(1);
        let b = make_trade_result(2);
        assert_ne!(to_vec(&a).unwrap(), to_vec(&b).unwrap());
    }

    // ---------------------------------------------------------------
    // Cross-cutting
    // ---------------------------------------------------------------

    #[test]
    fn test_nested_trade_result_roundtrip_many_seeds() {
        for seed in 1..30u8 {
            let tr = make_trade_result(seed);
            let bytes = to_vec(&tr).unwrap();
            let restored: TradeResult = from_slice(&bytes).unwrap();
            assert_eq!(bytes, to_vec(&restored).unwrap());
        }
    }

    #[test]
    fn test_repeated_encode_decode_cycles_are_idempotent() {
        let mut bytes = to_vec(&make_trade_result(1)).unwrap();
        for _ in 0..10 {
            let tr: TradeResult = from_slice(&bytes).unwrap();
            let next = to_vec(&tr).unwrap();
            assert_eq!(bytes, next);
            bytes = next;
        }
    }
}
