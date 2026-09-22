//! Trade represents the exchange between maker and taker orders.

use crate::base::Quote;
use crate::order::Order;
use crate::value::{Price, Quantity, TimestampMs};
use serde::{Deserialize, Serialize};

/// Enhanced trade result that includes symbol information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeResult {
    /// The symbol this trade result belongs to
    pub taker_order: Order,

    /// Remaining quantity of the taker order after matching.
    pub remaining_quantity: Quantity,

    /// Match outcome.
    pub out_come: MatchOutcome,

    /// Total quote-asset notional consumed by this trade, computed as
    /// `Σ price × quantity` across every transaction. Populated for both
    /// base-quantity (`match_market_order`) and quote-notional
    /// (`match_market_order_by_amount`) market-order paths so consumers
    /// have the field uniformly available without recomputing per-trade.
    ///
    /// Defaults to `0` when deserializing payloads from format versions
    /// that pre-date `quote_notional` so existing consumers keep parsing.
    pub quote_notional: Quote,

    /// List of trades that resulted from the match. The vector<Trade> is pooled
    /// in the free cache with RAII guard.
    pub trades: Vec<Trade>,

    /// Timestamp when the trade occurred in milliseconds since epoch.
    pub timestamp: TimestampMs,
}

/// Represents a completed trade between two orders.
///
/// All fields are private to enforce immutability after construction.
/// Use the provided accessor methods to read trade data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    /// Maker order.
    pub maker_order: Order,

    /// Price at which the trade occurred.
    pub price: Price,

    /// Quantity traded.
    pub quantity: Quantity,
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
