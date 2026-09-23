//! Trade represents the exchange between maker and taker orders.

use crate::base::Quote;
use crate::order::Order;
use crate::value::{Price, Quantity, TimestampMs};
use serde::{Deserialize, Serialize};

/// Enhanced trade result that includes symbol information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeEvent {
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

impl TradeEvent {
    /// Creates a new trade event.
    pub fn new(
        taker_order: Order,
        remaining_quantity: Quantity,
        out_come: MatchOutcome,
        quote_notional: Quote,
        trades: Vec<Trade>,
        timestamp: TimestampMs,
    ) -> Self {
        Self { taker_order, remaining_quantity, out_come, quote_notional, trades, timestamp }
    }

    /// Sets the taker order of the trade.
    pub fn with_taker_order(mut self, taker_order: Order) -> Self {
        self.taker_order = taker_order;
        self
    }

    /// Sets the remaining quantity of the taker order after matching.
    pub fn with_remaining_quantity(mut self, remaining_quantity: Quantity) -> Self {
        self.remaining_quantity = remaining_quantity;
        self
    }

    /// Sets the match outcome.
    pub fn with_out_come(mut self, out_come: MatchOutcome) -> Self {
        self.out_come = out_come;
        self
    }

    /// Sets the total quote-asset notional consumed by this trade.
    pub fn with_quote_notional(mut self, quote_notional: Quote) -> Self {
        self.quote_notional = quote_notional;
        self
    }

    /// Sets the list of trades that resulted from the match.
    pub fn with_trades(mut self, trades: Vec<Trade>) -> Self {
        self.trades = trades;
        self
    }

    /// Sets the timestamp when the trade occurred.
    pub fn with_timestamp(mut self, timestamp: TimestampMs) -> Self {
        self.timestamp = timestamp;
        self
    }
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

impl Trade {
    /// Creates a new trade.
    pub fn new(maker_order: Order, price: Price, quantity: Quantity) -> Self {
        Self { maker_order, price, quantity }
    }

    /// Sets the maker order.
    pub fn with_maker_order(mut self, maker_order: Order) -> Self {
        self.maker_order = maker_order;
        self
    }

    /// Sets the price at which the trade occurred.
    pub fn with_price(mut self, price: Price) -> Self {
        self.price = price;
        self
    }

    /// Sets the quantity traded.
    pub fn with_quantity(mut self, quantity: Quantity) -> Self {
        self.quantity = quantity;
        self
    }
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
    use super::*;
    use crate::events::test_utils::sample_order;

    // ---------------------------------------------------------------
    // Trade
    // ---------------------------------------------------------------

    #[test]
    fn test_trade_constructor() {
        let maker = sample_order(1);
        let trade = Trade::new(maker.clone(), Price(100), Quantity(10));
        assert_eq!(trade.maker_order, maker);
        assert_eq!(trade.price, Price(100));
        assert_eq!(trade.quantity, Quantity(10));
    }

    #[test]
    fn test_trade_with_setters() {
        let trade = Trade::new(sample_order(1), Price(1), Quantity(2))
            .with_maker_order(sample_order(3))
            .with_price(Price(4))
            .with_quantity(Quantity(5));
        assert_eq!(trade.maker_order, sample_order(3));
        assert_eq!(trade.price, Price(4));
        assert_eq!(trade.quantity, Quantity(5));
    }

    // ---------------------------------------------------------------
    // TradeEvent
    // ---------------------------------------------------------------

    #[test]
    fn test_trade_event_constructor() {
        let taker = sample_order(1);
        let maker = sample_order(2);
        let trade = Trade::new(maker.clone(), Price(100), Quantity(10));
        let event = TradeEvent::new(
            taker.clone(),
            Quantity(0),
            MatchOutcome::Filled,
            Quote(1_000),
            vec![trade.clone()],
            TimestampMs(42),
        );
        assert_eq!(event.taker_order, taker);
        assert_eq!(event.remaining_quantity, Quantity(0));
        assert_eq!(event.out_come, MatchOutcome::Filled);
        assert_eq!(event.quote_notional, Quote(1_000));
        assert_eq!(event.trades.len(), 1);
        assert_eq!(event.trades[0].maker_order, maker);
        assert_eq!(event.trades[0].price, Price(100));
        assert_eq!(event.trades[0].quantity, Quantity(10));
        assert_eq!(event.timestamp, TimestampMs(42));
    }

    #[test]
    fn test_trade_event_with_setters() {
        let maker = sample_order(2);
        let trade = Trade::new(maker.clone(), Price(100), Quantity(10));
        let event = TradeEvent::new(
            sample_order(1),
            Quantity(5),
            MatchOutcome::PartiallyFilled,
            Quote(500),
            vec![],
            TimestampMs(1),
        )
        .with_taker_order(sample_order(3))
        .with_remaining_quantity(Quantity(1))
        .with_out_come(MatchOutcome::NotFilled)
        .with_quote_notional(Quote(0))
        .with_trades(vec![trade])
        .with_timestamp(TimestampMs(2));
        assert_eq!(event.taker_order, sample_order(3));
        assert_eq!(event.remaining_quantity, Quantity(1));
        assert_eq!(event.out_come, MatchOutcome::NotFilled);
        assert_eq!(event.quote_notional, Quote(0));
        assert_eq!(event.trades.len(), 1);
        assert_eq!(event.trades[0].maker_order, maker);
        assert_eq!(event.trades[0].price, Price(100));
        assert_eq!(event.trades[0].quantity, Quantity(10));
        assert_eq!(event.timestamp, TimestampMs(2));
    }
}
