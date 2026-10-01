//! Pre-trade risk layer for orderbook.

use crate::address::Address;
use crate::base::Nonce;
use crate::message::side_path::RejectReason;
use crate::order::Order;
use crate::orderbook::config::RiskConfig;
use crate::value::{Price, Quantity};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

/// Risk state bound to a single [`OrderBook`](crate::OrderBook).
///
/// Carries the optional [`RiskConfig`], the per-account counters, the
/// per-order entry map, and a one-shot warning latch for the
/// "no reference price available" code path. All public operations
/// are no-ops when `config` is `None`.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskState {
    /// RiskState contains the risk config for risk handling.
    risk_config: RiskConfig,
    /// Tracked risk counters for per account.
    counters: FxHashMap<Address, RiskCounters>,
    /// Tracked risk entry for per order.
    orders: FxHashMap<(Address, Nonce), RiskEntry>,
    /// one-shot warning latch for the "no reference price available" code path.
    warned_no_reference: bool,
}

/// Per-account counters maintained by [`RiskState`].
///
/// Counters are updated with `Relaxed` ordering on the hot path. They
/// are estimative: a transient over- or under-count of one in-flight
/// order is acceptable and does not exceed the configured limit by
/// more than a single race window. Strict accuracy is enforced by
/// snapshot rebuild.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskCounters {
    /// Number of resting orders this account currently has on the book.
    open_count: u64,
    /// Sum of `price × remaining_qty` (in raw ticks) across all of
    /// this account's resting orders.
    resting_notional: u128,
}

/// Per-resting-order risk bookkeeping.
///
/// One entry per order admitted into the resting book. Used on cancel
/// and fill to compute the deltas applied to per-account counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct RiskEntry {
    account: Address,
    price: Price,
    remaining_qty: Quantity,
}

/// Source for the reference price used by the price-band check.
///
/// The price band rejects orders whose limit price deviates from the
/// reference by more than the configured number of basis points.
/// `LastTrade` and `Mid` resolve dynamically per check; `FixedPrice`
/// is operator-pinned (e.g. an external mark price piped in).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum ReferencePriceSource {
    /// Last executed trade price. The check is skipped when no trade
    /// has occurred yet on this book.
    #[default]
    LastTrade,
    /// Integer midpoint `(best_bid + best_ask) / 2`. Falls back to
    /// `LastTrade` when the book is one-sided. The check is skipped
    /// when neither a midpoint nor a last trade is available.
    Mid,
    /// Caller-supplied fixed reference price (raw integer ticks). The
    /// check always runs.
    FixedPrice(Price),
}

impl RiskState {
    /// Constructs the risk state for the given config. Every check is a
    /// no-op when the config disables it (`None` fields).
    pub fn new(risk_config: Option<RiskConfig>) -> Self {
        Self { risk_config: risk_config.unwrap_or_default(), ..Default::default() }
    }

    /// Whether any check is configured.
    fn enabled(&self) -> bool {
        let config = &self.risk_config;
        config.max_notional_per_account.is_some()
            || config.price_band_bps.is_some()
            || config.max_open_orders_per_account.is_some()
            || config.reference_price.is_some()
    }

    /// Resolves the reference price for the price-band check per the
    /// configured [`ReferencePriceSource`]. `None` when no reference is
    /// available, which skips the check.
    pub fn reference_price(
        &self,
        best_bid: Option<Price>,
        best_ask: Option<Price>,
        last_trade: Option<Price>,
    ) -> Option<Price> {
        match self.risk_config.reference_price? {
            ReferencePriceSource::LastTrade => last_trade,
            ReferencePriceSource::Mid => match (best_bid, best_ask) {
                (Some(bid), Some(ask)) => Some(Price((bid.0 + ask.0) / 2)),
                _ => last_trade,
            },
            ReferencePriceSource::FixedPrice(price) => Some(price),
        }
    }

    /// Checks the admission of an order against the configured limits:
    /// per-account open orders, per-account resting notional and the
    /// price-band around the reference price.
    pub fn check_admission(
        &self,
        account: Address,
        price: Price,
        quantity: Quantity,
        reference: Option<Price>,
    ) -> Result<(), RejectReason> {
        if !self.enabled() {
            return Ok(());
        }
        let config = &self.risk_config;
        if let Some(max_open) = config.max_open_orders_per_account {
            let open = self.counters.get(&account).map_or(0, |c| c.open_count);
            if open >= u64::from(max_open) {
                return Err(RejectReason::RiskMaxOpenOrders);
            }
        }
        if let Some(max_notional) = config.max_notional_per_account {
            let current = self.counters.get(&account).map_or(0, |c| c.resting_notional);
            let incoming = u128::from(price.0) * u128::from(quantity.0);
            if current + incoming > max_notional {
                return Err(RejectReason::RiskMaxNotional);
            }
        }
        if let (Some(bps), Some(reference)) = (config.price_band_bps, reference)
            && reference.0 > 0
        {
            let deviation =
                u128::from(price.0.abs_diff(reference.0)) * 10_000 / u128::from(reference.0);
            if deviation > u128::from(bps) {
                return Err(RejectReason::RiskPriceBand);
            }
        }
        Ok(())
    }

    /// Records an order admitted into the resting book.
    pub fn record_open(&mut self, order: &Order) {
        if !self.enabled() {
            return;
        }
        let account = order.hot.user;
        let price = order.hot.price;
        let quantity = order.total_quantity();
        let counters = self.counters.entry(account).or_default();
        counters.open_count += 1;
        counters.resting_notional += u128::from(price.0) * u128::from(quantity.0);
        self.orders.insert(
            (account, order.hot.nonce),
            RiskEntry { account, price, remaining_qty: quantity },
        );
    }

    /// Records an order leaving the resting book (filled or cancelled). The
    /// counter deltas mirror the values captured at [`RiskState::record_open`].
    pub fn record_removed(&mut self, account: Address, nonce: Nonce) {
        let Some(entry) = self.orders.remove(&(account, nonce)) else { return };
        let counters = self.counters.entry(entry.account).or_default();
        counters.open_count -= 1;
        counters.resting_notional -= u128::from(entry.price.0) * u128::from(entry.remaining_qty.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::{Hash32, Side, Symbol};
    use crate::order::{OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use crate::signature::Signature;
    use crate::time_in_force::TimeInForce;
    use crate::value::TimestampMs;

    fn order(user: u8, nonce: u64, price: u64, quantity: u64) -> Order {
        Order::new(
            OrderHot {
                user: Address([user; 20]),
                nonce: Nonce(nonce),
                price: Price(price),
                quantity: Quantity(quantity),
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

    // ---------------------------------------------------------------
    // RiskConfig
    // ---------------------------------------------------------------

    #[test]
    fn test_risk_config_defaults() {
        let config = RiskConfig::default();
        assert_eq!(config.max_notional_per_account, None);
        assert_eq!(config.price_band_bps, None);
        assert_eq!(config.max_open_orders_per_account, None);
        assert_eq!(config.reference_price, None);
    }

    #[test]
    fn test_risk_config_with_setters() {
        let config = RiskConfig::default()
            .with_max_notional_per_account(1_000_000)
            .with_price_band_bps(50)
            .with_max_open_orders_per_account(100)
            .with_reference_price(ReferencePriceSource::FixedPrice(Price(42)));
        assert_eq!(config.max_notional_per_account, Some(1_000_000));
        assert_eq!(config.price_band_bps, Some(50));
        assert_eq!(config.max_open_orders_per_account, Some(100));
        assert_eq!(config.reference_price, Some(ReferencePriceSource::FixedPrice(Price(42))));
    }

    // ---------------------------------------------------------------
    // RiskState
    // ---------------------------------------------------------------

    #[test]
    fn test_disabled_config_is_noop() {
        let mut state = RiskState::new(None);
        assert!(state.check_admission(Address([1; 20]), Price(1), Quantity(1), None).is_ok());
        state.record_open(&order(1, 1, 100, 10));
        assert!(state.counters.is_empty());
        assert!(state.orders.is_empty());
        state.record_removed(Address([1; 20]), Nonce(1));
    }

    #[test]
    fn test_reference_price_resolution() {
        let last_trade =
            RiskConfig::default().with_reference_price(ReferencePriceSource::LastTrade);
        let state = RiskState::new(Some(last_trade));
        assert_eq!(
            state.reference_price(Some(Price(90)), Some(Price(100)), Some(Price(95))),
            Some(Price(95))
        );
        assert_eq!(state.reference_price(None, None, None), None);

        let mid = RiskConfig::default().with_reference_price(ReferencePriceSource::Mid);
        let state = RiskState::new(Some(mid));
        assert_eq!(state.reference_price(Some(Price(90)), Some(Price(100)), None), Some(Price(95)));
        // One-sided book falls back to the last trade.
        assert_eq!(state.reference_price(Some(Price(90)), None, Some(Price(91))), Some(Price(91)));
        // Neither midpoint nor last trade: the check is skipped.
        assert_eq!(state.reference_price(Some(Price(90)), None, None), None);

        let fixed =
            RiskConfig::default().with_reference_price(ReferencePriceSource::FixedPrice(Price(77)));
        let state = RiskState::new(Some(fixed));
        assert_eq!(state.reference_price(None, None, None), Some(Price(77)));
    }

    #[test]
    fn test_check_admission_max_open_orders() {
        let config = RiskConfig::default().with_max_open_orders_per_account(2);
        let mut state = RiskState::new(Some(config));
        state.record_open(&order(1, 1, 100, 1));
        // One resting order leaves headroom for another.
        assert!(state.check_admission(Address([1; 20]), Price(100), Quantity(1), None).is_ok());
        state.record_open(&order(1, 2, 100, 1));
        // At the limit, another admission would breach it.
        assert_eq!(
            state.check_admission(Address([1; 20]), Price(100), Quantity(1), None),
            Err(RejectReason::RiskMaxOpenOrders)
        );
        // A different account is unaffected.
        assert!(state.check_admission(Address([2; 20]), Price(100), Quantity(1), None).is_ok());
    }

    #[test]
    fn test_check_admission_max_notional() {
        let config = RiskConfig::default().with_max_notional_per_account(1_000);
        let mut state = RiskState::new(Some(config));
        state.record_open(&order(1, 1, 100, 5));
        // 500 resting + 100 * 6 = 1_100 exceeds the cap.
        assert_eq!(
            state.check_admission(Address([1; 20]), Price(100), Quantity(6), None),
            Err(RejectReason::RiskMaxNotional)
        );
        // 500 + 500 is exactly the cap and passes.
        assert!(state.check_admission(Address([1; 20]), Price(100), Quantity(5), None).is_ok());
    }

    #[test]
    fn test_check_admission_price_band() {
        let config = RiskConfig::default()
            .with_price_band_bps(100)
            .with_reference_price(ReferencePriceSource::LastTrade);
        let state = RiskState::new(Some(config));
        // 1% deviation is exactly 100 bps and passes, 2% is rejected.
        assert!(
            state
                .check_admission(Address([1; 20]), Price(101), Quantity(1), Some(Price(100)))
                .is_ok()
        );
        assert_eq!(
            state.check_admission(Address([1; 20]), Price(102), Quantity(1), Some(Price(100))),
            Err(RejectReason::RiskPriceBand)
        );
        // No reference price available: the check is skipped.
        assert!(state.check_admission(Address([1; 20]), Price(999), Quantity(1), None).is_ok());
    }

    #[test]
    fn test_record_removed_releases_counters() {
        let config = RiskConfig::default().with_max_open_orders_per_account(2);
        let mut state = RiskState::new(Some(config));
        state.record_open(&order(1, 1, 100, 5));
        state.record_open(&order(1, 2, 100, 5));
        assert_eq!(
            state.check_admission(Address([1; 20]), Price(100), Quantity(1), None),
            Err(RejectReason::RiskMaxOpenOrders)
        );
        state.record_removed(Address([1; 20]), Nonce(1));
        assert!(state.check_admission(Address([1; 20]), Price(100), Quantity(1), None).is_ok());
    }

    #[test]
    fn test_record_removed_unknown_order_is_noop() {
        let config = RiskConfig::default().with_max_open_orders_per_account(1);
        let mut state = RiskState::new(Some(config));
        state.record_removed(Address([1; 20]), Nonce(9));
        assert!(state.counters.is_empty());
        assert!(state.orders.is_empty());
    }
}
