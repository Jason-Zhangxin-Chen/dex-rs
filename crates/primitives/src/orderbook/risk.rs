//! Pre-trade risk layer for orderbook.

use crate::address::Address;
use crate::base::Nonce;
use crate::orderbook::config::RiskConfig;
use crate::value::{Price, Quantity};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

// todo: impl the risk logics within this file.

/// Risk state bound to a single [`OrderBook`](crate::OrderBook).
///
/// Carries the optional [`RiskConfig`], the per-account counters, the
/// per-order entry map, and a one-shot warning latch for the
/// "no reference price available" code path. All public operations
/// are no-ops when `config` is `None`.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
