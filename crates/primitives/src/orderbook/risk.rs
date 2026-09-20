//! Pre-trade risk layer for orderbook.

use crate::address::Address;
use crate::base::Hash32;
use crate::value::{Price, Quantity};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

/// Risk state bound to a single [`OrderBook`](crate::OrderBook).
///
/// Carries the optional [`RiskConfig`], the per-account counters, the
/// per-order entry map, and a one-shot warning latch for the
/// "no reference price available" code path. All public operations
/// are no-ops when `config` is `None`.
#[derive(Debug, Default)]
pub struct RiskState {
    config: Option<RiskConfig>,
    counters: FxHashMap<Address, RiskCounters>,
    orders: FxHashMap<Hash32, RiskEntry>,
    warned_no_reference: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskConfig {
    /// Maximum number of resting orders a single account may have on
    /// this book at any time. `None` disables the check.
    max_open_orders_per_account: Option<u64>,
    /// Maximum notional (`price × quantity`, in raw ticks) a single
    /// account may have resting on this book at any time. `None`
    /// disables the check.
    max_notional_per_account: Option<u128>,
    /// Maximum allowed deviation in basis points between an incoming
    /// limit price and the resolved reference price. `None` (or
    /// `reference_price = None`) disables the check.
    price_band_bps: Option<u32>,
    /// Reference price source used by the price-band check.
    reference_price: Option<ReferencePriceSource>,
}

/// Per-account counters maintained by [`RiskState`].
///
/// Counters are updated with `Relaxed` ordering on the hot path. They
/// are estimative: a transient over- or under-count of one in-flight
/// order is acceptable and does not exceed the configured limit by
/// more than a single race window. Strict accuracy is enforced by
/// snapshot rebuild.
#[derive(Debug, Default)]
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
#[derive(Debug, Clone, Copy)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum ReferencePriceSource {
    /// Last executed trade price. The check is skipped when no trade
    /// has occurred yet on this book.
    LastTrade,
    /// Integer midpoint `(best_bid + best_ask) / 2`. Falls back to
    /// `LastTrade` when the book is one-sided. The check is skipped
    /// when neither a midpoint nor a last trade is available.
    Mid,
    /// Caller-supplied fixed reference price (raw integer ticks). The
    /// check always runs.
    FixedPrice(Price),
}
