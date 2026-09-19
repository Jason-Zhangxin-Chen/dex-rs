//! Orderbook definitions.

use crate::base::{Hash32, Symbol};
use crate::orderbook::price_level::{OrderIdx, OrderNode, PriceLevel};
use crate::value::{Price, Quantity, TimestampMs};
use rustc_hash::FxHashMap;
use slab::Slab;
use std::collections::BTreeMap;
use std::sync::Arc;
use crate::address::Address;
use crate::event::PriceLevelChangedEvent;
use crate::orderbook::fee::FeeSchedule;
use crate::orderbook::risk::RiskState;
use crate::orderbook::stp::STPMode;
use crate::trade::TradeResult;

pub type TradeListener = Arc<dyn Fn(&TradeResult) + Send + Sync>;

pub type PriceLevelChangedListener = Arc<dyn Fn(PriceLevelChangedEvent) + Send + Sync>;

/// Orderbook
pub struct OrderBook {
    /// The market symbol.
    symbol: Symbol,

    /// The arena of orders, all live orders.
    arena: Slab<OrderNode>,

    /// The bids price levels.
    bids: BTreeMap<Price, PriceLevel>,

    /// The asks price levels.
    asks: BTreeMap<Price, PriceLevel>,

    /// Index for an order.
    index: FxHashMap<Hash32, OrderIdx>,

    /// User orders.
    user_orders: FxHashMap<Address, Vec<OrderIdx>>,

    /// Kill switch.
    kill_switch: bool,

    /// Pre-trade risk state: optional [`RiskConfig`] plus per-account
    /// counters and per-order entries. When the embedded config is
    /// `None` (default), every check is a passthrough and every hook
    /// is a no-op. Always present so that [`Self::set_risk_config`]
    /// can engage the gates without constructor changes. The config
    /// is persisted across snapshot/restore; counters are rebuilt
    /// post-restore by walking the snapshot's resting orders.
    risk_state: RiskState,

    /// Last trade price.
    last_trade_price: Option<Price>,

    /// Flag indicating if there was a trade.
    hash_traded: bool,

    /// How many `ReserveOrder { auto_replenish: false, .. }` makers carrying
    /// hidden quantity are resting on this book (#230).
    ///
    /// Gates the pre-match scan that captures makers whose hidden depth a
    /// sweep would strand: at zero the scan can never report anything, and
    /// skipping it keeps the sweep off `PriceLevel::iter_orders`, whose
    /// `DashMap` iterator read-locks every shard per level match.
    /// `match_order_inner` reads this once per sweep and, when it is zero,
    /// allocates no capture buffer and runs no per-level capture at all.
    ///
    /// # Why the count is exact
    ///
    /// Not "an error would be harmless": the count is exact, and each side
    /// of it has a reason.
    ///
    /// **Increments** sit at the only two places an order is ever rested:
    /// the level insertion in `add_order_inner` and the snapshot-restore
    /// commit. A strandable maker cannot reach a level without passing one
    /// of them, so the count can never under-count.
    ///
    /// **Decrements** sit at the only three places such a maker leaves a
    /// level, and each decides from the removed order's **own body**, never
    /// from a cached id: `cancel_order_with_reason` (the funnel for user
    /// cancels, the three cancel-then-add modifies, the scoped mass cancels
    /// and expiry eviction) and `cancel_resting_maker_on_level` (the
    /// self-trade-prevention maker cancel) both hold the cancelled
    /// `OrderType`; the fill drain in `match_order_inner` decides from that
    /// sweep's own capture list. `cancel_all_orders` empties the book in
    /// bulk without that funnel and resets the count to zero instead.
    ///
    /// The fill drain is exact because of the gate rule in
    /// [`acquire_coherent_submit_gate`](Self::acquire_coherent_submit_gate):
    /// a sweep in a book holding strandable makers runs **exclusively**, so
    /// no cancel, admission or id reuse can interleave between a level's
    /// capture and its match. The capture therefore still describes the
    /// orders the match consumes, and an id in both the capture list and
    /// `filled_orders` is the same order in both — not a `Standard` order
    /// that reused a cancelled reserve's id.
    ///
    /// The saturating decrement is defence in depth against a future path
    /// that removes a maker without passing one of the three, not a licence
    /// to be approximate.
    ///
    /// Not part of the snapshot format: the restore commit resets it to
    /// zero with the rest of the book state and recounts from the orders it
    /// installs.
    ///
    /// Coherence with the sweep (#225 / #230): admitting a strandable maker
    /// takes the **exclusive** side of the submit gate, so no such maker can
    /// be admitted while a sweep holds the shared side. The once-per-sweep
    /// read and the per-level captures therefore observe the same set.
    strandable_makers_resting: usize,

    /// The timestamp of market close, if applicable (for DAY orders).
    market_close_timestamp: TimestampMs,

    /// Flag indicating if market close is set.
    has_market_close: bool,

    /// Listens to possible trades when an order is added.
    trade_listener: Option<TradeListener>,

    /// Price level change listener.
    price_level_changed_listener: Option<PriceLevelChangedListener>,

    /// Minimum price increment for orders. When set, order prices must be
    /// exact multiples of this value. `None` disables validation (default).
    tick_size: Option<Price>,

    /// Minimum quantity increment for orders. When set, order quantities must be
    /// exact multiples of this value. `None` disables validation (default).
    lot_size: Option<Quantity>,

    /// Minimum order size. When set, orders with `total_quantity() < min` are
    /// rejected. `None` disables validation (default).
    min_order_size: Option<Quantity>,

    /// Maximum order size. When set, orders with `total_quantity() > max` are
    /// rejected. `None` disables validation (default).
    max_order_size: Option<Quantity>,

    /// STP mode.
    stp_mode: STPMode,

    /// Fee schedule for calculating trading fees. When None, no fees are applied.
    /// Fees are calculated during trade execution and can be configured per orderbook.
    fee_schedule: Option<FeeSchedule>,

    // todo: order state listener.

    // todo: source the clock.
}
