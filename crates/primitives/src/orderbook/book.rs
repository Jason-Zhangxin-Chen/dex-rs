//! Orderbook definitions.

use crate::address::Address;
use crate::base::{Hash32, Nonce, Symbol};
use crate::clock::Clock;
use crate::event::PriceLevelChangedEvent;
use crate::order::Order;
use crate::orderbook::order_status::OrderStatus;
use crate::orderbook::price_level::{OrderIdx, OrderNode, PriceLevel};
use crate::orderbook::risk::RiskState;
use crate::orderbook::statistics::{BookStatistics, PriceLevelStatistics};
use crate::orderbook::stp::STPMode;
use crate::trade::TradeResult;
use crate::value::{Price, Quantity};
use rustc_hash::FxHashMap;
use slab::Slab;
use std::collections::BTreeMap;
use std::ops::Add;

/// Trade listener push trade event to the settlement services, to the storage infra and to the
/// external messaging service.
pub type TradeListener = Box<dyn Fn(&TradeResult)>;

/// Price level change event listener push changes of price level to the external system, UI etc...
pub type PriceLevelChangedListener = Box<dyn Fn(PriceLevelChangedEvent)>;

/// Order status listener push the latest order state and its status to the external of the core.
pub type OrderStatusListener = Box<dyn Fn(Order, &OrderStatus)>;

/// Statistics listener push the book and price level statistics to the external system.
pub type StatisticListener = Box<dyn Fn(BookStatistics, Vec<PriceLevelStatistics>)>;

/// OrderBook
pub struct OrderBook {
    /// Kill switch.
    kill_switch: bool,

    /// The arena of orders, all live orders.
    arena: Slab<OrderNode>,

    /// The bids price levels.
    bids: BTreeMap<Price, PriceLevel>,

    /// The asks price levels.
    asks: BTreeMap<Price, PriceLevel>,

    /// Index for an order, use below tuple to replace hash32 for cache line friendly loading.
    index: FxHashMap<(Address, Nonce), OrderIdx>,

    /// User orders.
    user_orders: FxHashMap<Address, Vec<OrderIdx>>,

    /// Book statistics.
    book_statistics: BookStatistics,

    /// Pre-trade risk state.
    risk_state: RiskState,

    /// Last trade price.
    last_trade_price: Option<Price>,

    /// Flag indicating if there was a trade.
    has_traded: bool,

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

    /// Clock source for ms.
    clock: Box<dyn Clock>,

    /// The market symbol.
    symbol: Symbol,

    /// Listeners push events to external systems, they are not blocking.

    /// Listens to possible trades when an order is added.
    trade_listener: Option<TradeListener>,

    /// Price level change listener.
    price_level_changed_listener: Option<PriceLevelChangedListener>,

    /// Order status listener.
    order_status_listener: Option<OrderStatusListener>,

    /// Statistic listener.
    statistic_listener: Option<StatisticListener>,
}
