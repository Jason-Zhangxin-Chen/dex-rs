//! Orderbook definitions.

use crate::address::Address;
use crate::base::{Nonce};
use crate::clock::Clock;
use crate::event::PriceLevelChangedEvent;
use crate::order::{Order, OrderIdx, OrderNode};
use crate::orderbook::order_status::OrderStatus;
use crate::orderbook::price_level::PriceLevel;
use crate::orderbook::risk::RiskState;
use crate::orderbook::statistics::{BookStatistics, PriceLevelStatistics};
use crate::trade::TradeResult;
use crate::value::{Price};
use slab::Slab;
use litemap::LiteMap;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use crate::orderbook::config::BookConfig;

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
    /// BookConfigs of the orderbook.
    config: BookConfig,

    /// The core state of the book, it should be recoverable from disaster.
    state: OrderBookState,

    /// Clock source for ms.
    clock: Box<dyn Clock>,

    /// Listeners push events to external systems, they are not blocking.
    listeners: Listeners,
}

/// Listeners collect a set of callback closure to notify engine event to external system.
/// They are none blocking functions.
pub struct Listeners {
    /// Trade listener listens to possible trades when an order is added.
    trade_listener: Option<TradeListener>,

    /// Price level change listener listens to price level changes and push it to external system.
    price_level_changed_listener: Option<PriceLevelChangedListener>,

    /// Order status listener listens to order status and push it to external system.
    order_status_listener: Option<OrderStatusListener>,

    /// Statistic listener listens to the statistic changes event and push it to external system.
    statistic_listener: Option<StatisticListener>,
}

/// OrderBookState stores the runtime state of the book, it should be recoverable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookState {
    /// The arena of orders, all live orders.
    arena: Slab<OrderNode>,

    /// The bids price levels, sorted by Price from high to low, pre-allocated with initial capacity.
    bids: LiteMap<Price, PriceLevel>,

    /// The asks price levels, sorted by Price from low to high, pre-allocated with initial capacity.
    asks: LiteMap<Price, PriceLevel>,

    /// Index for an order, use the hot data of an order as key for indexing.
    index: FxHashMap<(Address, Nonce), OrderIdx>,

    /// User orders. todo: use TLS vector pool for this to avoid allocation.
    user_orders: FxHashMap<Address, Vec<OrderIdx>>,

    /// Book statistics.
    book_statistics: BookStatistics,

    /// Pre-trade risk state.
    risk_state: RiskState,

    /// Last trade price.
    last_trade_price: Option<Price>,

    /// Flag indicating if there was a trade.
    has_traded: bool,

    /// Kill switch.
    kill_switch: bool,
}