//! Orderbook definitions.

use crate::address::Address;
use crate::base::{Nonce, Symbol};
use crate::clock::Clock;
use crate::events::trade_ev::Trade;
use crate::order::{OrderIdx, OrderNode};
use crate::orderbook::config::BookConfig;
use crate::orderbook::listener::Listeners;
use crate::orderbook::price_level::PriceLevel;
use crate::orderbook::risk::RiskState;
use crate::orderbook::statistics::{BookStatistics, PriceLevelStatistics};
use crate::value::Price;
use cache::object_pool::Cache;
use litemap::LiteMap;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use slab::Slab;

// todo: impl the orderbook internal logics in this file.
//  it includes, setup of order book, matching logics, state manipulations, and event publishing.

/// OrderBook
pub struct OrderBook {
    /// Pre-allocated object pools to avoid runtime heap allocation.
    object_pools: ObjectPools,

    /// BookConfigs of the orderbook.
    config: BookConfig,

    /// The core state of the book, it should be recoverable from disaster.
    /// The oms take snapshot of it and store to an append only journal.
    /// With the message offset in wire protocols and the snapshot, the recovery
    /// is base on a snapshot + delta process to rebuild the state of the book.
    state: OrderBookState,

    /// Clock source for ms.
    clock: Box<dyn Clock>,

    /// Listeners push events to external systems, they are not blocking.
    listeners: Listeners,
}

/// OrderBookState stores the runtime state of the book, it should be recoverable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookState {
    /// Symbol of the book, to be snapshot too.
    /// It is important to check the book recovered from a snapshot has the same symbol
    /// as the configured one, it prevents miss configuration which will introduce wrong
    /// task routing in the wire protocols (Kafka / Redpanda).
    symbol: Symbol,

    /// The arena of orders, all live orders.
    arena: Slab<OrderNode>,

    /// The bids price levels, sorted by Price from high to low, pre-allocated with initial capacity.
    bids: LiteMap<Price, PriceLevel>,

    /// The asks price levels, sorted by Price from low to high, pre-allocated with initial capacity.
    asks: LiteMap<Price, PriceLevel>,

    /// Index for an order, use the hot data of an order as key for indexing.
    index: FxHashMap<(Address, Nonce), OrderIdx>,

    /// User orders. The vector<OrderIdx> is pooled in the free cache with RAII guard.
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

/// A set of pre-allocated object pools for the book, it contains:
/// # A pool of Vec<Trade> which is used for TradeResult reporting.
/// # A pool of Vec<OrderIdx> which is used for tracking per user's orders.
pub struct ObjectPools {
    /// Pool of trade list. Although the core is running within single thread, however the reporting
    /// is none blocking, thus we need multiple instance of Vec<Trade> for trade event reporting.
    /// The length of the Vec<Trade> is configurable too.
    pub trade_list_pool: Cache<Vec<Trade>>,
    /// Pool of order index list. As the risk config contains max_open_orders_per_account, thus the
    /// length of this Vec<OrderIdx> should not exceed this value. The capacity of order_idx_list_pool
    /// is determined by the number of users who are opening trades on the system, we config one
    /// initially capacity, and it grows on runtime.
    pub order_idx_list_pool: Cache<Vec<OrderIdx>>,

    /// Pool of price level statistics.
    pub price_lvl_statistic_list_pool: Cache<Vec<PriceLevelStatistics>>,
}
