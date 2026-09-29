//! Orderbook definitions.

use crate::address::Address;
use crate::base::{Nonce, Symbol};
use crate::clock::Clock;
use crate::message::side_path::OrderChange;
use crate::order::{OrderIdx, OrderNode};
use crate::orderbook::config::BookConfig;
use crate::orderbook::listener::Listeners;
use crate::orderbook::price_level::PriceLevel;
use crate::orderbook::risk::RiskState;
use crate::value::Price;
use cache::object_pool::Cache;
use litemap::LiteMap;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use slab::Slab;
use crate::orderbook::statistics::BookStatistics;

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

/// A set of pre-allocated object pools for the book, it contains a pool of Vec<OrderChange>
/// which is used for state replication from OMS_Master to OMS_Slave via NATS.
pub struct ObjectPools {
    /// Pool of Vec<OrderChange>, it is use for state replication from OMS_Master to OMS_Slave.
    pub changes_pool: Cache<Vec<OrderChange>>,
}
