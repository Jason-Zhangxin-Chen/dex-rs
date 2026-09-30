//! Orderbook definitions.

use crate::address::Address;
use crate::base::{Nonce, Symbol};
use crate::clock::Clock;
use crate::message::hot_path::{CancelOrder, OrderMsg, Trade};
use crate::message::side_path::{OrderChange, ReplicationMsg};
use crate::order::{Order, OrderIdx, OrderNode};
use crate::orderbook::config::BookConfig;
use crate::orderbook::listener::Listeners;
use crate::orderbook::price_level::PriceLevel;
use crate::orderbook::risk::RiskState;
use crate::orderbook::statistics::BookStatistics;
use crate::value::Price;
use cache::object_pool::Cache;
use litemap::LiteMap;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use slab::Slab;

/// Orderbook errors defines the runtime errors of the book.
pub enum OrderBookErr {}

/// OrderBook
pub struct OrderBook {
    /// Pre-allocated memory pools to avoid runtime heap allocation.
    memory_pools: MemoryPools,

    /// BookConfigs of the orderbook.
    config: BookConfig,

    /// The core state of the book, it should be recoverable from disaster.
    /// The oms take snapshot of it and store to an append only journal.
    /// With the message offset in wire protocols and the snapshot, the recovery
    /// is base on a snapshot + delta process to rebuild the state of the book.
    state: OrderBookState,

    /// Clock source for ms.
    clock: Box<dyn Clock>,

    /// Listeners push book changes to the remote component for state replication.
    listeners: Listeners,
}

impl OrderBook {
    /// Execute is ran by OMS_Master to execute the ingress request from user.
    /// The listener callback will emit change events and trade events for the
    /// downstream components.
    pub fn execute(&mut self, input: &OrderMsg) -> Result<(), OrderBookErr> {
        match input {
            OrderMsg::NewOrder(new) => {
                let result = self.execute_new_order(&new)?;
                self.listeners.fanout_replication_msg(&ReplicationMsg(result.0));
                if result.1.is_some() {
                    self.listeners.fanout_trade_msg(&result.1.unwrap());
                }
                Ok(())
            }
            OrderMsg::CancelOrder(cancel) => {
                let result = self.execute_cancel_order(&cancel)?;
                self.listeners.fanout_replication_msg(&ReplicationMsg(result));
                Ok(())
            }
        }
    }

    /// Apply is ran by OMS_Slave to apply the deltas replicated from the OMS_Master.
    /// The listener callback will emit changes to Redis cluster and SQL cluster.
    pub fn apply(&mut self, replicated: &ReplicationMsg) -> Result<(), OrderBookErr> {
        // todo: implement the applying of the changes to the book, the statistics are
        //  also updated during the data applying.
        Ok(())
    }

    /// execute_new_order execute the new order, it matches the best price from the opposite side
    /// price levels one by one until there is no more available cross orders or the quantity is
    /// exhausted. It calls the PriceLevel's pub method `execute` to match available orders per
    /// level and returns Vec<OrderChange> of per level, after the task done, it merges the outputs.
    fn execute_new_order(
        &mut self,
        input: &Order,
    ) -> Result<(Vec<OrderChange>, Option<Vec<Trade>>), OrderBookErr> {
        Ok((Vec::new(), None))
    }

    /// execute_cancel_order execute the cancellation an order, it removes the order from the book,
    /// and pop out it from the corresponding price level. The return contains a Vec<OrderChange>
    /// which represents the change of the book.
    fn execute_cancel_order(
        &mut self,
        input: &CancelOrder,
    ) -> Result<Vec<OrderChange>, OrderBookErr> {
        Ok(Vec::new())
    }
}

/// OrderBookState stores the runtime state of the book, it should be recoverable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookState {
    /// Symbol of the book.
    symbol: Symbol,

    /// The arena of orders, all live orders.
    arena: Slab<OrderNode>,

    /// The bids price levels, sorted by Price from high to low, pre-allocated with
    /// initial capacity.
    bids: LiteMap<Price, PriceLevel>,

    /// The asks price levels, sorted by Price from low to high, pre-allocated with
    /// initial capacity.
    asks: LiteMap<Price, PriceLevel>,

    /// Index for an order, use the hot data of an order as key for indexing.
    index: FxHashMap<(Address, Nonce), OrderIdx>,

    /// User orders. The vector<OrderIdx> is pooled in the free cache with RAII guard.
    user_orders: FxHashMap<Address, Vec<OrderIdx>>,

    /// Book statistics. OMS_Master skip this for performance, the statistic task is
    /// done by OMS_Slave which replicates the book.
    book_statistics: Option<BookStatistics>,

    /// Pre-trade risk state.
    risk_state: RiskState,

    /// Last trade price.
    last_trade_price: Option<Price>,

    /// Flag indicating if there was a trade.
    has_traded: bool,

    /// Kill switch.
    kill_switch: bool,
}

/// A set of pre-allocated memory pools for the book, it contains a pool of Vec<OrderChange>
/// which is used for state replication.
pub struct MemoryPools {
    /// Pool of Vec<OrderChange>, it is use for state replication.
    pub changes_pool: Cache<Vec<OrderChange>>,
}
