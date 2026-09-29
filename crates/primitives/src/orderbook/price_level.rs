//! The definition of a price level.

use crate::base::Side;
use crate::message::side_path::OrderChange;
use crate::order::{NIL, Order, OrderIdx};
use crate::orderbook::statistics::PriceLevelStatistics;
use crate::value::{Price, Quantity};
use serde::{Deserialize, Serialize};

pub enum PriceLevelError {}

/// A price level in a limit order book, lock-free on the match path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    /// The order queue in time priority.
    orders: OrderQueue,

    /// Total visible quantity of this price level.
    visible_quantity: Quantity,

    /// Total hidden quantity of this price level.
    hidden_quantity: Quantity,

    /// The Side: Buy or Sell.
    side: Side,

    /// The statistics of the price level. OMS_Master skip this for performance, the statistic
    /// task is done by OMS_Slave which replicates the book.
    stats: Option<PriceLevelStatistics>,

    /// The price of the level.
    price: Price,
}

/// Builders for the price level. They are used for creating a new price level by the orderbook when
/// a new order ingested and there is no corresponding price level to store it. In such case, the
/// book have to construct the price level with Price, and stats which counts this ingesting order,
/// Side, hidden quantity if this order contains hidden quantity, visible quantity of this ingesting
/// one and the OrderQueue which contains this ingesting order:
/// {tail: NIL, head: ingesting order IDX, len: 1}
impl PriceLevel {
    /// execute an order over this level, call by OMS_Master. It generates changes of the level.
    pub fn execute(&mut self) -> Result<Vec<OrderChange>, PriceLevelError> {
        // todo: implement this
        Ok(Vec::new())
    }

    /// apply changes to the price level, only ran by the OMS_Slave to replicate the state.
    pub fn apply(&mut self) -> Result<(), PriceLevelError> {
        // todo: implement this
        Ok(())
    }

    /// Sets the price of the level.
    pub fn with_price(mut self, price: Price) -> Self {
        self.price = price;
        self
    }

    /// Sets the stats of the level.
    pub fn with_stats(mut self, stats: Option<PriceLevelStatistics>) -> Self {
        self.stats = stats;
        self
    }

    /// Sets the side of the level.
    pub fn with_size(mut self, side: Side) -> Self {
        self.side = side;
        self
    }

    /// Sets the hidden quantity.
    pub fn with_hidden_quantity(mut self, quantity: Quantity) -> Self {
        self.hidden_quantity = quantity;
        self
    }

    /// Sets the visible quantity.
    pub fn with_visible_quantity(mut self, quantity: Quantity) -> Self {
        self.visible_quantity = quantity;
        self
    }

    /// Sets the order queue.
    pub fn with_orders(mut self, orders: OrderQueue) -> Self {
        self.orders = orders;
        self
    }
}

/// OrderQueue in time priority, and fast operations.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OrderQueue {
    /// The newest order.
    tail: OrderIdx,

    /// The oldest order next to be matched.
    head: OrderIdx,

    /// The size of the queue.
    len: usize,
}

impl Default for OrderQueue {
    fn default() -> Self {
        Self { tail: NIL, head: NIL, len: 0 }
    }
}
