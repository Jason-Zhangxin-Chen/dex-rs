//! The definition of a price level.

use crate::base::Side;
use crate::order::Order;
use crate::orderbook::statistics::PriceLevelStatistics;
use crate::value::{Price, Quantity};
use serde::{Deserialize, Serialize};

/// A price level in a limit order book, lock-free on the match path.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PriceLevel {
    /// The price of the level.
    price: Price,

    /// Total visible quantity of this price level.
    visible_quantity: Quantity,

    /// Total hidden quantity of this price level.
    hidden_quantity: Quantity,

    /// The Side: Buy or Sell.
    side: Side,

    /// The order queue in time priority.
    orders: OrderQueue,

    /// The statistics of the price level.
    stats: PriceLevelStatistics,
}

/// OrderQueue in time priority, and fast operations.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OrderQueue {
    /// The oldest order next to be matched.
    head: OrderIdx,
    /// The newest order.
    tail: OrderIdx,
    /// The size of the queue.
    len: usize,
}

/// Index into the area.
pub type OrderIdx = usize;

/// NIL for order index pointer.
pub const NIL: OrderIdx = usize::MAX;

/// OrderNode wrap order for queue linking.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OrderNode {
    /// The order content.
    order: Order,
    /// The last order node.
    prev: OrderIdx,
    /// The next order node.
    next: OrderIdx,
}
