//! The definition of a price level.

use crate::base::Side;
use crate::order::OrderIdx;
use crate::orderbook::statistics::PriceLevelStatistics;
use crate::value::{Price, Quantity};
use serde::{Deserialize, Serialize};

// todo: impl the price level internal logics in this file.
//  it include the setup of it, match logic inside the level.

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

    /// The statistics of the price level.
    stats: PriceLevelStatistics,

    /// The price of the level.
    price: Price,
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
