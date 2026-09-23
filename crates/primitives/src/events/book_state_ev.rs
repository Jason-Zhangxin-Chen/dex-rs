use crate::base::Side;
use crate::orderbook::statistics::{BookStatistics, PriceLevelStatistics};
use crate::value::{Price, Quantity};
use serde::{Deserialize, Serialize};

/// Event data for orderbook price level changes.
/// It is assumed that the listener is aware of the
/// order book context so we are not adding symbol here.
/// This event is sent on operations that update the order book price levels
/// e.g. adding, cancelling, updating or matching order
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PriceLevelChangedEvent {
    /// the order book side of the price level
    side: Side,

    /// price level price
    price: Price,

    /// latest visible quantity of the order book at this price level
    quantity: Quantity,
}

/// Statistic event carries the orderbook statistic metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticsEvent {
    /// List of PriceLevelStatistics of the latest book. The vector<PriceLevelStatistics> is pooled
    /// in the free cache with RAII guard.
    price_level_statistics: Vec<PriceLevelStatistics>,
    /// Book Statistics.
    book_statistics: BookStatistics,
}
