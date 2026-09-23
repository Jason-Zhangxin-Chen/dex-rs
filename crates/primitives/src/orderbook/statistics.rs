//! Statistics for price level and order book.

use crate::value::{Price, TimestampMs};
use serde::{Deserialize, Serialize};

/// The statistics of the price level, it helps liquidity distribution analysis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceLevelStatistics {
    /// Price
    price: Price,

    /// Number of orders added.
    orders_added: usize,

    /// Number of orders removed.
    orders_removed: usize,

    /// Number of orders executed.
    orders_executed: usize,

    /// Total quantity executed.
    quantity_executed: usize,

    /// Total value executed.
    value_executed: u64,

    /// Last execution timestamp.
    last_execution_time: TimestampMs,

    /// Statistics initialization timestamp (set at construction / reset).
    /// Not updated on order arrival — see first_arrival_time().
    first_arrival_time: TimestampMs,

    /// Sum of waiting times for orders
    sum_waiting_time: TimestampMs,
}

/// Book statistics aggregated from price levels.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookStatistics {
    /// Number of orders added.
    orders_added: usize,

    /// Number of orders removed.
    orders_removed: usize,

    /// Number of orders executed.
    orders_executed: usize,

    /// Total quantity executed.
    quantity_executed: usize,

    /// Total value executed.
    value_executed: u64,
}
