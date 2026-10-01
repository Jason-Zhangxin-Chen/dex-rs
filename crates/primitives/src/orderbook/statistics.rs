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

    /// Number of orders removed, it should include cancelled and expired.
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

impl PriceLevelStatistics {
    /// Constructs the statistics of a newly created price level.
    pub fn new(price: Price, now: TimestampMs) -> Self {
        Self { price, first_arrival_time: now, ..Default::default() }
    }

    /// Records an order added to the level.
    pub(crate) fn record_added(&mut self) {
        self.orders_added += 1;
    }

    /// Records an order removed from the level.
    pub(crate) fn record_removed(&mut self) {
        self.orders_removed += 1;
    }

    /// Records an execution at the level.
    pub(crate) fn record_executed(&mut self, quantity: usize, value: u64, now: TimestampMs) {
        self.orders_executed += 1;
        self.quantity_executed += quantity;
        self.value_executed += value;
        self.last_execution_time = now;
    }
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

impl BookStatistics {
    /// Records an order added to the book.
    pub fn record_added(&mut self) {
        self.orders_added += 1;
    }

    /// Records an order removed from the book.
    pub fn record_removed(&mut self) {
        self.orders_removed += 1;
    }

    /// Records an executed order: `quantity` units filled for `value` quote.
    pub fn record_executed(&mut self, quantity: usize, value: u64) {
        self.orders_executed += 1;
        self.quantity_executed += quantity;
        self.value_executed += value;
    }

    /// Number of orders added.
    pub fn orders_added(&self) -> usize {
        self.orders_added
    }

    /// Number of orders removed.
    pub fn orders_removed(&self) -> usize {
        self.orders_removed
    }

    /// Number of orders executed.
    pub fn orders_executed(&self) -> usize {
        self.orders_executed
    }

    /// Total quantity executed.
    pub fn quantity_executed(&self) -> usize {
        self.quantity_executed
    }

    /// Total value executed.
    pub fn value_executed(&self) -> u64 {
        self.value_executed
    }
}
