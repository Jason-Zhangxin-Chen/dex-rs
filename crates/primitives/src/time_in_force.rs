//! Time in force policy of an order.

use serde::{Deserialize, Serialize};

/// Specifies how long an order remains active before it is executed or expires.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    /// Good till canceled.
    Gtc,
    /// Immediate or canceled.
    Ioc,
    /// Fill or Kill.
    Fok,
    /// Good till date, the u8 carries the lifetime of the order in hours.
    Gtd(u8),
    /// Good for the trading day.
    Day,
}
