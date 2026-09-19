//! Time in force policy of an order.

use serde::{Deserialize, Serialize};

/// Specifies how long an order remains active before it is executed or expires.

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    /// Good till canceled.
    Gtc,
    /// Immediate or canceled.
    Ioc,
    /// Fill or Kill.
    Fok,
    /// Good till date.
    Gtd(u64),
    /// Good for the trading day.
    Day,
}
