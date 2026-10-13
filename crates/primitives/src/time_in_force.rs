//! Time in force policy of an order.

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};

/// Specifies how long an order remains active before it is executed or expires.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, Zeroable)]
pub enum TimeInForce {
    /// Good till canceled.
    #[default]
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

// SAFETY: the queue discipline of [`crate::message::hot_path`] guarantees a
// shared memory slot is only ever read after the producer wrote a valid
// `TimeInForce` into it, so an invalid discriminant is never observed.
unsafe impl Pod for TimeInForce {}
