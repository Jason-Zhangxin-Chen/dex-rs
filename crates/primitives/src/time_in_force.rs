//! Time in force policy of an order.

/// Specifies how long an order remains active before it is executed or expires.

#[repr(u8)]
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
