//! Defines the value types for the exchange system.

/// Value type representing a price.
pub struct Price(u128);

/// Quantity type representing a quantity in an order.
pub struct Quantity(u64);

/// TimestampMS representing a TS in millisecond.
pub struct TimestampMs(u64);
