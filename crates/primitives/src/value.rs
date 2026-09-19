//! Defines the value types for the exchange system.

use serde::{Deserialize, Serialize};

/// Value type representing a price.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Price(u128);

/// Quantity type representing a quantity in an order.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Quantity(u64);

/// TimestampMS representing a TS in millisecond.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TimestampMs(u64);
