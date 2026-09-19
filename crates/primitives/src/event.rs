//! events for the workspace.

use crate::address::Address;
use crate::base::{Hash32, Symbol};
use crate::order::Order;
use crate::signature::Signature;
use crate::value::TimestampMs;
use serde::{Deserialize, Serialize};

/// New Order event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewOrder {
    /// The order signed by the client.
    order: Order,
}

/// Cancel Order event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelOrder {
    /// The symbol.
    symbol: Symbol,
    /// The order to be canceled.
    order_id: Hash32,
    /// The user who request the operation.
    user: Address,
    /// When the cancel operation is created.
    timestamp: TimestampMs,
    /// Signature of the operation.
    signature: Signature,
}
