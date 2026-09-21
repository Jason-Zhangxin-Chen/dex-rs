//! events for the workspace.

use crate::address::Address;
use crate::base::{Hash32, Nonce, Side, Symbol};
use crate::order::Order;
use crate::signature::Signature;
use crate::value::{Price, Quantity, TimestampMs};
use serde::{Deserialize, Serialize};

// todo: impl builder for below types.

/// New Order event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewOrder(Order);

/// Cancel Order event
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct CancelOrder {
    /// The symbol.
    symbol: Symbol,
    /// The order to be canceled.
    order_id: Hash32,
    /// The user who request the operation.
    user: Address,
    /// The nonce of the order.
    nonce: Nonce,
    /// When the cancel operation is created.
    timestamp: TimestampMs,
    /// Signature of the operation.
    signature: Signature,
}

/// Event data for orderbook price level changes.
/// It is assumed that the listener is aware of the
/// order book context so we are not adding symbol here.
/// This event is sent on operations that update the order book price levels
/// e.g. adding, cancelling, updating or matching order
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct PriceLevelChangedEvent {
    /// the order book side of the price level
    side: Side,

    /// price level price
    price: Price,

    /// latest visible quantity of the order book at this price level
    quantity: Quantity,
}
