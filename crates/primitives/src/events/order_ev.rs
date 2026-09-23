//! events for the workspace.

use crate::address::Address;
use crate::base::{Hash32, Nonce, Symbol};
use crate::order::Order;
use crate::signature::Signature;
use crate::value::TimestampMs;
use serde::{Deserialize, Serialize};

/// New Order event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewOrder(Order);

impl NewOrder {
    /// Creates a new order event.
    pub fn new(order: Order) -> Self {
        Self(order)
    }

    /// Sets the order carried by the event.
    pub fn with_order(mut self, order: Order) -> Self {
        self.0 = order;
        self
    }
}

/// Cancel Order event
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

impl CancelOrder {
    /// Creates a new cancel order event.
    pub fn new(
        symbol: Symbol,
        order_id: Hash32,
        user: Address,
        nonce: Nonce,
        timestamp: TimestampMs,
        signature: Signature,
    ) -> Self {
        Self { symbol, order_id, user, nonce, timestamp, signature }
    }

    /// Sets the symbol of the order to be canceled.
    pub fn with_symbol(mut self, symbol: Symbol) -> Self {
        self.symbol = symbol;
        self
    }

    /// Sets the order to be canceled.
    pub fn with_order_id(mut self, order_id: Hash32) -> Self {
        self.order_id = order_id;
        self
    }

    /// Sets the user who requests the operation.
    pub fn with_user(mut self, user: Address) -> Self {
        self.user = user;
        self
    }

    /// Sets the nonce of the order.
    pub fn with_nonce(mut self, nonce: Nonce) -> Self {
        self.nonce = nonce;
        self
    }

    /// Sets when the cancel operation is created.
    pub fn with_timestamp(mut self, timestamp: TimestampMs) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Sets the signature of the operation.
    pub fn with_signature(mut self, signature: Signature) -> Self {
        self.signature = signature;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::test_utils::sample_order;

    // ---------------------------------------------------------------
    // NewOrder
    // ---------------------------------------------------------------

    #[test]
    fn test_new_order_constructor() {
        let order = sample_order(1);
        let event = NewOrder::new(order.clone());
        assert_eq!(event.0, order);
    }

    #[test]
    fn test_new_order_with_order() {
        let event = NewOrder::new(sample_order(1)).with_order(sample_order(2));
        assert_eq!(event.0, sample_order(2));
    }

    // ---------------------------------------------------------------
    // CancelOrder
    // ---------------------------------------------------------------

    #[test]
    fn test_cancel_order_constructor() {
        let event = CancelOrder::new(
            Symbol([1u8; 32]),
            Hash32([2u8; 32]),
            Address([3u8; 20]),
            Nonce(4),
            TimestampMs(5),
            Signature([6u8; 65]),
        );
        assert_eq!(event.symbol, Symbol([1u8; 32]));
        assert_eq!(event.order_id, Hash32([2u8; 32]));
        assert_eq!(event.user, Address([3u8; 20]));
        assert_eq!(event.nonce, Nonce(4));
        assert_eq!(event.timestamp, TimestampMs(5));
        assert_eq!(event.signature, Signature([6u8; 65]));
    }

    #[test]
    fn test_cancel_order_default() {
        let event = CancelOrder::default();
        assert_eq!(event.symbol, Symbol::default());
        assert_eq!(event.order_id, Hash32::default());
        assert_eq!(event.user, Address::default());
        assert_eq!(event.nonce, Nonce::default());
        assert_eq!(event.timestamp, TimestampMs::default());
        assert_eq!(event.signature, Signature::default());
    }

    #[test]
    fn test_cancel_order_with_setters() {
        let event = CancelOrder::default()
            .with_symbol(Symbol([1u8; 32]))
            .with_order_id(Hash32([2u8; 32]))
            .with_user(Address([3u8; 20]))
            .with_nonce(Nonce(4))
            .with_timestamp(TimestampMs(5))
            .with_signature(Signature([6u8; 65]));
        assert_eq!(event.symbol, Symbol([1u8; 32]));
        assert_eq!(event.order_id, Hash32([2u8; 32]));
        assert_eq!(event.user, Address([3u8; 20]));
        assert_eq!(event.nonce, Nonce(4));
        assert_eq!(event.timestamp, TimestampMs(5));
        assert_eq!(event.signature, Signature([6u8; 65]));
    }
}
