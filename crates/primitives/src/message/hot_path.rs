//! Defines messages transferring between the hot path components.
//! All the message on the hotpath should be fixed sized for preallocation in share memory.
//!
use crate::address::Address;
use crate::base::{Hash32, Nonce, Symbol};
use crate::order::Order;
use crate::signature::Signature;
use crate::value::{Price, Quantity, TimestampMs};
use serde::{Deserialize, Serialize};

/// Messages sent from user end.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OrderMsg {
    /// New Order.
    NewOrder(Order),
    /// Cancel Order.
    CancelOrder(CancelOrder),
}

/// CancelOrder defines the data required for cancelling an order.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
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

/// Message sent from [`SVD_OMS_Master`] to [`SVD_OMS_Settlement`].
/// Trade defines the exchange between two orders, a matching can generate multiple
/// Trades for an ingress order. The [`SVD_Settlement`] batches it and submit them to
/// Settlement protocol contract.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Trade {
    /// Taker order, the ingress order.
    pub taker: Order,
    /// The remaining quantity of the ingress order.
    pub taker_remaining: Quantity,
    /// Maker order.
    pub maker: Order,
    /// Price at which the trade happens.
    pub price: Price,
    /// Traded quantity of this cross.
    pub traded_quantity: Quantity,
}

impl Trade {
    /// Create a new trade.
    pub fn new(
        taker: Order,
        taker_remaining: Quantity,
        maker: Order,
        price: Price,
        traded_quantity: Quantity,
    ) -> Self {
        Self { taker, taker_remaining, maker, price, traded_quantity }
    }

    pub fn with_price(mut self, price: Price) -> Self {
        self.price = price;
        self
    }

    pub fn with_taker(mut self, taker: Order) -> Self {
        self.taker = taker;
        self
    }

    pub fn with_maker(mut self, maker: Order) -> Self {
        self.maker = maker;
        self
    }

    pub fn with_taker_remaining(mut self, taker_remaining: Quantity) -> Self {
        self.taker_remaining = taker_remaining;
        self
    }

    pub fn with_traded_quantity(mut self, traded_quantity: Quantity) -> Self {
        self.traded_quantity = traded_quantity;
        self
    }
}
