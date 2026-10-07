//! Defines messages transferring between the hot path components.
//! All the message on the hotpath should be fixed sized for preallocation in share memory.
//!
use crate::address::Address;
use crate::base::{Hash32, Nonce, Symbol};
use crate::message::side_path::CancelReason;
use crate::order::Order;
use crate::signature::Signature;
use crate::value::{Price, Quantity, TimestampMs};
use serde::{Deserialize, Serialize};

/// Messages the ['SVD_Pretrade'] forwards to the [`SVD_OMS_Master`] via the
/// share memory SPSC queue: the validated user requests and the
/// settlement-driven removals. The messages are fixed sized for
/// preallocation in share memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderMsg {
    /// New Order.
    NewOrder(Order),
    /// Cancel Order.
    CancelOrder(CancelOrder),
    /// Removes one order of the book: a deterministic settlement failure of
    /// that order (forged signature, expired, bad price). Pushed by the
    /// settlement feed of the ['SVD_Pretrade'], not a user request.
    CancelBySettlement {
        /// The user of the order.
        user: Address,
        /// The nonce of the order.
        nonce: Nonce,
        /// Why the order is removed.
        reason: CancelReason,
    },
    /// Removes every resting order of an account: the account's margin is
    /// exhausted on-chain and it must stop trading.
    MassCancelByUser {
        /// The account to remove.
        user: Address,
        /// Why the account's orders are removed.
        reason: CancelReason,
    },
}

/// CancelOrder defines the data required for cancelling an order.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
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

    /// The user who requests the operation.
    pub fn user(&self) -> Address {
        self.user
    }

    /// The nonce of the order to be cancelled.
    pub fn nonce(&self) -> Nonce {
        self.nonce
    }

    /// The symbol of the order to be canceled.
    pub fn symbol(&self) -> Symbol {
        self.symbol
    }

    /// The id of the order to be canceled.
    pub fn order_id(&self) -> Hash32 {
        self.order_id
    }

    /// When the cancel operation is created.
    pub fn timestamp(&self) -> TimestampMs {
        self.timestamp
    }

    /// The signature of the operation.
    pub fn signature(&self) -> Signature {
        self.signature
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
/// Settlement protocol contract. The listener of the [`SVD_OMS_Master`] can fanout the
/// trade events to the downstream system for settlement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// The messages of the pipeline between [`SVD_Pretrade`] and
/// [`SVD_OMS_Master`]: the validated user requests and the settlement-driven
/// restores. Fixed sized for preallocation in the share memory queues.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineMsg {
    /// A user request (new order / cancel), validated by a handler thread.
    User(OrderMsg),
    /// Re-injects the crossed quantity of an innocent side: the trade failed
    /// to settle because of the other side, so this order's consumed
    /// quantity re-enters the book. The [`SVD_OMS_Master`] merges the
    /// quantity into the resting order when `(user, nonce)` is still in the
    /// book, and re-inserts the order at the tail of its price level when it
    /// is gone.
    RestoreOrder {
        /// The order of the innocent side, as it was in the failed trade.
        order: Order,
        /// The crossed quantity of the failed trade to re-insert.
        quantity: Quantity,
    },
}

#[cfg(test)]
mod pipeline_tests {
    use super::*;
    use crate::base::{Nonce, Side};
    use crate::order::{OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use crate::time_in_force::TimeInForce;
    use rmp_serde::{from_slice, to_vec};

    fn order(user: u8, nonce: u64) -> Order {
        Order::new(
            OrderHot {
                user: Address([user; 20]),
                nonce: Nonce(nonce),
                price: Price(100),
                quantity: Quantity(10),
                time_in_force: TimeInForce::Gtc,
                side: Side::Buy,
            },
            OrderCold::new(
                OrderColdCommon::new(
                    Hash32([0; 32]),
                    Symbol([0; 32]),
                    Signature::default(),
                    TimestampMs(0),
                ),
                OrderKind::Standard,
            ),
        )
    }

    #[test]
    fn test_pipeline_msg_user_roundtrip() {
        let msg = PipelineMsg::User(OrderMsg::NewOrder(order(1, 1)));
        let bytes = to_vec(&msg).unwrap();
        let restored: PipelineMsg = from_slice(&bytes).unwrap();
        assert_eq!(msg, restored);
    }

    #[test]
    fn test_pipeline_msg_restore_roundtrip() {
        let msg = PipelineMsg::RestoreOrder { order: order(1, 1), quantity: Quantity(7) };
        let bytes = to_vec(&msg).unwrap();
        let restored: PipelineMsg = from_slice(&bytes).unwrap();
        assert_eq!(msg, restored);
    }

    #[test]
    fn test_pipeline_msg_variants_are_distinct() {
        let user = to_vec(&PipelineMsg::User(OrderMsg::NewOrder(order(1, 1)))).unwrap();
        let restore =
            to_vec(&PipelineMsg::RestoreOrder { order: order(1, 1), quantity: Quantity(7) })
                .unwrap();
        assert_ne!(user, restore);
    }

    #[test]
    fn test_pipeline_msg_settlement_removals_roundtrip() {
        let cancel = PipelineMsg::User(OrderMsg::CancelBySettlement {
            user: Address([1; 20]),
            nonce: Nonce(3),
            reason: CancelReason::SettlementFailed,
        });
        let bytes = to_vec(&cancel).unwrap();
        assert_eq!(cancel, from_slice(&bytes).unwrap());

        let mass = PipelineMsg::User(OrderMsg::MassCancelByUser {
            user: Address([2; 20]),
            reason: CancelReason::SettlementFailed,
        });
        let bytes = to_vec(&mass).unwrap();
        assert_eq!(mass, from_slice(&bytes).unwrap());
    }
}
