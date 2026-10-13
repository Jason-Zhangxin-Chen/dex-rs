//! Defines messages transferring between the hot path components.
//! All the message on the hotpath should be fixed sized for preallocation in share memory.
//!
//! # The shared memory `Pod` contract
//!
//! The messages travel through the share memory queues of the `ipc` crate as raw
//! byte images (`SpscQueue<T>` requires `T: bytemuck::Pod`), so every message
//! type here has a fixed `#[repr(C)]` / `#[repr(C, u8)]` layout and no pointers,
//! references, `Vec`s, `String`s or anything else holding a process-specific
//! address. Both processes of a queue must run the same binary — the queue
//! header's magic and version fields reject files written by an older or
//! foreign layout, see `crates/ipc/src/mmap_spsc_fixed.rs`.
//!
//! The data carrying enums implement `Pod` manually (bytemuck's derive only
//! supports `#[repr(C)]` structs). The manual impls are sound under the
//! queue discipline: a queue slot is read only after the producer wrote a
//! valid value into it and published its index with a release store, so a
//! read never observes an invalid discriminant. The bytes of a slot must
//! never be fabricated from an arbitrary source; file corruption is caught
//! at open by the queue's magic, version, size and capacity validation.
use crate::address::Address;
use crate::base::{Hash32, Nonce, Symbol};
use crate::message::side_path::CancelReason;
use crate::order::Order;
use crate::signature::Signature;
use crate::value::{Price, Quantity, TimestampMs};
use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};

/// Messages the ['SVD_Pretrade'] forwards to the [`SVD_OMS_Master`] via the
/// share memory SPSC queue: the validated user requests and the
/// settlement-driven removals. The messages are fixed sized for
/// preallocation in share memory.
#[repr(C, u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Zeroable)]
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

// SAFETY: see the `Pod` contract of this module.
unsafe impl Pod for OrderMsg {}

/// CancelOrder defines the data required for cancelling an order.
///
/// `Pod` is implemented manually: the struct is `#[repr(C)]` but the 20-byte
/// `Address` and the 65-byte `Signature` fields leave padding bytes, which
/// the derive rejects.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, Zeroable)]
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

// SAFETY: `CancelOrder` is `#[repr(C)]`; every field is `Pod` and the struct
// contains no pointers, references or other process-specific addresses. The
// padding bytes are initialized by the constructors writing the whole value.
unsafe impl Pod for CancelOrder {}

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
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Pod, Zeroable)]
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
#[repr(C, u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Zeroable)]
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

// SAFETY: see the `Pod` contract of this module.
unsafe impl Pod for PipelineMsg {}

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

    /// The queue messages are shared between processes as raw byte images
    /// (`SpscQueue<T>` requires `T: Pod`): copying a value's bytes into
    /// another process's address space must reproduce the value exactly.
    #[test]
    fn test_messages_are_plain_old_data() {
        let messages = [
            PipelineMsg::User(OrderMsg::NewOrder(order(1, 7))),
            PipelineMsg::User(OrderMsg::CancelBySettlement {
                user: Address([1; 20]),
                nonce: Nonce(3),
                reason: CancelReason::SettlementFailed,
            }),
            PipelineMsg::RestoreOrder { order: order(2, 8), quantity: Quantity(3) },
        ];
        for message in messages {
            // An aligned scratch slot, like the pre-allocated queue slots the
            // messages travel through.
            let mut slot: Box<PipelineMsg> = Box::new(Zeroable::zeroed());
            bytemuck::bytes_of_mut(&mut *slot).copy_from_slice(bytemuck::bytes_of(&message));
            assert_eq!(*slot, message);
        }

        // The zero image is the default message: the first variant of the
        // first order kind.
        let zeroed: PipelineMsg = Zeroable::zeroed();
        assert_eq!(zeroed, PipelineMsg::User(OrderMsg::NewOrder(Order::default())));
    }
}
