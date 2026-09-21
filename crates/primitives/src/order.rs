//! The definition of orders

use crate::address::Address;
use crate::base::{Hash32, Nonce, PegReferenceType, Side, Symbol};
use crate::signature::Signature;
use crate::time_in_force::TimeInForce;
use crate::value::{Price, Quantity, TimestampMs};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// Index into the area, cast usize to u32 to make it catch line friendly since
/// 4_294_967_295 is sufficient for the book size.
pub type OrderIdx = u32;

/// NIL for order index pointer.
pub const NIL: OrderIdx = u32::MAX;

/// Order represents the trade intent of the user.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    /// Hot data for cache line friendly loading.
    pub hot: OrderHot,
    /// Cold data of order.
    pub cold: OrderCold,
}

/// Impl the `From` trait thus that we can convert the OrderNode into Order for trade result.
impl From<OrderNode> for Order {
    fn from(o: OrderNode) -> Self {
        Self {
            hot: o.hot,
            cold: o.cold,
        }
    }
}

/// OrderNode wraps the order and adds extra data for structuring the time priority.
/// It is split for cache line friendly loading.
///
/// `prev` and `next` are book-internal links used for time priority within a
/// price level. They are serialized as part of the snapshot so a restored book
/// preserves its linked-list structure exactly. On the wire (Kafka, Redpanda),
/// producers should set them to `NIL`; the engine overwrites them when the
/// order is inserted into a price level.
#[repr(C, align(64))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderNode {
    /// The hot data of order.
    pub hot: OrderHot, // 56 bytes.
    /// The last order node, only for price level time priority.
    /// It is not visible for end user and the other component.
    prev: OrderIdx, // 4 bytes.
    /// The next order node, only for price level time priority.
    /// It is not visible for end user and the other component.
    next: OrderIdx, // 4 bytes.
    /// The cold data of order.
    pub cold: OrderCold,
}

/// Impl the `From` trait thus that we can convert the Order into OrderNode when we off payload
/// from wire (Kafka, Redpanda).
impl From<Order> for OrderNode {
    fn from(o: Order) -> Self {
        Self { hot: o.hot, cold: o.cold, prev: NIL, next: NIL }
    }
}

/// OrderHot contains the core data for match engine, it is planed on purpose for cache line
/// friendly loading, the tuple (Address, Nonce) is used to index an order in the book.
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderHot {
    /// (Address, Nonce) works as the key to an order in the book.
    /// The user address.
    pub user: Address, // 20 Bytes,
    /// The order nonce.
    pub nonce: Nonce, // 8 Bytes,

    /// The price of the order.
    pub price: Price, // 8 Bytes,

    /// The visible quantity/quantity of the order.
    pub quantity: Quantity, // 8 Byte,

    /// Time in force policy.
    pub time_in_force: TimeInForce, // 2 Bytes,

    /// The side of the order.
    pub side: Side, // 1 Bytes,
}

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderCold {
    pub common: OrderColdCommon,
    pub kind: OrderKind,
}

/// Common code data for order.
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderColdCommon {
    /// The hash of the order.
    id: Hash32,

    /// Symbol of the project.
    symbol: Symbol,

    /// Signature of the order.
    signature: Signature,

    /// When the order is created.
    timestamp: TimestampMs,
}

/// OrderCold represents cold data of an order, it includes some common data and some type specific
/// data fields.
#[repr(C, u8)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderKind {
    /// Standard limit order.
    Standard,

    /// Iceberg order with hidden quantities.
    Iceberg { hidden_quantity: Quantity },

    /// Post only order that won't match immediately.
    PostOnly,

    /// Trailing stop order that adjusts with market movement
    TrailingStop {
        /// Amount to trail the market price.
        trail_amount: Quantity,

        /// Last reference price.
        last_ref_price: Price,
    },

    /// Pegged order that adjusts based on reference price
    Pegged {
        /// Offset from the reference price.
        reference_price_offset: i64,

        /// Type of reference price to track.
        reference_price_type: PegReferenceType,
    },

    /// Market-to-limit order that converts to limit after initial execution
    MarketToLimit,

    /// Reserve order with custom replenishment
    /// if `replenish_amount` is None, it uses DEFAULT_RESERVE_REPLENISH_AMOUNT
    /// if `auto_replenish` is false, and visible quantity is below threshold, it will not replenish
    /// if `auto_replenish` is false and visible quantity is zero it will be removed from the book
    /// if `auto_replenish` is true, and replenish_threshold is 0, it will use 1
    ReserveOrder {
        /// The hidden quantity of the order.
        hidden_quantity: Quantity,

        /// Threshold at which to replenish
        replenish_threshold: Quantity,

        /// Optional amount to replenish by, in quantity units. If `None`, uses
        /// [`DEFAULT_RESERVE_REPLENISH_AMOUNT`]. A replenish amount is
        /// structurally non-zero ([`NonZeroU64`]): a zero replenish would draw
        /// an empty visible tranche from hidden.
        replenish_amount: Option<NonZeroU64>,

        /// Whether to replenish automatically when below threshold.
        /// If false, only replenish on next match
        auto_replenish: bool,
    },
}
