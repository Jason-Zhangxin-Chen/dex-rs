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

/// Default replenish amount of a reserve order in quantity units, used when
/// [`OrderKind::ReserveOrder`]'s `replenish_amount` is `None`.
pub const DEFAULT_RESERVE_REPLENISH_AMOUNT: u64 = 1;

/// Order represents the trade intent of the user.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Order {
    /// Hot data for cache line friendly loading.
    pub hot: OrderHot,
    /// Cold data of order.
    pub cold: OrderCold,
}

impl Order {
    /// Constructs an order from its hot and cold parts.
    pub fn new(hot: OrderHot, cold: OrderCold) -> Self {
        Self { hot, cold }
    }

    /// The hidden quantity of the order, zero for the kinds without a hidden reserve.
    pub fn hidden_quantity(&self) -> Quantity {
        match self.cold.kind {
            OrderKind::Iceberg { hidden_quantity } => hidden_quantity,
            OrderKind::ReserveOrder { hidden_quantity, .. } => hidden_quantity,
            _ => Quantity::ZERO,
        }
    }

    /// The total remaining quantity of the order, visible plus hidden.
    pub fn total_quantity(&self) -> Quantity {
        Quantity(self.hot.quantity.0 + self.hidden_quantity().0)
    }

    /// Sets the hidden quantity of an iceberg or reserve order; a no-op for
    /// the kinds without a hidden reserve.
    pub fn set_hidden_quantity(&mut self, quantity: Quantity) {
        match &mut self.cold.kind {
            OrderKind::Iceberg { hidden_quantity } => *hidden_quantity = quantity,
            OrderKind::ReserveOrder { hidden_quantity, .. } => *hidden_quantity = quantity,
            _ => {}
        }
    }
}

/// Impl the `From` trait thus that we can convert the OrderNode into Order for trade result.
impl From<OrderNode> for Order {
    fn from(o: OrderNode) -> Self {
        Self { hot: o.hot, cold: o.cold }
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

impl OrderNode {
    /// The previous order node in the time priority queue, [`NIL`] when the
    /// node is the queue head or not queued.
    pub fn prev(&self) -> OrderIdx {
        self.prev
    }

    /// The next order node in the time priority queue, [`NIL`] when the node
    /// is the queue tail or not queued.
    pub fn next(&self) -> OrderIdx {
        self.next
    }

    /// Sets the previous link.
    pub fn set_prev(&mut self, prev: OrderIdx) {
        self.prev = prev;
    }

    /// Sets the next link.
    pub fn set_next(&mut self, next: OrderIdx) {
        self.next = next;
    }

    /// The hidden quantity of the order, zero for the kinds without a hidden reserve.
    pub fn hidden_quantity(&self) -> Quantity {
        Order::from(self.clone()).hidden_quantity()
    }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct OrderHot {
    /// (Address, Nonce) works as the key pointing to an order in the book.
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

/// OrderCold contains cold data of an order.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct OrderCold {
    /// Common cold data of an order.
    pub common: OrderColdCommon,
    /// Order type specific data.
    pub kind: OrderKind,
}

/// Common code data for order.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
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

impl OrderColdCommon {
    /// Constructs the common cold data of an order.
    pub fn new(id: Hash32, symbol: Symbol, signature: Signature, timestamp: TimestampMs) -> Self {
        Self { id, symbol, signature, timestamp }
    }

    /// The timestamp of the order creation in milliseconds.
    pub fn timestamp(&self) -> TimestampMs {
        self.timestamp
    }

    /// The symbol of the order.
    pub fn symbol(&self) -> Symbol {
        self.symbol
    }

    /// The signature of the order.
    pub fn signature(&self) -> Signature {
        self.signature
    }
}

impl OrderCold {
    /// Constructs the cold data of an order.
    pub fn new(common: OrderColdCommon, kind: OrderKind) -> Self {
        Self { common, kind }
    }
}

/// OrderCold represents cold data of an order, it includes some common data and some type specific
/// data fields.
#[repr(C, u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum OrderKind {
    /// Standard limit order.
    #[default]
    Standard,

    /// Iceberg order with hidden quantities.
    Iceberg { hidden_quantity: Quantity },

    /// Post only order that won't match immediately.
    PostOnly,

    /// Trailing stop order that adjusts with market movement
    TrailingStop {
        /// Amount to trail the market price, in price ticks.
        trail_amount: Price,

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

#[cfg(test)]
mod tests {
    use super::*;
    use rmp_serde::{from_slice, to_vec};

    // ---------------------------------------------------------------
    // OrderKind wire format
    // ---------------------------------------------------------------

    #[test]
    fn test_trailing_stop_roundtrip() {
        let kind = OrderKind::TrailingStop { trail_amount: Price(10), last_ref_price: Price(100) };
        let bytes = to_vec(&kind).unwrap();
        let restored: OrderKind = from_slice(&bytes).unwrap();
        assert_eq!(kind, restored);
    }

    #[test]
    fn test_trailing_stop_trail_amount_wire_format_is_raw_price_ticks() {
        // The trail amount is a price distance carried as a bare u64 on the
        // wire. The same variant with the trail expressed through the old
        // Quantity newtype encodes identically: both newtypes are
        // transparent u64s, so the field type change is wire-compatible.
        let new_shape =
            OrderKind::TrailingStop { trail_amount: Price(10), last_ref_price: Price(100) };

        #[derive(Serialize)]
        enum OldShape {
            TrailingStop { trail_amount: Quantity, last_ref_price: Price },
        }
        let old_shape =
            OldShape::TrailingStop { trail_amount: Quantity(10), last_ref_price: Price(100) };

        assert_eq!(to_vec(&new_shape).unwrap(), to_vec(&old_shape).unwrap());
    }
}
