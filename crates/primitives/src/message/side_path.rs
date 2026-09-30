//! Sidepath messages define the messages being transferred between [`SVD_OMS_Master`] and
//! [`SVD_OMS_Slave`], the state replication depends on the messages to replicate the changes.

use crate::order::Order;
use crate::value::Quantity;
use serde::{Deserialize, Serialize};

/// Replication message contains the changes of the book triggered by an ingress OrderMsg.
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationMsg(pub Vec<OrderChange>);

/// Order state event carries the changes of an order.
#[repr(C)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderChange {
    /// The last order state before the change.
    order: Order,
    /// The new state of the order.
    status: OrderStatus,
}

impl OrderChange {
    /// Creates a new order state event.
    pub fn new(order: Order, status: OrderStatus) -> Self {
        Self { order, status }
    }

    /// The last order state before the change.
    pub fn order(&self) -> &Order {
        &self.order
    }

    /// The new state of the order.
    pub fn status(&self) -> &OrderStatus {
        &self.status
    }

    /// Sets the order that changes its status.
    pub fn with_order(mut self, order: Order) -> Self {
        self.order = order;
        self
    }

    /// Sets the new state of the order.
    pub fn with_status(mut self, status: OrderStatus) -> Self {
        self.status = status;
        self
    }
}

/// Order status for lifecycle tracking.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    /// Order accepted in the book, no fills yet.
    Open,
    /// Order partially filled, remainder still resting in the book.
    PartiallyFilled {
        /// Filled quantity.
        filled_quantity: Quantity,
    },
    /// The order is off from the book, nothing remaining.
    Filled {
        /// Filled quantity.
        filled_quantity: Quantity,
    },
    /// Order canceled.
    Canceled {
        /// Quantity filled before cancellation.
        filled_quantity: Quantity,
        /// Reason for cancellation.
        reason: CancelReason,
    },
    /// Order rejected.
    Rejected {
        /// Reason.
        reason: RejectReason,
    },
}

/// Reason for order cancellation.
///
/// Each variant identifies the specific mechanism that triggered the
/// cancellation, enabling upstream services to provide detailed
/// notifications to clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum CancelReason {
    /// Cancelled by explicit user request via `cancel_order`.
    UserRequested,
    /// Cancelled by Self-Trade Prevention logic.
    SelfTradePrevention,
    /// Cancelled because the order's time-in-force expired.
    TimeInForceExpired,
    /// Cancelled by `cancel_all_orders`.
    MassCancelAll,
    /// Cancelled by `cancel_orders_by_side`.
    MassCancelBySide,
    /// Cancelled by `cancel_orders_by_user`.
    MassCancelByUser,
    /// Cancelled by `cancel_orders_by_price_range`.
    MassCancelByPriceRange,
    /// IOC or FOK order could not be fully filled.
    InsufficientLiquidity,
}

/// Closed taxonomy of reasons an order may be rejected at admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[repr(u16)]
pub enum RejectReason {
    /// New flow rejected because the operational kill switch is engaged.
    KillSwitchActive = 1,
    /// Per-account open-order limit would be breached by this admission.
    RiskMaxOpenOrders = 2,
    /// Per-account notional limit would be breached by this admission.
    RiskMaxNotional = 3,
    /// Submitted price exceeds the configured price band against the
    /// reference price.
    RiskPriceBand = 4,
    /// Post-only order would cross the resting opposite side at the
    /// time of admission.
    PostOnlyWouldCross = 5,
    /// Self-trade prevention rejected the incoming order.
    SelfTradePrevention = 6,
    /// Submitted price violates the configured tick-size validation.
    InvalidPrice = 7,
    /// Submitted quantity violates the configured lot-size validation.
    InvalidQuantity = 8,
    /// The targeted price level is invalid for the requested operation.
    InvalidPriceLevel = 9,
    /// Submitted quantity is outside the configured min/max range.
    OrderSizeOutOfRange = 10,
    /// `user_id` is missing or zero while STP is enabled.
    MissingUserId = 11,
    /// An order with the same id is already present in the book.
    DuplicateOrderId = 12,
    /// The order could not be filled with the available resting depth
    /// (IOC / FOK semantics).
    InsufficientLiquidity = 13,
    /// A cancel-then-add modify was refused because re-adding the order
    /// would exhaust a non-auto-replenishing reserve's visible tranche and
    /// discard its hidden remainder (#230).
    ReserveResidualWouldBeDiscarded = 14,
    /// Caller-supplied / unmapped code. The library never emits this
    /// variant; it exists so applications can ferry their own reject
    /// codes through the same channel without forking the enum.
    Other(u16),
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmp_serde::{from_slice, to_vec};

    // ---------------------------------------------------------------
    // Wire format
    // ---------------------------------------------------------------

    #[test]
    fn test_replication_msg_wire_format_is_transparent() {
        // ReplicationMsg is a transparent newtype over the change vector: the
        // pooled listener payload (the bare vector) encodes identically.
        let changes = Vec::<OrderChange>::new();
        let bytes = to_vec(&ReplicationMsg(changes.clone())).unwrap();
        assert_eq!(bytes, to_vec(&changes).unwrap());
    }

    #[test]
    fn test_replication_msg_roundtrip() {
        let msg = ReplicationMsg(Vec::new());
        let bytes = to_vec(&msg).unwrap();
        let restored: ReplicationMsg = from_slice(&bytes).unwrap();
        assert_eq!(msg, restored);
    }
}
