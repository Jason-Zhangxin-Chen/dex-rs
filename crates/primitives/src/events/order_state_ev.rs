//! Event of order status.

use crate::order::Order;
use crate::orderbook::order_status::OrderStatus;
use serde::{Deserialize, Serialize};

/// Order state event carries the changes of an order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderStateEvent {
    /// The order that changes its status.
    order: Order,
    /// The new state of the order.
    status: OrderStatus,
}
