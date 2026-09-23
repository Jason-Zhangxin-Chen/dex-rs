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

impl OrderStateEvent {
    /// Creates a new order state event.
    pub fn new(order: Order, status: OrderStatus) -> Self {
        Self { order, status }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::test_utils::sample_order;
    use crate::value::Quantity;

    #[test]
    fn test_order_state_event_constructor() {
        let order = sample_order(1);
        let event = OrderStateEvent::new(order.clone(), OrderStatus::Open);
        assert_eq!(event.order, order);
        assert!(matches!(event.status, OrderStatus::Open));
    }

    #[test]
    fn test_order_state_event_with_setters() {
        let event = OrderStateEvent::new(sample_order(1), OrderStatus::Open)
            .with_order(sample_order(2))
            .with_status(OrderStatus::Filled { filled_quantity: Quantity(9) });
        assert_eq!(event.order, sample_order(2));
        assert!(matches!(event.status, OrderStatus::Filled { filled_quantity: Quantity(9) }));
    }
}
