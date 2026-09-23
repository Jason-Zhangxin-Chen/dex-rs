//! Messages to be handled by OMS services.
//! # User plane: new order, cancel order.
//! # Control plane: pause, resume, kill.
//! # Topic: `OMS_$Symbol`, use the market symbol to identify the binding OMS on the wire.

use crate::events::control_ev::ControlEvent;
use crate::events::order_ev::{CancelOrderEvent, NewOrderEvent};
use serde::{Deserialize, Serialize};

/// MsgOMS defines the msgs to be handled by OMS services.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MsgOMS {
    /// New order event.
    NewOrder(NewOrderEvent),
    /// Cancel order event.
    CancelOrder(CancelOrderEvent),
    /// Control plane event.
    Control(ControlEvent),
}

/// Msg codes of OMS services.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MsgCodeOMS {
    NewOrder = 1,
    CancelOrder = 2,
    Control = 3,
}
