//! Messages of Settlement services which are stateless.
//! # Control plane: pause, resume, kill.
//! # Topic: Settlement, sharded by symbol between wire protocol's partitions.

use crate::events::control_ev::ControlEvent;
use serde::{Deserialize, Serialize};

/// MsgPostTrade defines the msgs to be handled by PostTrade services.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MsgPreTrade {
    // todo: add more msg here.
    /// Control plane event.
    Control(ControlEvent),
}

/// Msg codes of PreTrade services.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MsgCodePreTrade {
    Control = 1,
}
