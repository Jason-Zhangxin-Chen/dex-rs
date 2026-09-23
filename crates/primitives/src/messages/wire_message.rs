//! Wire message is share container of messages, it contains a message code field for decoding
//! the payload of a message. With such approach, different messages can share the topics on
//! the wire protocols (Kafka or Redpanda).

use serde::{Deserialize, Serialize};

/// Wire envelope — the code + payload shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireMessage {
    pub code: u32,
    pub payload: Vec<u8>,
}

// todo: impl the shared encoding, decoding functions.
