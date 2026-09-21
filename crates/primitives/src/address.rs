//! Address defines ethereum account address.

use serde::{Deserialize, Serialize};

/// Address in EVM compatible protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Address(pub [u8; 20]);
