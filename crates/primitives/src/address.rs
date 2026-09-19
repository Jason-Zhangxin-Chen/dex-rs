//! Address defines different blockchain's account address.

use serde::{Deserialize, Serialize};

/// Address with different L1 chain implements.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Address {
    /// Ethereum account address.
    Ethereum(EthAddress),
    /// Solana account address.
    Solana(SolAddress),
}

/// A 20-byte ethereum account address.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EthAddress(pub [u8; 20]);

/// A 32-byte solana account address.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SolAddress(pub [u8; 32]);
