//! Signature defines different blockchain's signature.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::convert::TryInto;

/// The signatures from different L1 protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Signature {
    /// Ethereum signature.
    Ethereum(EthSignature),
    /// Solana signature.
    Solana(SolSignature),
}

/// The implementation of ethereum signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EthSignature(pub [u8; 65]);

/// The implementation of solana signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SolSignature(pub [u8; 64]);

impl Serialize for EthSignature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_slice().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EthSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let arr: [u8; 65] = bytes
            .try_into()
            .map_err(|v: Vec<u8>| D::Error::invalid_length(v.len(), &"expected 65 bytes"))?;
        Ok(EthSignature(arr))
    }
}

impl Serialize for SolSignature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_slice().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SolSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let arr: [u8; 64] = bytes
            .try_into()
            .map_err(|v: Vec<u8>| D::Error::invalid_length(v.len(), &"expected 64 bytes"))?;
        Ok(SolSignature(arr))
    }
}
