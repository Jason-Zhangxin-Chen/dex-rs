//! Signature defines different blockchain's signature.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::convert::TryInto;

/// The implementation of EVM compatible signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Signature(pub [u8; 65]);

impl Serialize for Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_slice().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let arr: [u8; 65] = bytes
            .try_into()
            .map_err(|v: Vec<u8>| D::Error::invalid_length(v.len(), &"expected 65 bytes"))?;
        Ok(Signature(arr))
    }
}
