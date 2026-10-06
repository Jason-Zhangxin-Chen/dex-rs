//! Address defines ethereum account address.

use std::fmt::Write;

use serde::{Deserialize, Serialize};

/// Address in EVM compatible protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Address(pub [u8; 20]);

impl Address {
    /// Formats the address as 40 lowercase hex characters.
    pub fn hex(&self) -> String {
        let mut out = String::with_capacity(40);
        for byte in self.0 {
            let _ = write!(out, "{byte:02x}");
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_address_hex() {
        assert_eq!(Address([0xabu8; 20]).hex(), "ab".repeat(20));
    }
}
