//! Naming of the Redis resources of the pre-trade side path. Every resource
//! name derives from the symbol or the account address, so the services wire
//! to each other without shared registries.

use std::fmt::Write;

use primitives::address::Address;
use primitives::base::Symbol;

/// The channel the [SVD_Sync] publishes the margin states to (global, not
/// per symbol).
pub const MARGIN_CHANNEL: &str = "svd:sync:margin";
/// The prefix of the per-account margin state keys.
const MARGIN_KEY_PREFIX: &str = "svd:sync:margin";
/// The prefix of the settlement result channels.
const SETTLEMENT_PREFIX: &str = "svd:stl";

/// Formats the symbol as 64 lowercase hex characters.
pub fn symbol_hex(symbol: Symbol) -> String {
    let mut out = String::with_capacity(64);
    for byte in symbol.0 {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Formats the account as 40 lowercase hex characters.
pub fn address_hex(account: Address) -> String {
    let mut out = String::with_capacity(40);
    for byte in account.0 {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The Redis key of the latest margin state of an account — the on-demand
/// read path of the first-attach pull.
pub fn margin_account_key(account: Address) -> String {
    format!("{MARGIN_KEY_PREFIX}:{}", address_hex(account))
}

/// The Redis channel the [SVD_Settlement] publishes the settlement results
/// of the symbol to.
pub fn settlement_channel(symbol: Symbol) -> String {
    format!("{SETTLEMENT_PREFIX}:{}:settlements", symbol_hex(symbol))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_hex() {
        let mut bytes = [0u8; 32];
        bytes[31] = 0xab;
        assert_eq!(symbol_hex(Symbol(bytes)), format!("{}ab", "0".repeat(62)));
    }

    #[test]
    fn test_address_hex() {
        assert_eq!(address_hex(Address([0xabu8; 20])), "ab".repeat(20));
    }

    #[test]
    fn test_margin_account_key_is_account_scoped() {
        let a = Address([1u8; 20]);
        let b = Address([2u8; 20]);
        assert_ne!(margin_account_key(a), margin_account_key(b));
        assert_eq!(margin_account_key(a), format!("svd:sync:margin:{}", "01".repeat(20)));
    }

    #[test]
    fn test_settlement_channel_is_symbol_scoped() {
        let symbol = Symbol([0x7au8; 32]);
        assert_eq!(
            settlement_channel(symbol),
            format!("svd:stl:{}:settlements", symbol_hex(symbol))
        );
    }

    #[test]
    fn test_margin_channel_is_global() {
        assert_eq!(MARGIN_CHANNEL, "svd:sync:margin");
    }
}
