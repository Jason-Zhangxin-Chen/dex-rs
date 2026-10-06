//! Naming of the Redis resources of the pre-trade side path. Every resource
//! name derives from the symbol or the account address, so the services wire
//! to each other without shared registries.

use primitives::address::Address;
use primitives::base::Symbol;

/// The channel the [SVD_Sync] publishes the margin states to (global, not
/// per symbol).
pub const MARGIN_CHANNEL: &str = "svd:sync:margin";
/// The prefix of the per-account margin state keys.
const MARGIN_KEY_PREFIX: &str = "svd:sync:margin";
/// The prefix of the settlement result channels.
const SETTLEMENT_PREFIX: &str = "svd:stl";

/// The Redis key of the latest margin state of an account — the on-demand
/// read path of the first-attach pull.
pub fn margin_account_key(account: Address) -> String {
    format!("{MARGIN_KEY_PREFIX}:{}", account.hex())
}

/// The Redis channel the [SVD_Settlement] publishes the settlement results
/// of the symbol to.
pub fn settlement_channel(symbol: Symbol) -> String {
    format!("{SETTLEMENT_PREFIX}:{}:settlements", symbol.hex())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(settlement_channel(symbol), format!("svd:stl:{}:settlements", symbol.hex()));
    }

    #[test]
    fn test_margin_channel_is_global() {
        assert_eq!(MARGIN_CHANNEL, "svd:sync:margin");
    }
}
