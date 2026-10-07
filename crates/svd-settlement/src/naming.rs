//! The resource names of the settlement service, derived from the symbol
//! like the other services' naming modules.

use primitives::base::Symbol;

/// Prefix of the settlement channels.
const SETTLEMENT_PREFIX: &str = "svd:stl";

/// The Redis channel the [SVD_Settlement] publishes the `SettlementResult`
/// messages to — the same channel the pre-trade settlement feed subscribes
/// (see `svd-pretrade/src/naming.rs`; the two must stay byte-identical).
pub fn settlement_channel(symbol: Symbol) -> String {
    format!("{SETTLEMENT_PREFIX}:{}:settlements", symbol.hex())
}

/// The snapshot key placeholder of the `ChangeSink`: the settlement service
/// never saves snapshots, but the storage trait requires a key.
pub fn snapshot_key(symbol: Symbol) -> String {
    format!("{SETTLEMENT_PREFIX}:{}:snapshot", symbol.hex())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_settlement_channel_derives_from_the_symbol() {
        let symbol = Symbol([0xabu8; 32]);
        let hex = "ab".repeat(32);
        assert_eq!(settlement_channel(symbol), format!("svd:stl:{hex}:settlements"));
        assert_eq!(snapshot_key(symbol), format!("svd:stl:{hex}:snapshot"));
    }
}
