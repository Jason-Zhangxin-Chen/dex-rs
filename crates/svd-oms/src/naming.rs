//! Naming of the NATS and Redis resources of a symbol. Every resource name
//! derives from the 32-byte symbol, so services of the same symbol wire to
//! each other without shared registries.

use primitives::base::Symbol;

/// Prefix of the NATS subjects the OMS replication stream uses.
const SUBJECT_PREFIX: &str = "svd.oms";
/// Prefix of the NATS stream names the OMS replication stream uses.
const STREAM_PREFIX: &str = "svd_oms";
/// Prefix of the Redis keys the OMS side path uses.
const REDIS_PREFIX: &str = "svd:oms";

/// The NATS subject the master publishes the replication messages to.
pub fn subject_name(symbol: Symbol) -> String {
    format!("{SUBJECT_PREFIX}.{}", symbol.hex())
}

/// The NATS JetStream stream carrying the replication messages.
pub fn stream_name(symbol: Symbol) -> String {
    format!("{STREAM_PREFIX}_{}", symbol.hex())
}

/// The Redis channel the slave publishes the book state changes to.
pub fn redis_change_channel(symbol: Symbol) -> String {
    format!("{REDIS_PREFIX}:{}:changes", symbol.hex())
}

/// The Redis key the slave stores the latest snapshot under.
pub fn redis_snapshot_key(symbol: Symbol) -> String {
    format!("{REDIS_PREFIX}:{}:snapshot", symbol.hex())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_names_are_symbol_scoped() {
        let symbol = Symbol([0x7au8; 32]);
        let hex = symbol.hex();
        assert_eq!(subject_name(symbol), format!("svd.oms.{hex}"));
        assert_eq!(stream_name(symbol), format!("svd_oms_{hex}"));
        assert_eq!(redis_change_channel(symbol), format!("svd:oms:{hex}:changes"));
        assert_eq!(redis_snapshot_key(symbol), format!("svd:oms:{hex}:snapshot"));
    }

    #[test]
    fn test_distinct_symbols_never_collide() {
        let a = Symbol([1u8; 32]);
        let b = Symbol([2u8; 32]);
        assert_ne!(subject_name(a), subject_name(b));
        assert_ne!(stream_name(a), stream_name(b));
        assert_ne!(redis_change_channel(a), redis_change_channel(b));
        assert_ne!(redis_snapshot_key(a), redis_snapshot_key(b));
    }
}
