//! The book snapshot: the state of the book plus the checkpoint sequence of
//! the replication stream the state reflects. The slave persists snapshots
//! to the journal and to Redis, and recovery rebuilds the book from the
//! latest one by replaying the stream after the checkpoint.

use primitives::orderbook::book::OrderBookState;
use serde::{Deserialize, Serialize};

/// A snapshot of the book state taken after applying the replication message
/// with the NATS stream sequence `seq`. Recovery loads the snapshot and
/// replays the stream from `seq + 1`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// The NATS stream sequence the snapshot state reflects.
    pub seq: u64,
    /// The state of the book.
    pub state: OrderBookState,
}

impl Snapshot {
    /// Creates a snapshot of the book state at the given stream sequence.
    pub fn new(seq: u64, state: OrderBookState) -> Self {
        Self { seq, state }
    }

    /// Encodes the snapshot into the wire format (MessagePack).
    pub fn encode(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }

    /// Decodes a snapshot from the wire format.
    pub fn decode(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::orderbook::book::OrderBookState;
    use primitives::orderbook::config::BookConfig;

    #[test]
    fn test_snapshot_roundtrip() {
        let state = OrderBookState::new(&BookConfig::default());
        let snapshot = Snapshot::new(42, state);
        let bytes = snapshot.encode().unwrap();
        let restored = Snapshot::decode(&bytes).unwrap();
        assert_eq!(restored.seq, 42);
        // The restored state matches the original at the wire level.
        assert_eq!(
            rmp_serde::to_vec(&restored.state).unwrap(),
            rmp_serde::to_vec(&snapshot.state).unwrap()
        );
    }

    #[test]
    fn test_snapshot_checkpoint_boundaries() {
        for seq in [0u64, 1, u64::MAX - 1, u64::MAX] {
            let snapshot = Snapshot::new(seq, OrderBookState::new(&BookConfig::default()));
            let restored = Snapshot::decode(&snapshot.encode().unwrap()).unwrap();
            assert_eq!(restored.seq, seq);
        }
    }
}
