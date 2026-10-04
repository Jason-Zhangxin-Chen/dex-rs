//! Margin messages: the margin feed the [SVD_Sync] publishes to the storage
//! and the [SVD_Pretrade] instances consume.

use crate::address::Address;
use serde::{Deserialize, Serialize};

/// The latest margin state of one account — a final state, not a delta. The
/// subscribers overwrite their entry for the account; the last message wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarginChange {
    /// The account of the settlement protocol.
    pub account: Address,
    /// The equity of the account, quote asset units.
    pub equity: u128,
    /// The margin used by the account's open positions.
    pub used_margin: u128,
    /// The available margin, equity minus used.
    pub available: u128,
    /// The block number the state comes from.
    pub block: u64,
}

/// The messages of the `svd:sync:margin` channel, published by [SVD_Sync].
/// The channel is global (not per symbol): the margin account of the
/// settlement protocol backs every market, and the subscribers keep their
/// own subsets of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarginMsg {
    /// The latest margin state of one account. A republished state is
    /// harmless: the subscribers overwrite their entry for the account.
    Update(MarginChange),
    /// A periodic heartbeat carrying the confirmed block number; the
    /// subscribers use it to detect a dead feed.
    Heartbeat {
        /// The confirmed block number at the heartbeat.
        block: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmp_serde::{from_slice, to_vec};

    #[test]
    fn test_margin_change_roundtrip() {
        let change = MarginChange {
            account: Address([0xabu8; 20]),
            equity: 1_000,
            used_margin: 250,
            available: 750,
            block: 42,
        };
        let bytes = to_vec(&change).unwrap();
        let restored: MarginChange = from_slice(&bytes).unwrap();
        assert_eq!(change, restored);
    }

    #[test]
    fn test_margin_msg_update_roundtrip() {
        let msg = MarginMsg::Update(MarginChange {
            account: Address([1u8; 20]),
            equity: 5,
            used_margin: 1,
            available: 4,
            block: 7,
        });
        let bytes = to_vec(&msg).unwrap();
        let restored: MarginMsg = from_slice(&bytes).unwrap();
        assert_eq!(msg, restored);
    }

    #[test]
    fn test_margin_msg_heartbeat_roundtrip() {
        let msg = MarginMsg::Heartbeat { block: 100 };
        let bytes = to_vec(&msg).unwrap();
        let restored: MarginMsg = from_slice(&bytes).unwrap();
        assert_eq!(msg, restored);
    }

    #[test]
    fn test_margin_msg_variants_are_distinct() {
        let update = to_vec(&MarginMsg::Update(MarginChange {
            account: Address([1u8; 20]),
            equity: 0,
            used_margin: 0,
            available: 0,
            block: 0,
        }))
        .unwrap();
        let heartbeat = to_vec(&MarginMsg::Heartbeat { block: 0 }).unwrap();
        assert_ne!(update, heartbeat);
    }
}
