//! events for the workspace.

use crate::address::Address;
use crate::base::{Hash32, Symbol};
use crate::order::Order;
use crate::signature::Signature;
use crate::value::TimestampMs;
use serde::{Deserialize, Serialize};

/// New Order event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewOrder {
    /// The order signed by the client.
    order: Order,
}

/// Cancel Order event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelOrder {
    /// The symbol.
    symbol: Symbol,
    /// The order to be canceled.
    order_id: Hash32,
    /// The user who request the operation.
    user: Address,
    /// When the cancel operation is created.
    timestamp: TimestampMs,
    /// Signature of the operation.
    signature: Signature,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use alloy::signers::{SignerSync, local::PrivateKeySigner};
    use rmp_serde::{from_slice, to_vec};
    use solana_sdk::signer::{Signer, keypair::Keypair};

    use crate::address::{Address, EthAddress, SolAddress};
    use crate::base::{Hash32, Nonce, PegReferenceType, Side, Symbol};
    use crate::order::Order;
    use crate::signature::{EthSignature, Signature, SolSignature};
    use crate::time_in_force::TimeInForce;
    use crate::value::{Price, Quantity, TimestampMs};

    use super::{CancelOrder, NewOrder};

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    fn hash32(seed: u8) -> Hash32 {
        Hash32([seed; 32])
    }

    fn symbol(seed: u8) -> Symbol {
        Symbol([seed; 32])
    }

    fn eth_address() -> Address {
        let signer = PrivateKeySigner::random();
        Address::Ethereum(EthAddress(signer.address().0.0))
    }

    fn sol_address() -> Address {
        let keypair = Keypair::new();
        Address::Solana(SolAddress(keypair.pubkey().to_bytes()))
    }

    fn eth_signature() -> Signature {
        let signer = PrivateKeySigner::random();
        let sig = signer.sign_message_sync(b"event").unwrap();
        Signature::Ethereum(EthSignature(sig.as_bytes()))
    }

    fn sol_signature() -> Signature {
        let keypair = Keypair::new();
        let sig = keypair.sign_message(b"event");
        Signature::Solana(SolSignature(*sig.as_array()))
    }

    // ---------------------------------------------------------------
    // Sample orders (one representative per shape)
    // ---------------------------------------------------------------

    fn standard_order() -> Order {
        Order::Standard {
            id: hash32(1),
            price: Price(1_000),
            quantity: Quantity(10),
            side: Side::Buy,
            user: eth_address(),
            nonce: Nonce(1),
            timestamp: TimestampMs(1_700_000_000_000),
            time_in_force: TimeInForce::Gtc,
            symbol: symbol(1),
            signature: eth_signature(),
        }
    }

    fn iceberg_order() -> Order {
        Order::Iceberg {
            id: hash32(2),
            price: Price(2_000),
            visible_quantity: Quantity(5),
            hidden_quantity: Quantity(95),
            side: Side::Sell,
            user: sol_address(),
            nonce: Nonce(2),
            timestamp: TimestampMs(1_700_000_000_001),
            time_in_force: TimeInForce::Ioc,
            symbol: symbol(2),
            signature: sol_signature(),
        }
    }

    fn pegged_order() -> Order {
        Order::Pegged {
            id: hash32(5),
            price: Price(5_000),
            quantity: Quantity(50),
            side: Side::Buy,
            user: eth_address(),
            nonce: Nonce(5),
            timestamp: TimestampMs(1_700_000_000_005),
            time_in_force: TimeInForce::Gtc,
            reference_price_offset: -7,
            reference_price_type: PegReferenceType::Mid,
            symbol: symbol(5),
            signature: eth_signature(),
        }
    }

    fn reserve_order() -> Order {
        Order::ReserveOrder {
            id: hash32(7),
            price: Price(7_000),
            visible_quantity: Quantity(5),
            hidden_quantity: Quantity(95),
            side: Side::Sell,
            user: sol_address(),
            nonce: Nonce(7),
            timestamp: TimestampMs(1_700_000_000_007),
            time_in_force: TimeInForce::Gtd(1_700_000_000),
            replenish_threshold: Quantity(1),
            replenish_amount: NonZeroU64::new(10),
            auto_replenish: true,
            symbol: symbol(7),
            signature: sol_signature(),
        }
    }

    // ---------------------------------------------------------------
    // NewOrder — round-trips
    // ---------------------------------------------------------------

    #[test]
    fn test_new_order_roundtrip_standard() {
        let event = NewOrder { order: standard_order() };
        let bytes = to_vec(&event).unwrap();
        let restored: NewOrder = from_slice(&bytes).unwrap();
        // No PartialEq on NewOrder, so compare by re-encoding.
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_new_order_roundtrip_iceberg() {
        let event = NewOrder { order: iceberg_order() };
        let bytes = to_vec(&event).unwrap();
        let restored: NewOrder = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_new_order_roundtrip_pegged() {
        let event = NewOrder { order: pegged_order() };
        let bytes = to_vec(&event).unwrap();
        let restored: NewOrder = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_new_order_roundtrip_reserve() {
        let event = NewOrder { order: reserve_order() };
        let bytes = to_vec(&event).unwrap();
        let restored: NewOrder = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_new_order_preserves_inner_order_exactly() {
        let order = standard_order();
        let event = NewOrder { order: order.clone() };
        let restored: NewOrder = from_slice(&to_vec(&event).unwrap()).unwrap();

        // Compare the inner order directly — Order itself derives PartialEq.
        assert_eq!(order, restored.order);
    }

    #[test]
    fn test_new_order_encoding_is_deterministic() {
        let event = NewOrder { order: standard_order() };
        assert_eq!(to_vec(&event).unwrap(), to_vec(&event).unwrap());
    }

    #[test]
    fn test_new_order_roundtrip_is_stable() {
        let event = NewOrder { order: iceberg_order() };
        let bytes = to_vec(&event).unwrap();
        let restored: NewOrder = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_new_order_wire_format_is_struct_map() {
        // NewOrder is a struct with a single named field `order`, so it
        // encodes as a 1-element map/array (rmp-serde uses a map with a
        // single key). The exact bytes depend on the format; what matters
        // is that it round-trips, which the previous tests cover. This
        // test simply sanity-checks that encoding is non-empty and the
        // structure is stable across two calls.
        let event = NewOrder { order: standard_order() };
        let a = to_vec(&event).unwrap();
        let b = to_vec(&event).unwrap();
        assert_eq!(a, b);
        assert!(!a.is_empty());
    }

    // ---------------------------------------------------------------
    // CancelOrder — round-trips
    // ---------------------------------------------------------------

    fn sample_cancel_eth() -> CancelOrder {
        CancelOrder {
            symbol: symbol(1),
            order_id: hash32(1),
            user: eth_address(),
            timestamp: TimestampMs(1_700_000_000_000),
            signature: eth_signature(),
        }
    }

    fn sample_cancel_sol() -> CancelOrder {
        CancelOrder {
            symbol: symbol(2),
            order_id: hash32(2),
            user: sol_address(),
            timestamp: TimestampMs(1_700_000_000_001),
            signature: sol_signature(),
        }
    }

    #[test]
    fn test_cancel_order_roundtrip_eth() {
        let event = sample_cancel_eth();
        let bytes = to_vec(&event).unwrap();
        let restored: CancelOrder = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_cancel_order_roundtrip_sol() {
        let event = sample_cancel_sol();
        let bytes = to_vec(&event).unwrap();
        let restored: CancelOrder = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_cancel_order_fields_survive_roundtrip() {
        let event = sample_cancel_eth();
        let restored: CancelOrder = from_slice(&to_vec(&event).unwrap()).unwrap();

        assert_eq!(restored.symbol, event.symbol);
        assert_eq!(restored.order_id, event.order_id);
        assert_eq!(restored.user, event.user);
        assert_eq!(restored.timestamp, event.timestamp);
        assert_eq!(restored.signature, event.signature);
    }

    #[test]
    fn test_cancel_order_encoding_is_deterministic() {
        let event = sample_cancel_eth();
        assert_eq!(to_vec(&event).unwrap(), to_vec(&event).unwrap());
    }

    #[test]
    fn test_cancel_order_roundtrip_is_stable() {
        let event = sample_cancel_sol();
        let bytes = to_vec(&event).unwrap();
        let restored: CancelOrder = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_cancel_order_with_each_symbol() {
        // symbol is a 32-byte newtype; make sure different symbols survive.
        for seed in [0u8, 1, 0xAB, 0xFF] {
            let event = CancelOrder {
                symbol: symbol(seed),
                order_id: hash32(seed),
                user: eth_address(),
                timestamp: TimestampMs(seed as u64),
                signature: eth_signature(),
            };
            let restored: CancelOrder = from_slice(&to_vec(&event).unwrap()).unwrap();
            assert_eq!(restored.symbol, event.symbol);
            assert_eq!(restored.order_id, event.order_id);
        }
    }

    #[test]
    fn test_cancel_order_different_symbols_are_distinct() {
        let a = CancelOrder {
            symbol: symbol(1),
            order_id: hash32(1),
            user: eth_address(),
            timestamp: TimestampMs(1),
            signature: eth_signature(),
        };
        let b = CancelOrder {
            symbol: symbol(2),
            order_id: hash32(2),
            user: eth_address(),
            timestamp: TimestampMs(2),
            signature: eth_signature(),
        };
        assert_ne!(to_vec(&a).unwrap(), to_vec(&b).unwrap());
    }

    // ---------------------------------------------------------------
    // Cross-cutting
    // ---------------------------------------------------------------

    #[test]
    fn test_new_order_and_cancel_order_are_distinguishable() {
        // Different types, different encodings. This is trivially true
        // but a useful sanity check that no accidental alias exists.
        let new_order = to_vec(&NewOrder { order: standard_order() }).unwrap();
        let cancel = to_vec(&sample_cancel_eth()).unwrap();
        assert_ne!(new_order, cancel);
    }

    #[test]
    fn test_events_roundtrip_many_times() {
        // Repeated encode/decode cycles must be idempotent.
        let mut bytes = to_vec(&NewOrder { order: pegged_order() }).unwrap();
        for _ in 0..10 {
            let event: NewOrder = from_slice(&bytes).unwrap();
            let next = to_vec(&event).unwrap();
            assert_eq!(bytes, next);
            bytes = next;
        }
    }
}
