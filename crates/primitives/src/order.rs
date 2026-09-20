//! The definition of orders

use crate::address::Address;
use crate::base::{Hash32, Nonce, PegReferenceType, Side, Symbol};
use crate::signature::Signature;
use crate::time_in_force::TimeInForce;
use crate::value::{Price, Quantity, TimestampMs};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// Order represents different types of orders
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Order {
    /// Standard limit order.
    Standard {
        /// The hash of the order.
        id: Hash32,

        /// The price of the order.
        price: Price,

        /// The quantity of the order.
        quantity: Quantity,

        /// The side of the order.
        side: Side,

        /// The user address.
        user: Address,

        /// The nonce.
        nonce: Nonce,

        /// When the order is created.
        timestamp: TimestampMs,

        /// Time-in-force policy.
        time_in_force: TimeInForce,

        /// Symbol of the project.
        symbol: Symbol,

        /// Signature of the order.
        signature: Signature,
    },

    /// Iceberg order with visible and hidden quantities.
    Iceberg {
        /// The hash of the order.
        id: Hash32,

        /// The price of the order.
        price: Price,

        /// The visible quantity.
        visible_quantity: Quantity,

        /// The hidden quantity.
        hidden_quantity: Quantity,

        /// The side of the order.
        side: Side,

        /// The use address.
        user: Address,

        /// The nonce.
        nonce: Nonce,

        /// When the order is created.
        timestamp: TimestampMs,

        /// Time-in-force policy.
        time_in_force: TimeInForce,

        /// Symbol of the project.
        symbol: Symbol,

        /// Signature of the order.
        signature: Signature,
    },

    /// Post only order that won't match immediately.
    PostOnly {
        /// The hash of the order.
        id: Hash32,
        /// The price of the order.
        price: Price,
        /// The quantity of the order.
        quantity: Quantity,
        /// The side of the order.
        side: Side,
        /// The user address.
        user: Address,
        /// The nonce.
        nonce: Nonce,
        /// When the order is created.
        timestamp: TimestampMs,
        /// Time-in-force policy.
        time_in_force: TimeInForce,

        /// Symbol of the project.
        symbol: Symbol,

        /// Signature of the order.
        signature: Signature,
    },

    /// Trailing stop order that adjusts with market movement
    TrailingStop {
        /// The hash of the order.
        id: Hash32,
        /// The price of the order.
        price: Price,
        /// The quantity of the order.
        quantity: Quantity,
        /// The side of the order.
        side: Side,
        /// The user address.
        user: Address,
        /// The nonce.
        nonce: Nonce,
        /// When the order is created.
        timestamp: TimestampMs,
        /// Time-in-force policy.
        time_in_force: TimeInForce,
        /// Amount to trail the market price.
        trail_amount: Quantity,
        /// Last reference price.
        last_ref_price: Price,

        /// Symbol of the project.
        symbol: Symbol,

        /// Signature of the order.
        signature: Signature,
    },

    /// Pegged order that adjusts based on reference price
    Pegged {
        /// The hash of the order.
        id: Hash32,
        /// The price of the order.
        price: Price,
        /// The quantity of the order.
        quantity: Quantity,
        /// The side of the order.
        side: Side,
        /// The user address.
        user: Address,
        /// The Nonce.
        nonce: Nonce,
        /// When the order was created.
        timestamp: TimestampMs,
        /// Time-in-force policy.
        time_in_force: TimeInForce,
        /// Offset from the reference price.
        reference_price_offset: i64,
        /// Type of reference price to track.
        reference_price_type: PegReferenceType,

        /// Symbol of the project.
        symbol: Symbol,

        /// The signature.
        signature: Signature,
    },

    /// Market-to-limit order that converts to limit after initial execution
    MarketToLimit {
        /// The hash of the order.
        id: Hash32,
        /// The price of the order.
        price: Price,
        /// The quantity of the order.
        quantity: Quantity,
        /// The side of the order.
        side: Side,
        /// The user address.
        user: Address,
        /// The nonce.
        nonce: Nonce,
        /// When the order was created.
        timestamp: TimestampMs,
        /// Time-in-force policy
        time_in_force: TimeInForce,

        /// Symbol of the project.
        symbol: Symbol,

        /// The signature.
        signature: Signature,
    },

    /// Reserve order with custom replenishment
    /// if `replenish_amount` is None, it uses DEFAULT_RESERVE_REPLENISH_AMOUNT
    /// if `auto_replenish` is false, and visible quantity is below threshold, it will not replenish
    /// if `auto_replenish` is false and visible quantity is zero it will be removed from the book
    /// if `auto_replenish` is true, and replenish_threshold is 0, it will use 1
    ReserveOrder {
        /// The hash of the order.
        id: Hash32,
        /// The price of the order.
        price: Price,
        /// The visible quantity of the order.
        visible_quantity: Quantity,
        /// The hidden quantity of the order.
        hidden_quantity: Quantity,
        /// The side of the order.
        side: Side,
        /// The user address.
        user: Address,
        /// The nonce.
        nonce: Nonce,
        /// When the order was created
        timestamp: TimestampMs,
        /// Time-in-force policy
        time_in_force: TimeInForce,
        /// Threshold at which to replenish
        replenish_threshold: Quantity,
        /// Optional amount to replenish by, in quantity units. If `None`, uses
        /// [`DEFAULT_RESERVE_REPLENISH_AMOUNT`]. A replenish amount is
        /// structurally non-zero ([`NonZeroU64`]): a zero replenish would draw
        /// an empty visible tranche from hidden.
        replenish_amount: Option<NonZeroU64>,
        /// Whether to replenish automatically when below threshold. If false, only replenish on next match
        auto_replenish: bool,

        /// Symbol of the project.
        symbol: Symbol,

        /// The signature.
        signature: Signature,
    },
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use alloy::signers::{SignerSync, local::PrivateKeySigner};
    use rmp_serde::{from_slice, to_vec};
    use solana_sdk::signer::{Signer, keypair::Keypair};

    use crate::address::{Address, EthAddress, SolAddress};
    use crate::base::{Hash32, Nonce, PegReferenceType, Side, Symbol};
    use crate::signature::{EthSignature, Signature, SolSignature};
    use crate::time_in_force::TimeInForce;
    use crate::value::{Price, Quantity, TimestampMs};

    use super::Order;

    // ---------------------------------------------------------------
    // Helpers — small, deterministic-ish values
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
        let sig = signer.sign_message_sync(b"order").unwrap();
        Signature::Ethereum(EthSignature(sig.as_bytes()))
    }

    fn sol_signature() -> Signature {
        let keypair = Keypair::new();
        let sig = keypair.sign_message(b"order");
        Signature::Solana(SolSignature(*sig.as_array()))
    }

    /// Shared fields common to every variant. Returns them as a tuple so
    /// each constructor can pick what it needs.
    struct Common {
        id: Hash32,
        price: Price,
        quantity: Quantity,
        side: Side,
        user: Address,
        nonce: Nonce,
        timestamp: TimestampMs,
        time_in_force: TimeInForce,
        symbol: Symbol,
        signature: Signature,
    }

    fn common(seed: u8) -> Common {
        Common {
            id: hash32(seed),
            price: Price(1_000 * seed as u64),
            quantity: Quantity(10 * seed as u64),
            side: if seed.is_multiple_of(2) { Side::Buy } else { Side::Sell },
            user: if seed.is_multiple_of(2) { eth_address() } else { sol_address() },
            nonce: Nonce(seed as u64),
            timestamp: TimestampMs(1_700_000_000_000 + seed as u64),
            time_in_force: TimeInForce::Gtc,
            symbol: symbol(seed),
            signature: if seed.is_multiple_of(2) { eth_signature() } else { sol_signature() },
        }
    }

    // ---------------------------------------------------------------
    // Constructors — one per variant
    // ---------------------------------------------------------------

    fn make_standard(seed: u8) -> Order {
        let c = common(seed);
        Order::Standard {
            id: c.id,
            price: c.price,
            quantity: c.quantity,
            side: c.side,
            user: c.user,
            nonce: c.nonce,
            timestamp: c.timestamp,
            time_in_force: c.time_in_force,
            symbol: c.symbol,
            signature: c.signature,
        }
    }

    fn make_iceberg(seed: u8) -> Order {
        let c = common(seed);
        Order::Iceberg {
            id: c.id,
            price: c.price,
            visible_quantity: Quantity(5),
            hidden_quantity: Quantity(95),
            side: c.side,
            user: c.user,
            nonce: c.nonce,
            timestamp: c.timestamp,
            time_in_force: c.time_in_force,
            symbol: c.symbol,
            signature: c.signature,
        }
    }

    fn make_post_only(seed: u8) -> Order {
        let c = common(seed);
        Order::PostOnly {
            id: c.id,
            price: c.price,
            quantity: c.quantity,
            side: c.side,
            user: c.user,
            nonce: c.nonce,
            timestamp: c.timestamp,
            time_in_force: c.time_in_force,
            symbol: c.symbol,
            signature: c.signature,
        }
    }

    fn make_trailing_stop(seed: u8) -> Order {
        let c = common(seed);
        Order::TrailingStop {
            id: c.id,
            price: c.price,
            quantity: c.quantity,
            side: c.side,
            user: c.user,
            nonce: c.nonce,
            timestamp: c.timestamp,
            time_in_force: c.time_in_force,
            trail_amount: Quantity(50),
            last_ref_price: Price(999),
            symbol: c.symbol,
            signature: c.signature,
        }
    }

    fn make_pegged(seed: u8) -> Order {
        let c = common(seed);
        Order::Pegged {
            id: c.id,
            price: c.price,
            quantity: c.quantity,
            side: c.side,
            user: c.user,
            nonce: c.nonce,
            timestamp: c.timestamp,
            time_in_force: c.time_in_force,
            reference_price_offset: -42,
            reference_price_type: PegReferenceType::Mid,
            symbol: c.symbol,
            signature: c.signature,
        }
    }

    fn make_market_to_limit(seed: u8) -> Order {
        let c = common(seed);
        Order::MarketToLimit {
            id: c.id,
            price: c.price,
            quantity: c.quantity,
            side: c.side,
            user: c.user,
            nonce: c.nonce,
            timestamp: c.timestamp,
            time_in_force: c.time_in_force,
            symbol: c.symbol,
            signature: c.signature,
        }
    }

    fn make_reserve(seed: u8) -> Order {
        let c = common(seed);
        Order::ReserveOrder {
            id: c.id,
            price: c.price,
            visible_quantity: Quantity(5),
            hidden_quantity: Quantity(95),
            side: c.side,
            user: c.user,
            nonce: c.nonce,
            timestamp: c.timestamp,
            time_in_force: c.time_in_force,
            replenish_threshold: Quantity(1),
            replenish_amount: NonZeroU64::new(10),
            auto_replenish: true,
            symbol: c.symbol,
            signature: c.signature,
        }
    }

    /// All variants, one of each.
    fn all_variants() -> Vec<Order> {
        vec![
            make_standard(1),
            make_iceberg(2),
            make_post_only(3),
            make_trailing_stop(4),
            make_pegged(5),
            make_market_to_limit(6),
            make_reserve(7),
        ]
    }

    // ---------------------------------------------------------------
    // Per-variant round-trips
    // ---------------------------------------------------------------

    #[test]
    fn test_roundtrip_standard() {
        let order = make_standard(1);
        let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
        assert_eq!(order, restored);
    }

    #[test]
    fn test_roundtrip_iceberg() {
        let order = make_iceberg(2);
        let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
        assert_eq!(order, restored);
    }

    #[test]
    fn test_roundtrip_post_only() {
        let order = make_post_only(3);
        let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
        assert_eq!(order, restored);
    }

    #[test]
    fn test_roundtrip_trailing_stop() {
        let order = make_trailing_stop(4);
        let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
        assert_eq!(order, restored);
    }

    #[test]
    fn test_roundtrip_pegged() {
        let order = make_pegged(5);
        let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
        assert_eq!(order, restored);
    }

    #[test]
    fn test_roundtrip_market_to_limit() {
        let order = make_market_to_limit(6);
        let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
        assert_eq!(order, restored);
    }

    #[test]
    fn test_roundtrip_reserve() {
        let order = make_reserve(7);
        let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
        assert_eq!(order, restored);
    }

    #[test]
    fn test_roundtrip_all_variants() {
        for order in all_variants() {
            let bytes = to_vec(&order).unwrap();
            let restored: Order = from_slice(&bytes).unwrap();
            assert_eq!(order, restored);
        }
    }

    // ---------------------------------------------------------------
    // Variant index stability
    // ---------------------------------------------------------------

    #[test]
    fn test_variants_are_distinct() {
        let encoded: Vec<Vec<u8>> = all_variants().iter().map(|o| to_vec(o).unwrap()).collect();

        for i in 0..encoded.len() {
            for j in (i + 1)..encoded.len() {
                assert_ne!(encoded[i], encoded[j], "variants {i} and {j} collide");
            }
        }
    }

    // ---------------------------------------------------------------
    // Determinism and round-trip stability
    // ---------------------------------------------------------------

    #[test]
    fn test_encoding_is_deterministic() {
        for order in all_variants() {
            let a = to_vec(&order).unwrap();
            let b = to_vec(&order).unwrap();
            assert_eq!(a, b);
        }
    }

    #[test]
    fn test_roundtrip_is_stable() {
        // encode(decode(bytes)) == bytes
        for order in all_variants() {
            let bytes = to_vec(&order).unwrap();
            let restored: Order = from_slice(&bytes).unwrap();
            assert_eq!(bytes, to_vec(&restored).unwrap());
        }
    }

    #[test]
    fn test_roundtrip_many_random_values() {
        for seed in 1..50u8 {
            let order = make_standard(seed);
            let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
            assert_eq!(order, restored);

            let order = make_iceberg(seed);
            let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
            assert_eq!(order, restored);
        }
    }

    // ---------------------------------------------------------------
    // Field-level checks (per-variant edge cases)
    // ---------------------------------------------------------------

    #[test]
    fn test_iceberg_visible_and_hidden_are_kept_separate() {
        // Round-trip must not conflate visible and hidden quantities.
        let order = Order::Iceberg {
            id: hash32(1),
            price: Price(100),
            visible_quantity: Quantity(1),
            hidden_quantity: Quantity(999),
            side: Side::Buy,
            user: eth_address(),
            nonce: Nonce(1),
            timestamp: TimestampMs(1),
            time_in_force: TimeInForce::Gtc,
            symbol: symbol(1),
            signature: eth_signature(),
        };

        let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
        match restored {
            Order::Iceberg { visible_quantity, hidden_quantity, .. } => {
                assert_eq!(visible_quantity, Quantity(1));
                assert_eq!(hidden_quantity, Quantity(999));
            }
            other => panic!("expected Iceberg, got {other:?}"),
        }
    }

    #[test]
    fn test_pegged_negative_offset_roundtrip() {
        // reference_price_offset is i64 — make sure negative values survive.
        for offset in [-1i64, 0, 1, i64::MIN, i64::MAX] {
            let order = Order::Pegged {
                id: hash32(1),
                price: Price(100),
                quantity: Quantity(1),
                side: Side::Sell,
                user: eth_address(),
                nonce: Nonce(1),
                timestamp: TimestampMs(1),
                time_in_force: TimeInForce::Gtc,
                reference_price_offset: offset,
                reference_price_type: PegReferenceType::BestBid,
                symbol: symbol(1),
                signature: eth_signature(),
            };

            let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
            match restored {
                Order::Pegged { reference_price_offset, .. } => {
                    assert_eq!(reference_price_offset, offset);
                }
                other => panic!("expected Pegged, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_reserve_replenish_amount_none_roundtrip() {
        let order = Order::ReserveOrder {
            id: hash32(1),
            price: Price(100),
            visible_quantity: Quantity(5),
            hidden_quantity: Quantity(95),
            side: Side::Buy,
            user: eth_address(),
            nonce: Nonce(1),
            timestamp: TimestampMs(1),
            time_in_force: TimeInForce::Gtc,
            replenish_threshold: Quantity(1),
            replenish_amount: None,
            auto_replenish: false,
            symbol: symbol(1),
            signature: eth_signature(),
        };

        let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
        match restored {
            Order::ReserveOrder { replenish_amount, auto_replenish, .. } => {
                assert!(replenish_amount.is_none());
                assert!(!auto_replenish);
            }
            other => panic!("expected ReserveOrder, got {other:?}"),
        }
    }

    #[test]
    fn test_reserve_replenish_amount_some_roundtrip() {
        for value in [1u64, 10, u64::MAX] {
            let order = Order::ReserveOrder {
                id: hash32(1),
                price: Price(100),
                visible_quantity: Quantity(5),
                hidden_quantity: Quantity(95),
                side: Side::Buy,
                user: eth_address(),
                nonce: Nonce(1),
                timestamp: TimestampMs(1),
                time_in_force: TimeInForce::Gtc,
                replenish_threshold: Quantity(1),
                replenish_amount: NonZeroU64::new(value),
                auto_replenish: true,
                symbol: symbol(1),
                signature: eth_signature(),
            };

            let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
            match restored {
                Order::ReserveOrder { replenish_amount, .. } => {
                    assert_eq!(replenish_amount, NonZeroU64::new(value));
                }
                other => panic!("expected ReserveOrder, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_trailing_stop_uses_quantity_for_trail_amount() {
        // trail_amount is typed as Quantity (u64), last_ref_price as Price (u128).
        // Verify both survive with distinct values.
        let order = Order::TrailingStop {
            id: hash32(1),
            price: Price(100),
            quantity: Quantity(1),
            side: Side::Buy,
            user: eth_address(),
            nonce: Nonce(1),
            timestamp: TimestampMs(1),
            time_in_force: TimeInForce::Gtc,
            trail_amount: Quantity(7),
            last_ref_price: Price(12_345),
            symbol: symbol(1),
            signature: eth_signature(),
        };

        let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
        match restored {
            Order::TrailingStop { trail_amount, last_ref_price, .. } => {
                assert_eq!(trail_amount, Quantity(7));
                assert_eq!(last_ref_price, Price(12_345));
            }
            other => panic!("expected TrailingStop, got {other:?}"),
        }
    }

    // ---------------------------------------------------------------
    // Cross-chain signature / address coverage
    // ---------------------------------------------------------------

    #[test]
    fn test_order_with_solana_user_and_signature() {
        let order = Order::Standard {
            id: hash32(1),
            price: Price(100),
            quantity: Quantity(1),
            side: Side::Sell,
            user: sol_address(),
            nonce: Nonce(1),
            timestamp: TimestampMs(1),
            time_in_force: TimeInForce::Ioc,
            symbol: symbol(1),
            signature: sol_signature(),
        };

        let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
        assert_eq!(order, restored);

        match restored {
            Order::Standard { user, signature, .. } => {
                assert!(matches!(user, Address::Solana(_)));
                assert!(matches!(signature, Signature::Solana(_)));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_order_with_eth_user_and_signature() {
        let order = Order::Standard {
            id: hash32(1),
            price: Price(100),
            quantity: Quantity(1),
            side: Side::Buy,
            user: eth_address(),
            nonce: Nonce(1),
            timestamp: TimestampMs(1),
            time_in_force: TimeInForce::Fok,
            symbol: symbol(1),
            signature: eth_signature(),
        };

        let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
        assert_eq!(order, restored);

        match restored {
            Order::Standard { user, signature, .. } => {
                assert!(matches!(user, Address::Ethereum(_)));
                assert!(matches!(signature, Signature::Ethereum(_)));
            }
            _ => unreachable!(),
        }
    }

    // ---------------------------------------------------------------
    // Time-in-force variants inside orders
    // ---------------------------------------------------------------

    #[test]
    fn test_order_with_each_time_in_force() {
        let variants = [
            TimeInForce::Gtc,
            TimeInForce::Ioc,
            TimeInForce::Fok,
            TimeInForce::Gtd(1_700_000_000),
            TimeInForce::Day,
        ];

        for tif in variants {
            let order = Order::Standard {
                id: hash32(1),
                price: Price(100),
                quantity: Quantity(1),
                side: Side::Buy,
                user: eth_address(),
                nonce: Nonce(1),
                timestamp: TimestampMs(1),
                time_in_force: tif,
                symbol: symbol(1),
                signature: eth_signature(),
            };

            let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
            match restored {
                Order::Standard { time_in_force, .. } => {
                    assert_eq!(time_in_force, tif);
                }
                other => panic!("expected Standard, got {other:?}"),
            }
        }
    }

    // ---------------------------------------------------------------
    // Pegged reference type variants
    // ---------------------------------------------------------------

    #[test]
    fn test_pegged_with_each_reference_type() {
        for ref_type in [
            PegReferenceType::BestBid,
            PegReferenceType::BestAsk,
            PegReferenceType::Mid,
            PegReferenceType::LastTrade,
        ] {
            let order = Order::Pegged {
                id: hash32(1),
                price: Price(100),
                quantity: Quantity(1),
                side: Side::Buy,
                user: eth_address(),
                nonce: Nonce(1),
                timestamp: TimestampMs(1),
                time_in_force: TimeInForce::Gtc,
                reference_price_offset: 5,
                reference_price_type: ref_type,
                symbol: symbol(1),
                signature: eth_signature(),
            };

            let restored: Order = from_slice(&to_vec(&order).unwrap()).unwrap();
            assert_eq!(order, restored);
        }
    }
}
