//! Base order definitions.

use serde::{Deserialize, Serialize};

/// Symbol represents the symbol of a product.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Symbol(pub [u8; 32]);

/// Side represents the side of an order.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    /// Buy side (bids)
    Buy,

    /// Sell side (asks)
    Sell,
}

/// Hash32 represents 32 bytes hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, Hash)]
#[serde(transparent)]
pub struct Hash32(pub [u8; 32]);

/// Nonce represents a sequence number of user's order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Nonce(pub u64);

/// Reference price type for pegged orders.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PegReferenceType {
    /// Pegged to best bid price.
    BestBid,
    /// Pegged to best ask price.
    BestAsk,
    /// Pegged to mid-price between ask and bid.
    Mid,
    /// Pegged to last trade price.
    LastTrade,
}

/// Total quote-asset computed as `Σ price × quantity` across every transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Quote(pub u128);

#[cfg(test)]
mod tests {
    use super::*;
    use rmp_serde::{from_slice, to_vec};

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    fn random_bytes32() -> [u8; 32] {
        // Deterministic-ish pseudo-random fill without pulling in `rand`.
        let mut out = [0u8; 32];
        for (i, b) in out.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(31).wrapping_add(7);
        }
        out
    }

    fn sample_symbol() -> Symbol {
        Symbol(random_bytes32())
    }

    fn sample_hash32() -> Hash32 {
        Hash32(random_bytes32())
    }

    // ---------------------------------------------------------------
    // Symbol
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_symbol() {
        let symbol = sample_symbol();
        let bytes = to_vec(&symbol).unwrap();
        let restored: Symbol = from_slice(&bytes).unwrap();
        assert_eq!(symbol, restored);
    }

    #[test]
    fn test_symbol_default_roundtrip() {
        let symbol = Symbol::default();
        let bytes = to_vec(&symbol).unwrap();
        let restored: Symbol = from_slice(&bytes).unwrap();
        assert_eq!(symbol, restored);
        assert_eq!(restored.0, [0u8; 32]);
    }

    #[test]
    fn test_symbol_wire_format_is_raw_payload() {
        // #[serde(transparent)] means the wire format should match a plain
        // [u8; 32] — no wrapper, no length prefix beyond what rmp adds for
        // the array itself.
        let symbol = sample_symbol();
        let symbol_bytes = to_vec(&symbol).unwrap();
        let raw_bytes = to_vec(&symbol.0).unwrap();
        assert_eq!(symbol_bytes, raw_bytes);
    }

    #[test]
    fn test_symbol_encoding_is_deterministic() {
        let symbol = sample_symbol();
        assert_eq!(to_vec(&symbol).unwrap(), to_vec(&symbol).unwrap());
    }

    // ---------------------------------------------------------------
    // Hash32
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_hash32() {
        let hash = sample_hash32();
        let bytes = to_vec(&hash).unwrap();
        let restored: Hash32 = from_slice(&bytes).unwrap();
        assert_eq!(hash, restored);
    }

    #[test]
    fn test_hash32_default_roundtrip() {
        let hash = Hash32::default();
        let bytes = to_vec(&hash).unwrap();
        let restored: Hash32 = from_slice(&bytes).unwrap();
        assert_eq!(hash, restored);
        assert_eq!(restored.0, [0u8; 32]);
    }

    // ---------------------------------------------------------------
    // Nonce
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_nonce() {
        let nonce = Nonce(42);
        let bytes = to_vec(&nonce).unwrap();
        let restored: Nonce = from_slice(&bytes).unwrap();
        assert_eq!(nonce, restored);
    }

    #[test]
    fn test_nonce_boundary_values() {
        for value in [0u64, 1, u64::MAX, u64::MAX - 1] {
            let nonce = Nonce(value);
            let bytes = to_vec(&nonce).unwrap();
            let restored: Nonce = from_slice(&bytes).unwrap();
            assert_eq!(nonce, restored);
        }
    }

    #[test]
    fn test_nonce_wire_format_is_raw_payload() {
        let nonce = Nonce(12345);
        assert_eq!(to_vec(&nonce).unwrap(), to_vec(&12345u64).unwrap());
    }

    // ---------------------------------------------------------------
    // Side
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_side() {
        for side in [Side::Buy, Side::Sell] {
            let bytes = to_vec(&side).unwrap();
            let restored: Side = from_slice(&bytes).unwrap();
            assert_eq!(side, restored);
        }
    }

    #[test]
    fn test_side_variants_are_distinct() {
        let buy_bytes = to_vec(&Side::Buy).unwrap();
        let sell_bytes = to_vec(&Side::Sell).unwrap();
        assert_ne!(buy_bytes, sell_bytes);
    }

    #[test]
    fn test_side_encoding_is_deterministic() {
        assert_eq!(to_vec(&Side::Buy).unwrap(), to_vec(&Side::Buy).unwrap());
        assert_eq!(to_vec(&Side::Sell).unwrap(), to_vec(&Side::Sell).unwrap());
    }

    // ---------------------------------------------------------------
    // PegReferenceType
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_peg_reference_type() {
        for peg in [
            PegReferenceType::BestBid,
            PegReferenceType::BestAsk,
            PegReferenceType::Mid,
            PegReferenceType::LastTrade,
        ] {
            let bytes = to_vec(&peg).unwrap();
            let restored: PegReferenceType = from_slice(&bytes).unwrap();
            assert_eq!(peg, restored);
        }
    }

    #[test]
    fn test_peg_reference_type_all_variants_are_distinct() {
        let encoded: Vec<Vec<u8>> = [
            PegReferenceType::BestBid,
            PegReferenceType::BestAsk,
            PegReferenceType::Mid,
            PegReferenceType::LastTrade,
        ]
        .iter()
        .map(|p| to_vec(p).unwrap())
        .collect();

        for i in 0..encoded.len() {
            for j in (i + 1)..encoded.len() {
                assert_ne!(encoded[i], encoded[j], "variants {i} and {j} collide");
            }
        }
    }

    // ---------------------------------------------------------------
    // Quote (u128, no Eq derive on the wrapper)
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_quote() {
        let quote = Quote(1_000_000_000_000_000_000_000u128);
        let bytes = to_vec(&quote).unwrap();
        let restored: Quote = from_slice(&bytes).unwrap();
        assert_eq!(quote.0, restored.0);
    }

    #[test]
    fn test_quote_boundary_values() {
        for value in [0u128, 1, u64::MAX as u128, u128::MAX, u128::MAX - 1] {
            let quote = Quote(value);
            let bytes = to_vec(&quote).unwrap();
            let restored: Quote = from_slice(&bytes).unwrap();
            assert_eq!(quote.0, restored.0);
        }
    }

    #[test]
    fn test_quote_wire_format_is_raw_payload() {
        let quote = Quote(12345);
        assert_eq!(to_vec(&quote).unwrap(), to_vec(&12345u128).unwrap());
    }

    #[test]
    fn test_quote_encoding_is_deterministic() {
        let quote = Quote(999_999_999);
        assert_eq!(to_vec(&quote).unwrap(), to_vec(&quote).unwrap());
    }

    // ---------------------------------------------------------------
    // Cross-type sanity
    // ---------------------------------------------------------------

    #[test]
    fn test_symbol_and_hash32_have_same_wire_format() {
        // Both are #[serde(transparent)] over [u8; 32], so the same bytes
        // should produce identical encodings. This is fine — they are
        // distinguished by their Rust types, not by the wire format.
        let raw = random_bytes32();
        assert_eq!(to_vec(&Symbol(raw)).unwrap(), to_vec(&Hash32(raw)).unwrap(),);
    }

    #[test]
    fn test_roundtrip_is_stable() {
        // encode(decode(bytes)) == bytes
        let symbol = sample_symbol();
        let bytes = to_vec(&symbol).unwrap();
        let restored: Symbol = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }
}
