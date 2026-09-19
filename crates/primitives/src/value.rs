//! Defines the value types for the exchange system.

use serde::{Deserialize, Serialize};

/// Value type representing a price.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Price(pub u128);

/// Quantity type representing a quantity in an order.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Quantity(pub u64);

/// TimestampMS representing a TS in millisecond.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TimestampMs(pub u64);

#[cfg(test)]
mod tests {
    use rmp_serde::{from_slice, to_vec};

    use super::{Price, Quantity, TimestampMs};

    // ---------------------------------------------------------------
    // Price
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_price() {
        let price = Price(1_000_000_000_000_000_000_000u128);
        let bytes = to_vec(&price).unwrap();
        let restored: Price = from_slice(&bytes).unwrap();
        assert_eq!(price, restored);
    }

    #[test]
    fn test_price_boundary_values() {
        for value in [0u128, 1, u64::MAX as u128, u128::MAX, u128::MAX - 1] {
            let price = Price(value);
            let bytes = to_vec(&price).unwrap();
            let restored: Price = from_slice(&bytes).unwrap();
            assert_eq!(price, restored);
            assert_eq!(restored.0, value);
        }
    }

    #[test]
    fn test_price_default_roundtrip() {
        let price = Price::default();
        let bytes = to_vec(&price).unwrap();
        let restored: Price = from_slice(&bytes).unwrap();
        assert_eq!(price, restored);
        assert_eq!(restored.0, 0u128);
    }

    #[test]
    fn test_price_wire_format_is_raw_payload() {
        // Newtype structs serialize transparently by default under rmp-serde.
        let price = Price(12345);
        assert_eq!(to_vec(&price).unwrap(), to_vec(&12345u128).unwrap());
    }

    #[test]
    fn test_price_encoding_is_deterministic() {
        let price = Price(999_999_999);
        assert_eq!(to_vec(&price).unwrap(), to_vec(&price).unwrap());
    }

    #[test]
    fn test_price_roundtrip_is_stable() {
        let price = Price(1_234_567_890);
        let bytes = to_vec(&price).unwrap();
        let restored: Price = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_price_different_values_are_distinct() {
        let a = to_vec(&Price(1)).unwrap();
        let b = to_vec(&Price(2)).unwrap();
        assert_ne!(a, b);
    }

    // ---------------------------------------------------------------
    // Quantity
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_quantity() {
        let qty = Quantity(42);
        let bytes = to_vec(&qty).unwrap();
        let restored: Quantity = from_slice(&bytes).unwrap();
        assert_eq!(qty, restored);
    }

    #[test]
    fn test_quantity_boundary_values() {
        for value in [0u64, 1, u64::MAX, u64::MAX - 1] {
            let qty = Quantity(value);
            let bytes = to_vec(&qty).unwrap();
            let restored: Quantity = from_slice(&bytes).unwrap();
            assert_eq!(qty, restored);
            assert_eq!(restored.0, value);
        }
    }

    #[test]
    fn test_quantity_default_roundtrip() {
        let qty = Quantity::default();
        let bytes = to_vec(&qty).unwrap();
        let restored: Quantity = from_slice(&bytes).unwrap();
        assert_eq!(qty, restored);
        assert_eq!(restored.0, 0u64);
    }

    #[test]
    fn test_quantity_wire_format_is_raw_payload() {
        let qty = Quantity(12345);
        assert_eq!(to_vec(&qty).unwrap(), to_vec(&12345u64).unwrap());
    }

    #[test]
    fn test_quantity_encoding_is_deterministic() {
        let qty = Quantity(777);
        assert_eq!(to_vec(&qty).unwrap(), to_vec(&qty).unwrap());
    }

    #[test]
    fn test_quantity_roundtrip_is_stable() {
        let qty = Quantity(1_000_000);
        let bytes = to_vec(&qty).unwrap();
        let restored: Quantity = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    // ---------------------------------------------------------------
    // TimestampMs
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_timestamp_ms() {
        let ts = TimestampMs(1_700_000_000_000);
        let bytes = to_vec(&ts).unwrap();
        let restored: TimestampMs = from_slice(&bytes).unwrap();
        assert_eq!(ts, restored);
    }

    #[test]
    fn test_timestamp_ms_boundary_values() {
        for value in [0u64, 1, u64::MAX, u64::MAX - 1] {
            let ts = TimestampMs(value);
            let bytes = to_vec(&ts).unwrap();
            let restored: TimestampMs = from_slice(&bytes).unwrap();
            assert_eq!(ts, restored);
            assert_eq!(restored.0, value);
        }
    }

    #[test]
    fn test_timestamp_ms_default_roundtrip() {
        let ts = TimestampMs::default();
        let bytes = to_vec(&ts).unwrap();
        let restored: TimestampMs = from_slice(&bytes).unwrap();
        assert_eq!(ts, restored);
        assert_eq!(restored.0, 0u64);
    }

    #[test]
    fn test_timestamp_ms_wire_format_is_raw_payload() {
        let ts = TimestampMs(1_700_000_000_000);
        assert_eq!(to_vec(&ts).unwrap(), to_vec(&1_700_000_000_000u64).unwrap());
    }

    #[test]
    fn test_timestamp_ms_encoding_is_deterministic() {
        let ts = TimestampMs(1_700_000_000_000);
        assert_eq!(to_vec(&ts).unwrap(), to_vec(&ts).unwrap());
    }

    #[test]
    fn test_timestamp_ms_roundtrip_is_stable() {
        let ts = TimestampMs(1_700_000_000_000);
        let bytes = to_vec(&ts).unwrap();
        let restored: TimestampMs = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    // ---------------------------------------------------------------
    // Cross-type checks
    // ---------------------------------------------------------------

    #[test]
    fn test_quantity_and_timestamp_ms_have_same_wire_format() {
        // Both wrap u64 with no #[serde(transparent)], but Serde's default
        // for newtype structs is already transparent, so the same u64
        // produces identical encodings. They are distinguished by Rust
        // type, not by wire format.
        let raw = 42u64;
        assert_eq!(to_vec(&Quantity(raw)).unwrap(), to_vec(&TimestampMs(raw)).unwrap(),);
    }

    #[test]
    fn test_price_wire_format_differs_from_u64_wrappers_for_large_values() {
        // u128 values above u64::MAX should encode differently from any
        // u64-based wrapper. This guards against accidentally swapping
        // the inner types.
        let big = u64::MAX as u128 + 1;
        let price_bytes = to_vec(&Price(big)).unwrap();
        let qty_bytes = to_vec(&Quantity(u64::MAX)).unwrap();
        assert_ne!(price_bytes, qty_bytes);
    }

    #[test]
    fn test_roundtrip_many_values() {
        for i in 0..1000u64 {
            let v = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);

            let qty = Quantity(v);
            assert_eq!(qty, from_slice::<Quantity>(&to_vec(&qty).unwrap()).unwrap());

            let ts = TimestampMs(v);
            assert_eq!(ts, from_slice::<TimestampMs>(&to_vec(&ts).unwrap()).unwrap());

            let price = Price(v as u128);
            assert_eq!(price, from_slice::<Price>(&to_vec(&price).unwrap()).unwrap());
        }
    }
}
