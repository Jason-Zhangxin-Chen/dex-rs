//! Time in force policy of an order.

use serde::{Deserialize, Serialize};

/// Specifies how long an order remains active before it is executed or expires.
#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    /// Good till canceled.
    Gtc,
    /// Immediate or canceled.
    Ioc,
    /// Fill or Kill.
    Fok,
    /// Good till date.
    Gtd(u64),
    /// Good for the trading day.
    Day,
}

#[cfg(test)]
mod tests {
    use rmp_serde::{from_slice, to_vec};

    use crate::time_in_force::TimeInForce;

    // ---------------------------------------------------------------
    // Unit variants — round-trip
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_gtc() {
        let tif = TimeInForce::Gtc;
        let bytes = to_vec(&tif).unwrap();
        let restored: TimeInForce = from_slice(&bytes).unwrap();
        assert_eq!(tif, restored);
    }

    #[test]
    fn test_ser_deser_ioc() {
        let tif = TimeInForce::Ioc;
        let bytes = to_vec(&tif).unwrap();
        let restored: TimeInForce = from_slice(&bytes).unwrap();
        assert_eq!(tif, restored);
    }

    #[test]
    fn test_ser_deser_fok() {
        let tif = TimeInForce::Fok;
        let bytes = to_vec(&tif).unwrap();
        let restored: TimeInForce = from_slice(&bytes).unwrap();
        assert_eq!(tif, restored);
    }

    #[test]
    fn test_ser_deser_day() {
        let tif = TimeInForce::Day;
        let bytes = to_vec(&tif).unwrap();
        let restored: TimeInForce = from_slice(&bytes).unwrap();
        assert_eq!(tif, restored);
    }

    // ---------------------------------------------------------------
    // Data-carrying variant — Gtd(u64)
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_gtd() {
        let tif = TimeInForce::Gtd(1_700_000_000);
        let bytes = to_vec(&tif).unwrap();
        let restored: TimeInForce = from_slice(&bytes).unwrap();
        assert_eq!(tif, restored);
    }

    #[test]
    fn test_gtd_boundary_values() {
        for value in [0u64, 1, u64::MAX, u64::MAX - 1] {
            let tif = TimeInForce::Gtd(value);
            let bytes = to_vec(&tif).unwrap();
            let restored: TimeInForce = from_slice(&bytes).unwrap();
            assert_eq!(tif, restored);
        }
    }

    #[test]
    fn test_gtd_payload_is_preserved_exactly() {
        // Ensure the u64 inside Gtd survives the round-trip unchanged.
        let value = 1_234_567_890_123_456_789u64;
        let tif = TimeInForce::Gtd(value);
        let restored: TimeInForce = from_slice(&to_vec(&tif).unwrap()).unwrap();

        match restored {
            TimeInForce::Gtd(v) => assert_eq!(v, value),
            other => panic!("expected Gtd, got {other:?}"),
        }
    }

    #[test]
    fn test_gtd_different_values_are_distinct() {
        let a = to_vec(&TimeInForce::Gtd(1)).unwrap();
        let b = to_vec(&TimeInForce::Gtd(2)).unwrap();
        assert_ne!(a, b);
    }

    // ---------------------------------------------------------------
    // Variant index stability
    // ---------------------------------------------------------------

    #[test]
    fn test_all_variants_are_distinct() {
        let encoded: Vec<Vec<u8>> = [
            TimeInForce::Gtc,
            TimeInForce::Ioc,
            TimeInForce::Fok,
            TimeInForce::Gtd(0),
            TimeInForce::Day,
        ]
        .iter()
        .map(|tif| to_vec(tif).unwrap())
        .collect();

        for i in 0..encoded.len() {
            for j in (i + 1)..encoded.len() {
                assert_ne!(encoded[i], encoded[j], "variants {i} and {j} collide");
            }
        }
    }

    // ---------------------------------------------------------------
    // Determinism and stability
    // ---------------------------------------------------------------

    #[test]
    fn test_encoding_is_deterministic() {
        for tif in [
            TimeInForce::Gtc,
            TimeInForce::Ioc,
            TimeInForce::Fok,
            TimeInForce::Gtd(1_700_000_000),
            TimeInForce::Day,
        ] {
            assert_eq!(to_vec(&tif).unwrap(), to_vec(&tif).unwrap());
        }
    }

    #[test]
    fn test_roundtrip_is_stable() {
        // encode(decode(bytes)) == bytes
        for tif in [
            TimeInForce::Gtc,
            TimeInForce::Ioc,
            TimeInForce::Fok,
            TimeInForce::Gtd(1_700_000_000),
            TimeInForce::Day,
        ] {
            let bytes = to_vec(&tif).unwrap();
            let restored: TimeInForce = from_slice(&bytes).unwrap();
            assert_eq!(bytes, to_vec(&restored).unwrap());
        }
    }

    #[test]
    fn test_roundtrip_many_gtd_values() {
        for i in 0..1000u64 {
            let tif = TimeInForce::Gtd(i.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let restored: TimeInForce = from_slice(&to_vec(&tif).unwrap()).unwrap();
            assert_eq!(tif, restored);
        }
    }

    // ---------------------------------------------------------------
    // Error handling
    // ---------------------------------------------------------------

    #[test]
    fn test_deserialize_unknown_variant_index_fails() {
        // Index 5 doesn't correspond to any variant.
        let bad = to_vec(&5u8).unwrap();
        let result: Result<TimeInForce, _> = from_slice(&bad);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_gtd_with_missing_payload_fails() {
        // Just the variant index with no payload should fail for Gtd.
        let bad = to_vec(&3u8).unwrap();
        let result: Result<TimeInForce, _> = from_slice(&bad);
        assert!(result.is_err());
    }
}
