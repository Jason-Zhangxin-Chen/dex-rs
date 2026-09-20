//! Signature defines different blockchain's signature.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::convert::TryInto;

/// The signatures from different L1 protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Signature {
    /// Ethereum signature.
    Ethereum(EthSignature),
    /// Solana signature.
    Solana(SolSignature),
}

/// The implementation of ethereum signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EthSignature(pub [u8; 65]);

/// The implementation of solana signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SolSignature(pub [u8; 64]);

impl Serialize for EthSignature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_slice().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EthSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let arr: [u8; 65] = bytes
            .try_into()
            .map_err(|v: Vec<u8>| D::Error::invalid_length(v.len(), &"expected 65 bytes"))?;
        Ok(EthSignature(arr))
    }
}

impl Serialize for SolSignature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_slice().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SolSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let arr: [u8; 64] = bytes
            .try_into()
            .map_err(|v: Vec<u8>| D::Error::invalid_length(v.len(), &"expected 64 bytes"))?;
        Ok(SolSignature(arr))
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Signature as AlloySignature;
    use alloy::signers::{SignerSync, local::PrivateKeySigner};
    use rmp_serde::{from_slice, to_vec};
    use solana_sdk::signature::Signature as SolSignatureRaw;
    use solana_sdk::signer::{Signer, keypair::Keypair};

    use crate::signature::{EthSignature, Signature, SolSignature};

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    fn random_eth_signature() -> EthSignature {
        let signer = PrivateKeySigner::random();
        let sig: AlloySignature = signer.sign_message_sync(b"test message").unwrap();
        EthSignature(sig.as_bytes())
    }

    fn random_sol_signature() -> SolSignature {
        let keypair = Keypair::new();
        let sig: SolSignatureRaw = keypair.sign_message(b"test message");
        SolSignature(*sig.as_array())
    }

    // ---------------------------------------------------------------
    // EthSignature
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_eth_signature() {
        let sig = random_eth_signature();
        let bytes = to_vec(&sig).unwrap();
        let restored: EthSignature = from_slice(&bytes).unwrap();
        assert_eq!(sig, restored);
    }

    #[test]
    fn test_eth_signature_wire_format_is_raw_payload() {
        // Serialize should produce the same bytes as the inner slice.
        let sig = random_eth_signature();
        let wrapper_bytes = to_vec(&sig).unwrap();
        let raw_bytes = to_vec(&sig.0.as_slice()).unwrap();
        assert_eq!(wrapper_bytes, raw_bytes);
    }

    #[test]
    fn test_eth_signature_from_alloy_real_signature() {
        // Sign with a known key, verify the raw bytes land in the newtype.
        let signer = PrivateKeySigner::random();
        let alloy_sig: AlloySignature = signer.sign_message_sync(b"hello").unwrap();

        let eth_sig = EthSignature(alloy_sig.as_bytes());
        let restored: EthSignature = from_slice(&to_vec(&eth_sig).unwrap()).unwrap();

        assert_eq!(eth_sig, restored);
        assert_eq!(restored.0.len(), 65);
    }

    #[test]
    fn test_eth_signature_deserialize_wrong_length_too_short() {
        let bad = vec![0u8; 64];
        let bytes = to_vec(&bad).unwrap();
        let result: Result<EthSignature, _> = from_slice(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_eth_signature_deserialize_wrong_length_too_long() {
        let bad = vec![0u8; 66];
        let bytes = to_vec(&bad).unwrap();
        let result: Result<EthSignature, _> = from_slice(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_eth_signature_encoding_is_deterministic() {
        let sig = random_eth_signature();
        assert_eq!(to_vec(&sig).unwrap(), to_vec(&sig).unwrap());
    }

    #[test]
    fn test_eth_signature_roundtrip_is_stable() {
        let sig = random_eth_signature();
        let bytes = to_vec(&sig).unwrap();
        let restored: EthSignature = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_eth_signature_all_zeros_roundtrip() {
        let sig = EthSignature([0u8; 65]);
        let restored: EthSignature = from_slice(&to_vec(&sig).unwrap()).unwrap();
        assert_eq!(sig, restored);
    }

    // ---------------------------------------------------------------
    // SolSignature
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_sol_signature() {
        let sig = random_sol_signature();
        let bytes = to_vec(&sig).unwrap();
        let restored: SolSignature = from_slice(&bytes).unwrap();
        assert_eq!(sig, restored);
    }

    #[test]
    fn test_sol_signature_wire_format_is_raw_payload() {
        let sig = random_sol_signature();
        let wrapper_bytes = to_vec(&sig).unwrap();
        let raw_bytes = to_vec(&sig.0.as_slice()).unwrap();
        assert_eq!(wrapper_bytes, raw_bytes);
    }

    #[test]
    fn test_sol_signature_from_solana_real_signature() {
        let keypair = Keypair::new();
        let raw_sig: SolSignatureRaw = keypair.sign_message(b"hello");

        let sol_sig = SolSignature(*raw_sig.as_array());
        let restored: SolSignature = from_slice(&to_vec(&sol_sig).unwrap()).unwrap();

        assert_eq!(sol_sig, restored);
        assert_eq!(restored.0.len(), 64);
    }

    #[test]
    fn test_sol_signature_deserialize_wrong_length_too_short() {
        let bad = vec![0u8; 63];
        let bytes = to_vec(&bad).unwrap();
        let result: Result<SolSignature, _> = from_slice(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_sol_signature_deserialize_wrong_length_too_long() {
        let bad = vec![0u8; 65];
        let bytes = to_vec(&bad).unwrap();
        let result: Result<SolSignature, _> = from_slice(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_sol_signature_encoding_is_deterministic() {
        let sig = random_sol_signature();
        assert_eq!(to_vec(&sig).unwrap(), to_vec(&sig).unwrap());
    }

    #[test]
    fn test_sol_signature_roundtrip_is_stable() {
        let sig = random_sol_signature();
        let bytes = to_vec(&sig).unwrap();
        let restored: SolSignature = from_slice(&bytes).unwrap();
        assert_eq!(bytes, to_vec(&restored).unwrap());
    }

    #[test]
    fn test_sol_signature_all_zeros_roundtrip() {
        let sig = SolSignature([0u8; 64]);
        let restored: SolSignature = from_slice(&to_vec(&sig).unwrap()).unwrap();
        assert_eq!(sig, restored);
    }

    // ---------------------------------------------------------------
    // Signature enum
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_signature_enum_ethereum() {
        let sig = Signature::Ethereum(random_eth_signature());
        let bytes = to_vec(&sig).unwrap();
        let restored: Signature = from_slice(&bytes).unwrap();
        assert_eq!(sig, restored);
    }

    #[test]
    fn test_ser_deser_signature_enum_solana() {
        let sig = Signature::Solana(random_sol_signature());
        let bytes = to_vec(&sig).unwrap();
        let restored: Signature = from_slice(&bytes).unwrap();
        assert_eq!(sig, restored);
    }

    #[test]
    fn test_signature_enum_variants_are_distinct_after_roundtrip() {
        let eth = Signature::Ethereum(random_eth_signature());
        let sol = Signature::Solana(random_sol_signature());

        let eth_restored: Signature = from_slice(&to_vec(&eth).unwrap()).unwrap();
        let sol_restored: Signature = from_slice(&to_vec(&sol).unwrap()).unwrap();

        assert!(matches!(eth_restored, Signature::Ethereum(_)));
        assert!(matches!(sol_restored, Signature::Solana(_)));
        assert_ne!(eth_restored, sol_restored);
    }

    #[test]
    fn test_signature_enum_encoding_is_deterministic() {
        let sig = Signature::Ethereum(random_eth_signature());
        assert_eq!(to_vec(&sig).unwrap(), to_vec(&sig).unwrap());

        let sig = Signature::Solana(random_sol_signature());
        assert_eq!(to_vec(&sig).unwrap(), to_vec(&sig).unwrap());
    }

    #[test]
    fn test_signature_enum_roundtrip_is_stable() {
        for sig in
            [Signature::Ethereum(random_eth_signature()), Signature::Solana(random_sol_signature())]
        {
            let bytes = to_vec(&sig).unwrap();
            let restored: Signature = from_slice(&bytes).unwrap();
            assert_eq!(bytes, to_vec(&restored).unwrap());
        }
    }

    // ---------------------------------------------------------------
    // Cross-cutting sanity
    // ---------------------------------------------------------------

    #[test]
    fn test_eth_and_sol_signature_bytes_differ_in_length() {
        let eth = random_eth_signature();
        let sol = random_sol_signature();

        assert_eq!(eth.0.len(), 65);
        assert_eq!(sol.0.len(), 64);
        assert_ne!(eth.0.len(), sol.0.len());
    }

    #[test]
    fn test_signature_roundtrip_many_random_values() {
        for _ in 0..100 {
            let eth = Signature::Ethereum(random_eth_signature());
            let sol = Signature::Solana(random_sol_signature());

            assert_eq!(eth, from_slice::<Signature>(&to_vec(&eth).unwrap()).unwrap());
            assert_eq!(sol, from_slice::<Signature>(&to_vec(&sol).unwrap()).unwrap());
        }
    }
}
