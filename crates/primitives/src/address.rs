//! Address defines different blockchain's account address.

use serde::{Deserialize, Serialize};

/// Address with different L1 chain implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Address {
    /// Ethereum account address.
    Ethereum(EthAddress),
    /// Solana account address.
    Solana(SolAddress),
}

/// A 20-byte ethereum account address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EthAddress(pub [u8; 20]);

/// A 32-byte solana account address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SolAddress(pub [u8; 32]);

#[cfg(test)]
mod tests {
    use alloy::primitives::Address as AlloyAddress;
    use alloy::signers::local::PrivateKeySigner;
    use rmp_serde::{from_slice, to_vec};
    use solana_sdk::signer::{Signer, keypair::Keypair};

    use crate::address::{Address, EthAddress, SolAddress};

    // ---------------------------------------------------------------
    // Helper constructors for test data
    // ---------------------------------------------------------------

    fn random_eth_address() -> EthAddress {
        let signer = PrivateKeySigner::random();
        let address: AlloyAddress = signer.address();
        EthAddress(address.0.0)
    }

    fn random_sol_address() -> SolAddress {
        let keypair = Keypair::new();
        SolAddress(keypair.pubkey().to_bytes())
    }

    // ---------------------------------------------------------------
    // Individual new type tests
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_eth_address() {
        let eth_address = random_eth_address();
        let bytes = to_vec(&eth_address).unwrap();
        let restored: EthAddress = from_slice(&bytes).unwrap();
        assert_eq!(eth_address, restored);
    }

    #[test]
    fn test_ser_deser_solana_address() {
        let sol_address = random_sol_address();
        let bytes = to_vec(&sol_address).unwrap();
        let restored: SolAddress = from_slice(&bytes).unwrap();
        assert_eq!(sol_address, restored);
    }

    // ---------------------------------------------------------------
    // Enum round-trip tests
    // ---------------------------------------------------------------

    #[test]
    fn test_ser_deser_address_enum_ethereum() {
        let address = Address::Ethereum(random_eth_address());
        let bytes = to_vec(&address).unwrap();
        let restored: Address = from_slice(&bytes).unwrap();
        assert_eq!(address, restored);
    }

    #[test]
    fn test_ser_deser_address_enum_solana() {
        let address = Address::Solana(random_sol_address());
        let bytes = to_vec(&address).unwrap();
        let restored: Address = from_slice(&bytes).unwrap();
        assert_eq!(address, restored);
    }

    // ---------------------------------------------------------------
    // Enum-level property tests
    // ---------------------------------------------------------------

    #[test]
    fn test_enum_variants_are_distinct_after_roundtrip() {
        // Ensure the enum tag is preserved: an Ethereum variant must not
        // come back as a Solana variant, even if the payload bytes could
        // theoretically be interpreted as either.
        let eth = Address::Ethereum(random_eth_address());
        let sol = Address::Solana(random_sol_address());

        let eth_restored: Address = from_slice(&to_vec(&eth).unwrap()).unwrap();
        let sol_restored: Address = from_slice(&to_vec(&sol).unwrap()).unwrap();

        assert!(matches!(eth_restored, Address::Ethereum(_)));
        assert!(matches!(sol_restored, Address::Solana(_)));
        assert_ne!(eth_restored, sol_restored);
    }

    #[test]
    fn test_enum_encoding_is_deterministic() {
        // Encoding the same value twice must produce identical bytes.
        let address = Address::Ethereum(random_eth_address());
        let a = to_vec(&address).unwrap();
        let b = to_vec(&address).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn test_enum_roundtrip_is_stable() {
        // encode(decode(bytes)) == bytes
        let address = Address::Solana(random_sol_address());
        let bytes = to_vec(&address).unwrap();
        let restored: Address = from_slice(&bytes).unwrap();
        let bytes2 = to_vec(&restored).unwrap();
        assert_eq!(bytes, bytes2);
    }

    #[test]
    fn test_enum_default_variants_roundtrip() {
        // The inner types derive Default; ensure zero-filled addresses
        // round-trip cleanly too.
        let eth = Address::Ethereum(EthAddress::default());
        let sol = Address::Solana(SolAddress::default());

        assert_eq!(eth, from_slice::<Address>(&to_vec(&eth).unwrap()).unwrap());
        assert_eq!(sol, from_slice::<Address>(&to_vec(&sol).unwrap()).unwrap());
    }
}
