//! Signature defines different blockchain's signature.

/// The signatures from different L1 protocol.
pub enum Signature {
    /// Ethereum signature.
    Ethereum(EthSignature),
    /// Solana signature.
    Solana(SolSignature),
}

/// The implementation of ethereum signature.
pub struct EthSignature(pub [u8; 65]);

/// The implementation of solana signature.
pub struct SolSignature(pub [u8; 64]);
