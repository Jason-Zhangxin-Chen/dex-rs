//! Address defines different blockchain's account address.

pub enum Address {
    Ethereum(EthAddress),
    Solana(SolAddress),
}

pub struct EthAddress(pub [u8; 20]);

pub struct SolAddress(pub [u8; 32]);
