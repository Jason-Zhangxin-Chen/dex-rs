//! Base order definitions.

use serde::{Deserialize, Serialize};

/// Symbol represents the symbol of a product.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Symbol(pub [u8; 32]);

/// Side represents the side of an order.
#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    /// Buy side (bids)
    Buy,

    /// Sell side (asks)
    Sell,
}

/// Hash32 represents 32 bytes hash.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hash32(pub [u8; 32]);

/// Nonce represents a sequence number of user's order.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Nonce(pub u64);

/// Reference price type for pegged orders.
#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Quote(pub u128);

/// Fee for the takers or makers
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Fee(pub i128);
