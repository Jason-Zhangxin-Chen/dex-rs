//! Base order definitions.

/// Side represents the side of an order.
#[derive(Debug, Clone, Copy)]
pub enum Side {
    /// Buy side (bids)
    Buy,

    /// Sell side (asks)
    Sell,
}

/// Hash32 represents 32 bytes hash.
pub struct Hash32(pub [u8; 32]);

/// Nonce represents a sequence number of user's order.
pub struct Nonce(pub u64);

/// Reference price type for pegged orders.
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
