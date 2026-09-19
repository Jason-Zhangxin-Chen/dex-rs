//! The definition of orders

use crate::address::Address;
use crate::base::{Hash32, Nonce, PegReferenceType, Side, Symbol};
use crate::signature::Signature;
use crate::time_in_force::TimeInForce;
use crate::value::{Price, Quantity, TimestampMs};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// Order represents different types of orders
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
