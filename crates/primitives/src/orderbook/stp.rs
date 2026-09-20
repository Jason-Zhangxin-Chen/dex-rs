//! Self-Trade Prevention (STP) types and logic.
//!
//! Self-Trade Prevention prevents orders from the same user from matching
//! against each other in the order book. This is a critical exchange feature
//! that prevents wash trading.
//!
//! # Modes
//!
//! - `STPMode::None` — No STP checks (default, zero overhead).
//! - `STPMode::CancelTaker` — Cancel the incoming (taker) order on self-trade.
//! - `STPMode::CancelMaker` — Cancel the resting (maker) order and continue matching.
//! - `STPMode::CancelBoth` — Cancel both taker and maker orders.
//!
//! # Reachability
//!
//! `CancelTaker` and `CancelBoth` fire only when the sweep can still
//! execute into the same-user maker after the non-self depth queued ahead
//! of it; `CancelMaker` cancels every same-user order at a level the sweep
//! touches. See [`STPMode`](crate::orderbook::stp::STPMode) for the full
//! rule and its known asymmetry.
//!
//! # Bypass
//!
//! Orders with `user_id == Address::zero()` (anonymous) always bypass STP checks,
//! regardless of the configured mode.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[repr(u8)]
pub enum STPMode {
    /// No self-trade prevention (default). Orders from the same user can
    /// match freely. This mode adds zero overhead to the matching engine.
    #[default]
    None = 0,

    /// Cancel the incoming (taker) order when a self-trade would occur.
    /// Resting orders remain in the book. Partial fills against different
    /// users that precede the self-trade are kept.
    CancelTaker = 1,

    /// Cancel the resting (maker) order(s) from the same user and continue
    /// matching the taker against remaining orders. All same-user resting
    /// orders at each price level are removed before matching proceeds.
    CancelMaker = 2,

    /// Cancel both the incoming (taker) and the resting (maker) order.
    /// Matching stops immediately. Partial fills against different users
    /// that precede the self-trade are kept.
    CancelBoth = 3,
}