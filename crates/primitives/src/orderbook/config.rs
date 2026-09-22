//! Config for the orderbook.

use serde::{Deserialize, Serialize};
use crate::base::Symbol;
use crate::orderbook::risk::RiskConfig;
use crate::orderbook::stp::STPMode;
use crate::value::{Price, Quantity};

/// BookConfig owns the config data of the book.
#[repr(C)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookConfig {
    /// hot configs to be loaded during runtime.
    pub hot: BookConfigHot,

    /// cold configs to be used during setup.
    pub cold: BookConfigCold,
}

/// BookConfigCold owns the cold configs of the order book.
#[repr(C)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BookConfigCold {
    /// Capacity of pre-allocated [`OrderNode`] slab.
    pub arena_size: Option<u32>,

    /// Capacity of order index map.
    pub order_index_size: Option<u32>,

    /// Capacity of user orders map.
    pub user_order_map_size: Option<u32>,

    /// Capacity of sorted map of price levels.
    pub map_price_level_size: Option<u16>,
    
    /// Market Symbol for the book.
    pub symbol: Symbol,
}

/// BookConfigHot owns the hot configs for the book that are used frequently by the book.
#[repr(C)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookConfigHot {
    /// Minimum price increment for orders. When set, order prices must be
    /// exact multiples of this value. `None` disables validation (default).
    pub tick_size: Option<Price>,

    /// Minimum quantity increment for orders. When set, order quantities must be
    /// exact multiples of this value. `None` disables validation (default).
    pub lot_size: Option<Quantity>,

    /// Minimum order size. When set, orders with `total_quantity() < min` are
    /// rejected. `None` disables validation (default).
    pub min_order_size: Option<Quantity>,

    /// Maximum order size. When set, orders with `total_quantity() > max` are
    /// rejected. `None` disables validation (default).
    max_order_size: Option<Quantity>,

    /// Risk config bound to the book.
    pub risk_config: Option<RiskConfig>,

    /// STP mode controls the engine behavior over self-trade events.
    pub stp_mode: STPMode,
}


