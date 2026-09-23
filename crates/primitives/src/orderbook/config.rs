//! Config for the orderbook.

use crate::base::Symbol;
use crate::orderbook::risk::ReferencePriceSource;
use crate::orderbook::stp::STPMode;
use crate::value::{Price, Quantity};
use serde::{Deserialize, Serialize};

/// BookConfig owns the config data of the book.
#[repr(C)]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BookConfig {
    /// hot configs to be loaded during runtime.
    pub hot: BookConfigHot,

    /// cold configs to be used during setup.
    pub cold: BookConfigCold,
}

impl BookConfig {
    /// Sets the hot configs of the book.
    pub fn with_hot(mut self, hot: BookConfigHot) -> Self {
        self.hot = hot;
        self
    }

    /// Sets the cold configs of the book.
    pub fn with_cold(mut self, cold: BookConfigCold) -> Self {
        self.cold = cold;
        self
    }
}

/// BookConfigCold owns the cold configs of the order book.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct BookConfigCold {
    /// Market Symbol for the book.
    pub symbol: Symbol,

    /// Initial capacity of pre-allocated [`OrderNode`] slab.
    pub arena_size: Option<u32>,

    /// Initial capacity of order index map.
    pub order_index_size: Option<u32>,

    /// Initial capacity of user orders map.
    pub user_order_map_size: Option<u32>,

    /// Initial capacity of a trade list.
    pub trade_list_size: Option<u32>,

    /// Initial capacity of cache of order index list pool.
    pub order_index_list_pool_size: Option<u32>,

    /// Initial capacity of index list.
    pub order_index_list_size: Option<u32>,

    /// Initial capacity of the price level list pool.
    pub price_level_statistic_list_pool_size: Option<u32>,

    /// Initial capacity of the price level list.
    pub price_level_statistic_list_size: Option<u32>,

    /// Initial capacity of sorted map of price levels.
    pub price_level_map_size: Option<u16>,

    /// Initial capacity of cache of trade list pool.
    pub trade_list_pool_size: Option<u8>,
}

impl BookConfigCold {
    /// Sets the capacity of the pre-allocated order node slab.
    pub fn with_arena_size(mut self, arena_size: u32) -> Self {
        self.arena_size = Some(arena_size);
        self
    }

    /// Sets the capacity of the order index map.
    pub fn with_order_index_size(mut self, order_index_size: u32) -> Self {
        self.order_index_size = Some(order_index_size);
        self
    }

    /// Sets the capacity of the user orders map.
    pub fn with_user_order_map_size(mut self, user_order_map_size: u32) -> Self {
        self.user_order_map_size = Some(user_order_map_size);
        self
    }

    /// Sets the capacity of the sorted map of price levels.
    pub fn with_map_price_level_size(mut self, map_price_level_size: u16) -> Self {
        self.price_level_map_size = Some(map_price_level_size);
        self
    }

    /// Sets the capacity of the price level list size.
    pub fn with_price_lvl_statistic_list_size(
        mut self,
        price_lvl_statistic_list_size: u32,
    ) -> Self {
        self.price_level_statistic_list_size = Some(price_lvl_statistic_list_size);
        self
    }

    /// Sets the capacity of the price level list pool size.
    pub fn with_price_lvl_statistic_list_pool_size(
        mut self,
        price_lvl_statistic_list_pool_size: u32,
    ) -> Self {
        self.price_level_statistic_list_pool_size = Some(price_lvl_statistic_list_pool_size);
        self
    }

    /// Set the capacity of the order index list pool size.
    pub fn with_order_index_list_pool_size(mut self, order_index_list_size: u32) -> Self {
        self.order_index_list_pool_size = Some(order_index_list_size);
        self
    }

    /// Set the capacity of the order index list size.
    pub fn with_order_index_list_size(mut self, order_index_list_size: u32) -> Self {
        self.order_index_list_size = Some(order_index_list_size);
        self
    }

    /// Set the capacity of the trade list pool size.
    pub fn with_trade_list_pool_size(mut self, trade_list_pool_size: u8) -> Self {
        self.trade_list_pool_size = Some(trade_list_pool_size);
        self
    }

    /// Set the capacity of the trade list size.
    pub fn with_trade_list_size(mut self, trade_list_size: u32) -> Self {
        self.trade_list_size = Some(trade_list_size);
        self
    }

    /// Sets the market symbol for the book.
    pub fn with_symbol(mut self, symbol: Symbol) -> Self {
        self.symbol = symbol;
        self
    }
}

/// BookConfigHot owns the hot configs for the book that are used frequently by the book.
#[repr(C)]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    pub max_order_size: Option<Quantity>,

    /// Risk config bound to the book.
    pub risk_config: Option<RiskConfig>,

    /// STP mode controls the engine behavior over self-trade events.
    pub stp_mode: STPMode,
}

impl BookConfigHot {
    /// Sets the minimum price increment for orders.
    pub fn with_tick_size(mut self, tick_size: Price) -> Self {
        self.tick_size = Some(tick_size);
        self
    }

    /// Sets the minimum quantity increment for orders.
    pub fn with_lot_size(mut self, lot_size: Quantity) -> Self {
        self.lot_size = Some(lot_size);
        self
    }

    /// Sets the minimum order size.
    pub fn with_min_order_size(mut self, min_order_size: Quantity) -> Self {
        self.min_order_size = Some(min_order_size);
        self
    }

    /// Sets the maximum order size.
    pub fn with_max_order_size(mut self, max_order_size: Quantity) -> Self {
        self.max_order_size = Some(max_order_size);
        self
    }

    /// Sets the risk config bound to the book.
    pub fn with_risk_config(mut self, risk_config: RiskConfig) -> Self {
        self.risk_config = Some(risk_config);
        self
    }

    /// Sets the STP mode that controls the engine behavior over self-trade events.
    pub fn with_stp_mode(mut self, stp_mode: STPMode) -> Self {
        self.stp_mode = stp_mode;
        self
    }
}

/// RiskConfig of an orderbook.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskConfig {
    /// Maximum notional (`price × quantity`, in raw ticks) a single
    /// account may have resting on this book at any time. `None`
    /// disables the check.
    pub max_notional_per_account: Option<u128>,
    /// Maximum allowed deviation in basis points between an incoming
    /// limit price and the resolved reference price. `None` (or
    /// `reference_price = None`) disables the check.
    pub price_band_bps: Option<u32>,
    /// Maximum number of resting orders a single account may have on
    /// this book at any time. `None` disables the check.
    pub max_open_orders_per_account: Option<u32>,
    /// Reference price source used by the price-band check.
    pub reference_price: Option<ReferencePriceSource>,
}

impl RiskConfig {
    /// Sets the maximum notional (`price × quantity`, in raw ticks) a single
    /// account may have resting on this book at any time.
    pub fn with_max_notional_per_account(mut self, max_notional_per_account: u128) -> Self {
        self.max_notional_per_account = Some(max_notional_per_account);
        self
    }

    /// Sets the maximum allowed deviation in basis points between an incoming
    /// limit price and the resolved reference price.
    pub fn with_price_band_bps(mut self, price_band_bps: u32) -> Self {
        self.price_band_bps = Some(price_band_bps);
        self
    }

    /// Sets the maximum number of resting orders a single account may have on
    /// this book at any time.
    pub fn with_max_open_orders_per_account(mut self, max_open_orders_per_account: u32) -> Self {
        self.max_open_orders_per_account = Some(max_open_orders_per_account);
        self
    }

    /// Sets the reference price source used by the price-band check.
    pub fn with_reference_price(mut self, reference_price: ReferencePriceSource) -> Self {
        self.reference_price = Some(reference_price);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // BookConfigHot
    // ---------------------------------------------------------------

    #[test]
    fn test_hot_config_defaults() {
        let hot = BookConfigHot::default();
        assert_eq!(hot.tick_size, None);
        assert_eq!(hot.lot_size, None);
        assert_eq!(hot.min_order_size, None);
        assert_eq!(hot.max_order_size, None);
        assert_eq!(hot.risk_config, None);
        assert_eq!(hot.stp_mode, STPMode::None);
    }

    #[test]
    fn test_hot_config_with_setters() {
        let risk_config = RiskConfig::default().with_max_open_orders_per_account(8);
        let hot = BookConfigHot::default()
            .with_tick_size(Price(1))
            .with_lot_size(Quantity(2))
            .with_min_order_size(Quantity(3))
            .with_max_order_size(Quantity(4))
            .with_risk_config(risk_config.clone())
            .with_stp_mode(STPMode::CancelBoth);
        assert_eq!(hot.tick_size, Some(Price(1)));
        assert_eq!(hot.lot_size, Some(Quantity(2)));
        assert_eq!(hot.min_order_size, Some(Quantity(3)));
        assert_eq!(hot.max_order_size, Some(Quantity(4)));
        assert_eq!(hot.risk_config, Some(risk_config));
        assert_eq!(hot.stp_mode, STPMode::CancelBoth);
    }

    // ---------------------------------------------------------------
    // BookConfigCold
    // ---------------------------------------------------------------

    #[test]
    fn test_cold_config_defaults() {
        let cold = BookConfigCold::default();
        assert_eq!(cold.arena_size, None);
        assert_eq!(cold.order_index_size, None);
        assert_eq!(cold.user_order_map_size, None);
        assert_eq!(cold.price_level_map_size, None);
        assert_eq!(cold.order_index_list_pool_size, None);
        assert_eq!(cold.order_index_list_size, None);
        assert_eq!(cold.trade_list_pool_size, None);
        assert_eq!(cold.price_level_statistic_list_pool_size, None);
        assert_eq!(cold.price_level_statistic_list_size, None);
        assert_eq!(cold.trade_list_size, None);
        assert_eq!(cold.symbol, Symbol::default());
    }

    #[test]
    fn test_cold_config_with_setters() {
        let cold = BookConfigCold::default()
            .with_arena_size(1_000)
            .with_order_index_size(2_000)
            .with_user_order_map_size(3_000)
            .with_map_price_level_size(256)
            .with_order_index_list_pool_size(128u32)
            .with_order_index_list_size(u32::MAX)
            .with_trade_list_size(2_000u32)
            .with_trade_list_pool_size(128u8)
            .with_price_lvl_statistic_list_pool_size(u32::MAX)
            .with_price_lvl_statistic_list_size(u32::MAX)
            .with_symbol(Symbol([1u8; 32]));
        assert_eq!(cold.arena_size, Some(1_000));
        assert_eq!(cold.order_index_size, Some(2_000));
        assert_eq!(cold.user_order_map_size, Some(3_000));
        assert_eq!(cold.price_level_map_size, Some(256));
        assert_eq!(cold.order_index_list_pool_size, Some(128));
        assert_eq!(cold.order_index_list_size, Some(u32::MAX));
        assert_eq!(cold.trade_list_size, Some(2_000u32));
        assert_eq!(cold.trade_list_pool_size, Some(128u8));
        assert_eq!(cold.price_level_statistic_list_size, Some(u32::MAX));
        assert_eq!(cold.price_level_statistic_list_pool_size, Some(u32::MAX));
        assert_eq!(cold.symbol, Symbol([1u8; 32]));
    }

    // ---------------------------------------------------------------
    // BookConfig
    // ---------------------------------------------------------------

    #[test]
    fn test_book_config_defaults() {
        let config = BookConfig::default();
        assert_eq!(config.hot.tick_size, None);
        assert_eq!(config.hot.lot_size, None);
        assert_eq!(config.hot.min_order_size, None);
        assert_eq!(config.hot.max_order_size, None);
        assert_eq!(config.hot.risk_config, None);
        assert_eq!(config.hot.stp_mode, STPMode::None);
        assert_eq!(config.cold.arena_size, None);
        assert_eq!(config.cold.order_index_size, None);
        assert_eq!(config.cold.user_order_map_size, None);
        assert_eq!(config.cold.price_level_map_size, None);
        assert_eq!(config.cold.order_index_list_pool_size, None);
        assert_eq!(config.cold.order_index_list_size, None);
        assert_eq!(config.cold.trade_list_size, None);
        assert_eq!(config.cold.trade_list_pool_size, None);
        assert_eq!(config.cold.symbol, Symbol::default());
    }

    #[test]
    fn test_book_config_composes_hot_and_cold() {
        let config = BookConfig::default()
            .with_hot(BookConfigHot::default().with_tick_size(Price(5)))
            .with_cold(BookConfigCold::default().with_symbol(Symbol([9u8; 32])));
        assert_eq!(config.hot.tick_size, Some(Price(5)));
        assert_eq!(config.hot.stp_mode, STPMode::None);
        assert_eq!(config.cold.symbol, Symbol([9u8; 32]));
        assert_eq!(config.cold.arena_size, None);
    }
}
