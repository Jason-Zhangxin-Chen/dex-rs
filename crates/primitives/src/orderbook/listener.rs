use crate::event::PriceLevelChangedEvent;
use crate::order::Order;
use crate::orderbook::order_status::OrderStatus;
use crate::orderbook::statistics::{BookStatistics, PriceLevelStatistics};
use crate::trade::TradeResult;

/// Trade listener push trade event to the settlement services, to the storage infra and to the
/// external messaging service.
pub type TradeListener = Box<dyn Fn(&TradeResult)>;
/// Price level change event listener push changes of price level to the external system, UI etc...
pub type PriceLevelChangedListener = Box<dyn Fn(PriceLevelChangedEvent)>;
/// Order status listener push the latest order state and its status to the external of the core.
pub type OrderStatusListener = Box<dyn Fn(Order, &OrderStatus)>;
/// Statistics listener push the book and price level statistics to the external system.
pub type StatisticListener = Box<dyn Fn(BookStatistics, Vec<PriceLevelStatistics>)>;

/// Listeners collect a set of callback closure to notify engine event to external system.
/// They are none blocking functions.
#[derive(Default)]
pub struct Listeners {
    /// Trade listener listens to possible trades when an order is added.
    trade_listener: Option<TradeListener>,

    /// Price level change listener listens to price level changes and push it to external system.
    price_level_changed_listener: Option<PriceLevelChangedListener>,

    /// Order status listener listens to order status and push it to external system.
    order_status_listener: Option<OrderStatusListener>,

    /// Statistic listener listens to the statistic changes event and push it to external system.
    statistic_listener: Option<StatisticListener>,
}

impl Listeners {
    /// Set the Trade listener.
    pub fn with_trade_listener(mut self, trade_listener: TradeListener) -> Self {
        self.trade_listener = Some(trade_listener);
        self
    }

    /// Set the price level change listener.
    pub fn with_price_level_change_listener(
        mut self,
        price_level_changed_listener: PriceLevelChangedListener,
    ) -> Self {
        self.price_level_changed_listener = Some(price_level_changed_listener);
        self
    }

    /// Set the order status listener.
    pub fn with_order_status_listener(
        mut self,
        order_status_listener: OrderStatusListener,
    ) -> Self {
        self.order_status_listener = Some(order_status_listener);
        self
    }

    /// Set the statistic listener.
    pub fn with_statistic_listener(mut self, statistic_listener: StatisticListener) -> Self {
        self.statistic_listener = Some(statistic_listener);
        self
    }
}
