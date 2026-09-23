use crate::base::Side;
use crate::orderbook::statistics::{BookStatistics, PriceLevelStatistics};
use crate::value::{Price, Quantity};
use serde::{Deserialize, Serialize};

/// Event data for orderbook price level changes.
/// It is assumed that the listener is aware of the
/// order book context so we are not adding symbol here.
/// This event is sent on operations that update the order book price levels
/// e.g. adding, cancelling, updating or matching order
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PriceLevelChangedEvent {
    /// the order book side of the price level
    side: Side,

    /// price level price
    price: Price,

    /// latest visible quantity of the order book at this price level
    quantity: Quantity,
}

impl PriceLevelChangedEvent {
    /// Creates a new price level changed event.
    pub fn new(side: Side, price: Price, quantity: Quantity) -> Self {
        Self { side, price, quantity }
    }

    /// Sets the order book side of the price level.
    pub fn with_side(mut self, side: Side) -> Self {
        self.side = side;
        self
    }

    /// Sets the price level price.
    pub fn with_price(mut self, price: Price) -> Self {
        self.price = price;
        self
    }

    /// Sets the latest visible quantity of the order book at this price level.
    pub fn with_quantity(mut self, quantity: Quantity) -> Self {
        self.quantity = quantity;
        self
    }
}

/// Statistic event carries the orderbook statistic metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatisticsEvent {
    /// List of PriceLevelStatistics of the latest book. The vector<PriceLevelStatistics> is pooled
    /// in the free cache with RAII guard.
    price_level_statistics: Vec<PriceLevelStatistics>,
    /// Book Statistics.
    book_statistics: BookStatistics,
}

impl StatisticsEvent {
    /// Creates a new statistic event.
    pub fn new(
        price_level_statistics: Vec<PriceLevelStatistics>,
        book_statistics: BookStatistics,
    ) -> Self {
        Self { price_level_statistics, book_statistics }
    }

    /// Sets the price level statistics of the latest book.
    pub fn with_price_level_statistics(
        mut self,
        price_level_statistics: Vec<PriceLevelStatistics>,
    ) -> Self {
        self.price_level_statistics = price_level_statistics;
        self
    }

    /// Sets the book statistics.
    pub fn with_book_statistics(mut self, book_statistics: BookStatistics) -> Self {
        self.book_statistics = book_statistics;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    // ---------------------------------------------------------------
    // PriceLevelChangedEvent
    // ---------------------------------------------------------------

    #[test]
    fn test_price_level_changed_constructor() {
        let event = PriceLevelChangedEvent::new(Side::Sell, Price(100), Quantity(10));
        assert_eq!(event.side, Side::Sell);
        assert_eq!(event.price, Price(100));
        assert_eq!(event.quantity, Quantity(10));
    }

    #[test]
    fn test_price_level_changed_with_setters() {
        let event = PriceLevelChangedEvent::new(Side::Buy, Price(1), Quantity(2))
            .with_side(Side::Sell)
            .with_price(Price(3))
            .with_quantity(Quantity(4));
        assert_eq!(event.side, Side::Sell);
        assert_eq!(event.price, Price(3));
        assert_eq!(event.quantity, Quantity(4));
    }

    // ---------------------------------------------------------------
    // StatisticsEvent
    // ---------------------------------------------------------------

    /// Builds a non-zero [`BookStatistics`] through the wire format since its fields are
    /// private, see `BookStatisticsMirror`.
    fn sample_book_statistics() -> BookStatistics {
        let mirror = BookStatisticsMirror {
            orders_added: 1,
            orders_removed: 2,
            orders_executed: 3,
            quantity_executed: 4,
            value_executed: 5,
        };
        rmp_serde::from_slice(&rmp_serde::to_vec(&mirror).unwrap()).unwrap()
    }

    /// Mirror of [`BookStatistics`]'s wire shape.
    #[derive(Serialize)]
    struct BookStatisticsMirror {
        orders_added: usize,
        orders_removed: usize,
        orders_executed: usize,
        quantity_executed: usize,
        value_executed: u64,
    }

    #[test]
    fn test_statistics_event_default() {
        let event = StatisticsEvent::default();
        assert!(event.price_level_statistics.is_empty());
        assert_eq!(event.book_statistics, BookStatistics::default());
    }

    #[test]
    fn test_statistics_event_constructor() {
        let levels = vec![PriceLevelStatistics::default()];
        let stats = sample_book_statistics();
        let event = StatisticsEvent::new(levels.clone(), stats);
        assert_eq!(event.price_level_statistics, levels);
        assert_eq!(event.book_statistics, stats);
    }

    #[test]
    fn test_statistics_event_with_setters() {
        let event = StatisticsEvent::default()
            .with_price_level_statistics(vec![PriceLevelStatistics::default()])
            .with_book_statistics(sample_book_statistics());
        assert_eq!(event.price_level_statistics, vec![PriceLevelStatistics::default()]);
        assert_eq!(event.book_statistics, sample_book_statistics());
    }
}
