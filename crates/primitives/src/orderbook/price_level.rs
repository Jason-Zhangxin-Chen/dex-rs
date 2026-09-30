//! The definition of a price level.

use crate::address::Address;
use crate::base::Side;
use crate::message::hot_path::Trade;
use crate::message::side_path::{CancelReason, OrderChange, OrderStatus};
use crate::order::{DEFAULT_RESERVE_REPLENISH_AMOUNT, NIL, Order, OrderIdx, OrderKind, OrderNode};
use crate::orderbook::pooled::MemoryPools;
use crate::orderbook::statistics::PriceLevelStatistics;
use crate::orderbook::stp::STPMode;
use crate::time_in_force::TimeInForce;
use crate::value::{Price, Quantity, TimestampMs};
use cache::object_pool::CacheGuard;
use serde::{Deserialize, Serialize};
use slab::Slab;

/// Errors of the price level execution. The level currently cannot fail; the
/// variant-less enum keeps the fallible signature stable for future checks.
#[derive(Debug)]
pub enum PriceLevelError {}

/// The output of executing a taker order against one price level. The output
/// buffers are checked out from the book's memory pools and return to them
/// when the output is dropped.
pub struct PriceLevelExecution {
    /// Order changes generated at this level: maker fills, STP cancellations
    /// and lazy time-in-force expiries.
    pub changes: CacheGuard<Vec<OrderChange>>,
    /// Trades crossed at this level, all at the level price.
    pub trades: CacheGuard<Vec<Trade>>,
    /// Orders that left the queue at this level, fully filled or cancelled.
    /// They have been unlinked from the queue already; the book still has to
    /// purge them from the arena, the index, the user order map and the risk
    /// state.
    pub removed: CacheGuard<Vec<OrderIdx>>,
    /// The taker was killed by STP logic, the book sweep must stop.
    pub taker_killed: bool,
}

/// A price level in a limit order book, lock-free on the match path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    /// The order queue in time priority.
    orders: OrderQueue,

    /// Total visible quantity of this price level.
    visible_quantity: Quantity,

    /// Total hidden quantity of this price level.
    hidden_quantity: Quantity,

    /// The Side: Buy or Sell.
    side: Side,

    /// The statistics of the price level. OMS_Master skip this for performance, the statistic
    /// task is done by OMS_Slave which replicates the book.
    stats: Option<PriceLevelStatistics>,

    /// The price of the level.
    price: Price,
}

/// Builders for the price level. They are used for creating a new price level by the orderbook when
/// a new order ingested and there is no corresponding price level to store it. In such case, the
/// book have to construct the price level with Price, and stats which counts this ingesting order,
/// Side, hidden quantity if this order contains hidden quantity, visible quantity of this ingesting
/// one and the OrderQueue which contains this ingesting order:
/// {tail: ingesting order IDX, head: ingesting order IDX, len: 1}
impl PriceLevel {
    /// Constructs a new price level hosting the ingesting order `head` as its
    /// single queue entry. `visible` and `hidden` seed the level totals.
    pub fn new(
        price: Price,
        side: Side,
        stats: Option<PriceLevelStatistics>,
        head: OrderIdx,
        visible: Quantity,
        hidden: Quantity,
    ) -> Self {
        Self {
            orders: OrderQueue { head, tail: head, len: 1 },
            visible_quantity: visible,
            hidden_quantity: hidden,
            side,
            stats,
            price,
        }
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    /// The number of orders resting at the level.
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    /// The price of the level.
    pub fn price(&self) -> Price {
        self.price
    }

    /// The total visible quantity of the level.
    pub fn visible_quantity(&self) -> Quantity {
        self.visible_quantity
    }

    /// The total hidden quantity of the level.
    pub fn hidden_quantity(&self) -> Quantity {
        self.hidden_quantity
    }

    /// Appends a resting order to the tail of the queue and folds its
    /// quantities into the level totals.
    pub fn append(
        &mut self,
        arena: &mut Slab<OrderNode>,
        idx: OrderIdx,
        visible: Quantity,
        hidden: Quantity,
    ) {
        self.orders.push_back(arena, idx);
        self.visible_quantity.0 += visible.0;
        self.hidden_quantity.0 += hidden.0;
    }

    /// Removes an order from the queue without matching it (user cancel, mass
    /// cancel). The caller purges the order from the remaining book structures
    /// afterwards.
    pub fn remove(&mut self, arena: &mut Slab<OrderNode>, idx: OrderIdx) {
        let (visible, hidden) = {
            let node = arena.get(idx as usize).expect("queued order exists in the arena");
            (node.hot.quantity, node.hidden_quantity())
        };
        self.orders.remove(arena, idx);
        self.visible_quantity.0 -= visible.0;
        self.hidden_quantity.0 -= hidden.0;
    }

    /// The quantity a sweep can still extract from this level before it is
    /// stopped: the per-order visible plus replenishable hidden, minus the
    /// same-user depth the configured STP mode cancels or kills on. Expired
    /// orders contribute nothing, the sweep cancels them lazily. Used by the
    /// fill-or-kill pre-scan.
    pub fn fillable_quantity(
        &self,
        arena: &Slab<OrderNode>,
        taker_user: Address,
        stp_mode: STPMode,
        now: TimestampMs,
    ) -> Quantity {
        let stp_active = stp_mode != STPMode::None && taker_user != Address::default();
        let mut total = 0u64;
        let mut idx = self.orders.head();
        while idx != NIL {
            let node = arena.get(idx as usize).expect("queued order exists in the arena");
            let order = Order::from(node.clone());
            if order_expired(&order, now) {
                idx = node.next();
                continue;
            }
            if stp_active && order.hot.user == taker_user {
                match stp_mode {
                    // CancelMaker removes the same-user depth before matching,
                    // it contributes nothing to the fillable quantity.
                    STPMode::CancelMaker => {
                        idx = node.next();
                        continue;
                    }
                    // CancelTaker / CancelBoth kill the sweep as soon as it
                    // reaches a same-user maker with quantity left.
                    _ => break,
                }
            }
            total = total.saturating_add(match order.cold.kind {
                OrderKind::Iceberg { hidden_quantity } => order.hot.quantity.0 + hidden_quantity.0,
                OrderKind::ReserveOrder { hidden_quantity, auto_replenish: true, .. } => {
                    order.hot.quantity.0 + hidden_quantity.0
                }
                _ => order.hot.quantity.0,
            });
            idx = node.next();
        }
        Quantity(total)
    }

    /// Executes the taker against the orders of this level in time priority,
    /// it is called by OMS_Master. It generates the changes and the trades of
    /// the level; the book merges the outputs of the levels it sweeps. The
    /// output buffers are checked out from the book's memory pools, so the
    /// hot path performs no heap allocation.
    ///
    /// `taker` is the working state of the ingress order (its visible quantity
    /// decreases with every trade), `taker_original` is the order as submitted,
    /// carried by the [`Trade`] messages.
    pub fn execute(
        &mut self,
        arena: &mut Slab<OrderNode>,
        taker: &mut Order,
        taker_original: &Order,
        stp_mode: STPMode,
        now: TimestampMs,
        pools: &MemoryPools,
    ) -> Result<PriceLevelExecution, PriceLevelError> {
        let mut changes = pools.changes_pool.acquire();
        let mut trades = pools.trades_pool.acquire();
        let mut removed = pools.index_lists_pool.acquire();
        let mut taker_killed = false;

        let taker_user = taker.hot.user;
        // Zero-address orders always bypass the STP checks.
        let stp_active = stp_mode != STPMode::None && taker_user != Address::default();

        // CancelMaker: every same-user resting order at this level is removed
        // before the level is matched.
        if stp_active && stp_mode == STPMode::CancelMaker {
            self.cancel_same_user_orders(arena, taker_user, &mut changes, &mut removed);
        }

        while !self.orders.is_empty() && taker.hot.quantity.0 > 0 && !taker_killed {
            let maker_idx = self.orders.head();
            let maker: Order = arena
                .get(maker_idx as usize)
                .expect("queued order exists in the arena")
                .clone()
                .into();

            // Lazy GTD expiry: an expired resting order never matches, it is
            // cancelled the moment the sweep touches it.
            if order_expired(&maker, now) {
                let order = self.pop_order(arena);
                removed.push(maker_idx);
                changes.push(OrderChange::new(
                    order,
                    OrderStatus::Canceled {
                        filled_quantity: Quantity::ZERO,
                        reason: CancelReason::TimeInForceExpired,
                    },
                ));
                continue;
            }

            // STP: the sweep reached a same-user maker while the taker still
            // has quantity left to place.
            if stp_active && maker.hot.user == taker_user {
                match stp_mode {
                    STPMode::CancelTaker => taker_killed = true,
                    STPMode::CancelBoth => {
                        let order = self.pop_order(arena);
                        removed.push(maker_idx);
                        changes.push(OrderChange::new(
                            order,
                            OrderStatus::Canceled {
                                filled_quantity: Quantity::ZERO,
                                reason: CancelReason::SelfTradePrevention,
                            },
                        ));
                        taker_killed = true;
                    }
                    // CancelMaker was handled in the pre-pass above.
                    STPMode::CancelMaker | STPMode::None => {}
                }
                if taker_killed {
                    break;
                }
            }

            // The trade happens at the level price.
            let maker_visible = maker.hot.quantity;
            let traded = Quantity(maker_visible.0.min(taker.hot.quantity.0));
            {
                let maker_node =
                    arena.get_mut(maker_idx as usize).expect("queued order exists in the arena");
                maker_node.hot.quantity.0 -= traded.0;
                self.visible_quantity.0 -= traded.0;
                taker.hot.quantity.0 -= traded.0;
            }
            trades.push(Trade::new(
                *taker_original,
                taker.total_quantity(),
                maker,
                self.price,
                traded,
            ));

            // Resolve the maker's post-fill state.
            self.settle_maker(arena, maker_idx, maker, traded, &mut changes, &mut removed);
        }

        Ok(PriceLevelExecution { changes, trades, removed, taker_killed })
    }

    /// apply changes to the price level, only ran by the OMS_Slave to replicate the state.
    pub fn apply(&mut self) -> Result<(), PriceLevelError> {
        // todo: implement this
        Ok(())
    }

    /// Pops the queue head and removes its quantities from the level totals.
    /// Returns the order that left the queue.
    fn pop_order(&mut self, arena: &mut Slab<OrderNode>) -> Order {
        let idx = self.orders.pop_front(arena);
        let node = arena.get(idx as usize).expect("queued order exists in the arena");
        let order = Order::from(node.clone());
        self.visible_quantity.0 -= order.hot.quantity.0;
        self.hidden_quantity.0 -= order.hidden_quantity().0;
        order
    }

    /// Removes every resting order of `user` from the queue (STP CancelMaker).
    fn cancel_same_user_orders(
        &mut self,
        arena: &mut Slab<OrderNode>,
        user: Address,
        changes: &mut Vec<OrderChange>,
        removed: &mut Vec<OrderIdx>,
    ) {
        let mut idx = self.orders.head();
        while idx != NIL {
            let (order, next) = {
                let node = arena.get(idx as usize).expect("queued order exists in the arena");
                (Order::from(node.clone()), node.next())
            };
            if order.hot.user == user {
                self.orders.remove(arena, idx);
                self.visible_quantity.0 -= order.hot.quantity.0;
                self.hidden_quantity.0 -= order.hidden_quantity().0;
                removed.push(idx);
                changes.push(OrderChange::new(
                    order,
                    OrderStatus::Canceled {
                        filled_quantity: Quantity::ZERO,
                        reason: CancelReason::SelfTradePrevention,
                    },
                ));
            }
            idx = next;
        }
    }

    /// Resolves the maker's state after a fill: replenishes iceberg / reserve
    /// tranches (re-queuing the maker at the tail to preserve time priority),
    /// or reports the maker as fully filled and pops it.
    ///
    /// The [`OrderChange`] of a fill carries the maker as it was *before* the
    /// fill with the quantity filled by this event; the change of a
    /// cancellation carries the maker as it was at removal with zero filled.
    fn settle_maker(
        &mut self,
        arena: &mut Slab<OrderNode>,
        maker_idx: OrderIdx,
        maker: Order,
        traded: Quantity,
        changes: &mut Vec<OrderChange>,
        removed: &mut Vec<OrderIdx>,
    ) {
        let (remaining_visible, kind) = {
            let node = arena.get(maker_idx as usize).expect("queued order exists in the arena");
            (node.hot.quantity, node.cold.kind)
        };

        if remaining_visible.0 > 0 {
            // The visible tranche survives: a plain partial fill.
            changes.push(OrderChange::new(
                maker,
                OrderStatus::PartiallyFilled { filled_quantity: traded },
            ));

            // An auto-replenishing reserve tops the visible up to its
            // threshold after every fill.
            if let OrderKind::ReserveOrder { auto_replenish: true, .. } = kind {
                let drawn = {
                    let node = arena
                        .get_mut(maker_idx as usize)
                        .expect("queued order exists in the arena");
                    replenish_reserve(node)
                };
                if drawn.0 > 0 {
                    self.hidden_quantity.0 -= drawn.0;
                    self.visible_quantity.0 += drawn.0;
                    self.move_to_tail(arena, maker_idx);
                }
            }
            return;
        }

        // The visible tranche is exhausted.
        match kind {
            OrderKind::Iceberg { hidden_quantity } if hidden_quantity.0 > 0 => {
                // The next tranche has the size of the one just consumed.
                let drawn = {
                    let node = arena
                        .get_mut(maker_idx as usize)
                        .expect("queued order exists in the arena");
                    replenish_iceberg(node, maker.hot.quantity)
                };
                self.hidden_quantity.0 -= drawn.0;
                self.visible_quantity.0 += drawn.0;
                self.move_to_tail(arena, maker_idx);
                changes.push(OrderChange::new(
                    maker,
                    OrderStatus::PartiallyFilled { filled_quantity: traded },
                ));
            }
            OrderKind::ReserveOrder { hidden_quantity, auto_replenish: true, .. }
                if hidden_quantity.0 > 0 =>
            {
                let drawn = {
                    let node = arena
                        .get_mut(maker_idx as usize)
                        .expect("queued order exists in the arena");
                    replenish_reserve(node)
                };
                self.hidden_quantity.0 -= drawn.0;
                self.visible_quantity.0 += drawn.0;
                self.move_to_tail(arena, maker_idx);
                changes.push(OrderChange::new(
                    maker,
                    OrderStatus::PartiallyFilled { filled_quantity: traded },
                ));
            }
            OrderKind::ReserveOrder { auto_replenish: false, .. } => {
                // A non-auto reserve never replenishes: the hidden remainder
                // is discarded together with the exhausted visible tranche.
                self.pop_order(arena);
                removed.push(maker_idx);
                changes
                    .push(OrderChange::new(maker, OrderStatus::Filled { filled_quantity: traded }));
            }
            _ => {
                // No hidden reserve: the maker is fully filled.
                self.pop_order(arena);
                removed.push(maker_idx);
                changes
                    .push(OrderChange::new(maker, OrderStatus::Filled { filled_quantity: traded }));
            }
        }
    }

    /// Re-links the queue head to the tail: a replenished order loses its
    /// time priority slot.
    fn move_to_tail(&mut self, arena: &mut Slab<OrderNode>, idx: OrderIdx) {
        let popped = self.orders.pop_front(arena);
        debug_assert_eq!(popped, idx);
        self.orders.push_back(arena, idx);
    }

    /// Sets the price of the level.
    pub fn with_price(mut self, price: Price) -> Self {
        self.price = price;
        self
    }

    /// Sets the stats of the level.
    pub fn with_stats(mut self, stats: Option<PriceLevelStatistics>) -> Self {
        self.stats = stats;
        self
    }

    /// Sets the side of the level.
    pub fn with_size(mut self, side: Side) -> Self {
        self.side = side;
        self
    }

    /// Sets the hidden quantity.
    pub fn with_hidden_quantity(mut self, quantity: Quantity) -> Self {
        self.hidden_quantity = quantity;
        self
    }

    /// Sets the visible quantity.
    pub fn with_visible_quantity(mut self, quantity: Quantity) -> Self {
        self.visible_quantity = quantity;
        self
    }

    /// Sets the order queue.
    pub fn with_orders(mut self, orders: OrderQueue) -> Self {
        self.orders = orders;
        self
    }
}

/// Whether the order's time in force has expired at `now`. Only GTD orders
/// expire; GTC, IOC, FOK and Day never expire here.
pub(crate) fn order_expired(order: &Order, now: TimestampMs) -> bool {
    match order.hot.time_in_force {
        TimeInForce::Gtd(hours) => {
            let deadline = order
                .cold
                .common
                .timestamp()
                .0
                .saturating_add(u64::from(hours).saturating_mul(3_600_000));
            now.0 >= deadline
        }
        _ => false,
    }
}

/// Draws one visible tranche from the hidden reserve of an iceberg order. The
/// tranche size is the visible quantity the order had before it was consumed.
/// Returns the quantity drawn.
fn replenish_iceberg(node: &mut OrderNode, tranche: Quantity) -> Quantity {
    let OrderKind::Iceberg { hidden_quantity } = node.cold.kind else {
        unreachable!("replenish_iceberg called on a non-iceberg order");
    };
    let draw = Quantity(hidden_quantity.0.min(tranche.0));
    node.hot.quantity = draw;
    node.cold.kind = OrderKind::Iceberg { hidden_quantity: Quantity(hidden_quantity.0 - draw.0) };
    draw
}

/// Replenishes the visible quantity of an auto-replenishing reserve order up
/// to its threshold, drawing `replenish_amount` (or the default) per step.
/// Returns the total quantity drawn.
fn replenish_reserve(node: &mut OrderNode) -> Quantity {
    let OrderKind::ReserveOrder {
        hidden_quantity,
        replenish_threshold,
        replenish_amount,
        auto_replenish,
    } = node.cold.kind
    else {
        unreachable!("replenish_reserve called on a non-reserve order");
    };
    debug_assert!(auto_replenish);
    let threshold = if replenish_threshold.0 == 0 { Quantity(1) } else { replenish_threshold };
    let amount = replenish_amount.map_or(DEFAULT_RESERVE_REPLENISH_AMOUNT, |a| a.get());
    let mut hidden = hidden_quantity;
    let mut drawn = 0u64;
    while node.hot.quantity.0 < threshold.0 && hidden.0 > 0 {
        let draw = hidden.0.min(amount);
        node.hot.quantity.0 += draw;
        hidden.0 -= draw;
        drawn += draw;
    }
    node.cold.kind = OrderKind::ReserveOrder {
        hidden_quantity: hidden,
        replenish_threshold,
        replenish_amount,
        auto_replenish,
    };
    Quantity(drawn)
}

/// OrderQueue in time priority, and fast operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderQueue {
    /// The newest order.
    tail: OrderIdx,

    /// The oldest order next to be matched.
    head: OrderIdx,

    /// The size of the queue.
    len: usize,
}

impl OrderQueue {
    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The number of orders in the queue.
    pub fn len(&self) -> usize {
        self.len
    }

    /// The head of the queue, [`NIL`] when empty.
    pub fn head(&self) -> OrderIdx {
        self.head
    }

    /// The tail of the queue, [`NIL`] when empty.
    pub fn tail(&self) -> OrderIdx {
        self.tail
    }

    /// Appends a node to the tail of the queue.
    pub fn push_back(&mut self, arena: &mut Slab<OrderNode>, idx: OrderIdx) {
        let tail = self.tail;
        {
            let node = arena.get_mut(idx as usize).expect("queued order exists in the arena");
            node.set_prev(tail);
            node.set_next(NIL);
        }
        if tail != NIL {
            arena.get_mut(tail as usize).expect("queued order exists in the arena").set_next(idx);
        }
        self.tail = idx;
        if self.head == NIL {
            self.head = idx;
        }
        self.len += 1;
    }

    /// Pops the head of the queue. The node's links are reset to [`NIL`] and
    /// it keeps its position in the arena.
    pub fn pop_front(&mut self, arena: &mut Slab<OrderNode>) -> OrderIdx {
        debug_assert!(!self.is_empty());
        let head = self.head;
        let next = arena.get(head as usize).expect("queued order exists in the arena").next();
        if next != NIL {
            arena.get_mut(next as usize).expect("queued order exists in the arena").set_prev(NIL);
        } else {
            self.tail = NIL;
        }
        self.head = next;
        self.len -= 1;
        let node = arena.get_mut(head as usize).expect("queued order exists in the arena");
        node.set_prev(NIL);
        node.set_next(NIL);
        head
    }

    /// Unlinks a node from the queue. The node's links are reset to [`NIL`]
    /// and it keeps its position in the arena.
    pub fn remove(&mut self, arena: &mut Slab<OrderNode>, idx: OrderIdx) {
        debug_assert!(!self.is_empty());
        let (prev, next) = {
            let node = arena.get(idx as usize).expect("queued order exists in the arena");
            (node.prev(), node.next())
        };
        if prev != NIL {
            arena.get_mut(prev as usize).expect("queued order exists in the arena").set_next(next);
        } else {
            self.head = next;
        }
        if next != NIL {
            arena.get_mut(next as usize).expect("queued order exists in the arena").set_prev(prev);
        } else {
            self.tail = prev;
        }
        self.len -= 1;
        let node = arena.get_mut(idx as usize).expect("queued order exists in the arena");
        node.set_prev(NIL);
        node.set_next(NIL);
    }
}

impl Default for OrderQueue {
    fn default() -> Self {
        Self { tail: NIL, head: NIL, len: 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::{Hash32, Nonce, Symbol};
    use crate::order::{OrderCold, OrderColdCommon, OrderHot};
    use crate::orderbook::config::BookConfigCold;
    use crate::signature::Signature;

    fn order(
        user: u8,
        nonce: u64,
        price: u64,
        quantity: u64,
        side: Side,
        kind: OrderKind,
    ) -> Order {
        Order::new(
            OrderHot {
                user: Address([user; 20]),
                nonce: Nonce(nonce),
                price: Price(price),
                quantity: Quantity(quantity),
                time_in_force: TimeInForce::Gtc,
                side,
            },
            OrderCold::new(
                OrderColdCommon::new(
                    Hash32([0; 32]),
                    Symbol([0; 32]),
                    Signature::default(),
                    TimestampMs(0),
                ),
                kind,
            ),
        )
    }

    fn node(order: &Order) -> OrderNode {
        OrderNode::from(*order)
    }

    #[test]
    fn test_queue_push_and_pop_preserves_time_priority() {
        let mut arena = Slab::new();
        let a = arena.insert(node(&order(1, 1, 100, 1, Side::Buy, OrderKind::Standard))) as u32;
        let b = arena.insert(node(&order(1, 2, 100, 1, Side::Buy, OrderKind::Standard))) as u32;
        let c = arena.insert(node(&order(1, 3, 100, 1, Side::Buy, OrderKind::Standard))) as u32;

        let mut queue = OrderQueue::default();
        queue.push_back(&mut arena, a);
        queue.push_back(&mut arena, b);
        queue.push_back(&mut arena, c);
        assert_eq!(queue.len(), 3);
        assert_eq!(queue.head(), a);
        assert_eq!(queue.tail(), c);
        assert_eq!(arena.get(a as usize).unwrap().next(), b);
        assert_eq!(arena.get(b as usize).unwrap().prev(), a);
        assert_eq!(arena.get(b as usize).unwrap().next(), c);
        assert_eq!(arena.get(c as usize).unwrap().prev(), b);

        assert_eq!(queue.pop_front(&mut arena), a);
        assert_eq!(queue.head(), b);
        assert_eq!(arena.get(b as usize).unwrap().prev(), NIL);
        assert_eq!(queue.pop_front(&mut arena), b);
        assert_eq!(queue.pop_front(&mut arena), c);
        assert!(queue.is_empty());
        assert_eq!(queue.head(), NIL);
        assert_eq!(queue.tail(), NIL);
    }

    #[test]
    fn test_queue_remove_middle() {
        let mut arena = Slab::new();
        let a = arena.insert(node(&order(1, 1, 100, 1, Side::Buy, OrderKind::Standard))) as u32;
        let b = arena.insert(node(&order(1, 2, 100, 1, Side::Buy, OrderKind::Standard))) as u32;
        let c = arena.insert(node(&order(1, 3, 100, 1, Side::Buy, OrderKind::Standard))) as u32;
        let mut queue = OrderQueue::default();
        queue.push_back(&mut arena, a);
        queue.push_back(&mut arena, b);
        queue.push_back(&mut arena, c);

        queue.remove(&mut arena, b);
        assert_eq!(queue.len(), 2);
        assert_eq!(arena.get(a as usize).unwrap().next(), c);
        assert_eq!(arena.get(c as usize).unwrap().prev(), a);
        assert_eq!(arena.get(b as usize).unwrap().prev(), NIL);
        assert_eq!(arena.get(b as usize).unwrap().next(), NIL);

        queue.remove(&mut arena, a);
        assert_eq!(queue.head(), c);
        assert_eq!(queue.tail(), c);
        queue.remove(&mut arena, c);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_level_constructor_totals() {
        let mut arena = Slab::new();
        let idx = arena.insert(node(&order(1, 1, 100, 4, Side::Sell, OrderKind::Standard))) as u32;
        let level = PriceLevel::new(Price(100), Side::Sell, None, idx, Quantity(4), Quantity(0));
        assert_eq!(level.len(), 1);
        assert_eq!(level.price(), Price(100));
        assert_eq!(level.visible_quantity(), Quantity(4));
        assert_eq!(level.hidden_quantity(), Quantity(0));
        assert!(!level.is_empty());
    }

    #[test]
    fn test_iceberg_maker_replenishes_and_requeues() {
        let mut arena = Slab::new();
        let maker =
            order(2, 1, 100, 20, Side::Sell, OrderKind::Iceberg { hidden_quantity: Quantity(30) });
        let idx = arena.insert(node(&maker)) as u32;
        let mut level =
            PriceLevel::new(Price(100), Side::Sell, None, idx, Quantity(20), Quantity(30));

        let taker_original = order(1, 1, 100, 50, Side::Buy, OrderKind::Standard);
        let mut taker = taker_original;
        let pools = MemoryPools::new(&BookConfigCold::default());

        // A 20-fill consumes the visible tranche: it replenishes from the
        // hidden reserve with the size of the consumed tranche.
        taker.hot.quantity = Quantity(20);
        let exec = level
            .execute(&mut arena, &mut taker, &taker_original, STPMode::None, TimestampMs(0), &pools)
            .unwrap();
        assert_eq!(exec.trades.len(), 1);
        assert_eq!(exec.trades[0].traded_quantity, Quantity(20));
        assert_eq!(exec.trades[0].maker, maker);
        assert_eq!(exec.trades[0].taker_remaining, Quantity(0));
        assert!(exec.removed.is_empty());
        assert!(!exec.taker_killed);
        // The level shows the replenished tranche: visible 20, hidden 10.
        assert_eq!(level.visible_quantity(), Quantity(20));
        assert_eq!(level.hidden_quantity(), Quantity(10));
        let node = arena.get(idx as usize).unwrap();
        assert_eq!(node.hot.quantity, Quantity(20));
        assert_eq!(node.hidden_quantity(), Quantity(10));
    }
}
