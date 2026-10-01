//! Orderbook definitions.

use crate::address::Address;
use crate::base::{Nonce, PegReferenceType, Side, Symbol};
use crate::clock::{Clock, MonotonicClock};
use crate::message::hot_path::{CancelOrder, OrderMsg, Trade};
use crate::message::side_path::{
    CancelReason, OrderChange, OrderStatus, RejectReason, ReplicationMsg,
};
use crate::order::{DEFAULT_RESERVE_REPLENISH_AMOUNT, Order, OrderIdx, OrderKind, OrderNode};
use crate::orderbook::config::BookConfig;
use crate::orderbook::listener::{Listeners, PooledReplicationMsg};
use crate::orderbook::pooled::{MemoryPools, PooledIndexList};
use crate::orderbook::price_level::{PriceLevel, order_expired};
use crate::orderbook::risk::RiskState;
use crate::orderbook::statistics::BookStatistics;
use crate::time_in_force::TimeInForce;
use crate::value::{Price, Quantity, TimestampMs};
use litemap::LiteMap;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use slab::Slab;

/// Default initial arena capacity.
const DEFAULT_ARENA_CAPACITY: usize = 4096;
/// Default initial capacity of the order index map.
const DEFAULT_INDEX_CAPACITY: usize = 4096;
/// Default initial capacity of the user order map.
const DEFAULT_USER_MAP_CAPACITY: usize = 1024;
/// Default initial capacity of a side's price level map.
const DEFAULT_PRICE_LEVEL_MAP_CAPACITY: usize = 256;

/// Orderbook errors defines the runtime errors of the book. Admission
/// failures are expressed through the [`OrderChange`] stream (a
/// [`OrderStatus::Rejected`] change) instead of this error, so the enum
/// currently carries no variants.
#[derive(Debug)]
pub enum OrderBookErr {}

/// OrderBook
pub struct OrderBook {
    /// Pre-allocated memory pools to avoid runtime heap allocation.
    memory_pools: MemoryPools,

    /// BookConfigs of the orderbook.
    config: BookConfig,

    /// The core state of the book, it should be recoverable from disaster.
    /// The oms take snapshot of it and store to an append only journal.
    /// With the message offset in wire protocols and the snapshot, the recovery
    /// is base on a snapshot + delta process to rebuild the state of the book.
    state: OrderBookState,

    /// Clock source for ms.
    clock: Box<dyn Clock>,

    /// Listeners push book changes to the remote component for state replication.
    listeners: Listeners,
}

impl OrderBook {
    /// Constructs a new order book for the market described by `config`, with
    /// the state containers and the memory pools pre-allocated from the cold
    /// config. The statistics collection is off by default: OMS_Master skips
    /// it for performance, OMS_Slave enables it when it replicates the book.
    pub fn new(config: BookConfig) -> Self {
        let state = OrderBookState::new(&config);
        let memory_pools = MemoryPools::new(&config.cold);
        Self {
            memory_pools,
            config,
            state,
            clock: Box::new(MonotonicClock),
            listeners: Listeners::default(),
        }
    }

    /// Sets the clock source of the book (timestamps, GTD expiry checks).
    pub fn with_clock(mut self, clock: Box<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Sets the listeners that fanout book changes and trade events.
    pub fn with_listeners(mut self, listeners: Listeners) -> Self {
        self.listeners = listeners;
        self
    }

    /// Engages or releases the kill switch. While engaged, new orders are
    /// rejected; cancellations are still processed.
    pub fn set_kill_switch(&mut self, kill_switch: bool) {
        self.state.kill_switch = kill_switch;
    }

    /// Re-attaches the pooled buffers of the state to this book's memory
    /// pools. Call once after replacing the state with one deserialized from
    /// a snapshot: snapshot loading reconstructs detached buffers, and this
    /// makes them return to the real pools.
    pub fn attach_pools(&mut self) {
        self.state.attach_pools(&self.memory_pools);
    }

    /// Execute is ran by OMS_Master to execute the ingress request from user.
    /// The listener callback will emit change events and trade events for the
    /// downstream components. The fanout messages are pooled buffers: their
    /// ownership moves to the listener and the buffer returns to the pool
    /// when the (possibly asynchronous) fanout task drops the message.
    pub fn execute(&mut self, input: &OrderMsg) -> Result<(), OrderBookErr> {
        match input {
            OrderMsg::NewOrder(new) => {
                let mut changes = self.memory_pools.changes_pool.acquire();
                let mut trades = self.memory_pools.trades_pool.acquire();
                self.process_new_order(new, &mut changes, &mut trades)?;
                if !trades.is_empty() {
                    self.listeners.fanout_trade_msg(trades);
                }
                self.listeners.fanout_replication_msg(PooledReplicationMsg::new(changes));
                Ok(())
            }
            OrderMsg::CancelOrder(cancel) => {
                let mut changes = self.memory_pools.changes_pool.acquire();
                self.process_cancel_order(cancel, &mut changes)?;
                self.listeners.fanout_replication_msg(PooledReplicationMsg::new(changes));
                Ok(())
            }
        }
    }

    /// Apply is ran by OMS_Slave to apply the deltas replicated from the OMS_Master.
    /// The listener callback will emit changes to Redis cluster and SQL cluster.
    pub fn apply(&mut self, _replicated: &ReplicationMsg) -> Result<(), OrderBookErr> {
        // todo: implement the applying of the changes to the book, the statistics are
        //  also updated during the data applying.
        Ok(())
    }

    /// process_cancel_order cancels an order: it removes the order from the
    /// book and pops it out of its price level. The change of the book is
    /// appended to the caller's pooled buffer.
    fn process_cancel_order(
        &mut self,
        input: &CancelOrder,
        changes: &mut Vec<OrderChange>,
    ) -> Result<(), OrderBookErr> {
        // The index key is the (address, nonce) tuple; a cancellation of an
        // unknown order is a silent no-op. The order_id of the request is not
        // verified against the resting order for now.
        let key = (input.user(), input.nonce());
        if let Some(&idx) = self.state.index.get(&key) {
            let (order, side, price) = {
                let node =
                    self.state.arena.get(idx as usize).expect("indexed order exists in the arena");
                (Order::from(node.clone()), node.hot.side, node.hot.price)
            };
            let level_empty = {
                let levels = match side {
                    Side::Buy => &mut self.state.bids,
                    Side::Sell => &mut self.state.asks,
                };
                let level = levels.get_mut(&price).expect("a resting order has a price level");
                level.remove(&mut self.state.arena, idx);
                level.is_empty()
            };
            if level_empty {
                let levels = match side {
                    Side::Buy => &mut self.state.bids,
                    Side::Sell => &mut self.state.asks,
                };
                levels.remove(&price);
            }
            self.purge_order(idx);
            // The engine does not track the cumulative filled quantity of a
            // resting order, so zero is reported here.
            changes.push(OrderChange::new(
                order,
                OrderStatus::Canceled {
                    filled_quantity: Quantity::ZERO,
                    reason: CancelReason::UserRequested,
                },
            ));
        }
        Ok(())
    }

    /// Runs the admission checks and the matching sweep of a new order, then
    /// disposes of the remainder per its time in force and kind.
    fn process_new_order(
        &mut self,
        input: &Order,
        changes: &mut Vec<OrderChange>,
        trades: &mut Vec<Trade>,
    ) -> Result<(), OrderBookErr> {
        let now = self.clock.now_millis();

        // The operational kill switch blocks every new order.
        if self.state.kill_switch {
            changes.push(rejected(*input, RejectReason::KillSwitchActive));
            return Ok(());
        }

        // The (address, nonce) tuple identifies an order, duplicates are rejected.
        if self.state.index.contains_key(&(input.hot.user, input.hot.nonce)) {
            changes.push(rejected(*input, RejectReason::DuplicateOrderId));
            return Ok(());
        }

        // Price and quantity validation against the market config.
        if let Some(reason) = self.validate_order(input) {
            changes.push(rejected(*input, reason));
            return Ok(());
        }

        // A GTD order that already expired never enters the book.
        if order_expired(input, now) {
            changes.push(OrderChange::new(
                *input,
                OrderStatus::Canceled {
                    filled_quantity: Quantity::ZERO,
                    reason: CancelReason::TimeInForceExpired,
                },
            ));
            return Ok(());
        }

        // The effective limit price: `None` for a market-to-limit order,
        // which crosses any price.
        let limit = match self.effective_price(input) {
            Ok(limit) => limit,
            Err(reason) => {
                changes.push(rejected(*input, reason));
                return Ok(());
            }
        };

        // Pre-trade risk checks.
        if let Some(reason) = self.check_risk(input, limit) {
            changes.push(rejected(*input, reason));
            return Ok(());
        }

        // A post-only order never takes liquidity.
        if matches!(input.cold.kind, OrderKind::PostOnly) {
            self.process_post_only(input, changes);
            return Ok(());
        }

        // A fill-or-kill order is all-or-nothing: the available depth is
        // checked before the first trade happens.
        if input.hot.time_in_force == TimeInForce::Fok && !self.is_fillable(input, limit, now) {
            changes.push(rejected(*input, RejectReason::InsufficientLiquidity));
            return Ok(());
        }

        // The matching sweep.
        let mut taker = *input;
        // Pegged and trailing-stop orders rest at their effective limit price,
        // not at the submitted price.
        if let Some(price) = limit {
            taker.hot.price = price;
        }
        let (filled, killed) = self.sweep(&mut taker, input, limit, now, changes, trades);
        self.dispose_taker(input, &taker, filled, killed, changes);
        Ok(())
    }

    /// Validates the order against the market config: quantity, tick size,
    /// lot size and the min/max order size range.
    fn validate_order(&self, input: &Order) -> Option<RejectReason> {
        let hot = &self.config.hot;
        if input.hot.quantity.0 == 0 {
            return Some(RejectReason::InvalidQuantity);
        }
        if hot.tick_size.is_some_and(|tick| tick.0 > 0 && !input.hot.price.0.is_multiple_of(tick.0))
        {
            return Some(RejectReason::InvalidPrice);
        }
        if hot.lot_size.is_some_and(|lot| {
            lot.0 > 0
                && (!input.hot.quantity.0.is_multiple_of(lot.0)
                    || !input.hidden_quantity().0.is_multiple_of(lot.0))
        }) {
            return Some(RejectReason::InvalidQuantity);
        }
        let total = input.total_quantity().0;
        if hot.min_order_size.is_some_and(|min| total < min.0)
            || hot.max_order_size.is_some_and(|max| total > max.0)
        {
            return Some(RejectReason::OrderSizeOutOfRange);
        }
        None
    }

    /// Computes the effective limit price of the order per its kind. A
    /// trailing stop converts to a limit at `last_ref ± trail`; a pegged
    /// order prices off the requested reference; a market-to-limit order has
    /// no limit price (`None`).
    fn effective_price(&self, input: &Order) -> Result<Option<Price>, RejectReason> {
        match input.cold.kind {
            OrderKind::MarketToLimit => Ok(None),
            OrderKind::TrailingStop { trail_amount, last_ref_price } => {
                let price = match input.hot.side {
                    Side::Buy => Price(last_ref_price.0.saturating_add(trail_amount.0)),
                    Side::Sell => Price(last_ref_price.0.saturating_sub(trail_amount.0)),
                };
                Ok(Some(price))
            }
            OrderKind::Pegged { reference_price_offset, reference_price_type } => {
                let reference = self.reference_price(reference_price_type)?;
                let offset = reference_price_offset.unsigned_abs();
                let price = if reference_price_offset >= 0 {
                    Price(reference.0.saturating_add(offset))
                } else {
                    Price(reference.0.saturating_sub(offset))
                };
                Ok(Some(price))
            }
            _ => Ok(Some(input.hot.price)),
        }
    }

    /// Resolves the reference price requested by a pegged order. When the
    /// reference is unavailable the order cannot be priced and is rejected
    /// with [`RejectReason::InvalidPriceLevel`].
    fn reference_price(&self, reference_type: PegReferenceType) -> Result<Price, RejectReason> {
        let price = match reference_type {
            PegReferenceType::BestBid => self.state.bids.iter().next_back().map(|(p, _)| *p),
            PegReferenceType::BestAsk => self.state.asks.iter().next().map(|(p, _)| *p),
            PegReferenceType::Mid => {
                let best_bid = self.state.bids.iter().next_back().map(|(p, _)| *p);
                let best_ask = self.state.asks.iter().next().map(|(p, _)| *p);
                match (best_bid, best_ask) {
                    (Some(bid), Some(ask)) => Some(Price((bid.0 + ask.0) / 2)),
                    _ => None,
                }
            }
            PegReferenceType::LastTrade => self.state.last_trade_price,
        };
        price.ok_or(RejectReason::InvalidPriceLevel)
    }

    /// Runs the pre-trade risk checks of the configured risk state.
    fn check_risk(&self, input: &Order, limit: Option<Price>) -> Option<RejectReason> {
        let price = limit.unwrap_or(input.hot.price);
        let best_bid = self.state.bids.iter().next_back().map(|(p, _)| *p);
        let best_ask = self.state.asks.iter().next().map(|(p, _)| *p);
        let reference =
            self.state.risk_state.reference_price(best_bid, best_ask, self.state.last_trade_price);
        self.state
            .risk_state
            .check_admission(input.hot.user, price, input.total_quantity(), reference)
            .err()
    }

    /// Handles a post-only order: it never takes liquidity, it either rests
    /// or is rejected / cancelled.
    fn process_post_only(&mut self, input: &Order, changes: &mut Vec<OrderChange>) {
        let crosses = match input.hot.side {
            Side::Buy => {
                self.state.asks.iter().next().is_some_and(|(p, _)| p.0 <= input.hot.price.0)
            }
            Side::Sell => {
                self.state.bids.iter().next_back().is_some_and(|(p, _)| p.0 >= input.hot.price.0)
            }
        };
        if crosses {
            changes.push(rejected(*input, RejectReason::PostOnlyWouldCross));
            return;
        }
        match input.hot.time_in_force {
            // A post-only order can never execute immediately, so the
            // immediate time-in-force policies cannot be honoured.
            TimeInForce::Ioc => changes.push(OrderChange::new(
                *input,
                OrderStatus::Canceled {
                    filled_quantity: Quantity::ZERO,
                    reason: CancelReason::InsufficientLiquidity,
                },
            )),
            TimeInForce::Fok => changes.push(rejected(*input, RejectReason::InsufficientLiquidity)),
            _ => {
                self.rest_order(input);
                changes.push(OrderChange::new(*input, OrderStatus::Open));
            }
        }
    }

    /// The FOK pre-scan: whether the opposite side can absorb the whole order
    /// within the limit. Same-user depth is discounted per the STP mode (it
    /// is cancelled before matching, or kills the sweep), and expired orders
    /// contribute nothing.
    fn is_fillable(&self, input: &Order, limit: Option<Price>, now: TimestampMs) -> bool {
        let needed = input.total_quantity().0;
        let mut cumulative = 0u64;
        let stp_mode = self.config.hot.stp_mode;
        let user = input.hot.user;
        match input.hot.side {
            Side::Buy => {
                for (price, level) in self.state.asks.iter() {
                    if limit.is_some_and(|l| price.0 > l.0) {
                        break;
                    }
                    cumulative = cumulative.saturating_add(
                        level.fillable_quantity(&self.state.arena, user, stp_mode, now).0,
                    );
                    if cumulative >= needed {
                        return true;
                    }
                }
            }
            Side::Sell => {
                for (price, level) in self.state.bids.iter().rev() {
                    if limit.is_some_and(|l| price.0 < l.0) {
                        break;
                    }
                    cumulative = cumulative.saturating_add(
                        level.fillable_quantity(&self.state.arena, user, stp_mode, now).0,
                    );
                    if cumulative >= needed {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Matches the taker against the opposite side, level by level, until the
    /// quantity is exhausted, no crossing level remains, or STP kills the
    /// taker. Returns the filled quantity and whether the taker was killed.
    fn sweep(
        &mut self,
        taker: &mut Order,
        original: &Order,
        limit: Option<Price>,
        now: TimestampMs,
        changes: &mut Vec<OrderChange>,
        trades: &mut Vec<Trade>,
    ) -> (Quantity, bool) {
        let mut killed = false;
        // The initial visible quantity is the tranche size an iceberg taker
        // replenishes with.
        let peak = original.hot.quantity;
        let initial_total = original.total_quantity();

        loop {
            // Replenish the taker's visible tranche (iceberg / reserve)
            // before the next level execution, and stop once there is
            // nothing left to place.
            self.replenish_taker(taker, peak);
            if taker.total_quantity().0 == 0 {
                break;
            }

            // The best price of the opposite side. The maps are sorted, so
            // the best level is the first one in iteration order; when it
            // does not cross the limit, no deeper level crosses either.
            let best = match taker.hot.side {
                Side::Buy => self.state.asks.iter().next(),
                Side::Sell => self.state.bids.iter().next_back(),
            };
            let Some((&price, _)) = best else { break };
            let crosses = match taker.hot.side {
                Side::Buy => limit.is_none_or(|l| price.0 <= l.0),
                Side::Sell => limit.is_none_or(|l| price.0 >= l.0),
            };
            if !crosses {
                break;
            }

            let mut execution = {
                let levels = match taker.hot.side {
                    Side::Buy => &mut self.state.asks,
                    Side::Sell => &mut self.state.bids,
                };
                let level = levels.get_mut(&price).expect("a level exists for the best price");
                level
                    .execute(
                        &mut self.state.arena,
                        taker,
                        original,
                        self.config.hot.stp_mode,
                        now,
                        &self.memory_pools,
                    )
                    .expect("level execution is infallible")
            };

            if !execution.trades.is_empty() {
                self.state.last_trade_price = Some(price);
                self.state.has_traded = true;
            }
            // Merge the level buffers into the message buffers; the level
            // buffers return to their pools when `execution` drops.
            changes.append(&mut execution.changes);
            trades.append(&mut execution.trades);
            for idx in execution.removed.iter().copied() {
                self.purge_order(idx);
            }
            killed = execution.taker_killed;
            if killed {
                break;
            }

            let level_empty = {
                let levels = match taker.hot.side {
                    Side::Buy => &mut self.state.asks,
                    Side::Sell => &mut self.state.bids,
                };
                levels.get(&price).expect("a level exists for the best price").is_empty()
            };
            if level_empty {
                let levels = match taker.hot.side {
                    Side::Buy => &mut self.state.asks,
                    Side::Sell => &mut self.state.bids,
                };
                levels.remove(&price);
            }
        }

        let remaining = taker.total_quantity();
        (Quantity(initial_total.0 - remaining.0), killed)
    }

    /// Replenishes the visible tranche of an iceberg / reserve taker between
    /// level executions, so the sweep keeps consuming the hidden reserve.
    fn replenish_taker(&mut self, taker: &mut Order, peak: Quantity) {
        if taker.hot.quantity.0 > 0 {
            return;
        }
        match taker.cold.kind {
            OrderKind::Iceberg { hidden_quantity } if hidden_quantity.0 > 0 => {
                let draw = Quantity(hidden_quantity.0.min(peak.0));
                taker.hot.quantity = draw;
                taker.set_hidden_quantity(Quantity(hidden_quantity.0 - draw.0));
            }
            OrderKind::ReserveOrder {
                hidden_quantity,
                auto_replenish: true,
                replenish_threshold,
                replenish_amount,
            } if hidden_quantity.0 > 0 => {
                let threshold =
                    if replenish_threshold.0 == 0 { Quantity(1) } else { replenish_threshold };
                let amount = replenish_amount.map_or(DEFAULT_RESERVE_REPLENISH_AMOUNT, |a| a.get());
                let mut hidden = hidden_quantity;
                let mut visible = Quantity::ZERO;
                while visible.0 < threshold.0 && hidden.0 > 0 {
                    let draw = hidden.0.min(amount);
                    visible.0 += draw;
                    hidden.0 -= draw;
                }
                taker.hot.quantity = visible;
                taker.set_hidden_quantity(hidden);
            }
            OrderKind::ReserveOrder { auto_replenish: false, .. } => {
                // A non-auto reserve never replenishes: once the visible
                // tranche is consumed the hidden remainder is discarded.
                taker.set_hidden_quantity(Quantity::ZERO);
            }
            _ => {}
        }
    }

    /// Disposes of the taker's remainder after the sweep: fills, STP kills,
    /// IOC / FOK handling, market-to-limit conversion or resting.
    fn dispose_taker(
        &mut self,
        input: &Order,
        taker: &Order,
        filled: Quantity,
        killed: bool,
        changes: &mut Vec<OrderChange>,
    ) {
        let remaining = taker.total_quantity();

        if killed {
            // Partial fills that precede the self-trade are kept.
            let status = if filled.0 > 0 {
                OrderStatus::Canceled {
                    filled_quantity: filled,
                    reason: CancelReason::SelfTradePrevention,
                }
            } else {
                OrderStatus::Rejected { reason: RejectReason::SelfTradePrevention }
            };
            changes.push(OrderChange::new(*input, status));
            return;
        }

        if remaining.0 == 0 {
            changes.push(OrderChange::new(*input, OrderStatus::Filled { filled_quantity: filled }));
            return;
        }

        // FOK is guaranteed all-or-nothing by the admission pre-scan; this is
        // a defensive guard for an unexpected shortfall.
        if input.hot.time_in_force == TimeInForce::Fok {
            changes.push(rejected(*input, RejectReason::InsufficientLiquidity));
            return;
        }

        // An IOC order never rests.
        if input.hot.time_in_force == TimeInForce::Ioc {
            changes.push(OrderChange::new(
                *input,
                OrderStatus::Canceled {
                    filled_quantity: filled,
                    reason: CancelReason::InsufficientLiquidity,
                },
            ));
            return;
        }

        // A market-to-limit order converts its remainder into a limit order
        // at the last trade price; without a trade there is no price to
        // convert to.
        if matches!(input.cold.kind, OrderKind::MarketToLimit) {
            let Some(last) = self.state.last_trade_price else {
                changes.push(rejected(*input, RejectReason::InsufficientLiquidity));
                return;
            };
            let mut resting = *taker;
            resting.hot.price = last;
            self.rest_order(&resting);
            changes.push(OrderChange::new(
                *input,
                OrderStatus::PartiallyFilled { filled_quantity: filled },
            ));
            return;
        }

        // GTC, GTD and Day: the remainder rests.
        self.rest_order(taker);
        let status = if filled.0 > 0 {
            OrderStatus::PartiallyFilled { filled_quantity: filled }
        } else {
            OrderStatus::Open
        };
        changes.push(OrderChange::new(*input, status));
    }

    /// Rests an order: inserts the node into the arena, appends it to its
    /// price level (creating the level on demand) and indexes it.
    fn rest_order(&mut self, order: &Order) -> OrderIdx {
        let idx = self.state.arena.insert(OrderNode::from(*order)) as u32;
        let visible = order.hot.quantity;
        let hidden = order.hidden_quantity();
        {
            let levels = match order.hot.side {
                Side::Buy => &mut self.state.bids,
                Side::Sell => &mut self.state.asks,
            };
            match levels.get_mut(&order.hot.price) {
                Some(level) => level.append(&mut self.state.arena, idx, visible, hidden),
                None => {
                    levels.insert(
                        order.hot.price,
                        PriceLevel::new(
                            order.hot.price,
                            order.hot.side,
                            None,
                            idx,
                            visible,
                            hidden,
                        ),
                    );
                }
            }
        }
        self.state.index.insert((order.hot.user, order.hot.nonce), idx);
        self.state
            .user_orders
            .entry(order.hot.user)
            .or_insert_with(|| PooledIndexList::new(&self.memory_pools.index_lists_pool))
            .push(idx);
        self.state.risk_state.record_open(order);
        idx
    }

    /// Drops an order that already left its price level (filled or cancelled
    /// during the sweep) from the remaining book structures: the arena, the
    /// index, the user order map and the risk state.
    fn purge_order(&mut self, idx: OrderIdx) {
        let node = self.state.arena.remove(idx as usize);
        let order: Order = node.into();
        self.state.index.remove(&(order.hot.user, order.hot.nonce));
        let mut drop_user_entry = false;
        if let Some(orders) = self.state.user_orders.get_mut(&order.hot.user) {
            if let Some(position) = orders.iter().position(|&i| i == idx) {
                orders.swap_remove(position);
            }
            drop_user_entry = orders.is_empty();
        }
        if drop_user_entry {
            // Dropping the user's last list returns its buffer to the pool.
            self.state.user_orders.remove(&order.hot.user);
        }
        self.state.risk_state.record_removed(order.hot.user, order.hot.nonce);
    }
}

/// Builds a rejected order change.
fn rejected(order: Order, reason: RejectReason) -> OrderChange {
    OrderChange::new(order, OrderStatus::Rejected { reason })
}

/// OrderBookState stores the runtime state of the book, it should be recoverable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookState {
    /// Symbol of the book.
    symbol: Symbol,

    /// The arena of orders, all live orders.
    arena: Slab<OrderNode>,

    /// The bids price levels, sorted by Price from high to low, pre-allocated with
    /// initial capacity.
    bids: LiteMap<Price, PriceLevel>,

    /// The asks price levels, sorted by Price from low to high, pre-allocated with
    /// initial capacity.
    asks: LiteMap<Price, PriceLevel>,

    /// Index for an order, use the hot data of an order as key for indexing.
    index: FxHashMap<(Address, Nonce), OrderIdx>,

    /// User orders. The lists are pooled with RAII guards: a list returns its
    /// buffer to the index list pool when a user's last order leaves the book.
    user_orders: FxHashMap<Address, PooledIndexList>,

    /// Book statistics. OMS_Master skip this for performance, the statistic task is
    /// done by OMS_Slave which replicates the book.
    book_statistics: Option<BookStatistics>,

    /// Pre-trade risk state.
    risk_state: RiskState,

    /// Last trade price.
    last_trade_price: Option<Price>,

    /// Flag indicating if there was a trade.
    has_traded: bool,

    /// Kill switch.
    kill_switch: bool,
}

impl OrderBookState {
    /// Constructs an empty book state, pre-allocating the containers from the
    /// cold config (with defaults when a knob is not set).
    pub fn new(config: &BookConfig) -> Self {
        let cold = &config.cold;
        let price_level_map_capacity =
            cold.price_level_map_size.map_or(DEFAULT_PRICE_LEVEL_MAP_CAPACITY, |v| v as usize);
        Self {
            symbol: cold.symbol,
            arena: Slab::with_capacity(
                cold.arena_size.map_or(DEFAULT_ARENA_CAPACITY, |v| v as usize),
            ),
            bids: LiteMap::with_capacity(price_level_map_capacity),
            asks: LiteMap::with_capacity(price_level_map_capacity),
            index: FxHashMap::with_capacity_and_hasher(
                cold.order_index_size.map_or(DEFAULT_INDEX_CAPACITY, |v| v as usize),
                Default::default(),
            ),
            user_orders: FxHashMap::with_capacity_and_hasher(
                cold.user_order_map_size.map_or(DEFAULT_USER_MAP_CAPACITY, |v| v as usize),
                Default::default(),
            ),
            book_statistics: None,
            risk_state: RiskState::new(config.hot.risk_config.clone()),
            last_trade_price: None,
            has_traded: false,
            kill_switch: false,
        }
    }

    /// Re-attaches the user order lists to the memory pools of the book.
    /// The lists of a deserialized snapshot state wrap a throwaway drain
    /// pool; call this once after loading the state into a book so the
    /// buffers return to the real pool when a user's last order leaves.
    pub fn attach_pools(&mut self, pools: &MemoryPools) {
        for list in self.user_orders.values_mut() {
            list.attach(&pools.index_lists_pool);
        }
    }
}

impl OrderBook {
    /// Test helper mirroring the pooled execution path of [`OrderBook::execute`]:
    /// returns owned buffers taken out of the pool so tests can inspect them.
    #[cfg(test)]
    fn execute_new_order(
        &mut self,
        input: &Order,
    ) -> Result<(Vec<OrderChange>, Option<Vec<Trade>>), OrderBookErr> {
        let mut changes = self.memory_pools.changes_pool.acquire();
        let mut trades = self.memory_pools.trades_pool.acquire();
        self.process_new_order(input, &mut changes, &mut trades)?;
        let trades = if trades.is_empty() { None } else { Some(trades.take()) };
        Ok((changes.take(), trades))
    }

    /// Test helper mirroring the pooled cancellation path of [`OrderBook::execute`].
    #[cfg(test)]
    fn execute_cancel_order(
        &mut self,
        input: &CancelOrder,
    ) -> Result<Vec<OrderChange>, OrderBookErr> {
        let mut changes = self.memory_pools.changes_pool.acquire();
        self.process_cancel_order(input, &mut changes)?;
        Ok(changes.take())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::Hash32;
    use crate::order::{OrderCold, OrderColdCommon, OrderHot};
    use crate::orderbook::config::{BookConfigCold, BookConfigHot, RiskConfig};
    use crate::orderbook::stp::STPMode;
    use crate::signature::Signature;
    use std::cell::Cell;
    use std::path::{Path, PathBuf};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    fn addr(b: u8) -> Address {
        Address([b; 20])
    }

    fn build(
        user: u8,
        nonce: u64,
        price: u64,
        quantity: u64,
        side: Side,
        time_in_force: TimeInForce,
        kind: OrderKind,
    ) -> Order {
        Order::new(
            OrderHot {
                user: addr(user),
                nonce: Nonce(nonce),
                price: Price(price),
                quantity: Quantity(quantity),
                time_in_force,
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

    fn buy(user: u8, nonce: u64, price: u64, quantity: u64) -> Order {
        build(user, nonce, price, quantity, Side::Buy, TimeInForce::Gtc, OrderKind::Standard)
    }

    fn sell(user: u8, nonce: u64, price: u64, quantity: u64) -> Order {
        build(user, nonce, price, quantity, Side::Sell, TimeInForce::Gtc, OrderKind::Standard)
    }

    fn book() -> OrderBook {
        OrderBook::new(BookConfig::default())
    }

    fn change_of(changes: &[OrderChange], order: &Order) -> Option<OrderStatus> {
        changes.iter().find(|c| *c.order() == *order).map(|c| *c.status())
    }

    // ---------------------------------------------------------------
    // Standard matching
    // ---------------------------------------------------------------

    #[test]
    fn test_standard_order_full_fill() {
        let mut book = book();
        book.execute_new_order(&sell(2, 2, 100, 50)).unwrap();
        let (changes, trades) = book.execute_new_order(&buy(1, 1, 100, 50)).unwrap();

        let trades = trades.unwrap();
        assert_eq!(trades.len(), 1);
        let trade = trades[0];
        assert_eq!(trade.price, Price(100));
        assert_eq!(trade.traded_quantity, Quantity(50));
        assert_eq!(trade.taker, buy(1, 1, 100, 50));
        assert_eq!(trade.maker, sell(2, 2, 100, 50));
        assert_eq!(trade.taker_remaining, Quantity(0));

        assert_eq!(
            change_of(&changes, &buy(1, 1, 100, 50)),
            Some(OrderStatus::Filled { filled_quantity: Quantity(50) })
        );
        assert_eq!(
            change_of(&changes, &sell(2, 2, 100, 50)),
            Some(OrderStatus::Filled { filled_quantity: Quantity(50) })
        );

        assert!(book.state.asks.is_empty());
        assert!(book.state.index.is_empty());
        assert!(book.state.user_orders.is_empty());
        assert_eq!(book.state.last_trade_price, Some(Price(100)));
        assert!(book.state.has_traded);
    }

    #[test]
    fn test_standard_order_partial_fill_rests() {
        let mut book = book();
        book.execute_new_order(&sell(2, 2, 100, 40)).unwrap();
        let (changes, trades) = book.execute_new_order(&buy(1, 1, 100, 100)).unwrap();

        assert_eq!(trades.unwrap()[0].traded_quantity, Quantity(40));
        assert_eq!(
            change_of(&changes, &buy(1, 1, 100, 100)),
            Some(OrderStatus::PartiallyFilled { filled_quantity: Quantity(40) })
        );

        // The taker's remainder rests in the bids.
        let level = book.state.bids.get(&Price(100)).unwrap();
        assert_eq!(level.len(), 1);
        assert_eq!(level.visible_quantity(), Quantity(60));
        assert!(book.state.index.contains_key(&(addr(1), Nonce(1))));
        assert_eq!(&**book.state.user_orders.get(&addr(1)).unwrap(), &vec![0]);
    }

    #[test]
    fn test_no_cross_rests_open() {
        let mut book = book();
        book.execute_new_order(&sell(2, 2, 101, 10)).unwrap();
        let (changes, trades) = book.execute_new_order(&buy(1, 1, 100, 10)).unwrap();

        assert_eq!(trades, None);
        assert_eq!(change_of(&changes, &buy(1, 1, 100, 10)), Some(OrderStatus::Open));
        assert_eq!(book.state.bids.len(), 1);
        assert_eq!(book.state.asks.len(), 1);
        assert!(!book.state.has_traded);
    }

    #[test]
    fn test_time_priority_within_level() {
        let mut book = book();
        book.execute_new_order(&sell(2, 1, 100, 30)).unwrap();
        book.execute_new_order(&sell(2, 2, 100, 30)).unwrap();
        let (changes, trades) = book.execute_new_order(&buy(1, 1, 100, 40)).unwrap();

        let trades = trades.unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].maker, sell(2, 1, 100, 30));
        assert_eq!(trades[0].traded_quantity, Quantity(30));
        assert_eq!(trades[1].maker, sell(2, 2, 100, 30));
        assert_eq!(trades[1].traded_quantity, Quantity(10));
        assert_eq!(
            change_of(&changes, &sell(2, 2, 100, 30)),
            Some(OrderStatus::PartiallyFilled { filled_quantity: Quantity(10) })
        );
    }

    #[test]
    fn test_price_time_priority_sweep() {
        let mut book = book();
        // Resting asks: 98 (two makers, time priority), 99, and 101 beyond
        // the taker's limit.
        book.execute_new_order(&sell(2, 1, 98, 5)).unwrap();
        book.execute_new_order(&sell(2, 2, 98, 10)).unwrap();
        book.execute_new_order(&sell(3, 1, 99, 20)).unwrap();
        book.execute_new_order(&sell(4, 1, 101, 30)).unwrap();
        let taker = buy(1, 1, 100, 40);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        let trades = trades.unwrap();
        assert_eq!(trades.len(), 3);
        // Price priority: the sweep takes the best prices first, ascending.
        assert_eq!((trades[0].price, trades[0].traded_quantity), (Price(98), Quantity(5)));
        assert_eq!(trades[0].maker, sell(2, 1, 98, 5));
        // Time priority within the 98 level: the earlier order fills first.
        assert_eq!((trades[1].price, trades[1].traded_quantity), (Price(98), Quantity(10)));
        assert_eq!(trades[1].maker, sell(2, 2, 98, 10));
        assert_eq!((trades[2].price, trades[2].traded_quantity), (Price(99), Quantity(20)));
        assert_eq!(trades[2].maker, sell(3, 1, 99, 20));
        // The taker's remainder rests; the level beyond the limit is untouched.
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::PartiallyFilled { filled_quantity: Quantity(35) })
        );
        assert!(book.state.asks.get(&Price(98)).is_none());
        assert!(book.state.asks.get(&Price(99)).is_none());
        assert_eq!(book.state.asks.get(&Price(101)).unwrap().visible_quantity(), Quantity(30));
        assert_eq!(book.state.bids.get(&Price(100)).unwrap().visible_quantity(), Quantity(5));
    }

    #[test]
    fn test_replenished_order_loses_time_priority() {
        let mut book = book();
        // An iceberg maker rests first, a plain maker second, at the same price.
        let iceberg = build(
            2,
            1,
            100,
            10,
            Side::Sell,
            TimeInForce::Gtc,
            OrderKind::Iceberg { hidden_quantity: Quantity(90) },
        );
        book.execute_new_order(&iceberg).unwrap();
        let plain = sell(3, 1, 100, 20);
        book.execute_new_order(&plain).unwrap();

        let (_, trades) = book.execute_new_order(&buy(1, 1, 100, 15)).unwrap();
        let trades = trades.unwrap();
        // The iceberg's first tranche fills first; it then replenishes and
        // re-queues behind the plain maker, which fills next.
        assert_eq!(trades[0].maker, iceberg);
        assert_eq!(trades[0].traded_quantity, Quantity(10));
        assert_eq!(trades[1].maker, plain);
        assert_eq!(trades[1].traded_quantity, Quantity(5));
    }

    #[test]
    fn test_partially_filled_maker_keeps_time_priority() {
        let mut book = book();
        book.execute_new_order(&sell(2, 1, 100, 30)).unwrap();
        book.execute_new_order(&sell(3, 1, 100, 30)).unwrap();

        // The first taker partially fills the head maker.
        book.execute_new_order(&buy(1, 1, 100, 10)).unwrap();
        // The second taker continues with the same head maker first: it has
        // 20 left, then the second maker fills the rest.
        let (_, trades) = book.execute_new_order(&buy(1, 2, 100, 40)).unwrap();
        let trades = trades.unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].maker.hot.user, addr(2));
        assert_eq!(trades[0].maker.hot.nonce, Nonce(1));
        assert_eq!(trades[0].traded_quantity, Quantity(20));
        assert_eq!(trades[1].maker.hot.user, addr(3));
        assert_eq!(trades[1].traded_quantity, Quantity(20));
    }

    // ---------------------------------------------------------------
    // Iceberg orders
    // ---------------------------------------------------------------

    #[test]
    fn test_iceberg_maker_replenishes_tranches() {
        let mut book = book();
        let maker = build(
            2,
            1,
            100,
            20,
            Side::Sell,
            TimeInForce::Gtc,
            OrderKind::Iceberg { hidden_quantity: Quantity(20) },
        );
        book.execute_new_order(&maker).unwrap();
        let (changes, trades) = book.execute_new_order(&buy(1, 1, 100, 50)).unwrap();

        // 20 (visible) + 20 (replenished) = 40, the maker is exhausted after
        // two tranche-sized trades. Each trade carries the maker as it was
        // at trade time (the second one already shows the replenished state).
        let trades = trades.unwrap();
        assert_eq!(trades.len(), 2);
        for trade in &trades {
            assert_eq!(trade.maker.hot.user, addr(2));
            assert_eq!(trade.maker.hot.nonce, Nonce(1));
            assert_eq!(trade.price, Price(100));
            assert_eq!(trade.traded_quantity, Quantity(20));
        }
        // The first fill only consumes one tranche (the maker replenishes and
        // stays in the book), so the maker's first change is a partial fill.
        assert_eq!(
            change_of(&changes, &maker),
            Some(OrderStatus::PartiallyFilled { filled_quantity: Quantity(20) })
        );
        assert_eq!(
            change_of(&changes, &buy(1, 1, 100, 50)),
            Some(OrderStatus::PartiallyFilled { filled_quantity: Quantity(40) })
        );
        // The taker's 10 remainder rests.
        assert_eq!(book.state.bids.len(), 1);
    }

    #[test]
    fn test_iceberg_taker_sweeps_tranches() {
        let mut book = book();
        book.execute_new_order(&sell(2, 2, 100, 100)).unwrap();
        let taker = build(
            1,
            1,
            100,
            60,
            Side::Buy,
            TimeInForce::Gtc,
            OrderKind::Iceberg { hidden_quantity: Quantity(40) },
        );
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        // 60 (visible) + 40 (replenished) = 100.
        let trades = trades.unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].traded_quantity, Quantity(60));
        assert_eq!(trades[0].taker_remaining, Quantity(40));
        assert_eq!(trades[1].traded_quantity, Quantity(40));
        assert_eq!(trades[1].taker_remaining, Quantity(0));
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::Filled { filled_quantity: Quantity(100) })
        );
        assert!(book.state.asks.is_empty());
    }

    // ---------------------------------------------------------------
    // Reserve orders
    // ---------------------------------------------------------------

    #[test]
    fn test_reserve_maker_auto_replenish_above_threshold() {
        let mut book = book();
        let maker = build(
            2,
            1,
            100,
            10,
            Side::Sell,
            TimeInForce::Gtc,
            OrderKind::ReserveOrder {
                hidden_quantity: Quantity(100),
                replenish_threshold: Quantity(5),
                replenish_amount: None,
                auto_replenish: true,
            },
        );
        book.execute_new_order(&maker).unwrap();
        let (changes, trades) = book.execute_new_order(&buy(1, 1, 100, 7)).unwrap();

        assert_eq!(trades.unwrap()[0].traded_quantity, Quantity(7));
        assert_eq!(
            change_of(&changes, &maker),
            Some(OrderStatus::PartiallyFilled { filled_quantity: Quantity(7) })
        );
        // Visible dropped to 3, below the threshold of 5: it replenishes with
        // the default amount of 1 per step back up to 5, hidden 100 -> 98.
        let idx = *book.state.index.get(&(addr(2), Nonce(1))).unwrap();
        let node = book.state.arena.get(idx as usize).unwrap();
        assert_eq!(node.hot.quantity, Quantity(5));
        assert_eq!(node.hidden_quantity(), Quantity(98));
        assert_eq!(book.state.asks.get(&Price(100)).unwrap().visible_quantity(), Quantity(5));
        assert_eq!(book.state.asks.get(&Price(100)).unwrap().hidden_quantity(), Quantity(98));
    }

    #[test]
    fn test_reserve_maker_non_auto_removed_when_visible_exhausted() {
        let mut book = book();
        let maker = build(
            2,
            1,
            100,
            10,
            Side::Sell,
            TimeInForce::Gtc,
            OrderKind::ReserveOrder {
                hidden_quantity: Quantity(50),
                replenish_threshold: Quantity(5),
                replenish_amount: None,
                auto_replenish: false,
            },
        );
        book.execute_new_order(&maker).unwrap();
        let (changes, trades) = book.execute_new_order(&buy(1, 1, 100, 10)).unwrap();

        assert_eq!(trades.unwrap()[0].traded_quantity, Quantity(10));
        assert_eq!(
            change_of(&changes, &maker),
            Some(OrderStatus::Filled { filled_quantity: Quantity(10) })
        );
        // The hidden remainder is discarded, the level is gone.
        assert!(book.state.asks.is_empty());
        assert!(!book.state.index.contains_key(&(addr(2), Nonce(1))));
    }

    // ---------------------------------------------------------------
    // Self-trade prevention
    // ---------------------------------------------------------------

    fn book_with_stp(stp_mode: STPMode) -> OrderBook {
        OrderBook::new(
            BookConfig::default().with_hot(BookConfigHot::default().with_stp_mode(stp_mode)),
        )
    }

    #[test]
    fn test_stp_cancel_taker_keeps_partial_fills() {
        let mut book = book_with_stp(STPMode::CancelTaker);
        book.execute_new_order(&sell(2, 2, 100, 30)).unwrap();
        book.execute_new_order(&sell(1, 3, 100, 30)).unwrap();
        let taker = buy(1, 1, 100, 50);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        // The non-self depth ahead of the same-user maker is executed.
        assert_eq!(trades.unwrap()[0].traded_quantity, Quantity(30));
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::Canceled {
                filled_quantity: Quantity(30),
                reason: CancelReason::SelfTradePrevention
            })
        );
        // The same-user maker is untouched.
        assert!(book.state.index.contains_key(&(addr(1), Nonce(3))));
    }

    #[test]
    fn test_stp_cancel_taker_no_fill_is_rejected() {
        let mut book = book_with_stp(STPMode::CancelTaker);
        book.execute_new_order(&sell(1, 3, 100, 30)).unwrap();
        let taker = buy(1, 1, 100, 10);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        assert_eq!(trades, None);
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::Rejected { reason: RejectReason::SelfTradePrevention })
        );
    }

    #[test]
    fn test_stp_cancel_maker_removes_same_user_depth() {
        let mut book = book_with_stp(STPMode::CancelMaker);
        book.execute_new_order(&sell(1, 2, 100, 10)).unwrap();
        book.execute_new_order(&sell(2, 3, 100, 30)).unwrap();
        let taker = buy(1, 1, 100, 40);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        assert_eq!(
            change_of(&changes, &sell(1, 2, 100, 10)),
            Some(OrderStatus::Canceled {
                filled_quantity: Quantity(0),
                reason: CancelReason::SelfTradePrevention
            })
        );
        assert_eq!(trades.unwrap()[0].maker, sell(2, 3, 100, 30));
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::PartiallyFilled { filled_quantity: Quantity(30) })
        );
        // The remainder rests, the same-user maker is gone.
        assert_eq!(book.state.bids.len(), 1);
        assert!(!book.state.index.contains_key(&(addr(1), Nonce(2))));
    }

    #[test]
    fn test_stp_cancel_both_kills_taker_and_maker() {
        let mut book = book_with_stp(STPMode::CancelBoth);
        book.execute_new_order(&sell(1, 2, 100, 10)).unwrap();
        book.execute_new_order(&sell(2, 3, 100, 30)).unwrap();
        let taker = buy(1, 1, 100, 40);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        assert_eq!(trades, None);
        assert_eq!(
            change_of(&changes, &sell(1, 2, 100, 10)),
            Some(OrderStatus::Canceled {
                filled_quantity: Quantity(0),
                reason: CancelReason::SelfTradePrevention
            })
        );
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::Rejected { reason: RejectReason::SelfTradePrevention })
        );
        // The non-self maker is untouched.
        assert!(book.state.index.contains_key(&(addr(2), Nonce(3))));
    }

    #[test]
    fn test_stp_zero_address_bypasses() {
        let mut book = book_with_stp(STPMode::CancelBoth);
        // The zero-address maker has the same address as the taker but STP
        // never applies to anonymous orders.
        book.execute_new_order(&sell(0, 2, 100, 10)).unwrap();
        let taker = buy(0, 1, 100, 10);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        assert_eq!(trades.unwrap().len(), 1);
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::Filled { filled_quantity: Quantity(10) })
        );
    }

    // ---------------------------------------------------------------
    // Order kinds
    // ---------------------------------------------------------------

    #[test]
    fn test_post_only_crossing_is_rejected() {
        let mut book = book();
        book.execute_new_order(&sell(2, 2, 100, 10)).unwrap();
        let taker = build(1, 1, 100, 10, Side::Buy, TimeInForce::Gtc, OrderKind::PostOnly);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        assert_eq!(trades, None);
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::Rejected { reason: RejectReason::PostOnlyWouldCross })
        );
        assert!(book.state.bids.is_empty());
    }

    #[test]
    fn test_post_only_non_crossing_rests() {
        let mut book = book();
        book.execute_new_order(&sell(2, 2, 101, 10)).unwrap();
        let taker = build(1, 1, 100, 10, Side::Buy, TimeInForce::Gtc, OrderKind::PostOnly);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        assert_eq!(trades, None);
        assert_eq!(change_of(&changes, &taker), Some(OrderStatus::Open));
        assert_eq!(book.state.bids.len(), 1);
    }

    #[test]
    fn test_market_to_limit_rests_at_last_trade_price() {
        let mut book = book();
        book.execute_new_order(&sell(2, 2, 100, 30)).unwrap();
        book.execute_new_order(&sell(2, 3, 101, 30)).unwrap();
        let taker = build(1, 1, 0, 70, Side::Buy, TimeInForce::Gtc, OrderKind::MarketToLimit);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        let trades = trades.unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].price, Price(100));
        assert_eq!(trades[0].traded_quantity, Quantity(30));
        assert_eq!(trades[1].price, Price(101));
        assert_eq!(trades[1].traded_quantity, Quantity(30));
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::PartiallyFilled { filled_quantity: Quantity(60) })
        );
        // The 10 remainder converts to a limit order at the last trade price.
        let level = book.state.bids.get(&Price(101)).unwrap();
        assert_eq!(level.visible_quantity(), Quantity(10));
        assert!(book.state.asks.is_empty());
    }

    #[test]
    fn test_market_to_limit_without_liquidity_is_rejected() {
        let mut book = book();
        let taker = build(1, 1, 0, 50, Side::Buy, TimeInForce::Gtc, OrderKind::MarketToLimit);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        assert_eq!(trades, None);
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::Rejected { reason: RejectReason::InsufficientLiquidity })
        );
    }

    #[test]
    fn test_trailing_stop_converts_to_limit() {
        let mut book = book();
        book.execute_new_order(&sell(2, 2, 105, 10)).unwrap();
        let taker = build(
            1,
            1,
            0,
            10,
            Side::Buy,
            TimeInForce::Gtc,
            OrderKind::TrailingStop { trail_amount: Price(10), last_ref_price: Price(100) },
        );
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        // Effective limit 100 + 10 = 110 crosses the 105 ask.
        assert_eq!(trades.unwrap()[0].price, Price(105));
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::Filled { filled_quantity: Quantity(10) })
        );
    }

    #[test]
    fn test_pegged_orders_price_off_reference() {
        let mut book = book();
        // BestAsk peg fills against the ask it pegs to.
        book.execute_new_order(&sell(2, 2, 100, 10)).unwrap();
        let pegged = build(
            1,
            1,
            0,
            10,
            Side::Buy,
            TimeInForce::Gtc,
            OrderKind::Pegged {
                reference_price_offset: 0,
                reference_price_type: PegReferenceType::BestAsk,
            },
        );
        let (changes, trades) = book.execute_new_order(&pegged).unwrap();
        assert_eq!(trades.unwrap()[0].price, Price(100));
        assert_eq!(
            change_of(&changes, &pegged),
            Some(OrderStatus::Filled { filled_quantity: Quantity(10) })
        );

        // Mid peg rests at the midpoint.
        book.execute_new_order(&sell(2, 3, 100, 10)).unwrap();
        book.execute_new_order(&buy(3, 1, 90, 10)).unwrap();
        let mid = build(
            1,
            2,
            0,
            10,
            Side::Buy,
            TimeInForce::Gtc,
            OrderKind::Pegged {
                reference_price_offset: 0,
                reference_price_type: PegReferenceType::Mid,
            },
        );
        let (changes, trades) = book.execute_new_order(&mid).unwrap();
        assert_eq!(trades, None);
        assert_eq!(change_of(&changes, &mid), Some(OrderStatus::Open));
        assert_eq!(book.state.bids.get(&Price(95)).unwrap().len(), 1);
    }

    #[test]
    fn test_pegged_without_reference_is_rejected() {
        let mut book = book();
        let pegged = build(
            1,
            1,
            0,
            10,
            Side::Buy,
            TimeInForce::Gtc,
            OrderKind::Pegged {
                reference_price_offset: 0,
                reference_price_type: PegReferenceType::LastTrade,
            },
        );
        let (changes, trades) = book.execute_new_order(&pegged).unwrap();
        assert_eq!(trades, None);
        assert_eq!(
            change_of(&changes, &pegged),
            Some(OrderStatus::Rejected { reason: RejectReason::InvalidPriceLevel })
        );
    }

    // ---------------------------------------------------------------
    // Time in force
    // ---------------------------------------------------------------

    #[test]
    fn test_ioc_cancels_unfilled_remainder() {
        let mut book = book();
        book.execute_new_order(&sell(2, 2, 100, 30)).unwrap();
        let taker = build(1, 1, 100, 50, Side::Buy, TimeInForce::Ioc, OrderKind::Standard);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        assert_eq!(trades.unwrap()[0].traded_quantity, Quantity(30));
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::Canceled {
                filled_quantity: Quantity(30),
                reason: CancelReason::InsufficientLiquidity
            })
        );
        // Nothing rests.
        assert!(book.state.bids.is_empty());
    }

    #[test]
    fn test_fok_insufficient_liquidity_rejects_without_trades() {
        let mut book = book();
        book.execute_new_order(&sell(2, 2, 100, 30)).unwrap();
        let taker = build(1, 1, 100, 50, Side::Buy, TimeInForce::Fok, OrderKind::Standard);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        assert_eq!(trades, None);
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::Rejected { reason: RejectReason::InsufficientLiquidity })
        );
        // The maker is untouched.
        assert_eq!(book.state.asks.len(), 1);
    }

    #[test]
    fn test_fok_sufficient_liquidity_fills_across_levels() {
        let mut book = book();
        book.execute_new_order(&sell(2, 2, 100, 30)).unwrap();
        book.execute_new_order(&sell(2, 3, 101, 30)).unwrap();
        let taker = build(1, 1, 101, 50, Side::Buy, TimeInForce::Fok, OrderKind::Standard);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        let trades = trades.unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].traded_quantity, Quantity(30));
        assert_eq!(trades[1].traded_quantity, Quantity(20));
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::Filled { filled_quantity: Quantity(50) })
        );
    }

    #[test]
    fn test_gtd_expired_at_admission_is_cancelled() {
        let mut book = book();
        // TimestampMs(0) + any positive GTD lifetime is long in the past.
        let taker = build(1, 1, 100, 10, Side::Buy, TimeInForce::Gtd(1), OrderKind::Standard);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        assert_eq!(trades, None);
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::Canceled {
                filled_quantity: Quantity(0),
                reason: CancelReason::TimeInForceExpired
            })
        );
    }

    /// A clock the test can move forward, to age resting GTD orders.
    #[derive(Debug, Clone)]
    struct TestClock(Rc<Cell<TimestampMs>>);

    impl TestClock {
        fn new(now: TimestampMs) -> Self {
            Self(Rc::new(Cell::new(now)))
        }

        fn set(&self, now: TimestampMs) {
            self.0.set(now);
        }
    }

    impl Clock for TestClock {
        fn now_millis(&self) -> TimestampMs {
            self.0.get()
        }
    }

    #[test]
    fn test_gtd_maker_expired_is_cancelled_in_sweep() {
        let clock = TestClock::new(TimestampMs(1_000));
        let mut book = book().with_clock(Box::new(clock.clone()));
        // Deadline 0 + 1h = 3_600_000, far in the future at admission.
        let gtd_maker = build(2, 1, 100, 10, Side::Sell, TimeInForce::Gtd(1), OrderKind::Standard);
        book.execute_new_order(&gtd_maker).unwrap();
        book.execute_new_order(&sell(2, 2, 100, 10)).unwrap();

        // The GTD lifetime lapses while both orders rest.
        clock.set(TimestampMs(4_000_000));
        let taker = buy(1, 1, 100, 20);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();

        // The expired head is cancelled the moment the sweep touches it.
        assert_eq!(
            change_of(&changes, &gtd_maker),
            Some(OrderStatus::Canceled {
                filled_quantity: Quantity(0),
                reason: CancelReason::TimeInForceExpired
            })
        );
        // The healthy maker fills, the taker rests 10.
        assert_eq!(trades.unwrap()[0].maker, sell(2, 2, 100, 10));
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::PartiallyFilled { filled_quantity: Quantity(10) })
        );
    }

    // ---------------------------------------------------------------
    // Cancellation
    // ---------------------------------------------------------------

    #[test]
    fn test_cancel_removes_order_from_book() {
        let mut book = book();
        book.execute_new_order(&buy(1, 1, 100, 10)).unwrap();
        book.execute_new_order(&buy(1, 2, 100, 10)).unwrap();
        let cancel = CancelOrder::new(
            Symbol([0; 32]),
            Hash32([0; 32]),
            addr(1),
            Nonce(1),
            TimestampMs(0),
            Signature::default(),
        );
        let changes = book.execute_cancel_order(&cancel).unwrap();

        assert_eq!(
            change_of(&changes, &buy(1, 1, 100, 10)),
            Some(OrderStatus::Canceled {
                filled_quantity: Quantity(0),
                reason: CancelReason::UserRequested
            })
        );
        // The other order of the same user stays in the book.
        assert!(!book.state.index.contains_key(&(addr(1), Nonce(1))));
        assert!(book.state.index.contains_key(&(addr(1), Nonce(2))));
        let level = book.state.bids.get(&Price(100)).unwrap();
        assert_eq!(level.len(), 1);
        assert_eq!(level.visible_quantity(), Quantity(10));
    }

    #[test]
    fn test_cancel_unknown_order_is_noop() {
        let mut book = book();
        let cancel = CancelOrder::new(
            Symbol([0; 32]),
            Hash32([0; 32]),
            addr(1),
            Nonce(9),
            TimestampMs(0),
            Signature::default(),
        );
        assert!(book.execute_cancel_order(&cancel).unwrap().is_empty());
    }

    #[test]
    fn test_cancel_last_order_removes_level() {
        let mut book = book();
        book.execute_new_order(&buy(1, 1, 100, 10)).unwrap();
        let cancel = CancelOrder::new(
            Symbol([0; 32]),
            Hash32([0; 32]),
            addr(1),
            Nonce(1),
            TimestampMs(0),
            Signature::default(),
        );
        book.execute_cancel_order(&cancel).unwrap();
        assert!(book.state.bids.is_empty());
        assert!(book.state.user_orders.is_empty());
    }

    // ---------------------------------------------------------------
    // Admission checks
    // ---------------------------------------------------------------

    #[test]
    fn test_duplicate_order_rejected() {
        let mut book = book();
        book.execute_new_order(&buy(1, 1, 100, 10)).unwrap();
        let (changes, trades) = book.execute_new_order(&buy(1, 1, 100, 10)).unwrap();
        assert_eq!(trades, None);
        assert_eq!(
            change_of(&changes, &buy(1, 1, 100, 10)),
            Some(OrderStatus::Rejected { reason: RejectReason::DuplicateOrderId })
        );
        // Only the first order rests.
        assert_eq!(book.state.bids.get(&Price(100)).unwrap().len(), 1);
    }

    #[test]
    fn test_price_and_quantity_validation() {
        let hot = BookConfigHot::default()
            .with_tick_size(Price(5))
            .with_lot_size(Quantity(10))
            .with_min_order_size(Quantity(20))
            .with_max_order_size(Quantity(100));
        let mut book = OrderBook::new(BookConfig::default().with_hot(hot));

        let bad_tick = buy(1, 1, 103, 50);
        assert_eq!(
            change_of(&book.execute_new_order(&bad_tick).unwrap().0, &bad_tick),
            Some(OrderStatus::Rejected { reason: RejectReason::InvalidPrice })
        );

        let bad_lot = buy(1, 2, 100, 7);
        assert_eq!(
            change_of(&book.execute_new_order(&bad_lot).unwrap().0, &bad_lot),
            Some(OrderStatus::Rejected { reason: RejectReason::InvalidQuantity })
        );

        let too_small = buy(1, 3, 100, 10);
        assert_eq!(
            change_of(&book.execute_new_order(&too_small).unwrap().0, &too_small),
            Some(OrderStatus::Rejected { reason: RejectReason::OrderSizeOutOfRange })
        );

        let too_large = buy(1, 4, 100, 110);
        assert_eq!(
            change_of(&book.execute_new_order(&too_large).unwrap().0, &too_large),
            Some(OrderStatus::Rejected { reason: RejectReason::OrderSizeOutOfRange })
        );

        let zero = buy(1, 5, 100, 0);
        assert_eq!(
            change_of(&book.execute_new_order(&zero).unwrap().0, &zero),
            Some(OrderStatus::Rejected { reason: RejectReason::InvalidQuantity })
        );

        // A valid order still rests.
        let valid = buy(1, 6, 100, 20);
        assert_eq!(
            change_of(&book.execute_new_order(&valid).unwrap().0, &valid),
            Some(OrderStatus::Open)
        );
    }

    #[test]
    fn test_risk_checks_at_admission() {
        let risk = RiskConfig::default().with_max_open_orders_per_account(1);
        let hot = BookConfigHot::default().with_risk_config(risk);
        let mut book = OrderBook::new(BookConfig::default().with_hot(hot));

        book.execute_new_order(&buy(1, 1, 100, 10)).unwrap();
        let second = buy(1, 2, 100, 10);
        let (changes, trades) = book.execute_new_order(&second).unwrap();
        assert_eq!(trades, None);
        assert_eq!(
            change_of(&changes, &second),
            Some(OrderStatus::Rejected { reason: RejectReason::RiskMaxOpenOrders })
        );
    }

    #[test]
    fn test_kill_switch_blocks_new_orders_not_cancels() {
        let mut book = book();
        book.execute_new_order(&buy(1, 1, 100, 10)).unwrap();
        book.set_kill_switch(true);

        let taker = sell(2, 1, 100, 10);
        let (changes, trades) = book.execute_new_order(&taker).unwrap();
        assert_eq!(trades, None);
        assert_eq!(
            change_of(&changes, &taker),
            Some(OrderStatus::Rejected { reason: RejectReason::KillSwitchActive })
        );

        // Cancellations still go through.
        let cancel = CancelOrder::new(
            Symbol([0; 32]),
            Hash32([0; 32]),
            addr(1),
            Nonce(1),
            TimestampMs(0),
            Signature::default(),
        );
        let changes = book.execute_cancel_order(&cancel).unwrap();
        assert_eq!(
            change_of(&changes, &buy(1, 1, 100, 10)),
            Some(OrderStatus::Canceled {
                filled_quantity: Quantity(0),
                reason: CancelReason::UserRequested
            })
        );
    }

    // ---------------------------------------------------------------
    // Pooled fanout
    // ---------------------------------------------------------------

    #[test]
    fn test_fanout_returns_pooled_buffers_on_drop() {
        let mut book = book().with_listeners(
            Listeners::default()
                .with_book_state_listener(Box::new(|msg| {
                    // A synchronous consumer reads the message and drops it.
                    assert!(!msg.is_empty());
                }))
                .with_trade_state_listener(Box::new(|trades| {
                    assert!(!trades.is_empty());
                })),
        );
        book.execute(&OrderMsg::NewOrder(sell(2, 2, 100, 10))).unwrap();
        book.execute(&OrderMsg::NewOrder(buy(1, 1, 100, 10))).unwrap();
        // Both fanouts dropped their messages: the buffers are back in the pools.
        assert_eq!(
            book.memory_pools.changes_pool.available(),
            book.memory_pools.changes_pool.capacity()
        );
        assert_eq!(
            book.memory_pools.trades_pool.available(),
            book.memory_pools.trades_pool.capacity()
        );
    }

    #[test]
    fn test_fanout_async_consumer_returns_buffer_from_task() {
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut book = book().with_listeners(Listeners::default().with_book_state_listener(
            Box::new(move |msg| {
                // An asynchronous consumer moves the message into its task:
                // the buffer lives with the task and returns to the pool
                // when the task drops it, off the match engine thread.
                let sender = sender.clone();
                std::thread::spawn(move || {
                    assert!(!msg.is_empty());
                    drop(msg);
                    sender.send(()).expect("receiver is alive");
                });
            }),
        ));
        book.execute(&OrderMsg::NewOrder(sell(2, 2, 100, 10))).unwrap();
        receiver.recv().expect("fanout task finished");
        assert_eq!(
            book.memory_pools.changes_pool.available(),
            book.memory_pools.changes_pool.capacity()
        );
    }

    // ---------------------------------------------------------------
    // Pooled user order lists
    // ---------------------------------------------------------------

    #[test]
    fn test_user_order_list_buffer_returns_to_pool() {
        let mut book = book();
        book.execute_new_order(&buy(1, 1, 100, 10)).unwrap();
        assert_eq!(
            book.memory_pools.index_lists_pool.available(),
            book.memory_pools.index_lists_pool.capacity() - 1
        );
        let cancel = CancelOrder::new(
            Symbol([0; 32]),
            Hash32([0; 32]),
            addr(1),
            Nonce(1),
            TimestampMs(0),
            Signature::default(),
        );
        book.execute_cancel_order(&cancel).unwrap();
        // Dropping the user's last list returned its buffer to the pool.
        assert_eq!(
            book.memory_pools.index_lists_pool.available(),
            book.memory_pools.index_lists_pool.capacity()
        );
    }

    #[test]
    fn test_user_order_lists_snapshot_roundtrip() {
        let mut book = book();
        book.execute_new_order(&buy(1, 1, 100, 10)).unwrap();
        book.execute_new_order(&buy(1, 2, 100, 10)).unwrap();

        // The pooled list serializes as the plain vector: the wire format of
        // the state is unchanged.
        let list = book.state.user_orders.get(&addr(1)).unwrap();
        assert_eq!(
            rmp_serde::to_vec(&**list).unwrap(),
            rmp_serde::to_vec(&vec![0u32, 1u32]).unwrap()
        );

        // Snapshot roundtrip and restore into a fresh book.
        let snapshot = rmp_serde::to_vec(&book.state).unwrap();
        let restored: OrderBookState = rmp_serde::from_slice(&snapshot).unwrap();
        let mut restored_book = OrderBook::new(BookConfig::default());
        restored_book.state = restored;
        restored_book.attach_pools();
        assert_eq!(restored_book.state.user_orders.get(&addr(1)).unwrap().len(), 2);

        // The restored lists are attached to the real pool: removing the
        // orders returns their buffers.
        for nonce in [1u64, 2] {
            let cancel = CancelOrder::new(
                Symbol([0; 32]),
                Hash32([0; 32]),
                addr(1),
                Nonce(nonce),
                TimestampMs(0),
                Signature::default(),
            );
            restored_book.execute_cancel_order(&cancel).unwrap();
        }
        assert_eq!(
            restored_book.memory_pools.index_lists_pool.available(),
            restored_book.memory_pools.index_lists_pool.capacity()
        );
    }

    // ---------------------------------------------------------------
    // ---------------------------------------------------------------
    // Single-thread execution benchmark
    // ---------------------------------------------------------------

    /// The benchmark scenarios: each scenario prepares and persists its own
    /// seed data set and fires a dedicated order stream.
    #[derive(Clone, Copy)]
    enum BenchScenario {
        Standard,
        Iceberg,
        Reserve,
        Ioc,
        Mixed,
    }

    impl BenchScenario {
        const ALL: [BenchScenario; 5] = [
            BenchScenario::Standard,
            BenchScenario::Iceberg,
            BenchScenario::Reserve,
            BenchScenario::Ioc,
            BenchScenario::Mixed,
        ];

        fn name(self) -> &'static str {
            match self {
                BenchScenario::Standard => "standard",
                BenchScenario::Iceberg => "iceberg",
                BenchScenario::Reserve => "reserve",
                BenchScenario::Ioc => "ioc",
                BenchScenario::Mixed => "mixed",
            }
        }

        fn seed_rng(self) -> u64 {
            match self {
                BenchScenario::Standard => 0x9E37_79B9_7F4A_7C15,
                BenchScenario::Iceberg => 0xD1B5_4A32_D192_ED03,
                BenchScenario::Reserve => 0x4528_21E6_38D0_1377,
                BenchScenario::Ioc => 0x243F_6A88_85A3_08D3,
                BenchScenario::Mixed => 0x1319_8A2E_0370_7344,
            }
        }

        fn fire_rng(self) -> u64 {
            self.seed_rng().wrapping_add(0xA409_3822_299F_31D0)
        }

        /// The order kind of the `index`-th seed order of the scenario.
        fn seed_kind(self, index: usize, rng: &mut XorShift64) -> OrderKind {
            match self {
                BenchScenario::Standard | BenchScenario::Ioc => OrderKind::Standard,
                BenchScenario::Iceberg => {
                    OrderKind::Iceberg { hidden_quantity: Quantity(rng.range(20, 200)) }
                }
                BenchScenario::Reserve => OrderKind::ReserveOrder {
                    hidden_quantity: Quantity(200),
                    replenish_threshold: Quantity(5),
                    replenish_amount: None,
                    auto_replenish: true,
                },
                BenchScenario::Mixed => match index % 3 {
                    0 => OrderKind::Standard,
                    1 => OrderKind::Iceberg { hidden_quantity: Quantity(rng.range(20, 200)) },
                    _ => OrderKind::ReserveOrder {
                        hidden_quantity: Quantity(200),
                        replenish_threshold: Quantity(5),
                        replenish_amount: None,
                        auto_replenish: true,
                    },
                },
            }
        }

        /// The kind and time in force of the `index`-th fired order.
        fn fire_kind(self, index: u64, rng: &mut XorShift64) -> (OrderKind, TimeInForce) {
            match self {
                BenchScenario::Standard => (OrderKind::Standard, TimeInForce::Gtc),
                BenchScenario::Iceberg => (
                    OrderKind::Iceberg { hidden_quantity: Quantity(rng.range(0, 100)) },
                    TimeInForce::Gtc,
                ),
                BenchScenario::Reserve => (
                    OrderKind::ReserveOrder {
                        hidden_quantity: Quantity(rng.range(0, 100)),
                        replenish_threshold: Quantity(5),
                        replenish_amount: None,
                        auto_replenish: true,
                    },
                    TimeInForce::Gtc,
                ),
                BenchScenario::Ioc => (OrderKind::Standard, TimeInForce::Ioc),
                BenchScenario::Mixed => match index % 4 {
                    0 => (OrderKind::Standard, TimeInForce::Gtc),
                    1 => (
                        OrderKind::Iceberg { hidden_quantity: Quantity(rng.range(0, 100)) },
                        TimeInForce::Gtc,
                    ),
                    2 => (
                        OrderKind::ReserveOrder {
                            hidden_quantity: Quantity(rng.range(0, 100)),
                            replenish_threshold: Quantity(5),
                            replenish_amount: None,
                            auto_replenish: true,
                        },
                        TimeInForce::Gtc,
                    ),
                    _ => (OrderKind::Standard, TimeInForce::Ioc),
                },
            }
        }
    }

    /// Number of resting orders prepared, persisted and injected per scenario.
    const BENCH_SEED_ORDERS: usize = 20_000;
    /// Number of untimed orders fired to warm the pools before the statistics.
    const BENCH_WARMUP_ORDERS: u64 = 500_000;
    /// Duration of the latency phase and the throughput phase, in seconds.
    const BENCH_PHASE: Duration = Duration::from_secs(10);
    /// Root directory of the benchmark artifacts.
    const BENCH_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/benches");

    /// Minimal deterministic PRNG for the benchmark streams, no external deps.
    struct XorShift64(u64);

    impl XorShift64 {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn range(&mut self, low: u64, high: u64) -> u64 {
            low + self.next() % (high - low + 1)
        }
    }

    /// Pool sizing tuned to the benchmark scale: the containers are
    /// pre-allocated so the measurement phases never reallocate. The book
    /// holds ~20k seed orders plus a thin self-balancing layer of resting
    /// orders, and the per-user lists hold one entry per resting order of
    /// the ~250 benchmark users.
    fn bench_config() -> BookConfig {
        BookConfig::default().with_cold(
            BookConfigCold::default()
                .with_arena_size(100_000)
                .with_order_index_size(100_000)
                .with_user_order_map_size(256)
                .with_map_price_level_size(1_024)
                .with_order_index_list_size(65_536)
                .with_order_index_list_pool_size(256)
                .with_trade_list_size(32)
                .with_trade_list_pool_size(128),
        )
    }

    /// Generates the deterministic resting orders of a scenario: bids below
    /// 10_000, asks above, so the seed book does not cross itself.
    fn generate_seed_orders(scenario: BenchScenario) -> Vec<Order> {
        let mut rng = XorShift64(scenario.seed_rng());
        let mut orders = Vec::with_capacity(BENCH_SEED_ORDERS);
        for nonce in 0..BENCH_SEED_ORDERS {
            let (side, price) = if nonce % 2 == 0 {
                (Side::Sell, rng.range(10_001, 10_200))
            } else {
                (Side::Buy, rng.range(9_800, 9_999))
            };
            orders.push(build(
                rng.range(1, 250) as u8,
                nonce as u64 + 1,
                price,
                rng.range(1, 100),
                side,
                TimeInForce::Gtc,
                scenario.seed_kind(nonce, &mut rng),
            ));
        }
        orders
    }

    /// Loads the persisted seed orders of the scenario, generating and
    /// persisting them on the first run, so repeated runs inject the
    /// identical book.
    fn load_or_generate_seed_orders(scenario: BenchScenario) -> Vec<Order> {
        let path = Path::new(BENCH_DIR).join(format!("data/{}_seed.bin", scenario.name()));
        if path.exists() {
            let bytes = std::fs::read(&path).expect("read persisted seed orders");
            let orders: Vec<Order> = rmp_serde::from_slice(&bytes).expect("decode seed orders");
            assert_eq!(orders.len(), BENCH_SEED_ORDERS);
            return orders;
        }
        let orders = generate_seed_orders(scenario);
        std::fs::create_dir_all(path.parent().expect("bench data dir"))
            .expect("create bench data dir");
        std::fs::write(&path, rmp_serde::to_vec(&orders).expect("encode seed orders"))
            .expect("persist seed orders");
        orders
    }

    /// Builds the `fired`-th order of the scenario's fire stream. The stream
    /// alternates buys just above and sells just below the 10_000 boundary,
    /// so the two sides keep consuming each other's resting orders and the
    /// book stays at a steady size for the whole measurement.
    fn build_fire_order(scenario: BenchScenario, rng: &mut XorShift64, fired: u64) -> Order {
        let buy = fired.is_multiple_of(2);
        let (side, price) = if buy {
            (Side::Buy, 10_000 + rng.range(0, 10))
        } else {
            (Side::Sell, 10_000 - rng.range(0, 10))
        };
        let (kind, time_in_force) = scenario.fire_kind(fired, rng);
        build(
            rng.range(1, 250) as u8,
            BENCH_SEED_ORDERS as u64 + fired + 1,
            price,
            rng.range(1, 50),
            side,
            time_in_force,
            kind,
        )
    }

    /// Nearest-rank percentile over a sorted slice of nanosecond latencies.
    fn percentile(sorted: &[u64], quantile: f64) -> u64 {
        let rank = (sorted.len() as f64 * quantile).ceil() as usize;
        sorted[rank.saturating_sub(1)]
    }

    /// Latency histogram buckets over a sorted slice of nanoseconds.
    fn latency_histogram(latencies: &[u64]) -> Vec<(String, usize)> {
        let mut buckets = Vec::new();
        let mut start = 0;
        for &upper in &[1u64, 2, 4, 8, 16, 32, 64] {
            let end = latencies.partition_point(|&ns| ns < upper * 1_000);
            let label =
                if upper == 1 { "< 1".to_string() } else { format!("{} - {upper}", upper / 2) };
            buckets.push((label, end - start));
            start = end;
        }
        buckets.push(("≥ 64".to_string(), latencies.len() - start));
        buckets
    }

    /// The measurements of one benchmark scenario.
    struct ScenarioReport {
        scenario: BenchScenario,
        latency_orders: u64,
        trades: usize,
        resting_at_end: usize,
        latency_secs: f64,
        throughput: f64,
        mean_us: f64,
        p50_us: f64,
        p90_us: f64,
        p99_us: f64,
        max_us: f64,
        histogram: Vec<(String, usize)>,
    }

    /// Runs one scenario: injects the persisted seed data into a book with
    /// tuned pools, warms the memory, measures per-order latencies for
    /// [`BENCH_PHASE`], then measures the batch throughput for [`BENCH_PHASE`].
    fn run_scenario(scenario: BenchScenario) -> ScenarioReport {
        // 1. Prepare and persist the seed data set of the scenario.
        let seed_orders = load_or_generate_seed_orders(scenario);

        // 2. Inject the persisted data into a book with tuned pools.
        let mut book = OrderBook::new(bench_config());
        for order in &seed_orders {
            book.execute(&OrderMsg::NewOrder(*order)).expect("seed order executes");
        }
        assert_eq!(book.state.index.len(), BENCH_SEED_ORDERS);

        // Count the trades of the measurement phases.
        let trades_fired = Arc::new(AtomicUsize::new(0));
        let trades_counter = Arc::clone(&trades_fired);
        let mut book = book.with_listeners(Listeners::default().with_trade_state_listener(
            Box::new(move |trades| {
                trades_counter.fetch_add(trades.len(), Ordering::Relaxed);
            }),
        ));

        // Warmup: fire untimed orders so the pools, the arena and the price
        // levels reach their steady state before the statistics are taken.
        let mut rng = XorShift64(scenario.fire_rng());
        let mut fired = 0u64;
        for _ in 0..BENCH_WARMUP_ORDERS {
            let order = build_fire_order(scenario, &mut rng, fired);
            book.execute(&OrderMsg::NewOrder(order)).expect("warmup order executes");
            fired += 1;
        }

        // 3. Latency phase: per-order timings until the phase budget elapses.
        let mut latencies: Vec<u64> = Vec::with_capacity(20_000_000);
        let phase_start = Instant::now();
        while phase_start.elapsed() < BENCH_PHASE {
            let order = build_fire_order(scenario, &mut rng, fired);
            let start = Instant::now();
            book.execute(&OrderMsg::NewOrder(order)).expect("order executes");
            latencies.push(start.elapsed().as_nanos() as u64);
            fired += 1;
        }
        let latency_secs = phase_start.elapsed().as_secs_f64();
        let latency_orders = latencies.len() as u64;
        assert!(latency_orders > 0, "latency phase fired no orders");

        // Throughput phase: no per-order timings, only the batch duration.
        let mut throughput_orders = 0u64;
        let phase_start = Instant::now();
        while phase_start.elapsed() < BENCH_PHASE {
            let order = build_fire_order(scenario, &mut rng, fired);
            book.execute(&OrderMsg::NewOrder(order)).expect("order executes");
            throughput_orders += 1;
            fired += 1;
        }
        let throughput = throughput_orders as f64 / phase_start.elapsed().as_secs_f64();

        // Sanity: the stream really traded.
        let trades = trades_fired.load(Ordering::Relaxed);
        assert!(trades > 0, "scenario produced no trades");

        // 4. Percentiles and the report data.
        latencies.sort_unstable();
        let micros = |ns: u64| ns as f64 / 1_000.0;
        let mean_us = micros(latencies.iter().sum::<u64>() / latencies.len() as u64);
        let p50_us = micros(percentile(&latencies, 0.50));
        let p90_us = micros(percentile(&latencies, 0.90));
        let p99_us = micros(percentile(&latencies, 0.99));
        let max_us = micros(*latencies.last().expect("latencies are not empty"));
        let resting_at_end = book.state.index.len();

        ScenarioReport {
            scenario,
            latency_orders,
            trades,
            resting_at_end,
            latency_secs,
            throughput,
            mean_us,
            p50_us,
            p90_us,
            p99_us,
            max_us,
            histogram: latency_histogram(&latencies),
        }
    }

    /// Writes the combined markdown benchmark report and returns its path.
    fn write_bench_report(reports: &[ScenarioReport]) -> PathBuf {
        let path = Path::new(BENCH_DIR).join("reports/book_bench_report.md");
        std::fs::create_dir_all(path.parent().expect("bench report dir"))
            .expect("create report dir");

        let profile = if cfg!(debug_assertions) { "debug" } else { "release" };
        let mut report = String::new();
        report.push_str("# OrderBook execution benchmark (single thread)\n\n");
        report.push_str("## Configuration\n\n");
        report.push_str("| metric | value |\n|---|---|\n");
        report.push_str(&format!("| generated at | {} ms epoch |\n", util::time::now_ms()));
        report.push_str(&format!("| build profile | {profile} |\n"));
        report.push_str(&format!("| seed orders per scenario | {} |\n", BENCH_SEED_ORDERS));
        report.push_str(&format!("| warmup orders per scenario | {} |\n", BENCH_WARMUP_ORDERS));
        report.push_str(&format!("| phase duration | {} s |\n", BENCH_PHASE.as_secs()));
        report.push_str("| pool tuning | arena 100k, index 100k, user map 256, price levels 1k, index lists 65k, trade lists 32 |\n");
        report.push_str("\n## Summary\n\n");
        report.push_str(
            "| scenario | orders (latency phase) | p50 | p90 | p99 | mean | throughput |\n",
        );
        report.push_str("|---|---|---|---|---|---|---|\n");
        for r in reports {
            report.push_str(&format!(
                "| {} | {} | {:.2} µs | {:.2} µs | {:.2} µs | {:.2} µs | {:.0} orders/s |\n",
                r.scenario.name(),
                r.latency_orders,
                r.p50_us,
                r.p90_us,
                r.p99_us,
                r.mean_us,
                r.throughput,
            ));
        }
        for r in reports {
            report.push_str(&format!("\n## Scenario: {}\n\n", r.scenario.name()));
            report.push_str("| metric | value |\n|---|---|\n");
            report.push_str(&format!("| orders fired (latency phase) | {} |\n", r.latency_orders));
            report.push_str(&format!("| trades generated | {} |\n", r.trades));
            report.push_str(&format!("| resting orders at end | {} |\n", r.resting_at_end));
            report.push_str(&format!("| latency phase duration | {:.3} s |\n", r.latency_secs));
            report.push_str(&format!("| mean | {:.2} µs |\n", r.mean_us));
            report.push_str(&format!("| p50 | {:.2} µs |\n", r.p50_us));
            report.push_str(&format!("| p90 | {:.2} µs |\n", r.p90_us));
            report.push_str(&format!("| p99 | {:.2} µs |\n", r.p99_us));
            report.push_str(&format!("| max | {:.2} µs |\n", r.max_us));
            report.push_str(&format!("| throughput | {:.0} orders/s |\n", r.throughput));
            report.push_str("\n| bucket (µs) | count |\n|---|---|\n");
            for (label, count) in &r.histogram {
                report.push_str(&format!("| {label} | {count} |\n"));
            }
        }

        std::fs::write(&path, &report).expect("write benchmark report");
        path
    }

    #[test]
    #[ignore = "single-thread benchmark: cargo test --release -p primitives book_benchmark -- --ignored --nocapture"]
    fn book_benchmark() {
        let reports: Vec<ScenarioReport> =
            BenchScenario::ALL.iter().copied().map(run_scenario).collect();
        let report_path = write_bench_report(&reports);
        println!("benchmark report written to {}", report_path.display());
    }
}
