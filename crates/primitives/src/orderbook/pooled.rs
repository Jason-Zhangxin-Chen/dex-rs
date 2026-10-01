//! Pre-allocated memory pools and pooled types of the book.
//!
//! The pools back the hot-path buffers: state replication messages, trade
//! lists and the per-user order index lists. The buffers are checked out via
//! RAII guards and return to their pool when dropped, so the hot path never
//! allocates heap memory.

use std::fmt;
use std::ops::{Deref, DerefMut};

use cache::object_pool::{Cache, CacheGuard};
use serde::{Deserialize, Serialize};

use crate::message::hot_path::Trade;
use crate::message::side_path::OrderChange;
use crate::order::OrderIdx;
use crate::orderbook::config::BookConfigCold;

/// Default number of pooled change vectors.
const DEFAULT_CHANGES_POOL_SIZE: usize = 16;
/// Default capacity of a pooled change vector.
const DEFAULT_CHANGES_CAPACITY: usize = 8;
/// Default number of pooled trade vectors.
const DEFAULT_TRADES_POOL_SIZE: usize = 16;
/// Default capacity of a pooled trade vector.
const DEFAULT_TRADES_CAPACITY: usize = 16;
/// Default number of pooled order index vectors (user order lists).
const DEFAULT_INDEX_LIST_POOL_SIZE: usize = 64;
/// Default capacity of a pooled order index vector.
const DEFAULT_INDEX_LIST_CAPACITY: usize = 4;

/// A set of pre-allocated memory pools for the book, used by the execution
/// path to collect state replication messages and trade events, and to back
/// the per-user order index lists.
pub struct MemoryPools {
    /// Pool of the change vector buffers of replication messages. The
    /// execution collects the changes of an order execution into a buffer of
    /// this pool; the sweep's price levels check out sub-buffers from the
    /// same pool and merge them into the message buffer.
    pub changes_pool: Cache<Vec<OrderChange>>,

    /// Pool of Vec<Trade>, used by the matching sweep to collect the trades
    /// of an execution.
    pub trades_pool: Cache<Vec<Trade>>,

    /// Pool of Vec<OrderIdx> backing the per-user order lists and the
    /// per-level removed order lists of the sweep.
    pub index_lists_pool: Cache<Vec<OrderIdx>>,
}

impl MemoryPools {
    /// Pre-allocates the pools used on the hot path, sized from the cold
    /// config (with defaults when a knob is not set). The pooled vectors are
    /// created with a capacity so that the hot path never allocates.
    pub fn new(cold: &BookConfigCold) -> Self {
        let trade_pool_size =
            cold.trade_list_pool_size.map_or(DEFAULT_TRADES_POOL_SIZE, |v| v as usize);
        let trade_capacity = cold.trade_list_size.map_or(DEFAULT_TRADES_CAPACITY, |v| v as usize);
        let index_pool_size =
            cold.order_index_list_pool_size.map_or(DEFAULT_INDEX_LIST_POOL_SIZE, |v| v as usize);
        let index_capacity =
            cold.order_index_list_size.map_or(DEFAULT_INDEX_LIST_CAPACITY, |v| v as usize);
        Self {
            changes_pool: Cache::new(DEFAULT_CHANGES_POOL_SIZE, || {
                Vec::with_capacity(DEFAULT_CHANGES_CAPACITY)
            }),
            trades_pool: Cache::new(trade_pool_size, move || Vec::with_capacity(trade_capacity)),
            index_lists_pool: Cache::new(index_pool_size, move || {
                Vec::with_capacity(index_capacity)
            }),
        }
    }
}

/// A per-user list of order indices backed by a pooled buffer. The buffer is
/// checked out of the index list pool at construction and returns to the pool
/// when the list is dropped. In a snapshot the list serializes as the plain
/// vector (the wire format of the state is unchanged); deserialization
/// reconstructs the list around a detached buffer that
/// [`crate::orderbook::book::OrderBook::attach_pools`] re-attaches to the
/// real pool.
pub struct PooledIndexList {
    /// The pooled buffer.
    guard: CacheGuard<Vec<OrderIdx>>,
    /// The pool the buffer was checked out from, used by clone, attach and
    /// deserialization.
    pool: Cache<Vec<OrderIdx>>,
}

impl PooledIndexList {
    /// Creates a list with a buffer checked out of `pool`.
    pub(crate) fn new(pool: &Cache<Vec<OrderIdx>>) -> Self {
        Self { guard: pool.acquire(), pool: pool.clone() }
    }

    /// Re-attaches the buffer to another pool (snapshot restore).
    pub(crate) fn attach(&mut self, pool: &Cache<Vec<OrderIdx>>) {
        let list = self.guard.take();
        self.guard = pool.wrap(list);
        self.pool = pool.clone();
    }
}

impl Deref for PooledIndexList {
    type Target = Vec<OrderIdx>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for PooledIndexList {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Clone for PooledIndexList {
    fn clone(&self) -> Self {
        let mut guard = self.pool.acquire();
        guard.extend(self.guard.iter().copied());
        Self { guard, pool: self.pool.clone() }
    }
}

impl PartialEq for PooledIndexList {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl fmt::Debug for PooledIndexList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.guard.iter()).finish()
    }
}

impl Serialize for PooledIndexList {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // The pooled buffer serializes as the plain vector.
        (*self.guard).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PooledIndexList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let list = Vec::<OrderIdx>::deserialize(deserializer)?;
        // A detached buffer: the drain pool has capacity 0, so dropping the
        // list frees the buffer; `attach_pools` re-attaches it to the real
        // pool once the snapshot state is loaded into a book.
        let drain = Cache::new(0, Vec::new);
        Ok(Self { guard: drain.wrap(list), pool: drain })
    }
}
