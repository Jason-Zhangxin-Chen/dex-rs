//! The shared margin cache: the latest margin state of the accounts that
//! trade this market, read by the handler threads on every order and
//! written by the feed threads.
//!
//! The cache is a sharded concurrent map whose entries carry the margin
//! state, an atomic block flag (set by the settlement feed on an
//! insufficient margin, cleared by a fresh margin update) and an atomic
//! last-used timestamp (touched by the handler reads, swept by the margin
//! feed). The reads are cheap shard-local reads; the writes are rare and
//! serialized per entry.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use primitives::address::Address;
use rustc_hash::FxHashMap;

/// The latest margin state of one account (the pulled or fed value).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarginState {
    /// The equity of the account, quote asset units.
    pub equity: u128,
    /// The margin used by the account's open positions.
    pub used: u128,
    /// The available margin, equity minus used.
    pub available: u128,
    /// The block number the state comes from.
    pub block: u64,
}

impl MarginState {
    /// The zero state of an account without on-chain margin state yet.
    pub const ZERO: Self = Self { equity: 0, used: 0, available: 0, block: 0 };
}

/// The view of an account a handler reads: the state plus the block flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarginView {
    /// The margin state.
    pub state: MarginState,
    /// Whether the account is blocked (a settlement failed with an
    /// insufficient margin; cleared by the next fresh margin update).
    pub blocked: bool,
}

/// One cache entry.
#[derive(Debug)]
struct MarginEntry {
    /// The margin state, mutated by the feed updates and the attaches.
    state: MarginState,
    /// The block flag, set by the settlement feed, cleared by an update.
    blocked: bool,
    /// When the account was last touched by a handler read, for the idle
    /// eviction. A relaxed atomic: the readers touch it under a shard read
    /// lock, the sweep reads it under the write lock.
    last_used_ms: AtomicU64,
}

impl MarginEntry {
    /// Creates an entry around a state.
    fn new(state: MarginState, now_ms: u64) -> Self {
        Self { state, blocked: false, last_used_ms: AtomicU64::new(now_ms) }
    }
}

/// The shared sharded margin cache.
#[derive(Debug)]
pub struct MarginCache {
    /// The shards; an account maps to one shard by its last address byte.
    shards: Vec<RwLock<FxHashMap<Address, MarginEntry>>>,
}

impl MarginCache {
    /// Creates the cache with the shards pre-sized to `capacity_hint`.
    pub fn new(shards: usize, capacity_hint: usize) -> Self {
        let per_shard = capacity_hint.div_ceil(shards.max(1));
        let shards = (0..shards.max(1))
            .map(|_| {
                RwLock::new(FxHashMap::with_capacity_and_hasher(per_shard, Default::default()))
            })
            .collect();
        Self { shards }
    }

    /// The shard of an account.
    fn shard(&self, account: Address) -> &RwLock<FxHashMap<Address, MarginEntry>> {
        &self.shards[usize::from(account.0[19]) % self.shards.len()]
    }

    /// Reads the view of an account and touches its last-used timestamp.
    pub fn get(&self, account: Address, now_ms: u64) -> Option<MarginView> {
        let shard = read(self.shard(account));
        let entry = shard.get(&account)?;
        entry.last_used_ms.store(now_ms, Ordering::Relaxed);
        Some(MarginView { state: entry.state, blocked: entry.blocked })
    }

    /// Applies a fed margin update: overwrites the state of the account and
    /// clears its block flag (the fresh state decides the admission on its
    /// own from then on); attaches the account when it is not tracked yet.
    pub fn upsert(&self, account: Address, state: MarginState, now_ms: u64) {
        let mut shard = write(self.shard(account));
        match shard.get_mut(&account) {
            Some(entry) => {
                entry.state = state;
                entry.blocked = false;
            }
            None => {
                shard.insert(account, MarginEntry::new(state, now_ms));
            }
        }
    }

    /// Attaches an account with the pulled state when it is not tracked yet
    /// (a racing feed update wins), and returns the view of the current
    /// entry — the caller checks the returned state.
    pub fn attach_if_absent(
        &self,
        account: Address,
        state: MarginState,
        now_ms: u64,
    ) -> MarginView {
        let mut shard = write(self.shard(account));
        let entry = shard.entry(account).or_insert_with(|| MarginEntry::new(state, now_ms));
        entry.last_used_ms.store(now_ms, Ordering::Relaxed);
        MarginView { state: entry.state, blocked: entry.blocked }
    }

    /// Blocks an account that failed a settlement with an insufficient
    /// margin; a no-op for an untracked account (its first pull reads the
    /// fresh state anyway).
    pub fn set_blocked(&self, account: Address) {
        let mut shard = write(self.shard(account));
        if let Some(entry) = shard.get_mut(&account) {
            entry.blocked = true;
        }
    }

    /// Sweeps the entries idle beyond `idle_ms`, then evicts the oldest
    /// entries until the cache holds at most `max_accounts`.
    pub fn sweep(&self, now_ms: u64, idle_ms: u64, max_accounts: usize) {
        let mut removed = 0usize;
        for shard in &self.shards {
            let mut shard = write(shard);
            shard.retain(|_, entry| {
                let idle =
                    now_ms.saturating_sub(entry.last_used_ms.load(Ordering::Relaxed)) > idle_ms;
                if idle {
                    removed += 1;
                }
                !idle
            });
        }
        // The capacity bound: evict the oldest entries while over it.
        while self.len() > max_accounts {
            let mut oldest: Option<(Address, u64, usize)> = None;
            for (index, shard) in self.shards.iter().enumerate() {
                let shard = read(shard);
                for (account, entry) in shard.iter() {
                    let used = entry.last_used_ms.load(Ordering::Relaxed);
                    if oldest.is_none_or(|(_, when, _)| used < when) {
                        oldest = Some((*account, used, index));
                    }
                }
            }
            let Some((account, _, index)) = oldest else { break };
            write(&self.shards[index]).remove(&account);
            removed += 1;
        }
        if removed > 0 {
            tracing::debug!(removed, remaining = self.len(), "the margin cache sweep");
        }
    }

    /// The number of the tracked accounts.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| read(shard).len()).sum()
    }

    /// Whether the cache tracks no account.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Poison-tolerant read lock helper.
fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Poison-tolerant write lock helper.
fn write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(b: u8) -> Address {
        Address([b; 20])
    }

    fn state(available: u128) -> MarginState {
        MarginState { equity: available + 10, used: 10, available, block: 1 }
    }

    #[test]
    fn test_upsert_and_get() {
        let cache = MarginCache::new(4, 64);
        cache.upsert(addr(1), state(100), 0);
        let view = cache.get(addr(1), 1).unwrap();
        assert_eq!(view.state.available, 100);
        assert!(!view.blocked);
        assert_eq!(cache.len(), 1);
        assert!(cache.get(addr(2), 1).is_none());
    }

    #[test]
    fn test_upsert_clears_the_block_flag() {
        let cache = MarginCache::new(4, 64);
        cache.upsert(addr(1), state(100), 0);
        cache.set_blocked(addr(1));
        assert!(cache.get(addr(1), 1).unwrap().blocked);
        cache.upsert(addr(1), state(200), 1);
        assert!(!cache.get(addr(1), 2).unwrap().blocked);
    }

    #[test]
    fn test_set_blocked_ignores_untracked_accounts() {
        let cache = MarginCache::new(4, 64);
        cache.set_blocked(addr(1));
        assert!(cache.is_empty());
    }

    #[test]
    fn test_attach_if_absent_inserts_once() {
        let cache = MarginCache::new(4, 64);
        let view = cache.attach_if_absent(addr(1), state(100), 0);
        assert_eq!(view.state.available, 100);
        // A racing feed update landed between the pull and the attach: the
        // fresher state wins and the attach does not overwrite it.
        cache.upsert(addr(1), state(300), 1);
        let view = cache.attach_if_absent(addr(1), state(100), 2);
        assert_eq!(view.state.available, 300);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_sweep_evicts_idle_entries() {
        let cache = MarginCache::new(4, 64);
        cache.upsert(addr(1), state(100), 0);
        cache.upsert(addr(2), state(100), 0);
        // The first account was touched recently, the second one is stale.
        cache.get(addr(1), 95);
        cache.sweep(100, 10, 64);
        assert_eq!(cache.len(), 1);
        assert!(cache.get(addr(1), 100).is_some());
    }

    #[test]
    fn test_sweep_enforces_the_capacity_bound() {
        let cache = MarginCache::new(2, 4);
        for account in 0..8u8 {
            cache.upsert(addr(account), state(100), u64::from(account));
        }
        assert_eq!(cache.len(), 8);
        cache.sweep(1_000, 10_000, 4);
        assert_eq!(cache.len(), 4);
    }
}
