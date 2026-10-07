//! The operator nonce bookkeeping: the pure state machine the submitter
//! drives around the chain's transaction count.
//!
//! The rule that keeps the account usable is the *gap rule*: a nonce is
//! never assigned while a lower nonce is still in flight. A stuck
//! transaction is therefore not skipped — it is replaced at the same nonce
//! with bumped fees ([`crate::chain::ChainClient::bump_fee`]) until it lands
//! or the batch is abandoned. [`NonceState::resync`] is the repair path: it
//! folds the chain's transaction count back into the bookkeeping after a
//! failover or a restart.
//!
//! Only the pure bookkeeping lives here; the asynchronous RPC side (reading
//! the count, the pending set) belongs to the caller.

use std::collections::{BTreeMap, BTreeSet};

use primitives::base::Hash32;

/// The nonce cursor of one operator key.
///
/// `confirmed_through` is an exclusive boundary: every nonce below it is
/// confirmed, and the prefix below it is contiguous. Confirmations that
/// arrive above a still-pending nonce are parked and folded in
/// ([`NonceState::on_confirmed`]) once the hole closes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonceState {
    /// The next nonce to hand out; never below the chain's transaction
    /// count.
    next_unused: u64,
    /// The assigned, unconfirmed nonces: `None` between the assignment and
    /// the submission, `Some(hash)` once a node accepted the transaction
    /// (the hash changes on a replacement).
    in_flight: BTreeMap<u64, Option<Hash32>>,
    /// The exclusive upper bound of the contiguously confirmed prefix.
    confirmed_through: u64,
    /// Confirmed nonces at or above `confirmed_through` that cannot join
    /// the prefix yet because a lower nonce is still in flight.
    confirmed_ahead: BTreeSet<u64>,
}

/// The outcome of [`NonceState::assign`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonceAction {
    /// The nonce was reserved; submit the transaction with it.
    Assign(u64),
    /// The gap rule forbids reserving a new nonce: a lower one is still in
    /// flight and must be confirmed or replaced first.
    Blocked {
        /// The nonce that would have been assigned.
        expected: u64,
        /// The lowest nonce still in flight.
        lowest_pending: u64,
    },
}

impl NonceState {
    /// Creates the state from the chain's transaction count (as read with
    /// "latest": the count of mined transactions from the operator
    /// address). Nonces below the count are treated as confirmed.
    pub fn new(chain_count: u64) -> Self {
        Self {
            next_unused: chain_count,
            in_flight: BTreeMap::new(),
            confirmed_through: chain_count,
            confirmed_ahead: BTreeSet::new(),
        }
    }

    /// Reserves the next nonce, unless a lower one is still in flight (the
    /// gap rule). A blocked assignment changes nothing.
    ///
    /// The cursor is first lifted to the confirmed prefix: a run that
    /// recovered and watched confirmations of nonces it never assigned
    /// locally must not hand them out again.
    pub fn assign(&mut self) -> NonceAction {
        self.next_unused = self.next_unused.max(self.confirmed_through);
        let expected = self.next_unused;
        if let Some(lowest_pending) = self.lowest_pending()
            && lowest_pending < expected
        {
            return NonceAction::Blocked { expected, lowest_pending };
        }
        self.next_unused = expected.saturating_add(1);
        self.in_flight.insert(expected, None);
        NonceAction::Assign(expected)
    }

    /// Records that `nonce` was accepted by a node, with `hash` as the
    /// transaction to monitor.
    ///
    /// A submission also never hands the nonce out again: an unknown nonce
    /// (accepted before the assignment was recorded, or submitted by a
    /// recovered run) lifts `next_unused` past it, and a nonce below the
    /// confirmed prefix is already mined and is not tracked.
    pub fn on_submitted(&mut self, nonce: u64, hash: Hash32) {
        if nonce < self.confirmed_through {
            return;
        }
        self.next_unused = self.next_unused.max(nonce.saturating_add(1));
        self.in_flight.insert(nonce, Some(hash));
    }

    /// Records that `nonce` was mined, and folds the confirmation into the
    /// contiguous prefix. Returns whether the prefix advanced.
    ///
    /// A confirmation above a still-in-flight nonce advances nothing yet;
    /// it is parked and folded in by the confirmation that closes the hole.
    /// Re-reporting a nonce below the prefix returns `false`.
    pub fn on_confirmed(&mut self, nonce: u64) -> bool {
        self.in_flight.remove(&nonce);
        if nonce < self.confirmed_through {
            return false;
        }
        self.confirmed_ahead.insert(nonce);
        let before = self.confirmed_through;
        while self.confirmed_ahead.remove(&self.confirmed_through) {
            self.confirmed_through = self.confirmed_through.saturating_add(1);
        }
        self.confirmed_through != before
    }

    /// Records that the transaction of `nonce` was replaced: the tracked
    /// hash becomes `new_hash`. A nonce that is not in flight is ignored —
    /// replays after a resync must not resurrect it.
    pub fn on_replaced(&mut self, nonce: u64, new_hash: Hash32) {
        if let Some(hash) = self.in_flight.get_mut(&nonce) {
            *hash = Some(new_hash);
        }
    }

    /// The lowest nonce still in flight, if any. This is the nonce the
    /// submitter monitors or replaces.
    pub fn lowest_pending(&self) -> Option<u64> {
        self.in_flight.keys().next().copied()
    }

    /// Folds a fresh chain transaction count back into the state, returning
    /// the *vanished* nonces: the in-flight entries below the count that
    /// the count confirms without a tracked [`NonceState::on_confirmed`],
    /// in ascending order. The caller reconciles them (their transactions
    /// mined, so their batches reached a terminal state).
    ///
    /// The count is a floor, never a reset: `next_unused` and
    /// `confirmed_through` only move up, so a lagging or reorged node can
    /// never cause a nonce to be handed out twice. In-flight entries at or
    /// above the count are kept; parked confirmations below it are subsumed
    /// by the lifted prefix.
    pub fn resync(&mut self, new_chain_count: u64) -> Vec<u64> {
        let vanished: Vec<u64> =
            self.in_flight.range(..new_chain_count).map(|(&nonce, _)| nonce).collect();
        for nonce in &vanished {
            self.in_flight.remove(nonce);
        }
        self.confirmed_ahead.retain(|&nonce| nonce >= new_chain_count);
        self.confirmed_through = self.confirmed_through.max(new_chain_count);
        self.next_unused = self.next_unused.max(new_chain_count);
        vanished
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(byte: u8) -> Hash32 {
        Hash32([byte; 32])
    }

    #[test]
    fn test_new_starts_at_the_chain_count() {
        let mut state = NonceState::new(7);
        assert_eq!(state.lowest_pending(), None);
        assert_eq!(state.assign(), NonceAction::Assign(7));
        assert_eq!(state.lowest_pending(), Some(7));
    }

    #[test]
    fn test_assign_blocks_while_a_nonce_is_in_flight() {
        let mut state = NonceState::new(0);
        assert_eq!(state.assign(), NonceAction::Assign(0));
        // The assignment alone already blocks; so does a submission.
        assert_eq!(state.assign(), NonceAction::Blocked { expected: 1, lowest_pending: 0 });
        state.on_submitted(0, hash(0xaa));
        assert_eq!(state.assign(), NonceAction::Blocked { expected: 1, lowest_pending: 0 });
    }

    #[test]
    fn test_confirm_unblocks_the_next_assignment() {
        let mut state = NonceState::new(0);
        assert_eq!(state.assign(), NonceAction::Assign(0));
        state.on_submitted(0, hash(1));
        assert!(state.on_confirmed(0));
        assert_eq!(state.lowest_pending(), None);
        assert_eq!(state.assign(), NonceAction::Assign(1));
    }

    #[test]
    fn test_confirm_out_of_order_holds_the_prefix() {
        let mut state = NonceState::new(3);
        // Above the prefix while the prefix itself is not confirmed: parked.
        assert!(!state.on_confirmed(5));
        // The contiguous run 3, 4 closes the hole and drags 5 along.
        assert!(state.on_confirmed(3));
        assert!(state.on_confirmed(4));
        assert_eq!(state.assign(), NonceAction::Assign(6));
    }

    #[test]
    fn test_confirm_below_the_prefix_is_stale() {
        let mut state = NonceState::new(4);
        assert!(!state.on_confirmed(2));
        assert!(!state.on_confirmed(0));
        assert_eq!(state.assign(), NonceAction::Assign(4));
    }

    #[test]
    fn test_submitted_nonce_is_never_handed_out_again() {
        let mut state = NonceState::new(0);
        // A recovered or externally submitted nonce: the cursor lifts past
        // it and the gap rule blocks the next assignment.
        state.on_submitted(4, hash(4));
        assert_eq!(state.assign(), NonceAction::Blocked { expected: 5, lowest_pending: 4 });
        // The confirmation clears the in-flight entry, so the assignment
        // unblocks; the prefix itself stays put (0..4 were never confirmed).
        assert!(!state.on_confirmed(4));
        assert_eq!(state.lowest_pending(), None);
        assert_eq!(state.assign(), NonceAction::Assign(5));
    }

    #[test]
    fn test_submission_below_the_prefix_is_ignored() {
        let mut state = NonceState::new(3);
        state.on_submitted(2, hash(2));
        assert_eq!(state.lowest_pending(), None);
        assert_eq!(state.assign(), NonceAction::Assign(3));
    }

    #[test]
    fn test_replacement_updates_the_tracked_hash() {
        let mut state = NonceState::new(0);
        assert_eq!(state.assign(), NonceAction::Assign(0));
        state.on_submitted(0, hash(1));
        state.on_replaced(0, hash(2));
        assert_eq!(state.in_flight.get(&0), Some(&Some(hash(2))));
        // An unknown nonce is not resurrected.
        state.on_replaced(9, hash(9));
        assert_eq!(state.in_flight.len(), 1);
        assert_eq!(state.lowest_pending(), Some(0));
    }

    #[test]
    fn test_resync_drops_mined_in_flight_and_lifts_the_cursor() {
        let mut state = NonceState::new(0);
        assert_eq!(state.assign(), NonceAction::Assign(0));
        state.on_submitted(0, hash(1));
        assert!(state.on_confirmed(0));
        assert_eq!(state.assign(), NonceAction::Assign(1));
        state.on_submitted(1, hash(2));
        // The chain reports both mined: the second never got a tracked
        // confirmation, so it vanishes and the cursor is already at 2.
        assert_eq!(state.resync(2), vec![1]);
        assert_eq!(state.lowest_pending(), None);
        assert_eq!(state.assign(), NonceAction::Assign(2));
    }

    #[test]
    fn test_resync_returns_every_vanished_nonce_in_order() {
        // White box: a pipelined window as a recovered submitter would
        // rebuild it from the journal.
        let mut state = NonceState::new(0);
        for nonce in [0u64, 1, 2] {
            state.in_flight.insert(nonce, None);
        }
        state.next_unused = 3;
        assert_eq!(state.resync(5), vec![0, 1, 2]);
        assert_eq!(state.lowest_pending(), None);
        assert_eq!(state.assign(), NonceAction::Assign(5));
    }

    #[test]
    fn test_resync_keeps_unmined_entries_and_never_rewinds() {
        let mut state = NonceState::new(2);
        assert_eq!(state.assign(), NonceAction::Assign(2));
        state.on_submitted(2, hash(3));
        // A lagging node's lower count cannot un-confirm or un-assign.
        assert_eq!(state.resync(1), Vec::<u64>::new());
        assert_eq!(state.lowest_pending(), Some(2));
        assert_eq!(state.assign(), NonceAction::Blocked { expected: 3, lowest_pending: 2 });
        assert!(state.on_confirmed(2));
        assert_eq!(state.assign(), NonceAction::Assign(3));
    }

    #[test]
    fn test_resync_subsumes_parked_confirmations() {
        let mut state = NonceState::new(3);
        assert!(!state.on_confirmed(5));
        assert_eq!(state.resync(6), Vec::<u64>::new());
        // The prefix is now 6, and the parked 5 was folded in with it.
        assert_eq!(state.assign(), NonceAction::Assign(6));
        assert!(!state.on_confirmed(5));
    }
}
