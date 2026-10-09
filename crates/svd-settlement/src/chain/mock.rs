//! The scripted [`ChainClient`] double of the settlement tests: the
//! offline, deterministic stand-in for a node, std-only (no alloy, no
//! tokio).
//!
//! Every method has its own FIFO of scripted answers; an empty FIFO falls
//! back to a fabricated success (a fresh transaction for `submit` /
//! `bump_fee`, [`TxState::Pending`] for `tx_state`), so a scenario scripts
//! only the interesting prefix. Fabricated transaction hashes are
//! deterministic — the low eight bytes hold a one-based counter — which
//! keeps assertions readable.
//!
//! The scenario builders pair a typical node behaviour with the assertions
//! the submitter tests need:
//!
//! - [`MockChain::scenario_settle_ok`] — the transaction mines at depth 1.
//! - [`MockChain::scenario_revert_with`] — it mines and reverts with the
//!   given decoded `(code, index, side)`.
//! - [`MockChain::scenario_pending_forever`] — it never mines; every poll
//!   reports [`TxState::Pending`] through the fallback.
//! - [`MockChain::scenario_rpc_error_then_ok`] — the first poll fails with
//!   a transport error, the second confirms.
//! - [`MockChain::scenario_submit_transport_error`] — the first submit
//!   fails with a transport error.
//! - [`MockChain::scenario_nonce_gap`] — the first submit fails with
//!   [`ChainError::NonceGap`].
//!
//! [`transport_err`] is the canned transport failure the builders and the
//! tests share.

use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard};

use primitives::base::Hash32;

use crate::chain::{ChainClient, ChainError, SubmittedTx, TxState};

/// The block the scenario builders report for a mined transaction.
const SCENARIO_BLOCK: u64 = 1_000;
/// The confirmation depth the scenario builders report: enough for the
/// default `confirmations = 1`.
const SCENARIO_DEPTH: u64 = 1;
/// The node name of the canned transport failure.
const MOCK_NODE: &str = "mock-node";

/// A canned transient transport failure, the same one the scenario builders
/// script.
pub fn transport_err() -> ChainError {
    ChainError::Transport {
        node: MOCK_NODE.to_string(),
        detail: "the mock node is unreachable".to_string(),
    }
}

/// One scripted answer of the mock chain. The variant picks the FIFO the
/// answer belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scripted {
    /// The answer of the next `submit` call.
    Submit(Result<SubmittedTx, ChainError>),
    /// The answer of the next `tx_state` call.
    State(Result<TxState, ChainError>),
    /// The answer of the next `bump_fee` call.
    Bump(Result<SubmittedTx, ChainError>),
}

/// One recorded call of the mock chain, in invocation order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Call {
    /// `submit(calldata)`.
    Submit {
        /// The submitted calldata.
        calldata: Vec<u8>,
    },
    /// `tx_state(tx)`.
    TxState {
        /// The polled transaction hash.
        tx: Hash32,
    },
    /// `bump_fee(calldata, nonce)`.
    BumpFee {
        /// The re-sent calldata.
        calldata: Vec<u8>,
        /// The replaced nonce.
        nonce: u64,
    },
}

/// The scripted chain double. Share it behind an `Arc`; the recorded state
/// is interior-mutable behind a mutex, so the `&self` calls of
/// [`ChainClient`] record and drain without an external lock.
///
/// (A `RefCell` would be lighter, but [`ChainClient`] requires `Send +
/// Sync` and `RefCell` is never `Sync`; the mutex is the std-only way to
/// keep the required interior mutability.)
#[derive(Debug)]
pub struct MockChain {
    /// The scripted FIFOs, the recorded calls and the fabrication counters.
    inner: Mutex<MockInner>,
}

/// The interior state of the mock.
#[derive(Debug)]
struct MockInner {
    /// Scripted `submit` answers, oldest first.
    submit_script: VecDeque<Result<SubmittedTx, ChainError>>,
    /// Scripted `tx_state` answers, oldest first.
    state_script: VecDeque<Result<TxState, ChainError>>,
    /// Scripted `bump_fee` answers, oldest first.
    bump_script: VecDeque<Result<SubmittedTx, ChainError>>,
    /// Every call, in invocation order.
    calls: Vec<Call>,
    /// The hashes of the transactions handed back by successful
    /// `submit` / `bump_fee` answers, in call order.
    handed_out: Vec<Hash32>,
    /// The counter of the next fabricated hash (one-based).
    next_hash: u64,
    /// The nonce of the next fabricated transaction.
    next_nonce: u64,
}

impl MockInner {
    /// The empty script with the fabrication counters at their start.
    fn new() -> Self {
        Self {
            submit_script: VecDeque::new(),
            state_script: VecDeque::new(),
            bump_script: VecDeque::new(),
            calls: Vec::new(),
            handed_out: Vec::new(),
            next_hash: 1,
            next_nonce: 0,
        }
    }

    /// Fabricates the next transaction: a fresh hash and the next nonce.
    fn auto_tx(&mut self) -> SubmittedTx {
        let hash = self.fabricate_hash();
        let nonce = self.next_nonce;
        self.next_nonce = self.next_nonce.saturating_add(1);
        SubmittedTx { tx_hash: hash, nonce }
    }

    /// Fabricates a hash for a transaction that keeps a caller-provided
    /// nonce (a replacement).
    fn fabricate_hash(&mut self) -> Hash32 {
        let hash = fabricated_hash(self.next_hash);
        self.next_hash = self.next_hash.saturating_add(1);
        hash
    }

    /// The number of scripted answers not consumed yet.
    fn scripted_left(&self) -> usize {
        self.submit_script.len() + self.state_script.len() + self.bump_script.len()
    }

    /// Pushes one scripted answer onto its FIFO.
    fn script(&mut self, entry: Scripted) {
        match entry {
            Scripted::Submit(answer) => self.submit_script.push_back(answer),
            Scripted::State(answer) => self.state_script.push_back(answer),
            Scripted::Bump(answer) => self.bump_script.push_back(answer),
        }
    }
}

impl MockChain {
    /// Creates an unscripted mock: every call succeeds through the
    /// fallbacks.
    pub fn new() -> Self {
        Self { inner: Mutex::new(MockInner::new()) }
    }

    /// Pushes one scripted answer onto the FIFO of its method.
    pub fn script(&self, entry: Scripted) {
        self.lock().script(entry);
    }

    /// Every recorded call, in invocation order.
    pub fn calls(&self) -> Vec<Call> {
        self.lock().calls.clone()
    }

    /// The calldata of every recorded `submit` call, in call order.
    pub fn submitted_calldata(&self) -> Vec<Vec<u8>> {
        self.lock()
            .calls
            .iter()
            .filter_map(|call| match call {
                Call::Submit { calldata } => Some(calldata.clone()),
                _ => None,
            })
            .collect()
    }

    /// The hashes of the transactions handed back by successful
    /// `submit` / `bump_fee` answers, in call order.
    pub fn submitted_hashes(&self) -> Vec<Hash32> {
        self.lock().handed_out.clone()
    }

    /// The nonces of every recorded `bump_fee` call, in call order.
    pub fn bump_nonces(&self) -> Vec<u64> {
        self.lock()
            .calls
            .iter()
            .filter_map(|call| match call {
                Call::BumpFee { nonce, .. } => Some(*nonce),
                _ => None,
            })
            .collect()
    }

    /// The number of scripted answers not consumed yet; a drained scenario
    /// returns zero.
    pub fn scripted_left(&self) -> usize {
        self.lock().scripted_left()
    }

    /// The happy path: the transaction is accepted, then observed
    /// confirmed at block [`SCENARIO_BLOCK`].
    pub fn scenario_settle_ok() -> Self {
        let mock = Self::new();
        let tx = mock.auto_tx();
        mock.script(Scripted::Submit(Ok(tx)));
        mock.script(Scripted::State(Ok(TxState::Confirmed {
            block: SCENARIO_BLOCK,
            depth: SCENARIO_DEPTH,
        })));
        mock
    }

    /// The accepted transaction mines and reverts with the given decoded
    /// `(code, index, side)`.
    pub fn scenario_revert_with(code: u8, index: usize, side: u8) -> Self {
        let mock = Self::new();
        let tx = mock.auto_tx();
        mock.script(Scripted::Submit(Ok(tx)));
        mock.script(Scripted::State(Ok(TxState::Reverted { code, index, side })));
        mock
    }

    /// The accepted transaction never mines: every poll falls back to
    /// [`TxState::Pending`].
    pub fn scenario_pending_forever() -> Self {
        let mock = Self::new();
        let tx = mock.auto_tx();
        mock.script(Scripted::Submit(Ok(tx)));
        mock
    }

    /// The accepted transaction is polled once while the node is
    /// unreachable, then observed confirmed.
    pub fn scenario_rpc_error_then_ok() -> Self {
        let mock = Self::new();
        let tx = mock.auto_tx();
        mock.script(Scripted::Submit(Ok(tx)));
        mock.script(Scripted::State(Err(transport_err())));
        mock.script(Scripted::State(Ok(TxState::Confirmed {
            block: SCENARIO_BLOCK,
            depth: SCENARIO_DEPTH,
        })));
        mock
    }

    /// The first submit fails with a transport error; the retry is
    /// unscripted and falls back to a fabricated transaction.
    pub fn scenario_submit_transport_error() -> Self {
        let mock = Self::new();
        mock.script(Scripted::Submit(Err(transport_err())));
        mock
    }

    /// The first submit fails with a [`ChainError::NonceGap`] (the values
    /// are illustrative: the mock has no view of the real cursor); the
    /// retry after the operator resync is unscripted.
    pub fn scenario_nonce_gap() -> Self {
        let mock = Self::new();
        mock.script(Scripted::Submit(Err(ChainError::NonceGap { expected: 1, lowest_pending: 0 })));
        mock
    }

    /// Locks the interior state, poisoning-tolerant.
    fn lock(&self) -> MutexGuard<'_, MockInner> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Fabricates the next transaction through the interior state.
    fn auto_tx(&self) -> SubmittedTx {
        self.lock().auto_tx()
    }
}

impl Default for MockChain {
    fn default() -> Self {
        Self::new()
    }
}

/// The fabricated hash of the `counter`-th generated transaction: all zero
/// but the low eight bytes, which hold the counter big-endian. The counter
/// starts at one, so no fabricated hash is the all-zero [`Hash32::default`]
/// "unset" value.
fn fabricated_hash(counter: u64) -> Hash32 {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&counter.to_be_bytes());
    Hash32(bytes)
}

/// Fabricates the `counter`-th transaction of a scenario script: the hash
/// of [`fabricated_hash`] with the nonce `counter - 1`. A deterministic
/// [`SubmittedTx`] for multi-step scripts (the unscripted fallbacks consume
/// the mock's own counter, so the scripts must not collide with it).
pub fn tx_of(counter: u64) -> SubmittedTx {
    SubmittedTx { tx_hash: fabricated_hash(counter), nonce: counter.saturating_sub(1) }
}

impl ChainClient for MockChain {
    fn submit(&self, calldata: &[u8]) -> Result<SubmittedTx, ChainError> {
        let mut inner = self.lock();
        inner.calls.push(Call::Submit { calldata: calldata.to_vec() });
        let answer = match inner.submit_script.pop_front() {
            Some(answer) => answer,
            None => Ok(inner.auto_tx()),
        };
        if let Ok(tx) = &answer {
            inner.handed_out.push(tx.tx_hash);
        }
        answer
    }

    fn tx_state(&self, tx: Hash32) -> Result<TxState, ChainError> {
        let mut inner = self.lock();
        inner.calls.push(Call::TxState { tx });
        match inner.state_script.pop_front() {
            Some(answer) => answer,
            // A mined-but-unscripted transaction has no state to report:
            // pending is the safe default for the mock.
            None => Ok(TxState::Pending),
        }
    }

    fn bump_fee(&self, calldata: &[u8], nonce: u64) -> Result<SubmittedTx, ChainError> {
        let mut inner = self.lock();
        inner.calls.push(Call::BumpFee { calldata: calldata.to_vec(), nonce });
        let answer = match inner.bump_script.pop_front() {
            Some(answer) => answer,
            // A replacement keeps the nonce it replaces.
            None => {
                let tx_hash = inner.fabricate_hash();
                Ok(SubmittedTx { tx_hash, nonce })
            }
        };
        if let Ok(tx) = &answer {
            inner.handed_out.push(tx.tx_hash);
        }
        answer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scenario_settle_ok() {
        let chain = MockChain::scenario_settle_ok();
        let tx = chain.submit(b"batch-1").unwrap();
        assert_eq!(tx, SubmittedTx { tx_hash: fabricated_hash(1), nonce: 0 });
        assert_eq!(
            chain.tx_state(tx.tx_hash).unwrap(),
            TxState::Confirmed { block: SCENARIO_BLOCK, depth: SCENARIO_DEPTH }
        );
        assert_eq!(chain.scripted_left(), 0);
        assert_eq!(
            chain.calls(),
            vec![Call::Submit { calldata: b"batch-1".to_vec() }, Call::TxState { tx: tx.tx_hash },]
        );
        assert_eq!(chain.submitted_calldata(), vec![b"batch-1".to_vec()]);
        assert_eq!(chain.submitted_hashes(), vec![tx.tx_hash]);
        assert_eq!(chain.bump_nonces(), Vec::<u64>::new());
    }

    #[test]
    fn test_scenario_revert_with() {
        let chain = MockChain::scenario_revert_with(2, 7, 1);
        let tx = chain.submit(b"batch-2").unwrap();
        assert_eq!(
            chain.tx_state(tx.tx_hash).unwrap(),
            TxState::Reverted { code: 2, index: 7, side: 1 }
        );
        assert_eq!(chain.scripted_left(), 0);
    }

    #[test]
    fn test_scenario_pending_forever() {
        let chain = MockChain::scenario_pending_forever();
        let tx = chain.submit(b"batch-3").unwrap();
        assert_eq!(chain.scripted_left(), 0);
        // The fallback keeps answering pending and never drains.
        assert_eq!(chain.tx_state(tx.tx_hash).unwrap(), TxState::Pending);
        assert_eq!(chain.tx_state(tx.tx_hash).unwrap(), TxState::Pending);
    }

    #[test]
    fn test_scenario_rpc_error_then_ok() {
        let chain = MockChain::scenario_rpc_error_then_ok();
        let tx = chain.submit(b"batch-4").unwrap();
        let err = chain.tx_state(tx.tx_hash).unwrap_err();
        assert!(err.is_retryable(), "{err} must be retryable");
        assert_eq!(
            chain.tx_state(tx.tx_hash).unwrap(),
            TxState::Confirmed { block: SCENARIO_BLOCK, depth: SCENARIO_DEPTH }
        );
        assert_eq!(chain.scripted_left(), 0);
    }

    #[test]
    fn test_scenario_submit_transport_error() {
        let chain = MockChain::scenario_submit_transport_error();
        let err = chain.submit(b"batch-5").unwrap_err();
        assert_eq!(err, transport_err());
        assert_eq!(chain.scripted_left(), 0);
        // The retry is unscripted: the fallback fabricates a transaction.
        let tx = chain.submit(b"batch-5").unwrap();
        assert_eq!(tx, SubmittedTx { tx_hash: fabricated_hash(1), nonce: 0 });
        assert_eq!(chain.submitted_calldata(), vec![b"batch-5".to_vec(), b"batch-5".to_vec()]);
    }

    #[test]
    fn test_scenario_nonce_gap() {
        let chain = MockChain::scenario_nonce_gap();
        let err = chain.submit(b"batch-6").unwrap_err();
        assert_eq!(err, ChainError::NonceGap { expected: 1, lowest_pending: 0 });
        assert!(err.is_retryable());
        assert_eq!(chain.scripted_left(), 0);
        let tx = chain.submit(b"batch-6").unwrap();
        assert_eq!(tx.nonce, 0);
        assert_eq!(chain.submitted_hashes(), vec![tx.tx_hash]);
    }

    #[test]
    fn test_bump_keeps_the_nonce_and_is_recorded() {
        let chain = MockChain::new();
        let tx = chain.bump_fee(b"payload", 7).unwrap();
        assert_eq!(tx.nonce, 7);
        assert_eq!(chain.bump_nonces(), vec![7]);
        assert_eq!(chain.submitted_hashes(), vec![tx.tx_hash]);
        // A bump is not a submit: its calldata is only in `calls`.
        assert_eq!(chain.submitted_calldata(), Vec::<Vec<u8>>::new());
        assert_eq!(chain.calls(), vec![Call::BumpFee { calldata: b"payload".to_vec(), nonce: 7 }]);
    }

    #[test]
    fn test_scripts_drain_in_fifo_order_then_fall_back() {
        let chain = MockChain::new();
        chain.script(Scripted::State(Ok(TxState::Confirmed { block: 3, depth: 4 })));
        chain.script(Scripted::State(Ok(TxState::Pending)));
        assert_eq!(chain.scripted_left(), 2);
        assert_eq!(
            chain.tx_state(Hash32([1; 32])).unwrap(),
            TxState::Confirmed { block: 3, depth: 4 }
        );
        assert_eq!(chain.tx_state(Hash32([1; 32])).unwrap(), TxState::Pending);
        assert_eq!(chain.scripted_left(), 0);
        assert_eq!(chain.tx_state(Hash32([1; 32])).unwrap(), TxState::Pending);
    }

    #[test]
    fn test_fabricated_hashes_are_unique_and_ordered() {
        assert_eq!(fabricated_hash(1).0[24..], 1u64.to_be_bytes());
        assert_ne!(fabricated_hash(1), fabricated_hash(2));
        assert_ne!(fabricated_hash(1), Hash32::default());
        let chain = MockChain::new();
        let first = chain.submit(b"a").unwrap();
        let second = chain.submit(b"b").unwrap();
        assert_ne!(first.tx_hash, second.tx_hash);
        assert_eq!(second.nonce, 1);
    }

    #[test]
    fn test_default_is_the_empty_mock() {
        let chain = MockChain::default();
        assert_eq!(chain.scripted_left(), 0);
        assert_eq!(chain.calls(), Vec::<Call>::new());
    }
}
