//! The chain interop contract of the settlement submitter: the
//! [`ChainClient`] trait, the mock test double and (behind the
//! `chain-alloy` feature) the real alloy implementation.
//!
//! The contract is deliberately synchronous: the submitter drives one call
//! at a time from its own thread, so the trait has no async surface and the
//! [`mock`] double needs neither tokio nor alloy. The alloy-free modules —
//! [`gas`], [`nonce`], [`mock`] and [`operator`] — carry everything that can
//! be tested offline; only [`abi`], [`alloy_impl`] and [`rpc_pool`] touch the
//! network.

pub mod gas;
pub mod mock;
pub mod nonce;
pub mod operator;

#[cfg(feature = "chain-alloy")]
pub mod abi;
#[cfg(feature = "chain-alloy")]
pub mod alloy_impl;
#[cfg(feature = "chain-alloy")]
pub mod rpc_pool;

use primitives::base::Hash32;

/// A transaction accepted by a node: its hash and the nonce it was signed
/// with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmittedTx {
    /// The transaction hash the node returned.
    pub tx_hash: Hash32,
    /// The operator nonce the transaction was sent with.
    pub nonce: u64,
}

/// The observed state of a submitted transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    /// Not mined yet (or not known to the polled node).
    Pending,
    /// Mined at `block`; `depth` is the confirmation depth observed at the
    /// time of the poll.
    Confirmed {
        /// The block the transaction mined in.
        block: u64,
        /// The confirmation depth at the time of the poll.
        depth: u64,
    },
    /// Mined and reverted. `(code, index, side)` is the decoded
    /// `SettlementFailure` of the settlement protocol; `(0, 0, 0)` is the
    /// sentinel for a revert whose data could not be decoded.
    Reverted {
        /// The revert code.
        code: u8,
        /// The failing trade index.
        index: usize,
        /// The failing cross side.
        side: u8,
    },
}

/// The failures of the chain interop.
///
/// The variants split by recovery: [`ChainError::is_retryable`] names the
/// ones the submitter may retry as-is, the rest need a state change first
/// (a resync, a fresh nonce, the operator key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainError {
    /// The node is unreachable or the call failed in transit.
    Transport {
        /// The node name.
        node: String,
        /// The transport failure detail.
        detail: String,
    },
    /// The node accepted the connection but stopped answering.
    NodeStalled {
        /// The node name.
        node: String,
        /// How long the node stayed silent, in milliseconds.
        timeout_ms: u64,
    },
    /// The chain's view of the operator nonce collides with the local one:
    /// a lower nonce is still pending elsewhere.
    NonceGap {
        /// The nonce the submission expected.
        expected: u64,
        /// The lowest nonce the chain still sees as pending.
        lowest_pending: u64,
    },
    /// The gas estimate failed.
    GasEstimate {
        /// The estimation failure detail.
        detail: String,
    },
    /// The revert data could not be decoded.
    RevertDecode {
        /// The decode failure detail.
        detail: String,
    },
    /// The transaction was replaced by another with the same nonce.
    Replaced {
        /// The replaced nonce.
        nonce: u64,
    },
    /// The operator keystore could not be unlocked.
    Keystore {
        /// The keystore failure detail.
        detail: String,
    },
    /// The calldata could not be encoded.
    Encode {
        /// The encode failure detail.
        detail: String,
    },
    /// The chain configuration is unusable.
    Config {
        /// The configuration failure detail.
        detail: String,
    },
}

impl ChainError {
    /// Whether the failure is worth retrying as-is: the transient transport,
    /// stall, nonce-gap, gas-estimate and revert-decode failures are; a
    /// replacement, a keystore, encode or config failure needs a state
    /// change first.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ChainError::Transport { .. }
                | ChainError::NodeStalled { .. }
                | ChainError::NonceGap { .. }
                | ChainError::GasEstimate { .. }
                | ChainError::RevertDecode { .. }
        )
    }
}

impl std::fmt::Display for ChainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChainError::Transport { node, detail } => {
                write!(f, "transport error on {node}: {detail}")
            }
            ChainError::NodeStalled { node, timeout_ms } => {
                write!(f, "node {node} stalled for {timeout_ms} ms")
            }
            ChainError::NonceGap { expected, lowest_pending } => write!(
                f,
                "nonce gap: expected {expected}, lowest pending on chain {lowest_pending}"
            ),
            ChainError::GasEstimate { detail } => write!(f, "gas estimation failed: {detail}"),
            ChainError::RevertDecode { detail } => write!(f, "revert decode failed: {detail}"),
            ChainError::Replaced { nonce } => {
                write!(f, "transaction with nonce {nonce} was replaced")
            }
            ChainError::Keystore { detail } => write!(f, "keystore: {detail}"),
            ChainError::Encode { detail } => write!(f, "calldata encode failed: {detail}"),
            ChainError::Config { detail } => write!(f, "chain config: {detail}"),
        }
    }
}

impl std::error::Error for ChainError {}

/// The chain the submitter talks to: submission, monitoring and
/// fee-bump replacement, all synchronous.
///
/// Implementations are shared across the submitter threads, hence
/// `Send + Sync`; the methods take `&self` so a shared pool needs no
/// external lock.
pub trait ChainClient: Send + Sync {
    /// Signs and broadcasts `calldata`, returning the transaction and the
    /// nonce it consumed.
    fn submit(&self, calldata: &[u8]) -> Result<SubmittedTx, ChainError>;

    /// Polls the state of a submitted transaction.
    fn tx_state(&self, tx: Hash32) -> Result<TxState, ChainError>;

    /// Re-sends `calldata` as a replacement of the transaction with
    /// `nonce`, with bumped fees.
    fn bump_fee(&self, calldata: &[u8], nonce: u64) -> Result<SubmittedTx, ChainError>;

    /// Whether the failure is worth retrying as-is; by default the
    /// classification of [`ChainError::is_retryable`].
    fn is_retryable(&self, err: &ChainError) -> bool {
        err.is_retryable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain::mock::MockChain;

    #[test]
    fn test_is_retryable_classification() {
        let retryable = [
            ChainError::Transport { node: "n".into(), detail: "d".into() },
            ChainError::NodeStalled { node: "n".into(), timeout_ms: 10 },
            ChainError::NonceGap { expected: 2, lowest_pending: 1 },
            ChainError::GasEstimate { detail: "d".into() },
            ChainError::RevertDecode { detail: "d".into() },
        ];
        for err in &retryable {
            assert!(err.is_retryable(), "{err} must be retryable");
        }
        let terminal = [
            ChainError::Replaced { nonce: 3 },
            ChainError::Keystore { detail: "d".into() },
            ChainError::Encode { detail: "d".into() },
            ChainError::Config { detail: "d".into() },
        ];
        for err in &terminal {
            assert!(!err.is_retryable(), "{err} must not be retryable");
        }
    }

    #[test]
    fn test_display_names_every_variant() {
        let errors = [
            ChainError::Transport { node: "n".into(), detail: "d".into() },
            ChainError::NodeStalled { node: "n".into(), timeout_ms: 10 },
            ChainError::NonceGap { expected: 2, lowest_pending: 1 },
            ChainError::GasEstimate { detail: "d".into() },
            ChainError::RevertDecode { detail: "d".into() },
            ChainError::Replaced { nonce: 3 },
            ChainError::Keystore { detail: "d".into() },
            ChainError::Encode { detail: "d".into() },
            ChainError::Config { detail: "d".into() },
        ];
        for err in &errors {
            assert!(!err.to_string().is_empty(), "the message must not be empty");
        }
    }

    #[test]
    fn test_the_trait_default_delegates_to_the_error() {
        let chain = MockChain::new();
        assert!(
            chain.is_retryable(&ChainError::Transport { node: "n".into(), detail: "d".into() })
        );
        assert!(!chain.is_retryable(&ChainError::Replaced { nonce: 1 }));
    }
}
