//! The failover pool of settlement RPC nodes.
//!
//! The pool owns one JSON-RPC provider per configured node and always talks
//! to exactly one of them: the *active* node. Every call runs under the
//! node's `rpc_timeout` watchdog, and a node that stalls or fails to answer
//! transports rotates the active pointer to the next node — the pool never
//! fans a submission out to two nodes at once, which is what keeps a
//! duplicate nonce out of the mempool.
//!
//! The three configured timeouts bound three different failures:
//! `connect_timeout` bounds the TCP/TLS handshake (a node that is down),
//! `stall_timeout` is the transport-level ceiling on one request (a node
//! that accepts the connection and never answers), and `rpc_timeout` is the
//! watchdog the pool arms around the awaited call (a node that answers
//! slower than the trading loop can wait). A stall is a
//! [`ChainError::NodeStalled`], a failed answer is a
//! [`ChainError::Transport`]; both rotate.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use alloy::consensus::TxEnvelope;
use alloy::providers::{Provider, ProviderBuilder, RootProvider};
use alloy::transports::TransportError;
use alloy::transports::http::reqwest;
use primitives::base::Hash32;
use tokio::time::timeout;

use crate::chain::ChainError;
use crate::config::{RpcNodeConfig, RpcPoolConfig};

/// The boxed per-node call of [`RpcPool::with_active`].
///
/// The box is what breaks the borrow of the node out of the async block:
/// without it the closure's return type could not be named and the pool
/// could not rotate to the next node while the previous future is alive.
type NodeFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ChainError>> + Send + 'a>>;

/// One JSON-RPC node of the pool: the provider plus the timeouts that guard
/// it.
pub struct RpcNode {
    /// The node's configured name, for logs and error attribution.
    pub name: String,
    /// The HTTP JSON-RPC provider of this node.
    pub provider: RootProvider,
    /// The bound on the TCP/TLS handshake of one request.
    pub connect_timeout: Duration,
    /// The watchdog armed around one awaited RPC call.
    pub rpc_timeout: Duration,
    /// The transport-level ceiling on one request.
    pub stall_timeout: Duration,
}

impl RpcNode {
    /// Builds one node: the reqwest client (which carries the connect and
    /// stall timeouts) and the provider around it.
    fn build(cfg: &RpcNodeConfig) -> Result<Self, ChainError> {
        let url = reqwest::Url::parse(&cfg.url).map_err(|err| ChainError::Config {
            detail: format!("the url {:?} of RPC node {} is invalid: {err}", cfg.url, cfg.name),
        })?;
        let connect_timeout = Duration::from_millis(cfg.connect_timeout_ms);
        let rpc_timeout = Duration::from_millis(cfg.rpc_timeout_ms);
        let stall_timeout = Duration::from_millis(cfg.stall_timeout_ms);
        let client = reqwest::ClientBuilder::default()
            .connect_timeout(connect_timeout)
            .timeout(stall_timeout)
            .build()
            .map_err(|err| ChainError::Config {
                detail: format!("the http client of RPC node {} cannot be built: {err}", cfg.name),
            })?;
        Ok(Self {
            name: cfg.name.clone(),
            provider: ProviderBuilder::default().connect_reqwest(client, url),
            connect_timeout,
            rpc_timeout,
            stall_timeout,
        })
    }

    /// Maps an RPC failure onto the contract error: an unanswered call is a
    /// [`ChainError::Transport`] under this node's name, whatever the
    /// underlying cause.
    pub fn transport_error(&self, err: TransportError) -> ChainError {
        ChainError::Transport { node: self.name.clone(), detail: err.to_string() }
    }
}

/// The pool of settlement RPC nodes, indexed by a rotating active pointer.
pub struct RpcPool {
    nodes: Vec<RpcNode>,
    active: AtomicUsize,
}

impl RpcPool {
    /// Builds every configured node; the primary is the initial active one.
    ///
    /// A node whose url or http client is malformed is a
    /// [`ChainError::Config`]: the pool refuses to start half-configured and
    /// silently miss a failover target.
    pub fn build(cfg: &RpcPoolConfig) -> Result<Self, ChainError> {
        let mut nodes = Vec::with_capacity(1 + cfg.secondaries.len());
        nodes.push(RpcNode::build(&cfg.primary)?);
        for secondary in &cfg.secondaries {
            nodes.push(RpcNode::build(secondary)?);
        }
        Ok(Self { nodes, active: AtomicUsize::new(0) })
    }

    /// The current node count.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the pool has no node at all.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The name of the active node, for logs and error attribution.
    pub fn active_name(&self) -> &str {
        self.nodes.get(self.active.load(Ordering::Relaxed)).map_or("<none>", |node| &node.name)
    }

    /// Runs `call` against the active node, rotating on node-level failures.
    ///
    /// The call starts at the active node and walks the ring from there; the
    /// first node that answers becomes the new active one. Failures that are
    /// *not* the node's fault — an RPC answer, a nonce gap, a gas estimate —
    /// are returned as they are: the next node would answer identically.
    pub async fn with_active<T, F>(&self, call: F) -> Result<T, ChainError>
    where
        F: for<'a> Fn(&'a RpcNode) -> NodeFuture<'a, T>,
    {
        let start = self.active.load(Ordering::Relaxed);
        let mut last: Option<ChainError> = None;
        for offset in 0..self.nodes.len() {
            let index = (start + offset) % self.nodes.len();
            let node = &self.nodes[index];
            // The outer result is the watchdog, the inner one the call's own
            // answer — which may still be an RPC failure, i.e. an answer from
            // a healthy node.
            let outcome = match self.watch(node, call(node)).await {
                Ok(outcome) => outcome,
                Err(stall) => Err(stall),
            };
            match outcome {
                Ok(value) => {
                    self.active.store(index, Ordering::Relaxed);
                    return Ok(value);
                }
                Err(err) if rotates(&err) => last = Some(err),
                Err(err) => return Err(err),
            }
        }
        Err(last.unwrap_or_else(no_nodes))
    }

    /// The chain id of the active node.
    pub async fn chain_id(&self) -> Result<u64, ChainError> {
        self.with_active(|node| {
            Box::pin(async move {
                node.provider.get_chain_id().await.map_err(|err| node.transport_error(err))
            })
        })
        .await
    }

    /// Sends one signed envelope, returning the locally computed transaction
    /// hash.
    ///
    /// The envelope goes to the active node only; a node that stalls or
    /// fails to answer rotates the active pointer and the *same* bytes are
    /// offered to the next node. A node that answers "already known" is a
    /// success, not a failure: the bytes carry a fixed nonce, so the node
    /// that reports it has that exact transaction — the retry of a broadcast
    /// whose response was lost.
    pub async fn broadcast(&self, envelope: &TxEnvelope) -> Result<Hash32, ChainError> {
        let hash = Hash32(envelope.tx_hash().0);
        let start = self.active.load(Ordering::Relaxed);
        let mut last: Option<ChainError> = None;
        for offset in 0..self.nodes.len() {
            let index = (start + offset) % self.nodes.len();
            let node = &self.nodes[index];
            // The outer result is the watchdog, the inner one the RPC answer.
            match self.watch(node, node.provider.send_tx_envelope(envelope.clone())).await {
                Ok(Ok(_pending)) => {
                    self.active.store(index, Ordering::Relaxed);
                    return Ok(hash);
                }
                Ok(Err(err)) => {
                    if already_known(&err) {
                        self.active.store(index, Ordering::Relaxed);
                        return Ok(hash);
                    }
                    let err = node.transport_error(err);
                    if rotates(&err) {
                        last = Some(err);
                    } else {
                        return Err(err);
                    }
                }
                Err(stall) => last = Some(stall),
            }
        }
        Err(last.unwrap_or_else(no_nodes))
    }

    /// Arms this node's `rpc_timeout` watchdog around one awaited call.
    ///
    /// `Ok` carries the call's own result — which may still be an RPC
    /// failure, i.e. an *answer* from a healthy node. `Err` is the watchdog
    /// firing: the node never answered within the budget.
    async fn watch<T, Fut>(&self, node: &RpcNode, call: Fut) -> Result<T, ChainError>
    where
        Fut: Future<Output = T>,
    {
        match timeout(node.rpc_timeout, call).await {
            Ok(value) => Ok(value),
            Err(_elapsed) => Err(ChainError::NodeStalled {
                node: node.name.clone(),
                timeout_ms: node.rpc_timeout.as_millis() as u64,
            }),
        }
    }
}

/// Whether a failure is the node's, and so grounds for rotating to the next
/// one. Everything else is an answer the next node would repeat.
fn rotates(err: &ChainError) -> bool {
    matches!(err, ChainError::Transport { .. } | ChainError::NodeStalled { .. })
}

/// The error of a pool that has no node to offer at all. The configuration
/// always declares a primary, so this is unreachable in practice and only
/// keeps the failover loops total.
fn no_nodes() -> ChainError {
    ChainError::Config { detail: "the RPC pool has no node".to_string() }
}

/// Whether the node refused the envelope because it already holds exactly
/// these bytes.
fn already_known(err: &TransportError) -> bool {
    err.as_error_resp().is_some_and(|payload| is_already_known(&payload.message))
}

/// Whether an `eth_sendRawTransaction` complaint means "I have this exact
/// transaction".
///
/// Every Ethereum client words this differently ("already known", "known
/// transaction", "already imported", "already exists"), and the text is the
/// only channel — the call reports it as a plain JSON-RPC error. The check is
/// deliberately loose and made only while broadcasting a signed envelope
/// whose nonce is fixed by construction: the worst case of a false positive
/// is a submission the mempool has, which the poll then finds by hash.
fn is_already_known(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("already known")
        || message.contains("known transaction")
        || message.contains("already imported")
        || message.contains("already exists")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RpcNodeConfig, RpcPoolConfig};

    /// A node configuration with a local, never-contacted url.
    fn node(name: &str, url: &str) -> RpcNodeConfig {
        RpcNodeConfig {
            name: name.to_string(),
            url: url.to_string(),
            connect_timeout_ms: 1_000,
            rpc_timeout_ms: 2_000,
            stall_timeout_ms: 3_000,
        }
    }

    #[test]
    fn test_build_orders_the_primary_first() {
        let pool = RpcPool::build(&RpcPoolConfig {
            primary: node("primary", "http://127.0.0.1:8545"),
            secondaries: vec![
                node("secondary-a", "http://127.0.0.1:8546"),
                node("secondary-b", "http://127.0.0.1:8547"),
            ],
        })
        .expect("the pool builds");
        assert_eq!(pool.len(), 3);
        assert_eq!(pool.active_name(), "primary");
    }

    #[test]
    fn test_build_rejects_a_malformed_url() {
        let built = RpcPool::build(&RpcPoolConfig {
            primary: node("primary", "not a url"),
            secondaries: vec![],
        });
        match built {
            Err(err) => assert!(matches!(err, ChainError::Config { .. }), "{err:?}"),
            Ok(_) => panic!("a malformed url is a config error"),
        }
    }

    #[test]
    fn test_rotates_only_on_node_failures() {
        assert!(rotates(&ChainError::Transport { node: "a".into(), detail: "x".into() }));
        assert!(rotates(&ChainError::NodeStalled { node: "a".into(), timeout_ms: 1 }));
        // An answer is not a node failure: the next node would repeat it.
        assert!(!rotates(&ChainError::RevertDecode { detail: "x".into() }));
        assert!(!rotates(&ChainError::NonceGap { expected: 1, lowest_pending: 0 }));
        assert!(!rotates(&ChainError::GasEstimate { detail: "x".into() }));
    }

    #[test]
    fn test_already_known_recognizes_the_client_wordings() {
        assert!(is_already_known("already known"));
        assert!(is_already_known("Already Known"));
        assert!(is_already_known("known transaction: 0xdead"));
        assert!(is_already_known("transaction already imported"));
        assert!(is_already_known("Transaction already exists"));
        // The near misses must not read as success.
        assert!(!is_already_known("replacement transaction underpriced"));
        assert!(!is_already_known("insufficient funds for gas"));
        assert!(!is_already_known("nonce too low"));
        assert!(!is_already_known(""));
    }
}
