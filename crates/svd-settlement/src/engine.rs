//! The settlement runtime: the shared state of the process (config,
//! shutdown), the batch-assembling core thread and the side threads (the
//! submitter pool, the result publisher and the SQL writer).
//!
//! The core thread groups the drained trades by taker order and pushes one
//! frame per group into the submitter queues; each submitter drives its own
//! queue against the chain with its own operator key. No journal: the
//! durability lives in the file-mapped queues and the acks (see
//! [`crate::core`] and [`crate::submitter`]).

use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::mpsc;

use ipc::mmap_spsc_fixed::SpscQueue;
use ipc::mmap_spsc_var::ByteSpscQueue;
use primitives::message::hot_path::Trade;
use primitives::message::settlement::SettlementResult;
use storage::RedisStore;
use tracing::info;

use crate::chain::ChainClient;
use crate::config::{
    ConfigError, SettlementConfig, SharedChainConfig, SubmitterConfig as ConfigSubmitter,
};
use crate::core::{frame_budget, spin};
use crate::naming::{settlement_channel, snapshot_key};
use crate::publisher::spawn_publisher;
use crate::seq::SeqFile;
use crate::sql::{MySqlSettlement, SettlementSql, SqlError, spawn_sql_writer};
use crate::submitter::{ChainBuilder, SubmitChannels, SubmitterConfig, spawn as spawn_submitter};

/// Errors of the settlement engine.
#[derive(Debug)]
pub enum EngineError {
    /// A file or queue operation failed.
    Io(std::io::Error),
    /// The config could not be loaded.
    Config(ConfigError),
    /// A storage (Redis) operation failed.
    Storage(storage::StorageError),
    /// The SQL cluster failed.
    Sql(SqlError),
    /// The chain interop failed.
    Chain(crate::chain::ChainError),
    /// The config fails a startup invariant.
    BadConfig(String),
    /// A thread could not be built.
    Runtime(String),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Io(err) => write!(f, "engine io: {err}"),
            EngineError::Config(err) => write!(f, "engine config: {err}"),
            EngineError::Storage(err) => write!(f, "engine storage: {err}"),
            EngineError::Sql(err) => write!(f, "engine sql: {err}"),
            EngineError::Chain(err) => write!(f, "engine chain: {err}"),
            EngineError::BadConfig(what) => write!(f, "engine config: {what}"),
            EngineError::Runtime(what) => write!(f, "engine runtime: {what}"),
        }
    }
}

impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EngineError::Io(err) => Some(err),
            EngineError::Config(err) => Some(err),
            EngineError::Storage(err) => Some(err),
            EngineError::Sql(err) => Some(err),
            EngineError::Chain(err) => Some(err),
            EngineError::BadConfig(_) => None,
            EngineError::Runtime(_) => None,
        }
    }
}

impl From<std::io::Error> for EngineError {
    fn from(err: std::io::Error) -> Self {
        EngineError::Io(err)
    }
}

impl From<ConfigError> for EngineError {
    fn from(err: ConfigError) -> Self {
        EngineError::Config(err)
    }
}

impl From<storage::StorageError> for EngineError {
    fn from(err: storage::StorageError) -> Self {
        EngineError::Storage(err)
    }
}

impl From<SqlError> for EngineError {
    fn from(err: SqlError) -> Self {
        EngineError::Sql(err)
    }
}

impl From<crate::chain::ChainError> for EngineError {
    fn from(err: crate::chain::ChainError) -> Self {
        EngineError::Chain(err)
    }
}

/// Shared state of the settlement engine. Cheap to clone: the clones share
/// the config and the shutdown flag.
#[derive(Clone)]
pub struct Runtime {
    /// The active configuration, swapped by a SIGHUP reload.
    config: Arc<RwLock<SettlementConfig>>,
    /// The shutdown flag, set by SIGTERM / SIGINT.
    shutdown: Arc<AtomicBool>,
}

impl Runtime {
    /// Creates the runtime around the initial configuration.
    pub fn new(config: SettlementConfig) -> Self {
        Self { config: Arc::new(RwLock::new(config)), shutdown: Arc::new(AtomicBool::new(false)) }
    }

    /// The shutdown flag, for the signal handler.
    pub fn shutdown(&self) -> &Arc<AtomicBool> {
        &self.shutdown
    }

    /// Applies a reloaded config. The running threads snapshot their
    /// parameters at launch: the symbol, the queues, the submitter pool and
    /// the chain parameters take effect on the next restart.
    pub fn reload(&self, config: SettlementConfig) {
        *self.config.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = config;
    }

    /// Launches the engine: opens the queues and the sequence counter,
    /// spawns the side threads and the submitter pool, then runs the core
    /// loop until the shutdown.
    pub fn launch(&self) -> Result<(), EngineError> {
        let config = self.config.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
        info!(symbol = %config.symbol.hex(), "starting the settlement engine");

        // The trade SPSC queue wired from the SVD_OMS_Master: this process
        // owns the queue file.
        let queue = SpscQueue::<Trade>::open(
            &config.trade.path,
            config.trade.capacity,
            config.trade.create,
        )?;

        // The persistent batch-sequence counter, shared by the core thread
        // and the submitters.
        let seq = Arc::new(SeqFile::open(&config.core.seq_path)?);

        // Startup invariants of the submitter pool.
        if config.submitters.is_empty() {
            return Err(EngineError::BadConfig("at least one submitter is required".to_string()));
        }
        let budget = frame_budget(config.batch_size);
        for submitter in &config.submitters {
            if submitter.queue.capacity_bytes < budget {
                return Err(EngineError::BadConfig(format!(
                    "the submitter queue capacity {} cannot hold one {}-trade batch frame ({} bytes)",
                    submitter.queue.capacity_bytes, config.batch_size, budget
                )));
            }
        }

        // The submitter queues: this process owns both ends. It initializes
        // each file when missing and never reinitializes on a restart — the
        // unprocessed frames survive.
        let mut core_queues = Vec::with_capacity(config.submitters.len());
        for submitter in &config.submitters {
            let create = !Path::new(&submitter.queue.path).exists();
            core_queues.push(ByteSpscQueue::open(
                &submitter.queue.path,
                submitter.queue.capacity_bytes,
                create,
            )?);
        }

        // The outcome channels and the side threads, before the first trade
        // arrives.
        let (publisher_tx, publisher_rx) = mpsc::channel::<(SettlementResult, mpsc::Sender<()>)>();
        let (sql_tx, sql_rx) = mpsc::channel::<SettlementResult>();
        let sink = Box::new(RedisStore::connect(
            &config.redis,
            settlement_channel(config.symbol),
            snapshot_key(config.symbol),
        )?);
        spawn_publisher(publisher_rx, sink)?;
        let sql = build_sql_writer(&config)?;
        spawn_sql_writer(sql_rx, sql)?;

        // The submitter pool: one thread per operator key, each consuming
        // its own queue.
        let symbol = config.symbol;
        let shared = config.chain.clone();
        let submitters_alive = Arc::new(AtomicUsize::new(config.submitters.len()));
        let mut submitter_handles = Vec::with_capacity(config.submitters.len());
        for (index, submitter) in config.submitters.iter().enumerate() {
            // The submitter's own consumer instance of its queue file.
            let submitter_queue =
                ByteSpscQueue::open(&submitter.queue.path, submitter.queue.capacity_bytes, false)?;
            let build_chain = build_submitter_chain(submitter, &shared)?;
            let handle = spawn_submitter(
                submitter_queue,
                build_chain,
                Arc::clone(&seq),
                symbol,
                SubmitChannels { publisher: publisher_tx.clone(), sql: sql_tx.clone() },
                SubmitterConfig {
                    poll_interval_ms: config.poll_interval_ms,
                    confirm_depth: shared.confirmations,
                    tx_lost_grace_ms: config.tx_lost_grace_ms,
                    retry: config.retry.clone(),
                },
                Arc::clone(&self.shutdown),
                Arc::clone(&submitters_alive),
            )
            .map_err(|err| {
                EngineError::Runtime(format!(
                    "cannot spawn the submitter {index} of {}: {err}",
                    symbol.hex()
                ))
            })?;
            submitter_handles.push(handle);
        }

        // The core thread, joined below: it groups the trades and feeds the
        // submitter queues. Its buffers are pre-allocated from the trade
        // queue capacity — the hard bound of one taker group.
        let core_shutdown = Arc::clone(&self.shutdown);
        let core_id = config.core_id;
        let trade_capacity = config.trade.capacity;
        let thread = std::thread::Builder::new()
            .name("stl-core".to_string())
            .spawn(move || {
                util::pin::pin_current_thread(core_id);
                spin(queue, core_queues, seq, trade_capacity, submitters_alive, core_shutdown);
            })
            .map_err(|err| EngineError::Runtime(format!("cannot spawn the core thread: {err}")))?;
        thread.join().map_err(|_| EngineError::Runtime("the core thread panicked".to_string()))?;

        // The submitters finish their in-flight batches on the shutdown
        // (never acking unfinished work) and exit.
        for handle in submitter_handles {
            let _ = handle.join();
        }
        // Dropping the channel senders stops the publisher and the SQL
        // writer (they exit on the channel closure).
        drop(publisher_tx);
        drop(sql_tx);
        info!("the settlement engine stopped");
        Ok(())
    }
}

/// Builds the chain client constructor of one submitter: the closure runs
/// on the submitter thread, which builds its own current-thread tokio
/// runtime (kept alive for the thread's lifetime) and the alloy client on
/// it. Behind the `chain-alloy` feature this is the alloy stack; without it
/// the engine cannot submit and refuses to start.
fn build_submitter_chain(
    submitter: &ConfigSubmitter,
    shared: &SharedChainConfig,
) -> Result<ChainBuilder, EngineError> {
    #[cfg(feature = "chain-alloy")]
    {
        let chain_config = submitter.chain_config(shared);
        Ok(Box::new(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| crate::chain::ChainError::Config { detail: err.to_string() })?;
            let client = crate::chain::alloy_impl::AlloyChainClient::new(
                &chain_config,
                runtime.handle().clone(),
            )?;
            // The runtime stays alive on the submitter thread for the
            // client's lifetime.
            Ok(Arc::new(OwnedClient { client, _runtime: runtime }))
        }))
    }
    #[cfg(not(feature = "chain-alloy"))]
    {
        let _ = (submitter, shared);
        Err(EngineError::Runtime("the chain client requires the chain-alloy feature".to_string()))
    }
}

/// A chain client that owns the tokio runtime its alloy client drives.
#[cfg(feature = "chain-alloy")]
struct OwnedClient {
    /// The alloy client.
    client: crate::chain::alloy_impl::AlloyChainClient,
    /// The current-thread runtime of the submitter thread, kept alive for
    /// the client's lifetime.
    _runtime: tokio::runtime::Runtime,
}

#[cfg(feature = "chain-alloy")]
impl ChainClient for OwnedClient {
    fn submit(
        &self,
        calldata: &[u8],
    ) -> Result<crate::chain::SubmittedTx, crate::chain::ChainError> {
        self.client.submit(calldata)
    }

    fn tx_state(
        &self,
        tx: primitives::base::Hash32,
    ) -> Result<crate::chain::TxState, crate::chain::ChainError> {
        self.client.tx_state(tx)
    }

    fn bump_fee(
        &self,
        calldata: &[u8],
        nonce: u64,
    ) -> Result<crate::chain::SubmittedTx, crate::chain::ChainError> {
        self.client.bump_fee(calldata, nonce)
    }

    fn is_retryable(&self, err: &crate::chain::ChainError) -> bool {
        self.client.is_retryable(err)
    }
}

/// Connects the SQL writer: the first URL that accepts a connection wins.
fn build_sql_writer(config: &SettlementConfig) -> Result<Box<dyn SettlementSql>, EngineError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| EngineError::Runtime(err.to_string()))?;
    let sql = runtime.block_on(MySqlSettlement::connect(&config.sql.urls, config.sql.pool_size))?;
    Ok(Box::new(sql))
}
