//! The settlement runtime: the shared state of the process (config,
//! shutdown), the forwarding core thread and the side threads.
//!
//! The core thread is pure data forwarding: it drains the trade SPSC queue
//! wired from the [SVD_OMS_Master] in batches, journals each drained group
//! (the durability boundary — a drained trade survives a crash) and hands
//! the group to the submitter through a pooled buffer. No checks, no
//! locks beyond the journal append, no blocking, no allocation in the
//! steady state.

use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;

use cache::object_pool::{Cache, CacheGuard};
use ipc::mmap_spsc::SpscQueue;
use primitives::message::hot_path::Trade;
use primitives::message::settlement::SettlementResult;
use primitives::order::Order;
use primitives::value::{Price, Quantity};
use storage::RedisStore;
use tracing::{error, info};

use crate::chain::ChainClient;
use crate::config::{ConfigError, SettlementConfig};
use crate::journal::{
    SettlementJournal, SettlementJournalError, encode_trade_batch, trade_batch_budget,
};
use crate::naming::{settlement_channel, snapshot_key};
use crate::publisher::spawn_publisher;
use crate::sql::{MySqlSettlement, SettlementSql, SqlError, spawn_sql_writer};
use crate::submitter::{SubmitChannels, SubmitterConfig, spawn as spawn_submitter};

/// Number of empty spins before the core thread yields the CPU.
const SPINS_PER_YIELD: u32 = 4096;

/// Errors of the settlement engine.
#[derive(Debug)]
pub enum EngineError {
    /// A file or queue operation failed.
    Io(std::io::Error),
    /// The config could not be loaded.
    Config(ConfigError),
    /// The journal failed.
    Journal(SettlementJournalError),
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
            EngineError::Journal(err) => write!(f, "engine journal: {err}"),
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
            EngineError::Journal(err) => Some(err),
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

impl From<SettlementJournalError> for EngineError {
    fn from(err: SettlementJournalError) -> Self {
        EngineError::Journal(err)
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

    /// Applies a reloaded config. The batching and retry parameters follow
    /// the reload; the symbol, the queues, the journal and the chain
    /// parameters take effect on the next restart.
    pub fn reload(&self, config: SettlementConfig) {
        *self.config.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = config;
    }

    /// Launches the engine: opens the journal and the trade queue, spawns
    /// the side threads, then runs the core loop until the shutdown.
    pub fn launch(&self) -> Result<(), EngineError> {
        let config = self.config.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
        info!(symbol = %config.symbol.hex(), "starting the settlement engine");

        // The journal, with the startup check that one drain batch always
        // fits one record (the record header rides on top of the payload).
        let journal = SettlementJournal::open(&config.journal.path, config.journal.size)?;
        let budget =
            trade_batch_budget(config.batch_size) as u64 + storage::journal::RECORD_HEADER_SIZE;
        if config.journal.size.saturating_sub(storage::journal::HEADER_SIZE) < budget {
            return Err(EngineError::BadConfig(format!(
                "the journal size {} cannot hold one {} trade batch record ({budget} bytes)",
                config.journal.size, config.batch_size
            )));
        }

        // The trade SPSC queue wired from the SVD_OMS_Master: this process
        // owns the queue file.
        let queue = SpscQueue::<Trade>::open(
            &config.trade.path,
            config.trade.capacity,
            config.trade.create,
        )?;

        // The hand-off: the drained groups travel to the submitter in
        // pooled buffers.
        let (handoff_tx, handoff_rx) = mpsc::channel::<CacheGuard<Vec<Trade>>>();
        let handoff_capacity = config.handoff.capacity;
        let handoff_pool =
            Cache::new(config.handoff.pool_size, move || Vec::with_capacity(handoff_capacity));

        // The chain client of the submitter.
        let chain = build_chain_client(&config)?;

        // The outcome channels and the side threads, before the first trade
        // arrives.
        let (publisher_tx, publisher_rx) = mpsc::channel::<SettlementResult>();
        let (sql_tx, sql_rx) = mpsc::channel::<SettlementResult>();
        let sink = Box::new(RedisStore::connect(
            &config.redis,
            settlement_channel(config.symbol),
            snapshot_key(config.symbol),
        )?);
        spawn_publisher(publisher_rx, sink, Arc::clone(&self.shutdown))?;
        let sql = build_sql_writer(&config)?;
        spawn_sql_writer(sql_rx, sql, Arc::clone(&self.shutdown))?;
        let symbol = config.symbol;
        spawn_submitter(
            handoff_rx,
            journal.clone(),
            chain,
            symbol,
            SubmitChannels { publisher: publisher_tx, sql: sql_tx },
            SubmitterConfig {
                max_trades_per_batch: config.batch.max_trades_per_batch,
                batch_window_ms: config.batch.batch_window_ms,
                poll_interval_ms: config.poll_interval_ms,
                confirm_depth: config.chain.confirmations,
                tx_lost_grace_ms: config.tx_lost_grace_ms,
                retry: config.retry.clone(),
            },
            Arc::clone(&self.shutdown),
        )?;

        // The forwarding core thread, joined below.
        let core_shutdown = Arc::clone(&self.shutdown);
        let core_id = config.core_id;
        let batch_size = config.batch_size;
        let thread = std::thread::Builder::new()
            .name("stl-core".to_string())
            .spawn(move || {
                util::pin::pin_current_thread(core_id);
                spin(queue, journal, handoff_tx, handoff_pool, batch_size, core_shutdown);
            })
            .map_err(|err| EngineError::Runtime(format!("cannot spawn the core thread: {err}")))?;
        thread.join().map_err(|_| EngineError::Runtime("the core thread panicked".to_string()))?;
        info!("the settlement engine stopped");
        Ok(())
    }
}

/// Builds the chain client of the submitter. Behind the `chain-alloy`
/// feature this is the alloy stack; without it the engine cannot submit
/// and refuses to start.
fn build_chain_client(
    config: &SettlementConfig,
) -> Result<Arc<dyn ChainClient + Send + Sync>, EngineError> {
    #[cfg(feature = "chain-alloy")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| EngineError::Runtime(err.to_string()))?;
        let client = crate::chain::alloy_impl::AlloyChainClient::new(
            &config.chain,
            runtime.handle().clone(),
        )?;
        Ok(Arc::new(client))
    }
    #[cfg(not(feature = "chain-alloy"))]
    {
        let _ = config;
        Err(EngineError::Runtime("the chain client requires the chain-alloy feature".to_string()))
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

/// The forwarding core loop: drains the trade queue in batches, journals
/// each drained group and hands it to the submitter. The drain buffer and
/// the encode buffer are pre-allocated once and reused; the journal append
/// is a memory copy into the mapped pages, so the loop performs no
/// allocation and no blocking in the steady state.
///
/// The shutdown drains the queue: the trade queue is file mapped (unread
/// trades survive), and a drained group is journaled before the exit is
/// even considered — the exit condition only fires on an empty pop. A full
/// journal degrades gracefully: the record is halved until it fits, and a
/// single trade that cannot fit stops the loop with an error.
fn spin(
    mut queue: SpscQueue<Trade>,
    journal: SettlementJournal,
    handoff_tx: mpsc::Sender<CacheGuard<Vec<Trade>>>,
    handoff_pool: Cache<Vec<Trade>>,
    batch_size: usize,
    shutdown: Arc<AtomicBool>,
) {
    let mut drain: Vec<Trade> = Vec::with_capacity(batch_size);
    drain.resize_with(batch_size, dummy_trade);
    let mut encode_buf: Vec<u8> = Vec::with_capacity(trade_batch_budget(batch_size));
    let mut empty_spins = 0u32;
    info!("the settlement core loop started");
    loop {
        let n = queue.pop_batch(&mut drain);
        if n == 0 {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            std::hint::spin_loop();
            empty_spins += 1;
            if empty_spins.is_multiple_of(SPINS_PER_YIELD) {
                std::thread::yield_now();
            }
            continue;
        }
        // Journal first, hand off second — the durability boundary. A full
        // journal stops the loop: the log never overwrites records, so no
        // splitting makes room (two records always cost more than one) —
        // the startup check sizes the journal for one drain batch and the
        // log fills over the process lifetime until a compaction exists.
        let mut start = 0usize;
        while start < n {
            if let Err(err) = encode_trade_batch(&drain[start..n], &mut encode_buf) {
                error!(error = %err, "cannot encode the trade batch, stopping the core loop");
                return;
            }
            if let Err(err) = journal.write_encoded(&encode_buf) {
                error!(error = %err, "cannot journal the trade batch, stopping the core loop");
                return;
            }
            let mut guard = handoff_pool.acquire();
            guard.extend_from_slice(&drain[start..n]);
            if handoff_tx.send(guard).is_err() {
                error!("the submitter is gone, stopping the core loop");
                return;
            }
            start = n;
        }
    }
    info!("the settlement core loop stopped");
}

/// The filler of the drain buffer: `Trade` has no `Default`.
fn dummy_trade() -> Trade {
    Trade::new(Order::default(), Quantity::ZERO, Order::default(), Price::ZERO, Quantity::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// Monotonic counter so tests running in parallel never collide on
    /// temp paths.
    static SEQ: AtomicUsize = AtomicUsize::new(0);

    /// Removes the file when dropped.
    struct TempFile(std::path::PathBuf);

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dex_stl_engine_{}_{}_{}", std::process::id(), tag, seq))
    }

    fn trade(user: u8, nonce: u64) -> Trade {
        use primitives::address::Address;
        use primitives::base::{Hash32, Nonce, Side, Symbol};
        use primitives::order::{OrderCold, OrderColdCommon, OrderHot, OrderKind};
        use primitives::signature::Signature;
        use primitives::time_in_force::TimeInForce;
        use primitives::value::TimestampMs;
        Trade::new(
            Order::new(
                OrderHot {
                    user: Address([user; 20]),
                    nonce: Nonce(nonce),
                    price: Price(100),
                    quantity: Quantity(10),
                    time_in_force: TimeInForce::Gtc,
                    side: Side::Buy,
                },
                OrderCold::new(
                    OrderColdCommon::new(
                        Hash32([0; 32]),
                        Symbol([0; 32]),
                        Signature::default(),
                        TimestampMs(0),
                    ),
                    OrderKind::Standard,
                ),
            ),
            Quantity(9),
            Order::default(),
            Price(100),
            Quantity(1),
        )
    }

    #[test]
    fn test_spin_journals_and_hands_off_the_drained_trades() {
        let queue_path = temp_path("queue");
        let journal_path = temp_path("journal");
        let _guards = (TempFile(queue_path.clone()), TempFile(journal_path.clone()));

        let mut queue = SpscQueue::<Trade>::open(&queue_path, 64, true).unwrap();
        queue.push_batch(&[trade(1, 1), trade(2, 1)]);
        drop(queue);

        let journal = SettlementJournal::open(&journal_path.to_string_lossy(), 4096).unwrap();
        let (tx, rx) = mpsc::channel::<CacheGuard<Vec<Trade>>>();
        let shutdown = Arc::new(AtomicBool::new(true)); // drain once, then exit
        spin(
            SpscQueue::open(&queue_path, 64, false).unwrap(),
            journal.clone(),
            tx,
            Cache::new(2, || Vec::with_capacity(8)),
            16,
            shutdown,
        );

        // Both trades reached the hand-off channel.
        let mut received = Vec::new();
        for guard in rx.try_iter() {
            received.extend_from_slice(&guard);
        }
        assert_eq!(received.len(), 2);

        // And the journal holds them as a trade-batch record.
        let records = journal.replay().unwrap();
        assert_eq!(records.len(), 1);
        let (_, record) = &records[0];
        match record {
            crate::journal::SettlementRecord::TradeBatch { trades } => {
                assert_eq!(trades.len(), 2);
            }
            other => panic!("expected a trade batch record, got {other:?}"),
        }
    }

    #[test]
    fn test_spin_stops_when_the_journal_cannot_fit_the_batch() {
        let queue_path = temp_path("queue_full");
        let journal_path = temp_path("journal_full");
        let _guards = (TempFile(queue_path.clone()), TempFile(journal_path.clone()));

        // The journal holds a little over one one-trade record: a two-trade
        // drain does not fit, the loop stops with nothing journaled and
        // nothing handed off (the trades stay in the file-mapped queue —
        // a restart with a grown journal re-drains them).
        let mut queue = SpscQueue::<Trade>::open(&queue_path, 64, true).unwrap();
        queue.push_batch(&[trade(1, 1), trade(2, 1)]);
        drop(queue);

        let mut one_trade = Vec::new();
        encode_trade_batch(&[trade(1, 1)], &mut one_trade).unwrap();
        let journal = SettlementJournal::open(
            &journal_path.to_string_lossy(),
            storage::journal::HEADER_SIZE
                + storage::journal::RECORD_HEADER_SIZE
                + one_trade.len() as u64
                + 16,
        )
        .unwrap();
        let (tx, rx) = mpsc::channel::<CacheGuard<Vec<Trade>>>();
        spin(
            SpscQueue::open(&queue_path, 64, false).unwrap(),
            journal.clone(),
            tx,
            Cache::new(2, || Vec::with_capacity(8)),
            16,
            Arc::new(AtomicBool::new(true)),
        );

        assert!(journal.replay().unwrap().is_empty(), "nothing fits, nothing journals");
        assert_eq!(rx.try_iter().count(), 0, "nothing is handed off either");
    }
}
