//! The pre-trade runtime: the shared state of the process (config,
//! shutdown), the forwarding core thread, the feed threads and the HTTP
//! gateway.
//!
//! The core thread is pure data forwarding: it drains the shared MPSC
//! queue in batches and moves them into the SPSC share memory queue wired
//! to the [SVD_OMS_Master] — no checks, no locks, no blocking, no
//! allocation (the batch buffer is pre-allocated once and reused every
//! iteration). The signal handlers only write the shared flags: a SIGHUP
//! reload swaps the config, SIGTERM and SIGINT set the shutdown flag.

use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::config::{ConfigError, PretradeConfig};
use crate::feed;
use crate::gateway::Gateway;
use crate::margin::MarginCache;
use crate::naming;
use crossbeam_queue::ArrayQueue;
use ipc::mmap_spsc::SpscQueue;
use primitives::message::hot_path::{CancelOrder, OrderMsg, PipelineMsg};
use storage::RedisKeyStore;
use tracing::{error, info};
use util::pin;
use util::time::now_ms;

/// Number of empty spins before the core thread yields the CPU.
const SPINS_PER_YIELD: u32 = 4096;
/// The poll interval of the graceful shutdown watch, in milliseconds.
const SHUTDOWN_POLL_MS: u64 = 100;

/// Errors of the pre-trade engine.
#[derive(Debug)]
pub enum EngineError {
    /// A file or queue operation failed.
    Io(std::io::Error),
    /// The config could not be loaded.
    Config(ConfigError),
    /// A storage (Redis) operation failed.
    Storage(storage::StorageError),
    /// The tokio runtime could not be built.
    Runtime(String),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Io(err) => write!(f, "engine io: {err}"),
            EngineError::Config(err) => write!(f, "engine config: {err}"),
            EngineError::Storage(err) => write!(f, "engine storage: {err}"),
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

/// Shared state of the pre-trade engine. The signal handlers write into it,
/// the threads read it. Cheap to clone: the clones share the config and the
/// shutdown flag.
#[derive(Clone)]
pub struct Runtime {
    /// The active configuration, swapped by a SIGHUP reload.
    config: Arc<RwLock<PretradeConfig>>,
    /// The shutdown flag, set by SIGTERM / SIGINT.
    shutdown: Arc<AtomicBool>,
}

impl Runtime {
    /// Creates the runtime around the initial configuration.
    pub fn new(config: PretradeConfig) -> Self {
        Self { config: Arc::new(RwLock::new(config)), shutdown: Arc::new(AtomicBool::new(false)) }
    }

    /// The shared config, for the signal handler.
    pub fn config(&self) -> &Arc<RwLock<PretradeConfig>> {
        &self.config
    }

    /// The shutdown flag, for the signal handler.
    pub fn shutdown(&self) -> &Arc<AtomicBool> {
        &self.shutdown
    }

    /// Applies a reloaded config. The handlers read the margin parameters
    /// per request; the symbol, the queues and the chain parameters take
    /// effect on the next restart.
    pub fn reload(&self, config: PretradeConfig) {
        *self.config.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = config;
    }

    /// Launches the engine: spawns the feed threads and the forwarding core
    /// thread, then serves the HTTP gateway until the shutdown.
    pub fn launch(&self) -> Result<(), EngineError> {
        let config = self.config.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
        info!(symbol = %naming::symbol_hex(config.symbol), "starting the pre-trade gateway");

        // The shared structures: the pipeline queue, the margin cache, the
        // feed liveness and the keyed store of the pulls.
        let queue = Arc::new(ArrayQueue::<PipelineMsg>::new(config.mpsc_capacity));
        let cache =
            Arc::new(MarginCache::new(config.margin.shards, config.margin.max_tracked_accounts));
        let liveness = Arc::new(AtomicU64::new(now_ms()));
        let key_store = RedisKeyStore::connect(&config.redis)?;

        // The feed threads, ready before the first request arrives.
        feed::spawn_margin_feed(
            config.redis.urls.clone(),
            Arc::clone(&cache),
            Arc::clone(&liveness),
            config.margin.idle_evict_ms,
            config.margin.max_tracked_accounts,
            Arc::clone(&self.shutdown),
        );
        feed::spawn_settlement_feed(
            config.redis.urls.clone(),
            naming::settlement_channel(config.symbol),
            Arc::clone(&cache),
            Arc::clone(&queue),
            config.chain.chain_id,
            config.chain.verifying_contract,
            Arc::clone(&self.shutdown),
        );

        // The forwarding core thread.
        let core_shutdown = Arc::clone(&self.shutdown);
        let core_queue = Arc::clone(&queue);
        let core_config = config.clone();
        let thread = std::thread::Builder::new()
            .name("pretrade-core".to_string())
            .spawn(move || {
                pin::pin_current_thread(core_config.core_id);
                spin(core_queue, &core_config.egress, core_config.batch_size, core_shutdown);
            })
            .map_err(|err| EngineError::Runtime(format!("cannot spawn the core thread: {err}")))?;

        // The HTTP gateway on this thread, until the shutdown.
        let gateway = Arc::new(Gateway::new(
            &config,
            Arc::clone(&cache),
            Arc::clone(&liveness),
            Arc::clone(&queue),
            key_store,
        ));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| EngineError::Runtime(err.to_string()))?;
        runtime.block_on(async {
            let listener = tokio::net::TcpListener::bind(&config.listen_addr)
                .await
                .map_err(EngineError::Io)?;
            info!(addr = %config.listen_addr, "the HTTP gateway is listening");
            let shutdown = Arc::clone(&self.shutdown);
            axum::serve(listener, Gateway::router(gateway))
                .with_graceful_shutdown(async move {
                    while !shutdown.load(Ordering::Relaxed) {
                        tokio::time::sleep(Duration::from_millis(SHUTDOWN_POLL_MS)).await;
                    }
                })
                .await
                .map_err(|err| EngineError::Runtime(err.to_string()))
        })?;
        thread.join().map_err(|_| EngineError::Runtime("the core thread panicked".to_string()))?;
        info!("the pre-trade gateway stopped");
        Ok(())
    }
}

/// The forwarding loop: drains the MPSC queue in batches and moves them
/// into the SPSC share memory queue wired to the [SVD_OMS_Master]. The
/// batch buffer is pre-allocated once; the messages are fixed-size `Copy`
/// values, so every move is a plain memory copy into the pre-allocated
/// queue slots — no checks, no locks, no blocking, no allocation.
fn spin(
    queue: Arc<ArrayQueue<PipelineMsg>>,
    egress_config: &crate::config::SpScConfig,
    batch_size: usize,
    shutdown: Arc<AtomicBool>,
) {
    let mut egress = match SpscQueue::<PipelineMsg>::open(
        &egress_config.path,
        egress_config.capacity,
        egress_config.create,
    ) {
        Ok(queue) => queue,
        Err(err) => {
            error!(error = %err, "cannot open the egress queue");
            return;
        }
    };
    let mut batch = Vec::with_capacity(batch_size);
    batch.resize_with(batch_size, || {
        PipelineMsg::User(OrderMsg::CancelOrder(CancelOrder::default()))
    });
    let mut empty_spins = 0u32;
    info!("the forwarding core loop started");
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let mut n = 0usize;
        while n < batch_size {
            match queue.pop() {
                Some(message) => {
                    batch[n] = message;
                    n += 1;
                }
                None => break,
            }
        }
        if n > 0 {
            let mut pushed = 0usize;
            while pushed < n {
                pushed += egress.push_batch(&batch[pushed..n]);
                if pushed < n {
                    if shutdown.load(Ordering::Relaxed) {
                        return;
                    }
                    std::thread::yield_now();
                }
            }
        } else {
            std::hint::spin_loop();
            empty_spins += 1;
            if empty_spins.is_multiple_of(SPINS_PER_YIELD) {
                std::thread::yield_now();
            }
        }
    }
    info!("the forwarding core loop stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::address::Address;
    use primitives::base::{Hash32, Nonce, Side, Symbol};
    use primitives::order::{Order, OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use primitives::signature::Signature;
    use primitives::time_in_force::TimeInForce;
    use primitives::value::{Price, Quantity, TimestampMs};
    use std::sync::atomic::AtomicUsize;

    fn order(user: u8, nonce: u64) -> Order {
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
        )
    }

    #[test]
    fn test_spin_forwards_the_pipeline_messages() {
        let path = std::env::temp_dir().join(format!("dex_pretrade_spin_{}", std::process::id()));
        let _guard = {
            struct Guard(std::path::PathBuf);
            impl Drop for Guard {
                fn drop(&mut self) {
                    let _ = std::fs::remove_file(&self.0);
                }
            }
            Guard(path.clone())
        };
        let queue = Arc::new(ArrayQueue::<PipelineMsg>::new(64));
        queue
            .push(PipelineMsg::User(OrderMsg::NewOrder(order(1, 1))))
            .expect("the queue has capacity");
        queue
            .push(PipelineMsg::RestoreOrder { order: order(2, 2), quantity: Quantity(3) })
            .expect("the queue has capacity");

        // The core thread forwards both messages into the ingress queue.
        let forwarded = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&forwarded);
        let shutdown = Arc::new(AtomicBool::new(false));
        let spin_shutdown = Arc::clone(&shutdown);
        let spin_path = path.clone();
        let spin_queue = Arc::clone(&queue);
        let handle = std::thread::spawn(move || {
            let config = crate::config::SpScConfig {
                path: spin_path.to_string_lossy().into_owned(),
                capacity: 64,
                create: true,
            };
            // Drain the ingress after the spin processes the batch: a second
            // consumer thread reads what the spin pushed.
            let spin_queue = spin_queue;
            spin(spin_queue, &config, 16, spin_shutdown);
            // Signal completion by draining through this thread instead.
            let mut ingress = SpscQueue::<PipelineMsg>::open(&spin_path, 64, false).unwrap();
            let mut batch =
                vec![PipelineMsg::User(OrderMsg::CancelOrder(CancelOrder::default())); 16];
            let n = ingress.pop_batch(&mut batch);
            counter.fetch_add(n, Ordering::Relaxed);
        });

        // The spin forwards both messages then keeps spinning; stop it.
        std::thread::sleep(Duration::from_millis(200));
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
        assert_eq!(forwarded.load(Ordering::Relaxed), 2);
    }
}
