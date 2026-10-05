//! The OMS master: the hot path of the system.
//!
//! Three threads: the pinned core thread spins on the ingress SPSC queue
//! from SVD_Pretrade and executes the requests on the book (the user
//! requests and the settlement-driven restores of the `PipelineMsg`); the
//! NATS publisher thread serializes and publishes the replication messages;
//! the settlement writer thread pushes the trade events into the SPSC queue
//! wired to SVD_Settlement. The fanout closures only hand the pooled
//! buffers to the I/O threads through unbounded channels, so the core
//! thread never allocates and never blocks on the fanout I/O.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, RwLock, mpsc};
use std::time::Duration;

use async_nats::jetstream;
use cache::object_pool::Cache;
use ipc::mmap_spsc::SpscQueue;
use primitives::message::hot_path::{OrderMsg, PipelineMsg, Trade};
use primitives::orderbook::book::OrderBook;
use primitives::orderbook::listener::{Listeners, PooledReplicationMsg, PooledTrades};
use tracing::{error, info, warn};
use primitives::order::Order;
use super::{EngineError, connect_jetstream};
use crate::config::OmsConfig;

/// Number of empty spins before the core thread yields the CPU.
const SPINS_PER_YIELD: u32 = 4096;

/// Runs the master until shutdown. Promoted slaves enter here with their
/// already-recovered book.
pub fn run(
    config: Arc<RwLock<OmsConfig>>,
    _mode: Arc<AtomicU8>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), EngineError> {
    let oms = config.read().expect("config lock").clone();
    info!(symbol = %crate::naming::symbol_hex(oms.symbol), "starting the OMS master");

    // NATS: connect and ensure the stream (blocking on startup is fine).
    let jetstream = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| EngineError::Runtime(err.to_string()))?
        .block_on(connect_jetstream(&oms.nats, oms.symbol))?;
    let subject = oms.nats.subject(oms.symbol);

    // Unbounded fanout channels: the core thread's sends never block.
    let (repl_tx, repl_rx) = mpsc::channel::<PooledReplicationMsg>();
    let (trade_tx, trade_rx) = mpsc::channel::<PooledTrades>();

    // The I/O threads, ready before the first message arrives.
    spawn_nats_publisher(jetstream, subject, repl_rx, Arc::clone(&shutdown));
    spawn_settlement_writer(&oms, trade_rx, Arc::clone(&shutdown));

    // The listeners move the pooled buffers to the I/O threads; the buffers
    // return to the pools when those threads drop them.
    let book = OrderBook::new(oms.book_config()).with_listeners(
        Listeners::default()
            .with_book_state_listener(Box::new(move |msg| {
                let _ = repl_tx.send(msg);
            }))
            .with_trade_state_listener(Box::new(move |trades| {
                let _ = trade_tx.send(trades);
            })),
    );

    // The ingress queue wired from SVD_Pretrade.
    let ingress = SpscQueue::<PipelineMsg>::open(
        &oms.ingress.path,
        oms.ingress.capacity,
        oms.ingress.create,
    )?;

    // The batch buffer pool of the core loop: one pre-allocated buffer of
    // `batch_size` slots, checked out for the lifetime of the loop.
    let batch_pool = batch_buffer_pool(oms.batch_size);

    spin(book, ingress, batch_pool, shutdown);
    Ok(())
}

/// Pre-allocates the pool of the ingress batch buffers: one buffer of
/// `batch_size` default slots. The core loop checks its batch out of this
/// pool instead of allocating one, and the buffer returns to the pool
/// (cleared, capacity kept) when the loop stops.
fn batch_buffer_pool(batch_size: usize) -> Cache<Vec<PipelineMsg>> {
    Cache::new(1, move || {
        let mut batch = Vec::with_capacity(batch_size);
        batch.resize_with(batch_size, || {
            PipelineMsg::User(OrderMsg::NewOrder(Order::default()))
        });
        batch
    })
}

/// The core spin loop: pops a batch of requests and executes them. The
/// batch buffer is checked out of the pre-allocated pool and returned to it
/// when the loop stops; the empty loop only spins, so the hot path performs
/// no allocations and no blocking calls.
fn spin(
    mut book: OrderBook,
    mut ingress: SpscQueue<PipelineMsg>,
    batch_pool: Cache<Vec<PipelineMsg>>,
    shutdown: Arc<AtomicBool>,
) {
    let mut batch = batch_pool.acquire();
    let mut empty_spins = 0u32;
    info!("the master core loop started");
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let n = ingress.pop_batch(&mut batch);
        for msg in &batch[..n] {
            match msg {
                PipelineMsg::User(request) => {
                    book.execute(request).expect("the book execution is infallible")
                }
                PipelineMsg::RestoreOrder { order, quantity } => {
                    book.restore_order(order, *quantity)
                }
            }
        }
        if n == 0 {
            std::hint::spin_loop();
            empty_spins += 1;
            if empty_spins.is_multiple_of(SPINS_PER_YIELD) {
                std::thread::yield_now();
            }
        }
    }
    info!("the master core loop stopped");
}

/// Spawns the NATS publisher thread: serializes the replication messages
/// and publishes them in order. A failed publishing is retried until accepted,
/// so the replication stream never drops a change.
fn spawn_nats_publisher(
    jetstream: jetstream::Context,
    subject: String,
    receiver: mpsc::Receiver<PooledReplicationMsg>,
    shutdown: Arc<AtomicBool>,
) {
    std::thread::Builder::new()
        .name("oms-nats".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            runtime.block_on(async move {
                while let Ok(msg) = receiver.recv() {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    let bytes = match rmp_serde::to_vec(&msg) {
                        Ok(bytes) => bytes,
                        Err(err) => {
                            error!(error = %err, "cannot encode the replication message");
                            continue;
                        }
                    };
                    loop {
                        match jetstream.publish(subject.clone(), bytes.clone().into()).await {
                            Ok(_) => break,
                            Err(err) => {
                                warn!(error = %err, "cannot publish to NATS, retrying");
                                if shutdown.load(Ordering::Relaxed) {
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                }
            });
        })
        .expect("spawn the NATS publisher thread");
}

/// Spawns the settlement writer thread: pushes the trade events into the
/// SPSC queue wired to SVD_Settlement. The queue is never allowed to drop a
/// trade: a full queue spins until the settlement drains it.
fn spawn_settlement_writer(
    oms: &OmsConfig,
    receiver: mpsc::Receiver<PooledTrades>,
    shutdown: Arc<AtomicBool>,
) {
    let mut queue = match SpscQueue::<Trade>::open(
        &oms.settlement.path,
        oms.settlement.capacity,
        oms.settlement.create,
    ) {
        Ok(queue) => queue,
        Err(err) => {
            error!(error = %err, "cannot open the settlement queue");
            return;
        }
    };
    std::thread::Builder::new()
        .name("oms-settlement".to_string())
        .spawn(move || {
            while let Ok(trades) = receiver.recv() {
                let mut pushed = 0;
                while pushed < trades.len() {
                    pushed += queue.push_batch(&trades[pushed..]);
                    if pushed < trades.len() {
                        if shutdown.load(Ordering::Relaxed) {
                            return;
                        }
                        std::thread::yield_now();
                    }
                }
            }
        })
        .expect("spawn the settlement writer thread");
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::address::Address;
    use primitives::base::{Hash32, Nonce, Side, Symbol};
    use primitives::message::side_path::OrderStatus;
    use primitives::order::{Order, OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use primitives::orderbook::config::BookConfig;
    use primitives::signature::Signature;
    use primitives::time_in_force::TimeInForce;
    use primitives::value::{Price, Quantity, TimestampMs};
    use std::sync::atomic::AtomicUsize;

    /// Builds a minimal order for the fanout tests.
    fn order(user: u8, nonce: u64, side: Side) -> Order {
        Order::new(
            OrderHot {
                user: Address([user; 20]),
                nonce: Nonce(nonce),
                price: Price(100),
                quantity: Quantity(10),
                time_in_force: TimeInForce::Gtc,
                side,
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
    fn test_replication_closure_hands_off_to_channel() {
        let (tx, rx) = mpsc::channel::<PooledReplicationMsg>();
        let listener = Box::new(move |msg: PooledReplicationMsg| {
            let _ = tx.send(msg);
        });
        let mut book = OrderBook::new(BookConfig::default())
            .with_listeners(Listeners::default().with_book_state_listener(listener));
        // Execute through the real fanout path.
        book.execute(&OrderMsg::NewOrder(order(1, 1, Side::Buy))).unwrap();
        let msg = rx.recv().unwrap();
        assert!(!msg.is_empty());
        assert!(matches!(msg[0].status(), OrderStatus::Open));
    }

    #[test]
    fn test_trade_closure_hands_off_to_channel() {
        let (tx, rx) = mpsc::channel::<PooledTrades>();
        let listener = Box::new(move |trades: PooledTrades| {
            let _ = tx.send(trades);
        });
        let mut book = OrderBook::new(BookConfig::default())
            .with_listeners(Listeners::default().with_trade_state_listener(listener));
        book.execute(&OrderMsg::NewOrder(order(2, 1, Side::Sell))).unwrap(); // resting maker
        book.execute(&OrderMsg::NewOrder(order(1, 1, Side::Buy))).unwrap(); // crossing taker
        let trades = rx.recv().unwrap();
        assert_eq!(trades.len(), 1);
        drop(trades);
    }

    #[test]
    fn test_spin_loop_processes_batch_and_stops_on_shutdown() {
        let path = std::env::temp_dir().join(format!("dex_oms_master_spin_{}", std::process::id()));
        let _guard = {
            struct Guard(std::path::PathBuf);
            impl Drop for Guard {
                fn drop(&mut self) {
                    let _ = std::fs::remove_file(&self.0);
                }
            }
            Guard(path.clone())
        };
        let mut ingress = SpscQueue::<PipelineMsg>::open(&path, 64, true).unwrap();
        ingress.push(PipelineMsg::User(OrderMsg::NewOrder(order(1, 1, Side::Buy)))).unwrap();
        ingress.push(PipelineMsg::User(OrderMsg::NewOrder(order(1, 2, Side::Buy)))).unwrap();
        drop(ingress);
        // The book captures the executed orders through the listener. The
        // book lives on the spawned thread only (mirroring the core thread
        // of the engine, which owns the book).
        let executed = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&executed);
        let shutdown = Arc::new(AtomicBool::new(false));
        let spin_shutdown = Arc::clone(&shutdown);
        let spin_path = path.clone();
        let handle = std::thread::spawn(move || {
            let ingress = SpscQueue::<PipelineMsg>::open(&spin_path, 64, false).unwrap();
            let book = OrderBook::new(BookConfig::default()).with_listeners(
                Listeners::default().with_book_state_listener(Box::new(move |msg| {
                    counter.fetch_add(msg.len(), Ordering::Relaxed);
                })),
            );
            spin(book, ingress, batch_buffer_pool(16), spin_shutdown);
        });
        // Wait until both orders are processed, then stop.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while executed.load(Ordering::Relaxed) < 2 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
        assert_eq!(executed.load(Ordering::Relaxed), 2);
    }
}
