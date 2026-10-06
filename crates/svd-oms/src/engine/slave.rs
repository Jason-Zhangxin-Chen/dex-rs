//! The OMS slave: the side path of the system.
//!
//! Two threads: the pinned core thread consumes the master's replication
//! stream (NATS JetStream), applies the changes to a local book copy, and
//! dispatches the changes and the periodic snapshots to the side I/O
//! thread, which publishes the changes to Redis and persists the snapshots
//! to the journal and/or the Redis cluster, as configured. On restart the
//! slave recovers the book from the journal and/or the Redis snapshot (in
//! that order) and replays the stream from the checkpoint sequence. A
//! SIGHUP config update flipping the mode promotes the slave: it drops the
//! NATS consumer and starts executing the ingress requests as the new
//! master.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_nats::jetstream;
use futures_util::StreamExt;
use primitives::orderbook::book::{OrderBook, OrderBookState};
use primitives::orderbook::listener::PooledReplicationMsg;
use storage::{ChangeSink, RedisStore};
use tracing::{error, info, warn};

use super::{EngineError, MODE_SLAVE, connect_jetstream};
use crate::config::{OmsConfig, SnapshotPersist};
use crate::journal::Journal;
use crate::naming;
use crate::snapshot::Snapshot;

/// Interval of the control poll inside the consume loop.
const CONTROL_POLL: Duration = Duration::from_millis(100);

/// How the slave's consume loop ended.
enum ConsumeOutcome {
    /// The mode flipped: promote to a master.
    Promoted,
    /// The shutdown flag was set.
    Shutdown,
}

/// Runs the slave until shutdown or promotion. A promotion rewires the book
/// with the master's listeners and enters the master loop.
pub fn run(
    config: Arc<RwLock<OmsConfig>>,
    mode: Arc<AtomicU8>,
    shutdown: Arc<AtomicBool>,
    mode_rx: tokio::sync::watch::Receiver<u8>,
) -> Result<(), EngineError> {
    let oms = config.read().expect("config lock").clone();
    info!(symbol = %oms.symbol.hex(), "starting the OMS slave");

    // Recovery: the book rebuilt from the journal / Redis snapshot, and the
    // stream sequence to resume from.
    let (book, journal, start_seq) = recover(&oms)?;

    // The side I/O thread owns the journal from here on.
    let side = SideIo::spawn(&oms, journal)?;

    // NATS: connect and open the pull consumer at the checkpoint.
    let jetstream = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| EngineError::Runtime(err.to_string()))?
        .block_on(connect_jetstream(&oms.nats, oms.symbol))?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| EngineError::Runtime(err.to_string()))?;
    let outcome = runtime.block_on(consume(
        jetstream,
        oms.nats.stream(oms.symbol),
        book,
        start_seq,
        side,
        Control::new(&config, &mode, &shutdown, mode_rx),
    ))?;
    drop(runtime);

    match outcome {
        ConsumeOutcome::Shutdown => {
            info!("the slave consume loop stopped");
            Ok(())
        }
        ConsumeOutcome::Promoted => {
            info!("the slave is promoted to a master");
            super::master::run(config, mode, shutdown)
        }
    }
}

/// Recovers the book and the journal, and resolves the stream sequence the
/// consume loop resumes from. The sinks are those the config selects: the
/// journal is authoritative over the Redis snapshot; a missing or corrupt
/// journal falls back to Redis when Redis is a configured persistence
/// target, then to a fresh book.
fn recover(oms: &OmsConfig) -> Result<(OrderBook, Option<Journal>, u64), EngineError> {
    let persist = oms.snapshot.persist;
    let mut book = OrderBook::new(oms.book_config());
    let mut journal = match persist {
        SnapshotPersist::Journal | SnapshotPersist::Both => {
            Some(Journal::open(&oms.journal.path, oms.journal.size)?)
        }
        SnapshotPersist::Redis => None,
    };
    let recovered = match journal.as_mut() {
        Some(journal) => journal.recover()?,
        None => None,
    };
    let start_seq = match recovered {
        Some(bytes) => {
            let snapshot =
                Snapshot::decode(&bytes).map_err(|err| EngineError::Codec(err.to_string()))?;
            info!(seq = snapshot.seq, "journal snapshot loaded");
            book.restore_state(snapshot.state);
            snapshot.seq
        }
        None => {
            if persist.includes_redis() {
                if journal.is_some() {
                    info!("no journal snapshot, falling back to the Redis snapshot");
                }
                load_redis_snapshot(oms, &mut book).unwrap_or(0)
            } else {
                info!("no journal snapshot, starting from a fresh book");
                0
            }
        }
    };
    // The slave maintains the book statistics the master skips.
    book.enable_statistics();
    info!(seq = start_seq, "recovery done");
    Ok((book, journal, start_seq))
}

/// Loads the Redis snapshot into the book, returning its checkpoint
/// sequence. Failures degrade to a fresh book (the NATS replay rebuilds the
/// state), they never abort the recovery.
fn load_redis_snapshot(oms: &OmsConfig, book: &mut OrderBook) -> Option<u64> {
    let config = oms.redis.as_ref()?;
    let mut store = match RedisStore::connect(
        config,
        naming::redis_change_channel(oms.symbol),
        naming::redis_snapshot_key(oms.symbol),
    ) {
        Ok(store) => store,
        Err(err) => {
            warn!(error = %err, "cannot connect to Redis for the snapshot recovery");
            return None;
        }
    };
    match store.load_snapshot() {
        Ok(Some(bytes)) => match Snapshot::decode(&bytes) {
            Ok(snapshot) => {
                info!(seq = snapshot.seq, "Redis snapshot loaded");
                book.restore_state(snapshot.state);
                Some(snapshot.seq)
            }
            Err(err) => {
                warn!(error = %err, "cannot decode the Redis snapshot");
                None
            }
        },
        Ok(None) => {
            info!("no Redis snapshot found, starting from a fresh book");
            None
        }
        Err(err) => {
            warn!(error = %err, "cannot load the Redis snapshot");
            None
        }
    }
}

/// Shared control state the consume loop observes: the live config, the
/// mode flag with its watch channel, and the shutdown flag. Grouping them
/// keeps the loop's signature small.
struct Control {
    /// The live config, for the snapshot cadence.
    config: Arc<RwLock<OmsConfig>>,
    /// The mode flag.
    mode: Arc<AtomicU8>,
    /// The shutdown flag.
    shutdown: Arc<AtomicBool>,
    /// The watch channel announcing mode flips.
    mode_rx: tokio::sync::watch::Receiver<u8>,
}

impl Control {
    /// Clones the shared handles from the runtime.
    fn new(
        config: &Arc<RwLock<OmsConfig>>,
        mode: &Arc<AtomicU8>,
        shutdown: &Arc<AtomicBool>,
        mode_rx: tokio::sync::watch::Receiver<u8>,
    ) -> Self {
        Self {
            config: Arc::clone(config),
            mode: Arc::clone(mode),
            shutdown: Arc::clone(shutdown),
            mode_rx,
        }
    }
}

/// The consume loop: pulls the replication messages, applies them to the
/// book, dispatches them to the side I/O thread, and takes snapshots on the
/// configured cadence. A mode flip on the watch channel, or a shutdown
/// observed by the control poll, ends the loop.
async fn consume(
    jetstream: jetstream::Context,
    stream_name: String,
    mut book: OrderBook,
    start_seq: u64,
    side: SideIo,
    mut control: Control,
) -> Result<ConsumeOutcome, EngineError> {
    let stream = jetstream
        .get_stream(&stream_name)
        .await
        .map_err(|err| EngineError::Nats(err.to_string()))?;
    let consumer = stream
        .get_or_create_consumer(
            "svd_oms_slave",
            jetstream::consumer::pull::Config {
                deliver_policy: if start_seq == 0 {
                    jetstream::consumer::DeliverPolicy::All
                } else {
                    jetstream::consumer::DeliverPolicy::ByStartSequence {
                        start_sequence: start_seq.saturating_add(1),
                    }
                },
                ..Default::default()
            },
        )
        .await
        .map_err(|err| EngineError::Nats(err.to_string()))?;
    let mut messages =
        consumer.messages().await.map_err(|err| EngineError::Nats(err.to_string()))?;

    let mut last_applied = start_seq;
    let mut last_snapshot = Instant::now();
    let mut outcome = ConsumeOutcome::Shutdown;
    loop {
        let message = tokio::select! {
            biased;
            // The mode flip announces the promotion: drop the stream and
            // enter the master loop. A closed channel means the runtime is
            // gone; shut down.
            changed = control.mode_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                if control.mode.load(Ordering::Relaxed) != MODE_SLAVE {
                    outcome = ConsumeOutcome::Promoted;
                    break;
                }
                continue;
            }
            message = messages.next() => match message {
                Some(message) => message,
                None => {
                    error!("the NATS message stream ended unexpectedly");
                    break;
                }
            },
            _ = tokio::time::sleep(CONTROL_POLL) => {
                if control.shutdown.load(Ordering::Relaxed) {
                    break;
                }
                if control.mode.load(Ordering::Relaxed) != MODE_SLAVE {
                    outcome = ConsumeOutcome::Promoted;
                    break;
                }
                continue;
            }
        };

        let message = match message {
            Ok(message) => message,
            Err(err) => {
                error!(error = %err, "the NATS message stream failed");
                break;
            }
        };
        let info = message.info().map_err(|err| EngineError::Nats(err.to_string()))?;
        let seq = info.stream_sequence;

        // The ack is sent after the apply, so a crash redelivers the
        // message; the sequence dedupe makes the replay idempotent.
        if seq <= last_applied {
            if let Err(err) = message.ack().await {
                warn!(error = %err, seq = seq, "cannot ack a redelivered message");
            }
            continue;
        }

        let replicated: PooledReplicationMsg = rmp_serde::from_slice(&message.payload)
            .map_err(|err| EngineError::Codec(err.to_string()))?;
        book.apply(&replicated).expect("the book apply is infallible");
        last_applied = seq;

        // The side I/O thread publishes the raw payload to Redis; the
        // channel send never blocks.
        side.publish_change(&message.payload);

        // Snapshots on the configured cadence. The interval is read from
        // the live config, so a reload tunes it without a restart.
        let interval_ms = control.config.read().expect("config lock").snapshot.interval_ms;
        if last_snapshot.elapsed().as_millis() as u64 >= interval_ms {
            let state = book.snapshot_state().clone();
            side.snapshot(seq, state);
            last_snapshot = Instant::now();
        }

        if let Err(err) = message.ack().await {
            warn!(error = %err, seq = seq, "cannot ack the message, it will be redelivered");
        }
    }
    Ok(outcome)
}

/// Tasks of the slave's side I/O thread.
enum SideTask {
    /// A replicated change payload for the Redis cluster.
    Change(Vec<u8>),
    /// A book snapshot for the configured persistence sinks. The snapshot
    /// is boxed: the state is far larger than the change payloads the
    /// channel mostly carries.
    Snapshot(Box<Snapshot>),
}

/// The non-blocking handle of the side I/O thread.
struct SideIo {
    sender: std::sync::mpsc::Sender<SideTask>,
}

impl SideIo {
    /// Spawns the side I/O thread: it takes over the journal (when the
    /// persistence target includes it) and connects to the Redis cluster
    /// when configured.
    fn spawn(oms: &OmsConfig, journal: Option<Journal>) -> Result<Self, EngineError> {
        let (sender, receiver) = std::sync::mpsc::channel::<SideTask>();
        let persist = oms.snapshot.persist;
        let redis_config = oms.redis.clone();
        let channel = naming::redis_change_channel(oms.symbol);
        let snapshot_key = naming::redis_snapshot_key(oms.symbol);
        std::thread::Builder::new()
            .name("oms-side-io".to_string())
            .spawn(move || {
                side_loop(receiver, journal, persist, redis_config, channel, snapshot_key)
            })
            .map_err(|err| {
                EngineError::Runtime(format!("cannot spawn the side io thread: {err}"))
            })?;
        Ok(Self { sender })
    }

    /// Queues a change payload for the Redis publication.
    fn publish_change(&self, payload: &[u8]) {
        let _ = self.sender.send(SideTask::Change(payload.to_vec()));
    }

    /// Queues a snapshot of the book at the given stream sequence.
    fn snapshot(&self, seq: u64, state: OrderBookState) {
        let _ = self.sender.send(SideTask::Snapshot(Box::new(Snapshot::new(seq, state))));
    }
}

/// The side I/O loop: publishes the changes, persists the snapshots to the
/// configured sinks. Every failure degrades gracefully — the loop logs and
/// keeps going, the recovery falls back to the NATS replay.
fn side_loop(
    receiver: std::sync::mpsc::Receiver<SideTask>,
    mut journal: Option<Journal>,
    persist: SnapshotPersist,
    redis_config: Option<storage::RedisConfig>,
    channel: String,
    snapshot_key: String,
) {
    let mut store = redis_config.as_ref().and_then(|config| {
        match RedisStore::connect(config, channel, snapshot_key) {
            Ok(store) => Some(store),
            Err(err) => {
                warn!(error = %err, "cannot connect to Redis, the change publication is disabled");
                None
            }
        }
    });
    if persist.includes_redis() && store.is_none() {
        warn!("the Redis snapshot persistence is enabled but Redis is not connected");
    }
    while let Ok(task) = receiver.recv() {
        match task {
            SideTask::Change(payload) => {
                if let Some(store) = &mut store
                    && let Err(err) = store.publish_change(&payload)
                {
                    warn!(error = %err, "cannot publish a change to Redis");
                }
            }
            SideTask::Snapshot(snapshot) => match snapshot.encode() {
                Ok(bytes) => {
                    if persist.includes_journal()
                        && let Some(journal) = &mut journal
                        && let Err(err) = journal.write(&bytes)
                    {
                        warn!(error = %err, "cannot write the snapshot to the journal");
                    }
                    if persist.includes_redis()
                        && let Some(store) = &mut store
                        && let Err(err) = store.save_snapshot(&bytes)
                    {
                        warn!(error = %err, "cannot save the snapshot to Redis");
                    }
                }
                Err(err) => {
                    error!(error = %err, "cannot encode the snapshot");
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::address::Address;
    use primitives::base::{Hash32, Nonce, Side, Symbol};
    use primitives::message::hot_path::OrderMsg;
    use primitives::message::side_path::ReplicationMsg;
    use primitives::order::{Order, OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use primitives::orderbook::listener::Listeners;
    use primitives::signature::Signature;
    use primitives::time_in_force::TimeInForce;
    use primitives::value::{Price, Quantity, TimestampMs};
    use std::cell::RefCell;
    use std::rc::Rc;

    fn addr(user: u8) -> Address {
        Address([user; 20])
    }

    fn order(user: u8, nonce: u64, price: u64, quantity: u64, side: Side) -> Order {
        Order::new(
            OrderHot {
                user: addr(user),
                nonce: Nonce(nonce),
                price: Price(price),
                quantity: Quantity(quantity),
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

    fn config() -> OmsConfig {
        OmsConfig::from_toml(
            r#"
mode = "slave"
symbol = "TEST"
[ingress]
path = "/tmp/q"
capacity = 1024
[settlement]
path = "/tmp/s"
capacity = 1024
[book.hot]
stp_mode = "cancel_both"
"#,
        )
        .unwrap()
    }

    /// The apply pipeline: decodes one wire payload and applies it to the
    /// book with the sequence dedupe, mirroring the consume loop.
    fn apply_payload(
        book: &mut OrderBook,
        seq: u64,
        last_applied: &mut u64,
        bytes: &[u8],
    ) -> Result<bool, EngineError> {
        if seq <= *last_applied {
            return Ok(false);
        }
        let replicated: PooledReplicationMsg =
            rmp_serde::from_slice(bytes).map_err(|err| EngineError::Codec(err.to_string()))?;
        book.apply(&replicated).expect("the book apply is infallible");
        *last_applied = seq;
        Ok(true)
    }

    /// A unique path under the system temp dir.
    fn temp_path(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dex_oms_slave_{}_{}_{}", std::process::id(), tag, seq))
    }

    /// Removes the file when dropped, so failed tests don't litter the
    /// temp dir.
    struct TempFile(std::path::PathBuf);

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// Spawns the side loop without a Redis cluster and returns its
    /// channel and thread.
    fn run_side_loop(
        journal: Option<Journal>,
        persist: SnapshotPersist,
    ) -> (std::sync::mpsc::Sender<SideTask>, std::thread::JoinHandle<()>) {
        let (sender, receiver) = std::sync::mpsc::channel::<SideTask>();
        let handle = std::thread::spawn(move || {
            side_loop(receiver, journal, persist, None, String::new(), String::new())
        });
        (sender, handle)
    }

    #[test]
    fn test_slave_replays_the_master_stream() {
        // A master / slave pair mirroring the primitives replication test:
        // the master captures its replication stream, the slave applies the
        // wire payloads through the same decode path the consume loop uses.
        let book_config = config().book_config();
        let captured: Rc<RefCell<Vec<Vec<u8>>>> = Rc::new(RefCell::new(Vec::new()));
        let capture = Rc::clone(&captured);
        let mut master = OrderBook::new(book_config.clone()).with_listeners(
            Listeners::default().with_book_state_listener(Box::new(move |msg| {
                let owned =
                    ReplicationMsg::new(msg.iter().cloned().collect(), msg.last_trade_price());
                capture.borrow_mut().push(rmp_serde::to_vec(&owned).unwrap());
            })),
        );
        let mut slave = OrderBook::new(book_config.clone());
        // Statistics are a slave-side concern, exercised in the dedupe
        // test; this test asserts the pure replication of the book state.

        let steps = [
            OrderMsg::NewOrder(order(2, 1, 100, 40, Side::Sell)),
            OrderMsg::NewOrder(order(3, 1, 101, 30, Side::Sell)),
            // Partial fill: the taker rests 10 at 100.
            OrderMsg::NewOrder(order(1, 1, 100, 80, Side::Buy)),
            // STP CancelBoth: the same-user self-trade kills both orders.
            OrderMsg::NewOrder(order(1, 2, 100, 10, Side::Sell)),
            OrderMsg::NewOrder(order(1, 3, 100, 10, Side::Buy)),
            // Cancel the resting taker of the third step.
            OrderMsg::CancelOrder(primitives::message::hot_path::CancelOrder::new(
                Symbol([0; 32]),
                Hash32([0; 32]),
                addr(1),
                Nonce(1),
                TimestampMs(0),
                Signature::default(),
            )),
        ];
        for msg in &steps {
            master.execute(msg).expect("master executes");
            let payloads = std::mem::take(&mut *captured.borrow_mut());
            let mut last_applied = 0u64;
            for (index, payload) in payloads.iter().enumerate() {
                let seq = index as u64 + 1;
                assert!(apply_payload(&mut slave, seq, &mut last_applied, payload).unwrap());
            }
        }
        // The slave book converged to the master book.
        assert_eq!(
            rmp_serde::to_vec(master.snapshot_state()).unwrap(),
            rmp_serde::to_vec(slave.snapshot_state()).unwrap()
        );
    }

    #[test]
    fn test_dedupe_skips_redeliveries() {
        let config = config().book_config();
        let mut slave = OrderBook::new(config.clone());
        slave.enable_statistics();

        let mut last_applied = 0u64;
        let payload = {
            let master = OrderBook::new(config);
            let captured: Rc<RefCell<Vec<Vec<u8>>>> = Rc::new(RefCell::new(Vec::new()));
            let capture = Rc::clone(&captured);
            let master = master.with_listeners(Listeners::default().with_book_state_listener(
                Box::new(move |msg| {
                    let owned =
                        ReplicationMsg::new(msg.iter().cloned().collect(), msg.last_trade_price());
                    capture.borrow_mut().push(rmp_serde::to_vec(&owned).unwrap());
                }),
            ));
            let mut master = master;
            master.execute(&OrderMsg::NewOrder(order(2, 1, 100, 40, Side::Sell))).unwrap();
            let payloads = std::mem::take(&mut *captured.borrow_mut());
            assert!(apply_payload(&mut slave, 1, &mut last_applied, &payloads[0]).unwrap());
            payloads[0].clone()
        };
        // The redelivery of the same sequence is skipped: the book state
        // does not change.
        let before = rmp_serde::to_vec(slave.snapshot_state()).unwrap();
        assert!(!apply_payload(&mut slave, 1, &mut last_applied, &payload).unwrap());
        assert_eq!(rmp_serde::to_vec(slave.snapshot_state()).unwrap(), before);
        // An older sequence is skipped too (gap-less consumer ordering).
        assert!(!apply_payload(&mut slave, 0, &mut last_applied, &payload).unwrap());
    }

    #[test]
    fn test_snapshot_roundtrip_through_the_journal() {
        // The snapshot the side I/O thread persists is exactly what the
        // recovery loads back.
        let book_config = config().book_config();
        let mut book = OrderBook::new(book_config);
        book.execute(&OrderMsg::NewOrder(order(2, 1, 100, 40, Side::Sell))).unwrap();

        let path = temp_path("snapshot");
        let _guard = TempFile(path.clone());
        let mut journal = Journal::open(&path, 4096).unwrap();
        let snapshot = Snapshot::new(7, book.snapshot_state().clone());
        journal.write(&snapshot.encode().unwrap()).unwrap();

        let bytes = journal.recover().unwrap().unwrap();
        let restored = Snapshot::decode(&bytes).unwrap();
        assert_eq!(restored.seq, 7);
        let mut recovered = OrderBook::new(config().book_config());
        recovered.restore_state(restored.state);
        assert_eq!(
            rmp_serde::to_vec(recovered.snapshot_state()).unwrap(),
            rmp_serde::to_vec(book.snapshot_state()).unwrap()
        );
    }

    #[test]
    fn test_recover_opens_the_journal_when_selected() {
        let mut oms = config();
        oms.snapshot.persist = SnapshotPersist::Journal;
        oms.journal.path = temp_path("recover_journal");
        let _guard = TempFile(oms.journal.path.clone());
        let (_book, journal, seq) = recover(&oms).unwrap();
        assert!(journal.is_some(), "the journal target opens the journal");
        assert_eq!(seq, 0);
    }

    #[test]
    fn test_recover_skips_the_journal_when_redis_only() {
        let mut oms = config();
        oms.snapshot.persist = SnapshotPersist::Redis;
        oms.redis = None;
        let (_book, journal, seq) = recover(&oms).unwrap();
        assert!(journal.is_none(), "the redis-only target never opens the journal");
        assert_eq!(seq, 0);
    }

    #[test]
    fn test_side_loop_persists_snapshots_to_the_selected_journal() {
        let path = temp_path("side_journal");
        let _guard = TempFile(path.clone());
        let journal = Journal::open(&path, 4096).unwrap();
        let (sender, handle) = run_side_loop(Some(journal), SnapshotPersist::Journal);

        let mut book = OrderBook::new(config().book_config());
        book.execute(&OrderMsg::NewOrder(order(2, 1, 100, 40, Side::Sell))).unwrap();
        sender
            .send(SideTask::Snapshot(Box::new(Snapshot::new(9, book.snapshot_state().clone()))))
            .unwrap();
        drop(sender);
        handle.join().unwrap();

        // The snapshot landed in the journal the loop owned.
        let mut journal = Journal::open(&path, 4096).unwrap();
        let restored = Snapshot::decode(&journal.recover().unwrap().unwrap()).unwrap();
        assert_eq!(restored.seq, 9);
    }

    #[test]
    fn test_side_loop_skips_sinks_that_are_not_selected() {
        // Redis-only persistence with no Redis configured: the opened
        // journal file stays untouched.
        let path = temp_path("side_skip");
        let _guard = TempFile(path.clone());
        let journal = Journal::open(&path, 4096).unwrap();
        let (sender, handle) = run_side_loop(Some(journal), SnapshotPersist::Redis);

        let book = OrderBook::new(config().book_config());
        sender
            .send(SideTask::Snapshot(Box::new(Snapshot::new(3, book.snapshot_state().clone()))))
            .unwrap();
        drop(sender);
        handle.join().unwrap();

        let mut journal = Journal::open(&path, 4096).unwrap();
        assert_eq!(journal.recover().unwrap(), None);
    }
}
