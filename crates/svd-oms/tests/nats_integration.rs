//! End-to-end test of the OMS master and slave over a real NATS JetStream
//! server: the master executes the ingress requests, the slave consumes the
//! replication stream, applies the changes and snapshots the book to its
//! journal. The snapshot is then recovered and compared against a reference
//! book built through the same replication path.
//!
//! Ignored by default (CI has no NATS). Run with a local JetStream server:
//!
//! ```text
//! nats-server -js &
//! cargo test -p svd-oms --test nats_integration -- --ignored --nocapture
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use primitives::address::Address;
use primitives::base::{Hash32, Nonce, Side, Symbol};
use primitives::message::hot_path::{CancelOrder, OrderMsg};
use primitives::message::side_path::ReplicationMsg;
use primitives::order::{Order, OrderCold, OrderColdCommon, OrderHot, OrderKind};
use primitives::orderbook::book::OrderBook;
use primitives::orderbook::listener::Listeners;
use primitives::signature::Signature;
use primitives::time_in_force::TimeInForce;
use primitives::value::{Price, Quantity, TimestampMs};
use svd_oms::config::OmsConfig;
use svd_oms::engine::{MODE_SLAVE, master, slave};
use svd_oms::journal::Journal;
use svd_oms::snapshot::Snapshot;

/// The NATS url the local test server listens on.
const NATS_URL: &str = "nats://127.0.0.1:4222";

/// Removes the temp files when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("dex_oms_nats_{}_{}", std::process::id(), tag));
        std::fs::create_dir_all(&dir).expect("temp dir");
        Self(dir)
    }

    fn path(&self, name: &str) -> String {
        self.0.join(name).to_string_lossy().into_owned()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

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

/// Builds a config for the mode, bound to a unique symbol so repeated runs
/// never share a stream on the dev server.
fn config(mode: &str, symbol: &str, dir: &TempDir, ingress_create: bool) -> OmsConfig {
    OmsConfig::from_toml(&format!(
        r#"
mode = "{mode}"
symbol = "{symbol}"
[nats]
urls = ["{NATS_URL}"]
[ingress]
path = "{ingress}"
capacity = 65536
create = {ingress_create}
[settlement]
path = "{settlement}"
capacity = 65536
create = false
[snapshot]
interval_ms = 100
[journal]
path = "{journal}"
size = 1048576
"#,
        ingress = dir.path("ingress.queue"),
        settlement = dir.path("settlement.queue"),
        journal = dir.path("journal.bin"),
    ))
    .unwrap()
}

/// The request stream both engines process: resting makers, a partial
/// fill, an STP pair and a cancel. Every execution emits one replication
/// message, so the stream sequence after the last one is the step count.
fn steps() -> Vec<OrderMsg> {
    vec![
        OrderMsg::NewOrder(order(2, 1, 100, 40, Side::Sell)),
        OrderMsg::NewOrder(order(3, 1, 101, 30, Side::Sell)),
        OrderMsg::NewOrder(order(1, 1, 100, 80, Side::Buy)),
        OrderMsg::NewOrder(order(1, 2, 100, 10, Side::Sell)),
        OrderMsg::NewOrder(order(1, 3, 100, 10, Side::Buy)),
        OrderMsg::CancelOrder(CancelOrder::new(
            Symbol([0; 32]),
            Hash32([0; 32]),
            addr(1),
            Nonce(1),
            TimestampMs(0),
            Signature::default(),
        )),
    ]
}

/// Builds the reference book the slave must converge to: the same steps
/// executed on a master and applied to a statistics-enabled book through
/// the same decode path the consume loop uses. The book carries the same
/// symbol as the engines, so the serialized states are comparable.
fn reference_book(steps: &[OrderMsg], symbol: &str) -> Vec<u8> {
    let book_config = primitives::orderbook::config::BookConfig::default().with_cold(
        primitives::orderbook::config::BookConfigCold::default()
            .with_symbol(svd_oms::config::parse_symbol(symbol).unwrap()),
    );
    // Replay through the primitives replication path.
    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    let capture = Arc::clone(&captured);
    let closure_capture = Arc::clone(&capture);
    let mut master = OrderBook::new(book_config.clone()).with_listeners(
        Listeners::default().with_book_state_listener(Box::new(move |msg| {
            let owned = ReplicationMsg::new(msg.iter().cloned().collect(), msg.last_trade_price());
            closure_capture.lock().unwrap().push(rmp_serde::to_vec(&owned).unwrap());
        })),
    );
    let mut slave = OrderBook::new(book_config);
    slave.enable_statistics();
    for step in steps {
        master.execute(step).unwrap();
        for payload in std::mem::take(&mut *capture.lock().unwrap()) {
            let replicated: primitives::orderbook::listener::PooledReplicationMsg =
                rmp_serde::from_slice(&payload).unwrap();
            slave.apply(&replicated).unwrap();
        }
    }
    rmp_serde::to_vec(slave.snapshot_state()).unwrap()
}

#[test]
#[ignore = "requires a local nats-server -js on 127.0.0.1:4222"]
fn test_master_slave_replication_and_journal_recovery() {
    // A unique symbol per run so the dev server's stream starts empty.
    let unique = AtomicUsize::new(0);
    let symbol = format!("NATS{}{}", std::process::id(), unique.fetch_add(1, Ordering::Relaxed));
    let dir = TempDir::new("e2e");
    let steps = steps();
    let expected_seq = steps.len() as u64;
    let reference = reference_book(&steps, &symbol);

    // The SPSC queue files: the test owns them, the engines open them
    // without create (mirroring the production wiring).
    let master_config = config("master", &symbol, &dir, false);
    let slave_config = config("slave", &symbol, &dir, false);
    ipc::mmap_spsc_fixed::SpscQueue::<OrderMsg>::open(&master_config.ingress.path, 16, true)
        .unwrap();
    ipc::mmap_spsc_fixed::SpscQueue::<OrderMsg>::open(&master_config.settlement.path, 16, true)
        .unwrap();

    // Shared runtime handles, mirroring `Runtime` without its core thread.
    let mode = Arc::new(AtomicU8::new(MODE_SLAVE));
    let shutdown = Arc::new(AtomicBool::new(false));
    let (mode_tx, mode_rx) = tokio::sync::watch::channel(MODE_SLAVE);

    // The journal handles the test polls later, extracted before the config
    // moves into the slave's shared state.
    let journal_path = slave_config.journal.path.clone();
    let journal_size = slave_config.journal.size;
    let slave_config_arc = Arc::new(std::sync::RwLock::new(slave_config));
    let slave_mode = Arc::clone(&mode);
    let slave_shutdown = Arc::clone(&shutdown);
    let slave_handle = std::thread::spawn(move || {
        slave::run(slave_config_arc, slave_mode, slave_shutdown, mode_rx).unwrap();
    });

    let master_config_arc = Arc::new(std::sync::RwLock::new(master_config.clone()));
    let master_mode = Arc::clone(&mode);
    let master_shutdown = Arc::clone(&shutdown);
    let master_handle = std::thread::spawn(move || {
        master::run(master_config_arc, master_mode, master_shutdown).unwrap();
    });

    // Feed the ingress queue once the master's core loop is up.
    let ingress_path = master_config.ingress.path.clone();
    let producer = std::thread::spawn(move || {
        // The master reinitializes nothing (create=false), but the core
        // loop needs a moment to open the queue; retry pushes until the
        // queue accepts them.
        let mut queue =
            ipc::mmap_spsc_fixed::SpscQueue::<OrderMsg>::open(&ingress_path, 16, false).unwrap();
        let mut pending = steps.clone();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !pending.is_empty() && Instant::now() < deadline {
            let pushed = queue.push_batch(&pending);
            pending.drain(..pushed);
            if !pending.is_empty() {
                std::thread::yield_now();
            }
        }
        assert!(pending.is_empty(), "the ingress queue never drained");
    });
    producer.join().unwrap();

    // Wait until the slave journaled a snapshot covering all messages.
    let deadline = Instant::now() + Duration::from_secs(30);
    let snapshot = loop {
        let mut journal = match Journal::open(&journal_path, journal_size) {
            Ok(journal) => journal,
            Err(_) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            Err(err) => panic!("cannot open the journal: {err}"),
        };
        if let Some(bytes) = journal.recover().expect("recover") {
            let snapshot: Snapshot = rmp_serde::from_slice(&bytes).expect("snapshot decodes");
            if snapshot.seq >= expected_seq {
                break snapshot;
            }
        }
        assert!(Instant::now() < deadline, "the slave never snapshotted the full stream");
        std::thread::sleep(Duration::from_millis(100));
    };

    // The snapshot covers the whole stream and matches the reference book.
    assert_eq!(snapshot.seq, expected_seq);
    assert_eq!(
        rmp_serde::to_vec(&snapshot.state).unwrap(),
        reference,
        "the replicated book diverged from the reference"
    );

    // Clean shutdown: the slave stops, the master stops.
    shutdown.store(true, Ordering::Relaxed);
    let _ = mode_tx.send(MODE_SLAVE);
    slave_handle.join().unwrap();
    master_handle.join().unwrap();

    // The journal still holds the snapshot after the engines are gone.
    let mut journal = Journal::open(&journal_path, journal_size).unwrap();
    let bytes = journal.recover().unwrap().expect("snapshot survives the shutdown");
    let restored: Snapshot = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(restored.seq, expected_seq);
}
