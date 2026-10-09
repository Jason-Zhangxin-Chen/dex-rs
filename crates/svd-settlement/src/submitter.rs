//! The submitters: one side thread per operator key drives its own batch
//! queue through the chain state machine, classifies the failures and hands
//! the terminal outcomes to the publishers.
//!
//! One submitter owns one chain: the submissions and the polls run inline
//! in the drive loop, so the nonce order equals the batch order and two
//! batches never submit concurrently. The queue frames are consumed one at
//! a time and acked only after every batch of the frame — the batches it
//! split into included — is terminal and its result is published to the
//! Redis cluster (the publisher confirms each write). The unacked frame
//! stays in the file-mapped queue and survives a crash of this process; a
//! crash before the ack re-processes the frame, which may re-submit an
//! already-settled batch — the accepted at-least-once trade-off of the
//! stateless protocol.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use ipc::mmap_spsc_var::{ByteSpscQueue, FRAME_HEADER_SIZE};
use primitives::base::{Hash32, Symbol};
use primitives::message::settlement::SettlementResult;
use tracing::{error, info, warn};

use crate::batch::{Batch, BatchState, backoff_for};
use crate::calldata::encode_settle_batch;
use crate::chain::{ChainClient, ChainError, TxState};
use crate::classifier::{Action, RetryReason, classify, plan_isolation};
use crate::config::RetryConfig;
use crate::core::{BatchFrame, decode_frame};
use crate::seq::SeqFile;

/// The poll idle of the submitter loop, in milliseconds: the loop wakes
/// this often even when nothing is pending.
const IDLE_SLEEP_MS: u64 = 1;

/// The config the submitter (and the batch machine) needs.
#[derive(Debug, Clone)]
pub struct SubmitterConfig {
    /// The cadence of the pending transaction polls, in milliseconds.
    pub poll_interval_ms: u64,
    /// The confirmation depth of a settled transaction.
    pub confirm_depth: u64,
    /// The grace before a `Submitting` transaction never seen by the chain
    /// is re-submitted, in milliseconds.
    pub tx_lost_grace_ms: u64,
    /// The retry policy.
    pub retry: RetryConfig,
}

/// The terminal outcome channels of the submitter, shared by the pool.
#[derive(Debug, Clone)]
pub struct SubmitChannels {
    /// (result, confirm) → the Redis publisher; the publisher sends `()`
    /// on the confirm sender after the Redis write succeeds.
    pub publisher: mpsc::Sender<(SettlementResult, mpsc::Sender<()>)>,
    /// Terminal outcomes → the SQL writer.
    pub sql: mpsc::Sender<SettlementResult>,
}

/// The chain constructor of one submitter: it runs on the submitter thread,
/// which builds its own tokio runtime (kept alive for the thread's
/// lifetime) and the chain client on it.
pub type ChainBuilder =
    Box<dyn FnOnce() -> Result<Arc<dyn ChainClient + Send + Sync>, ChainError> + Send>;

/// Spawns one submitter thread: it consumes its own batch queue one frame
/// at a time and drives the batches until the shutdown. The `alive` counter
/// reports this submitter to the core thread and decrements on exit.
#[allow(clippy::too_many_arguments)]
pub fn spawn(
    queue: ByteSpscQueue,
    build_chain: ChainBuilder,
    seq: Arc<SeqFile>,
    symbol: Symbol,
    out: SubmitChannels,
    cfg: SubmitterConfig,
    shutdown: Arc<AtomicBool>,
    alive: Arc<AtomicUsize>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("stl-submitter".to_string())
        .spawn(move || run(queue, build_chain, seq, symbol, out, cfg, shutdown, alive))
}

/// Reports the submitter's exit to the core thread's liveness counter.
struct AliveGuard(Arc<AtomicUsize>);

impl Drop for AliveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

/// The submitter loop: one frame in flight at a time, acked only when every
/// batch of it is terminal and published.
#[allow(clippy::too_many_arguments)]
fn run(
    mut queue: ByteSpscQueue,
    build_chain: ChainBuilder,
    seq: Arc<SeqFile>,
    symbol: Symbol,
    out: SubmitChannels,
    cfg: SubmitterConfig,
    shutdown: Arc<AtomicBool>,
    alive: Arc<AtomicUsize>,
) {
    let _guard = AliveGuard(alive);
    let chain = match build_chain() {
        Ok(chain) => chain,
        Err(err) => {
            error!(error = %err, "cannot build the chain client, the submitter stops");
            return;
        }
    };
    // The single in-flight frame and its batches. The frame stays unacked
    // until every batch of it is terminal and published.
    let mut frame_buf: Vec<u8> = Vec::new();
    let mut frame_len: Option<usize> = None;
    let mut pending: VecDeque<Batch> = VecDeque::new();
    let mut pending_acks: Vec<mpsc::Receiver<()>> = Vec::new();
    let poll = Duration::from_millis(cfg.poll_interval_ms);
    let mut next_drive = Instant::now();
    info!("the submitter loop started");
    loop {
        // The next frame when nothing is in flight. The shutdown exits
        // before popping: the frames still queued survive the restart.
        if pending.is_empty() && frame_len.is_none() {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            match queue.peek(&mut frame_buf) {
                Ok(Some(len)) => {
                    frame_len = Some(len);
                    let frame: BatchFrame = match decode_frame(&frame_buf[..len]) {
                        Ok(frame) => frame,
                        Err(err) => {
                            error!(error = %err, "cannot decode the batch frame, the submitter stops");
                            return;
                        }
                    };
                    let BatchFrame(seq, trades) = frame;
                    pending.push_back(Batch {
                        seq,
                        trades: trades.clone(),
                        state: BatchState::Received { trades },
                        attempts: 0,
                        next_attempt_at: None,
                        nonce: None,
                        submitted_at: None,
                    });
                    next_drive = Instant::now();
                    continue;
                }
                Ok(None) => {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(IDLE_SLEEP_MS));
                    continue;
                }
                Err(err) => {
                    error!(error = %err, "a corrupt batch frame, the submitter stops");
                    return;
                }
            }
        }
        // One state-machine step per pending batch per tick, in batch
        // order — the single owner of the chain.
        let now = Instant::now();
        if now >= next_drive {
            drive(&*chain, symbol, &out, &cfg, &seq, &mut pending, now, &mut pending_acks);
            next_drive = now + poll;
        }
        // Every batch of the frame is terminal and the Redis cluster has
        // the results: release the frame to the producer.
        if pending.is_empty()
            && frame_len.is_some()
            && pending_acks.iter().all(|rx| rx.recv().is_ok())
        {
            let frame_bytes = FRAME_HEADER_SIZE + frame_len.expect("the frame is in flight");
            if let Err(err) = queue.ack(frame_bytes) {
                error!(error = %err, "cannot ack the batch frame, the submitter stops");
                return;
            }
            frame_len = None;
            frame_buf.clear();
            pending_acks.clear();
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            continue;
        }
        std::thread::sleep(Duration::from_millis(IDLE_SLEEP_MS));
    }
    info!("the submitter loop stopped; unacked frames stay in the queue for the next start");
}

/// Advances every pending batch by one step; batches that still need
/// driving re-enter the queue, published terminal batches leave it.
#[allow(clippy::too_many_arguments)]
fn drive(
    chain: &dyn ChainClient,
    symbol: Symbol,
    out: &SubmitChannels,
    cfg: &SubmitterConfig,
    seq: &SeqFile,
    pending: &mut VecDeque<Batch>,
    now: Instant,
    pending_acks: &mut Vec<mpsc::Receiver<()>>,
) {
    let count = pending.len();
    for _ in 0..count {
        let mut batch = pending.pop_front().expect("the counted batch exists");
        if step(&mut batch, chain, symbol, out, cfg, seq, pending, now, pending_acks) {
            pending.push_back(batch);
        }
    }
}

/// Advances one batch by one step. Returns whether the batch still needs
/// driving.
#[allow(clippy::too_many_arguments)]
fn step(
    batch: &mut Batch,
    chain: &dyn ChainClient,
    symbol: Symbol,
    out: &SubmitChannels,
    cfg: &SubmitterConfig,
    seq: &SeqFile,
    pending: &mut VecDeque<Batch>,
    now: Instant,
    pending_acks: &mut Vec<mpsc::Receiver<()>>,
) -> bool {
    // A terminal batch has nothing left to drive (its outcome was published
    // by the transition that made it terminal).
    if batch.is_terminal() {
        return false;
    }
    // The backoff holds this batch back.
    if batch.next_attempt_at.is_some_and(|at| now < at) {
        return true;
    }
    match &batch.state {
        BatchState::Received { .. } => {
            submit_batch(batch, chain, cfg, now);
        }
        BatchState::Submitting { .. } => {
            // A transaction never seen on-chain past the grace is
            // re-submitted (the crash window between the build and the
            // broadcast).
            let submitted_at = *batch.submitted_at.get_or_insert(now);
            if now.duration_since(submitted_at) >= Duration::from_millis(cfg.tx_lost_grace_ms) {
                warn!(batch_seq = batch.seq, "the transaction was never seen, re-submitting");
                submit_batch(batch, chain, cfg, now);
                return true;
            }
            let tx = batch.state.tx().expect("a submitting batch carries a transaction");
            match chain.tx_state(tx) {
                Ok(state) => {
                    observe(batch, symbol, out, cfg, seq, pending, tx, state, now, pending_acks)
                }
                Err(err) => retryable_failure(batch, chain, cfg, &err, now),
            }
        }
        BatchState::Submitted { .. } => {
            let tx = batch.state.tx().expect("a submitted batch carries a transaction");
            match chain.tx_state(tx) {
                Ok(state) => {
                    observe(batch, symbol, out, cfg, seq, pending, tx, state, now, pending_acks)
                }
                Err(err) => retryable_failure(batch, chain, cfg, &err, now),
            }
        }
        BatchState::Confirmed { .. } | BatchState::Reverted { .. } | BatchState::Split { .. } => {
            // Unreachable: terminal batches return at the top of `step`.
        }
    }
    true
}

/// Submits one batch: encodes the calldata and sends it through the chain.
/// A failed submission backs off per the retry policy.
fn submit_batch(batch: &mut Batch, chain: &dyn ChainClient, cfg: &SubmitterConfig, now: Instant) {
    let calldata = match encode_settle_batch(&batch.trades) {
        Ok(calldata) => calldata,
        Err(err) => {
            // Deterministic encoding failure: never retryable.
            error!(error = %err, batch_seq = batch.seq, "cannot encode the batch");
            backoff(batch, cfg, false, now);
            return;
        }
    };
    match chain.submit(&calldata) {
        Ok(tx) => {
            batch.nonce = Some(tx.nonce);
            batch.submitted_at = Some(now);
            batch.state = BatchState::Submitting { tx: tx.tx_hash };
            batch.attempts = 0;
            batch.next_attempt_at = None;
        }
        Err(err) => {
            warn!(error = %err, batch_seq = batch.seq, "cannot submit the batch");
            backoff(batch, cfg, chain.is_retryable(&err), now);
        }
    }
}

/// Applies one observed chain state of a submitted transaction.
#[allow(clippy::too_many_arguments)]
fn observe(
    batch: &mut Batch,
    symbol: Symbol,
    out: &SubmitChannels,
    cfg: &SubmitterConfig,
    seq: &SeqFile,
    pending: &mut VecDeque<Batch>,
    tx: Hash32,
    state: TxState,
    now: Instant,
    pending_acks: &mut Vec<mpsc::Receiver<()>>,
) {
    match state {
        TxState::Pending => {
            // The transaction is in flight: remember the nonce when known.
            if let (BatchState::Submitting { .. }, Some(nonce)) = (&batch.state, batch.nonce) {
                batch.state = BatchState::Submitted { tx, nonce };
            }
        }
        TxState::Confirmed { block, depth } => {
            if depth >= cfg.confirm_depth {
                batch.state = BatchState::Confirmed { tx, block };
            }
        }
        TxState::Reverted { code, index, side } => {
            match classify(code, index, side, batch.trades.len()) {
                Action::Revert { failed_trade, at_fault, reason } => {
                    if batch.trades.len() == 1 {
                        batch.state = BatchState::Reverted { tx, failed_trade, at_fault, reason };
                    } else {
                        split(batch, seq, pending, code, index, side, tx);
                    }
                }
                Action::Retry { reason } => {
                    if reason == RetryReason::Unclassified {
                        // Page the human: the symbol's settlement stalls
                        // behind this batch until it resolves.
                        error!(
                            code,
                            index,
                            side,
                            batch_seq = batch.seq,
                            "an unclassifiable settlement revert, retrying"
                        );
                    }
                    backoff(batch, cfg, true, now);
                }
            }
        }
    }
    // A terminal transition publishes immediately.
    if batch.is_terminal()
        && let Some(result) = batch.outcome(symbol)
    {
        publish(out, result, pending_acks);
    }
}

/// Handles a failed chain interaction of a submitted transaction: a
/// retryable error backs off (with a fee-bump replacement after the
/// configured attempts), a non-retryable one alarms and retries at the
/// maximum cadence.
fn retryable_failure(
    batch: &mut Batch,
    chain: &dyn ChainClient,
    cfg: &SubmitterConfig,
    err: &ChainError,
    now: Instant,
) {
    if !chain.is_retryable(err) {
        error!(error = %err, batch_seq = batch.seq, "a non-retryable chain failure, retrying at the maximum cadence");
        backoff(batch, cfg, false, now);
        return;
    }
    batch.attempts += 1;
    warn!(error = %err, batch_seq = batch.seq, attempts = batch.attempts, "a chain failure");
    if batch.attempts >= cfg.retry.fee_bump_after
        && let (Some(nonce), Some(_)) = (batch.nonce, batch.state.tx())
    {
        let calldata = match encode_settle_batch(&batch.trades) {
            Ok(calldata) => calldata,
            Err(err) => {
                error!(error = %err, batch_seq = batch.seq, "cannot encode the batch");
                backoff(batch, cfg, false, now);
                return;
            }
        };
        match chain.bump_fee(&calldata, nonce) {
            Ok(tx) => {
                info!(batch_seq = batch.seq, nonce, tx = %tx.tx_hash.hex(), "fee-bump replacement sent");
                batch.nonce = Some(tx.nonce);
                batch.state = BatchState::Submitting { tx: tx.tx_hash };
                batch.next_attempt_at = None;
                return;
            }
            Err(bump_err) => {
                warn!(error = %bump_err, batch_seq = batch.seq, "cannot replace the transaction");
            }
        }
    }
    backoff(batch, cfg, true, now);
}

/// Splits a failing batch: the children enter the drive loop in submission
/// order (the clean halves, then the poison), each with its own sequence,
/// and the parent becomes terminal.
fn split(
    batch: &mut Batch,
    seq: &SeqFile,
    pending: &mut VecDeque<Batch>,
    code: u8,
    index: usize,
    side: u8,
    tx: Hash32,
) {
    let plan = plan_isolation(&batch.trades, code, index, side);
    for trades in plan.settle.iter().chain(std::iter::once(&plan.poison)) {
        pending.push_back(Batch {
            seq: seq.next(),
            trades: trades.clone(),
            state: BatchState::Received { trades: trades.clone() },
            attempts: 0,
            next_attempt_at: None,
            nonce: None,
            submitted_at: None,
        });
    }
    batch.state = BatchState::Split { tx, code, index, side };
}

/// Hands one terminal outcome to both publishers and registers the Redis
/// confirm: the submitter releases the frame only after the publisher
/// reports the write. The sends never block (unbounded channels) and the
/// threads retry until accepted.
fn publish(
    out: &SubmitChannels,
    result: SettlementResult,
    pending_acks: &mut Vec<mpsc::Receiver<()>>,
) {
    let batch_seq = result.batch_seq;
    let (ack_tx, ack_rx) = mpsc::channel();
    if out.publisher.send((result.clone(), ack_tx)).is_err() {
        error!(batch_seq, "the publisher is gone");
    }
    pending_acks.push(ack_rx);
    if out.sql.send(result).is_err() {
        error!(batch_seq, "the sql writer is gone");
    }
}

/// Schedules the next attempt of a batch with the configured backoff.
fn backoff(batch: &mut Batch, cfg: &SubmitterConfig, retryable: bool, now: Instant) {
    batch.attempts += 1;
    let attempt = batch.attempts.saturating_sub(1);
    let wait = if retryable {
        backoff_for(attempt, cfg.retry.base_ms, cfg.retry.max_ms)
    } else {
        cfg.retry.max_ms
    };
    // The jitter: ± jitter_pct% around the wait, deterministic per
    // (batch, attempt) so a re-processed frame lands on the same schedule.
    let jitter = jitter_pct(wait, cfg.retry.jitter_pct, batch.seq ^ u64::from(batch.attempts));
    batch.next_attempt_at = Some(now + Duration::from_millis(jitter));
}

/// A deterministic jitter around `wait` within ± `pct` percent, seeded.
fn jitter_pct(wait: u64, pct: u32, seed: u64) -> u64 {
    if pct == 0 {
        return wait;
    }
    let mut state = seed | 1; // xorshift: never zero
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    let swing = wait.saturating_mul(u64::from(pct)) / 100;
    wait.saturating_sub(swing).saturating_add(state % (2 * swing + 1))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Mutex};

    use primitives::address::Address;
    use primitives::base::{Hash32, Nonce, Side, Symbol};
    use primitives::message::hot_path::Trade;
    use primitives::message::settlement::{FaultSide, SettlementFailure, SettlementOutcome};
    use primitives::order::{Order, OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use primitives::signature::Signature;
    use primitives::time_in_force::TimeInForce;
    use primitives::value::{Price, Quantity, TimestampMs};

    use crate::chain::mock::MockChain;
    use crate::config::RetryConfig;
    use crate::seq::SeqFile;

    use super::*;

    /// Monotonic counter so tests running in parallel never collide on
    /// temp paths.
    static SEQ: AtomicUsize = AtomicUsize::new(0);

    struct TempFile(std::path::PathBuf);

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let seq = SEQ.fetch_add(1, AtomicOrdering::Relaxed);
        std::env::temp_dir().join(format!(
            "dex_stl_submitter_{}_{}_{}",
            std::process::id(),
            tag,
            seq
        ))
    }

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

    fn trade(user: u8, nonce: u64) -> Trade {
        Trade::new(
            order(user, nonce),
            Quantity(9),
            order(user.wrapping_add(1), nonce),
            Price(100),
            Quantity(1),
        )
    }

    /// `n` distinct trades, taker user 1, nonces `0..n`.
    fn trades(n: usize) -> Vec<Trade> {
        (0..n as u64).map(|nonce| trade(1, nonce)).collect()
    }

    /// The config of the drive-loop tests: poll every tick, no lost
    /// transaction grace, instant backoffs.
    fn cfg() -> SubmitterConfig {
        SubmitterConfig {
            poll_interval_ms: 1,
            confirm_depth: 1,
            tx_lost_grace_ms: 60_000,
            retry: RetryConfig { base_ms: 1, max_ms: 1, jitter_pct: 0, fee_bump_after: 3 },
        }
    }

    /// A result sink standing in for the publisher thread: it records the
    /// results and confirms every one immediately, so the tests do not need
    /// a Redis cluster.
    #[derive(Clone, Default)]
    struct ConfirmSink {
        results: Arc<Mutex<Vec<SettlementResult>>>,
    }

    impl ConfirmSink {
        fn spawn(&self, rx: mpsc::Receiver<(SettlementResult, mpsc::Sender<()>)>) {
            let results = Arc::clone(&self.results);
            std::thread::spawn(move || {
                while let Ok((result, confirm)) = rx.recv() {
                    results.lock().unwrap().push(result);
                    let _ = confirm.send(());
                }
            });
        }

        fn results(&self) -> Vec<SettlementResult> {
            self.results.lock().unwrap().clone()
        }
    }

    /// Opens the queue file, pushes one frame into it and drops the handle.
    fn prepare_queue(tag: &str, frame: &BatchFrame) -> (std::path::PathBuf, usize) {
        let path = temp_path(tag);
        let mut bytes = Vec::new();
        crate::core::encode_frame(frame.0, &frame.1, &mut bytes).unwrap();
        let capacity = bytes.len() + FRAME_HEADER_SIZE + 1024;
        {
            let mut queue = ByteSpscQueue::open(&path, capacity, true).unwrap();
            assert!(queue.push(&bytes));
        }
        (path, capacity)
    }

    /// Waits until `predicate` holds, panicking on the timeout.
    fn wait_until(mut predicate: impl FnMut() -> bool, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while !predicate() {
            assert!(Instant::now() < deadline, "the condition was not reached in time");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// The full wiring of one submitter test: frames in a real queue file,
    /// a mock chain, a recording publisher double (which confirms every
    /// result immediately) and the sql receiver.
    struct Harness {
        /// The temp files, removed on drop.
        _guards: Vec<TempFile>,
        queue_path: std::path::PathBuf,
        queue_capacity: usize,
        seq: Arc<SeqFile>,
        publisher_sink: ConfirmSink,
        publisher_tx: mpsc::Sender<(SettlementResult, mpsc::Sender<()>)>,
        sql_tx: mpsc::Sender<SettlementResult>,
        sql_rx: mpsc::Receiver<SettlementResult>,
    }

    fn harness(tag: &str, frames: &[BatchFrame]) -> Harness {
        let (queue_path, queue_capacity) = prepare_queue(tag, &frames[0]);
        let seq_path = temp_path(tag);
        let guards = vec![TempFile(queue_path.clone()), TempFile(seq_path.clone())];
        let seq = Arc::new(SeqFile::open(&seq_path).unwrap());
        let publisher_sink = ConfirmSink::default();
        let (publisher_tx, publisher_rx) = mpsc::channel();
        publisher_sink.spawn(publisher_rx);
        let (sql_tx, sql_rx) = mpsc::channel();
        // The additional frames (if any) enter the queue behind the first.
        {
            let mut queue = ByteSpscQueue::open(&queue_path, queue_capacity, false).unwrap();
            for frame in &frames[1..] {
                let mut bytes = Vec::new();
                crate::core::encode_frame(frame.0, &frame.1, &mut bytes).unwrap();
                assert!(queue.push(&bytes), "the harness queue fits the extra frames");
            }
        }
        Harness {
            _guards: guards,
            queue_path,
            queue_capacity,
            seq,
            publisher_sink,
            publisher_tx,
            sql_tx,
            sql_rx,
        }
    }

    /// Spawns the submitter over the harness with the given mock chain.
    fn spawn_harness(
        harness: &Harness,
        chain: MockChain,
        shutdown: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        let queue =
            ByteSpscQueue::open(&harness.queue_path, harness.queue_capacity, false).unwrap();
        let out =
            SubmitChannels { publisher: harness.publisher_tx.clone(), sql: harness.sql_tx.clone() };
        spawn(
            queue,
            Box::new(move || Ok(Arc::new(chain))),
            Arc::clone(&harness.seq),
            Symbol([7; 32]),
            out,
            cfg(),
            shutdown,
            Arc::new(AtomicUsize::new(1)),
        )
        .unwrap()
    }

    /// The frames still waiting in the queue at `path` (opened read-only).
    fn frames_left(path: &std::path::Path, capacity: usize) -> Vec<BatchFrame> {
        let mut queue = ByteSpscQueue::open(path, capacity, false).unwrap();
        let mut buf = Vec::new();
        let mut frames = Vec::new();
        while let Ok(Some(len)) = queue.peek(&mut buf) {
            frames.push(crate::core::decode_frame(&buf[..len]).unwrap());
            queue.ack(FRAME_HEADER_SIZE + len).unwrap();
        }
        frames
    }

    #[test]
    fn test_happy_path_settles_publishes_and_acks() {
        let harness = harness("settle", &[BatchFrame(0, trades(2))]);
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle =
            spawn_harness(&harness, MockChain::scenario_settle_ok(), Arc::clone(&shutdown));
        wait_until(|| !harness.publisher_sink.results().is_empty(), Duration::from_secs(10));

        // The settled result reached the publisher (and the sql writer).
        let results = harness.publisher_sink.results();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].batch_seq, 0);
        assert_eq!(results[0].outcome, SettlementOutcome::Settled);
        let sql: Vec<SettlementResult> = harness.sql_rx.try_iter().collect();
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0].outcome, SettlementOutcome::Settled);

        // The frame is acked: the queue is empty.
        wait_until(
            || frames_left(&harness.queue_path, harness.queue_capacity).is_empty(),
            Duration::from_secs(10),
        );

        // A graceful stop: nothing is in flight, the loop exits on the flag.
        shutdown.store(true, Ordering::Relaxed);
        wait_until(|| handle.is_finished(), Duration::from_secs(10));
        handle.join().unwrap();
    }

    #[test]
    fn test_single_trade_revert_publishes_the_reverted_result() {
        let harness = harness("revert", &[BatchFrame(3, trades(1))]);
        let handle = spawn_harness(
            &harness,
            MockChain::scenario_revert_with(2, 0, 1),
            Arc::new(AtomicBool::new(false)),
        );
        wait_until(|| !harness.publisher_sink.results().is_empty(), Duration::from_secs(10));

        let results = harness.publisher_sink.results();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].outcome,
            SettlementOutcome::Reverted {
                failed_trade: 0,
                at_fault: FaultSide::Taker,
                reason: SettlementFailure::Protocol(2),
            }
        );
        assert_eq!(results[0].batch_seq, 3);
        wait_until(
            || frames_left(&harness.queue_path, harness.queue_capacity).is_empty(),
            Duration::from_secs(10),
        );
        drop(handle);
    }

    #[test]
    fn test_multi_trade_revert_binary_splits_before_the_ack() {
        // A 4-trade batch reverts at index 1: the split produces two clean
        // children (of 2 and 1 trades) and a poison singleton. The clean
        // halves settle, the poison reverts — all three results publish
        // before the frame is acked.
        let harness = harness("split", &[BatchFrame(5, trades(4))]);
        let chain = MockChain::scenario_revert_with(2, 1, 1);
        // The children then settle / revert through the unscripted
        // fallbacks: script the clean halves to settle and the poison to
        // revert.
        chain.script(crate::chain::mock::Scripted::Submit(Ok(crate::chain::mock::tx_of(1))));
        chain.script(crate::chain::mock::Scripted::State(Ok(TxState::Confirmed {
            block: 100,
            depth: 1,
        })));
        chain.script(crate::chain::mock::Scripted::Submit(Ok(crate::chain::mock::tx_of(2))));
        chain.script(crate::chain::mock::Scripted::State(Ok(TxState::Confirmed {
            block: 101,
            depth: 1,
        })));
        chain.script(crate::chain::mock::Scripted::Submit(Ok(crate::chain::mock::tx_of(3))));
        chain.script(crate::chain::mock::Scripted::State(Ok(TxState::Reverted {
            code: 2,
            index: 0,
            side: 1,
        })));
        let handle = spawn_harness(&harness, chain, Arc::new(AtomicBool::new(false)));
        wait_until(|| harness.publisher_sink.results().len() == 3, Duration::from_secs(10));

        let results = harness.publisher_sink.results();
        let outcomes: Vec<SettlementOutcome> = results.iter().map(|r| r.outcome).collect();
        assert_eq!(
            outcomes,
            vec![
                SettlementOutcome::Settled,
                SettlementOutcome::Settled,
                SettlementOutcome::Reverted {
                    failed_trade: 0,
                    at_fault: FaultSide::Taker,
                    reason: SettlementFailure::Protocol(2),
                },
            ],
            "the clean halves settle before the poison reverts"
        );
        // The children get their own sequences, distinct from the parent.
        let seqs: Vec<u64> = results.iter().map(|r| r.batch_seq).collect();
        assert!(seqs.iter().all(|seq| *seq != 5), "children never reuse the parent sequence");
        assert_eq!(seqs.iter().collect::<std::collections::BTreeSet<_>>().len(), 3);
        // Only after every child is published does the frame ack.
        wait_until(
            || frames_left(&harness.queue_path, harness.queue_capacity).is_empty(),
            Duration::from_secs(10),
        );
        drop(handle);
    }

    #[test]
    fn test_transport_error_retries_then_settles() {
        let harness = harness("transport", &[BatchFrame(1, trades(1))]);
        let chain = MockChain::scenario_submit_transport_error();
        // The retry re-submits (unscripted answers fall back), then the
        // transaction is observed confirmed.
        chain.script(crate::chain::mock::Scripted::Submit(Ok(crate::chain::mock::tx_of(1))));
        chain.script(crate::chain::mock::Scripted::State(Ok(TxState::Confirmed {
            block: 100,
            depth: 1,
        })));
        let handle = spawn_harness(&harness, chain, Arc::new(AtomicBool::new(false)));
        wait_until(|| !harness.publisher_sink.results().is_empty(), Duration::from_secs(10));
        assert_eq!(harness.publisher_sink.results()[0].outcome, SettlementOutcome::Settled);
        wait_until(
            || frames_left(&harness.queue_path, harness.queue_capacity).is_empty(),
            Duration::from_secs(10),
        );
        drop(handle);
    }

    #[test]
    fn test_shutdown_finishes_the_inflight_batch_then_acks() {
        // The frame is popped before the shutdown lands: the batch in
        // flight completes, publishes and acks — then the loop exits.
        let harness = harness("shutdown", &[BatchFrame(2, trades(1))]);
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle =
            spawn_harness(&harness, MockChain::scenario_settle_ok(), Arc::clone(&shutdown));
        wait_until(
            || {
                harness.publisher_sink.results().len() == 1
                    && frames_left(&harness.queue_path, harness.queue_capacity).is_empty()
            },
            Duration::from_secs(10),
        );
        shutdown.store(true, Ordering::Relaxed);
        wait_until(|| handle.is_finished(), Duration::from_secs(10));
        handle.join().unwrap();
    }

    #[test]
    fn test_shutdown_with_queued_frames_leaves_them_in_the_queue() {
        // Two frames; the shutdown lands right away: the loop never pops
        // the second frame — it stays in the file-mapped queue.
        let harness = harness("queued", &[BatchFrame(0, trades(1)), BatchFrame(1, trades(1))]);
        let handle = spawn_harness(
            &harness,
            MockChain::scenario_settle_ok(),
            Arc::new(AtomicBool::new(true)),
        );
        wait_until(|| handle.is_finished(), Duration::from_secs(10));
        handle.join().unwrap();
        let left = frames_left(&harness.queue_path, harness.queue_capacity);
        assert!(!left.is_empty(), "the unprocessed frames survive the shutdown");
    }

    #[test]
    fn test_two_frames_are_processed_sequentially() {
        // The blocking pipeline: the second frame is untouched until the
        // first is fully settled and acked.
        let harness = harness("sequential", &[BatchFrame(0, trades(1)), BatchFrame(1, trades(1))]);
        // Both frames settle through two scripted settle scenarios.
        let chain = MockChain::new();
        for (counter, block) in [(1u8, 100u64), (2, 101)] {
            chain.script(crate::chain::mock::Scripted::Submit(Ok(crate::chain::mock::tx_of(
                u64::from(counter),
            ))));
            chain.script(crate::chain::mock::Scripted::State(Ok(TxState::Confirmed {
                block,
                depth: 1,
            })));
        }
        let handle = spawn_harness(&harness, chain, Arc::new(AtomicBool::new(false)));
        wait_until(|| harness.publisher_sink.results().len() == 2, Duration::from_secs(10));
        let seqs: Vec<u64> = harness.publisher_sink.results().iter().map(|r| r.batch_seq).collect();
        assert_eq!(seqs, vec![0, 1], "the frames settle in queue order");
        wait_until(
            || frames_left(&harness.queue_path, harness.queue_capacity).is_empty(),
            Duration::from_secs(10),
        );
        drop(handle);
    }

    #[test]
    fn test_a_corrupt_frame_stops_the_submitter() {
        let harness = harness("corrupt", &[BatchFrame(0, trades(1))]);
        // Clobber the payload bytes of the committed frame (past the 192-byte
        // queue header and the 4-byte length header) with garbage.
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&harness.queue_path)
                .unwrap();
            file.seek(SeekFrom::Start(192 + FRAME_HEADER_SIZE as u64)).unwrap();
            file.write_all(&[0xff; 64]).unwrap();
            file.sync_all().unwrap();
        }
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle =
            spawn_harness(&harness, MockChain::scenario_settle_ok(), Arc::clone(&shutdown));
        wait_until(|| handle.is_finished(), Duration::from_secs(10));
        // The submitter stopped without publishing or acking: the corrupt
        // frame is still committed in the queue.
        assert!(harness.publisher_sink.results().is_empty());
        let queue =
            ByteSpscQueue::open(&harness.queue_path, harness.queue_capacity, false).unwrap();
        let mut buf = Vec::new();
        assert!(queue.peek(&mut buf).unwrap().is_some(), "the frame stays unacked");
    }
}
