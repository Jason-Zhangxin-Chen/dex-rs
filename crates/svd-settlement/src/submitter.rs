//! The submitter: the side thread that drives the settlement batches
//! through the chain state machine, classifies the failures and hands the
//! terminal outcomes to the publishers.
//!
//! The thread is the single owner of the chain: submissions and polls run
//! inline in the drive loop, so the nonce order equals the batch order and
//! two batches never submit concurrently. The journal lags reality, never
//! leads it: a state is journaled only after the chain action it describes
//! was taken or observed, so a crash may repeat an action (a duplicate
//! submit is absorbed by the protocol's `OrderFullySettled` code) but never
//! skips one.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::{Duration, Instant};

use cache::object_pool::CacheGuard;
use primitives::base::{Hash32, Symbol};
use primitives::message::hot_path::Trade;
use primitives::message::settlement::SettlementResult;
use tracing::{error, info, warn};

use crate::batch::{Batch, BatchAssembler, BatchState, backoff_for, replay_batches};
use crate::calldata::encode_settle_batch;
use crate::chain::{ChainClient, ChainError, TxState};
use crate::classifier::{Action, RetryReason, classify, plan_isolation};
use crate::config::RetryConfig;
use crate::journal::{SettlementJournal, SettlementRecord, reconstruct};

/// The poll idle of the submitter loop, in milliseconds: the loop wakes
/// this often even when nothing is pending.
const IDLE_SLEEP_MS: u64 = 1;

/// The config the submitter (and the batch machine) needs.
#[derive(Debug, Clone)]
pub struct SubmitterConfig {
    /// The bound of one `settleBatch` call.
    pub max_trades_per_batch: usize,
    /// The flush window of a partial batch, in milliseconds.
    pub batch_window_ms: u64,
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

/// The terminal outcome channels of the submitter.
#[derive(Debug, Clone)]
pub struct SubmitChannels {
    /// Terminal outcomes → the Redis publisher.
    pub publisher: std::sync::mpsc::Sender<SettlementResult>,
    /// Terminal outcomes → the SQL writer.
    pub sql: std::sync::mpsc::Sender<SettlementResult>,
}

/// Spawns the submitter thread: replays the journal, re-assembles the
/// pending trades, then drives the batches until the shutdown.
pub fn spawn(
    rx: Receiver<CacheGuard<Vec<Trade>>>,
    journal: SettlementJournal,
    chain: Arc<dyn ChainClient + Send + Sync>,
    symbol: Symbol,
    out: SubmitChannels,
    cfg: SubmitterConfig,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("stl-submitter".to_string())
        .spawn(move || run(rx, journal, chain, symbol, out, cfg, shutdown))
}

/// The submitter loop.
fn run(
    rx: Receiver<CacheGuard<Vec<Trade>>>,
    journal: SettlementJournal,
    chain: Arc<dyn ChainClient + Send + Sync>,
    symbol: Symbol,
    out: SubmitChannels,
    cfg: SubmitterConfig,
    shutdown: Arc<AtomicBool>,
) {
    // The crash replay first — before touching the chain or the channels.
    let records = match journal.replay() {
        Ok(records) => records,
        Err(err) => {
            error!(error = %err, "cannot replay the journal, the submitter stops");
            return;
        }
    };
    let mut replay = replay_batches(reconstruct(records), symbol);
    // Re-publish every journaled outcome (at-least-once: the consumers
    // deduplicate by the batch sequence), then re-assemble the pending
    // trade stream into batches.
    for outcome in replay.outcomes.drain(..) {
        publish(&out, outcome);
    }
    let mut pending: VecDeque<Batch> = replay.pending.into();
    for chunk in replay.pending_trades.chunks(cfg.max_trades_per_batch) {
        if let Err(err) = create_batch(&journal, chunk.to_vec(), &mut pending) {
            error!(error = %err, "cannot journal a recovered batch, the submitter stops");
            return;
        }
    }

    let mut assembler =
        BatchAssembler::new(cfg.max_trades_per_batch, Duration::from_millis(cfg.batch_window_ms));
    let poll = Duration::from_millis(cfg.poll_interval_ms);
    let mut next_drive = Instant::now();
    let mut core_gone = false;
    info!("the submitter loop started");
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let now = Instant::now();

        // Drain every available hand-off group.
        let mut disconnected = false;
        loop {
            match rx.try_recv() {
                Ok(mut guard) => {
                    let trades = guard.take();
                    for full in assembler.push(&trades, now) {
                        if let Err(err) = create_batch(&journal, full, &mut pending) {
                            error!(error = %err, "cannot journal a batch, the submitter stops");
                            return;
                        }
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }
        // The core thread is gone: flush the partial batch immediately (a
        // future `now` forces the window check without waiting).
        if disconnected && !core_gone {
            core_gone = true;
            warn!("the core thread is gone, flushing the partial batch");
            let flush_now =
                now.checked_add(Duration::from_millis(cfg.batch_window_ms + 1)).unwrap_or(now);
            if let Some(partial) = assembler.flush(flush_now)
                && let Err(err) = create_batch(&journal, partial, &mut pending)
            {
                error!(error = %err, "cannot journal a batch, the submitter stops");
                return;
            }
        }
        // The window flush of a still-running core.
        if let Some(partial) = assembler.flush(now)
            && let Err(err) = create_batch(&journal, partial, &mut pending)
        {
            error!(error = %err, "cannot journal a batch, the submitter stops");
            return;
        }

        // One state-machine step per pending batch per tick, in batch
        // order — the single owner of the chain.
        if now >= next_drive {
            drive(&journal, &*chain, symbol, &out, &cfg, &mut pending, now);
            next_drive = now + poll;
        }
        std::thread::sleep(Duration::from_millis(IDLE_SLEEP_MS));
    }
    info!("the submitter loop stopped; pending batches stay journaled for the replay");
}

/// Advances every pending batch by one step; batches that still need
/// driving re-enter the queue, published terminal batches leave it.
fn drive(
    journal: &SettlementJournal,
    chain: &dyn ChainClient,
    symbol: Symbol,
    out: &SubmitChannels,
    cfg: &SubmitterConfig,
    pending: &mut VecDeque<Batch>,
    now: Instant,
) {
    let count = pending.len();
    for _ in 0..count {
        let mut batch = pending.pop_front().expect("the counted batch exists");
        if step(&mut batch, journal, chain, symbol, out, cfg, pending, now) {
            pending.push_back(batch);
        }
    }
}

/// Advances one batch by one step. Returns whether the batch still needs
/// driving.
#[allow(clippy::too_many_arguments)]
fn step(
    batch: &mut Batch,
    journal: &SettlementJournal,
    chain: &dyn ChainClient,
    symbol: Symbol,
    out: &SubmitChannels,
    cfg: &SubmitterConfig,
    pending: &mut VecDeque<Batch>,
    now: Instant,
) -> bool {
    // A terminal batch whose outcome is missing (crash between the state
    // and the outcome): synthesize it, journal it and publish it.
    if batch.is_terminal() {
        if let Some(result) = batch.outcome(symbol) {
            finalize(journal, out, batch.seq, &result);
        }
        return false;
    }
    // The backoff holds this batch back.
    if batch.next_attempt_at.is_some_and(|at| now < at) {
        return true;
    }
    match &batch.state {
        BatchState::Received { .. } => {
            submit_batch(batch, journal, chain, cfg, now);
        }
        BatchState::Submitting { .. } => {
            // A transaction never seen on-chain past the grace is
            // re-submitted (the crash window between the build and the
            // broadcast). A replayed batch has no submit time: its grace
            // starts at the first poll.
            let submitted_at = *batch.submitted_at.get_or_insert(now);
            if now.duration_since(submitted_at) >= Duration::from_millis(cfg.tx_lost_grace_ms) {
                warn!(batch_seq = batch.seq, "the transaction was never seen, re-submitting");
                submit_batch(batch, journal, chain, cfg, now);
                return true;
            }
            let tx = batch.state.tx().expect("a submitting batch carries a transaction");
            match chain.tx_state(tx) {
                Ok(state) => observe(batch, journal, symbol, out, cfg, pending, tx, state, now),
                Err(err) => retryable_failure(batch, journal, chain, cfg, &err, now),
            }
        }
        BatchState::Submitted { .. } => {
            let tx = batch.state.tx().expect("a submitted batch carries a transaction");
            match chain.tx_state(tx) {
                Ok(state) => observe(batch, journal, symbol, out, cfg, pending, tx, state, now),
                Err(err) => retryable_failure(batch, journal, chain, cfg, &err, now),
            }
        }
        BatchState::Confirmed { .. } | BatchState::Reverted { .. } | BatchState::Split { .. } => {
            // Unreachable: terminal batches return at the top of `step`.
        }
    }
    true
}

/// Submits one batch: encodes the calldata, sends it through the chain and
/// journals the `Submitting` state. A failed submission backs off per the
/// retry policy.
fn submit_batch(
    batch: &mut Batch,
    journal: &SettlementJournal,
    chain: &dyn ChainClient,
    cfg: &SubmitterConfig,
    now: Instant,
) {
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
            journal_state(journal, batch.seq, BatchState::Submitting { tx: tx.tx_hash });
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
    journal: &SettlementJournal,
    symbol: Symbol,
    out: &SubmitChannels,
    cfg: &SubmitterConfig,
    pending: &mut VecDeque<Batch>,
    tx: Hash32,
    state: TxState,
    now: Instant,
) {
    match state {
        TxState::Pending => {
            // The transaction is in flight: remember the nonce when known
            // (a replayed `Submitting` batch lacks it until a re-submit).
            if let (BatchState::Submitting { .. }, Some(nonce)) = (&batch.state, batch.nonce) {
                journal_state(journal, batch.seq, BatchState::Submitted { tx, nonce });
                batch.state = BatchState::Submitted { tx, nonce };
            }
        }
        TxState::Confirmed { block, depth } => {
            if depth >= cfg.confirm_depth {
                journal_state(journal, batch.seq, BatchState::Confirmed { tx, block });
                batch.state = BatchState::Confirmed { tx, block };
            }
        }
        TxState::Reverted { code, index, side } => {
            match classify(code, index, side, batch.trades.len()) {
                Action::TreatAsSettled => {
                    // The idempotent double-submission path: the trades
                    // already settled. The block is unknown here (0 = the
                    // published result carries no block number).
                    journal_state(journal, batch.seq, BatchState::Confirmed { tx, block: 0 });
                    batch.state = BatchState::Confirmed { tx, block: 0 };
                }
                Action::Revert { failed_trade, at_fault, reason } => {
                    if batch.trades.len() == 1 {
                        journal_state(
                            journal,
                            batch.seq,
                            BatchState::Reverted { tx, failed_trade, at_fault, reason },
                        );
                        batch.state = BatchState::Reverted { tx, failed_trade, at_fault, reason };
                    } else {
                        split(batch, journal, pending, code, index, side, tx);
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
    // A terminal transition publishes immediately after its journaling.
    if batch.is_terminal()
        && let Some(result) = batch.outcome(symbol)
    {
        finalize(journal, out, batch.seq, &result);
    }
}

/// Handles a failed chain interaction of a submitted transaction: a
/// retryable error backs off (with a fee-bump replacement after the
/// configured attempts), a non-retryable one alarms and retries at the
/// maximum cadence.
fn retryable_failure(
    batch: &mut Batch,
    journal: &SettlementJournal,
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
                journal_state(journal, batch.seq, BatchState::Submitting { tx: tx.tx_hash });
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

/// Splits a failing batch: journals the child `Received` records FIRST (in
/// submission order: the clean halves, then the poison), then the parent's
/// `Split` record. The children enter the drive loop as pending batches.
fn split(
    batch: &mut Batch,
    journal: &SettlementJournal,
    pending: &mut VecDeque<Batch>,
    code: u8,
    index: usize,
    side: u8,
    tx: Hash32,
) {
    let plan = plan_isolation(&batch.trades, code, index, side);
    for trades in plan.settle.iter().chain(std::iter::once(&plan.poison)) {
        if let Err(err) = create_batch(journal, trades.clone(), pending) {
            error!(error = %err, batch_seq = batch.seq, "cannot journal a child batch");
            return;
        }
    }
    journal_state(journal, batch.seq, BatchState::Split { tx, code, index, side });
    batch.state = BatchState::Split { tx, code, index, side };
}

/// Journals one batch state transition.
fn journal_state(journal: &SettlementJournal, batch_seq: u64, state: BatchState) {
    if let Err(err) =
        journal.write(&SettlementRecord::BatchState { batch_seq, state: state.clone() })
    {
        error!(error = %err, batch_seq, "cannot journal a batch state");
    }
}

/// Journals the outcome record, then hands the result to the publishers.
/// The outcome is journaled BEFORE the publication: the replay re-publishes
/// it when the process dies in between.
fn finalize(
    journal: &SettlementJournal,
    out: &SubmitChannels,
    batch_seq: u64,
    result: &SettlementResult,
) {
    if let Err(err) =
        journal.write(&SettlementRecord::Outcome { batch_seq, result: result.clone() })
    {
        error!(error = %err, batch_seq, "cannot journal the outcome");
        return;
    }
    publish(out, result.clone());
}

/// Hands one terminal outcome to both publishers. The sends never block
/// (unbounded channels) and the threads retry until accepted.
fn publish(out: &SubmitChannels, result: SettlementResult) {
    let batch_seq = result.batch_seq;
    if out.publisher.send(result.clone()).is_err() {
        error!(batch_seq, "the publisher is gone");
    }
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
    // (batch, attempt) so a replay lands on the same schedule.
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

/// Journals a fresh `Received` batch from the given trades and queues it.
fn create_batch(
    journal: &SettlementJournal,
    trades: Vec<Trade>,
    pending: &mut VecDeque<Batch>,
) -> Result<u64, crate::journal::SettlementJournalError> {
    let seq = journal.write(&SettlementRecord::BatchState {
        batch_seq: 0, // replaced by the record's own sequence under the lock
        state: BatchState::Received { trades: trades.clone() },
    })?;
    pending.push_back(Batch {
        seq,
        trades: trades.clone(),
        state: BatchState::Received { trades },
        attempts: 0,
        next_attempt_at: None,
        nonce: None,
        submitted_at: None,
    });
    Ok(seq)
}
