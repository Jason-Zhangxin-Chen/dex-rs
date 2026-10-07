//! The feed threads of the gateway: the margin feed applies the [SVD_Sync]
//! margin states to the shared cache and sweeps it; the settlement feed
//! reacts to the [SVD_Settlement] results — it re-injects the innocent
//! side's crossed quantity of a failed trade into the pipeline queue,
//! removes the at-fault side's orders through the same queue and blocks
//! the at-fault account on an insufficient margin.
//!
//! Each feed owns its thread with a dedicated current-thread tokio runtime
//! and a dedicated Redis pub/sub connection (a single node of the cluster —
//! the cluster propagates every publish to every node). A connection error
//! reconnects with a backoff; the feeds are side-path I/O, never on the
//! core thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::margin::{MarginCache, MarginState};
use crossbeam_queue::ArrayQueue;
use futures_util::StreamExt;
use primitives::address::Address;
use primitives::message::hot_path::{OrderMsg, PipelineMsg};
use primitives::message::margin::MarginMsg;
use primitives::message::settlement::{
    FaultSide, SettlementFailure, SettlementOutcome, SettlementResult,
};
use primitives::message::side_path::CancelReason;
use tracing::{info, warn};
use util::time::now_ms;

/// The backoff of a reconnect, in milliseconds.
const RECONNECT_BACKOFF_MS: u64 = 100;

/// Spawns the margin feed thread: subscribes to the margin channel, applies
/// the updates to the shared cache (a fresh update clears the block flag of
/// the account), keeps the feed liveness timestamp alive on every message
/// and sweeps the idle entries.
pub fn spawn_margin_feed(
    urls: Vec<String>,
    cache: Arc<MarginCache>,
    liveness: Arc<AtomicU64>,
    idle_evict_ms: u64,
    max_accounts: usize,
    shutdown: Arc<AtomicBool>,
) {
    std::thread::Builder::new()
        .name("pretrade-margin-feed".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            runtime.block_on(margin_loop(
                urls,
                crate::naming::MARGIN_CHANNEL.to_string(),
                cache,
                liveness,
                idle_evict_ms,
                max_accounts,
                shutdown,
            ));
        })
        .expect("spawn the margin feed thread");
}

/// The async loop of the margin feed: connect, consume, reconnect.
async fn margin_loop(
    urls: Vec<String>,
    channel: String,
    cache: Arc<MarginCache>,
    liveness: Arc<AtomicU64>,
    idle_evict_ms: u64,
    max_accounts: usize,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::Relaxed) {
        match consume_margin(
            &urls,
            &channel,
            &cache,
            &liveness,
            idle_evict_ms,
            max_accounts,
            &shutdown,
        )
        .await
        {
            Ok(()) => break,
            Err(err) => {
                warn!(error = %err, "the margin feed disconnected, reconnecting");
                tokio::time::sleep(Duration::from_millis(RECONNECT_BACKOFF_MS)).await;
            }
        }
    }
}

/// Connects to the first reachable node, subscribes and consumes the
/// channel until the connection drops or the shutdown is requested.
async fn consume_margin(
    urls: &[String],
    channel: &str,
    cache: &MarginCache,
    liveness: &AtomicU64,
    idle_evict_ms: u64,
    max_accounts: usize,
    shutdown: &AtomicBool,
) -> Result<(), String> {
    let mut pubsub = connect_pubsub(urls).await?;
    pubsub.subscribe(channel).await.map_err(|err| err.to_string())?;
    info!(channel, "the margin feed subscribed");
    let mut last_sweep_ms = now_ms();
    while !shutdown.load(Ordering::Relaxed) {
        let Some(message) = pubsub.on_message().next().await else {
            return Err("the subscription ended".to_string());
        };
        let now = now_ms();
        liveness.store(now, Ordering::Relaxed);
        let payload: Vec<u8> = message.get_payload().unwrap_or_default();
        match rmp_serde::from_slice::<MarginMsg>(&payload) {
            Ok(MarginMsg::Update(change)) => {
                cache.upsert(
                    change.account,
                    MarginState {
                        equity: change.equity,
                        used: change.used_margin,
                        available: change.available,
                        block: change.block,
                    },
                    now,
                );
            }
            Ok(MarginMsg::Heartbeat { block }) => {
                tracing::trace!(block, "the margin heartbeat");
            }
            Err(err) => {
                warn!(error = %err, "cannot decode the margin message");
            }
        }
        // The idle sweep runs on the feed cadence, not per message.
        if now.saturating_sub(last_sweep_ms) > idle_evict_ms {
            cache.sweep(now, idle_evict_ms, max_accounts);
            last_sweep_ms = now;
        }
    }
    Ok(())
}

/// Spawns the settlement feed thread: subscribes to the settlement result
/// channel, re-injects the innocent side's crossed quantity into the
/// pipeline queue, removes the at-fault side's orders through the same
/// queue and blocks the at-fault account on an insufficient margin.
pub fn spawn_settlement_feed(
    urls: Vec<String>,
    channel: String,
    cache: Arc<MarginCache>,
    queue: Arc<ArrayQueue<PipelineMsg>>,
    chain_id: u64,
    verifying_contract: Address,
    shutdown: Arc<AtomicBool>,
) {
    std::thread::Builder::new()
        .name("pretrade-settlement-feed".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            runtime.block_on(settlement_loop(
                urls,
                channel,
                cache,
                queue,
                chain_id,
                verifying_contract,
                shutdown,
            ));
        })
        .expect("spawn the settlement feed thread");
}

/// The async loop of the settlement feed: connect, consume, reconnect.
async fn settlement_loop(
    urls: Vec<String>,
    channel: String,
    cache: Arc<MarginCache>,
    queue: Arc<ArrayQueue<PipelineMsg>>,
    chain_id: u64,
    verifying_contract: Address,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::Relaxed) {
        match consume_settlement(
            &urls,
            &channel,
            &cache,
            &queue,
            chain_id,
            verifying_contract,
            &shutdown,
        )
        .await
        {
            Ok(()) => break,
            Err(err) => {
                warn!(error = %err, "the settlement feed disconnected, reconnecting");
                tokio::time::sleep(Duration::from_millis(RECONNECT_BACKOFF_MS)).await;
            }
        }
    }
}

/// Connects, subscribes and consumes the settlement results until the
/// connection drops or the shutdown is requested.
async fn consume_settlement(
    urls: &[String],
    channel: &str,
    cache: &MarginCache,
    queue: &ArrayQueue<PipelineMsg>,
    chain_id: u64,
    verifying_contract: Address,
    shutdown: &AtomicBool,
) -> Result<(), String> {
    let mut pubsub = connect_pubsub(urls).await?;
    pubsub.subscribe(channel).await.map_err(|err| err.to_string())?;
    info!(channel, "the settlement feed subscribed");
    while !shutdown.load(Ordering::Relaxed) {
        let Some(message) = pubsub.on_message().next().await else {
            return Err("the subscription ended".to_string());
        };
        let payload: Vec<u8> = message.get_payload().unwrap_or_default();
        let result: SettlementResult = match rmp_serde::from_slice(&payload) {
            Ok(result) => result,
            Err(err) => {
                warn!(error = %err, "cannot decode the settlement result");
                continue;
            }
        };
        // Only the reverted outcome is consumed here: a settled batch needs
        // no action, and the [SVD_Settlement] retries a transient failure (a
        // paused protocol, a down chain) until it resolves — no other
        // outcome is ever published.
        let SettlementOutcome::Reverted { failed_trade, at_fault: at_fault_side, reason } =
            result.outcome
        else {
            continue;
        };
        let Some(trade) = result.trades.get(failed_trade) else {
            warn!(failed_trade, batch_seq = result.batch_seq, "the failing trade is out of range");
            continue;
        };
        // The failed cross by fault side: the at-fault side leaves the book,
        // the innocent side's crossed quantity re-enters it.
        let (at_fault, innocent, quantity) = match at_fault_side {
            FaultSide::Taker => (trade.taker, trade.maker, trade.traded_quantity),
            FaultSide::Maker => (trade.maker, trade.taker, trade.traded_quantity),
        };
        // The removal of the at-fault side, per the failure taxonomy of
        // doc/settlement-protocol.md: an exhausted margin blocks the account
        // and removes every resting order of it, a deterministic failure of
        // one order (forged signature, expired, bad price) removes just that
        // order, and an unclassifiable failure removes nothing.
        match reason {
            SettlementFailure::Protocol(2) => {
                cache.set_blocked(at_fault.hot.user);
                info!(account = %at_fault.hot.user.hex(), "blocked on an insufficient margin");
                let removal = PipelineMsg::User(OrderMsg::MassCancelByUser {
                    user: at_fault.hot.user,
                    reason: CancelReason::SettlementFailed,
                });
                if !push_pipeline(queue, removal, shutdown) {
                    return Ok(());
                }
            }
            SettlementFailure::Protocol(1 | 4 | 7 | 8) => {
                let removal = PipelineMsg::User(OrderMsg::CancelBySettlement {
                    user: at_fault.hot.user,
                    nonce: at_fault.hot.nonce,
                    reason: CancelReason::SettlementFailed,
                });
                if !push_pipeline(queue, removal, shutdown) {
                    return Ok(());
                }
            }
            _ => {
                warn!(
                    ?reason,
                    account = %at_fault.hot.user.hex(),
                    "an unclassifiable settlement failure, restoring the innocent side only"
                );
            }
        }
        // The innocent side's crossed quantity re-enters the pipeline. The
        // restore bypasses the margin gate — it re-enters an already-admitted
        // state — and its signature is re-verified before the re-injection.
        if let Err(err) = cryptography::evm::verify_order(&innocent, chain_id, verifying_contract) {
            warn!(
                error = %err,
                account = %innocent.hot.user.hex(),
                "the innocent order signature does not verify, dropping the restore"
            );
            continue;
        }
        if !push_pipeline(queue, PipelineMsg::RestoreOrder { order: innocent, quantity }, shutdown)
        {
            return Ok(());
        }
    }
    Ok(())
}

/// Pushes a message into the pipeline queue, spinning until it lands or the
/// shutdown is requested. Returns whether the message landed.
fn push_pipeline(queue: &ArrayQueue<PipelineMsg>, msg: PipelineMsg, shutdown: &AtomicBool) -> bool {
    while queue.push(msg).is_err() {
        if shutdown.load(Ordering::Relaxed) {
            return false;
        }
        std::thread::yield_now();
    }
    true
}

/// Connects a pub/sub client to the first reachable node URL.
async fn connect_pubsub(urls: &[String]) -> Result<redis::aio::PubSub, String> {
    let mut last_error: Option<String> = None;
    for url in urls {
        match redis::Client::open(url.as_str()) {
            Ok(client) => match client.get_async_pubsub().await {
                Ok(pubsub) => return Ok(pubsub),
                Err(err) => last_error = Some(err.to_string()),
            },
            Err(err) => last_error = Some(err.to_string()),
        }
    }
    Err(format!(
        "cannot connect to any redis url {urls:?}: {}",
        last_error.unwrap_or_else(|| "no urls configured".to_string())
    ))
}
