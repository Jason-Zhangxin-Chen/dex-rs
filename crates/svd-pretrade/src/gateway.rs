//! The HTTP gateway: the NGINX-facing endpoint of the hot path. The
//! handler threads run the whole validation pipeline per request — the
//! parsing, the signature verification, the order sanity and the margin
//! gate with the on-demand pull — and push the accepted requests into the
//! shared MPSC queue; the rejects are answered directly and never reach the
//! queue. A handler is a worker thread: the parsing, the Redis pull and the
//! serialization are allowed here, a full queue applies backpressure.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use crossbeam_queue::ArrayQueue;
use primitives::address::Address;
use primitives::base::Symbol;
use primitives::message::hot_path::{OrderMsg, PipelineMsg};
use primitives::message::margin::MarginChange;
use primitives::order::Order;
use storage::RedisKeyStore;
use tracing::{info, warn};

use crate::config::PretradeConfig;
use crate::margin::{MarginCache, MarginState, MarginView};
use crate::naming;

/// The reason strings of the rejects, stable for the user end.
pub const REJECT_INVALID_SIGNATURE: &str = "InvalidSignature";
pub const REJECT_INVALID_PRICE: &str = "InvalidPrice";
pub const REJECT_INVALID_QUANTITY: &str = "InvalidQuantity";
pub const REJECT_INSUFFICIENT_MARGIN: &str = "InsufficientMargin";
pub const REJECT_MARGIN_STATE_UNAVAILABLE: &str = "MarginStateUnavailable";

/// The shared state of the gateway handlers.
pub struct Gateway {
    /// The market symbol the gateway serves.
    symbol: Symbol,
    /// The chain id of the settlement protocol (the EIP-712 domain).
    chain_id: u64,
    /// The settlement contract address (the EIP-712 verifying contract).
    verifying_contract: Address,
    /// The margin requirement in basis points of the notional.
    margin_ratio_bps: u32,
    /// The taker fee in basis points of the notional.
    fee_bps: u32,
    /// The quote amount one price tick × one lot unit corresponds to.
    quote_per_tick_lot: u128,
    /// Whether an order passes the gate when no margin state is available.
    allow_unknown_accounts: bool,
    /// The timeout of an on-demand margin pull.
    pull_timeout_ms: u64,
    /// The stale feed timeout of the margin kill switch.
    stale_feed_ms: u64,
    /// The shared margin cache.
    cache: Arc<MarginCache>,
    /// The feed liveness timestamp, updated by the margin feed.
    liveness: Arc<AtomicU64>,
    /// The shared pipeline queue.
    queue: Arc<ArrayQueue<PipelineMsg>>,
    /// The keyed store of the on-demand margin pulls.
    key_store: RedisKeyStore,
}

impl Gateway {
    /// Builds the gateway state around the configuration and the shared
    /// structures.
    pub fn new(
        config: &PretradeConfig,
        cache: Arc<MarginCache>,
        liveness: Arc<AtomicU64>,
        queue: Arc<ArrayQueue<PipelineMsg>>,
        key_store: RedisKeyStore,
    ) -> Self {
        Self {
            symbol: config.symbol,
            chain_id: config.chain.chain_id,
            verifying_contract: config.chain.verifying_contract,
            margin_ratio_bps: config.margin.margin_ratio_bps,
            fee_bps: config.margin.fee_bps,
            quote_per_tick_lot: config.margin.quote_per_tick_lot,
            allow_unknown_accounts: config.margin.allow_unknown_accounts,
            pull_timeout_ms: config.margin.pull_timeout_ms,
            stale_feed_ms: config.margin.stale_feed_ms,
            cache,
            liveness,
            queue,
            key_store,
        }
    }

    /// The router of the gateway.
    pub fn router(gateway: Arc<Gateway>) -> Router {
        Router::new()
            .route("/api/v1/{symbol}/order", post(place_order))
            .route("/api/v1/{symbol}/cancel", post(cancel_order))
            .with_state(gateway)
    }

    /// Checks the request symbol against the configured symbol.
    fn symbol_matches(&self, text: &str) -> bool {
        crate::config::parse_symbol(text).is_ok_and(|symbol| symbol == self.symbol)
    }

    /// Whether the margin feed is dead: no channel message within the stale
    /// window.
    fn feed_is_stale(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.liveness.load(Ordering::Relaxed)) > self.stale_feed_ms
    }
}

/// The HTTP handler of `POST /api/v1/{symbol}/order`.
async fn place_order(
    Path(symbol): Path<String>,
    State(state): State<Arc<Gateway>>,
    body: axum::body::Bytes,
) -> Response {
    if !state.symbol_matches(&symbol) {
        return Response::not_found();
    }
    let message: OrderMsg = match serde_json::from_slice(&body) {
        Ok(message) => message,
        Err(err) => {
            return Response::reject(StatusCode::BAD_REQUEST, &format!("{err}"));
        }
    };
    let OrderMsg::NewOrder(order) = message else {
        return Response::reject(StatusCode::BAD_REQUEST, "expected a new order");
    };
    // The sanity and the signature never wait.
    if let Err(reason) =
        sanity_and_signature(&order, state.symbol, state.chain_id, state.verifying_contract)
    {
        return Response::reject(StatusCode::BAD_REQUEST, reason);
    }
    // The margin gate: read the cached state, pull on the first attach.
    let now = now_ms();
    if state.feed_is_stale(now) {
        return Response::reject(StatusCode::SERVICE_UNAVAILABLE, REJECT_MARGIN_STATE_UNAVAILABLE);
    }
    let view = match state.cache.get(order.hot.user, now) {
        Some(view) => view,
        None => match state.pull_margin(order.hot.user, now).await {
            Some(view) => view,
            None => {
                if state.allow_unknown_accounts {
                    return forward(&state, PipelineMsg::User(OrderMsg::NewOrder(order))).await;
                }
                return Response::reject(
                    StatusCode::SERVICE_UNAVAILABLE,
                    REJECT_MARGIN_STATE_UNAVAILABLE,
                );
            }
        },
    };
    if let Err(reason) =
        margin_gate(&order, view, state.margin_ratio_bps, state.fee_bps, state.quote_per_tick_lot)
    {
        return Response::reject(StatusCode::FORBIDDEN, reason);
    }
    forward(&state, PipelineMsg::User(OrderMsg::NewOrder(order))).await
}

/// The HTTP handler of `POST /api/v1/{symbol}/cancel`. A cancel bypasses
/// the margin gate entirely — a trader must always be able to pull their
/// orders.
async fn cancel_order(
    Path(symbol): Path<String>,
    State(state): State<Arc<Gateway>>,
    body: axum::body::Bytes,
) -> Response {
    if !state.symbol_matches(&symbol) {
        return Response::not_found();
    }
    let message: OrderMsg = match serde_json::from_slice(&body) {
        Ok(message) => message,
        Err(err) => {
            return Response::reject(StatusCode::BAD_REQUEST, &format!("{err}"));
        }
    };
    let OrderMsg::CancelOrder(cancel) = message else {
        return Response::reject(StatusCode::BAD_REQUEST, "expected a cancel order");
    };
    if cancel.symbol() != state.symbol {
        return Response::not_found();
    }
    if cryptography::evm::verify_cancel(&cancel, state.chain_id, state.verifying_contract).is_err()
    {
        return Response::reject(StatusCode::BAD_REQUEST, REJECT_INVALID_SIGNATURE);
    }
    forward(&state, PipelineMsg::User(OrderMsg::CancelOrder(cancel))).await
}

/// Forwards an accepted message into the shared MPSC queue with
/// backpressure: a full queue makes the handler wait for the capacity, it
/// never drops an accepted request.
async fn forward(state: &Gateway, message: PipelineMsg) -> Response {
    while state.queue.push(message).is_err() {
        tokio::task::yield_now().await;
    }
    Response::accepted()
}

impl Gateway {
    /// The on-demand pull of the first attach: reads the account's margin
    /// key on a blocking pool (a handler is a worker thread) with the
    /// configured timeout, attaches the pulled state and returns the view
    /// to check. A missing key attaches the zero state; a failed pull
    /// returns `None`.
    async fn pull_margin(&self, account: Address, now_ms: u64) -> Option<MarginView> {
        let store = self.key_store.clone();
        let key = naming::margin_account_key(account);
        let fetched = tokio::time::timeout(
            Duration::from_millis(self.pull_timeout_ms),
            tokio::task::spawn_blocking(move || store.get(&key)),
        )
        .await;
        let fetched = match fetched {
            Ok(Ok(fetched)) => fetched,
            Ok(Err(err)) => {
                warn!(error = %err, account = %naming::address_hex(account), "the margin pull panicked");
                return None;
            }
            Err(_) => {
                warn!(account = %naming::address_hex(account), "the margin pull timed out");
                return None;
            }
        };
        let fetched = match fetched {
            Ok(fetched) => fetched,
            Err(err) => {
                warn!(error = %err, account = %naming::address_hex(account), "the margin pull failed");
                return None;
            }
        };
        let state = match fetched {
            Some(bytes) => match rmp_serde::from_slice::<MarginChange>(&bytes) {
                Ok(change) => MarginState {
                    equity: change.equity,
                    used: change.used_margin,
                    available: change.available,
                    block: change.block,
                },
                Err(err) => {
                    warn!(error = %err, account = %naming::address_hex(account), "cannot decode the pulled margin state");
                    return None;
                }
            },
            None => MarginState::ZERO,
        };
        info!(
            account = %naming::address_hex(account),
            available = state.available,
            "the margin pull attached the account"
        );
        Some(self.cache.attach_if_absent(account, state, now_ms))
    }
}

/// The sanity and the signature checks of a new order — they never wait.
fn sanity_and_signature(
    order: &Order,
    symbol: Symbol,
    chain_id: u64,
    verifying_contract: Address,
) -> Result<(), &'static str> {
    if order.cold.common.symbol() != symbol {
        return Err("UnknownSymbol");
    }
    if order.hot.price.0 == 0 {
        return Err(REJECT_INVALID_PRICE);
    }
    if order.total_quantity().0 == 0 {
        return Err(REJECT_INVALID_QUANTITY);
    }
    cryptography::evm::verify_order(order, chain_id, verifying_contract)
        .map_err(|_| REJECT_INVALID_SIGNATURE)
}

/// The margin gate: `required ≤ available` against the latest synced state,
/// with the block flag rejecting the account outright.
fn margin_gate(
    order: &Order,
    view: MarginView,
    margin_ratio_bps: u32,
    fee_bps: u32,
    quote_per_tick_lot: u128,
) -> Result<(), &'static str> {
    if view.blocked {
        return Err(REJECT_INSUFFICIENT_MARGIN);
    }
    let required = required_margin(order, margin_ratio_bps, fee_bps, quote_per_tick_lot);
    if required > view.state.available {
        return Err(REJECT_INSUFFICIENT_MARGIN);
    }
    Ok(())
}

/// The worst-case exposure of an order: `notional × (margin ratio + taker
/// fee)` in basis points, saturating on the absurd overflows.
fn required_margin(
    order: &Order,
    margin_ratio_bps: u32,
    fee_bps: u32,
    quote_per_tick_lot: u128,
) -> u128 {
    let notional = u128::from(order.hot.price.0)
        .saturating_mul(u128::from(order.total_quantity().0))
        .saturating_mul(quote_per_tick_lot);
    let ratio = u128::from(margin_ratio_bps.saturating_add(fee_bps));
    notional.saturating_mul(ratio) / 10_000
}

/// The response of a handled request: accepted or rejected with a reason.
struct Response {
    status: StatusCode,
    body: (bool, String),
}

impl Response {
    fn accepted() -> Self {
        Self { status: StatusCode::OK, body: (true, String::new()) }
    }

    fn reject(status: StatusCode, reason: &str) -> Self {
        Self { status, body: (false, reason.to_string()) }
    }

    fn not_found() -> Self {
        Self::reject(StatusCode::NOT_FOUND, "UnknownSymbol")
    }
}

impl IntoResponse for Response {
    fn into_response(self) -> axum::response::Response {
        let json = serde_json::json!({
            "accepted": self.body.0,
            "reason": self.body.1,
        });
        (self.status, Json(json)).into_response()
    }
}

/// The current wall clock in milliseconds.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::base::{Hash32, Nonce, Side};
    use primitives::order::{OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use primitives::signature::Signature;
    use primitives::time_in_force::TimeInForce;
    use primitives::value::{Price, Quantity, TimestampMs};

    fn order(user: u8, nonce: u64, price: u64, quantity: u64) -> Order {
        Order::new(
            OrderHot {
                user: Address([user; 20]),
                nonce: Nonce(nonce),
                price: Price(price),
                quantity: Quantity(quantity),
                time_in_force: TimeInForce::Gtc,
                side: Side::Buy,
            },
            OrderCold::new(
                OrderColdCommon::new(
                    Hash32([0; 32]),
                    Symbol([0x7au8; 32]),
                    Signature::default(),
                    TimestampMs(0),
                ),
                OrderKind::Standard,
            ),
        )
    }

    fn view(available: u128, blocked: bool) -> MarginView {
        MarginView {
            state: MarginState { equity: available, used: 0, available, block: 1 },
            blocked,
        }
    }

    #[test]
    fn test_required_margin_math() {
        let order = order(1, 1, 100, 10);
        // notional = 100 × 10 × 2 = 2000; ratio 100% + fee 10 bps → 2002.
        assert_eq!(required_margin(&order, 10_000, 10, 2), 2_002);
        // A 50% ratio without a fee.
        assert_eq!(required_margin(&order, 5_000, 0, 1), 500);
    }

    #[test]
    fn test_required_margin_saturates_on_overflow() {
        let order = order(1, 1, u64::MAX, u64::MAX);
        // The notional saturates at the u128 maximum; the bps division
        // still applies to the saturated value.
        assert_eq!(required_margin(&order, 10_000, 0, u128::MAX), u128::MAX / 10_000);
    }

    #[test]
    fn test_margin_gate_passes_within_the_available_margin() {
        let order = order(1, 1, 100, 10);
        assert!(margin_gate(&order, view(1_000, false), 10_000, 0, 1).is_ok());
        assert_eq!(
            margin_gate(&order, view(999, false), 10_000, 0, 1),
            Err(REJECT_INSUFFICIENT_MARGIN)
        );
    }

    #[test]
    fn test_margin_gate_rejects_a_blocked_account() {
        let order = order(1, 1, 100, 10);
        assert_eq!(
            margin_gate(&order, view(1_000_000, true), 10_000, 0, 1),
            Err(REJECT_INSUFFICIENT_MARGIN)
        );
    }

    #[test]
    fn test_sanity_and_signature_rejects_bad_shapes() {
        let symbol = Symbol([0x7au8; 32]);
        let contract = Address([0xabu8; 20]);
        assert_eq!(
            sanity_and_signature(&order(1, 1, 0, 10), symbol, 1, contract),
            Err(REJECT_INVALID_PRICE)
        );
        assert_eq!(
            sanity_and_signature(&order(1, 1, 100, 0), symbol, 1, contract),
            Err(REJECT_INVALID_QUANTITY)
        );
        let wrong_symbol = Order::new(
            order(1, 1, 100, 10).hot,
            OrderCold::new(
                OrderColdCommon::new(
                    Hash32([0; 32]),
                    Symbol([1u8; 32]),
                    Signature::default(),
                    TimestampMs(0),
                ),
                OrderKind::Standard,
            ),
        );
        assert_eq!(sanity_and_signature(&wrong_symbol, symbol, 1, contract), Err("UnknownSymbol"));
        // An unsigned order fails the signature check.
        assert_eq!(
            sanity_and_signature(&order(1, 1, 100, 10), symbol, 1, contract),
            Err(REJECT_INVALID_SIGNATURE)
        );
    }

    #[test]
    fn test_feed_is_stale() {
        let config = PretradeConfig::from_toml(
            r#"
symbol = "X"
[ingress]
path = "/tmp/q"
capacity = 1024
[chain]
chain_id = 1
verifying_contract = "0x0000000000000000000000000000000000000001"
"#,
        )
        .unwrap();
        let cache = Arc::new(MarginCache::new(2, 4));
        let liveness = Arc::new(AtomicU64::new(100));
        let queue = Arc::new(ArrayQueue::new(8));
        let gateway = Gateway::new(
            &config,
            cache,
            liveness,
            queue,
            RedisKeyStore::connect(&config.redis).unwrap(),
        );
        assert!(!gateway.feed_is_stale(100));
        assert!(!gateway.feed_is_stale(100 + config.margin.stale_feed_ms));
        assert!(gateway.feed_is_stale(100 + config.margin.stale_feed_ms + 1));
    }
}
