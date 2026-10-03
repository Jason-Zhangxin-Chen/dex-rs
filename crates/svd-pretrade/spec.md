# The overview
Every symbol's [HotPath] starts at the [SVD_Pretrade]. It is the gateway of the hot path: it accepts the
user's HTTP requests routed by [NGINX] (the request path declares the symbol), verifies them, runs the
pre-trade checks — signature, order sanity, and the **on-chain margin pre-check** — and forwards the
accepted requests to the [SVD_OMS_Master] via the share memory SPSC queue implemented in the ipc crate.
A trader without sufficient on-chain margin balance is not allowed to trade anymore: their orders are
rejected at this gate, before they ever reach the book.

The pre-trade checks must run without blocking on the hot path, so the margin data never comes from a
synchronous chain RPC call or a Redis read at request time. The [SVD_Pretrade] keeps a **local margin
cache** of every account, maintained on a side thread: the [SVD_Sync] service is the authoritative
producer of the margin state (it synces the on-chain [MarginAccount] state into the [Redis_Cluster]),
and the [SVD_Pretrade] subscribes to the margin change channel, applies the deltas to a new snapshot,
and atomically swaps the snapshot the core thread reads. On top of the synced state the [SVD_Pretrade]
tracks **local margin reservations** for the orders it forwarded, so that a trader cannot spend the
same margin twice between two on-chain updates. The design document of the assumed on-chain protocol
lives in doc/settlement-protocol.md.

## The data flow
The [NGINX] gateway extracts the symbol from the request path and routes the request to the
[SVD_Pretrade] of the corresponding market. The HTTP acceptor threads of the [SVD_Pretrade] push the
requests into an MPSC queue to the core thread; the core thread runs a spin loop which drains the
request batch, runs the checks against the margin cache and the reservations, forwards the accepted
requests to the [SVD_OMS_Master] through the share memory SPSC queue, and responds to the user end
through a oneshot channel: an accepted order gets an immediate accept response, a rejected one gets
the reject reason, and the reject never reaches the book.

Three side feeds keep the local state fresh, none of them on the hot path:

- **[SVD_Sync] margin feed** — the [SVD_Pretrade] subscribes to the margin change channel
  `svd:sync:margin` of the [Redis_Cluster], applies the deltas to a fresh snapshot of the margin
  cache, and swaps it into place with an atomic `Arc` swap. The cache is read-only on the core
  thread, so the swap needs no locks around the hot path reads. A snapshot carries the monotonic
  sequence of the [SVD_Sync] journal; on cold start the [SVD_Pretrade] subscribes first, then loads
  the latest margin snapshot from the [Redis_Cluster], then applies the deltas newer than the
  snapshot's sequence, which closes the gap between the snapshot and the subscription.
- **Book state feed** — the [SVD_Pretrade] subscribes to the book change channel
  `svd:oms:{symbol_hex}:changes` published by the [SVD_OMS_Slave]. The changes released the
  reservations of the orders that left the book without a settlement result: cancellations (user,
  STP, time in force, mass cancel from [SVD_Settlement]) and rejections by the book (duplicate id,
  book risk limits, ...). The deltas are enqueued to the core thread, which applies them between
  the request batches, so the reservation map is owned by one thread.
- **Settlement result feed** — the [SVD_Pretrade] subscribes to the settlement result channel
  `svd:stl:{symbol_hex}:settlements` published by the [SVD_Settlement]. A settled trade releases the
  reservation of its filled portion — margin spent on-chain is the trader's own state now, the
  reservation must not double-count it. A failed settlement with `InsufficientMargin` blocks the
  account: the orders of the account are being removed from the book by the [SVD_Settlement], and
  the account stays blocked until the [SVD_Sync] margin feed observes the recovered equity (the
  feed is the only source that clears a block). The results carry the settlement's monotonic batch
  sequence; the [SVD_Pretrade] applies each result once by the sequence watermark, so a republished
  result never releases a reservation twice.

## Ingress Message and Outgress Message
The ingress from the [NGINX] is HTTP; the outgress to the [SVD_OMS_Master] is the share memory SPSC
queue of `OrderMsg` defined in crates/primitives/src/message/hot_path.rs (the message is fixed sized
for preallocation in share memory):

```Rust
/// The HTTP gateway of the [SVD_Pretrade], routed by [NGINX] on the symbol in the path:
/// POST /api/v1/{symbol}/order   — a new order, body carries the order and the signature.
/// POST /api/v1/{symbol}/cancel  — a cancel, body carries the cancel request.
/// The response: {"accepted": true} or {"accepted": false, "reason": "<reject reason>"}.

/// Messages sent from user end. It is forwarded to [SVD_OMS_Master] from [SVD_Pretrade] for
/// processing via share memory SPSC queue.
pub enum OrderMsg {
    /// New Order.
    NewOrder(Order),
    /// Cancel Order.
    CancelOrder(CancelOrder),
}
```

The feeds the [SVD_Pretrade] consumes are the messages the other services publish to the
[Redis_Cluster] (the wire format is MessagePack, rmp-serde):

- `MarginChange` — the [SVD_Sync] margin delta, to be defined in
  crates/primitives/src/message/margin.rs. It carries the sync sequence, the account, the equity,
  the used margin, the available margin, and the block the state comes from.
- `SettlementResult` — the [SVD_Settlement] result, to be defined in
  crates/primitives/src/message/settlement.rs. It carries the settlement batch sequence, the
  outcome, and the trades of the batch, so the released reservation is computed from the executed
  price and the traded quantity of each cross.
- `OrderChange` — the book state changes of the [SVD_OMS_Slave], already defined in
  crates/primitives/src/message/side_path.rs.

The rejection reasons reuse the `RejectReason` taxonomy of the primitives crate; two variants are
added for the margin gate (the enum is `#[non_exhaustive]`, so the appending is sanctioned):
`InsufficientMargin` (the check failed) and `MarginStateUnavailable` (no margin state for the
account in the cache — rejected unless the config allows unknown accounts).

## The margin pre-check
The margin state of an account has two parts, and the check is the difference of the two:

```
required_margin(order) ≤ available_synced(account) − reserved(account)
```

- **available_synced** — the authoritative available margin of the account, produced by
  [SVD_Sync] from the on-chain [MarginAccount] state. It is read from the margin cache snapshot;
  a read is a plain lookup in the swapped snapshot.
- **reserved** — the [SVD_Pretrade]-local reservations: the margin the account's in-flight and
  resting orders may still consume, tracked per account. Without it a trader could place
  arbitrarily many orders against the same synced balance before any of them settles.
- **required_margin** — the worst-case exposure of the incoming order:
  `notional × margin_ratio_bps`, with `notional = price × quantity × quotePerTickLot` and the
  taker fee added. The config of the [SVD_Pretrade] must be kept in lockstep with the on-chain
  symbol config of the settlement protocol (an ops invariant — the on-chain values are the final
  arbiter, the off-chain check is an early conservative gate).

The reservation of one order is its worst-case exposure, computed at admission. The lifecycle of a
reservation is conservative by design — it releases late rather than early, and a stale release
can only reject valid orders, never admit an over-spending one:

| event | source | action on the reservation |
| --- | --- | --- |
| accepted new order | local (forwarded) | reserve the full `required_margin` |
| accepted cancel | local (forwarded) | release the full reservation of `(user, nonce)` |
| order rejected by the book | book state feed (`Rejected`) | release the full reservation |
| order canceled on the book (user / STP / TIF / mass cancel) | book state feed (`Canceled`) | release the full reservation |
| order filled on the book | book state feed (`Filled` / `PartiallyFilled`) | **keep** — the release waits for the settlement result |
| trade settled on-chain | settlement result feed (`Settled`) | release the filled portion of the cross (notional of the executed price × traded quantity) |
| settlement failed with `InsufficientMargin` | settlement result feed | release the failed portion, **block the account** (new orders rejected, cancels still pass) until the sync margin feed observes the account again |
| settlement rolled back (`RolledBack`) | settlement result feed | **keep** the reservation — the failed trades' orders are re-inserted into the book by the reversal messages, so their margin stays reserved; the restored orders' `Open` changes arrive through the book state feed and carry no reservation action (a reservation is only taken at admission, never re-taken from the feed) |

A settlement result is applied at most once by the settlement sequence watermark, so a republished
result cannot double-release. The [SVD_Settlement] journal guarantees the results are published
exactly once per settled batch anyway; the watermark is the defense against at-least-once delivery
of the channel.

Two safety valves bound the staleness of the synced state:

- **Stale feed kill switch** — when no margin event arrived within `margin_feed_stale_ms`, the
  [SVD_Pretrade] rejects new orders (cancels still pass) until the feed resumes. A silent margin
  feed must never mean "everyone has infinite margin".
- **Unknown accounts** — an account with no margin state in the cache is rejected with
  `MarginStateUnavailable` unless `allow_unknown_accounts` is set (the escape hatch for a
  bootstrapping market).

## The dependency of svd-pretrade crate
- primitives: the `OrderMsg`, `Order`, `RejectReason` types and the value types, all in
  crates/primitives.
- ipc: the share memory SPSC queue in crates/ipc, wired to the [SVD_OMS_Master] ingress.
- storage: the [Redis_Cluster] helpers in crates/storage for the margin snapshot load and the
  channel subscriptions (a subscription uses a dedicated async cluster connection — the pooled
  client of the storage crate is for publishing).
- cryptography: the signature verification traits in crates/cryptography, with the EVM signature
  implementation over the EIP-712 order hash defined in doc/settlement-protocol.md (alloy provides
  the secp256k1 recovery).
- net: the HTTP server helpers in crates/net for the [NGINX]-facing gateway.

## The concurrency model of svd-pretrade crate
- **Core thread** (1, pinned): a spin loop which drains the HTTP request batch, applies the
  enqueued reservation deltas from the side threads, runs the checks, forwards the accepted
  requests into the ingress SPSC queue, and answers the user through the oneshot channels. The
  margin cache read is a clone of the swapped `Arc` snapshot (an atomic refcount), no locks, no
  blocking, no allocation on the hot path — the response and request buffers are pooled.
- **Margin feed thread** (1): subscribes to `svd:sync:margin`, applies the deltas to a fresh
  snapshot, swaps the `Arc`; runs the snapshot load on cold start.
- **Book feed thread** (1): subscribes to `svd:oms:{symbol_hex}:changes` and enqueues the
  reservation deltas (rejects and cancels) to the core thread.
- **Settlement feed thread** (1): subscribes to `svd:stl:{symbol_hex}:settlements` and enqueues
  the settlement deltas (releases and blocks) to the core thread.
- **HTTP acceptor threads** (N): the [NGINX] connections, they parse the requests and push them
  into the MPSC queue to the core thread.

The feeds are queued into the core thread rather than applied on the side threads, so the
reservation map has a single owner and the hot path reads never race a writer.

## The features in svd-pretrade crate
- config: a TOML config loaded on start and reloaded at runtime via SIGHUP, holding the symbol, the
  core id, the HTTP listen address, the ingress SPSC queue (path, capacity, create), the margin
  parameters (margin ratio bps, fee bps, quotePerTickLot, allow_unknown_accounts, the stale feed
  timeout), and the [Redis_Cluster] connections of the three feeds.
- signature verification: the EIP-712 order hash of doc/settlement-protocol.md, recovered and
  matched against the order's user; a forged order never reaches the book.
- the margin cache: the swapped snapshot of the synced margin state, with the cold start sequence
  (subscribe, load snapshot, apply the newer deltas) and the stale feed kill switch.
- the reservation engine: the per-account reservation map and the lifecycle of the section above,
  applied by the core thread from the local events and the side feeds.
- the margin gate: the `required ≤ available − reserved` check with the reject reasons
  `InsufficientMargin` and `MarginStateUnavailable`.
- the HTTP gateway: the `/api/v1/{symbol}/order` and `/api/v1/{symbol}/cancel` endpoints with the
  accept / reject responses, ready for the [NGINX] symbol routing.
