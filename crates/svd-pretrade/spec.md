# The overview
Every symbol's [HotPath] starts at the [SVD_Pretrade]. It is the gateway of the hot path: it accepts the
user's HTTP requests routed by [NGINX] (the request path declares the symbol), verifies them, runs the
pre-trade checks — signature, order sanity, and the **on-chain margin pre-check** — and forwards the
accepted requests to the [SVD_OMS_Master] via the share memory SPSC queue implemented in the ipc crate.
A trader without sufficient on-chain margin balance is not allowed to trade anymore: their orders are
rejected at this gate, before they ever reach the book.

Like every hot path service, the core thread obeys the **no allocation rule**: no heap allocation
and no blocking call at runtime — every structure it touches is pre-allocated and reused, the same
discipline as the [SVD_OMS_Master] core thread (see the hot path discipline section).

The pre-trade checks must run without blocking on the hot path, so the margin data never comes from a
synchronous chain RPC call or a Redis read at request time. The [SVD_Sync] service is the authoritative
producer of the margin state: it watches the on-chain [MarginAccount] of the settlement protocol and
persists the **latest state** of every account into the [Redis_Cluster] — a pub/sub channel for the
live updates and a key per account for the on-demand reads. The [SVD_Pretrade] keeps a local margin
cache, but only of the accounts that actually trade this market: on the **first order of an account**
it pulls the account's margin balance from the [Redis_Cluster] key, and from then on it applies the
channel updates of that account. The other instances of the [SVD_Pretrade] keep their own subsets, so
the system level state is not duplicated everywhere. The design document of the assumed on-chain
protocol lives in doc/settlement-protocol.md.

There is deliberately **no margin reservation mechanism**: the check uses the latest synced margin
state, so between two chain updates a trader may place orders whose sum exceeds the balance — the
tolerance is taken on purpose to keep the admission fast and simple. The on-chain settlement protocol
is the final arbiter: the trades that cannot settle are removed from the book and the account is
blocked (see the margin pre-check section). A reservation mechanism can be introduced later on top of
this design if the tolerance turns out to be expensive.

## The data flow
The [NGINX] gateway extracts the symbol from the request path and routes the request to the
[SVD_Pretrade] of the corresponding market. The HTTP acceptor threads of the [SVD_Pretrade] push the
requests into an MPSC queue to the core thread; the core thread runs a spin loop which drains the
request batch, runs the checks against the margin cache, forwards the accepted requests to the
[SVD_OMS_Master] through the share memory SPSC queue, and responds to the user end through a oneshot
channel: an accepted order gets an immediate accept response, a rejected one gets the reject reason,
and the reject never reaches the book.

On the first request of an account the margin state is not in the cache yet. The core thread cannot
block on a Redis read, so the request is **deferred** — it is queued under the account and a pull is
handed to the margin feed thread; when the pull lands the queued requests are checked and forwarded:

1. The core thread verifies the signature and the order sanity (these never wait; cancels bypass the
   margin gate entirely and are forwarded straight away).
2. A cache miss registers the account as `pulling` and queues the request under it; the margin feed
   thread GETs the account's key `svd:sync:margin:{account}` from the [Redis_Cluster]. A missing key
   means the account has no margin state on-chain yet — it attaches with a zero state.
3. When the pull lands, the core thread attaches the account (inserts the state into the cache) and
   drains the queued requests through the normal checks. A pull that fails or exceeds
   `margin_pull_timeout_ms` rejects the queued requests with `MarginStateUnavailable`; the account
   stays unattached, so the next request retriggers the pull.
4. From then on the account's state is kept fresh by the margin feed: the margin feed thread
   subscribes to the `svd:sync:margin` channel and enqueues the channel messages to the core
   thread, which overwrites the entries of the cached accounts and drops the rest. The messages
   carry **final states, not deltas** — the last one wins, no sequences to manage.

Two more mechanisms complete the picture:

- **The settlement result feed** — the [SVD_Pretrade] subscribes to
  `svd:stl:{symbol_hex}:settlements` of the [SVD_Settlement]. A reverted batch with
  `InsufficientMargin` means the on-chain margin of an account is exhausted while the cached state
  may still be stale (a reverted transaction emits no chain event, so no sync update will correct
  the cache soon): the core thread blocks the cached accounts of the failed trades. The block
  rejects the account's new orders (cancels still pass) until a fresh margin update arrives for it —
  the fresh state clears the block and decides the admission on its own from then on. Untracked
  accounts need no block: their first pull reads the fresh state anyway.
- **The heartbeat and the kill switch** — the [SVD_Sync] publishes a heartbeat on the margin channel
  every interval. When no channel message at all arrives within `margin_feed_stale_ms`, the feed is
  dead: the [SVD_Pretrade] rejects the new orders (cancels still pass) until the channel resumes,
  because a silent margin feed must never mean "everyone has infinite margin".

The cache is bounded: every entry carries a last-used timestamp touched by each request of the
account, and a periodic sweep on the core loop evicts the entries idle beyond `account_idle_evict_ms`
(or the oldest ones beyond `max_tracked_accounts`). An evicted account re-attaches on its next order
through the same pull.

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

- `MarginMsg` — the [SVD_Sync] margin feed, defined in crates/primitives/src/message/margin.rs (see
  the sync spec for the full definition): `Update(MarginChange)` carries the **final state** of one
  account (equity, used margin, available margin, block), `Heartbeat { block }` keeps the kill
  switch alive. The [SVD_Pretrade] applies an `Update` only to the cached accounts.
- `SettlementResult` — the [SVD_Settlement] results, defined in
  crates/primitives/src/message/settlement.rs (see the settlement spec). The [SVD_Pretrade] consumes
  only the outcome: `Reverted { reason: Protocol(2) }` blocks the accounts of the batch's trades
  until their next fresh margin update.

The rejection reasons reuse the `RejectReason` taxonomy of the primitives crate; two variants are
added for the margin gate (the enum is `#[non_exhaustive]`, so the appending is sanctioned):
`InsufficientMargin` (the check failed) and `MarginStateUnavailable` (no margin state for the
account — a failed or timed-out pull, rejected unless the config allows unknown accounts).

## The margin pre-check
The check is a plain comparison against the latest synced state of the account:

```
required_margin(order) ≤ available_synced(account)
```

- **available_synced** — the available margin of the account, the latest state in the local cache:
  the pulled value at attach, then the last `MarginChange` received for the account. The on-chain
  state already nets the used margin out of the equity, so the check reads the available margin
  directly.
- **required_margin** — the worst-case exposure of the incoming order:
  `notional × margin_ratio_bps`, with `notional = price × quantity × quotePerTickLot` and the
  taker fee added. The config of the [SVD_Pretrade] must be kept in lockstep with the on-chain
  symbol config of the settlement protocol (an ops invariant — the on-chain values are the final
  arbiter, the off-chain check is an early conservative gate).

**No reservations** — the check trusts the synced state as it is. Between two chain updates a
trader may over-subscribe their balance with several orders; the tolerance is deliberate. The
on-chain settlement protocol is the final arbiter: the trades that cannot settle revert there, the
[SVD_Settlement] removes the account's orders from the book, and the [SVD_Pretrade] blocks the
account from the settlement result until a fresh margin update arrives. A reservation mechanism
can be introduced later on top of this design if the tolerance turns out to be expensive.

Three safety valves bound the behavior of the gate:

- **Cancels always pass** — a cancel never needs margin and is forwarded without the check, even
  for a blocked or unattached account: a trader must always be able to pull their orders.
- **The stale feed kill switch** — no channel message (updates or heartbeats) within
  `margin_feed_stale_ms` rejects the new orders until the feed resumes.
- **Unknown accounts** — an account without a cache entry goes through the pull; a pull that fails
  rejects the request with `MarginStateUnavailable` unless `allow_unknown_accounts` is set (the
  escape hatch for a bootstrapping market).

## The dependency of svd-pretrade crate
- primitives: the `OrderMsg`, `Order`, `RejectReason` types and the value types, all in
  crates/primitives.
- ipc: the share memory SPSC queue in crates/ipc, wired to the [SVD_OMS_Master] ingress.
- storage: the [Redis_Cluster] helpers in crates/storage for the on-demand account pulls (the GET of
  `svd:sync:margin:{account}`) and the channel subscriptions (a subscription uses a dedicated async
  cluster connection — the pooled client of the storage crate is for publishing).
- cryptography: the signature verification traits in crates/cryptography, with the EVM signature
  implementation over the EIP-712 order hash defined in doc/settlement-protocol.md (alloy provides
  the secp256k1 recovery).
- net: the HTTP server helpers in crates/net for the [NGINX]-facing gateway.

## The concurrency model of svd-pretrade crate
- **Core thread** (1, pinned): the single owner of the margin cache, the pending pulls and the
  blocked set. Its spin loop drains the HTTP request batch, applies the enqueued feed deltas (margin
  updates, pull results, block signals) between the batches, runs the checks, forwards the accepted
  requests into the ingress SPSC queue, answers the user through the oneshot channels, and sweeps
  the cache (the pull deadlines and the eviction). The margin cache reads are plain lookups in the
  map it owns — no locks, no blocking, no allocation at runtime (see the hot path discipline
  section).
- **Margin feed thread** (1): subscribes to `svd:sync:margin`, enqueues the updates and the
  heartbeats to the core thread, and serves the on-demand pulls (the GET of the account key) that
  the core thread hands it.
- **Settlement feed thread** (1): subscribes to `svd:stl:{symbol_hex}:settlements` and enqueues the
  block signals to the core thread.
- **HTTP acceptor threads** (N): the [NGINX] connections, they parse the requests and push them
  into the MPSC queue to the core thread.

## The hot path discipline
The core thread follows the same **no allocation rule** as the [SVD_OMS_Master] core thread: **no
heap allocation and no blocking call at runtime** — every structure it touches is pre-allocated and
reused, the same way the master pre-allocates and reuses its resources:

- **The request batch** — the buffer the core loop drains the acceptor MPSC queue into is
  allocated once at startup with the configured batch size and reused every iteration, the same
  pattern as the master's spin loop batch.
- **The pooled buffers** — the requests and the responses travel in buffers checked out from the
  object pools of the cache crate (`Cache` / `CacheGuard`), sized to the configured worst case so
  a steady-state checkout never allocates. The HTTP acceptors fill the request buffers and write
  the response buffers on their own threads, so the core thread never touches the wire format;
  the buffers return to the pools after the responses are written.
- **The margin cache** — the map is pre-sized to `max_tracked_accounts` at startup and the entries
  are fixed-size records: an update overwrites an entry in place, an attach moves a record
  prepared by the margin feed thread into an existing slot — nothing grows on the hot path.
- **The pending queues** — the per-account deferred request queues are bounded by
  `max_pending_orders_per_account`; the overflow is rejected (an application reject code) instead
  of growing the queue.
- **The check itself** — the signature verification and the margin computation work on stack
  buffers and integer math; a cache miss never allocates on the core thread: the request is parked
  in a pre-allocated pending slot and the Redis pull runs on the margin feed thread.

Everything that is allowed to allocate or block — the HTTP parsing, the wire serialization, the
Redis reads and the channel subscriptions — runs on the acceptor threads and the feed threads,
never on the core thread.

## The features in svd-pretrade crate
- config: a TOML config loaded on start and reloaded at runtime via SIGHUP, holding the symbol, the
  core id, the HTTP listen address, the ingress SPSC queue (path, capacity, create), the margin
  parameters (margin ratio bps, fee bps, quotePerTickLot, allow_unknown_accounts, the pull timeout,
  the stale feed timeout, the cache bounds, the eviction, the per-account pending bound), and the
  [Redis_Cluster] connections of the feeds.
- signature verification: the EIP-712 order hash of doc/settlement-protocol.md, recovered and
  matched against the order's user; a forged order never reaches the book.
- the on-demand margin cache: the pull-on-first-attach flow with the deferred request queue and the
  pull timeout, the channel updates applied per cached account (final states, last one wins), and
  the idle / capacity eviction with the re-attach on the next order.
- the margin gate: the `required ≤ available` check with the reject reasons `InsufficientMargin`
  and `MarginStateUnavailable`, the settlement-driven block set cleared by the next fresh margin
  update, and the stale feed kill switch on the heartbeats.
- the HTTP gateway: the `/api/v1/{symbol}/order` and `/api/v1/{symbol}/cancel` endpoints with the
  accept / reject responses, ready for the [NGINX] symbol routing.
