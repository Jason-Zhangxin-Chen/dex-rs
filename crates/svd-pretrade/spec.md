# The overview
Every symbol's [HotPath] starts at the [SVD_Pretrade]. It is the gateway of the hot path: it accepts the
user's HTTP requests routed by [NGINX] (the request path declares the symbol), verifies them, runs the
pre-trade checks — signature, order sanity, and the **on-chain margin pre-check** — and forwards the
accepted requests to the [SVD_OMS_Master] via the share memory SPSC queue implemented in the ipc crate.
A trader without sufficient on-chain margin balance is not allowed to trade anymore: their orders are
rejected at this gate, before they ever reach the book.

The checks run on the **HTTP handler threads** — the workers of the gateway. A handler parses the
request, runs the validations, and pushes the accepted request into a shared pre-allocated lock-free
MPSC queue; the **core thread** has one simple job: it drains batches from the MPSC queue and moves
them into the SPSC share memory queue wired to the [SVD_OMS_Master]. The core thread is pure data
forwarding — it never checks, never blocks and never allocates (see the hot path discipline
section).

The feeds also carry the correction path: when a settlement fails because of one side of a cross,
the [SVD_Settlement] removes the at-fault order and the [SVD_Pretrade] re-injects the **innocent
counterparty's crossed quantity** into the pipeline — the settlement result feed pushes the restore
into the shared MPSC queue and the core thread forwards it like any other message (see the data
flow section).

The pre-trade checks must run without stalling the pipeline, so the margin data never comes from a
synchronous chain RPC call at request time. The [SVD_Sync] service is the authoritative producer of
the margin state: it watches the on-chain [MarginAccount] of the settlement protocol and persists
the **latest state** of every account into the [Redis_Cluster] — a pub/sub channel for the live
updates and a key per account for the on-demand reads. The [SVD_Pretrade] keeps a local margin
cache within a lock free map, but only of the accounts that actually trade this market: on the
**first order of an account** the handler pulls the account's margin balance from the [Redis_Cluster]
key (a handler is a worker thread — the blocking pull is allowed there, the hot path is untouched),
and from then on the cache entry is kept fresh by the margin feed. The other instances of the
[SVD_Pretrade] keep their own subsets, so the system level state is not duplicated everywhere.
The design document of the assumed on-chain protocol lives in doc/settlement-protocol.md.

There is deliberately **no margin reservation mechanism**: the check uses the latest synced margin
state, so between two chain updates a trader may place orders whose sum exceeds the balance — the
tolerance is taken on purpose to keep the admission fast and simple. The on-chain settlement protocol
is the final arbiter: the trades that cannot settle are removed from the book and the account is
blocked (see the margin pre-check section). A reservation mechanism can be introduced later on top of
this design if the tolerance turns out to be expensive.

## The data flow
The [NGINX] gateway extracts the symbol from the request path and routes the request to the
[SVD_Pretrade] of the corresponding market. The HTTP handler threads run the whole validation
pipeline per request:

1. **Parse, signature, sanity** — these never wait.
2. **The margin gate** — a cancel bypasses it entirely and is forwarded straight away. An order
   reads the account's state from the shared margin cache; on the first order of an account (a
   cache miss) the handler pulls the account's key `svd:sync:margin:{account}` from the
   [Redis_Cluster] and attaches the account. A missing key means the account has no margin state
   on-chain yet — it attaches with a zero state. A pull that fails or exceeds
   `margin_pull_timeout_ms` rejects the order with `MarginStateUnavailable`. The attach is
   idempotent: two racing handlers pulling the same account insert the same state, the last one
   wins.
3. **Forward or reject** — an accepted request is pushed (as a `PipelineMsg::User`) into the
   shared pre-allocated lock-free MPSC queue and the handler responds accept; a rejected one
   responds with the reject reason and never reaches the queue.
4. **The forwarding loop** — the core thread drains the MPSC queue in batches (a pre-allocated
   batch buffer, reused every iteration) and moves the batch into the SPSC share memory queue wired
   to the [SVD_OMS_Master] — just forwarding, nothing else.

Two feeds keep the shared state fresh, none of them on the core thread:

- **The margin feed** — the margin feed thread subscribes to the `svd:sync:margin` channel and
  applies the updates to the shared margin cache; the messages carry **final states, not deltas** —
  the last one wins, no sequences to manage. A fresh update of a blocked account clears its block
  flag: the fresh state decides the admission on its own from then on. The feed also updates the
  liveness timestamp on every channel message and sweeps the cache (the idle eviction below).
- **The settlement result feed** — subscribes to `svd:stl:{symbol_hex}:settlements` of the
  [SVD_Settlement]. A reverted trade singles out one failing cross with an at-fault side, and the
  feed reacts twice. First it **re-injects the innocent side's crossed quantity into the
  pipeline**: when the trade failed because of the taker, the crossed maker order goes back into
  the book — the feed pushes a `RestoreOrder` of the maker's order with the failed cross's
  `traded_quantity` into the shared MPSC queue, from where the core thread forwards it to the
  [SVD_OMS_Master] like any other message (symmetrically, a maker at fault restores the taker's
  crossed quantity). The restore bypasses the margin gate — it re-enters an already-admitted
  state, not a new exposure — and its signature is re-verified on the feed thread before the
  re-injection. Second, when the failure is `InsufficientMargin`, the on-chain margin of the
  at-fault account is exhausted while the cached state may still be stale (a reverted transaction
  emits no chain event, so no sync update will correct the cache soon): the feed sets the block
  flag of the at-fault account of the failed trade. The block rejects the account's new orders
  (cancels still pass) until the fresh margin update arrives. Untracked accounts need no block:
  their first pull reads the fresh state anyway.
- **The heartbeat and the kill switch** — the [SVD_Sync] publishes a heartbeat on the margin channel
  every interval. When no channel message at all arrives within `margin_feed_stale_ms`, the feed is
  dead: the handlers reject the new orders (cancels still pass) until the channel resumes, because a
  silent margin feed must never mean "everyone has infinite margin".

The cache is bounded: every entry carries a last-used timestamp touched by each request of the
account, and the margin feed thread sweeps the entries idle beyond `account_idle_evict_ms` (or the
oldest ones beyond `max_tracked_accounts`). An evicted account re-attaches on its next order
through the same pull.

## Ingress Message and Outgress Message
The ingress from the [NGINX] is HTTP; the outgress to the [SVD_OMS_Master] is the share memory SPSC
queue of `PipelineMsg` — the union of the `OrderMsg` user requests and the settlement-driven
restores — defined in crates/primitives/src/message/hot_path.rs (the messages are fixed sized for
preallocation in share memory):

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

/// The messages of the pipeline between [SVD_Pretrade] and [SVD_OMS_Master]:
/// the validated user requests and the settlement-driven restores. Fixed
/// sized for preallocation in the share memory queues.
pub enum PipelineMsg {
    /// A user request (new order / cancel), validated by a handler thread.
    User(OrderMsg),
    /// Re-injects the crossed quantity of an innocent side: the trade failed
    /// to settle because of the other side, so this order's consumed
    /// quantity re-enters the book. The [SVD_OMS_Master] applies the same
    /// merge semantics as a rollback restore (see the settlement spec).
    RestoreOrder { order: Order, quantity: Quantity },
}
```

In between the handlers and the core thread the accepted `PipelineMsg`s travel through the shared
pre-allocated lock-free MPSC queue (the crossbeam `ArrayQueue`), and the core thread moves them
into the ingress SPSC queue wired to the [SVD_OMS_Master]. The queue carries fixed-size `Copy`
values — the pushes are plain memory copies into the pre-allocated slots — and the
[SVD_OMS_Master] drains the same `PipelineMsg` from its ingress queue: a `User` message executes as
the current `OrderMsg`, a `RestoreOrder` merges the quantity into the resting order or re-inserts
the order at the tail of its price level.

The feeds the [SVD_Pretrade] consumes are the messages the other services publish to the
[Redis_Cluster] (the wire format is MessagePack, rmp-serde):

- `MarginMsg` — the [SVD_Sync] margin feed, defined in crates/primitives/src/message/margin.rs (see
  the sync spec for the full definition): `Update(MarginChange)` carries the **final state** of one
  account (equity, used margin, available margin, block), `Heartbeat { block }` keeps the kill
  switch alive. The [SVD_Pretrade] applies an `Update` only to the cached accounts.
- `SettlementResult` — the [SVD_Settlement] results, defined in
  crates/primitives/src/message/settlement.rs (see the settlement spec). The [SVD_Pretrade] consumes
  the outcome twice: a `Reverted` result re-injects the innocent side's crossed quantity into the
  pipeline (the `RestoreOrder` of the failed trade's counterparty with the cross's
  `traded_quantity`), and a `Reverted { reason: Protocol(2) }` (insufficient margin) sets the block
  flag of the at-fault account until its next fresh margin update.

The rejection reasons reuse the `RejectReason` taxonomy of the primitives crate; two variants are
added for the margin gate (the enum is `#[non_exhaustive]`, so the appending is sanctioned):
`InsufficientMargin` (the check failed) and `MarginStateUnavailable` (no margin state for the
account — a failed or timed-out pull, rejected unless the config allows unknown accounts).

## The margin pre-check
The check is a plain comparison against the latest synced state of the account:

```
required_margin(order) ≤ available_synced(account)
```

- **available_synced** — the available margin of the account, the latest state in the shared cache:
  the pulled value at attach, then the last `MarginChange` received for the account. The on-chain
  state already nets the used margin out of the equity, so the check reads the available margin
  directly.
- **required_margin** — the worst-case exposure of the incoming order:
  `notional × margin_ratio_bps`, with `notional = price × quantity × quotePerTickLot` and the
  taker fee added. The config of the [SVD_Pretrade] must be kept in lockstep with the on-chain
  symbol config of the settlement protocol (an ops invariant — the on-chain values are the final
  arbiter, the off-chain check is an early conservative gate).

**No reservations** — the check trusts the synced state as it is. Between two chain updates a
trader may over-subscribe their balance with several orders; the tolerance is deliberate, and it
also covers the races of the concurrent handler threads checking the same account at the same time.
The on-chain settlement protocol is the final arbiter: the trades that cannot settle revert there,
the [SVD_Settlement] removes the account's orders from the book, and the [SVD_Pretrade] blocks the
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
- crossbeam-queue: the shared pre-allocated lock-free MPSC queue between the handler threads and
  the core thread (the `ArrayQueue`), added to the workspace dependencies — crates.io only,
  MIT/Apache-2.0, fits the deny policy.
- storage: the [Redis_Cluster] helpers in crates/storage for the on-demand account pulls (the GET of
  `svd:sync:margin:{account}`) and the channel subscriptions (a subscription uses a dedicated async
  cluster connection — the pooled client of the storage crate is for publishing).
- cryptography: the signature verification traits in crates/cryptography, with the EVM signature
  implementation over the EIP-712 order hash defined in doc/settlement-protocol.md (alloy provides
  the secp256k1 recovery).
- net: the HTTP server helpers in crates/net for the [NGINX]-facing gateway.

## The concurrency model of svd-pretrade crate
- **HTTP handler threads** (N): the [NGINX] connections; each handler runs the whole validation
  pipeline — the parsing, the signature verification, the order sanity, the margin gate with the
  on-demand pull — and pushes the accepted requests into the shared MPSC queue and answers the
  user. A handler is a worker thread: the parsing, the Redis pull and the serialization are
  allowed here; a full MPSC queue applies backpressure (the handler waits for the capacity, it
  never drops an accepted request).
- **Core thread** (1, pinned): the data forwarding loop — drains batches from the MPSC queue into
  a pre-allocated batch buffer and moves them into the SPSC share memory queue wired to the
  [SVD_OMS_Master]. No checks, no locks, no blocking, no allocation.
- **Margin feed thread** (1): subscribes to `svd:sync:margin`, applies the updates to the shared
  margin cache, updates the feed liveness timestamp, and sweeps the cache (the idle eviction).
- **Settlement feed thread** (1): subscribes to `svd:stl:{symbol_hex}:settlements`, re-injects the
  innocent side's crossed quantity of a failed trade into the shared MPSC queue (a
  `PipelineMsg::RestoreOrder`, re-verified before the push), and sets the block flag of the
  at-fault account.
- **The shared margin cache**: a sharded concurrent map (dashmap-style) whose entries carry the
  margin state, an atomic block flag and an atomic last-used timestamp. The handler reads are
  cheap shard-local reads; the writes — the feed updates and the handler attaches — are rare and
  serialized per entry, and the attached state is idempotent (two racing pulls insert the same
  state).

## The hot path discipline
The core thread follows the same **no allocation rule** as the [SVD_OMS_Master] core thread: **no
heap allocation and no blocking call at runtime** — and the rule is easy to keep because the core
thread does one job:

- **The forwarding loop** — the core thread drains the MPSC queue into a batch buffer allocated
  once at startup with the configured batch size and reused every iteration (the same pattern as
  the master's spin loop batch), and moves the batch into the SPSC share memory queue — the user
  requests and the settlement-driven restores alike. `PipelineMsg` is a fixed-size `Copy` value,
  so every move is a plain memory copy into the pre-allocated queue slots.
- **The shared MPSC queue** — the queue between the handlers and the core thread is a pre-allocated
  bounded lock-free queue (the crossbeam `ArrayQueue`): the capacity is fixed at startup, the
  pushes and the pops are lock-free, and a full queue applies backpressure to the handlers instead
  of growing.
- **The shared margin cache** — the sharded concurrent map is pre-sized to `max_tracked_accounts`
  at startup and the entries are fixed-size records: an update overwrites an entry in place, an
  attach inserts an entry into the existing capacity — nothing grows on the hot path.

All the work that is allowed to allocate or block — the HTTP parsing, the signature verification
and the margin computation, the Redis pulls, the wire serialization, the channel subscriptions —
lives on the handler threads and the feed threads, never on the core thread.

## The features in svd-pretrade crate
- config: a TOML config loaded on start and reloaded at runtime via SIGHUP, holding the symbol, the
  core id, the HTTP listen address, the MPSC queue capacity, the ingress SPSC queue (path,
  capacity, create), the margin parameters (margin ratio bps, fee bps, quotePerTickLot,
  allow_unknown_accounts, the pull timeout, the stale feed timeout, the cache bounds and the
  eviction), and the [Redis_Cluster] connections of the feeds.
- signature verification: the EIP-712 order hash of doc/settlement-protocol.md, recovered and
  matched against the order's user; a forged order never reaches the queue.
- the on-demand margin cache: the pull-on-first-attach run inline on the handler (a worker thread —
  the hot path is untouched), the channel updates applied per cached account (final states, last
  one wins), and the idle / capacity eviction with the re-attach on the next order.
- the margin gate: the `required ≤ available` check with the reject reasons `InsufficientMargin`
  and `MarginStateUnavailable`, the settlement-driven block flags of the at-fault account cleared
  by the next fresh margin update, and the stale feed kill switch on the heartbeats.
- the correction path: the settlement result feed's re-injection of the innocent side's crossed
  quantity (the `PipelineMsg::RestoreOrder` pushed into the shared MPSC queue, bypassing the
  margin gate and re-verified on the feed thread).
- the forwarding core: the shared pre-allocated lock-free MPSC queue and the core thread's batch
  forwarding loop into the SPSC share memory queue, forwarding the user requests and the
  settlement-driven restores alike.
- the HTTP gateway: the `/api/v1/{symbol}/order` and `/api/v1/{symbol}/cancel` endpoints with the
  accept / reject responses, ready for the [NGINX] symbol routing.
