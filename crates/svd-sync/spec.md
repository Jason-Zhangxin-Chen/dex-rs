# The overview
The [SVD_Sync] is the bridge between the on-chain settlement protocol and the storage clusters: it
listens to the margin account's state changes on the chain and synces them into the [Redis_Cluster]
and the [SQL_Cluster]. It is the **authoritative producer of the margin state** in the system — the
[SVD_Pretrade] instances of every symbol build their local margin caches from its publications, and
the [SVD_PubSub] cluster relays the margin position changes to the subscribed users. Nobody else
publishes margin state; the [SVD_Settlement] publishes settlement results (its own channel), never
margin numbers.

Unlike the other services, the [SVD_Sync] is **not symbol-scoped**: the margin account of the
settlement protocol is global, one account trades every symbol against the same equity. The
[SVD_Sync] watches the [MarginAccount] and [Settlement] contracts (the assumed interfaces in
doc/settlement-protocol.md) and publishes the account changes to a global channel. One instance
serves the whole chain; for scale it shards by account address ranges (see the features section).

The chain watch is asynchronous and reorganizations happen: the [SVD_Sync] buffers the events by
confirmation depth, journals the applied events to a local log with a checkpoint, and recovers by
snapshot + replay, mirroring the state recovery design of the [SVD_OMS_Slave].

## The data flow
The chain subscriber connects to the [Web3RPCNodes] with an alloy WebSocket provider and subscribes
to the `MarginAccountUpdated` event of the [MarginAccount] contract (plus `MarginDeposited` /
`MarginWithdrawn` for the deposit / withdraw detail). The event already carries the complete new
state — equity, used margin, available margin — so one event is enough to rebuild the account's
state; the [SVD_Sync] never calls back into the chain for a balance on the hot loop, the events are
the data.

The events flow through the pipeline:

1. **The pending buffer** — the subscriber decodes the raw logs and pushes them into a pending
   buffer ordered by `(block, log index)`. Nothing is published from the pending buffer yet.
2. **The confirmer** — a thread tracks the chain head and promotes the pending events to the
   applier once their block reaches the configured confirmation depth (default 3). The depth is
   the tradeoff: deeper is safer against reorganizations, shallower publishes the margin state
   sooner; the on-chain margin is the final arbiter either way.
3. **The applier** — the applier owns the margin cache. For each promoted event it assigns the
   next journal sequence, applies the delta to the cache, appends the event to the local journal
   (the journal is the durable log of the applied events), publishes the `MarginChange` to the
   [Redis_Cluster] channel `svd:sync:margin`, and hands the row to the [SQL_Cluster] writer. The
   publication carries the sequence, so the subscribers can deduplicate by it.
4. **The snapshot thread** — every snapshot interval the applier's cache is snapshotted with its
   current sequence and persisted to the [Redis_Cluster] under `svd:sync:margin:snapshot` (and to
   the journal). The snapshot bounds the cold start and the recovery of the subscribers and of the
   [SVD_Sync] itself.
5. **The SQL writer** — a side thread batches the margin rows into upserts of the
   `margin_accounts` table (account, equity, used margin, available margin, block, updated at).

A reorganization is detected by the confirmer: the tracked block hash at the confirmation depth
does not match the canonical chain anymore. The [SVD_Sync] then rolls the state back — it
invalidates the applied events of the dropped blocks from the cache, rewinds the journal to the
checkpoint of the first dropped block, and replays the chain's replacement blocks (fetched via
`eth_getLogs` on the RPC). The rollback is rare and off the hot loop; the margin cache is small (one
entry per account), so the rewind is cheap.

## Ingress Message and Outgress Message
The ingress is the chain itself — the contract events of doc/settlement-protocol.md, decoded with
alloy. The outgress is the `MarginChange` message published to the [Redis_Cluster] (MessagePack on
the wire, rmp-serde), to be defined in crates/primitives/src/message/margin.rs:

```Rust
/// The margin state delta of one account, published by [SVD_Sync] to the
/// [Redis_Cluster] channel `svd:sync:margin`. The [SVD_Pretrade] instances of
/// every symbol subscribe to the channel and apply the deltas to their local
/// margin caches; the [SVD_PubSub] cluster relays them to the users.
pub struct MarginChange {
    /// The monotonic sequence assigned by the [SVD_Sync] journal. The
    /// subscribers apply a delta only once by the sequence, and drop the
    /// older ones (at-least-once delivery of the channel).
    pub seq: u64,
    /// The account of the settlement protocol.
    pub account: Address,
    /// The equity of the account, quote asset units.
    pub equity: u128,
    /// The margin used by the account's open positions.
    pub used_margin: u128,
    /// The available margin, equity minus used.
    pub available: u128,
    /// The block number the state comes from.
    pub block: u64,
}
```

The journal entry of the applied event carries the same payload plus the block hash and the log
index, so the checkpoint (block, log index, sequence) is enough to rewind and replay.

## The dependency of svd-sync crate
- alloy: the EVM transport (WebSocket subscription, `eth_getLogs` polling fallback) and the ABI
  decoding of the contract events — the workspace already pins alloy for chain interop.
- primitives: the address and value types of crates/primitives, and the `MarginChange` message
  defined there.
- storage: the [Redis_Cluster] helpers of crates/storage for the change channel and the snapshot
  key (the publish side only — the [SVD_Sync] produces, it never subscribes).
- journal: the local journal is the same design as the [SVD_OMS_Slave] journal in crates/svd-oms
  (memory-mapped file, dual header metadata for the corruption detection, flushed touched ranges).
  The implementation should be extracted into a shared crate (the storage crate is the natural
  home) so both services maintain one copy of the format.
- net: the RPC helper bits of crates/net (endpoint failover, keepalive).

## The concurrency model of svd-sync crate
- **Chain subscriber thread** (1, async): the alloy WebSocket subscription, decodes the logs and
  pushes them into the pending buffer.
- **Confirmer thread** (1, async): tracks the head, promotes the confirmed events to the applier,
  detects the reorganizations and drives the rollback.
- **Applier thread** (1, pinned optional): owns the margin cache and the journal; applies the
  promoted events, appends the journal, publishes the [Redis_Cluster] channel and hands the rows to
  the SQL writer. The [Redis_Cluster] publish is a side-path I/O, it may block — the applier is not
  on any hot path, but a slow publication must not stall the chain watch, so the applier's inbox is
  an unbounded queue the confirmer never blocks on.
- **Snapshot thread** (1): snapshots the cache on the configured cadence and persists it to the
  journal and the [Redis_Cluster].
- **SQL writer thread** (1): batches the margin rows into upserts with retries.

## The features in svd-sync crate
- config: a TOML config loaded on start and reloadable via SIGHUP, holding the RPC endpoints
  (WebSocket + HTTPS), the contract addresses, the start block, the confirmation depth, the
  snapshot interval, the journal path, the [Redis_Cluster] and [SQL_Cluster] connections, and the
  shard of the instance (an account range). The contract address and the start block are
  reloadable to move the watch point.
- event decoding: the alloy ABI decoding of `MarginAccountUpdated`, `MarginDeposited` and
  `MarginWithdrawn`, with the tolerance for the unknown topics (the protocol is not frozen — a new
  event is skipped and logged, never a crash).
- the confirmation pipeline: the pending buffer, the confirmation depth, and the reorg detection
  with the rollback described above.
- the journal: the applied events with the (block, log index, sequence) checkpoint, the same dual
  header corruption handling as the [SVD_OMS_Slave] journal.
- snapshot + recovery: on restart the [SVD_Sync] loads the latest snapshot from the journal (or the
  [Redis_Cluster]), replays the journal entries newer than the snapshot sequence, then continues
  the chain watch from the last applied block plus one. The replay is deterministic — the events
  are applied by their order again.
- the sharding: when one instance is not enough, the [SVD_Sync] shards by account address range
  (address mod N). Every shard publishes to its own channel `svd:sync:margin:{shard}` and keeps its
  own snapshot; the subscribers (the [SVD_Pretrade] instances) subscribe to all the shard channels
  and merge the feeds. A single instance is the default and needs no shard config.
- the SQL writer: the batched upserts of the `margin_accounts` table with the retry and the
  backoff, so the [SQL_Cluster] outage never stalls the [Redis_Cluster] publications.
