# The overview
The [SVD_Sync] is the bridge between the on-chain settlement protocol and the storage clusters: it
listens to the margin account's state changes on the chain and synces them into the [Redis_Cluster]
and the [SQL_Cluster]. It is the **authoritative producer of the margin state** in the system — the
[SVD_Pretrade] instances of every symbol build their local margin caches from its publications, and
the [SVD_PubSub] cluster relays the margin position changes to the subscribed users. Nobody else
publishes margin state; the [SVD_Settlement] publishes settlement results (its own channel), never
margin numbers.

The feed carries **final states, not deltas**: every message is the latest margin state of one
account, the subscribers overwrite their entry for the account and always hold the latest view —
there are no per-message sequences to manage, no dedup, no snapshot + sequence gap logic. The
[SVD_Sync] tracks its own publication position by the **block height and the log index** — the
watermark guard of the data flow section — so the history re-delivered by a delayed [Web3RPCNodes]
node is dropped, never republished. The [SVD_Sync]
persists the state in two shapes into the [Redis_Cluster]: the pub/sub channel `svd:sync:margin`
carries the live updates, and a key per account (`svd:sync:margin:{account}`) holds the latest
state for the on-demand reads — the [SVD_Pretrade] pulls an account's balance from the key on the
account's first order in a market, so the instances only subscribe to the accounts that trade their
market (see the pre-trade spec). The per-account keys **are the snapshot**: there is no separate
snapshot and no sequence bookkeeping, which simplifies the recovery of the [SVD_Sync] itself too.

Unlike the other services, the [SVD_Sync] is **not symbol-scoped**: the margin account of the
settlement protocol is global, one account trades every symbol against the same equity. The
[SVD_Sync] watches the [MarginAccount] and [Settlement] contracts (the assumed interfaces in
doc/settlement-protocol.md). One instance serves the whole chain; for scale it shards by account
address ranges (see the features section).

# The data flow
The chain subscriber connects to a **pool of [Web3RPCNodes]** — one primary node and the
secondaries — with alloy WebSocket providers, and subscribes to the `MarginAccountUpdated` event of
the [MarginAccount] contract (plus `MarginDeposited` / `MarginWithdrawn` for the deposit / withdraw
detail) on the primary. When the primary goes off — a connection failure or a head that stalls
beyond `node_stall_timeout_ms` — the subscriber **fails over** to the next secondary: it
re-subscribes there, re-fetches the range from the watermark to the secondary's head via
`eth_getLogs` (the secondary may lag behind the primary's view — the nodes may see the chain at
different heights due to the delays), and resumes. The re-delivered history is dropped by the
watermark guard below, the missing range is applied. The event already carries the complete new
state — equity, used margin, available margin — so one event is enough to rebuild the account's
state; the [SVD_Sync] never calls back into the chain for a balance on the hot loop, the events are
the data.

The events flow through the pipeline:

1. **The pending buffer** — the subscriber decodes the raw logs and pushes them into a pending
   buffer ordered by `(block, log index)`. Nothing is published from the pending buffer yet.
2. **The confirmer** — a thread tracks the chain head and promotes the pending events to the
   applier once their block reaches the configured confirmation depth (default 3). The depth is
   the tradeoff: deeper is safer against reorganizations, shallower publishes the margin state
   sooner; the on-chain margin is the final arbiter either way. The confirmer also publishes the
   heartbeat on the margin channel every interval, carrying the confirmed block number — the
   subscribers use it to detect a dead feed — and it watches the liveness of the node: a stalled
   head triggers the failover. A secondary that lags behind the watermark is waited for, never
   rewound: the [SVD_Sync] resumes publishing once the node catches up.
3. **The applier** — for each promoted event the applier updates the per-account key
   `svd:sync:margin:{account}` in the [Redis_Cluster] (the SET of the serialized latest state), and
   publishes the `MarginChange` to the channel `svd:sync:margin`, then hands the row to the
   [SQL_Cluster] writer. The publishes are idempotent by nature: the same final state published
   twice is harmless, the subscribers just overwrite twice.
4. **The watermark** — the applied `(block height, log index)` is persisted to the local journal
   (the same memory-mapped dual header design as the [SVD_OMS_Slave] journal, now with a tiny
   payload). The watermark is the **publication guard**: an event is applied only when its `(block,
   log index)` is strictly beyond the watermark — the [SVD_Sync] bases its position on the block
   height and the log index, so the history re-delivered by a delayed or a switched RPC node (the
   nodes may have different views of the chain due to the delays) is dropped, never republished.
   The watermark advances monotonically and never moves backward. The publish happens before the
   watermark persist: a crash window may repeat the last events, which the final-state semantics
   absorb. The watermark is the only thing the [SVD_Sync] needs to recover: the chain is the source
   of truth, the per-account keys are the snapshot, so there is no snapshot thread and no sequence.
5. **The SQL writer** — a side thread batches the margin rows into upserts of the
   `margin_accounts` table (account, equity, used margin, available margin, block, updated at).

A reorganization is detected by the confirmer: the tracked block hash at the confirmation depth
does not match the canonical chain anymore. A switched node that merely lags does not look like a
reorganization — its head number is below the tracked head, so the confirmer waits for it to catch
up and the watermark never rewinds; only a hash mismatch at the tracked depth is one. On a real
reorganization the events of the dropped blocks name the affected accounts; the [SVD_Sync] re-reads
their state from the chain (the contract views at the current head) and republishes the correct
final states — a deliberate correction, exempt from the watermark guard, no rollback ledger is
needed.

# Ingress Message and Outgress Message
The ingress is the chain itself — the contract events of doc/settlement-protocol.md, decoded with
alloy. The outgress is the margin feed published to the [Redis_Cluster] (MessagePack on the wire,
rmp-serde), defined in crates/primitives/src/message/margin.rs:

```Rust
/// The messages on the `svd:sync:margin` channel, published by [SVD_Sync].
/// The channel is global (not per symbol): the margin account of the
/// settlement protocol backs every market, and the subscribers keep their
/// own subsets of it.
pub enum MarginMsg {
    /// The latest margin state of one account — a final state, not a delta.
    /// The subscribers overwrite their entry for the account; the last
    /// message wins, so a republished state is harmless.
    Update(MarginChange),
    /// A periodic heartbeat carrying the confirmed block number; the
    /// subscribers use it to detect a dead feed.
    Heartbeat { block: u64 },
}

/// The latest margin state of one account.
pub struct MarginChange {
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

The same state is persisted under the per-account key `svd:sync:margin:{account}` (the account
serialized as a hex address), which is the on-demand read path of the subscribers.

# The dependency of svd-sync crate
- alloy: the EVM transport (WebSocket subscription, `eth_getLogs` polling fallback), the ABI
  decoding of the contract events and the state reads for the reorg correction — the workspace
  already pins alloy for chain interop.
- primitives: the address and value types of crates/primitives, and the `MarginMsg` /
  `MarginChange` messages defined there.
- storage: the [Redis_Cluster] helpers of crates/storage for the change channel and the per-account
  state keys (the publish side only — the [SVD_Sync] produces, it never subscribes). The storage
  crate gains a keyed store helper beside the channel publish (the SET / GET of the per-account
  keys).
- journal: the watermark journal is the same design as the [SVD_OMS_Slave] journal in crates/svd-oms
  (memory-mapped file, dual header metadata for the corruption detection, flushed touched ranges).
  The implementation should be extracted into a shared crate (the storage crate is the natural
  home) so both services maintain one copy of the format.
- net: the RPC helper bits of crates/net (the node pool failover, keepalive).

# The concurrency model of svd-sync crate
- **Chain subscriber thread** (1, async): connects to the primary of the RPC pool with the alloy
  WebSocket subscription; on a connection failure or a stall it fails over to the next secondary —
  re-subscribes, re-fetches the lag range from the watermark, and resumes. It decodes the logs and
  pushes them into the pending buffer.
- **Confirmer thread** (1, async): tracks the head, promotes the confirmed events to the applier,
  publishes the heartbeat on the margin channel, detects the stalled nodes and triggers the
  failover, waits for a lagging secondary to catch up (the watermark never rewinds), and detects
  the reorganizations with the correction described above.
- **Applier thread** (1, pinned optional): applies the promoted events — the per-account key SET,
  the channel publish and the watermark journal — and hands the rows to the SQL writer. The
  [Redis_Cluster] publishes are side-path I/O, they may block — the applier is not on any hot path,
  but a slow publication must not stall the chain watch, so the applier's inbox is an unbounded
  queue the confirmer never blocks on.
- **SQL writer thread** (1): batches the margin rows into upserts with retries.

# The features in svd-sync crate
- config: a TOML config loaded on start and reloadable via SIGHUP, holding the RPC node pool — one
  primary and the secondaries in the failover order, each with its WebSocket + HTTPS endpoints —
  the connection and node stall timeouts, the contract addresses, the start block, the
  confirmation depth, the heartbeat interval, the journal path, the [Redis_Cluster] and
  [SQL_Cluster] connections, and the shard of the instance (an account range). The contract
  address and the start block are reloadable to move the watch point.
- event decoding: the alloy ABI decoding of `MarginAccountUpdated`, `MarginDeposited` and
  `MarginWithdrawn`, with the tolerance for the unknown topics (the protocol is not frozen — a new
  event is skipped and logged, never a crash).
- the confirmation pipeline: the pending buffer, the confirmation depth, and the reorg detection
  with the chain-state correction described above.
- the node failover: the RPC pool with one primary and the secondaries, the failover on a
  connection failure or a stalled head, the lag re-fetch from the watermark and the catch-up wait
  (the nodes may have different views of the chain due to the delays, the watermark guard absorbs
  the difference).
- the watermark journal: the applied (block height, log index) checkpoint — the position the
  [SVD_Sync] bases its publications on — with the same dual header corruption handling as the
  [SVD_OMS_Slave] journal. The watermark is the publication guard: an event is applied only when
  its (block, log index) is strictly beyond it, so the history re-delivered by a delayed or a
  switched RPC node is dropped, never republished. On restart the [SVD_Sync] reads the watermark,
  re-fetches the range from the watermark block (`eth_getLogs` — the chain is the source of truth)
  and applies the events beyond it; the publish happens before the persist, so a crash window may
  repeat the last events, which the final-state semantics absorb.
- the per-account persistence: the `svd:sync:margin:{account}` keys — the snapshot of the system —
  and the `svd:sync:margin` channel with the heartbeat.
- the sharding: when one instance is not enough, the [SVD_Sync] shards by account address range
  (address mod N). Every shard publishes to its own channel `svd:sync:margin:{shard}` and keeps its
  own watermark; the per-account keys stay the global keys of the cluster. The subscribers (the
  [SVD_Pretrade] instances) subscribe to all the shard channels. A single instance is the default
  and needs no shard config.
- the SQL writer: the batched upserts of the `margin_accounts` table with the retry and the
  backoff, so the [SQL_Cluster] outage never stalls the [Redis_Cluster] publications.
