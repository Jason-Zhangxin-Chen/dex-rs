# The overview
The [SVD_Settlement] is the last hop of the [HotPath] and the bridge back to the chain: it drains
the trade events of the [SVD_OMS_Master] from the share memory SPSC queue, assembles them into
batches on the core thread, and a pool of submitters submits the batches to the on-chain
settlement protocol (the assumed interfaces in doc/settlement-protocol.md), listens to the
results, and decides what the result means for the book. A settled trade is final. A failed
trade is classified: the deterministic failures remove the orders (they must never trade again —
forged signature, exhausted margin), and the transient failures are retried until they resolve —
the [SVD_Settlement] never drops a batch, so the book stays the source of truth for the pending
crosses while the chain recovers. Every result is published to the [Redis_Cluster] and the
[SQL_Cluster], because the downstream services depend on it: the [SVD_Pretrade] consumes the
results threefold — it re-injects the **innocent side's crossed quantity** of a failed trade into
the pre-trade pipeline (a trade failed because of one side, the innocent counterparty's order
goes back into the book through the pipeline), it removes the **at-fault side's orders** from the
book through the same pipeline (a targeted cancel for a deterministic failure of one order, a
mass cancel for an exhausted margin), and it blocks the at-fault account when the failure is
`InsufficientMargin` (the on-chain margin is exhausted while the cached margin state may still be
stale) — the [SVD_PubSub] relays the settlement status to the users, and the [SVD_Sync] picks up
the authoritative margin changes from the chain events the settlement produced.

The submission is the slow I/O of the system (transaction building, gas estimation, mempool,
confirmations), so everything past the submitter queues is asynchronous — the [HotPath] ends at
the core thread's push into a submitter queue, which never blocks and never allocates in the
steady state: the core thread follows the same **no allocation rule** as the [SVD_OMS_Master]
core thread (see the hot path discipline section).

## The data flow
The [SVD_OMS_Master] pushes the `Trade` events into the file mapped share memory SPSC queue (the
queue is persistent messaging — unread trades survive a restart of either side). The core thread
of the [SVD_Settlement] spins on the queue:

1. **Drain and group** — the core thread peeks the trade queue, groups the consecutive crosses of
   one taker order into one batch, and encodes each complete group into one frame (the batch
   sequence plus the MessagePack-encoded trades). The group invariant: the [SVD_OMS_Master]
   pushes the crosses of one execution contiguously (its settlement writer never interleaves the
   executions), so a run of consecutive trades with the same `(taker.hot.user, taker.hot.nonce)`
   IS the complete set of that taker order's crosses. A group closes when the taker changes; the
   open group stays in the core's accumulator across the drains and flushes on the shutdown.
2. **Route and ack** — the core thread pushes the frame into one submitter queue (round-robin
   starting point, the first queue with room) and acks the group's trades on the trade queue
   **only after the frame is pushed** — the durability boundary: the unacked trades stay in the
   file-mapped trade queue and survive a crash of this process (a crash between the push and the
   ack duplicates the batch; the accepted at-least-once trade-off below).
3. **Submit and monitor** — one submitter thread per operator key consumes its own queue one
   frame at a time: it builds the calldata with alloy (ABI encode of the `MatchedTrade[]`),
   estimates the gas, signs the transaction with its operator key (unlocked from its keystore at
   startup) and submits it through its own nonce manager on the primary node of its own RPC pool.
   The nonce manager holds the operator's transaction sequence, resynchronizes it from the chain
   on startup, and detects the gaps.
4. **Classification and action** — the submitter watches the transaction: pending in the mempool,
   mined, or reverted. A mined transaction is watched until the configured confirmation depth
   (default 1); a reorganized transaction returns to the submission step. A node that goes off
   switches the watch to the next secondary — the monitoring continues on the new node's view. A
   reverted transaction carries the `SettlementError(code, index)` revert data; the submitter
   decodes it and dispatches per the table below.
5. **Publish and ack** — every final outcome is handed to the shared result publisher and the SQL
   writer as a `SettlementResult`. The publisher writes it to the [Redis_Cluster] channel
   `svd:stl:{symbol_hex}:settlements` with an at-least-once retry and confirms the write back;
   the submitter acks the frame **only after every batch of it is terminal and its result is
   confirmed by the publisher** — a frame acked never replays. The settled trades are also
   written to the [SQL_Cluster] `trades` and `settlements` tables (the SQL write is enqueue-only:
   a crash can still lose SQL rows).

A batch fails as a whole (the on-chain `settleBatch` is all-or-nothing and the revert data names
the failing trade index). To isolate one bad trade the submitter **binary-splits** the batch: it
resubmits the two halves, keeps splitting the failing half until the failing trade is singled
out, then applies the per-code action to it and settles the rest — the children are driven in
order (the clean halves, then the poison) before the frame acks. A batch that stays unresolved
(a pause, a down chain) is retried as a whole until it settles — the frame stays unacked while it
is pending, and the submitter processes nothing else from its queue in the meantime (a blocking
pipeline per submitter).

## The durability model
There is no journal. The durability lives in the file-mapped queues and the acks: the trade queue
(the [SVD_OMS_Master] side never drops a trade), the per-submitter batch queues (this process owns
both ends: the files are initialized when missing and never reinitialized on a restart, so the
unprocessed frames survive), and the two ack rules above. The `batch_seq` values come from a
persistent 4 KiB sequence file (one `AtomicUsize` in a mapping — not a journal: no records, no
replay), assigned by the core thread per group and by the submitters per split child, so the SQL
tables' `UNIQUE(batch_seq)` keys never collide across restarts.

The accepted trade-offs, by design:

- **At-least-once** — a crash between a chain confirmation and an ack re-processes the frame,
  which may re-submit an already-settled batch; the stateless protocol double-settles it. The
  window is minimized by acking immediately after the push (core) / the publisher confirm
  (submitters), never removed.
- **Replayed split children get fresh sequences** — the duplicate SQL rows of a re-processed
  frame are absorbed by the `INSERT IGNORE` semantics of the tables.
- **The shutdown can block** — the submitters wait for the publisher confirms, and the publisher
  retries forever against a down Redis cluster; a SIGKILL is at-least-once safe (the unacked
  frames replay on the next start).

## The failure classification and the actions

| outcome | condition | action |
| --- | --- | --- |
| mined & confirmed | the batch settled | publish `Settled`, write the SQL rows |
| `SettlementError` code 1 `InvalidSignature` | a party's signature does not recover | the at-fault order is forged: publish the `Reverted` result — the [SVD_Pretrade] pushes `CancelBySettlement` for the at-fault order and re-injects the innocent side's crossed quantity into the pipeline; never retry |
| code 2 `InsufficientMargin` | an account's available margin is exhausted on-chain | the at-fault account must not trade: publish the `Reverted` result — the [SVD_Pretrade] pushes `MassCancelByUser` (all its resting orders leave the book), re-injects the innocent side's crossed quantity into the pipeline and blocks the at-fault account until a fresh margin update arrives for it (any fresh state clears the block and decides the admission on its own from then on) |
| code 5 / 6 `SymbolPaused` / `SettlementPaused` | the protocol is not accepting | transient: retry the batch with backoff until the pause lifts — the batch stays pending, no outcome is published |
| tx-level failure (RPC error, gas, nonce gap, reorg) | the transaction never settled | retry the submission with backoff and gas re-pricing until it lands — the batch stays pending |
| unclassifiable revert | unknown code or missing revert data | conservative: retry the batch with backoff (the revert stays unexplained — an alarm to page on); the symbol's settlement stalls behind the batch until it resolves |

The on-chain protocol is stateless: it tracks no filled quantities and validates no order
timestamps or prices — the [SVD_OMS_Master] book is the single source of truth for those. The
failure taxonomy therefore carries no `OrderFullySettled`, `OrderExpired` or
`InvalidPrice` / `InvalidQuantity` codes; a revert carrying a code outside the table is
unclassifiable and takes the conservative retry path above.

When the failing trade is singled out, the published `Reverted` result carries the failing trade
and the at-fault side, and the [SVD_Pretrade] applies both actions through the ingress pipeline
(the MPSC queue → the ingress SPSC queue → the [SVD_OMS_Master]): the at-fault side's order leaves
the book (`CancelBySettlement` for a deterministic failure of one order, `MassCancelByUser` for an
exhausted margin), and the **innocent side's crossed quantity goes back into the book** — the
[SVD_Pretrade] re-injects the innocent counterparty's order with the failed cross's
`traded_quantity`, and the master merges the quantity into the resting order or re-inserts the order
at the tail of its price level. The re-opened order replicates to the slave like any other
execution. The re-injected order bypasses the margin
gate — it restores an already-admitted state, not a new exposure — and its signature is re-verified
on the way.

## The message types

```Rust
/// The result of one settlement batch, published by [SVD_Settlement] to the
/// [Redis_Cluster] channel `svd:stl:{symbol_hex}:settlements`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementResult {
    /// The batch sequence — unique across restarts (the persistent sequence
    /// file); the consumers (the [SVD_Pretrade] among them) do not
    /// bookkeep it: applying a result is idempotent (setting a block twice
    /// is a no-op).
    pub batch_seq: u64,
    /// The symbol of the batch.
    pub symbol: Symbol,
    /// The outcome of the batch.
    pub outcome: SettlementOutcome,
    /// The transaction hash when one was submitted.
    pub tx_hash: Option<Hash32>,
    /// The block number when the transaction was confirmed.
    pub block: Option<u64>,
    /// The trades of the batch — the [SVD_Pretrade] derives the at-fault
    /// removals, the innocent restores and the accounts to block from them
    /// when the outcome is a reverted batch.
    pub trades: Vec<Trade>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SettlementOutcome {
    /// The batch settled on-chain.
    Settled,
    /// One failing trade was singled out of the batch: the at-fault side's
    /// order is removed from the book and the innocent side's crossed
    /// quantity is re-injected into the pre-trade pipeline by the
    /// [SVD_Pretrade].
    Reverted {
        /// The index of the failing trade in `trades`.
        failed_trade: usize,
        /// Which side of the failing cross is at fault.
        at_fault: FaultSide,
        /// The decoded failure.
        reason: SettlementFailure,
    },
}

/// Which side of a failing cross is at fault. The revert data of the
/// protocol names the side (see doc/settlement-protocol.md); the settlement
/// derives it for the off-chain failure reasons.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FaultSide {
    /// The taker's order caused the revert — the maker is innocent.
    Taker,
    /// The maker's order caused the revert — the taker is innocent.
    Maker,
}

/// The failure a reverted settlement outcome carries: the decoded
/// `SettlementError` code of doc/settlement-protocol.md, or an unclassifiable
/// cause. A transient failure (a paused protocol, a down chain) never
/// publishes an outcome — the [SVD_Settlement] retries it until it resolves.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SettlementFailure {
    /// The decoded on-chain `SettlementError` code.
    Protocol(u8),
    /// The revert could not be classified.
    Unclassified,
}
```

The restore semantics on the master:

- **Merge** — when `(user, nonce)` still rests in the book (other crosses of the order settled in
  the meantime), the restored quantity is merged into the resting quantity; for an iceberg or a
  reserve order the quantity goes into the hidden tranche, and the visible/hidden accounting
  follows the same replenishment rules as a fill in reverse.
- **Re-insert** — when the order is gone (the failed fills removed it), it is re-inserted at the
  tail of its price level. A restored order loses its original time priority — the tail is the
  only position that does not need the lost queue state.
- **Idempotency** — the merge makes a duplicate restore harmless (it would double-add only if the
  settlement published the same result twice, which the frame ack prevents; the merge keeps the
  master safe if it ever happens anyway).

The restored orders generate `Open` changes, which the [SVD_OMS_Master] replicates to the
[SVD_OMS_Slave] like any other execution — the book state converges on both sides without a
special replication path. The `CancelReason` taxonomy carries `SettlementFailed` for the
removals above.

## The submission pipeline
- **The batch state machine, in memory** — every batch moves through `Received → Submitting(tx) →
  Submitted(tx, nonce) → Confirmed(block) | Reverted(reason)`, with `Split` for a binary-split
  parent. A crash mid-flight re-processes the frame from `Received` (the frame is self-contained),
  re-submitting it under a fresh nonce — the accepted at-least-once trade-off.
- **The nonce manager** — each submitter's own operator transaction sequence: fetched from the
  chain on startup, assigned sequentially, with gap detection (a missing nonce stalls the queue
  until it lands or is replaced) and fee-bump replacement for a stuck transaction.
- **The gas strategy** — config: max priority fee, the bump percentage per retry, the base fee
  tolerance; a retry re-estimates and re-prices within the caps.
- **The RPC node pool** — each submitter holds its own pool of [Web3RPCNodes]: one primary and
  the secondaries in the failover order (the same HA pattern as the [SVD_Sync]). A transport
  error or a stalled node switches the submission to the next secondary; the monitoring of the
  pending transactions continues on the new node — a transaction hash is node-agnostic — and the
  submitter never submits the same transaction twice through two nodes in parallel. After a
  switch the nonce manager re-synchronizes from the new node's view.
- **The operator keystores** — every submitter holds its own operator key. The hot wallet private
  keys are stored in encrypted keystore files on disk (the Web3 Secret Storage JSON format). The
  config carries only the keystore paths; the password is read from the system environment
  variable `SVD_SETTLEMENT_KEYSTORE_PASSWORD` — one variable for every keystore, never from the
  config, never in a log. The keys are decrypted at startup and kept in memory only, behind the
  cryptography crate's signer traits; the key material never travels the wire. Two submitters
  must never share one keystore (each would run its own nonce manager for the same account).
- **The result publisher** — the one shared publisher thread serializes the `SettlementResult`
  messages and publishes them to the [Redis_Cluster] with the at-least-once retry, confirming
  each write back to the submitter. It stops when the engine drops the channel.

## The dependency of svd-settlement crate
- primitives: the `Trade` ingress of crates/primitives/src/message/hot_path.rs, and the
  `SettlementResult` of crates/primitives/src/message/settlement.rs. The ingress pipeline message
  of the hot path carries the `PipelineMsg` union (the user requests, the `RestoreOrder` of the
  innocent-side re-injection and the settlement removals `CancelBySettlement` / `MassCancelByUser`
  of the at-fault side), and the book of the primitives crate holds the execution paths for the
  pipeline restores and the removals (the merge semantics, the targeted cancel, the mass cancel);
  the `CancelReason` taxonomy carries `SettlementFailed`.
- ipc: the share memory SPSC queues — the fixed-size `Trade` queue from the [SVD_OMS_Master]
  (with the explicit read-index ack of `SpscQueue::peek_batch` / `ack`), and the variable-size
  `ByteSpscQueue` batch queues of the submitter pool (frames of a fixed 4-byte payload-length
  header, all-or-nothing pushes, the `peek` / `ack` pair).
- alloy: the ABI encoding of `settleBatch`, the revert data decoding of `SettlementError`, the
  transaction building, the signing, the receipts.
- cryptography: the signer traits of crates/cryptography for the operator keystores.
- storage: the [Redis_Cluster] publication of the results and the [SQL_Cluster] writer of the
  trades / settlements tables.

## The concurrency model of svd-settlement crate
- **Core thread** (1, pinned): peeks the trade SPSC queue, groups the consecutive crosses of one
  taker order into a batch, encodes each complete group into one frame and pushes the frames to
  the submitter queues, acking the trade queue only after a frame is pushed. Nothing else — the
  hot path ends here; the drain window and the encode buffer are pre-allocated once at startup
  from the object pools, sized by the trade queue capacity (the hard bound of one taker group —
  see the hot path discipline section).
- **Submitter threads** (N, configurable, async tokio + alloy): one per operator key, each owns
  its own batch queue, nonce manager, gas strategy, RPC node pool, confirmation watch and
  classification. Each processes one frame at a time to a determined result — every batch of the
  frame terminal and its result confirmed by the publisher — before it acks the frame and moves
  to the next: a blocking pipeline per submitter, so a poisoned batch stalls only its own
  submitter while the pool keeps settling.
- **Storage publisher thread** (1): serializes the `SettlementResult` messages and publishes them
  to the [Redis_Cluster], confirming each write back to the submitting thread; the retry loop
  never gives up on a Redis outage.
- **SQL writer thread** (1): batches the settled trades and the settlement rows into the
  [SQL_Cluster] with retries; an SQL outage never stalls the [Redis_Cluster] publication.

## The hot path discipline
The core thread follows the same **no allocation rule** as the [SVD_OMS_Master] core thread: **no
heap allocation and no blocking call at runtime**. The [HotPath] ends at this thread, and
everything past it (the submission, the monitoring, the publication) is side-path work:

- **The drain window** — the buffer the core loop peeks the SPSC queue into is allocated once at
  startup from the pre-allocated object pool, sized by the **trade queue capacity**: the open
  group is a subset of the unacked trades (the queue holds at most `capacity - 1`), so the window
  of `capacity` trades always holds the whole group plus the boundary trade of the next taker.
  Nothing on the loop path grows it.
- **The encode buffer** — one reused `Vec<u8>` from the pre-allocated object pool, sized by the
  frame budget of the whole trade queue (the worst-case group), receives the MessagePack encoding
  of each closed group right before the push. The hot-path encoder writes from the borrowed
  trades — nothing is cloned, nothing is allocated past the buffer's capacity.
- **The queue pushes and the acks** — memory copies into the mapped pages and atomic index
  stores, no syscalls, no locks.

Everything that is allowed to allocate or block — the ABI encoding, the gas estimation, the
signing, the RPC I/O, the result serialization — runs on the submitter, the storage publisher and
the SQL writer threads, never on the core thread.

## The features in svd-settlement crate
- config: a TOML config loaded on start and reloadable via SIGHUP, holding the symbol, the core
  id, the trade SPSC queue (path, capacity, create), the batch sequence file, the chain facts (the
  chain id, the settlement contract address, the confirmation depth), the shared retry policy and
  poll cadences, the [Redis_Cluster] / [SQL_Cluster] connections, and the submitter pool — one
  section per submitter with its keystore path (the password comes from the system environment,
  never from the config), its batch queue (path, byte capacity) and its chain resources (the RPC
  node pool — one primary and the secondaries in the failover order, each with its endpoints —
  the connection and node stall timeouts, the submission timeout, the gas strategy). Every
  running thread snapshots its parameters at launch: the submitter pool, the queues and the
  chain parameters take effect on the next restart.
- the batch assembly: the taker-grouping core loop with the round-robin routing and the
  ack-after-push durability rule.
- the submitter pool: one thread per operator key with the nonce manager, the gas strategy, the
  RPC node pool failover (one primary and the secondaries), the confirmation watch and the reorg
  re-submission.
- the operator keystores: the encrypted keystore files with the password from the system
  environment variable, unlocked at startup and held in memory only.
- the classifier: the `SettlementError` decoding and the action table of the failure
  classification section, with the binary-split isolation of the failing trades.
- the result publisher: the `SettlementResult` publication with the per-write confirms and the
  batch sequences of the persistent sequence file (the consumers apply a result idempotently —
  the [SVD_Pretrade] blocks the accounts of a reverted batch, and setting the block twice is a
  no-op).
