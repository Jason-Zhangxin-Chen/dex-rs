# The overview
The [SVD_Settlement] is the last hop of the [HotPath] and the bridge back to the chain: it drains
the trade events of the [SVD_OMS_Master] from the share memory SPSC queue, batches them, submits
the batches to the on-chain settlement protocol (the assumed interfaces in
doc/settlement-protocol.md), listens to the results, and decides what the result means for the
book. A settled trade is final. A failed trade is classified: the deterministic failures remove
the orders (they must never trade again — forged signature, exhausted margin, expired order), the
transient failures are retried with a deadline, and when the retries are exhausted the [SVD_Settlement]
**rolls the trades back — it puts the failed trades' orders back into the book** through a reversal
message to the [SVD_OMS_Master]. Every result is published to the [Redis_Cluster] and the
[SQL_Cluster], because the downstream services depend on it: the [SVD_Pretrade] consumes the results
to block the accounts of the failed trades (a reverted batch means the on-chain margin is exhausted
while the cached margin state may still be stale), the [SVD_PubSub] relays the settlement status to
the users, and the [SVD_Sync] picks up the authoritative margin changes from the chain events the
settlement produced.

The submission is the slow I/O of the system (transaction building, gas estimation, mempool,
confirmations), so everything after the trade queue is asynchronous — the [HotPath] ends at the
queue drain and the journal append, which never block and never allocate: the core thread follows
the same **no allocation rule** as the [SVD_OMS_Master] core thread (see the hot path discipline
section).

## The data flow
The [SVD_OMS_Master] pushes the `Trade` events into the file mapped share memory SPSC queue (the
queue is persistent messaging — unread trades survive a restart of either side). The core thread
of the [SVD_Settlement] spins on the queue:

1. **Drain and journal** — the core thread pops a batch of trades, appends them to the local
   journal (the same memory-mapped dual header design as the [SVD_OMS_Slave] journal), and hands
   the batch to the submitter. The journal is the durability boundary: from here on a crash never
   loses a trade.
2. **Batch assembly** — the submitter aggregates the trades into batches bounded by
   `max_trades_per_batch` and `batch_window_ms` (both config). A batch becomes a `settleBatch`
   call of the on-chain protocol.
3. **Submission** — the submitter builds the calldata with alloy (ABI encode of the
   `MatchedTrade[]`), estimates the gas, signs the transaction with the operator key, and submits
   it through the nonce manager. The nonce manager holds the operator's transaction sequence,
   resynchronizes it from the chain on startup, and detects the gaps.
4. **Monitoring** — the submitter watches the transaction: pending in the mempool, mined, or
   reverted. A mined transaction is watched until the configured confirmation depth (default 1);
   a reorganized transaction returns to the submission step.
5. **Classification and action** — a reverted transaction carries the `SettlementError(code,
   index)` revert data. The [SVD_Settlement] decodes it and dispatches per the table below; a
   transient failure retries the batch with backoff, a permanent failure removes the orders, and
   the deadline (or an unclassifiable revert) rolls the batch back.
6. **Publication** — every final outcome is journaled and published as a `SettlementResult` to the
   [Redis_Cluster] channel `svd:stl:{symbol_hex}:settlements`, and the settled trades are written
   to the [SQL_Cluster] `trades` and `settlements` tables.

## The failure classification and the actions

| outcome | condition | action |
| --- | --- | --- |
| mined & confirmed | the batch settled | publish `Settled`, write the SQL rows |
| `SettlementError` code 1 `InvalidSignature` | a party's signature does not recover | the order is forged: send `CancelOrder` to the master (remove the order's resting remainder), publish the result — never retry |
| code 2 `InsufficientMargin` | an account's available margin is exhausted on-chain | the account must not trade: send `MassCancelByUser` to the master (all its resting orders leave the book), publish the result — the [SVD_Pretrade] blocks the account until a fresh margin update arrives for it (any fresh state clears the block and decides the admission on its own from then on) |
| code 3 `OrderFullySettled` | the trade was already settled (double submission) | treat as settled: publish `Settled` — the idempotent path |
| code 4 `OrderExpired` | the order's lifetime passed | send `CancelOrder` to the master, publish the result |
| code 5 / 6 `SymbolPaused` / `SettlementPaused` | the protocol is not accepting | transient: retry the batch with backoff; roll back on the deadline |
| code 7 / 8 `InvalidPrice` / `InvalidQuantity` | the off-chain validation failed to catch a bad trade | send `CancelOrder` to the master for both orders, publish the result (a bug to alarm on) |
| tx-level failure (RPC error, gas, nonce gap, reorg) | the transaction never settled | retry the submission with backoff and gas re-pricing; roll back on the deadline |
| unclassifiable revert | unknown code or missing revert data | conservative: roll the batch back, publish `RolledBack` |

A batch fails as a whole (the on-chain `settleBatch` is all-or-nothing and the revert data names
the failing trade index). To isolate one bad trade the [SVD_Settlement] **binary-splits** the batch:
it resubmits the two halves, keeps splitting the failing half until the failing trade is singled
out, then applies the per-code action to it and settles the rest. A batch that hits the deadline
unresolved is rolled back as a whole.

## The rollback — putting the failed trades back into the book

When the retries are exhausted, the trades of the batch never happened on-chain, but the book
already moved: the match removed the makers' quantities and consumed the takers. The rollback puts
them back. The [SVD_Settlement] sends a reversal `SettlementMsg` to the [SVD_OMS_Master] through a
dedicated file mapped SPSC queue (the `reversal` queue, symmetric to the ingress one). On the
master side the queue is a new `[reversal]` config section beside `[ingress]` and `[settlement]`,
and the master's core loop drains it every iteration after the ingress batch — the reversals are
applied by the single owner of the book, in the same thread as the executions:

```Rust
/// Messages sent from [SVD_Settlement] to [SVD_OMS_Master] on the reversal
/// queue. The master drains the queue on its core loop, so the reversals
/// and the ingress requests are applied by the single owner of the book.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SettlementMsg {
    /// Rolls one failed trade back: re-inserts `quantity` of the order into
    /// the book (the failed cross). The [SVD_OMS_Master] merges the quantity
    /// into the resting order when `(user, nonce)` is still in the book
    /// (part of the order kept trading), and re-inserts the order at the
    /// tail of its price level when it is gone.
    RestoreOrder { order: Order, quantity: Quantity },
    /// Removes one order of the book: a deterministic settlement failure of
    /// that order (forged signature, expired, bad price).
    CancelOrder { user: Address, nonce: Nonce, reason: CancelReason },
    /// Removes every resting order of an account: the account's margin is
    /// exhausted on-chain and it must stop trading.
    MassCancelByUser { user: Address, reason: CancelReason },
}

/// The result of one settlement batch, published by [SVD_Settlement] to the
/// [Redis_Cluster] channel `svd:stl:{symbol_hex}:settlements`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementResult {
    /// The batch sequence of the settlement journal — the sequence of the
    /// [SVD_Settlement]'s own at-most-once publication; the consumers (the
    /// [SVD_Pretrade] among them) do not bookkeep it: applying a result is
    /// idempotent (setting a block twice is a no-op).
    pub batch_seq: u64,
    /// The symbol of the batch.
    pub symbol: Symbol,
    /// The outcome of the batch.
    pub outcome: SettlementOutcome,
    /// The transaction hash when one was submitted.
    pub tx_hash: Option<Hash32>,
    /// The block number when the transaction was confirmed.
    pub block: Option<u64>,
    /// The trades of the batch — the [SVD_Pretrade] derives the accounts to
    /// block from them when the outcome is a reverted batch.
    pub trades: Vec<Trade>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SettlementOutcome {
    /// The batch settled on-chain.
    Settled,
    /// The batch was reverted and the recovery action was applied (the
    /// orders are removed from the book or the failure was idempotent).
    Reverted { reason: SettlementFailure },
    /// The batch was rolled back: the trades' orders were re-inserted into
    /// the book by the reversal messages.
    RolledBack { reason: SettlementFailure },
}

/// The failure a settlement outcome carries: the decoded `SettlementError`
/// code of doc/settlement-protocol.md, or an off-chain cause.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SettlementFailure {
    /// The decoded on-chain `SettlementError` code.
    Protocol(u8),
    /// The retry deadline expired before the batch settled.
    DeadlineExceeded,
    /// The transaction failed for an off-chain reason (transport, gas,
    /// nonce) and was not resubmitted in time.
    Submission,
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
  settlement pushed the same reversal twice, which its journaled state machine prevents; the merge
  keeps the master safe if it ever happens anyway).

The restored orders generate `Open` changes, which the [SVD_OMS_Master] replicates to the
[SVD_OMS_Slave] like any other execution — the book state converges on both sides without a
special replication path. The `CancelReason` taxonomy gains a `SettlementFailed` variant for the
removals above.

## The submission pipeline
- **The batch state machine, journaled** — every batch moves through `Received → Submitting(tx) →
  Submitted(tx, nonce) → Confirmed(block) | Reverted(reason) | RolledBack(reason)`, and every
  transition is journaled. On restart the [SVD_Settlement] replays the journal: a `Submitted`
  batch re-enters the monitoring, a `Submitting` batch re-submits, a `Confirmed` batch publishes
  its result — each outcome is published at most once because the state is persisted before the
  publication.
- **The nonce manager** — the operator's transaction sequence: fetched from the chain on startup,
  assigned sequentially, with gap detection (a missing nonce stalls the queue until it lands or is
  replaced) and fee-bump replacement for a stuck transaction.
- **The gas strategy** — config: max priority fee, the bump percentage per retry, the base fee
  tolerance; a retry re-estimates and re-prices within the caps.
- **The RPC failover** — a pool of RPC endpoints; the submitter fails over on transport errors and
  never submits the same transaction twice through two endpoints in parallel.
- **The operator key** — the hot wallet of the [SVD_Settlement], loaded from a key file or an
  external signer (HSM / KMS) behind the cryptography crate's signer traits; the key material
  never travels the wire.

## The dependency of svd-settlement crate
- primitives: the `Trade` ingress of crates/primitives/src/message/hot_path.rs, and the new
  `SettlementMsg` / `SettlementResult` of crates/primitives/src/message/settlement.rs. The book of
  the primitives crate gains the execution paths for the reversal messages (the restore with the
  merge semantics, the targeted cancel, the mass cancel), and the `CancelReason` taxonomy gains
  `SettlementFailed`.
- ipc: the share memory SPSC queues — the trade ingress from the [SVD_OMS_Master] and the reversal
  outgress back to it.
- alloy: the ABI encoding of `settleBatch`, the revert data decoding of `SettlementError`, the
  transaction building, the signing, the receipts.
- cryptography: the signer traits of crates/cryptography for the operator key.
- storage: the [Redis_Cluster] publication of the results and the [SQL_Cluster] writer of the
  trades / settlements tables.
- journal: the same journal design as the [SVD_OMS_Slave] (to be extracted to a shared crate, see
  the sync spec) — the [SVD_Settlement] journals the received trades, the batch states and the
  published results.

## The concurrency model of svd-settlement crate
- **Core thread** (1, pinned): spins on the trade SPSC queue, drains the trades, appends them to
  the journal and hands them to the submitter. Nothing else — the hot path ends here; the queue
  drain runs in a pre-allocated batch buffer reused every iteration, the journal append is
  memory-mapped, and the hand-off uses pooled buffers (see the hot path discipline section).
- **Submitter thread** (1, async tokio + alloy): the batch assembly, the submission, the
  monitoring, the classification and the reversal decisions; it pushes the reversal messages into
  the reversal SPSC queue and the results to the storage publisher.
- **Storage publisher thread** (1): serializes the `SettlementResult` messages and publishes them
  to the [Redis_Cluster], with the at-least-once retry — the subscribers deduplicate by the batch
  sequence, so a republish is harmless.
- **SQL writer thread** (1): batches the settled trades and the settlement rows into the
  [SQL_Cluster] with retries; an SQL outage never stalls the [Redis_Cluster] publication.

## The hot path discipline
The core thread follows the same **no allocation rule** as the [SVD_OMS_Master] core thread: **no
heap allocation and no blocking call at runtime**. The [HotPath] ends at this thread, and
everything past it (the submission, the monitoring, the publication) is side-path work:

- **The trade batch** — the buffer the core loop drains the SPSC queue into is allocated once at
  startup with the configured batch size and reused every iteration, the same pattern as the
  master's spin loop batch.
- **The journal append** — the journal is memory-mapped: the append copies the trade payload into
  the mapped pages and flushes only the touched ranges, so it performs no heap allocation and no
  read / write syscalls (the same journal design as the [SVD_OMS_Slave]).
- **The hand-off buffers** — the drained batches travel to the submitter in buffers checked out
  from the object pools of the cache crate (the same pooling as the primitives `PooledTrades`),
  sized to the configured worst case so a steady-state checkout never allocates; the submitter
  returns the buffers to the pools after the batch is encoded, so the core thread hands the work
  off without allocating.
- **The batch state machine** — the core thread journals only the received trades; the batch state
  transitions are appended by the submitter thread, off the hot path.

Everything that is allowed to allocate or block — the ABI encoding, the gas estimation, the
signing, the RPC I/O, the result serialization — runs on the submitter, the storage publisher and
the SQL writer threads, never on the core thread.

## The features in svd-settlement crate
- config: a TOML config loaded on start and reloadable via SIGHUP, holding the symbol, the core
  id, the trade SPSC queue (path, capacity, create), the reversal SPSC queue, the chain
  connections (RPC pool, chain id, the settlement contract address), the operator key, the
  batching (max trades, window) and the hand-off pool sizes, the retry policy (max retries,
  backoff, the rollback deadline), the confirmation depth, the gas strategy, the journal path, and
  the [Redis_Cluster] / [SQL_Cluster] connections.
- the journaled state machine: the trade log and the batch states with the crash replay described
  above.
- the batch builder: the aggregation of the trades into the `settleBatch` calls within the gas
  limits.
- the submitter: the nonce manager, the gas strategy, the RPC failover, the confirmation watch and
  the reorg re-submission.
- the classifier: the `SettlementError` decoding and the action table of the failure
  classification section, with the binary-split isolation of the failing trades.
- the reversal engine: the `SettlementMsg` composition (one `RestoreOrder` per failed trade) and
  the at-most-once delivery through the journaled state machine.
- the result publisher: the `SettlementResult` publication with the batch sequence of the
  settlement journal (the consumers apply a result idempotently — the [SVD_Pretrade] blocks the
  accounts of a reverted batch, and setting the block twice is a no-op).
