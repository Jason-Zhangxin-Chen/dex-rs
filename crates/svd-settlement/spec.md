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
twice — it re-injects the **innocent side's crossed quantity** of a failed trade into the pre-trade
pipeline (a trade failed because of one side, the innocent counterparty's order goes back into the
book through the pipeline), and it blocks the at-fault account when the failure is
`InsufficientMargin` (the on-chain margin is exhausted while the cached margin state may still be
stale) — the [SVD_PubSub] relays the settlement status to the users, and the [SVD_Sync] picks up
the authoritative margin changes from the chain events the settlement produced.

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
   `MatchedTrade[]`), estimates the gas, signs the transaction with the operator key (unlocked
   from the keystore at startup), and submits it through the nonce manager on the primary node of
   the RPC pool. The nonce manager holds the operator's transaction sequence, resynchronizes it
   from the chain on startup, and detects the gaps.
4. **Monitoring** — the submitter watches the transaction: pending in the mempool, mined, or
   reverted. A mined transaction is watched until the configured confirmation depth (default 1);
   a reorganized transaction returns to the submission step. A node that goes off switches the
   watch to the next secondary — the monitoring continues on the new node's view.
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
| `SettlementError` code 1 `InvalidSignature` | a party's signature does not recover | the at-fault order is forged: send `CancelOrder` to the master (remove its resting remainder) and publish the result — the [SVD_Pretrade] re-injects the innocent side's crossed quantity into the pipeline; never retry |
| code 2 `InsufficientMargin` | an account's available margin is exhausted on-chain | the at-fault account must not trade: send `MassCancelByUser` to the master (all its resting orders leave the book) and publish the result — the [SVD_Pretrade] re-injects the innocent side's crossed quantity into the pipeline and blocks the at-fault account until a fresh margin update arrives for it (any fresh state clears the block and decides the admission on its own from then on) |
| code 3 `OrderFullySettled` | the trade was already settled (double submission) | treat as settled: publish `Settled` — the idempotent path |
| code 4 `OrderExpired` | the order's lifetime passed | send `CancelOrder` to the master (remove the expired order) and publish the result — the [SVD_Pretrade] re-injects the innocent side's crossed quantity into the pipeline |
| code 5 / 6 `SymbolPaused` / `SettlementPaused` | the protocol is not accepting | transient: retry the batch with backoff; roll back on the deadline |
| code 7 / 8 `InvalidPrice` / `InvalidQuantity` | the off-chain validation failed to catch a bad trade | send `CancelOrder` to the master for the at-fault order and publish the result — the [SVD_Pretrade] re-injects the innocent side's crossed quantity into the pipeline (a bug to alarm on) |
| tx-level failure (RPC error, gas, nonce gap, reorg) | the transaction never settled | retry the submission with backoff and gas re-pricing; roll back on the deadline |
| unclassifiable revert | unknown code or missing revert data | conservative: roll the batch back, publish `RolledBack` |

A batch fails as a whole (the on-chain `settleBatch` is all-or-nothing and the revert data names
the failing trade index). To isolate one bad trade the [SVD_Settlement] **binary-splits** the batch:
it resubmits the two halves, keeps splitting the failing half until the failing trade is singled
out, then applies the per-code action to it and settles the rest. A batch that hits the deadline
unresolved is rolled back as a whole.

When the failing trade is singled out, the at-fault side's order leaves the book (the `CancelOrder`
/ `MassCancelByUser` of the table above) and the **innocent side's crossed quantity goes back
through the pre-trade pipeline**: the published result carries the failing trade and the at-fault
side, the [SVD_Pretrade] re-injects the innocent counterparty's order with the failed cross's
`traded_quantity` into its pipeline (the MPSC queue → the ingress SPSC queue → the
[SVD_OMS_Master]), and the master merges the quantity into the resting order or re-inserts the order
at the tail of its price level — the same merge semantics as a rollback restore, and the re-opened
order replicates to the slave like any other execution. The re-injected order bypasses the margin
gate — it restores an already-admitted state, not a new exposure — and its signature is re-verified
on the way. The batch rollback stays on the reversal queue: there both sides are innocent and the
whole batch is restored at once.

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
    /// Rolls one failed trade back (the batch rollback path — the per-trade
    /// innocent-side restores go through the [SVD_Pretrade] pipeline
    /// instead): re-inserts `quantity` of the order into
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
    /// The batch was rolled back: the trades' orders were re-inserted into
    /// the book by the reversal messages.
    RolledBack { reason: SettlementFailure },
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
- **The RPC node pool** — the submitter holds a pool of [Web3RPCNodes]: one primary and the
  secondaries in the failover order (the same HA pattern as the [SVD_Sync]). A transport error or
  a stalled node switches the submission to the next secondary; the monitoring of the pending
  transactions continues on the new node — a transaction hash is node-agnostic — and the submitter
  never submits the same transaction twice through two nodes in parallel. After a switch the
  nonce manager re-synchronizes from the new node's view.
- **The operator keystore** — the hot wallet private key of the [SVD_Settlement] is stored in an
  encrypted keystore file on disk (the Web3 Secret Storage JSON format). The config carries only
  the keystore path; the password is read from the system environment variable
  `SVD_SETTLEMENT_KEYSTORE_PASSWORD` — never from the config, never in a log. The key is decrypted
  at startup and kept in memory only, behind the cryptography crate's signer traits; the key
  material never travels the wire.

## The dependency of svd-settlement crate
- primitives: the `Trade` ingress of crates/primitives/src/message/hot_path.rs, and the new
  `SettlementMsg` / `SettlementResult` of crates/primitives/src/message/settlement.rs. The ingress
  pipeline message of the hot path gains the `PipelineMsg` union (the user requests plus the
  `RestoreOrder` of the innocent-side re-injection), the book of the primitives crate gains the
  execution paths for the reversal messages and the pipeline restores (the merge semantics, the
  targeted cancel, the mass cancel), and the `CancelReason` taxonomy gains `SettlementFailed`.
- ipc: the share memory SPSC queues — the trade ingress from the [SVD_OMS_Master] and the reversal
  outgress back to it.
- alloy: the ABI encoding of `settleBatch`, the revert data decoding of `SettlementError`, the
  transaction building, the signing, the receipts.
- cryptography: the signer traits of crates/cryptography for the operator keystore.
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
  monitoring, the classification and the reversal decisions — with the RPC node pool failover (the
  primary first, then the secondaries); it pushes the reversal messages into
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
  connections (the RPC node pool — one primary and the secondaries in the failover order, each
  with its endpoints — the connection and node stall timeouts, the chain id, the settlement
  contract address), the operator keystore path (the password comes from the system environment,
  never from the config), the
  batching (max trades, window) and the hand-off pool sizes, the retry policy (max retries,
  backoff, the rollback deadline), the confirmation depth, the gas strategy, the journal path, and
  the [Redis_Cluster] / [SQL_Cluster] connections.
- the journaled state machine: the trade log and the batch states with the crash replay described
  above.
- the batch builder: the aggregation of the trades into the `settleBatch` calls within the gas
  limits.
- the submitter: the nonce manager, the gas strategy, the RPC node pool failover (one primary and
  the secondaries), the confirmation watch and the reorg re-submission.
- the operator keystore: the encrypted keystore file with the password from the system environment
  variable, unlocked at startup and held in memory only.
- the classifier: the `SettlementError` decoding and the action table of the failure
  classification section, with the binary-split isolation of the failing trades.
- the reversal engine: the `SettlementMsg` composition (one `RestoreOrder` per failed trade) and
  the at-most-once delivery through the journaled state machine.
- the result publisher: the `SettlementResult` publication with the batch sequence of the
  settlement journal (the consumers apply a result idempotently — the [SVD_Pretrade] blocks the
  accounts of a reverted batch, and setting the block twice is a no-op).
