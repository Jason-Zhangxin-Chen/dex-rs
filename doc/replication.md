# Replication: OMS_Master → OMS_Slave

This document describes the state replication protocol between the OMS master
and the OMS slave, and the contract that the slave's `apply` implementation
relies on. The implementation lives in `crates/primitives/src/orderbook/book.rs`
(`OrderBook::apply`).

## Overview

- The **master** executes the ingress `OrderMsg`s (new orders, cancels) and
  produces two outputs: the **trade stream** (pushed to `svd-settlement`) and
  the **order change stream** (replicated to the slave via the NATS stream).
- The **slave** never executes an order. It applies the replicated
  `ReplicationMsg { changes, last_trade_price }` deltas to rebuild the book
  state exactly, publishes book state changes to the Redis/SQL clusters,
  maintains the statistics, and keeps the snapshot + NATS sequence checkpoint
  for recovery. A slave can be switched to a master in disaster.

The replication message carries the order changes of one execution and the
execution's **last trade price** (`None` when no trade happened). The price
replays the execution output into the slave's state: a `Some` price updates
`last_trade_price` and carries the has-traded state, so a promoted slave owns
both. The trades themselves are not replicated — settlement consumes them
directly.

Both sides of the NATS stream use the same message shape: the master encodes
the pooled message (`PooledReplicationMsg`, whose `Serialize` mirrors
`ReplicationMsg`), the slave decodes the payload directly back into the pooled
type and applies it; its change buffer wraps a detached pool and is freed on
drop (the wire decode allocates the vector either way).

## The change stream is the complete protocol

Each `OrderChange` carries

- `order`: the order as it was **before** the change (a snapshot), and
- `status`: the new state, with the quantity filled by this event.

These two are enough to replay the whole execution on the slave, including the
**removals** — the master's internal `removed` list (the arena indices purged
from the book) is not replication material. The replication invariant is:

> Every removed order has a terminal change (`Filled` or `Canceled`), and every
> terminal change corresponds to a removed order.

The invariant is enforced at runtime by a `debug_assert` in the sweep's merge
step, and covered by the replication tests.

### Change → remove encoding

| master event | change emitted | remove |
| --- | --- | --- |
| maker fully filled | `Filled { filled_quantity }` | yes |
| iceberg / reserve maker consumed | `Filled { filled_quantity }` | yes (hidden discard derivable from the snapshot) |
| non-auto reserve, visible exhausted | `Filled { traded }` | yes (hidden discarded) |
| STP `CancelMaker` / `CancelBoth` | `Canceled { 0, SelfTradePrevention }` | yes |
| lazy GTD expiry | `Canceled { 0, TimeInForceExpired }` | yes |
| user cancel | `Canceled { 0, UserRequested }` | yes |

The identity of the removed order comes from the snapshot's `(user, nonce)`;
the price level deltas come from the snapshot's visible/hidden quantities; the
reason comes from the status.

## Applying a change (slave)

`OrderBook::apply` dispatches each change by whether the order rests in the
book (index lookup on `(user, nonce)`):

| change status | resting (maker) | not resting (taker) |
| --- | --- | --- |
| `Open` | impossible | rest the order |
| `PartiallyFilled { filled }` | deduct `filled`, replay the replenishment | rest with the remainder |
| `Filled { filled }` | remove the order | nothing rests |
| `Canceled { .. }` | remove the order | nothing rests |
| `Rejected { .. }` | impossible | the order never entered the book |

### Deterministic replay requirements

The slave must mirror the master's rules exactly so that the two states
converge bit-for-bit (the replication test asserts full state equality,
including the arena and the queue links):

- **Replenishment** — `PriceLevel::apply_fill` replays the same iceberg /
  reserve replenishment rules as the master's `settle_maker`, including
  re-queuing a replenished maker at the queue tail.
- **Taker remainder reconstruction** — a resting taker's change carries only
  the submitted snapshot and the total filled quantity. The slave replays the
  sweep's consumption greedily (visible tranche by tranche, replenishing as
  the master's `replenish_taker` does), including the final replenishment
  after the last level execution: a taker never rests with an exhausted
  visible tranche while hidden quantity remains.
- **Level totals** — visible/hidden totals are maintained with the same
  additions/subtractions as the master, so both sides converge.

### Statistics

The slave runs with the statistics enabled (`OrderBook::enable_statistics`);
`apply` maintains the book statistics (added / removed / executed / quantity /
value) and the per-price-level statistics. The master skips them for
performance. For taker executions the value is attributed at the taker's
snapshot price (the change stream does not carry per-trade prices).

## What the change stream does not carry

- **The trade details** — per-trade prices, quantities and counterparties go
  to settlement only. The slave does not need them to rebuild the book, only
  the execution's last trade price (carried by the message) and the change
  deltas. Consequently the slave's executed-value statistics attribute a
  taker's fills at the taker's snapshot price (an approximation; makers are
  exact).

## Recovery

The slave keeps a snapshot of the book and the NATS message sequence. Recovery
is snapshot + delta replay: load the snapshot, then apply the replicated
messages after the checkpoint (see `OrderBook::attach_pools` for re-attaching
the deserialized pooled buffers to the runtime pools).
