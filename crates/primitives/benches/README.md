# OrderBook execution benchmark

The single-thread benchmark of the dex-rs matching engine (`primitives` crate). It
measures the end-to-end `OrderBook::execute()` path for one order: admission checks,
the matching sweep over the price levels, pooled buffer acquire/release, and the
listener fanout — the same code path the OMS master runs on the hot path.

## Run it

```bash
cargo test --release -p primitives book_benchmark -- --ignored --nocapture
```

The benchmark is an `#[ignore]`d unit test in
[`book.rs`](../src/orderbook/book.rs), so the CI test suites skip it. A full run
covers five scenarios and takes roughly two minutes.

## Test design

### Scenarios

Each scenario prepares and persists its own seed data set and fires a dedicated
order stream:

| scenario | seed orders | fire stream | what it exercises |
| --- | --- | --- | --- |
| `standard` | plain GTC limit orders | standard GTC orders | the basic matching path |
| `iceberg` | iceberg makers (visible 1–100, hidden 20–200) | iceberg orders (hidden 0–100) | tranche replenishment and requeueing |
| `reserve` | auto-replenishing reserve makers (threshold 5) | reserve orders | reserve replenishment on the sweep |
| `ioc` | plain GTC limit orders | IOC orders | the immediate-or-cancel path |
| `mixed` | standard / iceberg / reserve mix | standard / iceberg / reserve / IOC mix | the combined workload |

### Data persistence

- The 20 000 resting orders of each scenario are generated **deterministically**
  (a built-in xorshift64 PRNG, no `rand` dependency) and persisted as MessagePack
  to `data/{scenario}_seed.bin` on the first run. Every later run loads the same
  bytes, so repeated runs inject the identical book. Bids sit below 10 000 and
  asks above, so the seed book never crosses itself.
- The fire streams are deterministic too (per-scenario seeds); they are
  regenerated in memory on each run.

### Fire stream and steady state

The fire stream alternates buys just above and sells just below the 10 000 price
boundary. The two sides keep consuming each other's resting orders, so the book
stays at a steady size for the whole measurement (confirmed by the
*resting orders at end* figures: ~20k in every scenario). Without this the book
would grow without bound over a 10 s phase.

### Phases

1. **Inject** — the 20 000 persisted seed orders run through `execute()`.
2. **Warmup** — 500 000 untimed orders fill the memory pools, the arena and the
   price levels so the statistics never capture first-touch costs.
3. **Latency phase (10 s)** — per-order `Instant` timings around `execute()`;
   p50/p90/p99 are nearest-rank percentiles over the sorted nanosecond samples.
4. **Throughput phase (10 s)** — the batch is timed only as a whole, with no
   per-order timing overhead, so the throughput figure is not diluted by the
   measurement itself.

### Pool tuning

The benchmark book pre-allocates its containers from the cold config so the
measurement phases never reallocate:

| knob | value | sized for |
| --- | --- | --- |
| `arena_size` / `order_index_size` | 100 000 | seed orders + steady-state resting layer |
| `user_order_map_size` | 256 | ~250 benchmark users |
| `price_level_map_size` | 1 024 | the 9 800–10 200 price range |
| `order_index_list_size` | 65 536 | per-user resting order lists |
| `order_index_list_pool_size` / `trade_list_pool_size` | 256 / 128 | pool depth of the hot-path buffers |
| `trade_list_size` | 32 | trades collected per level execution |

## Environment

| metric | value |
| --- | --- |
| machine | Intel(R) Core(TM) i7-8665U CPU @ 1.90GHz |
| OS | Linux 5.15.0-139-generic x86_64 |
| toolchain | rustc 1.94.1 |
| build profile | release |

## Results

Measured on 2026-10-01 at commit `ef7f8e1`.

### Summary

| scenario | orders (latency phase) | p50 | p90 | p99 | mean | throughput |
| --- | --- | --- | --- | --- | --- | --- |
| standard | 19 317 596 | 0.36 µs | 0.94 µs | 1.45 µs | 0.47 µs | 2 120 524 orders/s |
| iceberg | 5 268 563 | 0.75 µs | 4.58 µs | 9.81 µs | 1.84 µs | 538 377 orders/s |
| reserve | 4 871 005 | 0.68 µs | 5.16 µs | 6.80 µs | 1.99 µs | 560 108 orders/s |
| ioc | 39 868 818 | 0.20 µs | 0.21 µs | 0.29 µs | 0.19 µs | 4 594 747 orders/s |
| mixed | 8 974 302 | 0.47 µs | 2.55 µs | 6.37 µs | 1.03 µs | 948 353 orders/s |

### standard

| metric | value |
| --- | --- |
| trades generated | 40 137 868 |
| resting orders at end | 20 790 |
| max latency | 80.34 µs |

| bucket (µs) | count |
| --- | --- |
| < 1 | 18 230 300 |
| 1 – 2 | 1 063 320 |
| 2 – 4 | 22 338 |
| 4 – 8 | 1 179 |
| 8 – 16 | 326 |
| 16 – 32 | 108 |
| 32 – 64 | 23 |
| ≥ 64 | 2 |

### iceberg

| metric | value |
| --- | --- |
| trades generated | 152 696 966 |
| resting orders at end | 21 732 |
| max latency | 165.97 µs |

| bucket (µs) | count |
| --- | --- |
| < 1 | 3 029 481 |
| 1 – 2 | 598 839 |
| 2 – 4 | 933 978 |
| 4 – 8 | 611 154 |
| 8 – 16 | 79 677 |
| 16 – 32 | 15 333 |
| 32 – 64 | 97 |
| ≥ 64 | 4 |

### reserve

| metric | value |
| --- | --- |
| trades generated | 80 440 381 |
| resting orders at end | 22 217 |
| max latency | 157.58 µs |

| bucket (µs) | count |
| --- | --- |
| < 1 | 2 598 571 |
| 1 – 2 | 429 857 |
| 2 – 4 | 865 736 |
| 4 – 8 | 972 203 |
| 8 – 16 | 4 552 |
| 16 – 32 | 69 |
| 32 – 64 | 13 |
| ≥ 64 | 4 |

### ioc

| metric | value |
| --- | --- |
| trades generated | 3 013 |
| resting orders at end | 18 989 |
| max latency | 2431.19 µs |

| bucket (µs) | count |
| --- | --- |
| < 1 | 39 866 420 |
| 1 – 2 | 550 |
| 2 – 4 | 1 397 |
| 4 – 8 | 298 |
| 8 – 16 | 129 |
| 16 – 32 | 11 |
| 32 – 64 | 6 |
| ≥ 64 | 7 |

### mixed

| metric | value |
| --- | --- |
| trades generated | 81 605 683 |
| resting orders at end | 19 382 |
| max latency | 508.46 µs |

| bucket (µs) | count |
| --- | --- |
| < 1 | 6 530 049 |
| 1 – 2 | 1 107 577 |
| 2 – 4 | 1 098 081 |
| 4 – 8 | 177 268 |
| 8 – 16 | 43 788 |
| 16 – 32 | 17 475 |
| 32 – 64 | 61 |
| ≥ 64 | 3 |

## Observations

- **standard** matches in sub-microsecond median latency (p50 0.36 µs) at ~2.1M
  orders/s.
- **ioc** is the fastest path (4.6M orders/s) but measures the reject/cancel
  lifecycle, not matching: the IOC stream consumed the seed book during warmup
  and then mostly cancelled against an empty book (3 013 trades in the whole
  run).
- **iceberg / reserve** are the slowest per order for a good reason: replenishing
  makers make every taker sweep deep, averaging ~30–40 trades per order. Their
  latency tail also reflects the trade-list pool capacity (`trade_list_size` = 32)
  being exceeded by deep sweeps — a tuning knob for the next iteration.
- The warmup phase removed the multi-millisecond first-touch outliers of earlier
  runs: max latency is now tens to a few hundred microseconds across scenarios.
- Every scenario confirms the steady-state design: resting orders at end ≈ 20k,
  i.e. the book size is bounded for the whole measurement.

The raw machine output of each run is written to `reports/book_bench_report.md`
(gitignored, regenerated on every run); this file is the curated report.
