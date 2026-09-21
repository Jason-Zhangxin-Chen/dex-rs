# AGENTS.md

Guidance for AI coding agents (Claude Code, Codex, Cursor, etc.) working in this repository.

## Project overview

dex-rs is a decentralized exchange with **off-chain execution and on-chain settlement**:

- **On-chain protocols** handle margin-account management and settlement management.
  Current primitives are EVM-compatible (`Address` = `[u8; 20]`, `Signature` = `[u8; 65]`);
  `alloy` is pinned as workspace dependencies for chain interop.
- **Off-chain distributed system wired by Redpanda** (Kafka-compatible event bus) composed
  of five services: `svd-pretrade` (pre-trade risk), `svd-oms` (order matching),
  `svd-posttrade` (post-trade processing), `svd-settle` (settlement), `svd-sync` (state sync).
- **`crates/primitives`** is the heart of the workspace: the matching-engine types and core
  logic that every service consumes.

Status: `primitives` is the only populated crate. The five service binaries and the
`cache` / `codec` / `net` / `storage` crates are scaffolds; Redpanda topic wiring does not
exist in code yet — the architecture above is the target design.

## Workspace layout

Rust workspace (edition 2024, resolver 2, Apache-2.0). Crates:

| Crate | Role |
| --- | --- |
| `crates/primitives` | Core matching-engine types (order model, order book, risk, STP, trades, events) |
| `crates/util` | Shared helpers (`time::now_ms()` — userspace call, not a syscall) |
| `crates/cryptography` | Scheme-agnostic crypto traits (`Signer` / `PublicKey` / `Signature` / `PrivateKey` / `CryptoError`) — no concrete impls yet |
| `crates/codec`, `crates/net`, `crates/storage`, `crates/cache` | Scaffolds (placeholder `add`) |
| `crates/svd-{pretrade,oms,posttrade,settle,sync}` | Service binaries (scaffolds) |

Workspace deps (import with `workspace = true`): `alloy`, `solana-sdk`, `serde`,
`rmp-serde` (MessagePack), `slab`, `rustc-hash`.

## Architecture — the matching-engine core

### Order model (hot/cold split)

The order structure is split for cache-line-friendly loading; this design is deliberate
and load-bearing:

- `OrderHot` (`repr(C)`): the data touched on the match path — the `(Address, Nonce)` key,
  `Price`, `Quantity`, `TimeInForce`, `Side`.
- `OrderCold`: `id` (`Hash32`), `symbol`, `Signature`, `TimestampMs`, plus order-type
  parameters in `OrderKind`: `Standard`, `Iceberg`, `PostOnly`, `TrailingStop`, `Pegged`,
  `MarketToLimit`, `ReserveOrder`.
- `OrderNode` (`repr(C, align(64))`): `Order` plus `prev` / `next` `OrderIdx` links forming
  the time-priority queue inside a price level. The links are serialized as part of book
  snapshots (a restored book keeps its list structure); on the wire (Redpanda/Kafka)
  producers must set them to `NIL` — the engine rewrites them on insertion.
- `OrderIdx = u32` with sentinel `NIL = u32::MAX` — a small index type to keep book
  structures cache-friendly.

### Order book (`orderbook::book::OrderBook`)

- All live orders in a `slab::Slab<OrderNode>` arena; bids/asks in
  `BTreeMap<Price, PriceLevel>`; order lookup via `FxHashMap<(Address, Nonce), OrderIdx>`;
  per-user tracking via `FxHashMap<Address, Vec<OrderIdx>>`.
- Each `PriceLevel` holds a time-priority `OrderQueue` (head/tail indices) with
  visible/hidden quantity accounting (iceberg support).
- Non-blocking listener hooks push events out: `TradeListener` (to settlement, storage,
  messaging), `PriceLevelChangedListener`, `OrderStatusListener`, `StatisticListener`.
- Config: tick size, lot size, min/max order size, `STPMode`, `Clock` (injected trait —
  `MonotonicClock` provided), kill switch.
- Pre-trade risk (`orderbook::risk::RiskState`): per-account open-order count, resting
  notional, and price-band (bps vs. a `ReferencePriceSource`) checks. All checks are no-ops
  when no `RiskConfig` is set.
- Self-trade prevention (`orderbook::stp::STPMode`): `None` / `CancelTaker` / `CancelMaker`
  / `CancelBoth`; zero-address orders always bypass STP.
- The matching `impl` (add/cancel/match) is not written yet — only the type structure
  exists.

### Lifecycle and value types

- `TradeResult` carries the taker order, `remaining_quantity`, `MatchOutcome`
  (`Filled` / `PartiallyFilled` / `NotFilled` / `Killed` / `Rejected`), `quote_notional`
  (`Σ price × quantity` as `Quote`), the list of maker `Trade`s, and a `TimestampMs`.
- `OrderStatus` tracks the lifecycle (`Open` → `PartiallyFilled` / `Filled` / `Canceled` /
  `Rejected`) with closed taxonomies of `CancelReason` and `RejectReason`.
- Value types are newtypes over integers: `Price(u64)`, `Quantity(u64)`, `TimestampMs(u64)`,
  `Quote(u128)`; `Symbol` / `Hash32` are `[u8; 32]`. All serialize **transparently** — the
  wire format is MessagePack (rmp-serde) with no wrappers or length prefixes. This is
  load-bearing: round-trip/wire-format tests enforce it, and changing serde annotations can
  break cross-service compatibility.

## Development rules
- Do not allocate heap memory on the hot path!
### Toolchain

`rust-toolchain.toml` pins Rust **1.94.1** (first stable of edition 2024, which every crate
uses). CI is pinned to the same version — keep them in lockstep; do not bump one without
updating the other. `Cargo.lock` is committed; dependabot (weekly, Monday 09:00 UTC) owns
dependency bumps with grouped patch/minor updates.

### CI gates (`.github/workflows/ci.yml`)

The single required check `all-checks` composes four jobs — every PR must pass all of:

1. Build & test: `cargo build --workspace --locked` and `cargo nextest run --workspace`
   (CI sets `RUSTFLAGS="-D warnings"` — warnings fail the build)
2. Clippy: `cargo clippy --workspace --all-targets --locked -- -D warnings`
3. Format: `cargo fmt --all -- --check`
4. Dependency audit: `cargo deny check`

### Lint and format configuration

- `clippy.toml`: `cognitive-complexity-threshold = 30`, `type-complexity-threshold = 400`,
  and a `doc-valid-idents` whitelist of domain terms (DEX, Redpanda, OrderBook, …). Add new
  domain terms there rather than sprinkling `#[allow]` on doc lints.
- `rustfmt.toml`: `max_width = 100`, imports/modules reordered, `use_small_heuristics = "Max"`,
  Unix newlines.
- The core crate must stay **executor-agnostic**: no tokio types in public signatures
  (documented in `clippy.toml`; enforced via `#[deny(clippy::disallowed_types)]`).

### Dependency policy (`deny.toml`)

- Sources: crates.io only — git and unknown registries are denied; wildcard versions denied.
- Licenses: permissive / weak-copyleft allow-list (MIT, Apache-2.0, BSD, BSL-1.0, ISC,
  MPL-2.0, …) — new dependencies must fit; `ring` is clarified as `MIT AND ISC AND OpenSSL`.
- Advisories: yanked versions denied, `unsound = all`, `unmaintained = workspace`.

## Conventions

- **Commit messages**: lowercase `<type-or-scope>, <description>`, e.g.
  `feature, zero value definitions for value types.` /
  `refine, cache line friendly structure design for book.` /
  `lint, fix fmt errors.` / `test, fix test.` / `ci, target to main.`
- **Workflow**: feature branch off `main` → PR (CI also runs on merge queues). Branches are
  named for the work (`primitives`, `serde`, `book`, `visibility`, `core`, `impl`).
- **Tests**: inline `#[cfg(test)] mod tests` in the same source file, heavy on MessagePack
  round-trips and wire-format assertions. CI runs nextest; plain `cargo test` works locally.
- **Performance principles**: cache-line-friendly hot/cold splits, lock-free match path,
  no `Copy` for large types, non-blocking listeners. When touching hot structures, preserve
  the layout discipline — there is commit history dedicated to it.
