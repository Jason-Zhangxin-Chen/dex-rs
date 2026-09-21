# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

- Build: `cargo build --workspace --locked`
- Test: `cargo nextest run --workspace` (CI; `cargo test --workspace` is fine locally)
- Single test: `cargo test -p primitives <filter>` (e.g. `cargo test -p primitives test_ser_deser_price`)
  or `cargo nextest run -p primitives <filter>`
- Lint: `cargo clippy --workspace --all-targets --locked -- -D warnings`
- Format: `cargo fmt --all` (verify with `cargo fmt --all -- --check`)
- Dependency audit: `cargo deny check`

`cargo-nextest` and `cargo-deny` are not installed locally by default — install with
`cargo install cargo-nextest` / `cargo install cargo-deny` if needed (CI installs them per job).

CI (`all-checks` gate in `.github/workflows/ci.yml`) requires build + nextest, clippy with
`-D warnings`, rustfmt check, and cargo-deny to all pass; CI also builds with
`RUSTFLAGS="-D warnings"`, so treat warnings as errors.

## Architecture

dex-rs is a DEX with **off-chain execution and on-chain settlement**. The full rules and
the detailed type model live in `AGENTS.md`; the big picture:

- **`crates/primitives`** — the shared core: matching-engine types consumed by every
  service (order model, order book, risk, STP, trade results, events). The only populated
  crate today.
- **Five services wired by Redpanda** (Kafka-compatible bus): `svd-pretrade` (pre-trade
  risk), `svd-oms` (order matching), `svd-posttrade` (post-trade processing),
  `svd-settle` (settlement), `svd-sync` (state sync). Their binaries are scaffolds today;
  topic wiring is not in code yet.
- **Support crates**: `util` (helpers), `cryptography` (scheme-agnostic crypto traits), and
  the scaffolded `cache` / `codec` / `net` / `storage`.
- **On-chain interop**: EVM address/signature types in `primitives`; `alloy` is
   pinned as workspace dependencies.

Key design invariants to preserve when editing (details in `AGENTS.md`):

- **Wire format**: MessagePack via rmp-serde; value types are transparent newtypes — the
  wire format must stay deterministic, enforced by round-trip tests.
- **Cache-line discipline**: `Order` is split hot/cold; `OrderNode` is `repr(C, align(64))`
  with u32 index links (`NIL = u32::MAX`); producers on the wire set the links to `NIL`.
- **The matching `impl` is not written yet** — `OrderBook` is structure + configuration
  only; the book exposes listener hooks (`TradeListener`, `PriceLevelChangedListener`,
  `OrderStatusListener`, `StatisticListener`) for the services to plug into.

## Where the rules live

- CI: `.github/workflows/ci.yml`; toolchain pin: `rust-toolchain.toml` (Rust 1.94.1 —
  keep in lockstep with CI)
- Lint knobs: `clippy.toml` (complexity thresholds, `doc-valid-idents` domain terms)
- Format: `rustfmt.toml` (max width 100); dependency policy: `deny.toml` (crates.io only,
  permissive licenses, no yanked)
- Commit style and branch flow: see `AGENTS.md` (lowercase `type, description` messages)
- Memory allocation: Do not allocate heap memory on the hot path!