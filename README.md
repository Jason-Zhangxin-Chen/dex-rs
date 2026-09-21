# dex-rs

A decentralized exchange with **off-chain execution and on-chain settlement**.

[![CI](https://github.com/Jason-Zhangxin-Chen/dex-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/Jason-Zhangxin-Chen/dex-rs/actions/workflows/ci.yml)

## Overview

dex-rs is a decentralized exchange composed of two halves:

- **On-chain protocols** for margin-account management and settlement management,
  using EVM-compatible address/signature primitives (`alloy` is pinned for chain interop).
- **An off-chain distributed system wired by Redpanda** (Kafka-compatible event bus) made
  up of five services:

  | Service | Responsibility |
  | --- | --- |
  | `svd-pretrade` | Pre-trade risk checks |
  | `svd-oms` | Order matching (order management system) |
  | `svd-posttrade` | Post-trade processing |
  | `svd-settle` | Settlement management |
  | `svd-sync` | State synchronization |

All services consume the shared matching-engine types from **`crates/primitives`** — the
heart of the workspace: the order model, the order book, risk and self-trade-prevention
layers, trade results, and lifecycle events, all encoded as deterministic MessagePack on
the wire.

> **Status:** `primitives` is the only populated crate today. The five service binaries
> and the supporting crates are scaffolds; the Redpanda topic wiring is not in code yet —
> the architecture above is the target design.

## Workspace layout

| Crate | Role |
| --- | --- |
| `crates/primitives` | Core matching-engine types (order model, order book, risk, STP, trades, events) |
| `crates/util` | Shared helpers |
| `crates/cryptography` | Scheme-agnostic crypto traits (`Signer` / `PublicKey` / `Signature` / `PrivateKey`) |
| `crates/codec`, `crates/net`, `crates/storage`, `crates/cache` | Scaffolds — future wire codecs, networking, persistence, caching |
| `crates/svd-{pretrade,oms,posttrade,settle,sync}` | The five service binaries |

## Getting started

The workspace uses Rust **edition 2024**, pinned to toolchain **1.94.1** via
`rust-toolchain.toml` (rustup installs it automatically).

```bash
cargo build --workspace --locked   # build everything
cargo test --workspace             # run all tests (CI uses cargo nextest)
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all                    # format (CI checks with -- --check)
cargo deny check                   # license / advisory / source audit
```

Run a single test with `cargo test -p primitives <name>`.

## Development

- **CI** (`.github/workflows/ci.yml`) gates every PR on build + tests, clippy with
  `-D warnings`, rustfmt, and `cargo-deny`. Treat warnings as errors.
- **Dependencies** are audited by `deny.toml`: crates.io only, permissive licenses,
  no yanked versions. `Cargo.lock` is committed; dependabot owns dependency bumps.
- **Performance discipline:** cache-line-friendly structure layouts (hot/cold order
  splits), a lock-free match path, and **no heap allocation on the hot path**.
- **Commit style:** lowercase `<type-or-scope>, <description>`, e.g.
  `feature, zero value definitions for value types.`
- **Agent guidance:** coding agents should read [`AGENTS.md`](AGENTS.md) and
  [`CLAUDE.md`](CLAUDE.md) for the full architecture and rules.

## License

[Apache-2.0](LICENSE)
