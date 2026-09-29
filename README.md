# dex-rs

A decentralized exchange with **off-chain execution and on-chain settlement**.

[![CI](https://github.com/Jason-Zhangxin-Chen/dex-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/Jason-Zhangxin-Chen/dex-rs/actions/workflows/ci.yml)

## Overview

dex-rs is a decentralized exchange composed of two halves:

- **On-chain protocols** for margin-account management and settlement management,
  using EVM-compatible address/signature primitives (`alloy` is pinned for chain interop).
- **An off-chain distributed system** for order execution, wired by low latency IPC queue and NATS streaming protocols
  made up of seven services:

  | Service | Responsibility |
  | --- | --- |
  | `svd-pretrade` | Pre-trade risk checks |
  | `svd-oms-master` | Order matching (order management system) |
  | `svd-oms-slave` | off the svd-oms-master load by state replication and publishing changes |
  | `svd-settlement` | Settlement management |
  | `svd-sync` | On-chain state to svd-storage synchronization  |
  | `svd-pub-sub` | System state pub/sub services  |
  | `svd-query` | System state query services  |

## Architecture and design philosophy.
The design targets to 3 critical properties: Low latency, scalability and recoverability. There is no blocking execution
on the hot path. That is why the share memory based SPSC and NATS messaging are employed on the hot path, the other wire
protocols were considered, for example Kafka, Redpanda and Pulsar, as they introduce much more latency with heavy
execution context, so we eventually decide the current design and architecture.

- **The Architecture**

![Architecture diagram](./doc/architecture.png)

- **Hot path**
[User]---(Order/CancelOrder)--->[NGINX]--->[SVD_Pretrade]--->[SVD_OMS_Master]--->[SVD_Settlement]--->[Web3RPCNodes].
The user's request are routed by symbol as it is explicitly declared in the api path exposed by the SVD_Pretrade, thus
NGINX route the market's request to the corresponding [SVD_Pretrade], in between the SVDs, there is a file mapped share
memory SPSC which provides persistence messaging to wire the pipeline of the market, so the SVDs of the same market are
deployed in the same host for ultra low latency. All the resources required for the computing in this pipeline are
pre-allocated and reused. The book state tracking load of [SVD_OMS_Master] is moved to [SVD_OMS_Slave] by state
replication, thus that the Master can focus on the matching and deliver the trade event to [SVD_Settlement] only.

- **Side path**
[SVD_OMS_Master]---(NATS messages)--->[SVD_OMS_Slave]---(Book State Changes)--->[[Redis_Cluster], [SQL_Cluster],
[SVD_PubSub]].
The master pushes change events via the NATS stream with configurable sync/async mode to the slave node, the replication
introduces the high availability and load sharing of [SVD_OMS_Master] because the [SVD_OMS_Slave] tracks the book state
changes and publish them into [Redis_Cluster], [SQL_Cluster] and the [SVD_PubSub] cluster. Also the [SVD_OMS_Slave] can
switch to an [SVD_OMS_Master] when the [SVD_OMS_Master] is in disaster.

- **MarketData PubSub**
[User]---(websock)--->[NGINX]---(round robin relay)--->[SVD_PubSub]Cluster--->[Redis_Cluster].
The subscription comes from the [User] end via web socket, [NGINX] forward the HTTP handshake to [SVD_PubSub] cluster by
round robin, once the session is being created, the subscriptions from the [User] end are processed in one of the
[SVD_PubSub] instance, the instance then subscribe to [Redis_Cluster] for the corresponding topic asked by the [User].
Both [SVD_OMS_Slave] and [SVD_SYNC] are state change producers, one produces book state changes and the other one
produces margin position changes synced from on-chain settlement protocol. They push the changes to the [Redis_Cluster],
with [Redis_Cluster]'s built-in Pub&Sub protocols, the cluster pushes changes to those [SVD_PubSub] instances which
subscribe to the corresponding topics on demand as the [User] requested.

- **Data Query**
[User]---(HTTP)--->[NGINX]---(round robin relay)--->[SVD_Query]Cluster--->[Redis_Cluster].
The data query comes from User via HTTP RPC, NGINX works as a load balancer which forward the requests to [SVD_Query]
Cluster by round robin. The instance in the cluster fetches data from [Redis_Cluster].

- **OMS_Master and OMS_Slave**
The responsibility of an [SVD_OMS_Master] is that, it executes the user request, and replicate the changes to the
 [SVD_OMS_Slave], it also publish the trade messages to the downstream [SVD_Settlement] service via share memory SPSC
 queue. All the other computing are offload to [SVD_OMS_Slave], for example the statistics, the market data publishing
 over the [Redis_Cluster] and [SQL_Cluster]. The [SVD_OMS_Slave] also manages the snapshot of the book and the NATS
 message sequence, with this checkpoint and the sync point, it manages the state recovery of a book. A slave can be
 switched to a master during runtime.

## Workspace layout

| Crate | Role |
| --- | --- |
| `crates/cache` | common cache libs which place object pool, etc... |
| `crates/cryptography` | common cryptography libs which place hashing, signature signing and verifications, etc... |
| `crates/ipc` | common libs which place IPC functions like shared memory SPSC queue, etc... |
| `crates/net` | common libs which place networking helpers like web socket, https, etc... |
| `crates/primitives` | Core matching-engine types: order model, order book, risk, STP, trades, events, etc... |
| `crates/storage` | helpers for redis cluster and SQL cluster I/O. |
| `crates/svd-oms` | The oms service which runs for different mode: master or slave. |
| `crates/svd-pretrade` | The pre-trade service for pre-trade risk management. |
| `crates/svd-pubsub` | The pubsub service for realtime state change publishing. |
| `crates/svd-settlement` | The settlement service which process the trade events from oms. |
| `crates/svd-sync` | The sync service which sync the on-chain settlement protocol's state to Redis and SQL cluster. |
| `crates/svd-query` | The query service which provides data query for user end request. |
| `crates/util` | Shared helpers |

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
