# The overview
Every symbol's data pipeline is composed of its own **HotPath**: SVD_Pretrade, SVD_OMS_Master, SVD_Settlement and
**SidePath**: SVD_OMS_Slave, Redis_Cluster, SQL_Cluster. The components of the HotPath are deployed in the same host
for ultra low latency wired via share memory IPC, please refer to the ipc crate. And the components of the SidePath
are deployed in a cluster wired via NATS in between SVD_OMS_Master and SVD_OMS_Slave for high availability and
load distribution. The HotPath is responsible for executing the user request, and the SidePath is responsible for
tracking the book state changes and publishing them to the downstream services. The SVD_OMS_Master replicates the order
 change to the SVD_OMS_Slave, which is responsible for tracking the book state changes and publishing them to the
 Redis_Cluster and SQL_Cluster. The SVD_OMS_Slave can also be promoted to a master in case of failure of the current
 master.

## The data flow
The NGINX is the gateway of the user request, it routes the request to the corresponding SVD_Pretrade by symbol
extracted from the request path. The SVD_Pretrade is the pre-trade risk control service, it checks the user request
 and forwards the request to the SVD_OMS_Master via the share memory SPSC queue implemented in the ipc crate. The
 SVD_OMS_Master runs a spin loop to fetch batch of requests from the SPSC queue, it executes the user request and
 replicates the order change to the SVD_OMS_Slave via NATS streaming protocol. The SVD_OMS_Slave runs a spin loop to
 fetch the order change from the NATS stream, it applies the order change to the book state and publishes the book
 state changes to the Redis_Cluster and SQL_Cluster. It also runs a snapshot thread to take a snapshot of the book
 state with a NATS message sequence as the check point, and persist it to the local journal and the Redis_Cluster. The
 snapshot is used for the state recovery of the book, when the SVD_OMS_Slave is restarted, it fetches the latest snapshot
  from the Redis_Cluster, and replays the order change from the NATS stream starting from the check point to rebuild
  the book state. The SVD_OMS_Slave can also be promoted to a master in case of failure of the current master,
  it will take over the NATS stream and start executing the user request.

## Ingress Message and Outgress Message
Please refer to the crates/primitives/src/message/hot_path.rs and crates/primitives/src/message/side_path.rs for the
definition of the ingress message and outgress message.
```Rust
/// Messages sent from user end. It is forwarded to [`SVD_OMS_Master`] from ['SVD_Pretrade']for
/// processing via share memory SPSC queue. The messages are fixed sized for preallocation
/// in share memory.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OrderMsg {
    /// New Order.
    NewOrder(Order),
    /// Cancel Order.
    CancelOrder(CancelOrder),
}

/// Message sent from [`SVD_OMS_Master`] to [`SVD_OMS_Settlement`].
/// Trade defines the exchange between two orders, a matching can generate multiple
/// Trades for an ingress order. The [`SVD_Settlement`] batches it and submit them to
/// Settlement protocol contract. The listener of the [`SVD_OMS_Master`] can fanout the
/// trade events to the downstream system for settlement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trade {
    /// Taker order, the ingress order.
    pub taker: Order,
    /// The remaining quantity of the ingress order.
    pub taker_remaining: Quantity,
    /// Maker order.
    pub maker: Order,
    /// Price at which the trade happens.
    pub price: Price,
    /// Traded quantity of this cross.
    pub traded_quantity: Quantity,
}

/// Replication message contains the changes of the book triggered by an
/// ingress OrderMsg and the last trade price of the execution. The last trade
/// price is `None` when the execution produced no trades; a `Some` price also
/// carries the has-traded state (the flag is true once any execution traded). The listener of
/// the [`SVD_OMS_Master`] can fanout the replication messages to the downstream system
/// for state replication.
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationMsg {
    /// The order changes of the execution.
    pub changes: Vec<OrderChange>,
    /// The price of the last trade of the execution, if any.
    pub last_trade_price: Option<Price>,
}

```
## The features in svd-oms crate

- config: A shared configuration for the orderbook, a [Master/Slave] mode config bind with their corresponding
**Hot Path** or **Side Path** upstream or downstream components' interfaces. The config is loaded from a toml file,
and can be reloaded at runtime via SIGHUP signal.

- oms_master: The master of the orderbook, it runs a spin loop to fetch batch of requests from the SPSC queue,
 executes the user request and replicates the order change to the SVD_OMS_Slave via NATS streaming protocol.
 It also fanout the trades into the downstream system for settlement.

- oms_slave: The slave of the orderbook, it receives the order changes from the SVD_OMS_Master via NATS streaming
protocol and updates its local orderbook state accordingly. It also publishes the order changes to the
Redis_Cluster and SQL_Cluster for state replication, thus that the downstream system can subscribe to the order
changes and update their own state accordingly. It also runs a snapshot thread to take a snapshot of the book
  state with a NATS message sequence as the check point, and persist it to the local journal and Redis Cluster.
  The snapshot is used forthe state recovery of the book. It also can be promoted to a master in case of failure of the
 current master, it will take over the NATS stream and start executing the user request.

- the journal: The journal is a local persistent storage for the orderbook state, it contains a fixed header and
payloads, where the header contains dual instances of metadata of the last two writes, with each stores the
monotonic increasing sequence number of the write operation, the offset and length of the writen payload. When
writing the binary log, the log engine always replace the metadata which is to be expired in the header, making the
current write as the latest version while letting the last write as the previous one, thus that we can detect the
corruption of the journal by checking the sequence number of the last two writes. And select the lower version as
the valid one for recovery.

- state recovery: The state recovery is the process of rebuilding the orderbook state from the journal and the NATS
stream. When the SVD_OMS_Slave is restarted, it fetches the latest snapshot from the journal, and replays the order
change from the NATS stream starting from the check point to rebuild the book state. If the journal is not available,
it will fetch the latest snapshot from the Redis_Cluster, and replays the order change from the NATS.

- mode switch: It is triggered by system admin after get prepared: Dual instances of the SVD_OMS_Slave are running,
with both of them get synced to the latest state, then a switching command (the configuration update event) is sent to
one of the SVD_OMS_Slave to promote it to a master.