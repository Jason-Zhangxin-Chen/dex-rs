//! svd-oms: the OMS service of the exchange system.
//!
//! The service runs one order book per process, in one of two modes:
//!
//! - **Master** ([`engine::master`]): the hot path. A pinned core thread
//!   spins on the share-memory SPSC queue from SVD_Pretrade, executes the
//!   requests on the book, and fans out the replication messages to
//!   SVD_OMS_Slave via NATS JetStream and the trade events to SVD_Settlement
//!   via a share-memory SPSC queue. The fanout I/O runs on dedicated threads
//!   so the core thread never blocks.
//! - **Slave** ([`engine::slave`]): the side path. A pinned core thread
//!   consumes the replication stream of the master, applies the changes to a
//!   local book copy, and hands the changes and periodic snapshots to a
//!   side I/O thread that publishes them to Redis and persists them to the
//!   local journal. On restart the slave recovers from the journal (or the
//!   Redis snapshot) and replays the stream from the checkpoint sequence. A
//!   configuration update can promote the slave to a master.

pub mod config;
pub mod engine;
pub mod journal;
pub mod naming;
pub mod snapshot;
