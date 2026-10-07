//! The settlement service: the last hop of the hot path and the bridge
//! back to the chain. See `spec.md` for the full design.

pub mod batch;
pub mod calldata;
pub mod chain;
pub mod classifier;
pub mod config;
pub mod engine;
pub mod journal;
pub mod naming;
pub mod publisher;
pub mod sql;
pub mod submitter;
