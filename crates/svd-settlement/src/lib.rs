//! The settlement service: the batch-assembling core thread, the submitter
//! pool and the outcome publishers.
//!
//! The `chain` module is always exported for the gas / nonce / mock /
//! operator building blocks; the alloy bindings and the live client are
//! feature-gated behind `chain-alloy`.

pub mod batch;
pub mod calldata;
pub mod chain;
pub mod classifier;
pub mod config;
pub mod core;
pub mod engine;
pub mod naming;
pub mod publisher;
pub mod seq;
pub mod sql;
pub mod submitter;
