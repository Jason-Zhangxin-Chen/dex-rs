//! svd-pretrade: the pre-trade gateway of the hot path.
//!
//! The service accepts the user's HTTP requests routed by NGINX (the
//! request path declares the symbol), runs the pre-trade checks — the
//! signature verification, the order sanity and the on-chain margin
//! pre-check — and forwards the accepted requests to the SVD_OMS_Master
//! via the share memory SPSC queue.
//!
//! - **The HTTP handler threads** run the whole validation pipeline per
//!   request, including the on-demand pull of the account's margin state on
//!   the first order, and push the accepted requests into a shared
//!   pre-allocated lock-free MPSC queue.
//! - **The core thread** is pure data forwarding: it drains the MPSC queue
//!   in batches and moves them into the SPSC queue wired to the master. No
//!   checks, no locks, no blocking, no allocation.
//! - **The feed threads** keep the shared margin cache fresh: the margin
//!   feed applies the [SVD_Sync] updates (final states, the last one wins)
//!   and sweeps the idle entries; the settlement feed re-injects the
//!   innocent side's crossed quantity of a failed settlement into the
//!   pipeline and blocks the at-fault account on an insufficient margin.

pub mod config;
pub mod engine;
pub mod feed;
pub mod gateway;
pub mod margin;
pub mod naming;
