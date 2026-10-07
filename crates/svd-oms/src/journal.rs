//! The journal of the book snapshots, from the storage crate.
//!
//! The implementation moved to [`storage::journal`] so the settlement
//! service shares the same persistence machinery. The `Journal` here is the
//! snapshot journal: each write replaces the latest snapshot, the previous
//! one survives as the fallback of the next write.

pub use storage::journal::{HEADER_SIZE, Journal, JournalError};
