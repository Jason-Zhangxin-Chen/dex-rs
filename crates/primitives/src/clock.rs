//! A source of wall-clock or logical millisecond timestamp.

use std::fmt;
use crate::value::TimestampMs;

pub trait Clock: fmt::Debug {
    /// Current millisecond timestamp.
    ///
    /// Semantics depend on the implementation:
    /// - production ([`MonotonicClock`]): wall-clock milliseconds since
    ///   the Unix epoch.
    /// - replay / test ([`StubClock`]): a monotonic logical counter,
    ///   not wall-clock.
    fn now_millis(&self) -> TimestampMs;
}

