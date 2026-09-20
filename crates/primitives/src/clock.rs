//! A source of wall-clock or logical millisecond timestamp.

use crate::value::TimestampMs;
use std::fmt;
use util::time::now_ms;

/// Clock provides clock in milliseconds since UNIX epoch.
pub trait Clock: fmt::Debug {
    /// Current millisecond timestamp.
    fn now_millis(&self) -> TimestampMs;
}

/// Monotonic Clock since UNIX epoch in milliseconds.
#[derive(Debug, Default, Clone, Copy)]
pub struct MonotonicClock;

impl Clock for MonotonicClock {
    #[inline]
    fn now_millis(&self) -> TimestampMs {
        TimestampMs(now_ms())
    }
}
