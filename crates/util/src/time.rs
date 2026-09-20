//! Returns the wall-clock time in milliseconds since the UNIX epoch.

use std::time::{SystemTime, UNIX_EPOCH};

/// Now in millisecond. It is not a traditional blocking syscall, it is a userspace call that
/// reads a kernel-maintained share memory page.
#[must_use = "The current time returned must be used"]
pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}
