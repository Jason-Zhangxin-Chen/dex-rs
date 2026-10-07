//! Helpers for the storage clusters of the system: Redis now, SQL later.
//!
//! The [`ChangeSink`] trait is the pluggable interface the services use to
//! publish state changes and to persist / load snapshots. The Redis
//! implementation ([`RedisStore`]) is wired to the Redis cluster; the SQL
//! cluster plugs in here once its schema is defined.

pub mod journal;
pub mod redis_store;

pub use redis_store::{RedisConfig, RedisKeyStore, RedisStore};

use std::error::Error;
use std::fmt;

/// Errors of the storage helpers.
#[derive(Debug)]
pub enum StorageError {
    /// The downstream store rejected the operation.
    Redis(redis::RedisError),
    /// The operation is not supported by the sink (e.g. snapshot load on a
    /// publish-only sink).
    Unsupported(&'static str),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Redis(err) => write!(f, "redis: {err}"),
            StorageError::Unsupported(op) => write!(f, "unsupported operation: {op}"),
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            StorageError::Redis(err) => Some(err),
            StorageError::Unsupported(_) => None,
        }
    }
}

impl From<redis::RedisError> for StorageError {
    fn from(err: redis::RedisError) -> Self {
        StorageError::Redis(err)
    }
}

/// Sink of book state changes and snapshots, bound to one symbol's
/// resources. The services depend on this trait so that a SQL sink can
/// replace or accompany the Redis one without touching the engine.
pub trait ChangeSink {
    /// Publishes one state change payload to the downstream cluster.
    fn publish_change(&mut self, payload: &[u8]) -> Result<(), StorageError>;

    /// Persists the latest snapshot payload of the book.
    fn save_snapshot(&mut self, payload: &[u8]) -> Result<(), StorageError>;

    /// Loads the latest snapshot payload of the book, if any.
    fn load_snapshot(&mut self) -> Result<Option<Vec<u8>>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_error_display() {
        let err = StorageError::Unsupported("load_snapshot");
        assert_eq!(err.to_string(), "unsupported operation: load_snapshot");
    }

    #[test]
    fn test_redis_error_conversion() {
        let err = redis::RedisError::from((
            redis::ErrorKind::Io,
            "description",
            "broken pipe".to_string(),
        ));
        let storage: StorageError = err.into();
        assert!(matches!(storage, StorageError::Redis(_)));
        assert!(storage.to_string().contains("redis:"));
    }
}
