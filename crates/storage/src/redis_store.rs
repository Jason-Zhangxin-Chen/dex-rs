//! Redis cluster helpers: a synchronous client bound to one symbol's
//! resources (change channel and snapshot key). The services use it on their
//! side-path threads only, never on the hot path.
//!
//! The store talks to a Redis **cluster** through
//! [`redis::cluster::ClusterClient`]. A cluster connection spans the whole
//! cluster (one TCP connection per node plus the slot map), which is
//! expensive to set up, so the store keeps a bounded pool of connections:
//! every operation checks a connection out and returns it afterwards. The
//! pool size is configured in [`RedisConfig::pool_size`].

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use serde::{Deserialize, Serialize};

use crate::{ChangeSink, StorageError};

/// Default number of pooled cluster connections.
const DEFAULT_POOL_SIZE: usize = 4;

/// Connection config of the Redis cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    /// Seed node URLs of the cluster, e.g. `redis://127.0.0.1:6379`. At
    /// least one node must be reachable; the cluster topology is discovered
    /// from the seeds.
    #[serde(default = "default_urls")]
    pub urls: Vec<String>,
    /// Number of cluster connections kept in the pool. An operation checks
    /// a connection out and returns it afterwards; a checkout beyond the
    /// pool size blocks until a connection comes back.
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self { urls: default_urls(), pool_size: default_pool_size() }
    }
}

fn default_urls() -> Vec<String> {
    vec!["redis://127.0.0.1:6379".to_string()]
}

fn default_pool_size() -> usize {
    DEFAULT_POOL_SIZE
}

/// A bounded pool of connections. The connections are created on demand by
/// the factory, up to `size` in total (idle plus checked out); a checkout
/// beyond the limit blocks until a connection returns. The pool is cheap to
/// clone: the clones share the same connections and accounting.
pub(crate) struct ConnectionPool<C> {
    /// Creates a new connection.
    factory: Arc<dyn Fn() -> redis::RedisResult<C> + Send + Sync + 'static>,
    /// The pool limit.
    size: usize,
    /// The state shared by the clones.
    shared: Arc<Shared<C>>,
}

impl<C> Clone for ConnectionPool<C> {
    fn clone(&self) -> Self {
        Self {
            factory: Arc::clone(&self.factory),
            size: self.size,
            shared: Arc::clone(&self.shared),
        }
    }
}

/// The state shared by the pool's clones.
struct Shared<C> {
    /// The idle connections and the checkout accounting.
    inner: Mutex<Inner<C>>,
    /// Wakes the waiters of a full pool on a checkin.
    ready: Condvar,
}

/// The pool's checkout accounting.
struct Inner<C> {
    /// Connections sitting idle, ready to be handed out.
    idle: Vec<C>,
    /// Connections currently checked out.
    checked_out: usize,
}

impl<C> ConnectionPool<C> {
    /// Creates the pool around a connection factory.
    pub(crate) fn new<F>(factory: F, size: usize) -> Self
    where
        F: Fn() -> redis::RedisResult<C> + Send + Sync + 'static,
    {
        assert!(size > 0, "the pool size must be positive");
        Self {
            factory: Arc::new(factory),
            size,
            shared: Arc::new(Shared {
                inner: Mutex::new(Inner { idle: Vec::with_capacity(size), checked_out: 0 }),
                ready: Condvar::new(),
            }),
        }
    }

    /// Checks a connection out of the pool; the returned guard puts it back
    /// on drop.
    pub(crate) fn checkout(&self) -> redis::RedisResult<PoolGuard<'_, C>> {
        let mut inner = lock(&self.shared.inner);
        loop {
            if let Some(conn) = inner.idle.pop() {
                inner.checked_out += 1;
                return Ok(PoolGuard { pool: self, conn: Some(conn) });
            }
            if inner.checked_out < self.size {
                inner.checked_out += 1;
                drop(inner);
                // Creating a connection spans the whole cluster and must not
                // hold the pool lock; the reservation above keeps the total
                // within the limit while it is being created.
                match (self.factory)() {
                    Ok(conn) => return Ok(PoolGuard { pool: self, conn: Some(conn) }),
                    Err(err) => {
                        // Return the reservation so a later checkout can try
                        // again within the limit.
                        let mut inner = lock(&self.shared.inner);
                        inner.checked_out -= 1;
                        drop(inner);
                        self.shared.ready.notify_one();
                        return Err(err);
                    }
                }
            }
            inner = self.shared.ready.wait(inner).unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// Returns a connection to the pool.
    fn checkin(&self, conn: C) {
        let mut inner = lock(&self.shared.inner);
        inner.checked_out -= 1;
        inner.idle.push(conn);
        drop(inner);
        self.shared.ready.notify_one();
    }
}

/// Poison-tolerant lock helper.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A connection checked out of a pool; returns to the pool on drop.
pub(crate) struct PoolGuard<'a, C> {
    pool: &'a ConnectionPool<C>,
    conn: Option<C>,
}

impl<C> Deref for PoolGuard<'_, C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().expect("a guard always holds its connection")
    }
}

impl<C> DerefMut for PoolGuard<'_, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().expect("a guard always holds its connection")
    }
}

impl<C> Drop for PoolGuard<'_, C> {
    fn drop(&mut self) {
        self.pool.checkin(self.conn.take().expect("a guard always holds its connection"));
    }
}

/// A synchronous Redis cluster sink bound to one symbol: changes are
/// published to a channel, snapshots are stored under a key.
pub struct RedisStore {
    /// The pooled cluster connections.
    pool: ConnectionPool<redis::cluster::ClusterConnection>,
    /// The change channel of the symbol.
    channel: String,
    /// The snapshot key of the symbol.
    snapshot_key: String,
}

impl RedisStore {
    /// Connects to the cluster and binds the store to the resources of one
    /// symbol. The connection is lazy: the pool creates cluster connections
    /// on the first operations.
    pub fn connect(
        config: &RedisConfig,
        channel: String,
        snapshot_key: String,
    ) -> Result<Self, StorageError> {
        let client = redis::cluster::ClusterClient::new(config.urls.iter().cloned())?;
        let pool_size = config.pool_size;
        let pool = ConnectionPool::new(move || client.get_connection(), pool_size);
        Ok(Self { pool, channel, snapshot_key })
    }

    /// Checks a cluster connection out of the pool.
    fn checkout(&self) -> Result<PoolGuard<'_, redis::cluster::ClusterConnection>, StorageError> {
        Ok(self.pool.checkout()?)
    }
}

impl ChangeSink for RedisStore {
    fn publish_change(&mut self, payload: &[u8]) -> Result<(), StorageError> {
        let mut conn = self.checkout()?;
        redis::cmd("PUBLISH").arg(&self.channel).arg(payload).query::<()>(&mut *conn)?;
        Ok(())
    }

    fn save_snapshot(&mut self, payload: &[u8]) -> Result<(), StorageError> {
        let mut conn = self.checkout()?;
        redis::cmd("SET").arg(&self.snapshot_key).arg(payload).query::<()>(&mut *conn)?;
        Ok(())
    }

    fn load_snapshot(&mut self) -> Result<Option<Vec<u8>>, StorageError> {
        let mut conn = self.checkout()?;
        let value =
            redis::cmd("GET").arg(&self.snapshot_key).query::<Option<Vec<u8>>>(&mut *conn)?;
        Ok(value)
    }
}

/// A synchronous Redis cluster store of the keyed states: the latest margin
/// state of each account lives under its own key, read on demand by the
/// [SVD_Pretrade] pulls and written by the [SVD_Sync]. The store is cheap to
/// clone: the clones share the connection pool.
#[derive(Clone)]
pub struct RedisKeyStore {
    /// The pooled cluster connections.
    pool: ConnectionPool<redis::cluster::ClusterConnection>,
}

impl RedisKeyStore {
    /// Connects to the cluster. The connection is lazy: the pool creates
    /// cluster connections on the first operations.
    pub fn connect(config: &RedisConfig) -> Result<Self, StorageError> {
        let client = redis::cluster::ClusterClient::new(config.urls.iter().cloned())?;
        let pool_size = config.pool_size;
        let pool = ConnectionPool::new(move || client.get_connection(), pool_size);
        Ok(Self { pool })
    }

    /// Reads the value stored under `key`, if any.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let mut conn = self.pool.checkout()?;
        Ok(redis::cmd("GET").arg(key).query::<Option<Vec<u8>>>(&mut *conn)?)
    }

    /// Stores `value` under `key`.
    pub fn set(&self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        let mut conn = self.pool.checkout()?;
        redis::cmd("SET").arg(key).arg(value).query::<()>(&mut *conn)?;
        Ok(())
    }
}

#[cfg(test)]
mod keyed_store_tests {
    use super::*;

    #[test]
    fn test_key_store_connect_is_lazy() {
        // The cluster connection is created on the first operation; opening
        // the store itself performs no I/O.
        let store = RedisKeyStore::connect(&RedisConfig::default()).expect("store opens");
        assert_eq!(store.pool.size, DEFAULT_POOL_SIZE);
    }

    #[test]
    fn test_key_store_clones_share_the_pool() {
        let store = RedisKeyStore::connect(&RedisConfig::default()).expect("store opens");
        let clone = store.clone();
        assert!(std::sync::Arc::ptr_eq(&store.pool.shared, &clone.pool.shared));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    /// A pool whose factory counts the connections it created.
    fn counting_pool(size: usize) -> (ConnectionPool<usize>, Arc<AtomicUsize>) {
        let created = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&created);
        let pool = ConnectionPool::new(
            move || {
                count.fetch_add(1, Ordering::Relaxed);
                Ok(1usize)
            },
            size,
        );
        (pool, created)
    }

    #[test]
    fn test_pool_size_must_be_positive() {
        let created = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&created);
        let factory = move || {
            count.fetch_add(1, Ordering::Relaxed);
            Ok(1usize)
        };
        let result = std::panic::catch_unwind(|| ConnectionPool::new(factory, 0));
        assert!(result.is_err());
    }

    #[test]
    fn test_checkin_returns_the_connection_to_the_pool() {
        let (pool, created) = counting_pool(2);
        drop(pool.checkout().unwrap());
        drop(pool.checkout().unwrap());
        // Both checkouts reused the single created connection.
        assert_eq!(created.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_pool_creates_up_to_the_size() {
        let (pool, created) = counting_pool(2);
        let first = pool.checkout().unwrap();
        let second = pool.checkout().unwrap();
        assert_eq!(created.load(Ordering::Relaxed), 2);
        drop(first);
        drop(second);
        // The returned connections are reused, nothing new is created.
        drop(pool.checkout().unwrap());
        assert_eq!(created.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_full_pool_blocks_until_a_checkin() {
        let (pool, _) = counting_pool(1);
        let held = pool.checkout().unwrap();
        let (tx, rx) = mpsc::channel::<()>();

        let waiter_pool = pool.clone();
        let waiter = std::thread::spawn(move || {
            let conn = waiter_pool.checkout().unwrap();
            tx.send(()).unwrap();
            drop(conn);
            tx.send(()).unwrap();
        });
        // The pool is exhausted: the waiter must block on the checkout.
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err());
        // Returning the held connection wakes the waiter.
        drop(held);
        rx.recv_timeout(Duration::from_secs(5)).expect("waiter checked out");
        rx.recv_timeout(Duration::from_secs(5)).expect("waiter returned the connection");
        waiter.join().unwrap();
    }

    #[test]
    fn test_failed_connection_returns_the_reservation() {
        let (pool, _) = {
            let pool: ConnectionPool<usize> = ConnectionPool::new(|| Err(redis_error()), 1);
            (pool, ())
        };
        assert!(pool.checkout().is_err());
        // The failed creation released its reservation: the next checkout
        // calls the factory again instead of blocking forever.
        assert!(pool.checkout().is_err());
    }

    /// A RedisError without a live server.
    fn redis_error() -> redis::RedisError {
        redis::RedisError::from((redis::ErrorKind::Io, "no connection", String::new()))
    }
}
