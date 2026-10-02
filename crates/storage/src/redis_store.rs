//! Redis cluster helpers: a synchronous client bound to one symbol's
//! resources (change channel and snapshot key). The services use it on their
//! side-path threads only, never on the hot path.

use crate::{ChangeSink, StorageError};
use serde::{Deserialize, Serialize};

/// Connection config of the Redis cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedisConfig {
    /// Connection URL of the cluster, e.g. `redis://127.0.0.1:6379`.
    pub url: String,
}

/// A synchronous Redis sink bound to one symbol: changes are published to a
/// channel, snapshots are stored under a key.
pub struct RedisStore {
    conn: redis::Connection,
    channel: String,
    snapshot_key: String,
}

impl RedisStore {
    /// Connects to the cluster and binds the store to the resources of one
    /// symbol.
    pub fn connect(
        config: &RedisConfig,
        channel: String,
        snapshot_key: String,
    ) -> Result<Self, StorageError> {
        let client = redis::Client::open(config.url.as_str())?;
        let conn = client.get_connection()?;
        Ok(Self { conn, channel, snapshot_key })
    }
}

impl ChangeSink for RedisStore {
    fn publish_change(&mut self, payload: &[u8]) -> Result<(), StorageError> {
        redis::cmd("PUBLISH").arg(&self.channel).arg(payload).query::<()>(&mut self.conn)?;
        Ok(())
    }

    fn save_snapshot(&mut self, payload: &[u8]) -> Result<(), StorageError> {
        redis::cmd("SET").arg(&self.snapshot_key).arg(payload).query::<()>(&mut self.conn)?;
        Ok(())
    }

    fn load_snapshot(&mut self) -> Result<Option<Vec<u8>>, StorageError> {
        let value =
            redis::cmd("GET").arg(&self.snapshot_key).query::<Option<Vec<u8>>>(&mut self.conn)?;
        Ok(value)
    }
}
