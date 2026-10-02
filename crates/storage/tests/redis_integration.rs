//! Integration tests of the storage helpers against a real Redis cluster.
//!
//! Ignored by default (CI has no Redis). Run with a local single-node
//! cluster (a fresh single-node cluster owns no slots until they are
//! assigned, otherwise it stays in `fail` state):
//!
//! ```text
//! docker run -d --rm --name dex-redis-test -p 6379:6379 redis:7-alpine \
//!     redis-server --cluster-enabled yes --cluster-config-file /tmp/nodes.conf
//! docker exec dex-redis-test redis-cli -p 6379 cluster addslots $(seq 0 16383)
//! cargo test -p storage --test redis_integration -- --ignored --nocapture
//! ```

use storage::{ChangeSink, RedisConfig, RedisStore};

/// The test requires a Redis server on the default local port.
const URL: &str = "redis://127.0.0.1:6379/";

fn store(tag: &str) -> RedisStore {
    let config = RedisConfig { urls: vec![URL.to_string()], pool_size: 2 };
    RedisStore::connect(
        &config,
        format!("svd.test.{tag}.changes"),
        format!("svd.test.{tag}.snapshot"),
    )
    .expect("redis cluster is up")
}

#[test]
#[ignore = "requires a local redis-server on 127.0.0.1:6379"]
fn test_change_publication_and_snapshot_roundtrip() {
    let tag = format!("storage_{}", std::process::id());
    let mut store = store(&tag);

    // The snapshot key starts empty.
    assert_eq!(store.load_snapshot().unwrap(), None);

    // A change publication is accepted.
    store.publish_change(b"hello-change").unwrap();

    // A snapshot roundtrips.
    store.save_snapshot(b"hello-snapshot").unwrap();
    assert_eq!(store.load_snapshot().unwrap(), Some(b"hello-snapshot".to_vec()));
}

#[test]
#[ignore = "requires a local redis-server on 127.0.0.1:6379"]
fn test_pooled_connections_survive_many_operations() {
    let tag = format!("storage_pool_{}", std::process::id());
    // A pool of 2 connections serving 32 operations: every operation checks
    // a connection out and returns it, exercising the reuse path.
    let mut store = store(&tag);
    for round in 0..32u64 {
        store.save_snapshot(&round.to_le_bytes()).unwrap();
        assert_eq!(store.load_snapshot().unwrap(), Some(round.to_le_bytes().to_vec()));
    }
}
