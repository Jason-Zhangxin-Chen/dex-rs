//! Integration tests of the storage helpers against a real Redis server.
//!
//! Ignored by default (CI has no Redis). Run with a local server:
//!
//! ```text
//! redis-server --port 6379 &
//! cargo test -p storage --test redis_integration -- --ignored --nocapture
//! ```

use storage::{ChangeSink, RedisConfig, RedisStore};

/// The test requires a Redis server on the default local port.
const URL: &str = "redis://127.0.0.1:6379/";

fn store(tag: &str) -> RedisStore {
    let config = RedisConfig { url: URL.to_string() };
    RedisStore::connect(
        &config,
        format!("svd.test.{tag}.changes"),
        format!("svd.test.{tag}.snapshot"),
    )
    .expect("redis is up")
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
