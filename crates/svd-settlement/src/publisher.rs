//! The result publisher: serializes the `SettlementResult` messages and
//! publishes them to the Redis cluster with an at-least-once retry.
//!
//! The publisher runs on its own thread, never on the hot path. The retry
//! loop never gives up on a Redis outage: the results queue up in the
//! channel while the publication is suspended, and the shutdown flag is
//! checked before every attempt, so a shutdown in the middle of a retry
//! returns at once — the journal replay re-publishes the unwritten results
//! on the next start. A result that cannot be encoded is logged and skipped
//! for the same reason: it stays in the journal and re-publishes later.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::time::Duration;

use primitives::message::settlement::SettlementResult;
use storage::ChangeSink;
use tracing::{error, info, warn};

use crate::batch::backoff_for;

/// The base of the publish retry backoff, in milliseconds.
const RETRY_BASE_MS: u64 = 100;
/// The cap of the publish retry backoff, in milliseconds.
const RETRY_MAX_MS: u64 = 10_000;
/// The name of the publisher thread.
const THREAD_NAME: &str = "stl-publisher";

/// Spawns the result publisher thread: it drains the results from `rx`,
/// serializes them and publishes the payloads to the [`ChangeSink`] with an
/// at-least-once retry.
///
/// The thread stops when the engine drops the sender end of `rx`, or when
/// `shutdown` is set — a shutdown is honoured before every publish attempt,
/// including the attempts of a retry in flight.
pub fn spawn_publisher(
    rx: Receiver<SettlementResult>,
    mut sink: Box<dyn ChangeSink + Send>,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new().name(THREAD_NAME.to_string()).spawn(move || {
        info!("the result publisher started");
        'publish: while let Ok(result) = rx.recv() {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            let bytes = match rmp_serde::to_vec(&result) {
                Ok(bytes) => bytes,
                Err(err) => {
                    error!(error = %err, batch_seq = result.batch_seq, "cannot encode the settlement result");
                    continue;
                }
            };
            let mut attempt = 0u32;
            loop {
                // The check runs before every attempt, so a shutdown during
                // the backoff sleep stops the thread on the next iteration.
                if shutdown.load(Ordering::Relaxed) {
                    break 'publish;
                }
                match sink.publish_change(&bytes) {
                    Ok(()) => break,
                    Err(err) => {
                        warn!(error = %err, batch_seq = result.batch_seq, "cannot publish, retrying");
                        let backoff = backoff_for(attempt, RETRY_BASE_MS, RETRY_MAX_MS);
                        std::thread::sleep(Duration::from_millis(backoff));
                        attempt = attempt.saturating_add(1);
                    }
                }
            }
        }
        info!("the result publisher stopped");
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::mpsc::channel;
    use std::time::{Duration, Instant};

    use primitives::base::{Hash32, Symbol};
    use primitives::message::settlement::SettlementOutcome;
    use storage::StorageError;

    use super::*;

    /// The state of a [`RecordingSink`], shared with the test.
    #[derive(Debug, Default)]
    struct SinkState {
        /// The number of the publish attempts seen.
        attempts: usize,
        /// The payloads accepted, in order.
        published: Vec<Vec<u8>>,
    }

    /// A [`ChangeSink`] that rejects the first `fail_attempts` publications
    /// and then records the accepted payloads. The snapshot methods are
    /// unsupported: the publisher never calls them.
    struct RecordingSink {
        /// The number of the leading publications to fail.
        fail_attempts: usize,
        /// The shared state, observable by the test after the thread ends.
        state: Arc<Mutex<SinkState>>,
    }

    impl RecordingSink {
        fn new(fail_attempts: usize) -> (Self, Arc<Mutex<SinkState>>) {
            let state = Arc::new(Mutex::new(SinkState::default()));
            (Self { fail_attempts, state: Arc::clone(&state) }, state)
        }
    }

    impl ChangeSink for RecordingSink {
        fn publish_change(&mut self, payload: &[u8]) -> Result<(), StorageError> {
            let mut state = self.state.lock().expect("the recording sink mutex");
            state.attempts += 1;
            if state.attempts <= self.fail_attempts {
                return Err(StorageError::Unsupported("injected publish failure"));
            }
            state.published.push(payload.to_vec());
            Ok(())
        }

        fn save_snapshot(&mut self, _payload: &[u8]) -> Result<(), StorageError> {
            Err(StorageError::Unsupported("save_snapshot"))
        }

        fn load_snapshot(&mut self) -> Result<Option<Vec<u8>>, StorageError> {
            Err(StorageError::Unsupported("load_snapshot"))
        }
    }

    /// A settlement result of the given batch: the settled shape with one
    /// trade is enough for the publication tests.
    fn result(batch_seq: u64) -> SettlementResult {
        SettlementResult {
            batch_seq,
            symbol: Symbol([7; 32]),
            outcome: SettlementOutcome::Settled,
            tx_hash: Some(Hash32([9; 32])),
            block: Some(100 + batch_seq),
            trades: Vec::new(),
        }
    }

    /// Joins the thread once it finishes, panicking when it outlives the
    /// timeout instead of hanging the test run.
    fn join_within(handle: std::thread::JoinHandle<()>, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while !handle.is_finished() {
            assert!(Instant::now() < deadline, "the publisher thread did not stop");
            std::thread::sleep(Duration::from_millis(2));
        }
        handle.join().expect("the publisher thread does not panic");
    }

    /// Waits until `predicate` holds, panicking on the timeout.
    fn wait_until(mut predicate: impl FnMut() -> bool, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while !predicate() {
            assert!(Instant::now() < deadline, "the condition was not reached in time");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn test_publisher_retries_then_publishes_once() {
        let (sink, state) = RecordingSink::new(3);
        let (tx, rx) = channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle =
            spawn_publisher(rx, Box::new(sink), Arc::clone(&shutdown)).expect("the thread spawns");

        let input = result(11);
        tx.send(input.clone()).expect("the publisher is alive");
        drop(tx);
        join_within(handle, Duration::from_secs(10));

        let state = state.lock().expect("the recording sink mutex");
        assert_eq!(state.attempts, 4, "three failures then one accepted attempt");
        assert_eq!(state.published.len(), 1, "exactly one publication after the failures");
        let published: SettlementResult =
            rmp_serde::from_slice(&state.published[0]).expect("the payload decodes");
        assert_eq!(published, input);
    }

    #[test]
    fn test_publisher_publishes_every_result_in_order() {
        let (sink, state) = RecordingSink::new(0);
        let (tx, rx) = channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle =
            spawn_publisher(rx, Box::new(sink), Arc::clone(&shutdown)).expect("the thread spawns");

        tx.send(result(1)).expect("the publisher is alive");
        tx.send(result(2)).expect("the publisher is alive");
        drop(tx);
        join_within(handle, Duration::from_secs(10));

        let state = state.lock().expect("the recording sink mutex");
        let batch_seqs: Vec<u64> = state
            .published
            .iter()
            .map(|bytes| rmp_serde::from_slice::<SettlementResult>(bytes).unwrap().batch_seq)
            .collect();
        assert_eq!(batch_seqs, vec![1, 2]);
    }

    #[test]
    fn test_publisher_shutdown_during_retry_exits_without_publishing() {
        // The sink never accepts: the thread sits in the retry loop until the
        // shutdown flag is raised, and the result is left to the journal
        // replay of the next start.
        let (sink, state) = RecordingSink::new(usize::MAX);
        let (tx, rx) = channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle =
            spawn_publisher(rx, Box::new(sink), Arc::clone(&shutdown)).expect("the thread spawns");

        tx.send(result(3)).expect("the publisher is alive");
        wait_until(|| state.lock().expect("the sink mutex").attempts >= 1, Duration::from_secs(5));
        shutdown.store(true, Ordering::Relaxed);
        join_within(handle, Duration::from_secs(5));

        assert!(state.lock().expect("the sink mutex").published.is_empty());
    }

    #[test]
    fn test_publisher_exits_when_the_channel_drops() {
        let (sink, state) = RecordingSink::new(0);
        let (tx, rx) = channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle =
            spawn_publisher(rx, Box::new(sink), Arc::clone(&shutdown)).expect("the thread spawns");

        drop(tx);
        join_within(handle, Duration::from_secs(5));

        assert_eq!(state.lock().expect("the sink mutex").attempts, 0);
    }
}
