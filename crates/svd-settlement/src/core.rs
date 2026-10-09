//! The settlement core: the batch assembly of the drained trades and the
//! frame codec of the submitter queues.
//!
//! The core thread is pure data forwarding: it peeks the trade SPSC queue
//! wired from the [SVD_OMS_Master], groups the consecutive crosses of one
//! taker order into one batch, encodes each complete group into one frame
//! and pushes the frame to a submitter queue. The trade queue is acked only
//! after the frame is pushed — the unacked trades stay in the file-mapped
//! queue and survive a crash of this process (at-least-once: a crash
//! between the push and the ack duplicates the batch).
//!
//! The group invariant: the [SVD_OMS_Master] pushes the crosses of one
//! execution contiguously (its settlement writer never interleaves the
//! executions), so a run of consecutive trades with the same
//! `(taker.hot.user, taker.hot.nonce)` IS the complete set of that taker
//! order's crosses. The core closes a group when the taker changes; the open
//! group is the unacked prefix of the trade queue itself and flushes on the
//! shutdown.
//!
//! The hot path discipline: **no heap allocation at runtime**. The open
//! group is bounded by the trade queue capacity (it is a subset of the
//! unacked trades), so both buffers are pre-allocated once at startup to
//! that worst case — the drain window of `capacity` trades (the unacked
//! prefix plus the boundary trade) and the encode buffer of
//! [`frame_budget`]`(capacity)` bytes — checked out of the pre-allocated
//! object pools and reused every iteration. Nothing on the loop path grows,
//! clones or allocates.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cache::object_pool::Cache;
use ipc::mmap_spsc_fixed::SpscQueue;
use ipc::mmap_spsc_var::{ByteSpscQueue, FRAME_HEADER_SIZE};
use primitives::message::hot_path::Trade;
use primitives::order::Order;
use primitives::value::{Price, Quantity};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::seq::SeqFile;

/// Number of empty spins before the core thread yields the CPU.
const SPINS_PER_YIELD: u32 = 4096;

/// The payload of one submitter-queue frame: one taker order's complete
/// group of crosses, assigned a sequence by the core thread. A tuple struct
/// so the MessagePack wire shape is a two-element array, which the
/// allocation-free hot-path encoder writes from borrowed trades.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchFrame(
    /// The batch sequence — unique across restarts (the persistent
    /// sequence file), carried by the published `SettlementResult`.
    pub u64,
    /// The crosses of one taker order, in submission order.
    pub Vec<Trade>,
);

/// Clears `out` and MessagePack-encodes one frame of `seq` and `trades`
/// into it: a two-element array, the shape [`decode_frame`] reads. The
/// trades are borrowed — nothing is cloned, and the only write is the
/// append into `out`, so the caller pre-sizes `out` to [`frame_budget`] of
/// the worst-case group and the encoder never allocates.
pub fn encode_frame(seq: u64, trades: &[Trade], out: &mut Vec<u8>) -> Result<(), String> {
    out.clear();
    rmp_serde::encode::write(out, &(seq, trades)).map_err(|err| err.to_string())
}

/// Decodes one frame payload (the submitter's path — allocation is fine
/// off the hot path).
pub fn decode_frame(bytes: &[u8]) -> Result<BatchFrame, String> {
    rmp_serde::from_slice(bytes).map_err(|err| err.to_string())
}

/// The conservative byte budget of one frame of `trades` trades: the fixed
/// overhead (the array headers and the sequence) plus twice the in-memory
/// size of the trades. Used to pre-size the core's encode buffer and the
/// startup capacity check of the submitter queues.
pub fn frame_budget(trades: usize) -> usize {
    1 + 9 + 32 + 2 * size_of::<Trade>() * trades
}

/// The core loop: groups the trade queue by taker order, encodes each
/// complete group into a frame, routes the frames to the submitter queues
/// and acks the trade queue only after a frame is pushed.
///
/// The drain window and the encode buffer are checked out of the
/// pre-allocated object pools sized by `trade_capacity` — the hard bound of
/// one group (a group is a subset of the unacked trades, which the queue
/// holds at most `capacity - 1` of) — and reused every iteration: the loop
/// performs no allocation and no blocking at runtime. The exit flushes the
/// open group; a frame no submitter queue can hold stops the loop with the
/// trades unacked (they stay in the trade queue for the next start).
pub fn spin(
    mut queue: SpscQueue<Trade>,
    mut out: Vec<ByteSpscQueue>,
    seq: Arc<SeqFile>,
    trade_capacity: usize,
    submitters_alive: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
) {
    // The drain window, sized for the whole unacked prefix (the open group
    // plus the boundary trade): a peek copies at most `capacity - 1`
    // trades, so the window never grows.
    let drain_pool = Cache::new(1, move || {
        let mut window = Vec::with_capacity(trade_capacity);
        window.resize_with(trade_capacity, dummy_trade);
        window
    });
    let mut drain = drain_pool.acquire();
    // The encode buffer, sized for the frame of a whole-queue group.
    let encode_pool = Cache::new(1, move || Vec::with_capacity(frame_budget(trade_capacity)));
    let mut encode_buf = encode_pool.acquire();
    // The round-robin cursor of the submitter queues.
    let mut rr = 0usize;
    let mut empty_spins = 0u32;
    info!("the settlement core loop started");
    loop {
        let n = queue.peek_batch(&mut drain);
        if n == 0 {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            std::hint::spin_loop();
            empty_spins += 1;
            if empty_spins.is_multiple_of(SPINS_PER_YIELD) {
                std::thread::yield_now();
            }
            continue;
        }
        // The boundary scan: the first trade whose taker order differs from
        // the head's closes the group before it.
        let head_key = (drain[0].taker.hot.user, drain[0].taker.hot.nonce);
        let group_end = (1..n)
            .find(|&index| (drain[index].taker.hot.user, drain[index].taker.hot.nonce) != head_key)
            .unwrap_or(n);
        if group_end < n {
            // The taker changed: drain[..group_end] is the complete group.
            if !push_group(
                &mut queue,
                &mut out,
                &seq,
                &drain[..group_end],
                &mut encode_buf,
                &mut rr,
                &submitters_alive,
                &shutdown,
            ) {
                error!("cannot deliver a batch frame, stopping the core loop");
                return;
            }
            continue;
        }
        // The whole peek is one open group: wait for the boundary trade of
        // the next execution, or flush it on the shutdown.
        if shutdown.load(Ordering::Relaxed) {
            if !push_group(
                &mut queue,
                &mut out,
                &seq,
                &drain[..n],
                &mut encode_buf,
                &mut rr,
                &submitters_alive,
                &shutdown,
            ) {
                error!(
                    "cannot deliver the final group, the trades stay unacked for the next start"
                );
            }
            break;
        }
        std::hint::spin_loop();
        empty_spins += 1;
        if empty_spins.is_multiple_of(SPINS_PER_YIELD) {
            std::thread::yield_now();
        }
    }
    info!("the settlement core loop stopped");
}

/// Closes a group: encodes the frame, pushes it to a submitter queue and
/// acks the trades on the trade queue. Returns false when the frame cannot
/// be delivered — the trades stay unacked in the trade queue.
#[allow(clippy::too_many_arguments)]
fn push_group(
    queue: &mut SpscQueue<Trade>,
    out: &mut [ByteSpscQueue],
    seq: &SeqFile,
    group: &[Trade],
    encode_buf: &mut Vec<u8>,
    rr: &mut usize,
    submitters_alive: &AtomicUsize,
    shutdown: &AtomicBool,
) -> bool {
    if let Err(err) = encode_frame(seq.next(), group, encode_buf) {
        error!(error = %err, "cannot encode the batch frame, stopping the core loop");
        return false;
    }
    let need = FRAME_HEADER_SIZE + encode_buf.len();
    if !out.iter().any(|q| q.capacity() > need) {
        // The startup check sizes the queues for the expected batch bound;
        // a larger group that no queue can hold is a configuration error.
        error!("the frame of {need} bytes exceeds every submitter queue, stopping the core loop");
        return false;
    }
    let Some(index) = wait_for_space(out, need, *rr, submitters_alive, shutdown) else {
        return false;
    };
    *rr = (index + 1) % out.len();
    if !out[index].push(encode_buf) {
        error!("the submitter queue rejected the frame, stopping the core loop");
        return false;
    }
    // The durability boundary: the trades are released only after the
    // frame reached the submitter queue.
    queue.ack(group.len());
    true
}

/// Finds a submitter queue with room for a frame of `need` bytes, probing
/// in round-robin order from `rr`. Returns None when the delivery cannot
/// happen: every submitter is gone, or the shutdown arrived with all the
/// queues full (the submitters stop draining — the trades stay unacked).
fn wait_for_space(
    out: &[ByteSpscQueue],
    need: usize,
    rr: usize,
    submitters_alive: &AtomicUsize,
    shutdown: &AtomicBool,
) -> Option<usize> {
    let mut spins = 0u32;
    loop {
        for offset in 0..out.len() {
            let index = (rr + offset) % out.len();
            if out[index].available() >= need {
                return Some(index);
            }
        }
        if submitters_alive.load(Ordering::Acquire) == 0 {
            error!("every submitter is gone, stopping the core loop");
            return None;
        }
        if shutdown.load(Ordering::Relaxed) {
            warn!(
                "the shutdown arrived with every submitter queue full, leaving the group unacked"
            );
            return None;
        }
        std::hint::spin_loop();
        spins += 1;
        if spins.is_multiple_of(SPINS_PER_YIELD) {
            std::thread::yield_now();
        }
    }
}

/// The filler of the drain buffer: `Trade` has no `Default`.
fn dummy_trade() -> Trade {
    Trade::new(Order::default(), Quantity::ZERO, Order::default(), Price::ZERO, Quantity::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;

    /// Monotonic counter so tests running in parallel never collide on
    /// temp paths.
    static SEQ: AtomicUsize = AtomicUsize::new(0);

    struct TempFile(PathBuf);

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn temp_path(tag: &str) -> PathBuf {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dex_stl_core_{}_{}_{}", std::process::id(), tag, seq))
    }

    fn trade(user: u8, nonce: u64) -> Trade {
        use primitives::address::Address;
        use primitives::base::{Hash32, Nonce, Side, Symbol};
        use primitives::order::{OrderCold, OrderColdCommon, OrderHot, OrderKind};
        use primitives::signature::Signature;
        use primitives::time_in_force::TimeInForce;
        use primitives::value::TimestampMs;
        Trade::new(
            Order::new(
                OrderHot {
                    user: Address([user; 20]),
                    nonce: Nonce(nonce),
                    price: Price(100),
                    quantity: Quantity(10),
                    time_in_force: TimeInForce::Gtc,
                    side: Side::Buy,
                },
                OrderCold::new(
                    OrderColdCommon::new(
                        Hash32([0; 32]),
                        Symbol([0; 32]),
                        Signature::default(),
                        TimestampMs(0),
                    ),
                    OrderKind::Standard,
                ),
            ),
            Quantity(9),
            Order::default(),
            Price(100),
            Quantity(1),
        )
    }

    /// A trade stream: runs of `(user, count)` crosses of one taker order
    /// per run — the crosses of one execution share the exact taker order,
    /// so the nonce stays 1 within a run.
    fn stream(groups: &[(u8, usize)]) -> Vec<Trade> {
        let mut trades = Vec::new();
        for (user, count) in groups {
            for _ in 0..*count {
                trades.push(trade(*user, 1));
            }
        }
        trades
    }

    /// Opens the temp trade queue pre-filled with `trades` and a sequence
    /// file, returning the paths (the queue is dropped).
    fn prepare_queues(tag: &str, trades: &[Trade], capacity: usize) -> (PathBuf, PathBuf) {
        let queue_path = temp_path(tag);
        let seq_path = temp_path(tag);
        {
            let mut queue = SpscQueue::<Trade>::open(&queue_path, capacity, true).unwrap();
            queue.push_batch(trades);
        }
        (queue_path, seq_path)
    }

    /// Pops every frame of the queue at `path` (opened read-only), decoding
    /// the frames in order.
    fn drain_frames(path: &PathBuf, capacity: usize) -> Vec<BatchFrame> {
        let mut queue = ByteSpscQueue::open(path, capacity, false).unwrap();
        let mut buf = Vec::new();
        let mut frames = Vec::new();
        while let Ok(Some(len)) = queue.peek(&mut buf) {
            frames.push(decode_frame(&buf[..len]).unwrap());
            queue.ack(FRAME_HEADER_SIZE + len).unwrap();
        }
        frames
    }

    #[test]
    fn test_encode_decode_frame_roundtrip() {
        let frame = BatchFrame(7, stream(&[(1, 2), (2, 1)]));
        let mut bytes = Vec::new();
        encode_frame(frame.0, &frame.1, &mut bytes).unwrap();
        assert_eq!(decode_frame(&bytes).unwrap(), frame);
    }

    #[test]
    fn test_decode_frame_rejects_garbage() {
        assert!(decode_frame(&[0x00, 0xff, 0xfe]).is_err());
        assert!(decode_frame(&[]).is_err());
    }

    #[test]
    fn test_frame_budget_covers_the_actual_encode_size() {
        for count in [1usize, 2, 16, 1024] {
            let trades = stream(&[(1, count)]);
            let mut bytes = Vec::new();
            encode_frame(u64::MAX, &trades, &mut bytes).unwrap();
            assert!(
                frame_budget(count) >= bytes.len(),
                "{count} trades: budget {} < actual {}",
                frame_budget(count),
                bytes.len()
            );
        }
    }

    #[test]
    fn test_spin_groups_by_taker_and_routes_round_robin() {
        let (queue_path, seq_path) =
            prepare_queues("spin_group", &stream(&[(1, 2), (2, 3), (3, 1)]), 64);
        let _guards = (TempFile(queue_path.clone()), TempFile(seq_path.clone()));
        let submit_paths: Vec<PathBuf> = (0..2).map(|i| temp_path(&format!("sub{i}"))).collect();
        let _submit_guards: Vec<TempFile> = submit_paths.iter().cloned().map(TempFile).collect();

        let out: Vec<ByteSpscQueue> = submit_paths
            .iter()
            .map(|path| ByteSpscQueue::open(path, 64 * 1024, true).unwrap())
            .collect();
        let seq = Arc::new(SeqFile::open(&seq_path).unwrap());
        let submitters_alive = Arc::new(AtomicUsize::new(1));
        // The shutdown set upfront: the core drains and routes the first two
        // groups (the taker change closes them), then the never-closed third
        // group flushes on the shutdown.
        spin(
            SpscQueue::open(&queue_path, 64, false).unwrap(),
            out,
            Arc::clone(&seq),
            64,
            submitters_alive,
            Arc::new(AtomicBool::new(true)),
        );

        // The trade queue is fully acked.
        let mut peeked = [dummy_trade(); 8];
        let trade_queue = SpscQueue::<Trade>::open(&queue_path, 64, false).unwrap();
        assert_eq!(trade_queue.peek_batch(&mut peeked), 0, "all trades are acked");

        // The frames alternate across the two submitter queues, one per
        // taker group; the flushed final group follows the round-robin.
        let frames0 = drain_frames(&submit_paths[0], 64 * 1024);
        let frames1 = drain_frames(&submit_paths[1], 64 * 1024);
        assert_eq!(frames0.len(), 2, "groups 1 and 3 round-robin onto queue 0");
        assert_eq!(frames1.len(), 1, "group 2 lands on queue 1");
        assert_eq!(frames0[0].1.len(), 2);
        assert_eq!(frames1[0].1.len(), 3);
        assert_eq!(frames0[1].1.len(), 1);
        assert_eq!(frames0[0].0, 0);
        assert_eq!(frames1[0].0, 1);
        assert_eq!(frames0[1].0, 2);
    }

    #[test]
    fn test_spin_keeps_a_group_open_across_peeks() {
        // A group of 5 trades: the first peek sees the whole group plus the
        // boundary trade of the next taker and closes it as one batch.
        let (queue_path, seq_path) = prepare_queues("spin_window", &stream(&[(1, 5), (2, 1)]), 64);
        let _guards = (TempFile(queue_path.clone()), TempFile(seq_path.clone()));
        let submit_path = temp_path("sub_window");
        let _submit_guard = TempFile(submit_path.clone());

        let out = vec![ByteSpscQueue::open(&submit_path, 64 * 1024, true).unwrap()];
        let seq = Arc::new(SeqFile::open(&seq_path).unwrap());
        spin(
            SpscQueue::open(&queue_path, 64, false).unwrap(),
            out,
            Arc::clone(&seq),
            64,
            Arc::new(AtomicUsize::new(1)),
            Arc::new(AtomicBool::new(true)),
        );

        let frames = drain_frames(&submit_path, 64 * 1024);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].1.len(), 5, "the whole taker group stays in one batch");
        assert_eq!(frames[1].1.len(), 1);
    }

    #[test]
    fn test_spin_holds_the_group_filling_the_whole_queue_until_the_shutdown() {
        // The queue holds `capacity - 1` trades of one taker: no boundary
        // trade can enter, so the group waits and the shutdown flushes it
        // as one whole-queue batch (the worst case of the drain window).
        let trades = stream(&[(1, 7)]);
        let (queue_path, seq_path) = prepare_queues("spin_full", &trades, 8);
        let _guards = (TempFile(queue_path.clone()), TempFile(seq_path.clone()));
        let submit_path = temp_path("sub_full");
        let _submit_guard = TempFile(submit_path.clone());

        let out = vec![ByteSpscQueue::open(&submit_path, 64 * 1024, true).unwrap()];
        let seq = Arc::new(SeqFile::open(&seq_path).unwrap());
        spin(
            SpscQueue::open(&queue_path, 8, false).unwrap(),
            out,
            Arc::clone(&seq),
            8,
            Arc::new(AtomicUsize::new(1)),
            Arc::new(AtomicBool::new(true)),
        );

        let frames = drain_frames(&submit_path, 64 * 1024);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].1.len(), 7, "the whole-queue group flushes as one batch");
    }

    #[test]
    fn test_spin_flushes_the_open_group_on_shutdown() {
        let (queue_path, seq_path) = prepare_queues("spin_flush", &stream(&[(1, 3)]), 64);
        let _guards = (TempFile(queue_path.clone()), TempFile(seq_path.clone()));
        let submit_path = temp_path("sub_flush");
        let _submit_guard = TempFile(submit_path.clone());

        let out = vec![ByteSpscQueue::open(&submit_path, 64 * 1024, true).unwrap()];
        let seq = Arc::new(SeqFile::open(&seq_path).unwrap());
        spin(
            SpscQueue::open(&queue_path, 64, false).unwrap(),
            out,
            Arc::clone(&seq),
            64,
            Arc::new(AtomicUsize::new(1)),
            Arc::new(AtomicBool::new(true)), // shutdown before any peek
        );

        // The never-closed group still becomes a frame and the trades are
        // acked.
        let frames = drain_frames(&submit_path, 64 * 1024);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].1.len(), 3);
        let mut peeked = [dummy_trade(); 8];
        let trade_queue = SpscQueue::<Trade>::open(&queue_path, 64, false).unwrap();
        assert_eq!(trade_queue.peek_batch(&mut peeked), 0);
    }

    #[test]
    fn test_spin_leaves_trades_unacked_when_no_queue_can_take_the_frame() {
        // A submitter queue that holds exactly one small frame: the second
        // group does not fit and the shutdown arrives — the core exits with
        // the trades unacked (they survive in the trade queue).
        let (queue_path, seq_path) = prepare_queues("spin_unacked", &stream(&[(1, 1), (2, 1)]), 64);
        let _guards = (TempFile(queue_path.clone()), TempFile(seq_path.clone()));
        let submit_path = temp_path("sub_unacked");
        let _submit_guard = TempFile(submit_path.clone());

        // The queue capacity: one frame of one trade plus nothing.
        let probe = stream(&[(1, 1)]);
        let mut bytes = Vec::new();
        encode_frame(0, &probe, &mut bytes).unwrap();
        let capacity = bytes.len() + FRAME_HEADER_SIZE + 1;

        let out = vec![ByteSpscQueue::open(&submit_path, capacity, true).unwrap()];
        let seq = Arc::new(SeqFile::open(&seq_path).unwrap());
        spin(
            SpscQueue::open(&queue_path, 64, false).unwrap(),
            out,
            Arc::clone(&seq),
            64,
            Arc::new(AtomicUsize::new(1)),
            Arc::new(AtomicBool::new(true)),
        );

        // The first group delivered, the second stayed unacked.
        let frames = drain_frames(&submit_path, capacity);
        assert_eq!(frames.len(), 1);
        let mut peeked = [dummy_trade(); 8];
        let trade_queue = SpscQueue::<Trade>::open(&queue_path, 64, false).unwrap();
        assert_eq!(trade_queue.peek_batch(&mut peeked), 1, "one trade stays for the next start");
    }
}
