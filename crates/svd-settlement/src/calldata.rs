//! The ABI encoding of the `settleBatch` calldata, hand-rolled and
//! dependency-free so the submitter builds real calldata bytes in every
//! feature configuration. The alloy bindings of `chain::abi` cross-check
//! this encoder byte-for-byte behind the `chain-alloy` feature.
//!
//! The layout follows doc/settlement-protocol.md: `Order` is a fully
//! static tuple of nine words (`bytes32 symbol`, `address user`, six
//! `uint64`s, `uint8 side` — with `timeInForce` as the packed `uint64`
//! tag of the EIP-712 hash), and `MatchedTrade` wraps two orders, the two
//! raw 65-byte signatures, the executed price, the traded quantity and the
//! taker's remaining quantity.

use primitives::message::hot_path::Trade;
use primitives::order::Order;
use primitives::signature::Signature;
use sha3::{Digest, Keccak256};

/// The canonical tuple type string of the `settleBatch` function — the
/// input of the selector hash. Solidity expands the nested `Order` structs
/// inline, so the string carries the two nested 9-field tuples and 23
/// fields per `MatchedTrade` in total.
const SETTLE_BATCH_TYPE: &str = "settleBatch(((bytes32,address,uint64,uint64,uint64,uint64,uint8,uint64,uint64),bytes,(bytes32,address,uint64,uint64,uint64,uint64,uint8,uint64,uint64),bytes,uint64,uint64,uint64)[])";

/// The number of words of the static head of one `MatchedTrade`: the two
/// orders (9 words each), the two signature offsets, and the three scalar
/// fields.
const HEAD_WORDS: usize = 23;
/// The word offsets of the dynamic fields inside the `MatchedTrade` head.
const TAKER_SIG_OFFSET_WORD: usize = 9;
const MAKER_SIG_OFFSET_WORD: usize = 19;
/// The tail words of one element: each signature occupies one length word
/// plus its 65 bytes padded to three words.
const SIG_TAIL_WORDS: usize = 4;
/// The tail words of one element: the two signatures.
const TAIL_WORDS: usize = SIG_TAIL_WORDS * 2;

/// Errors of the calldata encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalldataError {
    /// A batch without trades cannot be encoded.
    EmptyBatch,
}

impl std::fmt::Display for CalldataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CalldataError::EmptyBatch => write!(f, "a settlement batch must carry trades"),
        }
    }
}

impl std::error::Error for CalldataError {}

/// The 4-byte selector of `settleBatch(MatchedTrade[])`, computed once.
pub fn settle_batch_selector() -> [u8; 4] {
    static SELECTOR: std::sync::OnceLock<[u8; 4]> = std::sync::OnceLock::new();
    *SELECTOR.get_or_init(|| {
        let mut hasher = Keccak256::new();
        hasher.update(SETTLE_BATCH_TYPE.as_bytes());
        let digest: [u8; 32] = hasher.finalize().into();
        [digest[0], digest[1], digest[2], digest[3]]
    })
}

/// ABI-encodes one `settleBatch` call over the given trades.
///
/// Layout (canonical for a dynamic array of a tuple with dynamic members):
/// the 4-byte selector, the argument offset (`0x20`), the array length,
/// the per-element head offsets (relative to the start of the offsets
/// array), then each element as its head followed by its signature tail —
/// the elements concatenate, so an element stride is `HEAD_WORDS +
/// TAIL_WORDS` words.
pub fn encode_settle_batch(trades: &[Trade]) -> Result<Vec<u8>, CalldataError> {
    if trades.is_empty() {
        return Err(CalldataError::EmptyBatch);
    }
    let n = trades.len();
    // 4 selector bytes + arg offset + length + element offsets + elements.
    let words = 1 + 1 + n + n * (HEAD_WORDS + TAIL_WORDS);
    let mut calldata = vec![0u8; 4 + words * 32];
    calldata[0..4].copy_from_slice(&settle_batch_selector());
    put_word(&mut calldata, 0, 0x20);

    // The array data begins at word 1 (the argument offset points at the
    // length word). The element offsets are relative to the start of the
    // offsets array (word 2); each element's head follows, and its tail
    // follows its head.
    let array_start = 1;
    put_word(&mut calldata, array_start, n as u128);
    let elements_start = array_start + 1 + n;
    for (i, trade) in trades.iter().enumerate() {
        let base = elements_start + i * (HEAD_WORDS + TAIL_WORDS);
        put_word(&mut calldata, array_start + 1 + i, ((base - (array_start + 1)) * 32) as u128);
        encode_trade(&mut calldata, base, trade);
    }
    Ok(calldata)
}

/// Encodes one `MatchedTrade` element: the 23-word head followed by the
/// signature tail. The in-head offsets are relative to the head.
fn encode_trade(calldata: &mut [u8], head: usize, trade: &Trade) {
    let tail = head + HEAD_WORDS;
    encode_order(calldata, head, &trade.taker);
    put_word(&mut calldata[..], head + TAKER_SIG_OFFSET_WORD, (HEAD_WORDS as u128) * 32);
    encode_order(calldata, head + 10, &trade.maker);
    put_word(
        &mut calldata[..],
        head + MAKER_SIG_OFFSET_WORD,
        ((HEAD_WORDS + SIG_TAIL_WORDS) as u128) * 32,
    );
    put_word(calldata, head + 20, u128::from(trade.price.0));
    put_word(calldata, head + 21, u128::from(trade.traded_quantity.0));
    put_word(calldata, head + 22, u128::from(trade.taker_remaining.0));
    encode_signature(calldata, tail, trade.taker.cold.common.signature());
    encode_signature(calldata, tail + SIG_TAIL_WORDS, trade.maker.cold.common.signature());
}

/// Encodes one `Order` into its nine-word static slot.
fn encode_order(calldata: &mut [u8], word: usize, order: &Order) {
    put_bytes32(calldata, word, &order.cold.common.symbol().0);
    put_address(calldata, word + 1, &order.hot.user);
    put_word(calldata, word + 2, u128::from(order.hot.nonce.0));
    put_word(calldata, word + 3, u128::from(order.hot.price.0));
    put_word(calldata, word + 4, u128::from(order.hot.quantity.0));
    put_word(calldata, word + 5, u128::from(order.total_quantity().0));
    put_word(calldata, word + 6, u128::from(order.hot.side as u8));
    put_word(calldata, word + 7, cryptography::evm::time_in_force_tag(order.hot.time_in_force));
    put_word(calldata, word + 8, u128::from(order.cold.common.timestamp().0));
}

/// Encodes one raw 65-byte signature: a length word plus the bytes padded
/// to three words.
fn encode_signature(calldata: &mut [u8], word: usize, signature: Signature) {
    put_word(calldata, word, 65);
    calldata[4 + word * 32 + 32..4 + word * 32 + 32 + 65].copy_from_slice(&signature.0);
}

/// Writes a big-endian integer right-aligned into the given word. The
/// calldata holds the 4 selector bytes before word 0, so the byte offset
/// of a word is `4 + word * 32`.
fn put_word(calldata: &mut [u8], word: usize, value: u128) {
    calldata[4 + word * 32 + 16..4 + (word + 1) * 32].copy_from_slice(&value.to_be_bytes());
}

/// Writes a raw 32-byte value into the given word.
fn put_bytes32(calldata: &mut [u8], word: usize, value: &[u8; 32]) {
    calldata[4 + word * 32..4 + (word + 1) * 32].copy_from_slice(value);
}

/// Writes a 20-byte address right-aligned into the given word.
fn put_address(calldata: &mut [u8], word: usize, address: &primitives::address::Address) {
    calldata[4 + word * 32 + 12..4 + (word + 1) * 32].copy_from_slice(&address.0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::address::Address;
    use primitives::base::{Hash32, Nonce, Side, Symbol};
    use primitives::order::{OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use primitives::time_in_force::TimeInForce;
    use primitives::value::{Price, Quantity, TimestampMs};

    fn order(user: u8, nonce: u64, side: Side, tif: TimeInForce, sig: u8) -> Order {
        Order::new(
            OrderHot {
                user: Address([user; 20]),
                nonce: Nonce(nonce),
                price: Price(1_000),
                quantity: Quantity(100),
                time_in_force: tif,
                side,
            },
            OrderCold::new(
                OrderColdCommon::new(
                    Hash32([0xaa; 32]),
                    Symbol([0xbb; 32]),
                    Signature([sig; 65]),
                    TimestampMs(1_700_000_000_123),
                ),
                OrderKind::Standard,
            ),
        )
    }

    fn trade(taker: Order, maker: Order) -> Trade {
        Trade::new(taker, Quantity(150), maker, Price(1_000), Quantity(100))
    }

    /// Reads the word at the given index of the calldata; the values are
    /// written right-aligned, so the low 16 bytes carry the integer.
    fn word(calldata: &[u8], index: usize) -> u128 {
        let bytes: [u8; 32] = calldata[4 + index * 32..4 + (index + 1) * 32].try_into().unwrap();
        u128::from_be_bytes(bytes[16..].try_into().unwrap())
    }

    #[test]
    fn test_selector_is_the_keccak_of_the_type_string() {
        let mut hasher = Keccak256::new();
        hasher.update(SETTLE_BATCH_TYPE.as_bytes());
        let digest: [u8; 32] = hasher.finalize().into();
        assert_eq!(&digest[0..4], &settle_batch_selector());
    }

    #[test]
    fn test_empty_batch_is_rejected() {
        assert_eq!(encode_settle_batch(&[]), Err(CalldataError::EmptyBatch));
    }

    #[test]
    fn test_single_trade_layout() {
        let taker = order(1, 7, Side::Buy, TimeInForce::Gtd(48), 0x01);
        let maker = order(2, 8, Side::Sell, TimeInForce::Gtc, 0x01);
        let trade = trade(taker, maker);
        let calldata = encode_settle_batch(&[trade]).unwrap();

        // selector ‖ 0x20 ‖ length 1 ‖ element head offset.
        assert_eq!(&calldata[0..4], &settle_batch_selector());
        assert_eq!(word(&calldata, 0), 0x20);
        assert_eq!(word(&calldata, 1), 1);
        // One element: the offsets array holds one word, the head offset
        // is 0x20 relative to it.
        assert_eq!(word(&calldata, 2), 0x20);

        // Head: the taker order fields.
        let head = 3;
        assert_eq!(&calldata[4 + head * 32..4 + head * 32 + 32], &[0xbb; 32]); // symbol
        assert_eq!(word(&calldata, head + 2), 7); // nonce
        assert_eq!(word(&calldata, head + 3), 1_000); // price
        assert_eq!(word(&calldata, head + 4), 100); // visible quantity
        assert_eq!(word(&calldata, head + 5), 100); // total (standard order)
        assert_eq!(word(&calldata, head + 6), 0); // side buy
        assert_eq!(word(&calldata, head + 7), 3 | (48 << 8)); // packed gtd tag
        assert_eq!(word(&calldata, head + 8), 1_700_000_000_123); // timestamp

        // The taker signature offset points past the 23-word head.
        assert_eq!(word(&calldata, head + 9), (HEAD_WORDS as u128) * 32);
        // The maker order sits at head + 10.
        assert_eq!(word(&calldata, head + 12), 8); // maker nonce
        assert_eq!(word(&calldata, head + 16), 1); // maker side sell
        assert_eq!(word(&calldata, head + 17), 0); // gtc tag
        // The maker signature offset points past the taker signature tail.
        assert_eq!(word(&calldata, head + 19), ((HEAD_WORDS + SIG_TAIL_WORDS) as u128) * 32);
        // The scalars.
        assert_eq!(word(&calldata, head + 20), 1_000); // price
        assert_eq!(word(&calldata, head + 21), 100); // traded quantity
        assert_eq!(word(&calldata, head + 22), 150); // taker remaining

        // Tail: the taker signature length + 65 raw bytes; the maker
        // signature follows one signature tail later.
        let tail = head + HEAD_WORDS;
        assert_eq!(word(&calldata, tail), 65);
        assert_eq!(&calldata[4 + tail * 32 + 32..4 + tail * 32 + 32 + 65], &[0x01; 65]);
        assert_eq!(word(&calldata, tail + SIG_TAIL_WORDS), 65);
    }

    #[test]
    fn test_two_trades_layout() {
        let taker = order(1, 1, Side::Buy, TimeInForce::Gtc, 0x01);
        let maker_a = order(2, 1, Side::Sell, TimeInForce::Gtc, 0x02);
        let maker_b = order(3, 1, Side::Sell, TimeInForce::Gtc, 0x03);
        let calldata =
            encode_settle_batch(&[trade(taker, maker_a), trade(taker, maker_b)]).unwrap();

        assert_eq!(word(&calldata, 1), 2); // length
        // The elements concatenate with the head-plus-tail stride: element
        // 0 at word 4, element 1 one stride later; the offsets are
        // relative to the offsets array (word 2).
        let stride = HEAD_WORDS + TAIL_WORDS;
        let base0 = 1 + 1 + 2;
        let base1 = base0 + stride;
        assert_eq!(word(&calldata, 2), (base0 - 2) as u128 * 32);
        assert_eq!(word(&calldata, 3), (base1 - 2) as u128 * 32);
        assert_eq!(word(&calldata, base0 + 2), 1); // element 0 taker nonce
        assert_eq!(word(&calldata, base1 + 2), 1); // element 1 taker nonce
        assert_eq!(word(&calldata, base1 + 12), 1); // element 1 maker nonce (user 3)

        // Each element's signatures sit right after its own head, so the
        // three distinct signature bytes all survive.
        let sigs_at = |element: usize| {
            let word_offset = base0 + element * stride + HEAD_WORDS;
            calldata[4 + word_offset * 32 + 32..4 + word_offset * 32 + 32 + 65].to_vec()
        };
        // Element 0: taker 0x01, maker 0x02.
        assert_eq!(&sigs_at(0)[..1], &[0x01]);
        assert_eq!(&calldata[4 + (base0 + HEAD_WORDS + SIG_TAIL_WORDS) * 32 + 32..][..1], &[0x02]);
        // Element 1: taker 0x01, maker 0x03.
        assert_eq!(&sigs_at(1)[..1], &[0x01]);
        assert_eq!(&calldata[4 + (base1 + HEAD_WORDS + SIG_TAIL_WORDS) * 32 + 32..][..1], &[0x03]);
    }
}
