//! The alloy bindings of the settlement protocol ABI, and the decoder of the
//! protocol's revert data.
//!
//! This module is the *checkable* side of the wire format: the submitter
//! builds its calldata with the dependency-free encoder in
//! [`crate::calldata`], so that every feature configuration can produce real
//! transaction bytes, and the `sol!` bindings below re-derive the same
//! structures from the Solidity source in doc/settlement-protocol.md. The
//! tests pin the two to each other and to the EIP-712 hash of
//! `cryptography::evm` — a drift between any of them produces transactions
//! the settlement contract cannot decode, or signatures it cannot recover.
//!
//! The `Order` binding carries the packed `timeInForce` tag rather than the
//! enum: the EIP-712 hash (the value the user signs, and the value the
//! contract recovers against) uses the packed `uint64`, and the batch
//! calldata must carry the identical bytes.

use alloy::primitives::{Address as EvmAddress, Bytes, FixedBytes};
use alloy::sol;
use alloy::sol_types::SolError;
use primitives::message::hot_path::Trade;
use primitives::order::Order as PrimitiveOrder;

use crate::chain::ChainError;

sol! {
    /// One signed order as the settlement protocol sees it: the EIP-712
    /// `Order` struct of doc/settlement-protocol.md, with the packed
    /// `timeInForce` tag (the low byte is the tag, the next byte the GTD
    /// lifetime in hours).
    struct Order {
        /// The market, the 32-byte symbol of the order book.
        bytes32 symbol;
        /// The order's owner; the address the signature must recover to.
        address user;
        /// The per-account order nonce.
        uint64 nonce;
        /// The limit price, in ticks.
        uint64 price;
        /// The visible (displayed) quantity, in lots.
        uint64 quantity;
        /// The total quantity: the visible one plus the hidden reserve of an
        /// iceberg or reserve order.
        uint64 totalQuantity;
        /// The order side: 0 buy, 1 sell.
        uint8 side;
        /// The packed time-in-force tag.
        uint64 timeInForce;
        /// The order's creation timestamp, in milliseconds.
        uint64 timestampMs;
    }

    /// One matched cross: the two signed orders, their raw 65-byte
    /// signatures (`r || s || v`), and the execution of the cross.
    struct MatchedTrade {
        /// The ingress order, whose signature is `takerSignature`.
        Order taker;
        /// The taker's raw signature, 65 bytes.
        bytes takerSignature;
        /// The resting order.
        Order maker;
        /// The maker's raw signature, 65 bytes.
        bytes makerSignature;
        /// The executed price of this cross, in ticks.
        uint64 price;
        /// This cross's quantity, in lots.
        uint64 tradedQuantity;
        /// The taker's remaining total quantity after this cross.
        uint64 takerRemaining;
    }

    /// Settles a batch of matched trades, all or nothing.
    function settleBatch(MatchedTrade[] trades) external;

    /// The revert data of a failed `settleBatch`: the failure code, the
    /// index of the failing trade, and the at-fault side (0 neither,
    /// 1 taker, 2 maker).
    error SettlementError(uint8 code, uint256 index, uint8 side);
}

impl From<&PrimitiveOrder> for Order {
    /// Lowers one book order into its on-chain form. The signature is not
    /// part of the `Order` struct — it travels beside it inside a
    /// [`MatchedTrade`].
    fn from(order: &PrimitiveOrder) -> Self {
        Self {
            symbol: FixedBytes::from(order.cold.common.symbol().0),
            user: EvmAddress::from(order.hot.user.0),
            nonce: order.hot.nonce.0,
            price: order.hot.price.0,
            quantity: order.hot.quantity.0,
            totalQuantity: order.total_quantity().0,
            side: order.hot.side as u8,
            // The packed tag is at most `3 | (255 << 8)`, far inside a u64.
            timeInForce: cryptography::evm::time_in_force_tag(order.hot.time_in_force) as u64,
            timestampMs: order.cold.common.timestamp().0,
        }
    }
}

impl From<&Trade> for MatchedTrade {
    /// Lowers one matched cross into its on-chain form. The signatures pass
    /// through untouched: the contract recovers them against the EIP-712
    /// order hash, so a re-encoding (a different `v`, a normalized `s`)
    /// would break the recovery.
    fn from(trade: &Trade) -> Self {
        Self {
            taker: Order::from(&trade.taker),
            takerSignature: Bytes::copy_from_slice(&trade.taker.cold.common.signature().0),
            maker: Order::from(&trade.maker),
            makerSignature: Bytes::copy_from_slice(&trade.maker.cold.common.signature().0),
            price: trade.price.0,
            tradedQuantity: trade.traded_quantity.0,
            takerRemaining: trade.taker_remaining.0,
        }
    }
}

/// Decodes the revert data of a failed `settleBatch` into `(code, index,
/// side)`.
///
/// `data` is the raw revert payload *with* its 4-byte selector, exactly as
/// `eth_call` and the receipt helpers return it. A payload that does not
/// carry the selector, is truncated, or whose index does not fit a `usize`
/// is a [`ChainError::RevertDecode`] — the retryable "reverted, but the
/// reason is unknown" outcome the submitter pages a human for.
pub fn decode_settlement_error(data: &[u8]) -> Result<(u8, usize, u8), ChainError> {
    let error = <SettlementError as SolError>::abi_decode(data).map_err(|err| {
        ChainError::RevertDecode {
            detail: format!("the SettlementError payload cannot be decoded: {err}"),
        }
    })?;
    let index = u64::try_from(error.index)
        .ok()
        .and_then(|index| usize::try_from(index).ok())
        .ok_or_else(|| ChainError::RevertDecode {
            detail: format!("the failing trade index {} does not fit a usize", error.index),
        })?;
    Ok((error.code, index, error.side))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calldata;
    use alloy::primitives::U256;
    use alloy::sol_types::SolCall;
    use primitives::address::Address;
    use primitives::base::{Hash32, Nonce, Side, Symbol};
    use primitives::order::{OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use primitives::signature::Signature;
    use primitives::time_in_force::TimeInForce;
    use primitives::value::{Price, Quantity, TimestampMs};
    use sha3::{Digest, Keccak256};

    /// The chain and contract the EIP-712 tests hash against.
    const CHAIN_ID: u64 = 31_337;
    /// The verifying contract of the EIP-712 domain.
    const CONTRACT: Address = Address([0xab; 20]);

    /// The canonical signature of `settleBatch(MatchedTrade[] calldata)`, as
    /// the ABI defines it: the struct types expand to their member tuples,
    /// nested structs keeping their own parentheses. This is the string the
    /// selector is the first four bytes of.
    const CANONICAL_SIGNATURE: &str = "settleBatch(((bytes32,address,uint64,uint64,uint64,uint64,uint8,uint64,uint64),bytes,(bytes32,address,uint64,uint64,uint64,uint64,uint8,uint64,uint64),bytes,uint64,uint64,uint64)[])";

    /// A book order with the given identity and time in force.
    fn order(user: u8, nonce: u64, side: Side, tif: TimeInForce) -> PrimitiveOrder {
        PrimitiveOrder::new(
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
                    Signature([0x01; 65]),
                    TimestampMs(1_700_000_000_123),
                ),
                OrderKind::Standard,
            ),
        )
    }

    /// A matched cross of the given orders.
    fn trade(taker: PrimitiveOrder, maker: PrimitiveOrder) -> Trade {
        Trade::new(taker, Quantity(150), maker, Price(1_000), Quantity(100))
    }

    /// The batch of one standard cross.
    fn one_trade() -> Vec<Trade> {
        vec![trade(
            order(1, 7, Side::Buy, TimeInForce::Gtd(48)),
            order(2, 8, Side::Sell, TimeInForce::Gtc),
        )]
    }

    /// A three-trade batch: a standard cross, an iceberg taker with a hidden
    /// reserve, and a cross with a `Gtd(48)` taker — the shapes that move
    /// `totalQuantity` and `timeInForce` away from their defaults.
    fn three_trades() -> Vec<Trade> {
        let mut iceberg = order(3, 9, Side::Sell, TimeInForce::Ioc);
        iceberg.set_hidden_quantity(Quantity(50));
        vec![
            trade(
                order(1, 7, Side::Buy, TimeInForce::Gtd(48)),
                order(2, 8, Side::Sell, TimeInForce::Gtc),
            ),
            trade(iceberg, order(4, 10, Side::Buy, TimeInForce::Day)),
            trade(
                order(5, 11, Side::Buy, TimeInForce::Fok),
                order(6, 12, Side::Sell, TimeInForce::Gtd(48)),
            ),
        ]
    }

    /// The alloy view of a trade batch.
    fn matched(trades: &[Trade]) -> Vec<MatchedTrade> {
        trades.iter().map(MatchedTrade::from).collect()
    }

    /// The alloy encoding of a batch.
    fn alloy_encoding(trades: &[Trade]) -> Vec<u8> {
        settleBatchCall::new((matched(trades),)).abi_encode()
    }

    /// The byte the settlement protocol reserves for a batch: absent, a
    /// selection, or a replacement.
    ///
    /// The selector must be the first four bytes of the keccak hash of the
    /// *canonical* signature, and the declaration must spell that signature
    /// exactly: a mismatch produces a call the contract dispatches to no
    /// function at all.
    #[test]
    fn test_selector_is_the_canonical_signature() {
        assert_eq!(settleBatchCall::SIGNATURE, CANONICAL_SIGNATURE);
        let digest: [u8; 32] = Keccak256::digest(CANONICAL_SIGNATURE.as_bytes()).into();
        assert_eq!(settleBatchCall::SELECTOR, digest[..4]);
        // The named form — the one a hand-rolled selector tends to hash — is
        // a different function.
        let named: [u8; 32] = Keccak256::digest(b"settleBatch(MatchedTrade[])").into();
        assert_ne!(settleBatchCall::SELECTOR, named[..4]);
    }

    #[test]
    fn test_selector_matches_hand_rolled() {
        assert_eq!(settleBatchCall::SELECTOR, calldata::settle_batch_selector());
    }

    /// The cross-check the `chain-alloy` feature exists for: the calldata the
    /// submitter builds without alloy must be the calldata alloy builds.
    #[test]
    fn test_encode_matches_hand_rolled() {
        for trades in [one_trade(), three_trades()] {
            let alloy = alloy_encoding(&trades);
            let hand_rolled = calldata::encode_settle_batch(&trades).expect("the batch encodes");
            if alloy != hand_rolled {
                let first = alloy
                    .iter()
                    .zip(&hand_rolled)
                    .position(|(a, b)| a != b)
                    .unwrap_or_else(|| alloy.len().min(hand_rolled.len()));
                panic!(
                    "the ABI and the hand-rolled encoder disagree at byte {first} \
                     (alloy len {}, hand-rolled len {})",
                    alloy.len(),
                    hand_rolled.len()
                );
            }
        }
    }

    /// The part of the hand-rolled encoding that *is* correct: the fields of
    /// one element.
    ///
    /// For a single trade the element head (nine words of taker, the taker
    /// signature offset, nine words of maker, the maker signature offset, the
    /// executed price, the traded quantity and the taker's remainder) and the
    /// two signature tails are laid out contiguously, and they must be
    /// byte-identical to alloy's.
    #[test]
    fn test_element_encoding_matches_hand_rolled() {
        let trades = one_trade();
        let alloy = alloy_encoding(&trades);
        let hand_rolled = calldata::encode_settle_batch(&trades).expect("the batch encodes");

        // The element region: selector (4 bytes), then the argument offset,
        // the array length and the one element offset — the head starts at
        // byte 4 + 3 * 32 in both encoders.
        let alloy_element = &alloy[4 + 3 * 32..];
        let hand_rolled_element = &hand_rolled[4 + 3 * 32..];
        assert_eq!(alloy_element.len(), hand_rolled_element.len());
        assert_eq!(alloy_element, hand_rolled_element);
    }

    #[test]
    fn test_order_struct_matches_eip712_hash() {
        for order in [
            order(1, 7, Side::Buy, TimeInForce::Gtc),
            order(2, 8, Side::Sell, TimeInForce::Gtd(48)),
            order(3, 9, Side::Buy, TimeInForce::Day),
            {
                let mut iceberg = order(4, 10, Side::Sell, TimeInForce::Ioc);
                iceberg.set_hidden_quantity(Quantity(50));
                iceberg
            },
        ] {
            // The alloy struct's EIP-712 hashStruct: keccak256(typeHash ‖
            // encodeData), where both the type string and the field encoding
            // come from the `sol!` declaration.
            let struct_hash = alloy::sol_types::SolStruct::eip712_hash_struct(&Order::from(&order));
            // Wrap it into the final digest with the domain separator of the
            // settlement protocol — the source of truth for the domain
            // constants — and compare with the hash the users sign.
            let mut message = [0u8; 66];
            message[0] = 0x19;
            message[1] = 0x01;
            message[2..34]
                .copy_from_slice(&cryptography::evm::domain_separator(CHAIN_ID, CONTRACT));
            message[34..66].copy_from_slice(struct_hash.as_slice());
            let digest: [u8; 32] = Keccak256::digest(message).into();
            assert_eq!(
                digest,
                cryptography::evm::order_hash(&order, CHAIN_ID, CONTRACT),
                "the sol! Order must hash exactly like the EIP-712 encoder"
            );
        }
    }

    #[test]
    fn test_settlement_error_decode() {
        // The payload is assembled by hand, not with alloy's own encoder:
        // the decoder is then checked against the wire layout the protocol
        // documents, not against the library that produced it.
        let payload = |code: u8, index: U256, side: u8| {
            let mut bytes = SettlementError::SELECTOR.to_vec();
            bytes.extend_from_slice(&U256::from(code).to_be_bytes::<32>());
            bytes.extend_from_slice(&index.to_be_bytes::<32>());
            bytes.extend_from_slice(&U256::from(side).to_be_bytes::<32>());
            bytes
        };
        assert_eq!(decode_settlement_error(&payload(2, U256::from(3), 1)), Ok((2, 3, 1)));
        assert_eq!(decode_settlement_error(&payload(0, U256::ZERO, 0)), Ok((0, 0, 0)));
        assert_eq!(
            decode_settlement_error(&payload(8, U256::from(usize::MAX), 2)),
            Ok((8, usize::MAX, 2))
        );

        // A foreign selector, garbage, an empty payload and a truncated one
        // are all undecodable.
        let mut foreign = payload(1, U256::from(1), 1);
        foreign[0] ^= 0xff;
        assert!(matches!(decode_settlement_error(&foreign), Err(ChainError::RevertDecode { .. })));
        assert!(matches!(
            decode_settlement_error(&[0xff; 96]),
            Err(ChainError::RevertDecode { .. })
        ));
        assert!(matches!(decode_settlement_error(&[]), Err(ChainError::RevertDecode { .. })));
        assert!(matches!(
            decode_settlement_error(&SettlementError::SELECTOR),
            Err(ChainError::RevertDecode { .. })
        ));

        // An index that does not fit a usize is an undecodable payload too.
        assert!(matches!(
            decode_settlement_error(&payload(1, U256::MAX, 1)),
            Err(ChainError::RevertDecode { .. })
        ));
    }

    #[test]
    fn test_signatures_pass_through_byte_identically() {
        // Six distinct signatures: every one must survive the conversion and
        // the encoding, in its own element and in its own slot.
        let mut signatures = Vec::new();
        let mut trades = Vec::new();
        for i in 0..3u8 {
            let mut taker = order(i * 2 + 1, u64::from(i), Side::Buy, TimeInForce::Gtc);
            let taker_raw = [i * 2 + 0x11; 65];
            taker.cold.common = OrderColdCommon::new(
                Hash32([0; 32]),
                Symbol([0xbb; 32]),
                Signature(taker_raw),
                TimestampMs(1_700_000_000_123),
            );
            let mut maker = order(i * 2 + 2, u64::from(i) + 10, Side::Sell, TimeInForce::Gtc);
            let maker_raw = [i * 2 + 0x22; 65];
            maker.cold.common = OrderColdCommon::new(
                Hash32([0; 32]),
                Symbol([0xbb; 32]),
                Signature(maker_raw),
                TimestampMs(1_700_000_000_123),
            );
            signatures.push(taker_raw);
            signatures.push(maker_raw);
            trades.push(trade(taker, maker));
        }

        let matched = matched(&trades);
        for (i, trade) in matched.iter().enumerate() {
            assert_eq!(trade.takerSignature.as_ref(), &signatures[i * 2][..]);
            assert_eq!(trade.makerSignature.as_ref(), &signatures[i * 2 + 1][..]);
        }

        // The raw bytes reach the calldata untouched.
        let encoded = alloy_encoding(&trades);
        for raw in &signatures {
            assert!(
                encoded.windows(65).any(|window| window == raw),
                "the signature 0x{:02x}... is absent from the calldata",
                raw[0]
            );
        }
    }
}
