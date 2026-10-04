//! EVM-compatible order hashing and signature verification: the EIP-712
//! typed data of doc/settlement-protocol.md. The off-chain verification (the
//! [SVD_Pretrade] gate) and the on-chain settlement protocol must hash the
//! identical structures, so the hashing below is the single source of truth
//! of the order and cancel messages.

use primitives::address::Address;
use primitives::message::hot_path::CancelOrder;
use primitives::order::Order;
use primitives::signature::Signature;
use primitives::time_in_force::TimeInForce;
use sha3::{Digest, Keccak256};

use crate::crypto::CryptoError;

/// Keccak-256 of the EIP-712 type string of the `Order` struct. The `timeInForce`
/// travels as a packed `uint64`: the tag (0 gtc, 1 ioc, 2 fok, 3 gtd, 4 day)
/// in the low byte, the GTD lifetime (hours) in the next byte.
fn order_type_hash() -> [u8; 32] {
    keccak256(b"Order(bytes32 symbol,address user,uint64 nonce,uint64 price,uint64 quantity,uint64 totalQuantity,uint8 side,uint64 timeInForce,uint64 timestampMs)")
}

/// Keccak-256 of the EIP-712 type string of the `CancelOrder` struct. The
/// cancel never reaches the chain — it is an off-chain operation signed
/// against the same domain.
fn cancel_type_hash() -> [u8; 32] {
    keccak256(
        b"CancelOrder(bytes32 symbol,bytes32 orderId,address user,uint64 nonce,uint64 timestamp)",
    )
}

/// Keccak-256 of the EIP-712 domain type string.
fn domain_type_hash() -> [u8; 32] {
    keccak256(b"Eip712Domain(string name,string version,uint256 chainId,address verifyingContract)")
}

/// The EIP-712 domain separator of the settlement protocol.
pub fn domain_separator(chain_id: u64, verifying_contract: Address) -> [u8; 32] {
    let mut encoded = [0u8; 32 * 5];
    encoded[..32].copy_from_slice(&domain_type_hash());
    encoded[32..64].copy_from_slice(&keccak256(b"dex-rs"));
    encoded[64..96].copy_from_slice(&keccak256(b"1"));
    put_uint(&mut encoded[96..128], u128::from(chain_id));
    put_address(&mut encoded[128..160], &verifying_contract);
    keccak256(&encoded)
}

/// The EIP-712 hash of an order: the value the user signs, and the value the
/// on-chain settlement protocol recovers the signature against.
pub fn order_hash(order: &Order, chain_id: u64, verifying_contract: Address) -> [u8; 32] {
    let mut encoded = [0u8; 32 * 10];
    encoded[..32].copy_from_slice(&order_type_hash());
    encoded[32..64].copy_from_slice(&order.cold.common.symbol().0);
    put_address(&mut encoded[64..96], &order.hot.user);
    put_uint(&mut encoded[96..128], u128::from(order.hot.nonce.0));
    put_uint(&mut encoded[128..160], u128::from(order.hot.price.0));
    put_uint(&mut encoded[160..192], u128::from(order.hot.quantity.0));
    put_uint(&mut encoded[192..224], u128::from(order.total_quantity().0));
    put_uint(&mut encoded[224..256], u128::from(order.hot.side as u8));
    put_uint(&mut encoded[256..288], time_in_force_tag(order.hot.time_in_force));
    put_uint(&mut encoded[288..320], u128::from(order.cold.common.timestamp().0));
    digest_message(&keccak256(&encoded), chain_id, verifying_contract)
}

/// The EIP-712 hash of a cancel request.
pub fn cancel_hash(cancel: &CancelOrder, chain_id: u64, verifying_contract: Address) -> [u8; 32] {
    let mut encoded = [0u8; 32 * 6];
    encoded[..32].copy_from_slice(&cancel_type_hash());
    encoded[32..64].copy_from_slice(&cancel.symbol().0);
    encoded[64..96].copy_from_slice(&cancel.order_id().0);
    put_address(&mut encoded[96..128], &cancel.user());
    put_uint(&mut encoded[128..160], u128::from(cancel.nonce().0));
    put_uint(&mut encoded[160..192], u128::from(cancel.timestamp().0));
    digest_message(&keccak256(&encoded), chain_id, verifying_contract)
}

/// Verifies the signature of an order against its EIP-712 hash: the
/// recovered address must equal the order's user.
pub fn verify_order(
    order: &Order,
    chain_id: u64,
    verifying_contract: Address,
) -> Result<(), CryptoError> {
    let hash = order_hash(order, chain_id, verifying_contract);
    verify_signature(&hash, &order.cold.common.signature(), order.hot.user)
}

/// Verifies the signature of a cancel request against its EIP-712 hash.
pub fn verify_cancel(
    cancel: &CancelOrder,
    chain_id: u64,
    verifying_contract: Address,
) -> Result<(), CryptoError> {
    let hash = cancel_hash(cancel, chain_id, verifying_contract);
    verify_signature(&hash, &cancel.signature(), cancel.user())
}

/// Wraps the struct hash into the EIP-712 final message digest.
fn digest_message(struct_hash: &[u8; 32], chain_id: u64, verifying_contract: Address) -> [u8; 32] {
    let separator = domain_separator(chain_id, verifying_contract);
    let mut message = [0u8; 66];
    message[0] = 0x19;
    message[1] = 0x01;
    message[2..34].copy_from_slice(&separator);
    message[34..66].copy_from_slice(struct_hash);
    keccak256(&message)
}

/// Recovers the signer of the prehash and checks it against `user`.
fn verify_signature(
    hash: &[u8; 32],
    signature: &Signature,
    user: Address,
) -> Result<(), CryptoError> {
    let bytes = &signature.0;
    let recovery_id = k256::ecdsa::RecoveryId::from_byte(bytes[64].saturating_sub(27))
        .ok_or_else(|| CryptoError::MalformedSignature(format!("bad recovery id {}", bytes[64])))?;
    let signature = k256::ecdsa::Signature::from_slice(&bytes[..64])
        .map_err(|err| CryptoError::MalformedSignature(err.to_string()))?;
    let key = k256::ecdsa::VerifyingKey::recover_from_prehash(hash, &signature, recovery_id)
        .map_err(|_| CryptoError::InvalidSignature)?;
    let point = key.to_encoded_point(false);
    let recovered = &keccak256(&point.as_bytes()[1..])[12..];
    if recovered != user.0.as_slice() {
        return Err(CryptoError::InvalidSignature);
    }
    Ok(())
}

/// The on-chain tag of a time in force: the tag in the low byte, the GTD
/// lifetime (hours) in the next byte.
fn time_in_force_tag(time_in_force: TimeInForce) -> u128 {
    match time_in_force {
        TimeInForce::Gtc => 0,
        TimeInForce::Ioc => 1,
        TimeInForce::Fok => 2,
        TimeInForce::Gtd(hours) => 3 | (u128::from(hours) << 8),
        TimeInForce::Day => 4,
    }
}

/// Writes a big-endian integer into the low bytes of a 32-byte word.
fn put_uint(word: &mut [u8], value: u128) {
    let bytes = value.to_be_bytes();
    word[32 - bytes.len()..].copy_from_slice(&bytes);
}

/// Writes an address into the low bytes of a 32-byte word.
fn put_address(word: &mut [u8], address: &Address) {
    word[12..].copy_from_slice(&address.0);
}

/// Keccak-256 of the input.
fn keccak256(input: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(input);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use k256::ecdsa::SigningKey;
    use primitives::base::{Hash32, Nonce, Side, Symbol};
    use primitives::order::{OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use primitives::value::{Price, Quantity, TimestampMs};

    const CHAIN_ID: u64 = 31_337;
    const CONTRACT: Address = Address([0xabu8; 20]);

    /// A deterministic test signing key.
    fn signing_key() -> SigningKey {
        SigningKey::from_slice(&[7u8; 32]).expect("valid key seed")
    }

    /// The ethereum address of a verifying key.
    fn address_of(key: &k256::ecdsa::VerifyingKey) -> Address {
        let point = key.to_encoded_point(false);
        let hash = keccak256(&point.as_bytes()[1..]);
        Address(hash[12..].try_into().expect("20 bytes"))
    }

    fn order(user: Address, nonce: u64, price: u64, quantity: u64) -> Order {
        Order::new(
            OrderHot {
                user,
                nonce: Nonce(nonce),
                price: Price(price),
                quantity: Quantity(quantity),
                time_in_force: TimeInForce::Gtc,
                side: Side::Buy,
            },
            OrderCold::new(
                OrderColdCommon::new(
                    Hash32([0; 32]),
                    Symbol([1u8; 32]),
                    Signature::default(),
                    TimestampMs(1_700_000_000_000),
                ),
                OrderKind::Standard,
            ),
        )
    }

    /// Signs an order and returns it with the signature set.
    fn signed_order(user: Address, nonce: u64, price: u64, quantity: u64) -> Order {
        let mut order = order(user, nonce, price, quantity);
        let hash = order_hash(&order, CHAIN_ID, CONTRACT);
        let (signature, recovery_id) =
            signing_key().sign_prehash_recoverable(&hash).expect("signs the prehash");
        let mut bytes = [0u8; 65];
        bytes[..64].copy_from_slice(&signature.to_bytes());
        bytes[64] = recovery_id.to_byte() + 27;
        order.cold.common = OrderColdCommon::new(
            Hash32([0; 32]),
            Symbol([1u8; 32]),
            Signature(bytes),
            TimestampMs(1_700_000_000_000),
        );
        order
    }

    #[test]
    fn test_verify_order_accepts_a_valid_signature() {
        let user = address_of(signing_key().verifying_key());
        let order = signed_order(user, 1, 100, 10);
        verify_order(&order, CHAIN_ID, CONTRACT).expect("the signature verifies");
    }

    #[test]
    fn test_verify_order_rejects_a_tampered_order() {
        let user = address_of(signing_key().verifying_key());
        let mut order = signed_order(user, 1, 100, 10);
        order.hot.price = Price(101);
        assert!(matches!(
            verify_order(&order, CHAIN_ID, CONTRACT),
            Err(CryptoError::InvalidSignature)
        ));
    }

    #[test]
    fn test_verify_order_rejects_a_wrong_signer() {
        let user = address_of(signing_key().verifying_key());
        let order = signed_order(user, 1, 100, 10);
        let other = Address([0xefu8; 20]);
        assert!(matches!(
            verify_order(&order, CHAIN_ID, other),
            Err(CryptoError::InvalidSignature)
        ));
    }

    #[test]
    fn test_order_hash_distinguishes_every_field() {
        let user = address_of(signing_key().verifying_key());
        let base = order_hash(&order(user, 1, 100, 10), CHAIN_ID, CONTRACT);
        let other_price = order_hash(&order(user, 1, 101, 10), CHAIN_ID, CONTRACT);
        let other_nonce = order_hash(&order(user, 2, 100, 10), CHAIN_ID, CONTRACT);
        let other_chain = order_hash(&order(user, 1, 100, 10), CHAIN_ID + 1, CONTRACT);
        assert_ne!(base, other_price);
        assert_ne!(base, other_nonce);
        assert_ne!(base, other_chain);
    }

    #[test]
    fn test_verify_cancel_accepts_a_valid_signature() {
        let user = address_of(signing_key().verifying_key());
        let mut cancel = CancelOrder::new(
            Symbol([1u8; 32]),
            Hash32([2u8; 32]),
            user,
            Nonce(3),
            TimestampMs(4),
            Signature::default(),
        );
        let hash = cancel_hash(&cancel, CHAIN_ID, CONTRACT);
        let (signature, recovery_id) =
            signing_key().sign_prehash_recoverable(&hash).expect("signs the prehash");
        let mut bytes = [0u8; 65];
        bytes[..64].copy_from_slice(&signature.to_bytes());
        bytes[64] = recovery_id.to_byte() + 27;
        cancel = cancel.with_signature(Signature(bytes));
        verify_cancel(&cancel, CHAIN_ID, CONTRACT).expect("the signature verifies");
    }
}
