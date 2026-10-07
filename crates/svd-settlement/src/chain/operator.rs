//! The operator key: the Web3 Secret Storage v3 keystore and the workspace's
//! first concrete cryptography implementations.
//!
//! [`OperatorKey::unlock`] reads the keystore file and derives the operator
//! address; the password never travels in the config, only through
//! [`password_from_env`] (`SVD_SETTLEMENT_KEYSTORE_PASSWORD`). The key
//! material is zeroized on drop by `k256` and never reaches a log line:
//! [`OperatorKey`] and [`EvmSigner`] redact their `Debug`.
//!
//! [`EvmSigner`] is the signing end of the settlement protocol: it signs
//! the 32-byte EIP-712 prehash built by `cryptography::evm` with secp256k1
//! and emits the 65-byte `r || s || v` form with the Ethereum convention
//! `v ∈ {27, 28}`, the exact bytes `verify_order` recovers the signer from.

use std::fmt;
use std::path::Path;

use aes::cipher::{KeyIvInit, StreamCipher};
use cryptography::crypto::{CryptoError, PublicKey, Signature, Signer};
use k256::ecdsa::signature::hazmat::PrehashVerifier;
use k256::elliptic_curve::sec1::ToEncodedPoint;
use primitives::address::Address;
use serde::Deserialize;
use sha3::{Digest, Keccak256};
use zeroize::Zeroize;

/// The environment variable holding the operator keystore password.
pub const KEYSTORE_PASSWORD_ENV: &str = "SVD_SETTLEMENT_KEYSTORE_PASSWORD";

/// The AES-128-CTR key length of the v3 format.
const AES_KEY_LEN: usize = 16;
/// The derived key length of the v3 format: 16 bytes of AES key plus 16
/// bytes of MAC key.
const DERIVED_KEY_LEN: usize = 32;
/// The iv length of AES-128-CTR.
const IV_LEN: usize = 16;
/// The MAC length: one Keccak-256 digest.
const MAC_LEN: usize = 32;

/// The AES-128-CTR stream cipher of the v3 format.
type Aes128Ctr = ctr::Ctr128BE<aes::Aes128>;

/// The failures of the operator keystore.
///
/// The message never interpolates the password or any key material: only
/// the file layout and the check that failed.
#[derive(Debug)]
pub struct KeystoreError(String);

impl KeystoreError {
    /// Wraps one detail message.
    fn new(detail: impl Into<String>) -> Self {
        Self(detail.into())
    }
}

impl fmt::Display for KeystoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "keystore: {}", self.0)
    }
}

impl std::error::Error for KeystoreError {}

/// The operator signing key: the secp256k1 secret and the address it
/// controls.
pub struct OperatorKey {
    /// The decrypted secret scalar.
    secret: k256::SecretKey,
    /// The operator address, derived from `secret`.
    address: Address,
}

impl OperatorKey {
    /// Unlocks the Web3 Secret Storage v3 keystore at `path` with
    /// `password` and derives the operator address.
    ///
    /// The file is checked end to end: the version, the cipher, the KDF
    /// parameters, the Keccak-256 MAC (which is what rejects a wrong
    /// password), the decryption, the secret's validity, and — when the
    /// file carries one — the address it claims.
    pub fn unlock(path: &Path, password: &str) -> Result<Self, KeystoreError> {
        let text = std::fs::read_to_string(path).map_err(|err| {
            KeystoreError::new(format!(
                "the keystore file {} cannot be read: {err}",
                path.display()
            ))
        })?;
        Self::from_json(&text, password)
    }

    /// The operator address, derived from the key.
    pub fn address(&self) -> Address {
        self.address
    }

    /// Parses and decrypts one v3 document.
    fn from_json(text: &str, password: &str) -> Result<Self, KeystoreError> {
        let file: KeystoreFile = serde_json::from_str(text)
            .map_err(|err| KeystoreError::new(format!("the keystore is not valid JSON: {err}")))?;
        if file.version != 3 {
            return Err(KeystoreError::new(format!(
                "unsupported keystore version {} (expected 3)",
                file.version
            )));
        }
        let crypto = file.crypto;
        if !crypto.cipher.eq_ignore_ascii_case("aes-128-ctr") {
            return Err(KeystoreError::new(format!(
                "unsupported keystore cipher {:?} (expected aes-128-ctr)",
                crypto.cipher
            )));
        }
        let iv = decode_hex_field(&crypto.cipherparams.iv, "cipherparams.iv")?;
        if iv.len() != IV_LEN {
            return Err(KeystoreError::new(format!(
                "the iv must be {IV_LEN} bytes, got {}",
                iv.len()
            )));
        }
        let ciphertext = decode_hex_field(&crypto.ciphertext, "ciphertext")?;
        let mac = decode_hex_field(&crypto.mac, "mac")?;
        if mac.len() != MAC_LEN {
            return Err(KeystoreError::new(format!(
                "the mac must be {MAC_LEN} bytes, got {}",
                mac.len()
            )));
        }

        let mut derived = derive_key(&crypto, password)?;
        let mut hasher = Keccak256::new();
        hasher.update(&derived[AES_KEY_LEN..]);
        hasher.update(&ciphertext);
        let computed: [u8; MAC_LEN] = hasher.finalize().into();
        if computed != mac.as_slice() {
            return Err(KeystoreError::new(
                "the keystore MAC does not match: wrong password or corrupted file",
            ));
        }

        let mut plaintext = ciphertext;
        let mut cipher = Aes128Ctr::new_from_slices(&derived[..AES_KEY_LEN], &iv)
            .map_err(|_| KeystoreError::new("the derived key or iv has the wrong length"))?;
        cipher.apply_keystream(&mut plaintext);
        let secret = k256::SecretKey::from_slice(&plaintext)
            .map_err(|_| KeystoreError::new("the decrypted key is not a valid secp256k1 scalar"))?;
        plaintext.zeroize();
        derived.zeroize();

        let address = address_of_point(&secret.public_key().to_encoded_point(false));
        check_claimed_address(file.address.as_deref(), address)?;
        Ok(Self { secret, address })
    }
}

impl fmt::Debug for OperatorKey {
    /// Redacts the secret: only the address is printed.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OperatorKey")
            .field("address", &format_args!("0x{}", self.address.hex()))
            .field("secret", &"[redacted]")
            .finish()
    }
}

/// The keystore password from the environment: a fatal error when
/// `SVD_SETTLEMENT_KEYSTORE_PASSWORD` is unset (or empty).
pub fn password_from_env() -> Result<String, KeystoreError> {
    password_from_env_named(KEYSTORE_PASSWORD_ENV)
}

/// The keystore password from the named environment variable; the seam the
/// tests read the missing-variable path through.
pub fn password_from_env_named(var: &str) -> Result<String, KeystoreError> {
    match std::env::var(var) {
        Ok(password) if !password.is_empty() => Ok(password),
        Ok(_) => Err(KeystoreError::new(format!("the environment variable {var} is empty"))),
        Err(std::env::VarError::NotPresent) => {
            Err(KeystoreError::new(format!("the environment variable {var} is not set")))
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(KeystoreError::new(format!("the environment variable {var} is not valid unicode")))
        }
    }
}

/// The v3 document.
#[derive(Debug, Deserialize)]
struct KeystoreFile {
    /// The format version; only 3 is accepted.
    version: u64,
    /// The cipher and KDF parameters (some exporters capitalize the key).
    #[serde(rename = "crypto", alias = "Crypto")]
    crypto: CryptoSection,
    /// The address the file claims; checked when present.
    #[serde(default)]
    address: Option<String>,
}

/// The `crypto` section of the document.
#[derive(Debug, Deserialize)]
struct CryptoSection {
    /// The symmetric cipher; only `aes-128-ctr` is supported.
    cipher: String,
    /// The cipher parameters.
    cipherparams: CipherParams,
    /// The encrypted secret key, hex.
    ciphertext: String,
    /// The key derivation function; `scrypt` or `pbkdf2`.
    kdf: String,
    /// The KDF parameters, shaped by `kdf`.
    kdfparams: serde_json::Value,
    /// The Keccak-256 check over the derived key and the ciphertext, hex.
    mac: String,
}

/// The `cipherparams` section.
#[derive(Debug, Deserialize)]
struct CipherParams {
    /// The AES-128-CTR initialization vector, hex.
    iv: String,
}

/// The `scrypt` KDF parameters.
#[derive(Debug, Deserialize)]
struct ScryptParams {
    /// The derived key length in bytes.
    dklen: u64,
    /// The CPU/memory cost, a power of two.
    n: u64,
    /// The parallelization.
    p: u64,
    /// The block size.
    r: u64,
    /// The salt, hex.
    salt: String,
}

/// The `pbkdf2` KDF parameters.
#[derive(Debug, Deserialize)]
struct Pbkdf2Params {
    /// The derived key length in bytes.
    dklen: u64,
    /// The iteration count.
    c: u64,
    /// The PRF; only `hmac-sha256` is supported.
    prf: String,
    /// The salt, hex.
    salt: String,
}

/// Derives the 32-byte key material from the password and the KDF
/// parameters: 16 bytes of AES-128 key then 16 bytes of MAC key.
fn derive_key(
    crypto: &CryptoSection,
    password: &str,
) -> Result<[u8; DERIVED_KEY_LEN], KeystoreError> {
    if crypto.kdf.eq_ignore_ascii_case("scrypt") {
        let params: ScryptParams =
            serde_json::from_value(crypto.kdfparams.clone()).map_err(|err| {
                KeystoreError::new(format!("the scrypt parameters are invalid: {err}"))
            })?;
        check_dklen(params.dklen)?;
        if params.n < 2 || !params.n.is_power_of_two() {
            return Err(KeystoreError::new(
                "the scrypt cost n must be a power of two greater than one",
            ));
        }
        let log_n = u8::try_from(params.n.trailing_zeros()).expect("log2 of a u64 fits a u8");
        let r = u32::try_from(params.r)
            .map_err(|_| KeystoreError::new("the scrypt parameter r is out of range"))?;
        let p = u32::try_from(params.p)
            .map_err(|_| KeystoreError::new("the scrypt parameter p is out of range"))?;
        let salt = decode_hex_field(&params.salt, "kdfparams.salt")?;
        let scrypt_params = scrypt::Params::new(log_n, r, p, DERIVED_KEY_LEN)
            .map_err(|_| KeystoreError::new("the scrypt parameters are invalid"))?;
        let mut derived = [0u8; DERIVED_KEY_LEN];
        scrypt::scrypt(password.as_bytes(), &salt, &scrypt_params, &mut derived)
            .map_err(|_| KeystoreError::new("the scrypt derivation failed"))?;
        Ok(derived)
    } else if crypto.kdf.eq_ignore_ascii_case("pbkdf2") {
        let params: Pbkdf2Params =
            serde_json::from_value(crypto.kdfparams.clone()).map_err(|err| {
                KeystoreError::new(format!("the pbkdf2 parameters are invalid: {err}"))
            })?;
        check_dklen(params.dklen)?;
        if !params.prf.eq_ignore_ascii_case("hmac-sha256") {
            return Err(KeystoreError::new(format!(
                "unsupported pbkdf2 prf {:?} (expected hmac-sha256)",
                params.prf
            )));
        }
        if params.c == 0 {
            return Err(KeystoreError::new("the pbkdf2 iteration count must be positive"));
        }
        let rounds = u32::try_from(params.c)
            .map_err(|_| KeystoreError::new("the pbkdf2 iteration count is out of range"))?;
        let salt = decode_hex_field(&params.salt, "kdfparams.salt")?;
        let mut derived = [0u8; DERIVED_KEY_LEN];
        pbkdf2::pbkdf2_hmac::<sha2::Sha256>(password.as_bytes(), &salt, rounds, &mut derived);
        Ok(derived)
    } else {
        Err(KeystoreError::new(format!(
            "unsupported keystore KDF {:?} (expected scrypt or pbkdf2)",
            crypto.kdf
        )))
    }
}

/// Requires the derived key length the MAC slicing and the AES-128 key
/// assume.
fn check_dklen(dklen: u64) -> Result<(), KeystoreError> {
    if dklen != DERIVED_KEY_LEN as u64 {
        return Err(KeystoreError::new(format!(
            "the derived key length must be {DERIVED_KEY_LEN}, got {dklen}"
        )));
    }
    Ok(())
}

/// Checks the optional `address` field of the file against the derived one.
fn check_claimed_address(claimed: Option<&str>, derived: Address) -> Result<(), KeystoreError> {
    let Some(claimed) = claimed else {
        return Ok(());
    };
    let digits =
        claimed.strip_prefix("0x").or_else(|| claimed.strip_prefix("0X")).unwrap_or(claimed);
    let bytes = decode_hex_field(digits, "address")?;
    if bytes.len() != derived.0.len() {
        return Err(KeystoreError::new(format!(
            "the keystore address must have {} hex digits, got {}",
            derived.0.len() * 2,
            digits.len()
        )));
    }
    if bytes != derived.0 {
        return Err(KeystoreError::new("the keystore address does not match the decrypted key"));
    }
    Ok(())
}

/// Decodes one hex field of the document.
fn decode_hex_field(text: &str, field: &str) -> Result<Vec<u8>, KeystoreError> {
    hex::decode(text)
        .map_err(|err| KeystoreError::new(format!("the {field} is not valid hex: {err}")))
}

/// The Ethereum address of an uncompressed SEC1 point: the last 20 bytes of
/// the Keccak-256 of the point without its `0x04` prefix.
fn address_of_point(point: &k256::EncodedPoint) -> Address {
    let hash = keccak256(&point.as_bytes()[1..]);
    Address(hash[12..].try_into().expect("20 bytes"))
}

/// Keccak-256 of the input.
fn keccak256(input: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(input);
    hasher.finalize().into()
}

/// An EVM signature: 65 bytes of `r (32) || s (32) || v (1)`, with `v` in
/// `{27, 28}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvmSignature(pub [u8; 65]);

impl Signature for EvmSignature {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, CryptoError> {
        let raw: [u8; 65] = bytes.try_into().map_err(|_| {
            CryptoError::MalformedSignature(format!("expected 65 bytes, got {}", bytes.len()))
        })?;
        if raw[64] != 27 && raw[64] != 28 {
            return Err(CryptoError::MalformedSignature(format!(
                "the recovery byte must be 27 or 28, got {}",
                raw[64]
            )));
        }
        k256::ecdsa::Signature::from_slice(&raw[..64])
            .map_err(|err| CryptoError::MalformedSignature(err.to_string()))?;
        Ok(Self(raw))
    }
}

/// A secp256k1 public key in SEC1 form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvmPublicKey(pub k256::ecdsa::VerifyingKey);

impl PublicKey for EvmPublicKey {
    type Signature = EvmSignature;

    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_encoded_point(false).as_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, CryptoError> {
        k256::ecdsa::VerifyingKey::from_sec1_bytes(bytes)
            .map(Self)
            .map_err(|err| CryptoError::MalformedPublicKey(err.to_string()))
    }

    /// Verifies over the 32-byte prehash (the EIP-712 digest).
    fn verify(&self, msg: &[u8], sig: &EvmSignature) -> Result<(), CryptoError> {
        let prehash: &[u8; 32] = msg.try_into().map_err(|_| {
            CryptoError::MalformedSignature(format!(
                "the EVM verifier expects a 32-byte prehash, got {} bytes",
                msg.len()
            ))
        })?;
        let signature = k256::ecdsa::Signature::from_slice(&sig.0[..64])
            .map_err(|err| CryptoError::MalformedSignature(err.to_string()))?;
        PrehashVerifier::verify_prehash(&self.0, prehash, &signature)
            .map_err(|_| CryptoError::InvalidSignature)
    }
}

/// The operator signer: a secp256k1 key that signs 32-byte EIP-712
/// prehashes. The `Debug` is manual: the key never prints.
#[derive(Clone)]
pub struct EvmSigner(pub k256::ecdsa::SigningKey);

impl From<&OperatorKey> for EvmSigner {
    fn from(key: &OperatorKey) -> Self {
        Self(k256::ecdsa::SigningKey::from(&key.secret))
    }
}

impl fmt::Debug for EvmSigner {
    /// Redacts the key.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EvmSigner").field("key", &"[redacted]").finish()
    }
}

impl Signer for EvmSigner {
    type Signature = EvmSignature;
    type PublicKey = EvmPublicKey;

    fn public_key(&self) -> EvmPublicKey {
        EvmPublicKey(*self.0.verifying_key())
    }

    /// Signs the 32-byte prehash `msg`; `v` comes out as `27` or `28`.
    fn sign(&self, msg: &[u8]) -> Result<EvmSignature, CryptoError> {
        let prehash: &[u8; 32] = msg.try_into().map_err(|_| {
            CryptoError::MalformedSignature(format!(
                "the EVM signer expects a 32-byte prehash, got {} bytes",
                msg.len()
            ))
        })?;
        let (signature, recovery_id) =
            self.0.sign_prehash_recoverable(prehash).map_err(|_| CryptoError::SigningFailed)?;
        let mut bytes = [0u8; 65];
        bytes[..64].copy_from_slice(&signature.to_bytes());
        bytes[64] = recovery_id.to_byte() + 27;
        Ok(EvmSignature(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cryptography::evm::{order_hash, verify_order};
    use primitives::base::{Hash32, Nonce, Side, Symbol};
    use primitives::order::{Order, OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use primitives::signature::Signature as OrderSignature;
    use primitives::time_in_force::TimeInForce;
    use primitives::value::{Price, Quantity, TimestampMs};
    use std::sync::atomic::{AtomicUsize, Ordering};

    const CHAIN_ID: u64 = 31_337;
    const CONTRACT: Address = Address([0xabu8; 20]);

    /// The canonical geth keystore vector: an empty password, scrypt with
    /// n = 2, r = 8, p = 1.
    fn geth_vector_json() -> String {
        serde_json::json!({
            "address": "45dea0fb0bba44f4fcf290bba71fd57d7117cbb8",
            "crypto": {
                "cipher": "aes-128-ctr",
                "cipherparams": { "iv": "dc4926b48a105133d2f16b96833abf1e" },
                "ciphertext": "b87781948a1befd247bff51ef4063f716cf6c2d3481163e9a8f42e1f9bb74145",
                "kdf": "scrypt",
                "kdfparams": {
                    "dklen": 32,
                    "n": 2,
                    "p": 1,
                    "r": 8,
                    "salt": "004244bbdc51cadda545b1cfa43cff9ed2ae88e08c61f1479dbb45410722f8f0",
                },
                "mac": "39990c1684557447940d4c69e06b1b82b2aceacb43f284df65c956daf3046b85",
            },
            "id": "3198bc9c-6672-5ab3-d995-4942343ae5b6",
            "version": 3,
        })
        .to_string()
    }

    /// A temporary keystore file, removed on drop.
    struct TempKeystore {
        path: std::path::PathBuf,
    }

    impl TempKeystore {
        /// Writes `text` into a fresh file under the system temp directory.
        fn write(text: &str) -> Self {
            static SEQ: AtomicUsize = AtomicUsize::new(0);
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "dex_stl_keystore_{}_{}.json",
                std::process::id(),
                seq
            ));
            std::fs::write(&path, text).expect("the temp keystore is writable");
            Self { path }
        }

        /// The file path.
        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempKeystore {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    /// Unlocks the canonical geth vector.
    fn geth_vector_key() -> OperatorKey {
        let file = TempKeystore::write(&geth_vector_json());
        OperatorKey::unlock(file.path(), "").expect("the vector unlocks with the empty password")
    }

    /// The Ethereum address of a raw secret scalar.
    fn address_of_secret(secret: &[u8; 32]) -> Address {
        let signing = k256::ecdsa::SigningKey::from_slice(secret).expect("a valid scalar");
        address_of_point(&signing.verifying_key().to_encoded_point(false))
    }

    /// Encrypts `secret` into a v3 document with pbkdf2-hmac-sha256 (1024
    /// rounds) and aes-128-ctr: the inverse of `unlock`, written
    /// independently of it.
    fn encrypt_pbkdf2(secret: &[u8; 32], password: &str, salt: &[u8], iv: &[u8; IV_LEN]) -> String {
        const ROUNDS: u32 = 1_024;
        let mut derived = [0u8; DERIVED_KEY_LEN];
        pbkdf2::pbkdf2_hmac::<sha2::Sha256>(password.as_bytes(), salt, ROUNDS, &mut derived);
        let mut ciphertext = secret.to_vec();
        let mut cipher = Aes128Ctr::new_from_slices(&derived[..AES_KEY_LEN], iv)
            .expect("the key and iv lengths are fixed");
        cipher.apply_keystream(&mut ciphertext);
        let mut hasher = Keccak256::new();
        hasher.update(&derived[AES_KEY_LEN..]);
        hasher.update(&ciphertext);
        let mac: [u8; MAC_LEN] = hasher.finalize().into();
        serde_json::json!({
            "address": address_of_secret(secret).hex(),
            "crypto": {
                "cipher": "aes-128-ctr",
                "cipherparams": { "iv": hex::encode(iv) },
                "ciphertext": hex::encode(&ciphertext),
                "kdf": "pbkdf2",
                "kdfparams": {
                    "dklen": 32,
                    "c": ROUNDS,
                    "prf": "hmac-sha256",
                    "salt": hex::encode(salt),
                },
                "mac": hex::encode(mac),
            },
            "version": 3,
        })
        .to_string()
    }

    /// A test order, built like the `cryptography::evm` tests build theirs.
    fn order(user: Address, nonce: u64, price: u64) -> Order {
        Order::new(
            OrderHot {
                user,
                nonce: Nonce(nonce),
                price: Price(price),
                quantity: Quantity(10),
                time_in_force: TimeInForce::Gtc,
                side: Side::Buy,
            },
            OrderCold::new(
                OrderColdCommon::new(
                    Hash32([0; 32]),
                    Symbol([1u8; 32]),
                    OrderSignature::default(),
                    TimestampMs(1_700_000_000_000),
                ),
                OrderKind::Standard,
            ),
        )
    }

    #[test]
    fn test_unlock_geth_scrypt_vector() {
        let key = geth_vector_key();
        assert_eq!(key.address().hex(), "45dea0fb0bba44f4fcf290bba71fd57d7117cbb8");
    }

    #[test]
    fn test_unlock_wrong_password_is_a_mac_error() {
        let file = TempKeystore::write(&geth_vector_json());
        let err = OperatorKey::unlock(file.path(), "not-the-password").unwrap_err();
        assert!(err.to_string().contains("MAC"), "{err}");
    }

    #[test]
    fn test_unlock_rejects_a_tampered_mac() {
        let tampered = geth_vector_json().replace(
            "39990c1684557447940d4c69e06b1b82b2aceacb43f284df65c956daf3046b85",
            "49990c1684557447940d4c69e06b1b82b2aceacb43f284df65c956daf3046b85",
        );
        let file = TempKeystore::write(&tampered);
        let err = OperatorKey::unlock(file.path(), "").unwrap_err();
        assert!(err.to_string().contains("MAC"), "{err}");
    }

    #[test]
    fn test_unlock_rejects_a_tampered_ciphertext() {
        let tampered = geth_vector_json().replace(
            "b87781948a1befd247bff51ef4063f716cf6c2d3481163e9a8f42e1f9bb74145",
            "b87781948a1befd247bff51ef4063f716cf6c2d3481163e9a8f42e1f9bb74146",
        );
        let file = TempKeystore::write(&tampered);
        assert!(OperatorKey::unlock(file.path(), "").is_err());
    }

    #[test]
    fn test_unlock_pbkdf2_roundtrip() {
        let secret = [7u8; 32];
        let json = encrypt_pbkdf2(&secret, "hunter2", &[9u8; 16], &[3u8; IV_LEN]);
        let file = TempKeystore::write(&json);
        let key = OperatorKey::unlock(file.path(), "hunter2").expect("the pbkdf2 keystore unlocks");
        assert_eq!(key.address(), address_of_secret(&secret));
        // The wrong password fails on the MAC, not on the parse.
        let err = OperatorKey::unlock(file.path(), "hunter3").unwrap_err();
        assert!(err.to_string().contains("MAC"), "{err}");
    }

    #[test]
    fn test_unlock_rejects_unsupported_documents() {
        // Version 2.
        let text = geth_vector_json().replace("\"version\":3", "\"version\":2");
        let file = TempKeystore::write(&text);
        assert!(OperatorKey::unlock(file.path(), "").unwrap_err().to_string().contains("version"));

        // An unsupported KDF.
        let text = geth_vector_json().replace("\"scrypt\"", "\"argon2\"");
        let file = TempKeystore::write(&text);
        assert!(OperatorKey::unlock(file.path(), "").unwrap_err().to_string().contains("KDF"));

        // A claimed address that is not the key's.
        let text = geth_vector_json().replace(
            "45dea0fb0bba44f4fcf290bba71fd57d7117cbb8",
            "0000000000000000000000000000000000000000",
        );
        let file = TempKeystore::write(&text);
        let err = OperatorKey::unlock(file.path(), "").unwrap_err();
        assert!(err.to_string().contains("address"), "{err}");
    }

    #[test]
    fn test_unlock_missing_file_reports_the_path() {
        let path = std::env::temp_dir().join("dex_stl_keystore_that_does_not_exist.json");
        let err = OperatorKey::unlock(&path, "").unwrap_err();
        assert!(err.to_string().contains("cannot be read"), "{err}");
    }

    #[test]
    fn test_password_from_env_missing_is_fatal() {
        let name = format!("SVD_SETTLEMENT_TEST_MISSING_{}", std::process::id());
        let err = password_from_env_named(&name).unwrap_err();
        assert!(err.to_string().contains("not set"), "{err}");
        assert!(err.to_string().contains(&name), "{err}");
    }

    #[test]
    fn test_debug_redacts_the_secret() {
        let key = geth_vector_key();
        let debug = format!("{key:?}");
        assert!(debug.contains("redacted"), "{debug}");
        assert!(debug.contains(&key.address().hex()), "{debug}");
        let secret_hex = hex::encode(key.secret.to_bytes());
        assert!(!debug.contains(&secret_hex), "{debug}");
        // The signer redacts too.
        let signer = EvmSigner::from(&key);
        assert!(format!("{signer:?}").contains("redacted"));
    }

    #[test]
    fn test_signer_signs_an_order_hash() {
        let key = geth_vector_key();
        let signer = EvmSigner::from(&key);
        let user = key.address();
        let mut signed = order(user, 1, 100);
        let hash = order_hash(&signed, CHAIN_ID, CONTRACT);
        let signature = signer.sign(&hash).expect("the prehash signs");
        signed.cold.common = OrderColdCommon::new(
            Hash32([0; 32]),
            Symbol([1u8; 32]),
            OrderSignature(signature.0),
            TimestampMs(1_700_000_000_000),
        );
        verify_order(&signed, CHAIN_ID, CONTRACT).expect("the order signature verifies");
        // A tampered order no longer matches the signature.
        signed.hot.price = Price(101);
        assert!(matches!(
            verify_order(&signed, CHAIN_ID, CONTRACT),
            Err(CryptoError::InvalidSignature)
        ));
    }

    #[test]
    fn test_signature_and_public_key_roundtrip() {
        let signer = EvmSigner::from(&geth_vector_key());
        let message = [0x42u8; 32];
        let signature = signer.sign(&message).expect("signs");
        assert!(signature.0[64] == 27 || signature.0[64] == 28);
        let parsed = EvmSignature::from_bytes(&signature.to_bytes()).expect("round-trips");
        assert_eq!(parsed, signature);
        signer.public_key().verify(&message, &signature).expect("the signature verifies");
        assert!(matches!(
            signer.public_key().verify(&[0x43u8; 32], &signature),
            Err(CryptoError::InvalidSignature)
        ));
        let public = signer.public_key();
        assert_eq!(EvmPublicKey::from_bytes(&public.to_bytes()).unwrap(), public);
        assert_eq!(public.to_bytes().len(), 65);
    }

    #[test]
    fn test_sign_and_verify_reject_malformed_input() {
        let signer = EvmSigner::from(&geth_vector_key());
        assert!(matches!(signer.sign(&[0u8; 31]), Err(CryptoError::MalformedSignature(_))));
        assert!(matches!(
            signer.public_key().verify(&[0u8; 33], &EvmSignature([0u8; 65])),
            Err(CryptoError::MalformedSignature(_))
        ));
        assert!(matches!(
            EvmSignature::from_bytes(&[0u8; 64]),
            Err(CryptoError::MalformedSignature(_))
        ));
        let mut bad_v = [0u8; 65];
        bad_v[64] = 29;
        assert!(matches!(
            EvmSignature::from_bytes(&bad_v),
            Err(CryptoError::MalformedSignature(_))
        ));
        assert!(matches!(
            EvmPublicKey::from_bytes(&[0u8; 3]),
            Err(CryptoError::MalformedPublicKey(_))
        ));
    }
}
