//! cryptography helpers for the workspace.

/// Errors from signing/verifying. Keep the cases distinct — a `bool` is a footgun.
#[derive(Debug)]
pub enum CryptoError {
    InvalidSignature,           // well-formed, but wrong
    MalformedSignature(String), // wrong length / non-canonical encoding
    MalformedPublicKey(String),
    SigningFailed,
}

/// A signature value. Knows how to serialize, nothing else.
pub trait Signature: Clone + Send + Sync + 'static {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Result<Self, CryptoError>
    where
        Self: Sized;
}

/// A public key. Verifies signatures *of its own scheme*.
pub trait PublicKey: Clone + Send + Sync + 'static {
    type Signature: Signature;

    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Result<Self, CryptoError>
    where
        Self: Sized;

    fn verify(&self, msg: &[u8], sig: &Self::Signature) -> Result<(), CryptoError>;
}

/// Anything that can produce a signature — not necessarily a raw private key.
pub trait Signer: Send + Sync + 'static {
    type Signature: Signature;
    type PublicKey: PublicKey<Signature = Self::Signature>;

    fn public_key(&self) -> Self::PublicKey;
    fn sign(&self, msg: &[u8]) -> Result<Self::Signature, CryptoError>;
}

/// A raw, exportable private key: a `Signer` that can also be serialized.
pub trait PrivateKey: Signer {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Result<Self, CryptoError>
    where
        Self: Sized;
}
