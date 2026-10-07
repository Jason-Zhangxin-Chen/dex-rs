//! The alloy-backed [`ChainClient`]: the concrete, live-chain implementation
//! of the settlement submission contract.
//!
//! The trait is synchronous — the submitter's loop owns its own clock and
//! must never await a node mid-batch — while every RPC here is asynchronous.
//! The adapter bridges the two with an owned [`tokio::runtime::Handle`]: each
//! trait method enters the runtime for one bounded round of RPC work.
//!
//! # The submission flow
//!
//! One batch maps to one transaction, and a retry of that batch reuses the
//! *same* nonce, so a node that already has the bytes answers "already
//! known" instead of accepting a duplicate:
//!
//! 1. reserve the nonce ([`NonceState::assign`]) and hold it until the
//!    submission reaches a terminal state;
//! 2. price the transaction (base fee from the latest block, tip from the
//!    node's suggestion, both through [`crate::chain::gas`]) and estimate its
//!    gas limit;
//! 3. sign locally — the transaction is sealed before it ever leaves the
//!    process, so a retry of an identical request is byte-identical;
//! 4. broadcast through the pool and record the nonce as in flight.
//!
//! A reservation is only released by an accepted broadcast or by the chain
//! itself: a transaction that never reached a node keeps its nonce, and the
//! next `submit` rebuilds the same bytes from the cached fees (step 2 reads
//! the cache first), so the retry cannot fork the account's nonce sequence —
//! and cannot form a gap that [`NonceState`] refuses to skip.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use alloy::consensus::{SignableTransaction, Transaction as _, TxEnvelope};
use alloy::eips::BlockId;
use alloy::network::TxSignerSync;
use alloy::primitives::{Address as EvmAddress, B256, Bytes};
use alloy::providers::Provider;
use alloy::rpc::types::{TransactionInput, TransactionRequest};
use alloy::signers::local::{LocalSigner, PrivateKeySigner};
use primitives::base::Hash32;
use tokio::time::timeout;

use crate::chain::abi::decode_settlement_error;
use crate::chain::gas;
use crate::chain::nonce::{NonceAction, NonceState};
use crate::chain::operator::{self, EvmSigner, OperatorKey};
use crate::chain::rpc_pool::RpcPool;
use crate::chain::{ChainClient, ChainError, SubmittedTx, TxState};
use crate::config::{ChainConfig, GasConfig};

/// The per-nonce fee record: the 1-based replacement attempt counter and the
/// fees that attempt compounds *from* (see [`gas::bump`]).
type FeeRecord = (u32, u128, u128);

/// The live-chain [`ChainClient`].
///
/// The type is `Send + Sync` and holds no lock across an await: the
/// submitter may call it from its own thread while the OMS side keeps
/// producing batches.
pub struct AlloyChainClient {
    /// The runtime the synchronous trait methods enter for their RPC work.
    rt: tokio::runtime::Handle,
    /// The failover pool of RPC nodes.
    pool: RpcPool,
    /// The nonce bookkeeping of the operator account.
    nonce: Mutex<NonceState>,
    /// The nonce reserved by a submission whose broadcast never returned,
    /// if any. It is handed out again — with the identical transaction — so
    /// the assignment cannot strand the account.
    reserved: Mutex<Option<u64>>,
    /// The gas strategy (a `Copy` config; kept for the price/bump asks).
    gas_config: GasConfig,
    /// The fee record of every nonce, for the replacement math.
    fees: Mutex<HashMap<u64, FeeRecord>>,
    /// The operator key, already unlocked and decoded.
    signer: PrivateKeySigner,
    /// The settlement contract.
    contract: EvmAddress,
    /// The chain id the config declares (the pool's nodes are checked
    /// against it at construction).
    chain_id: u64,
    /// The ceiling on one whole broadcast, failover included.
    submit_timeout: Duration,
}

impl AlloyChainClient {
    /// Builds the client: unlocks the operator keystore, builds the pool,
    /// verifies that the node's chain id matches the configured one, and
    /// starts the nonce cursor at the operator's chain transaction count.
    ///
    /// The client takes a [`tokio::runtime::Handle`] rather than creating a
    /// runtime: the service owns its runtime, and the settlement loop must
    /// not grow a second one. The handle must therefore belong to a runtime
    /// other than the calling thread's — [`tokio::runtime::Handle::block_on`]
    /// panics inside a runtime worker.
    ///
    /// The initial count is read with `pending`, the widest view: a
    /// transaction of a previous run that is still in a node's mempool has
    /// consumed its nonce for real, and handing it out again would make the
    /// node reject the batch as a replacement.
    pub fn new(config: &ChainConfig, rt: tokio::runtime::Handle) -> Result<Self, ChainError> {
        let password = operator::password_from_env()
            .map_err(|err| ChainError::Keystore { detail: err.to_string() })?;
        let operator_key = OperatorKey::unlock(Path::new(&config.keystore_path), &password)
            .map_err(|err| ChainError::Keystore { detail: err.to_string() })?;
        let signer = LocalSigner::from_signing_key(EvmSigner::from(&operator_key).0);
        let pool = RpcPool::build(&config.rpc_pool)?;

        let chain_id = rt.block_on(pool.chain_id())?;
        if chain_id != config.chain_id {
            return Err(ChainError::Config {
                detail: format!(
                    "the RPC node reports chain id {chain_id}, the settlement config declares {}",
                    config.chain_id
                ),
            });
        }

        let operator = signer.address();
        let count = rt.block_on(pool.with_active(move |node| {
            Box::pin(async move {
                node.provider
                    .get_transaction_count(operator)
                    .block_id(BlockId::pending())
                    .await
                    .map_err(|err| node.transport_error(err))
            })
        }))?;

        Ok(Self {
            rt,
            pool,
            nonce: Mutex::new(NonceState::new(count)),
            reserved: Mutex::new(None),
            gas_config: config.gas,
            fees: Mutex::new(HashMap::new()),
            signer,
            contract: EvmAddress::from(config.settlement_contract.0),
            chain_id: config.chain_id,
            submit_timeout: Duration::from_millis(config.submit_timeout_ms),
        })
    }

    /// The operator address, i.e. the account the batches are sent from.
    pub fn operator(&self) -> EvmAddress {
        self.signer.address()
    }

    /// The chain id the client was built against.
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// The number of currently reserved-or-unconfirmed nonces, for
    /// observability.
    pub fn pending_nonce(&self) -> Option<u64> {
        self.lock_nonce().lowest_pending()
    }

    /// The transaction request skeleton of one batch: the settlement
    /// contract, the calldata, and the operator as the sender.
    fn request(&self, calldata: &[u8]) -> TransactionRequest {
        TransactionRequest::default()
            .to(self.contract)
            .from(self.signer.address())
            .input(TransactionInput::new(Bytes::copy_from_slice(calldata)))
    }

    /// Seals a request into a signed envelope.
    fn seal(&self, request: TransactionRequest) -> Result<TxEnvelope, ChainError> {
        seal_with(&self.signer, request)
    }

    /// Reserves the nonce of a submission, reusing the reservation of a
    /// broadcast that never came back.
    fn reserve_nonce(&self) -> Result<u64, ChainError> {
        let mut reserved = self.lock_reserved();
        if let Some(nonce) = *reserved {
            return Ok(nonce);
        }
        match self.lock_nonce().assign() {
            NonceAction::Assign(nonce) => {
                *reserved = Some(nonce);
                Ok(nonce)
            }
            NonceAction::Blocked { expected, lowest_pending } => {
                Err(ChainError::NonceGap { expected, lowest_pending })
            }
        }
    }

    /// The price `(max_fee_per_gas, max_priority_fee_per_gas)` of a fresh
    /// submission: the base fee of the latest block with the configured
    /// headroom, and the node's suggested tip capped by the config.
    async fn suggested_fees(&self) -> Result<(u128, u128), ChainError> {
        let gas_config = self.gas_config;
        self.pool
            .with_active(move |node| {
                Box::pin(async move {
                    let suggestion = node
                        .provider
                        .estimate_eip1559_fees()
                        .await
                        .map_err(|err| node.transport_error(err))?;
                    let block = node
                        .provider
                        .get_block(BlockId::latest())
                        .await
                        .map_err(|err| node.transport_error(err))?
                        .ok_or_else(|| ChainError::Transport {
                            node: node.name.clone(),
                            detail: "the node reports no latest block".to_string(),
                        })?;
                    let base_fee = u128::from(block.header.base_fee_per_gas.unwrap_or_default());
                    Ok(gas::price_eip1559(
                        base_fee,
                        suggestion.max_priority_fee_per_gas,
                        &gas_config,
                    ))
                })
            })
            .await
    }

    /// The gas limit of one batch: the node's estimate with the configured
    /// buffer and cap.
    ///
    /// A node that *answers* with an error — a revert, an out-of-gas, an
    /// insufficient balance — is a [`ChainError::GasEstimate`] and no ground
    /// for failover: every node would answer the same. Only an unanswered
    /// call is a [`ChainError::Transport`], which the pool rotates on.
    async fn estimate_gas_limit(&self, calldata: &[u8]) -> Result<u64, ChainError> {
        let request = self.request(calldata);
        let gas_config = self.gas_config;
        let estimate = self
            .pool
            .with_active(move |node| {
                let request = request.clone();
                Box::pin(async move {
                    node.provider.estimate_gas(request).await.map_err(|err| {
                        if let Some(payload) = err.as_error_resp() {
                            ChainError::GasEstimate {
                                detail: format!("{}: {}", node.name, payload.message),
                            }
                        } else {
                            node.transport_error(err)
                        }
                    })
                })
            })
            .await?;
        Ok(gas::gas_limit(estimate, &gas_config))
    }

    /// The fee record of a nonce, if one was recorded.
    fn fee_record(&self, nonce: u64) -> Option<FeeRecord> {
        self.lock_fees().get(&nonce).copied()
    }

    /// Signs `request` and sends it through the pool within the submission
    /// deadline.
    async fn broadcast(
        &self,
        request: TransactionRequest,
        nonce: u64,
    ) -> Result<Hash32, ChainError> {
        let envelope = self.seal(request)?;
        let timeout_ms = self.submit_timeout.as_millis() as u64;
        let hash = timeout(self.submit_timeout, self.pool.broadcast(&envelope)).await.map_err(
            |_elapsed| ChainError::NodeStalled {
                node: self.pool.active_name().to_string(),
                timeout_ms,
            },
        )??;
        self.lock_nonce().on_submitted(nonce, hash);
        *self.lock_reserved() = None;
        Ok(hash)
    }

    /// The synopsis of a submission: the transaction bytes, priced and
    /// sealed, plus the nonce they carry.
    async fn prepare(&self, calldata: &[u8], nonce: u64) -> Result<TransactionRequest, ChainError> {
        // A retry of a reservation whose broadcast was lost must rebuild the
        // *same* bytes, or the node would answer a different envelope at the
        // same nonce as a replacement. The cached fees are what makes that
        // possible; a nonce the process has never priced asks the node.
        let (max_fee, tip) = match self.fee_record(nonce) {
            Some((_, max_fee, tip)) => (max_fee, tip),
            None => self.suggested_fees().await?,
        };
        let gas_limit = self.estimate_gas_limit(calldata).await?;
        self.lock_fees()
            .insert(nonce, (self.fee_record(nonce).map_or(0, |record| record.0), max_fee, tip));
        let mut request = self
            .request(calldata)
            .nonce(nonce)
            .gas_limit(gas_limit)
            .max_fee_per_gas(max_fee)
            .max_priority_fee_per_gas(tip);
        request.chain_id = Some(self.chain_id);
        Ok(request)
    }

    /// Submits one batch of `calldata` as one transaction.
    async fn submit_async(&self, calldata: &[u8]) -> Result<SubmittedTx, ChainError> {
        let nonce = self.reserve_nonce()?;
        let request = self.prepare(calldata, nonce).await?;
        let hash = self.broadcast(request, nonce).await?;
        Ok(SubmittedTx { tx_hash: hash, nonce })
    }

    /// Replaces the transaction of `nonce` with a re-priced copy of the same
    /// batch.
    ///
    /// The nonce is *not* re-reserved: a replacement races the original, so
    /// it must carry the identical nonce and the same calldata, only at a
    /// higher price. The attempt counter compounds from the fees the
    /// submission was priced at, so a run of replacements grows
    /// geometrically instead of adding a fixed step each time.
    async fn bump_fee_async(&self, calldata: &[u8], nonce: u64) -> Result<SubmittedTx, ChainError> {
        let (attempt, prev_max_fee, prev_tip) = match self.fee_record(nonce) {
            Some(record) => record,
            // A replacement of a nonce this process never priced (a resumed
            // run): re-read the chain and treat it as the base.
            None => {
                let (max_fee, tip) = self.suggested_fees().await?;
                self.lock_fees().insert(nonce, (0, max_fee, tip));
                self.fee_record(nonce).expect("the record was just inserted")
            }
        };
        let attempt = attempt.saturating_add(1);
        let (max_fee, tip) = gas::bump(prev_max_fee, prev_tip, attempt, &self.gas_config);
        self.lock_fees().insert(nonce, (attempt, prev_max_fee, prev_tip));

        let gas_limit = self.estimate_gas_limit(calldata).await?;
        let mut request = self
            .request(calldata)
            .nonce(nonce)
            .gas_limit(gas_limit)
            .max_fee_per_gas(max_fee)
            .max_priority_fee_per_gas(tip);
        request.chain_id = Some(self.chain_id);

        let hash = self.broadcast(request, nonce).await?;
        self.lock_nonce().on_replaced(nonce, hash);
        Ok(SubmittedTx { tx_hash: hash, nonce })
    }

    /// The receipt of `hash`, if it is already mined.
    async fn receipt(
        &self,
        hash: B256,
    ) -> Result<Option<alloy::rpc::types::TransactionReceipt>, ChainError> {
        self.pool
            .with_active(move |node| {
                Box::pin(async move {
                    node.provider
                        .get_transaction_receipt(hash)
                        .await
                        .map_err(|err| node.transport_error(err))
                })
            })
            .await
    }

    /// The head block number.
    async fn block_number(&self) -> Result<u64, ChainError> {
        self.pool
            .with_active(|node| {
                Box::pin(async move {
                    node.provider.get_block_number().await.map_err(|err| node.transport_error(err))
                })
            })
            .await
    }

    /// The transaction of `hash`, if a node still remembers it.
    async fn transaction(
        &self,
        hash: B256,
    ) -> Result<Option<alloy::rpc::types::Transaction>, ChainError> {
        self.pool
            .with_active(move |node| {
                Box::pin(async move {
                    node.provider
                        .get_transaction_by_hash(hash)
                        .await
                        .map_err(|err| node.transport_error(err))
                })
            })
            .await
    }

    /// The failure of a reverted batch: `(code, index, side)`.
    ///
    /// A receipt carries no revert bytes, so the transaction is replayed with
    /// `eth_call` against the block it was mined in — the state the failure
    /// was raised in — and the payload is decoded. Anything that does not
    /// decode into a protocol failure, and a replay that unexpectedly
    /// succeeds, is the `(0, 0, 0)` "undecodable" sentinel the classifier
    /// maps to a human. A replay that cannot be *run* at all is a
    /// [`ChainError::RevertDecode`]: retryable, and honest about the fact
    /// that the batch reverted for a reason we do not know.
    async fn revert_outcome(
        &self,
        hash: B256,
        block: Option<u64>,
    ) -> Result<(u8, usize, u8), ChainError> {
        let Some(transaction) = self.transaction(hash).await? else {
            return Ok((0, 0, 0));
        };
        let input = transaction.input().clone();
        let request = self.request(&input).gas_limit(self.gas_config.gas_limit_cap);
        let block_id = block.map_or(BlockId::latest(), BlockId::number);
        let revert: Option<Bytes> = self
            .pool
            .with_active(move |node| {
                let request = request.clone();
                Box::pin(async move {
                    match node.provider.call(request).block(block_id).await {
                        // The replay succeeded — the state moved on. There is
                        // no reason to decode.
                        Ok(_) => Ok(None),
                        Err(err) => match err.as_error_resp() {
                            Some(payload) => Ok(payload.as_revert_data()),
                            None => Err(node.transport_error(err)),
                        },
                    }
                })
            })
            .await
            .map_err(|err| ChainError::RevertDecode {
                detail: format!("the revert of {hash} cannot be replayed: {err}"),
            })?;
        Ok(revert.map_or((0, 0, 0), |data| decode_settlement_error(&data).unwrap_or((0, 0, 0))))
    }

    /// Folds the chain's transaction count back into the nonce bookkeeping.
    async fn resync_nonce(&self) -> Result<(), ChainError> {
        let operator = self.signer.address();
        let count = self
            .pool
            .with_active(move |node| {
                Box::pin(async move {
                    node.provider
                        .get_transaction_count(operator)
                        .block_id(BlockId::latest())
                        .await
                        .map_err(|err| node.transport_error(err))
                })
            })
            .await?;
        let mut nonce = self.lock_nonce();
        let vanished = nonce.resync(count);
        drop(nonce);
        // A reservation below the count is a transaction that mined after
        // all: its submission may still be retried, but not at that nonce.
        let mut reserved = self.lock_reserved();
        if reserved.is_some_and(|reserved| reserved < count || vanished.contains(&reserved)) {
            *reserved = None;
        }
        Ok(())
    }

    /// The outcome of one transaction hash, as the submitter sees it.
    async fn tx_state_async(&self, tx: Hash32) -> Result<TxState, ChainError> {
        let hash = B256::from(tx.0);
        let Some(receipt) = self.receipt(hash).await? else {
            return Ok(TxState::Pending);
        };
        let block = receipt.block_number.unwrap_or_default();
        // A mined transaction consumes its nonce whichever way it went;
        // folding the count back in is what lets the next batch through the
        // gap rule.
        self.resync_nonce().await?;
        if receipt.status() {
            let head = self.block_number().await?;
            return Ok(TxState::Confirmed { block, depth: head.saturating_sub(block) + 1 });
        }
        let (code, index, side) = self.revert_outcome(hash, receipt.block_number).await?;
        Ok(TxState::Reverted { code, index, side })
    }

    /// The nonce bookkeeping, poisoned or not: the state machine is a plain
    /// value with no invariant a panic could break half-way.
    fn lock_nonce(&self) -> MutexGuard<'_, NonceState> {
        self.nonce.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// The reserved-nonce slot.
    fn lock_reserved(&self) -> MutexGuard<'_, Option<u64>> {
        self.reserved.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// The fee records.
    fn lock_fees(&self) -> MutexGuard<'_, HashMap<u64, FeeRecord>> {
        self.fees.lock().unwrap_or_else(|err| err.into_inner())
    }
}

impl ChainClient for AlloyChainClient {
    fn submit(&self, calldata: &[u8]) -> Result<SubmittedTx, ChainError> {
        self.rt.block_on(self.submit_async(calldata))
    }

    fn tx_state(&self, tx: Hash32) -> Result<TxState, ChainError> {
        self.rt.block_on(self.tx_state_async(tx))
    }

    fn bump_fee(&self, calldata: &[u8], nonce: u64) -> Result<SubmittedTx, ChainError> {
        self.rt.block_on(self.bump_fee_async(calldata, nonce))
    }
}

/// Signs one EIP-1559 request into a broadcastable envelope.
///
/// The request must be complete by the time it gets here: every field the
/// node would otherwise fill is priced locally, so the signed bytes are a
/// deterministic function of the request — which is what lets a retry of a
/// lost broadcast be recognized by the node as the same transaction.
fn seal_with(
    signer: &PrivateKeySigner,
    request: TransactionRequest,
) -> Result<TxEnvelope, ChainError> {
    let mut transaction = request.build_1559().map_err(|err| ChainError::Encode {
        detail: format!("the EIP-1559 transaction cannot be built: {err}"),
    })?;
    let signature =
        signer.sign_transaction_sync(&mut transaction).map_err(|err| ChainError::Keystore {
            detail: format!("the operator key cannot sign the settlement batch: {err}"),
        })?;
    Ok(TxEnvelope::from(transaction.into_signed(signature)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::TxKind;

    /// A fixed operator key, so the tests are reproducible.
    fn signer() -> PrivateKeySigner {
        LocalSigner::from_signing_key(
            k256::ecdsa::SigningKey::from_slice(&[0x11; 32])
                .expect("the fixed test key is a valid secp256k1 scalar"),
        )
    }

    /// A request like the one a submission builds.
    fn request() -> TransactionRequest {
        let mut request = TransactionRequest::default()
            .to(EvmAddress::from([0xab; 20]))
            .from(signer().address())
            .input(TransactionInput::new(Bytes::from(vec![0xde, 0xad, 0xbe, 0xef])))
            .nonce(7)
            .gas_limit(500_000)
            .max_fee_per_gas(3_000_000_000)
            .max_priority_fee_per_gas(1_000_000_000);
        request.chain_id = Some(31_337);
        request
    }

    #[test]
    fn test_seal_carries_every_field_and_is_deterministic() {
        let first = seal_with(&signer(), request()).expect("the request seals");
        let second = seal_with(&signer(), request()).expect("the request seals");
        // Determinism is load bearing: a retry must be the same bytes, or
        // the node reads it as a replacement at the same nonce.
        assert_eq!(first.tx_hash(), second.tx_hash());

        match &first {
            TxEnvelope::Eip1559(signed) => {
                assert_eq!(signed.tx().nonce, 7);
                assert_eq!(signed.tx().chain_id, 31_337);
                assert_eq!(signed.tx().gas_limit, 500_000);
                assert_eq!(signed.tx().max_fee_per_gas, 3_000_000_000);
                assert_eq!(signed.tx().max_priority_fee_per_gas, 1_000_000_000);
                assert_eq!(signed.tx().to, TxKind::Call(EvmAddress::from([0xab; 20])));
                assert_eq!(signed.tx().input.as_ref(), &[0xde, 0xad, 0xbe, 0xef]);
                // The signature is the operator's, and recoverable from the
                // transaction's own signing hash.
                let recovered = signed
                    .signature()
                    .recover_address_from_prehash(&signed.tx().signature_hash())
                    .expect("the signature recovers");
                assert_eq!(recovered, signer().address());
            }
            other => panic!("a settlement batch is an EIP-1559 transaction, got {other:?}"),
        }
    }

    #[test]
    fn test_seal_of_a_different_nonce_is_a_different_transaction() {
        let first = seal_with(&signer(), request()).expect("the request seals");
        let mut other = request();
        other.nonce = Some(8);
        let second = seal_with(&signer(), other).expect("the request seals");
        assert_ne!(first.tx_hash(), second.tx_hash());
    }

    #[test]
    fn test_seal_reports_an_incomplete_request_as_an_encode_error() {
        let mut incomplete = request();
        incomplete.max_fee_per_gas = None;
        let err = seal_with(&signer(), incomplete).expect_err("a missing fee cannot be sealed");
        assert!(matches!(err, ChainError::Encode { .. }), "{err:?}");
    }
}
