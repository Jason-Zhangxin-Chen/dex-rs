//! The live-chain smoke test of the alloy settlement layer.
//!
//! Ignored by default: it dials a real RPC node and spends real gas, so it
//! cannot run in CI. It is the only test that exercises the whole path —
//! keystore, pool, pricing, signing, broadcast, receipt polling — against a
//! node that actually validates the transaction.
//!
//! Run it against a development chain (anvil, or a testnet with a funded
//! operator) with:
//!
//! ```text
//! SVD_SETTLEMENT_TEST_RPC=http://127.0.0.1:8545 \
//! SVD_SETTLEMENT_TEST_CHAIN_ID=31337 \
//! SVD_SETTLEMENT_TEST_KEYSTORE=/path/to/operator.keystore \
//! SVD_SETTLEMENT_KEYSTORE_PASSWORD=... \
//! cargo test -p svd-settlement --test chain_live -- --ignored
//! ```
//!
//! Without `SVD_SETTLEMENT_TEST_RPC` (and the keystore password) the test
//! reports success without touching anything: a live test that cannot
//! distinguish "not configured" from "broken" is worse than none, but so is
//! one that fails a checkout that simply has no node.

use std::time::{Duration, Instant};

use primitives::address::Address;
use primitives::base::{Hash32, Nonce, Side, Symbol};
use primitives::message::hot_path::Trade;
use primitives::order::{Order, OrderCold, OrderColdCommon, OrderHot, OrderKind};
use primitives::signature::Signature;
use primitives::time_in_force::TimeInForce;
use primitives::value::{Price, Quantity, TimestampMs};
use svd_settlement::calldata;
use svd_settlement::chain::alloy_impl::AlloyChainClient;
use svd_settlement::chain::{ChainClient, TxState};
use svd_settlement::config::{ChainConfig, GasConfig, RpcNodeConfig, RpcPoolConfig};

/// The RPC endpoint of the test node. Unset means "skip".
const RPC_ENV: &str = "SVD_SETTLEMENT_TEST_RPC";
/// The chain id of the test node.
const CHAIN_ID_ENV: &str = "SVD_SETTLEMENT_TEST_CHAIN_ID";
/// The settlement contract to call.
const CONTRACT_ENV: &str = "SVD_SETTLEMENT_TEST_CONTRACT";
/// The operator keystore to sign with.
const KEYSTORE_ENV: &str = "SVD_SETTLEMENT_TEST_KEYSTORE";
/// The password of the operator keystore (the client reads it from the
/// environment itself).
const PASSWORD_ENV: &str = "SVD_SETTLEMENT_KEYSTORE_PASSWORD";

/// The deadline for the submitted transaction to reach a terminal state.
const CONFIRMATION_DEADLINE: Duration = Duration::from_secs(120);

/// The poll interval of the receipt watch.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// One order of the test batch: a standard order signed by `user`.
fn order(user: u8, nonce: u64, side: Side) -> Order {
    Order::new(
        OrderHot {
            user: Address([user; 20]),
            nonce: Nonce(nonce),
            price: Price(1_000),
            quantity: Quantity(100),
            time_in_force: TimeInForce::Gtc,
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

/// The batch the live test settles: one cross of two orders.
fn batch() -> Vec<Trade> {
    vec![Trade::new(
        order(1, 7, Side::Buy),
        Quantity(150),
        order(2, 8, Side::Sell),
        Price(1_000),
        Quantity(100),
    )]
}

/// The client configuration of the test run.
///
/// The test drives a development chain, so the gas strategy is the
/// conservative default-like one rather than a tuning: a 2 gwei tip cap, a
/// 15 percent replacement bump, 20 bps of base-fee headroom, and a 30M gas
/// ceiling.
fn config(url: &str, chain_id: u64, contract: Address, keystore: &str) -> ChainConfig {
    ChainConfig {
        chain_id,
        settlement_contract: contract,
        confirmations: 1,
        keystore_path: keystore.to_string(),
        submit_timeout_ms: 30_000,
        rpc_pool: RpcPoolConfig {
            primary: RpcNodeConfig {
                name: "live".to_string(),
                url: url.to_string(),
                connect_timeout_ms: 5_000,
                rpc_timeout_ms: 30_000,
                stall_timeout_ms: 60_000,
            },
            secondaries: Vec::new(),
        },
        gas: GasConfig {
            max_priority_fee_gwei: 2,
            bump_pct: 15,
            base_fee_tolerance_bps: 20,
            gas_limit_cap: 30_000_000,
            gas_buffer_pct: 10,
        },
    }
}

/// Parses a `0x`-prefixed address.
fn parse_address(text: &str) -> Address {
    let hex = text.strip_prefix("0x").unwrap_or(text);
    let mut bytes = [0u8; 20];
    let decoded = hex::decode(hex).expect("the address is hex");
    assert_eq!(decoded.len(), 20, "an address is 20 bytes");
    bytes.copy_from_slice(&decoded);
    Address(bytes)
}

#[test]
#[ignore = "requires a live RPC node; set SVD_SETTLEMENT_TEST_RPC and SVD_SETTLEMENT_KEYSTORE_PASSWORD"]
fn test_submit_settle_batch_on_a_live_node() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(url) = std::env::var(RPC_ENV) else {
        // Not configured: nothing to smoke-test.
        return Ok(());
    };
    if std::env::var(PASSWORD_ENV).is_err() {
        return Ok(());
    }
    let chain_id = std::env::var(CHAIN_ID_ENV).map_or(Ok(31_337u64), |value| value.parse())?;
    let contract = std::env::var(CONTRACT_ENV)
        .map_or_else(|_| Address([0u8; 20]), |value| parse_address(&value));
    let keystore = std::env::var(KEYSTORE_ENV).unwrap_or_else(|_| "operator.keystore".to_string());

    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let client = AlloyChainClient::new(
        &config(&url, chain_id, contract, &keystore),
        runtime.handle().clone(),
    )?;
    assert_eq!(client.chain_id(), chain_id);
    assert_eq!(client.pending_nonce(), None, "a fresh client has nothing in flight");

    // The dependency-free encoder is what the submitter actually sends, so
    // the live test submits exactly those bytes.
    let trades = batch();
    let bytes = calldata::encode_settle_batch(&trades)?;
    let submitted = client.submit(&bytes)?;

    // First look: the node may not have mined it yet, but it must know it.
    let deadline = Instant::now() + CONFIRMATION_DEADLINE;
    loop {
        match client.tx_state(submitted.tx_hash)? {
            TxState::Pending if Instant::now() < deadline => std::thread::sleep(POLL_INTERVAL),
            TxState::Pending => {
                panic!(
                    "the transaction 0x{} never reached a terminal state",
                    hex::encode(submitted.tx_hash.0)
                )
            }
            TxState::Confirmed { depth, .. } => {
                assert!(depth >= 1, "a mined transaction is at least one block deep");
                break;
            }
            TxState::Reverted { code, index, side } => {
                // A revert is a legitimate outcome of a settlement attempt —
                // the batch is reported, not retried, and the test's job was
                // to get an answer from the chain.
                panic!("the batch reverted: code {code}, trade {index}, side {side}");
            }
        }
    }

    // The nonce was consumed and folded in: the client can submit again.
    assert_eq!(client.pending_nonce(), None, "a confirmed submission leaves nothing in flight");
    Ok(())
}
