//! The configuration of the settlement service.
//!
//! The config is loaded from a TOML file and can be reloaded at runtime via
//! SIGHUP. The reload swaps the active config, but every running thread
//! snapshots its parameters at launch: the symbol, the queues, the submitter
//! pool and the chain parameters take effect on the next restart.

use std::path::Path;

use primitives::address::Address;
use primitives::base::Symbol;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use storage::RedisConfig;

/// Default batch size of the core drain loop.
const DEFAULT_BATCH_SIZE: usize = 1024;
/// Default base of the retry backoff, in milliseconds.
const DEFAULT_RETRY_BASE_MS: u64 = 1_000;
/// Default cap of the retry backoff, in milliseconds.
const DEFAULT_RETRY_MAX_MS: u64 = 60_000;
/// Default jitter of the retry backoff, in percent.
const DEFAULT_RETRY_JITTER_PCT: u32 = 20;
/// Default number of tx-level failures before a fee-bump replacement.
const DEFAULT_FEE_BUMP_AFTER: u32 = 3;
/// Default cadence of the pending transaction polls, in milliseconds.
const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;
/// Default grace before a `Submitting` transaction never seen by the chain
/// is re-submitted, in milliseconds.
const DEFAULT_TX_LOST_GRACE_MS: u64 = 60_000;
/// Default confirmation depth of a settled transaction.
const DEFAULT_CONFIRMATIONS: u64 = 1;
/// Default budget of one submission acknowledgement, in milliseconds.
const DEFAULT_SUBMIT_TIMEOUT_MS: u64 = 30_000;
/// Default connection timeout of one RPC node, in milliseconds.
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 2_000;
/// Default per-call watchdog of one RPC node, in milliseconds.
const DEFAULT_RPC_TIMEOUT_MS: u64 = 5_000;
/// Default stall window of one RPC node, in milliseconds.
const DEFAULT_STALL_TIMEOUT_MS: u64 = 10_000;
/// Default cap of the priority fee, in gwei.
const DEFAULT_MAX_PRIORITY_FEE_GWEI: u128 = 2;
/// Default per-retry fee bump, in percent.
const DEFAULT_BUMP_PCT: u16 = 15;
/// Default headroom over the current base fee, in basis points.
const DEFAULT_BASE_FEE_TOLERANCE_BPS: u16 = 20;
/// Default ceiling of the gas limit.
const DEFAULT_GAS_LIMIT_CAP: u64 = 30_000_000;
/// Default headroom over the gas estimate, in percent.
const DEFAULT_GAS_BUFFER_PCT: u16 = 10;

/// Errors of the config loading.
#[derive(Debug)]
pub enum ConfigError {
    /// The file could not be read.
    Io(std::io::Error),
    /// The file is not a valid settlement config.
    Toml(toml::de::Error),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Io(err) => write!(f, "config io: {err}"),
            ConfigError::Toml(err) => write!(f, "config parse: {err}"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::Io(err) => Some(err),
            ConfigError::Toml(err) => Some(err),
        }
    }
}

impl From<std::io::Error> for ConfigError {
    fn from(err: std::io::Error) -> Self {
        ConfigError::Io(err)
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(err: toml::de::Error) -> Self {
        ConfigError::Toml(err)
    }
}

/// The settlement config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettlementConfig {
    /// The symbol of the market this settlement serves.
    #[serde(with = "symbol_serde")]
    pub symbol: Symbol,
    /// The core the hot-path thread pins to.
    #[serde(default)]
    pub core_id: Option<usize>,
    /// The expected bound of one batch (the crosses of one taker order),
    /// used by the startup check of the submitter queue capacities: every
    /// submitter queue must hold a frame of this many trades. A larger
    /// group is caught at runtime (the core stops with the trades unacked).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// The trade SPSC queue wired from the [SVD_OMS_Master]. This process
    /// owns the queue file: it creates it, the OMS attaches to it.
    pub trade: SpScConfig,
    /// The persistent batch-sequence counter of the core thread.
    pub core: CoreConfig,
    /// The retry and monitoring policy of the submitters.
    #[serde(default)]
    pub retry: RetryConfig,
    /// The cadence of the pending transaction polls, in milliseconds.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// The grace before a submitted-but-unseen transaction is re-submitted,
    /// in milliseconds.
    #[serde(default = "default_tx_lost_grace_ms")]
    pub tx_lost_grace_ms: u64,
    /// The Redis cluster the results publish to.
    #[serde(default)]
    pub redis: RedisConfig,
    /// The SQL cluster the settled trades write to.
    #[serde(default)]
    pub sql: SqlConfig,
    /// The shared chain facts: the protocol-level parameters.
    pub chain: SharedChainConfig,
    /// The submitter pool: one submitter per operator key, each with its
    /// own batch queue and chain resources.
    pub submitters: Vec<SubmitterConfig>,
}

impl SettlementConfig {
    /// Loads the config from a TOML file.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path)?;
        Self::from_toml(&text)
    }

    /// Parses the config from TOML text.
    pub fn from_toml(text: &str) -> Result<Self, ConfigError> {
        Ok(toml::from_str(text)?)
    }
}

/// The share memory SPSC queue of one pipeline hop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpScConfig {
    /// The file path of the queue.
    pub path: String,
    /// The queue capacity (at least 2; one slot stays free).
    pub capacity: usize,
    /// Whether this process initializes the queue's file on open.
    #[serde(default = "default_true")]
    pub create: bool,
}

/// The persistent batch-sequence counter of the core thread: a 4 KiB
/// memory-mapped file, created when missing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreConfig {
    /// The file path of the sequence counter.
    pub seq_path: String,
}

/// The retry policy of a submitter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    /// The base of the exponential backoff, in milliseconds.
    #[serde(default = "default_retry_base_ms")]
    pub base_ms: u64,
    /// The cap of the exponential backoff, in milliseconds.
    #[serde(default = "default_retry_max_ms")]
    pub max_ms: u64,
    /// The jitter of the backoff, in percent.
    #[serde(default = "default_retry_jitter_pct")]
    pub jitter_pct: u32,
    /// The number of tx-level failures before a fee-bump replacement.
    #[serde(default = "default_fee_bump_after")]
    pub fee_bump_after: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            base_ms: DEFAULT_RETRY_BASE_MS,
            max_ms: DEFAULT_RETRY_MAX_MS,
            jitter_pct: DEFAULT_RETRY_JITTER_PCT,
            fee_bump_after: DEFAULT_FEE_BUMP_AFTER,
        }
    }
}

/// The SQL cluster.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlConfig {
    /// The MySQL URLs; the first that accepts a connection wins.
    #[serde(default)]
    pub urls: Vec<String>,
    /// The size of the connection pool.
    #[serde(default = "default_sql_pool_size")]
    pub pool_size: u32,
}

/// The shared chain facts of the settlement service: the protocol-level
/// parameters every submitter shares.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedChainConfig {
    /// The chain id, verified against the RPC nodes at startup.
    pub chain_id: u64,
    /// The settlement contract address (the verifying contract of the
    /// EIP-712 domain).
    #[serde(with = "address_serde")]
    pub settlement_contract: Address,
    /// The confirmation depth of a settled transaction.
    #[serde(default = "default_confirmations")]
    pub confirmations: u64,
}

/// The chain interop of one submitter: the full parameter set the chain
/// client consumes (see [`SubmitterConfig::chain_config`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    /// The chain id, verified against the RPC nodes at startup.
    pub chain_id: u64,
    /// The settlement contract address (the verifying contract of the
    /// EIP-712 domain).
    #[serde(with = "address_serde")]
    pub settlement_contract: Address,
    /// The confirmation depth of a settled transaction.
    #[serde(default = "default_confirmations")]
    pub confirmations: u64,
    /// The path of the operator keystore file (Web3 Secret Storage v3).
    /// The password comes from the `SVD_SETTLEMENT_KEYSTORE_PASSWORD`
    /// environment variable, never from the config.
    pub keystore_path: String,
    /// The budget of one submission acknowledgement, in milliseconds.
    #[serde(default = "default_submit_timeout_ms")]
    pub submit_timeout_ms: u64,
    /// The RPC node pool: one primary and the secondaries in the failover
    /// order.
    pub rpc_pool: RpcPoolConfig,
    /// The gas strategy.
    pub gas: GasConfig,
}

/// The RPC node pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcPoolConfig {
    /// The primary node.
    pub primary: RpcNodeConfig,
    /// The secondaries in the failover order.
    #[serde(default)]
    pub secondaries: Vec<RpcNodeConfig>,
}

/// One RPC node and its failure profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcNodeConfig {
    /// The node name, for the logs.
    pub name: String,
    /// The HTTP endpoint of the node.
    pub url: String,
    /// The connection timeout, in milliseconds.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// The per-call watchdog, in milliseconds.
    #[serde(default = "default_rpc_timeout_ms")]
    pub rpc_timeout_ms: u64,
    /// The stall window, in milliseconds: a node that stays silent this
    /// long is switched away from.
    #[serde(default = "default_stall_timeout_ms")]
    pub stall_timeout_ms: u64,
}

/// The gas strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GasConfig {
    /// The hard cap of the priority fee, in gwei.
    #[serde(default = "default_max_priority_fee_gwei")]
    pub max_priority_fee_gwei: u128,
    /// The per-retry replacement increment, in percent.
    #[serde(default = "default_bump_pct")]
    pub bump_pct: u16,
    /// The headroom over the current base fee, in basis points.
    #[serde(default = "default_base_fee_tolerance_bps")]
    pub base_fee_tolerance_bps: u16,
    /// The absolute ceiling of the gas limit.
    #[serde(default = "default_gas_limit_cap")]
    pub gas_limit_cap: u64,
    /// The headroom over the gas estimate, in percent.
    #[serde(default = "default_gas_buffer_pct")]
    pub gas_buffer_pct: u16,
}

impl Default for GasConfig {
    fn default() -> Self {
        Self {
            max_priority_fee_gwei: DEFAULT_MAX_PRIORITY_FEE_GWEI,
            bump_pct: DEFAULT_BUMP_PCT,
            base_fee_tolerance_bps: DEFAULT_BASE_FEE_TOLERANCE_BPS,
            gas_limit_cap: DEFAULT_GAS_LIMIT_CAP,
            gas_buffer_pct: DEFAULT_GAS_BUFFER_PCT,
        }
    }
}

/// The batch queue of one submitter: a file-mapped byte SPSC queue owned by
/// this process (it initializes the file when missing and never
/// reinitializes on a restart, so the unprocessed frames survive).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ByteQueueConfig {
    /// The file path of the queue.
    pub path: String,
    /// The capacity of the data region, in bytes (at least 16).
    pub capacity_bytes: usize,
}

/// One submitter of the pool: its operator key and its private chain
/// resources. The submitter holds its own nonce manager, gas strategy, RPC
/// node pool and confirmation watch, and consumes one batch at a time from
/// its own queue.
///
/// Two submitters must never share one keystore: each would run its own
/// nonce manager for the same operator account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitterConfig {
    /// The path of the operator keystore file (Web3 Secret Storage v3).
    /// The password comes from the `SVD_SETTLEMENT_KEYSTORE_PASSWORD`
    /// environment variable — one variable for every keystore, never from
    /// the config, never in a log.
    pub keystore_path: String,
    /// The budget of one submission acknowledgement, in milliseconds.
    #[serde(default = "default_submit_timeout_ms")]
    pub submit_timeout_ms: u64,
    /// The submitter's own batch queue.
    pub queue: ByteQueueConfig,
    /// The submitter's RPC node pool: one primary and the secondaries in
    /// the failover order.
    pub rpc_pool: RpcPoolConfig,
    /// The submitter's gas strategy.
    #[serde(default)]
    pub gas: GasConfig,
}

impl SubmitterConfig {
    /// Merges the shared chain facts with this submitter's resources into
    /// the full [`ChainConfig`] the chain client consumes.
    pub fn chain_config(&self, shared: &SharedChainConfig) -> ChainConfig {
        ChainConfig {
            chain_id: shared.chain_id,
            settlement_contract: shared.settlement_contract,
            confirmations: shared.confirmations,
            keystore_path: self.keystore_path.clone(),
            submit_timeout_ms: self.submit_timeout_ms,
            rpc_pool: self.rpc_pool.clone(),
            gas: self.gas,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_retry_base_ms() -> u64 {
    DEFAULT_RETRY_BASE_MS
}

fn default_retry_max_ms() -> u64 {
    DEFAULT_RETRY_MAX_MS
}

fn default_retry_jitter_pct() -> u32 {
    DEFAULT_RETRY_JITTER_PCT
}

fn default_fee_bump_after() -> u32 {
    DEFAULT_FEE_BUMP_AFTER
}

fn default_poll_interval_ms() -> u64 {
    DEFAULT_POLL_INTERVAL_MS
}

fn default_tx_lost_grace_ms() -> u64 {
    DEFAULT_TX_LOST_GRACE_MS
}

fn default_sql_pool_size() -> u32 {
    4
}

fn default_confirmations() -> u64 {
    DEFAULT_CONFIRMATIONS
}

fn default_submit_timeout_ms() -> u64 {
    DEFAULT_SUBMIT_TIMEOUT_MS
}

fn default_connect_timeout_ms() -> u64 {
    DEFAULT_CONNECT_TIMEOUT_MS
}

fn default_rpc_timeout_ms() -> u64 {
    DEFAULT_RPC_TIMEOUT_MS
}

fn default_stall_timeout_ms() -> u64 {
    DEFAULT_STALL_TIMEOUT_MS
}

fn default_max_priority_fee_gwei() -> u128 {
    DEFAULT_MAX_PRIORITY_FEE_GWEI
}

fn default_bump_pct() -> u16 {
    DEFAULT_BUMP_PCT
}

fn default_base_fee_tolerance_bps() -> u16 {
    DEFAULT_BASE_FEE_TOLERANCE_BPS
}

fn default_gas_limit_cap() -> u64 {
    DEFAULT_GAS_LIMIT_CAP
}

fn default_gas_buffer_pct() -> u16 {
    DEFAULT_GAS_BUFFER_PCT
}

/// Serde plumbing of a symbol: `0x`-prefixed hex (left-padded to 32 bytes)
/// or a plain ASCII name of at most 32 bytes (right-padded with zeros).
mod symbol_serde {
    use super::*;

    pub fn serialize<S>(symbol: &Symbol, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("0x{}", symbol.hex()))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Symbol, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        parse_symbol(&text).map_err(serde::de::Error::custom)
    }
}

/// Serde plumbing of an address: a `0x`-prefixed hex string of 40 digits.
mod address_serde {
    use super::*;

    pub fn serialize<S>(address: &Address, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("0x{}", address.hex()))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Address, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        parse_address(&text).map_err(serde::de::Error::custom)
    }
}

/// Parses a symbol from its TOML representation: a `0x`-prefixed hex string
/// (left-padded to 32 bytes) or a plain ASCII name of at most 32 bytes
/// (right-padded with zeros).
pub fn parse_symbol(text: &str) -> Result<Symbol, String> {
    if text.is_empty() {
        return Err("the symbol is empty".to_string());
    }
    let mut bytes = [0u8; 32];
    if let Some(hex) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        if hex.is_empty() || hex.len() > 64 {
            return Err(format!("hex symbol must have 1 to 64 digits, got {}", hex.len()));
        }
        let raw = decode_hex(hex)?;
        // Hex symbols are big-endian byte strings: left-pad with zeros.
        bytes[32 - raw.len()..].copy_from_slice(&raw);
    } else {
        let raw = text.as_bytes();
        if raw.len() > 32 {
            return Err(format!("ascii symbol must be at most 32 bytes, got {}", raw.len()));
        }
        if !raw.iter().all(u8::is_ascii) {
            return Err("the symbol must be hex (`0x…`) or plain ASCII".to_string());
        }
        // ASCII symbols are left-aligned strings: right-pad with zeros.
        bytes[..raw.len()].copy_from_slice(raw);
    }
    Ok(Symbol(bytes))
}

/// Parses an address from its TOML representation: a `0x`-prefixed hex
/// string of exactly 40 digits.
pub fn parse_address(text: &str) -> Result<Address, String> {
    let hex = text
        .strip_prefix("0x")
        .or_else(|| text.strip_prefix("0X"))
        .ok_or_else(|| "the address must be a 0x-prefixed hex string".to_string())?;
    if hex.len() != 40 {
        return Err(format!("the address must have 40 hex digits, got {}", hex.len()));
    }
    let raw = decode_hex(hex)?;
    Ok(Address(raw.try_into().expect("40 hex digits decode into 20 bytes")))
}

/// Decodes a hex string (without the `0x` prefix) into bytes.
fn decode_hex(hex: &str) -> Result<Vec<u8>, String> {
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            let pair = &hex[i..i + 2];
            u8::from_str_radix(pair, 16).map_err(|_| format!("invalid hex digit in {pair:?}"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = include_str!("../config/settlement.toml");

    #[test]
    fn test_sample_config_parses() {
        let config = SettlementConfig::from_toml(SAMPLE).expect("the sample config parses");
        assert_eq!(config.symbol, Symbol([0; 32]));
        assert_eq!(config.batch_size, 1024);
        assert_eq!(config.chain.chain_id, 31_337);
        assert_eq!(config.submitters.len(), 2);
    }

    #[test]
    fn test_minimal_config_gets_defaults() {
        let text = r#"
            symbol = "ETHUSDC"
            [trade]
            path = "/dev/shm/svd_stl_ethusdc.trade"
            capacity = 65536
            [core]
            seq_path = "/var/lib/svd-settlement/svd_stl_ethusdc.seq"
            [chain]
            chain_id = 31337
            settlement_contract = "0x9fe46736679d2d9a65f0992f2272de9f3c7fa6e0"
            [[submitters]]
            keystore_path = "/var/lib/svd-settlement/operator.keystore"
            [submitters.queue]
            path = "/dev/shm/svd_stl_ethusdc.submit.0"
            capacity_bytes = 8388608
            [submitters.rpc_pool.primary]
            name = "primary"
            url = "http://10.0.0.10:8545"
            [submitters.gas]
        "#;
        let config = SettlementConfig::from_toml(text).expect("the minimal config parses");
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(config.retry.base_ms, DEFAULT_RETRY_BASE_MS);
        assert_eq!(config.chain.confirmations, DEFAULT_CONFIRMATIONS);
        let submitter = &config.submitters[0];
        assert_eq!(submitter.submit_timeout_ms, DEFAULT_SUBMIT_TIMEOUT_MS);
        assert_eq!(submitter.gas, GasConfig::default());
        assert_eq!(submitter.rpc_pool.secondaries, Vec::new());
        assert_eq!(config.redis.urls, storage::RedisConfig::default().urls);
        assert_eq!(config.sql.urls, Vec::<String>::new());
    }

    #[test]
    fn test_chain_config_merges_the_shared_facts_with_the_submitter() {
        let text = r#"
            symbol = "ETHUSDC"
            [trade]
            path = "/dev/shm/x"
            capacity = 2
            [core]
            seq_path = "/tmp/x.seq"
            [chain]
            chain_id = 5
            settlement_contract = "0x9fe46736679d2d9a65f0992f2272de9f3c7fa6e0"
            confirmations = 3
            [[submitters]]
            keystore_path = "/k"
            [submitters.queue]
            path = "/dev/shm/q"
            capacity_bytes = 4096
            [submitters.rpc_pool.primary]
            name = "p"
            url = "http://127.0.0.1:8545"
        "#;
        let config = SettlementConfig::from_toml(text).unwrap();
        let chain = config.submitters[0].chain_config(&config.chain);
        assert_eq!(chain.chain_id, 5);
        assert_eq!(chain.confirmations, 3);
        assert_eq!(chain.keystore_path, "/k");
        assert_eq!(chain.submit_timeout_ms, DEFAULT_SUBMIT_TIMEOUT_MS);
        assert_eq!(chain.gas, GasConfig::default());
        assert_eq!(chain.rpc_pool, config.submitters[0].rpc_pool);
    }

    #[test]
    fn test_unknown_field_is_rejected() {
        let text = r#"
            symbol = "ETHUSDC"
            bogus = 1
            [trade]
            path = "/dev/shm/x"
            capacity = 2
            [core]
            seq_path = "/tmp/x.seq"
            [chain]
            chain_id = 1
            settlement_contract = "0x9fe46736679d2d9a65f0992f2272de9f3c7fa6e0"
            [[submitters]]
            keystore_path = "/x"
            [submitters.queue]
            path = "/dev/shm/q"
            capacity_bytes = 4096
            [submitters.rpc_pool.primary]
            name = "p"
            url = "http://127.0.0.1:8545"
        "#;
        assert!(matches!(SettlementConfig::from_toml(text), Err(ConfigError::Toml(_))));
    }

    #[test]
    fn test_leftover_sections_of_the_old_design_are_rejected() {
        // The batching, hand-off and journal sections no longer exist: a
        // config carrying them fails instead of silently ignoring them.
        for section in ["[batch]", "[handoff]", "[journal]"] {
            let text = format!(
                r#"
                symbol = "ETHUSDC"
                [trade]
                path = "/dev/shm/x"
                capacity = 2
                [core]
                seq_path = "/tmp/x.seq"
                [chain]
                chain_id = 1
                settlement_contract = "0x9fe46736679d2d9a65f0992f2272de9f3c7fa6e0"
                [[submitters]]
                keystore_path = "/x"
                [submitters.queue]
                path = "/dev/shm/q"
                capacity_bytes = 4096
                [submitters.rpc_pool.primary]
                name = "p"
                url = "http://127.0.0.1:8545"
                {section}
            "#
            );
            assert!(
                matches!(SettlementConfig::from_toml(&text), Err(ConfigError::Toml(_))),
                "{section} must be rejected"
            );
        }
    }

    #[test]
    fn test_toml_roundtrip() {
        let config = SettlementConfig::from_toml(SAMPLE).unwrap();
        let text = toml::to_string(&config).unwrap();
        assert_eq!(SettlementConfig::from_toml(&text).unwrap(), config);
    }

    #[test]
    fn test_an_empty_submitter_pool_parses_but_is_rejected_at_launch() {
        let text = r#"
            symbol = "ETHUSDC"
            submitters = []
            [trade]
            path = "/dev/shm/x"
            capacity = 2
            [core]
            seq_path = "/tmp/x.seq"
            [chain]
            chain_id = 1
            settlement_contract = "0x9fe46736679d2d9a65f0992f2272de9f3c7fa6e0"
        "#;
        let config = SettlementConfig::from_toml(text).unwrap();
        assert!(config.submitters.is_empty());
    }

    #[test]
    fn test_parse_symbol_and_address() {
        // Hex symbols are left-padded (big-endian byte strings).
        assert_eq!(
            parse_symbol("0x0a").unwrap(),
            Symbol([
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 10
            ])
        );
        assert_eq!(
            parse_symbol("ETHUSDC").unwrap(),
            Symbol([
                69, 84, 72, 85, 83, 68, 67, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0
            ])
        );
        assert_eq!(
            parse_address("0x9fe46736679d2d9a65f0992f2272de9f3c7fa6e0").unwrap(),
            Address([
                0x9f, 0xe4, 0x67, 0x36, 0x67, 0x9d, 0x2d, 0x9a, 0x65, 0xf0, 0x99, 0x2f, 0x22, 0x72,
                0xde, 0x9f, 0x3c, 0x7f, 0xa6, 0xe0
            ])
        );
        assert!(parse_symbol("").is_err());
        assert!(parse_address("0x1234").is_err());
    }
}
