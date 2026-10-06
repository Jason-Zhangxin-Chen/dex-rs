//! The configuration of the pre-trade gateway.
//!
//! The config is loaded from a TOML file and can be reloaded at runtime via
//! SIGHUP. The symbol, the core id, the HTTP listen address, the queue
//! capacities and the chain parameters take effect on the next restart; the
//! margin parameters are read per request and follow a reload.

use std::path::Path;

use primitives::address::Address;
use primitives::base::Symbol;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use storage::RedisConfig;

use crate::naming;

/// Default batch size of the forwarding loop.
const DEFAULT_BATCH_SIZE: usize = 1024;
/// Default capacity of the shared MPSC queue.
const DEFAULT_MPSC_CAPACITY: usize = 65_536;
/// Default listen address of the HTTP gateway.
const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:8080";
/// Default timeout of an on-demand margin pull, in milliseconds.
const DEFAULT_PULL_TIMEOUT_MS: u64 = 1_000;
/// Default stale feed timeout of the margin kill switch, in milliseconds.
const DEFAULT_STALE_FEED_MS: u64 = 5_000;
/// Default bound of the tracked accounts.
const DEFAULT_MAX_TRACKED_ACCOUNTS: usize = 100_000;
/// Default idle eviction of the tracked accounts, in milliseconds.
const DEFAULT_IDLE_EVICT_MS: u64 = 3_600_000;
/// Default number of the margin cache shards.
const DEFAULT_SHARDS: usize = 16;

/// Errors of the config loading.
#[derive(Debug)]
pub enum ConfigError {
    /// The file could not be read.
    Io(std::io::Error),
    /// The file is not a valid pre-trade config.
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

/// Serde helper that (de)serializes the symbol as a hex / ASCII string.
mod symbol_serde {
    use super::*;

    pub fn serialize<S: Serializer>(symbol: &Symbol, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("0x{}", naming::symbol_hex(*symbol)))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Symbol, D::Error> {
        let text = String::deserialize(deserializer)?;
        parse_symbol(&text).map_err(serde::de::Error::custom)
    }
}

/// Serde helper that (de)serializes the address as a hex string.
mod address_serde {
    use super::*;

    pub fn serialize<S: Serializer>(address: &Address, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("0x{}", naming::address_hex(*address)))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Address, D::Error> {
        let text = String::deserialize(deserializer)?;
        parse_address(&text).map_err(serde::de::Error::custom)
    }
}

// todo: evaluate the below configurations, as they might be not necessary.

/// The chain parameters of the settlement protocol: the EIP-712 domain the
/// signatures are verified against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    /// The chain id of the settlement protocol.
    pub chain_id: u64,
    /// The settlement contract address — the EIP-712 verifying contract.
    #[serde(with = "address_serde")]
    pub verifying_contract: Address,
}

/// The margin gate parameters. They must be kept in lockstep with the
/// on-chain symbol config of the settlement protocol (an ops invariant).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarginConfig {
    /// The margin requirement of an order, in basis points of the notional.
    pub margin_ratio_bps: u32,
    /// The taker fee added to the requirement, in basis points of the
    /// notional.
    #[serde(default)]
    pub fee_bps: u32,
    /// The quote amount one price tick × one lot unit corresponds to.
    #[serde(default = "default_quote_per_tick_lot")]
    pub quote_per_tick_lot: u128,
    /// Whether an order passes the gate when no margin state is available
    /// at all (the escape hatch for a bootstrapping market).
    #[serde(default)]
    pub allow_unknown_accounts: bool,
    /// Timeout of the on-demand margin pull, in milliseconds.
    #[serde(default = "default_pull_timeout_ms")]
    pub pull_timeout_ms: u64,
    /// Stale feed timeout of the kill switch: no channel message within
    /// this window rejects the new orders, in milliseconds.
    #[serde(default = "default_stale_feed_ms")]
    pub stale_feed_ms: u64,
    /// Bound of the tracked accounts of the local margin cache.
    #[serde(default = "default_max_tracked_accounts")]
    pub max_tracked_accounts: usize,
    /// Entries idle beyond this window are evicted, in milliseconds.
    #[serde(default = "default_idle_evict_ms")]
    pub idle_evict_ms: u64,
    /// Number of shards of the margin cache.
    #[serde(default = "default_shards")]
    pub shards: usize,
}

impl Default for MarginConfig {
    fn default() -> Self {
        Self {
            margin_ratio_bps: 10_000,
            fee_bps: 0,
            quote_per_tick_lot: 1,
            allow_unknown_accounts: false,
            pull_timeout_ms: default_pull_timeout_ms(),
            stale_feed_ms: default_stale_feed_ms(),
            max_tracked_accounts: default_max_tracked_accounts(),
            idle_evict_ms: default_idle_evict_ms(),
            shards: default_shards(),
        }
    }
}

fn default_quote_per_tick_lot() -> u128 {
    1
}

fn default_pull_timeout_ms() -> u64 {
    DEFAULT_PULL_TIMEOUT_MS
}

fn default_stale_feed_ms() -> u64 {
    DEFAULT_STALE_FEED_MS
}

fn default_max_tracked_accounts() -> usize {
    DEFAULT_MAX_TRACKED_ACCOUNTS
}

fn default_idle_evict_ms() -> u64 {
    DEFAULT_IDLE_EVICT_MS
}

fn default_shards() -> usize {
    DEFAULT_SHARDS
}

/// A share-memory SPSC queue wired to the [SVD_OMS_Master] ingress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpScConfig {
    /// Path of the queue's backing file.
    pub path: String,
    /// Number of slots of the queue (at least 2).
    pub capacity: usize,
    /// Whether this process initializes the queue's file on open.
    #[serde(default = "default_true")]
    pub create: bool,
}

fn default_true() -> bool {
    true
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_mpsc_capacity() -> usize {
    DEFAULT_MPSC_CAPACITY
}

fn default_listen_addr() -> String {
    DEFAULT_LISTEN_ADDR.to_string()
}

/// The configuration of one pre-trade gateway: one symbol's gateway of the
/// hot path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PretradeConfig {
    /// The market symbol of the gateway, hex (`0x…`) or plain ASCII.
    #[serde(with = "symbol_serde")]
    pub symbol: Symbol,
    /// Core id the forwarding thread is pinned to; `None` leaves it unpinned.
    #[serde(default)]
    pub core_id: Option<usize>,
    /// Max number of messages the forwarding thread pops per batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Listen address of the HTTP gateway.
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    /// Capacity of the shared MPSC queue between the handlers and the
    /// forwarding thread.
    #[serde(default = "default_mpsc_capacity")]
    pub mpsc_capacity: usize,
    /// The egress SPSC queue wired to the [SVD_OMS_Master].
    pub egress: SpScConfig,
    /// The chain parameters of the settlement protocol.
    pub chain: ChainConfig,
    /// The margin gate parameters.
    #[serde(default)]
    pub margin: MarginConfig,
    /// The [Redis_Cluster] of the side feeds and the margin pulls.
    #[serde(default)]
    pub redis: RedisConfig,
}

impl PretradeConfig {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A complete config exercising every section.
    const FULL_TOML: &str = r#"
symbol = "BTCUSDC"
core_id = 2
batch_size = 256
listen_addr = "0.0.0.0:8080"
mpsc_capacity = 65536

[ingress]
path = "/dev/shm/svd_oms.ingress"
capacity = 65536
create = true

[chain]
chain_id = 31337
verifying_contract = "0xabababababababababababababababababababab"

[margin]
margin_ratio_bps = 10000
fee_bps = 10
quote_per_tick_lot = 1000
allow_unknown_accounts = false
pull_timeout_ms = 2000
stale_feed_ms = 5000
max_tracked_accounts = 100000
idle_evict_ms = 3600000
shards = 16

[redis]
urls = ["redis://127.0.0.1:6379"]
pool_size = 4
"#;

    #[test]
    fn test_parse_full_config() {
        let config = PretradeConfig::from_toml(FULL_TOML).unwrap();
        assert_eq!(config.symbol, parse_symbol("BTCUSDC").unwrap());
        assert_eq!(config.core_id, Some(2));
        assert_eq!(config.batch_size, 256);
        assert_eq!(config.listen_addr, "0.0.0.0:8080");
        assert_eq!(config.mpsc_capacity, 65_536);
        assert_eq!(config.egress.capacity, 65_536);
        assert!(config.egress.create);
        assert_eq!(config.chain.chain_id, 31_337);
        assert_eq!(config.chain.verifying_contract, Address([0xabu8; 20]));
        assert_eq!(config.margin.margin_ratio_bps, 10_000);
        assert_eq!(config.margin.fee_bps, 10);
        assert_eq!(config.margin.quote_per_tick_lot, 1_000);
        assert_eq!(config.margin.pull_timeout_ms, 2_000);
        assert_eq!(config.margin.shards, 16);
        assert_eq!(config.redis.urls, vec!["redis://127.0.0.1:6379"]);
    }

    #[test]
    fn test_parse_minimal_config_with_defaults() {
        let config = PretradeConfig::from_toml(
            r#"
symbol = "X"
[ingress]
path = "/tmp/q"
capacity = 1024
[chain]
chain_id = 1
verifying_contract = "0x0000000000000000000000000000000000000001"
"#,
        )
        .unwrap();
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(config.mpsc_capacity, DEFAULT_MPSC_CAPACITY);
        assert_eq!(config.listen_addr, DEFAULT_LISTEN_ADDR);
        assert!(config.egress.create);
        assert_eq!(config.margin, MarginConfig::default());
        assert_eq!(config.margin.margin_ratio_bps, 10_000);
        assert_eq!(config.margin.stale_feed_ms, DEFAULT_STALE_FEED_MS);
        assert_eq!(config.margin.max_tracked_accounts, DEFAULT_MAX_TRACKED_ACCOUNTS);
        assert_eq!(config.margin.shards, DEFAULT_SHARDS);
        assert_eq!(config.redis, RedisConfig::default());
    }

    #[test]
    fn test_parse_symbol_hex_and_ascii() {
        let symbol = parse_symbol("0x1234").unwrap();
        let mut bytes = [0u8; 32];
        bytes[30] = 0x12;
        bytes[31] = 0x34;
        assert_eq!(symbol, Symbol(bytes));
        assert_eq!(parse_symbol("BTC-USDC").unwrap().0[..8], *b"BTC-USDC");
    }

    #[test]
    fn test_parse_symbol_errors() {
        assert!(parse_symbol("").is_err());
        assert!(parse_symbol("0x").is_err());
        assert!(parse_symbol("0xzz").is_err());
        assert!(parse_symbol(&"a".repeat(33)).is_err());
    }

    #[test]
    fn test_parse_address() {
        assert_eq!(
            parse_address(&format!("0x{}", "ab".repeat(20))).unwrap(),
            Address([0xabu8; 20])
        );
        assert!(parse_address("0x123").is_err());
        assert!(parse_address("1234").is_err());
    }

    #[test]
    fn test_unknown_field_is_rejected() {
        assert!(PretradeConfig::from_toml("symbol = \"X\"\nbogus = 1\n").is_err());
        assert!(
            PretradeConfig::from_toml(
                "symbol = \"X\"\n[chain]\nchain_id = 1\nverifying_contract = \"0x0000000000000000000000000000000000000001\"\nbogus = 1\n"
            )
            .is_err()
        );
    }

    #[test]
    fn test_config_toml_roundtrip() {
        let config = PretradeConfig::from_toml(FULL_TOML).unwrap();
        let text = toml::to_string(&config).unwrap();
        let restored = PretradeConfig::from_toml(&text).unwrap();
        assert_eq!(config, restored);
    }

    #[test]
    fn test_sample_config_parses() {
        let sample = include_str!("../config/pretrade.toml");
        let config = PretradeConfig::from_toml(sample).unwrap();
        assert_eq!(config.symbol, parse_symbol("BTCUSDC").unwrap());
        assert_eq!(config.margin.margin_ratio_bps, 10_000);
    }
}
