//! The configuration of the OMS engine.
//!
//! The config is loaded from a TOML file and can be reloaded at runtime via
//! SIGHUP. Reloadable at runtime: [`OmsConfig::mode`] (a slave is promoted
//! to a master when the mode flips) and the [`OmsConfig::snapshot`] interval.
//! Everything else — the snapshot persistence target included — takes effect
//! on the next restart.

use std::path::{Path, PathBuf};

use primitives::base::Symbol;
use primitives::orderbook::config::{BookConfig, BookConfigCold, BookConfigHot, RiskConfig};
use primitives::orderbook::risk::ReferencePriceSource;
use primitives::orderbook::stp::STPMode;
use primitives::value::{Price, Quantity};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use storage::RedisConfig;

use crate::naming;

/// Default batch size of the core loop.
const DEFAULT_BATCH_SIZE: usize = 1024;
/// Default snapshot cadence of the slave, in milliseconds.
const DEFAULT_SNAPSHOT_INTERVAL_MS: u64 = 5_000;
/// Default journal file size (64 MiB, header included).
const DEFAULT_JOURNAL_SIZE: u64 = 64 * 1024 * 1024;

/// The mode of the OMS engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// SVD_OMS_Master: executes the user requests.
    Master,
    /// SVD_OMS_Slave: replicates the master's book state.
    Slave,
}

/// Errors of the config loading.
#[derive(Debug)]
pub enum ConfigError {
    /// The file could not be read.
    Io(std::io::Error),
    /// The file is not a valid OMS config.
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

/// TOML shape of the reference price source: the named variants are plain
/// strings, a fixed price is an inline table `{ fixed_price = <ticks> }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReferencePriceToml {
    LastTrade,
    Mid,
    FixedPrice(Price),
}

impl From<ReferencePriceSource> for ReferencePriceToml {
    fn from(source: ReferencePriceSource) -> Self {
        match source {
            ReferencePriceSource::LastTrade => ReferencePriceToml::LastTrade,
            ReferencePriceSource::Mid => ReferencePriceToml::Mid,
            ReferencePriceSource::FixedPrice(price) => ReferencePriceToml::FixedPrice(price),
        }
    }
}

impl From<ReferencePriceToml> for ReferencePriceSource {
    fn from(value: ReferencePriceToml) -> Self {
        match value {
            ReferencePriceToml::LastTrade => ReferencePriceSource::LastTrade,
            ReferencePriceToml::Mid => ReferencePriceSource::Mid,
            ReferencePriceToml::FixedPrice(price) => ReferencePriceSource::FixedPrice(price),
        }
    }
}

/// Serde helper that (de)serializes the STP mode as a name instead of the
/// `repr(u8)` discriminant the wire format uses.
mod stp_mode_serde {
    use super::STPMode;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(mode: &STPMode, serializer: S) -> Result<S::Ok, S::Error> {
        let name = match mode {
            STPMode::None => "none",
            STPMode::CancelTaker => "cancel_taker",
            STPMode::CancelMaker => "cancel_maker",
            STPMode::CancelBoth => "cancel_both",
        };
        serializer.serialize_str(name)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<STPMode, D::Error> {
        let name = <&str>::deserialize(deserializer)?;
        match name {
            "none" => Ok(STPMode::None),
            "cancel_taker" => Ok(STPMode::CancelTaker),
            "cancel_maker" => Ok(STPMode::CancelMaker),
            "cancel_both" => Ok(STPMode::CancelBoth),
            _ => Err(serde::de::Error::unknown_variant(
                name,
                &["none", "cancel_taker", "cancel_maker", "cancel_both"],
            )),
        }
    }
}

/// Serde helper that (de)serializes the risk config through its TOML shape.
mod risk_config_serde {
    use super::*;

    /// The TOML shape of [`RiskConfig`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RiskConfigDef {
        pub max_notional_per_account: Option<u128>,
        pub price_band_bps: Option<u32>,
        pub max_open_orders_per_account: Option<u32>,
        #[serde(default, with = "reference_price_serde")]
        pub reference_price: Option<ReferencePriceSource>,
    }

    impl From<RiskConfig> for RiskConfigDef {
        fn from(config: RiskConfig) -> Self {
            Self {
                max_notional_per_account: config.max_notional_per_account,
                price_band_bps: config.price_band_bps,
                max_open_orders_per_account: config.max_open_orders_per_account,
                reference_price: config.reference_price,
            }
        }
    }

    impl From<RiskConfigDef> for RiskConfig {
        fn from(def: RiskConfigDef) -> Self {
            RiskConfig {
                max_notional_per_account: def.max_notional_per_account,
                price_band_bps: def.price_band_bps,
                max_open_orders_per_account: def.max_open_orders_per_account,
                reference_price: def.reference_price,
            }
        }
    }

    pub fn serialize<S: Serializer>(
        config: &Option<RiskConfig>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        config.as_ref().map(|config| RiskConfigDef::from(config.clone())).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<RiskConfig>, D::Error> {
        Option::<RiskConfigDef>::deserialize(deserializer).map(|def| def.map(Into::into))
    }
}

/// Serde helper that (de)serializes the reference price source through its
/// TOML shape.
mod reference_price_serde {
    use super::*;

    pub fn serialize<S: Serializer>(
        source: &Option<ReferencePriceSource>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        source.map(ReferencePriceToml::from).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<ReferencePriceSource>, D::Error> {
        Option::<ReferencePriceToml>::deserialize(deserializer).map(|value| value.map(Into::into))
    }
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

/// Remote derive of [`BookConfigHot`]: the STP mode and the risk config use
/// their TOML shapes, everything else is deserialized directly. The fields
/// default so that partial `[book.hot]` tables work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "BookConfigHot")]
struct BookConfigHotDef {
    #[serde(default)]
    pub tick_size: Option<Price>,
    #[serde(default)]
    pub lot_size: Option<Quantity>,
    #[serde(default)]
    pub min_order_size: Option<Quantity>,
    #[serde(default)]
    pub max_order_size: Option<Quantity>,
    #[serde(default, with = "risk_config_serde")]
    pub risk_config: Option<RiskConfig>,
    #[serde(default, with = "stp_mode_serde")]
    pub stp_mode: STPMode,
}

/// Remote derive of [`BookConfigCold`]: the symbol appears as a hex /
/// ASCII string instead of the raw 32-byte array. The fields default so
/// that partial `[book.cold]` tables work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "BookConfigCold")]
struct BookConfigColdDef {
    #[serde(default, with = "symbol_serde")]
    pub symbol: Symbol,
    #[serde(default)]
    pub arena_size: Option<u32>,
    #[serde(default)]
    pub order_index_size: Option<u32>,
    #[serde(default)]
    pub user_order_map_size: Option<u32>,
    #[serde(default)]
    pub trade_list_size: Option<u32>,
    #[serde(default)]
    pub order_index_list_pool_size: Option<u32>,
    #[serde(default)]
    pub order_index_list_size: Option<u32>,
    #[serde(default)]
    pub price_level_statistic_list_pool_size: Option<u32>,
    #[serde(default)]
    pub price_level_statistic_list_size: Option<u32>,
    #[serde(default)]
    pub price_level_map_size: Option<u16>,
    #[serde(default)]
    pub trade_list_pool_size: Option<u8>,
}

/// The NATS connections of the replication stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NatsConfig {
    /// Server URLs; the engine connects to the first one that answers.
    pub urls: Vec<String>,
    /// Stream name; derived from the symbol when not set.
    pub stream_name: Option<String>,
    /// Subject of the replication messages; derived from the symbol when not
    /// set.
    pub subject: Option<String>,
}

impl NatsConfig {
    /// The JetStream stream name of the symbol's replication stream.
    pub fn stream(&self, symbol: Symbol) -> String {
        self.stream_name.clone().unwrap_or_else(|| naming::stream_name(symbol))
    }

    /// The subject of the symbol's replication messages.
    pub fn subject(&self, symbol: Symbol) -> String {
        self.subject.clone().unwrap_or_else(|| naming::subject_name(symbol))
    }
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self { urls: vec!["nats://127.0.0.1:4222".to_string()], stream_name: None, subject: None }
    }
}

/// A share-memory SPSC queue wired to a neighbor service.
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

/// Which sinks the slave persists the snapshots to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotPersist {
    /// The local journal only.
    Journal,
    /// The Redis cluster only.
    Redis,
    /// The local journal and the Redis cluster.
    #[default]
    Both,
}

impl SnapshotPersist {
    /// Whether the snapshots are persisted to the local journal.
    pub fn includes_journal(self) -> bool {
        matches!(self, SnapshotPersist::Journal | SnapshotPersist::Both)
    }

    /// Whether the snapshots are persisted to the Redis cluster.
    pub fn includes_redis(self) -> bool {
        matches!(self, SnapshotPersist::Redis | SnapshotPersist::Both)
    }
}

/// The snapshot behavior of the slave: the cadence and the sinks the
/// snapshots are persisted to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotConfig {
    /// Snapshot interval in milliseconds.
    #[serde(default = "default_snapshot_interval_ms")]
    pub interval_ms: u64,
    /// Which sinks the snapshots are persisted to.
    #[serde(default)]
    pub persist: SnapshotPersist,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self { interval_ms: DEFAULT_SNAPSHOT_INTERVAL_MS, persist: SnapshotPersist::default() }
    }
}

/// The local journal of the slave.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalConfig {
    /// Path of the journal file.
    #[serde(default = "default_journal_path")]
    pub path: PathBuf,
    /// Total size of the journal file in bytes, header included.
    #[serde(default = "default_journal_size")]
    pub size: u64,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self { path: default_journal_path(), size: default_journal_size() }
    }
}

/// The order book configs, under the `[book]` table.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookToml {
    /// Hot configs of the book.
    #[serde(default, with = "BookConfigHotDef")]
    pub hot: BookConfigHot,
    /// Cold configs of the book; the `symbol` entry is ignored, the book
    /// takes the top-level symbol.
    #[serde(default, with = "BookConfigColdDef")]
    pub cold: BookConfigCold,
}

/// The configuration of one OMS engine: one symbol's book in one mode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OmsConfig {
    /// The mode of the engine.
    pub mode: Mode,
    /// The market symbol of the book, hex (`0x…`) or plain ASCII.
    #[serde(with = "symbol_serde")]
    pub symbol: Symbol,
    /// Core id the core thread is pinned to; `None` leaves it unpinned.
    #[serde(default)]
    pub core_id: Option<usize>,
    /// Max number of requests the core thread pops per batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// NATS connections of the replication stream.
    #[serde(default)]
    pub nats: NatsConfig,
    /// The ingress SPSC queue wired from SVD_Pretrade (master mode).
    pub ingress: SpScConfig,
    /// The trade SPSC queue wired to SVD_Settlement (master mode).
    pub settlement: SpScConfig,
    /// Snapshot cadence of the slave.
    #[serde(default)]
    pub snapshot: SnapshotConfig,
    /// Local journal of the slave.
    #[serde(default)]
    pub journal: JournalConfig,
    /// Redis cluster of the slave's side path; disabled when not set.
    #[serde(default)]
    pub redis: Option<RedisConfig>,
    /// The order book configs.
    #[serde(default)]
    pub book: BookToml,
}

impl OmsConfig {
    /// Loads the config from a TOML file.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path)?;
        Self::from_toml(&text)
    }

    /// Parses the config from TOML text.
    pub fn from_toml(text: &str) -> Result<Self, ConfigError> {
        Ok(toml::from_str(text)?)
    }

    /// Assembles the book config: the cold symbol always follows the
    /// top-level symbol, so the two can never disagree.
    pub fn book_config(&self) -> BookConfig {
        let mut cold = self.book.cold;
        cold.symbol = self.symbol;
        BookConfig::default().with_hot(self.book.hot.clone()).with_cold(cold)
    }
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_snapshot_interval_ms() -> u64 {
    DEFAULT_SNAPSHOT_INTERVAL_MS
}

fn default_journal_path() -> PathBuf {
    PathBuf::from("svd-oms.journal")
}

fn default_journal_size() -> u64 {
    DEFAULT_JOURNAL_SIZE
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A complete master config exercising every section.
    const MASTER_TOML: &str = r#"
mode = "master"
symbol = "0x0102ab"
core_id = 2
batch_size = 256

[nats]
urls = ["nats://127.0.0.1:4222"]

[ingress]
path = "/dev/shm/svd_oms.ingress"
capacity = 65536
create = true

[settlement]
path = "/dev/shm/svd_oms.settlement"
capacity = 65536
create = false

[snapshot]
interval_ms = 1000
persist = "both"

[journal]
path = "/var/lib/svd-oms/journal.bin"
size = 16777216

[redis]
urls = ["redis://127.0.0.1:6379"]
pool_size = 4

[book.hot]
tick_size = 1
lot_size = 1
min_order_size = 10
max_order_size = 100000
stp_mode = "cancel_both"

[book.hot.risk_config]
max_open_orders_per_account = 100
max_notional_per_account = 1000000000
price_band_bps = 500
reference_price = "mid"

[book.cold]
symbol = "ignored"
arena_size = 100000
order_index_size = 100000
user_order_map_size = 256
price_level_map_size = 1024
trade_list_size = 32
trade_list_pool_size = 128
"#;

    #[test]
    fn test_parse_master_config() {
        let config = OmsConfig::from_toml(MASTER_TOML).unwrap();
        assert_eq!(config.mode, Mode::Master);
        let mut symbol = [0u8; 32];
        symbol[29] = 0x01;
        symbol[30] = 0x02;
        symbol[31] = 0xab;
        assert_eq!(config.symbol, Symbol(symbol));
        assert_eq!(config.core_id, Some(2));
        assert_eq!(config.batch_size, 256);
        assert_eq!(config.ingress.capacity, 65536);
        assert!(!config.settlement.create);
        assert_eq!(config.snapshot.interval_ms, 1000);
        assert_eq!(config.snapshot.persist, SnapshotPersist::Both);
        assert_eq!(config.journal.path, PathBuf::from("/var/lib/svd-oms/journal.bin"));
        assert_eq!(config.journal.size, 16 * 1024 * 1024);
        let redis = config.redis.as_ref().unwrap();
        assert_eq!(redis.urls, vec!["redis://127.0.0.1:6379"]);
        assert_eq!(redis.pool_size, 4);

        let hot = &config.book.hot;
        assert_eq!(hot.tick_size, Some(Price(1)));
        assert_eq!(hot.stp_mode, STPMode::CancelBoth);
        let risk = hot.risk_config.as_ref().unwrap();
        assert_eq!(risk.max_open_orders_per_account, Some(100));
        assert_eq!(risk.reference_price, Some(ReferencePriceSource::Mid));

        // The top-level symbol overrides the cold symbol.
        let book = config.book_config();
        assert_eq!(book.cold.symbol, config.symbol);
    }

    #[test]
    fn test_parse_minimal_config_with_defaults() {
        let config = OmsConfig::from_toml(
            r#"
mode = "slave"
symbol = "BTCUSDC"
[ingress]
path = "/tmp/q"
capacity = 1024
[settlement]
path = "/tmp/s"
capacity = 1024
"#,
        )
        .unwrap();
        assert_eq!(config.mode, Mode::Slave);
        assert_eq!(config.symbol, parse_symbol("BTCUSDC").unwrap());
        assert_eq!(config.nats.urls, vec!["nats://127.0.0.1:4222"]);
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(config.snapshot.interval_ms, DEFAULT_SNAPSHOT_INTERVAL_MS);
        assert_eq!(config.snapshot.persist, SnapshotPersist::Both);
        assert_eq!(config.journal.path, default_journal_path());
        assert_eq!(config.journal.size, default_journal_size());
        assert!(config.redis.is_none());
        assert!(config.ingress.create);
    }

    #[test]
    fn test_snapshot_persist_names() {
        for (name, persist) in [
            ("journal", SnapshotPersist::Journal),
            ("redis", SnapshotPersist::Redis),
            ("both", SnapshotPersist::Both),
        ] {
            let config = OmsConfig::from_toml(&format!(
                "mode = \"slave\"\nsymbol = \"X\"\n[ingress]\npath = \"/tmp/q\"\ncapacity = 8\n[settlement]\npath = \"/tmp/s\"\ncapacity = 8\n[snapshot]\npersist = \"{name}\"\n"
            ))
            .unwrap();
            assert_eq!(config.snapshot.persist, persist);
        }
        assert!(OmsConfig::from_toml(
            "mode = \"slave\"\nsymbol = \"X\"\n[ingress]\npath = \"/tmp/q\"\ncapacity = 8\n[settlement]\npath = \"/tmp/s\"\ncapacity = 8\n[snapshot]\npersist = \"memory\"\n"
        )
        .is_err());
    }

    #[test]
    fn test_parse_symbol_hex_and_ascii() {
        // Hex: left-padded big-endian bytes.
        let symbol = parse_symbol("0x1234").unwrap();
        let mut bytes = [0u8; 32];
        bytes[30] = 0x12;
        bytes[31] = 0x34;
        assert_eq!(symbol, Symbol(bytes));

        // Full-width hex.
        let hex64 = "ab".repeat(32);
        assert_eq!(parse_symbol(&format!("0x{hex64}")).unwrap().0, [0xabu8; 32]);

        // ASCII: left-aligned, zero-padded.
        let symbol = parse_symbol("ETH-USDC").unwrap();
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(b"ETH-USDC");
        assert_eq!(symbol, Symbol(bytes));
    }

    #[test]
    fn test_parse_symbol_errors() {
        assert!(parse_symbol("").is_err());
        assert!(parse_symbol("0x").is_err());
        assert!(parse_symbol("0xzz").is_err());
        assert!(parse_symbol(&"a".repeat(33)).is_err());
        assert!(parse_symbol(&format!("0x{}", "0".repeat(65))).is_err());
    }

    #[test]
    fn test_unknown_mode_is_rejected() {
        let err = OmsConfig::from_toml("mode = \"worker\"\n").unwrap_err();
        assert!(matches!(err, ConfigError::Toml(_)));
    }

    #[test]
    fn test_unknown_field_is_rejected() {
        assert!(OmsConfig::from_toml("mode = \"master\"\nbogus = 1\n").is_err());
        assert!(OmsConfig::from_toml("mode = \"master\"\n[nats]\nurls = []\nbogus = 1\n").is_err());
    }

    #[test]
    fn test_missing_mode_is_rejected() {
        assert!(OmsConfig::from_toml("symbol = \"X\"\n").is_err());
    }

    #[test]
    fn test_fixed_price_reference_price() {
        let config = OmsConfig::from_toml(
            r#"
mode = "master"
symbol = "X"
[ingress]
path = "/tmp/q"
capacity = 1024
[settlement]
path = "/tmp/s"
capacity = 1024
[book.hot.risk_config]
reference_price = { fixed_price = 12345 }
"#,
        )
        .unwrap();
        assert_eq!(
            config.book.hot.risk_config.unwrap().reference_price,
            Some(ReferencePriceSource::FixedPrice(Price(12345)))
        );
    }

    #[test]
    fn test_stp_mode_names() {
        for (name, mode) in [
            ("none", STPMode::None),
            ("cancel_taker", STPMode::CancelTaker),
            ("cancel_maker", STPMode::CancelMaker),
            ("cancel_both", STPMode::CancelBoth),
        ] {
            let config = OmsConfig::from_toml(&format!(
                "mode = \"master\"\nsymbol = \"X\"\n[ingress]\npath = \"/tmp/q\"\ncapacity = 8\n[settlement]\npath = \"/tmp/s\"\ncapacity = 8\n[book.hot]\nstp_mode = \"{name}\"\n"
            ))
            .unwrap();
            assert_eq!(config.book.hot.stp_mode, mode);
        }
    }

    #[test]
    fn test_config_toml_roundtrip() {
        let config = OmsConfig::from_toml(MASTER_TOML).unwrap();
        let text = toml::to_string(&config).unwrap();
        let restored = OmsConfig::from_toml(&text).unwrap();
        assert_eq!(config, restored);
    }

    #[test]
    fn test_sample_configs_parse() {
        let master = include_str!("../config/master.toml");
        let slave = include_str!("../config/slave.toml");
        assert_eq!(OmsConfig::from_toml(master).unwrap().mode, Mode::Master);
        assert_eq!(OmsConfig::from_toml(slave).unwrap().mode, Mode::Slave);
    }
}
