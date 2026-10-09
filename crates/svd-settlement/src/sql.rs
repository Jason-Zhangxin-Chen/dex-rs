//! The SQL writer: the settled trades and the settlement rows of the SQL
//! cluster, behind the [`SettlementSql`] trait.
//!
//! [`MySqlSettlement`] is the production writer over a `sqlx` MySQL pool; the
//! trait is the seam the tests stub and the replay drives. The writer thread
//! ([`spawn_sql_writer`]) owns a current-thread tokio runtime and retries
//! every failed write forever, so an SQL outage never blocks the submitter:
//! the channel between them queues the results, and the `INSERT IGNORE`
//! statements make a replayed write a no-op.

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use primitives::base::Symbol;
use primitives::message::settlement::{
    FaultSide, SettlementFailure, SettlementOutcome, SettlementResult,
};
use sqlx::mysql::MySqlPoolOptions;
use tracing::{debug, error, info, warn};

use crate::batch::backoff_for;

/// The base of the SQL retry backoff, in milliseconds.
const RETRY_BASE_MS: u64 = 100;
/// The cap of the SQL retry backoff, in milliseconds.
const RETRY_MAX_MS: u64 = 10_000;
/// The name of the SQL writer thread.
const THREAD_NAME: &str = "stl-sql";

/// The DDL of the `settlements` table: one row per settlement batch, unique
/// by the batch sequence — the replay idempotence key.
const CREATE_SETTLEMENTS: &str = r#"
CREATE TABLE IF NOT EXISTS settlements (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    batch_seq        BIGINT UNSIGNED NOT NULL,
    symbol           CHAR(64)        NOT NULL,
    outcome          ENUM('settled', 'reverted') NOT NULL,
    tx_hash          CHAR(64)        NULL,
    block            BIGINT UNSIGNED NULL,
    failed_trade     INT UNSIGNED    NULL,
    at_fault         ENUM('taker', 'maker') NULL,
    reason           VARCHAR(20)     NULL,
    created_at       TIMESTAMP(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uq_batch (batch_seq)
) ENGINE = InnoDB"#;

/// The DDL of the `trades` table: one row per settled trade, unique by the
/// batch sequence and the trade index — the replay idempotence key.
const CREATE_TRADES: &str = r#"
CREATE TABLE IF NOT EXISTS trades (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    batch_seq        BIGINT UNSIGNED NOT NULL,
    trade_index      INT UNSIGNED    NOT NULL,
    symbol           CHAR(64)        NOT NULL,
    tx_hash          CHAR(64)        NULL,
    block            BIGINT UNSIGNED NULL,
    price            BIGINT UNSIGNED NOT NULL,
    traded_quantity  BIGINT UNSIGNED NOT NULL,
    taker            CHAR(40)        NOT NULL,
    taker_nonce      BIGINT UNSIGNED NOT NULL,
    taker_remaining  BIGINT UNSIGNED NOT NULL,
    maker            CHAR(40)        NOT NULL,
    maker_nonce      BIGINT UNSIGNED NOT NULL,
    created_at       TIMESTAMP(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uq_trade (batch_seq, trade_index)
) ENGINE = InnoDB"#;

/// The settlement row of a settled batch: the trades land right after it in
/// the same transaction.
const INSERT_SETTLED: &str = "INSERT IGNORE INTO settlements \
     (batch_seq, symbol, outcome, tx_hash, block) VALUES (?, ?, 'settled', ?, ?)";

/// The settlement row of a reverted batch: no trade row is written — the
/// at-fault side's order is removed and the innocent side's quantity is
/// restored by the [SVD_Pretrade] instead.
const INSERT_REVERTED: &str = "INSERT IGNORE INTO settlements \
     (batch_seq, symbol, outcome, tx_hash, block, failed_trade, at_fault, reason) \
     VALUES (?, ?, 'reverted', ?, ?, ?, ?, ?)";

/// One trade row; `trade_index` is the position of the trade in the batch,
/// i.e. the on-chain trade index.
const INSERT_TRADE: &str = "INSERT IGNORE INTO trades \
     (batch_seq, trade_index, symbol, tx_hash, block, price, traded_quantity, \
      taker, taker_nonce, taker_remaining, maker, maker_nonce) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

/// Errors of the SQL writer.
#[derive(Debug)]
pub enum SqlError {
    /// No SQL url is configured.
    NoUrl,
    /// No SQL node accepted a connection.
    Connect(sqlx::Error),
    /// A DDL statement, a write or a transaction failed.
    Query(sqlx::Error),
    /// The write does not accept the given result.
    Unsupported(&'static str),
}

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlError::NoUrl => write!(f, "no sql url configured"),
            SqlError::Connect(err) => write!(f, "sql connect: {err}"),
            SqlError::Query(err) => write!(f, "sql query: {err}"),
            SqlError::Unsupported(what) => write!(f, "unsupported sql write: {what}"),
        }
    }
}

impl std::error::Error for SqlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SqlError::Connect(err) | SqlError::Query(err) => Some(err),
            SqlError::NoUrl | SqlError::Unsupported(_) => None,
        }
    }
}

impl From<sqlx::Error> for SqlError {
    fn from(err: sqlx::Error) -> Self {
        SqlError::Query(err)
    }
}

/// The writer of the settlement results into the SQL cluster.
///
/// The methods return boxed futures instead of being declared `async fn`:
/// an `async fn` trait is not dyn compatible, and the writer thread drives
/// its single implementation behind a `Box<dyn SettlementSql>`. Each method
/// accepts only its outcome — the writer thread dispatches on
/// [`SettlementOutcome`] — and is a dumb writer: the retry policy lives in
/// the thread, not in the implementation.
pub trait SettlementSql: Send {
    /// Writes the settled batch: the settlement row and one trade row per
    /// trade, in one transaction.
    fn write_settled<'a>(
        &'a mut self,
        result: &'a SettlementResult,
    ) -> Pin<Box<dyn Future<Output = Result<(), SqlError>> + Send + 'a>>;

    /// Writes the reverted batch: the settlement row carrying the failed
    /// trade index, the at-fault side and the decoded reason.
    fn write_reverted<'a>(
        &'a mut self,
        result: &'a SettlementResult,
    ) -> Pin<Box<dyn Future<Output = Result<(), SqlError>> + Send + 'a>>;
}

/// The production [`SettlementSql`]: the MySQL connection pool and the rows'
/// schema.
pub struct MySqlSettlement {
    /// The connection pool of the SQL cluster.
    pool: sqlx::MySqlPool,
    /// The market this writer serves, for the log context; the rows carry
    /// the symbol of every result.
    symbol: Symbol,
}

impl MySqlSettlement {
    /// Opens the pool against the first url that accepts a connection and
    /// creates the tables when missing. The DDL is idempotent, so it runs on
    /// every connect and the writer can start against a fresh database.
    pub async fn connect(urls: &[String], max_connections: u32) -> Result<Self, SqlError> {
        let mut last_error = None;
        for url in urls {
            let pool =
                MySqlPoolOptions::new().max_connections(max_connections.max(1)).connect(url).await;
            match pool {
                Ok(pool) => {
                    sqlx::query(CREATE_SETTLEMENTS).execute(&pool).await?;
                    sqlx::query(CREATE_TRADES).execute(&pool).await?;
                    info!(url = %url, "connected to the sql cluster");
                    return Ok(Self { pool, symbol: Symbol::default() });
                }
                Err(err) => {
                    warn!(url = %url, error = %err, "cannot connect to the sql node, trying the next url");
                    last_error = Some(err);
                }
            }
        }
        match last_error {
            Some(err) => Err(SqlError::Connect(err)),
            None => Err(SqlError::NoUrl),
        }
    }

    /// Binds the market this writer serves: `connect` cannot know it, so it
    /// defaults to the zero symbol and appears in the log context of every
    /// write once set.
    pub fn with_symbol(mut self, symbol: Symbol) -> Self {
        self.symbol = symbol;
        self
    }
}

impl SettlementSql for MySqlSettlement {
    fn write_settled<'a>(
        &'a mut self,
        result: &'a SettlementResult,
    ) -> Pin<Box<dyn Future<Output = Result<(), SqlError>> + Send + 'a>> {
        Box::pin(async move {
            debug!(symbol = %self.symbol.hex(), batch_seq = result.batch_seq, "writing the settled batch");
            let tx_hash = result.tx_hash.map(|hash| hash.hex());
            let mut tx = self.pool.begin().await?;
            sqlx::query(INSERT_SETTLED)
                .bind(result.batch_seq)
                .bind(result.symbol.hex())
                .bind(tx_hash.clone())
                .bind(result.block)
                .execute(&mut *tx)
                .await?;
            for (index, trade) in result.trades.iter().enumerate() {
                sqlx::query(INSERT_TRADE)
                    .bind(result.batch_seq)
                    .bind(index as u32)
                    .bind(result.symbol.hex())
                    .bind(tx_hash.clone())
                    .bind(result.block)
                    .bind(trade.price.0)
                    .bind(trade.traded_quantity.0)
                    .bind(trade.taker.hot.user.hex())
                    .bind(trade.taker.hot.nonce.0)
                    .bind(trade.taker_remaining.0)
                    .bind(trade.maker.hot.user.hex())
                    .bind(trade.maker.hot.nonce.0)
                    .execute(&mut *tx)
                    .await?;
            }
            tx.commit().await?;
            Ok(())
        })
    }

    fn write_reverted<'a>(
        &'a mut self,
        result: &'a SettlementResult,
    ) -> Pin<Box<dyn Future<Output = Result<(), SqlError>> + Send + 'a>> {
        Box::pin(async move {
            debug!(symbol = %self.symbol.hex(), batch_seq = result.batch_seq, "writing the reverted batch");
            let (failed_trade, at_fault, reason) = match result.outcome {
                SettlementOutcome::Reverted { failed_trade, at_fault, reason } => {
                    (failed_trade, at_fault, reason)
                }
                SettlementOutcome::Settled => {
                    return Err(SqlError::Unsupported("write_reverted of a settled result"));
                }
            };
            sqlx::query(INSERT_REVERTED)
                .bind(result.batch_seq)
                .bind(result.symbol.hex())
                .bind(result.tx_hash.map(|hash| hash.hex()))
                .bind(result.block)
                .bind(failed_trade as u32)
                .bind(fault_side_name(at_fault))
                .bind(failure_reason(reason))
                .execute(&self.pool)
                .await?;
            Ok(())
        })
    }
}

/// The SQL name of the at-fault side of a failed cross.
fn fault_side_name(side: FaultSide) -> &'static str {
    match side {
        FaultSide::Taker => "taker",
        FaultSide::Maker => "maker",
    }
}

/// The SQL reason of a decoded failure: the protocol code, or the
/// unclassified marker.
fn failure_reason(failure: SettlementFailure) -> String {
    match failure {
        SettlementFailure::Protocol(code) => format!("protocol:{code}"),
        SettlementFailure::Unclassified => "unclassified".to_string(),
    }
}

/// A [`SettlementSql`] that records the written results in memory: the fake
/// of the unit tests and of any downstream test.
#[derive(Debug, Default, Clone)]
pub struct RecordingSql {
    /// The results written by [`SettlementSql::write_settled`], in order.
    pub settled: Vec<SettlementResult>,
    /// The results written by [`SettlementSql::write_reverted`], in order.
    pub reverted: Vec<SettlementResult>,
}

impl SettlementSql for RecordingSql {
    fn write_settled<'a>(
        &'a mut self,
        result: &'a SettlementResult,
    ) -> Pin<Box<dyn Future<Output = Result<(), SqlError>> + Send + 'a>> {
        Box::pin(async move {
            self.settled.push(result.clone());
            Ok(())
        })
    }

    fn write_reverted<'a>(
        &'a mut self,
        result: &'a SettlementResult,
    ) -> Pin<Box<dyn Future<Output = Result<(), SqlError>> + Send + 'a>> {
        Box::pin(async move {
            self.reverted.push(result.clone());
            Ok(())
        })
    }
}

/// The shared handle of a [`RecordingSql`]: the test hands a clone to the
/// writer thread and keeps one to inspect the records after the join.
impl SettlementSql for Arc<Mutex<RecordingSql>> {
    fn write_settled<'a>(
        &'a mut self,
        result: &'a SettlementResult,
    ) -> Pin<Box<dyn Future<Output = Result<(), SqlError>> + Send + 'a>> {
        Box::pin(async move {
            self.lock().expect("the recording sql mutex").settled.push(result.clone());
            Ok(())
        })
    }

    fn write_reverted<'a>(
        &'a mut self,
        result: &'a SettlementResult,
    ) -> Pin<Box<dyn Future<Output = Result<(), SqlError>> + Send + 'a>> {
        Box::pin(async move {
            self.lock().expect("the recording sql mutex").reverted.push(result.clone());
            Ok(())
        })
    }
}

/// Spawns the SQL writer thread: it owns a current-thread tokio runtime,
/// drains the results from `rx` and writes them through the [`SettlementSql`]
/// impl, retrying every failure forever.
///
/// The thread stops when the engine drops the sender end of `rx`. The
/// write is the "delivered to the SQL writer" threshold of the submitter's
/// frame ack: a crash can still lose the unwritten rows (the accepted
/// at-least-once trade-off — the results are the authoritative truth in the
/// Redis cluster).
pub fn spawn_sql_writer(
    rx: Receiver<SettlementResult>,
    mut sql: Box<dyn SettlementSql>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new().name(THREAD_NAME.to_string()).spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(runtime) => runtime,
            Err(err) => {
                error!(error = %err, "cannot build the runtime of the sql writer");
                return;
            }
        };
        runtime.block_on(async move {
            info!("the sql writer started");
            while let Ok(result) = rx.recv() {
                let mut attempt = 0u32;
                loop {
                    let written = match result.outcome {
                        SettlementOutcome::Settled => sql.write_settled(&result).await,
                        SettlementOutcome::Reverted { .. } => sql.write_reverted(&result).await,
                    };
                    match written {
                        Ok(()) => break,
                        Err(err) => {
                            warn!(error = %err, batch_seq = result.batch_seq, "cannot write to the sql cluster, retrying");
                            let backoff = backoff_for(attempt, RETRY_BASE_MS, RETRY_MAX_MS);
                            tokio::time::sleep(Duration::from_millis(backoff)).await;
                            attempt = attempt.saturating_add(1);
                        }
                    }
                }
            }
            info!("the sql writer stopped");
        });
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::channel;
    use std::time::{Duration, Instant};

    use primitives::address::Address;
    use primitives::base::{Hash32, Nonce};
    use primitives::message::hot_path::Trade;
    use primitives::order::{Order, OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use primitives::signature::Signature;
    use primitives::time_in_force::TimeInForce;
    use primitives::value::{Price, Quantity, TimestampMs};

    use super::*;

    /// The symbol of the test results.
    const SYMBOL: Symbol = Symbol([0x5a; 32]);

    /// Builds a standard order of the test symbol.
    fn order(user: u8, nonce: u64) -> Order {
        Order::new(
            OrderHot {
                user: Address([user; 20]),
                nonce: Nonce(nonce),
                price: Price(100),
                quantity: Quantity(10),
                time_in_force: TimeInForce::Gtc,
                side: primitives::base::Side::Buy,
            },
            OrderCold::new(
                OrderColdCommon::new(Hash32([0; 32]), SYMBOL, Signature::default(), TimestampMs(0)),
                OrderKind::Standard,
            ),
        )
    }

    /// Builds a trade crossing `user`'s order against `user + 1`'s.
    fn trade(user: u8, nonce: u64) -> Trade {
        Trade::new(
            order(user, nonce),
            Quantity(9),
            order(user + 1, nonce + 1),
            Price(100),
            Quantity(1),
        )
    }

    /// A settled result with two trades.
    fn settled_result(batch_seq: u64) -> SettlementResult {
        SettlementResult {
            batch_seq,
            symbol: SYMBOL,
            outcome: SettlementOutcome::Settled,
            tx_hash: Some(Hash32([0xee; 32])),
            block: Some(1234),
            trades: vec![trade(1, 1), trade(2, 2)],
        }
    }

    /// A result reverted on-chain: the second trade failed, the maker at
    /// fault, with the protocol code 2.
    fn reverted_result(batch_seq: u64) -> SettlementResult {
        SettlementResult {
            batch_seq,
            symbol: SYMBOL,
            outcome: SettlementOutcome::Reverted {
                failed_trade: 1,
                at_fault: FaultSide::Maker,
                reason: SettlementFailure::Protocol(2),
            },
            tx_hash: Some(Hash32([0xdd; 32])),
            block: None,
            trades: vec![trade(1, 1), trade(2, 2)],
        }
    }

    /// Joins the thread once it finishes, panicking when it outlives the
    /// timeout instead of hanging the test run.
    fn join_within(handle: std::thread::JoinHandle<()>, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while !handle.is_finished() {
            assert!(Instant::now() < deadline, "the sql writer thread did not stop");
            std::thread::sleep(Duration::from_millis(2));
        }
        handle.join().expect("the sql writer thread does not panic");
    }

    /// A [`SettlementSql`] that fails the first `failures` writes with an
    /// injected query error and then accepts them into a shared
    /// [`RecordingSql`].
    struct FailingSql {
        /// The number of the leading writes to fail.
        failures: usize,
        /// The number of the writes seen, shared with the test.
        attempts: Arc<AtomicUsize>,
        /// The accepted results, shared with the test.
        recorded: Arc<Mutex<RecordingSql>>,
    }

    impl FailingSql {
        /// Builds the writer and the two handles the test inspects.
        fn new(failures: usize) -> (Self, Arc<AtomicUsize>, Arc<Mutex<RecordingSql>>) {
            let attempts = Arc::new(AtomicUsize::new(0));
            let recorded = Arc::new(Mutex::new(RecordingSql::default()));
            let sql =
                Self { failures, attempts: Arc::clone(&attempts), recorded: Arc::clone(&recorded) };
            (sql, attempts, recorded)
        }

        /// Counts one attempt and reports whether it must fail.
        fn fail(&self) -> bool {
            self.attempts.fetch_add(1, Ordering::SeqCst) < self.failures
        }
    }

    impl SettlementSql for FailingSql {
        fn write_settled<'a>(
            &'a mut self,
            result: &'a SettlementResult,
        ) -> Pin<Box<dyn Future<Output = Result<(), SqlError>> + Send + 'a>> {
            Box::pin(async move {
                if self.fail() {
                    return Err(SqlError::Query(sqlx::Error::RowNotFound));
                }
                self.recorded.lock().expect("the failing sql mutex").settled.push(result.clone());
                Ok(())
            })
        }

        fn write_reverted<'a>(
            &'a mut self,
            result: &'a SettlementResult,
        ) -> Pin<Box<dyn Future<Output = Result<(), SqlError>> + Send + 'a>> {
            Box::pin(async move {
                if self.fail() {
                    return Err(SqlError::Query(sqlx::Error::RowNotFound));
                }
                self.recorded.lock().expect("the failing sql mutex").reverted.push(result.clone());
                Ok(())
            })
        }
    }

    #[test]
    fn test_writer_dispatches_the_settled_result() {
        let recorded = Arc::new(Mutex::new(RecordingSql::default()));
        let (tx, rx) = channel();
        let handle =
            spawn_sql_writer(rx, Box::new(Arc::clone(&recorded))).expect("the thread spawns");

        let result = settled_result(7);
        tx.send(result.clone()).expect("the writer is alive");
        drop(tx);
        join_within(handle, Duration::from_secs(5));

        let recorded = recorded.lock().expect("the recording sql mutex");
        assert_eq!(recorded.settled, vec![result]);
        assert!(recorded.reverted.is_empty());
    }

    #[test]
    fn test_writer_dispatches_the_reverted_result() {
        let recorded = Arc::new(Mutex::new(RecordingSql::default()));
        let (tx, rx) = channel();
        let handle =
            spawn_sql_writer(rx, Box::new(Arc::clone(&recorded))).expect("the thread spawns");

        let result = reverted_result(8);
        tx.send(result.clone()).expect("the writer is alive");
        drop(tx);
        join_within(handle, Duration::from_secs(5));

        let recorded = recorded.lock().expect("the recording sql mutex");
        assert!(recorded.settled.is_empty());
        assert_eq!(recorded.reverted, vec![result]);
    }

    #[test]
    fn test_writer_retries_until_the_write_succeeds() {
        let (sql, attempts, recorded) = FailingSql::new(2);
        let (tx, rx) = channel();
        let handle = spawn_sql_writer(rx, Box::new(sql)).expect("the thread spawns");

        let result = settled_result(9);
        tx.send(result.clone()).expect("the writer is alive");
        drop(tx);
        join_within(handle, Duration::from_secs(10));

        assert_eq!(attempts.load(Ordering::SeqCst), 3, "two failures then one accepted write");
        assert_eq!(recorded.lock().expect("the recording sql mutex").settled, vec![result]);
    }

    #[test]
    fn test_writer_exits_when_the_channel_drops() {
        let recorded = Arc::new(Mutex::new(RecordingSql::default()));
        let (tx, rx) = channel();
        let handle =
            spawn_sql_writer(rx, Box::new(Arc::clone(&recorded))).expect("the thread spawns");

        drop(tx);
        join_within(handle, Duration::from_secs(5));

        let recorded = recorded.lock().expect("the recording sql mutex");
        assert!(recorded.settled.is_empty());
        assert!(recorded.reverted.is_empty());
    }

    #[test]
    fn test_connect_without_urls_reports_no_url() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the test runtime builds");
        let urls: Vec<String> = Vec::new();
        let result = runtime.block_on(MySqlSettlement::connect(&urls, 4));
        assert!(matches!(result, Err(SqlError::NoUrl)));
    }

    #[test]
    fn test_fault_side_and_reason_names() {
        assert_eq!(fault_side_name(FaultSide::Taker), "taker");
        assert_eq!(fault_side_name(FaultSide::Maker), "maker");
        assert_eq!(failure_reason(SettlementFailure::Protocol(2)), "protocol:2");
        assert_eq!(failure_reason(SettlementFailure::Protocol(255)), "protocol:255");
        assert_eq!(failure_reason(SettlementFailure::Unclassified), "unclassified");
    }

    #[test]
    fn test_sql_error_display_and_source() {
        assert_eq!(SqlError::NoUrl.to_string(), "no sql url configured");
        let connect = SqlError::Connect(sqlx::Error::RowNotFound);
        assert!(connect.to_string().starts_with("sql connect:"));
        assert!(std::error::Error::source(&connect).is_some());
        let query = SqlError::Query(sqlx::Error::RowNotFound);
        assert!(query.to_string().starts_with("sql query:"));
        assert!(std::error::Error::source(&query).is_some());
        let unsupported = SqlError::Unsupported("write_reverted");
        assert_eq!(unsupported.to_string(), "unsupported sql write: write_reverted");
        assert!(std::error::Error::source(&unsupported).is_none());
        let converted: SqlError = sqlx::Error::RowNotFound.into();
        assert!(matches!(converted, SqlError::Query(_)));
    }
}
