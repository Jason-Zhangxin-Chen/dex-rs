//! Integration tests of the SQL writer against a real MySQL server.
//!
//! Ignored by default (CI has no MySQL). The test connects to the database
//! the url points at and runs the schema DDL on it, so point it at a
//! **disposable** database: the rows are keyed by a batch sequence unique to
//! every run, the assertions read only those rows, and the tables are never
//! dropped.
//!
//! ```text
//! docker run -d --rm --name dex-mysql-test -e MYSQL_ROOT_PASSWORD=root \
//!     -e MYSQL_DATABASE=svd_settlement_test -p 3306:3306 mysql:8
//! SVD_SQL_TEST_URL=mysql://root:root@127.0.0.1:3306/svd_settlement_test \
//!     cargo test -p svd-settlement --test sql_integration -- --ignored --nocapture
//! ```

use std::time::{SystemTime, UNIX_EPOCH};

use primitives::address::Address;
use primitives::base::{Hash32, Nonce, Side, Symbol};
use primitives::message::hot_path::Trade;
use primitives::message::settlement::{
    FaultSide, SettlementFailure, SettlementOutcome, SettlementResult,
};
use primitives::order::{Order, OrderCold, OrderColdCommon, OrderHot, OrderKind};
use primitives::signature::Signature;
use primitives::time_in_force::TimeInForce;
use primitives::value::{Price, Quantity, TimestampMs};
use sqlx::Row;
use sqlx::mysql::MySqlPoolOptions;
use svd_settlement::sql::{MySqlSettlement, SettlementSql};

/// The environment variable holding the url of the disposable database.
const URL_ENV: &str = "SVD_SQL_TEST_URL";
/// The batch sequence of the settled result of the round-trip test.
const ROUNDTRIP_OFFSET: u64 = 0;
/// The batch sequence of the settled result of the idempotence test.
const IDEMPOTENCE_OFFSET: u64 = 1_000_000;

/// Runs the async test body against the database of [`URL_ENV`]; an unset
/// variable skips the test with a message instead of failing it.
fn run_test<F, Fut>(body: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let Ok(url) = std::env::var(URL_ENV) else {
        println!("skipping: {URL_ENV} is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(body(url))
}

/// A batch sequence unique to this run.
fn unique_seq(offset: u64) -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after the epoch")
        .as_millis() as u64;
    millis + offset
}

/// A standard order of `symbol`.
fn order(user: u8, nonce: u64, symbol: Symbol) -> Order {
    Order::new(
        OrderHot {
            user: Address([user; 20]),
            nonce: Nonce(nonce),
            price: Price(100),
            quantity: Quantity(10),
            time_in_force: TimeInForce::Gtc,
            side: Side::Buy,
        },
        OrderCold::new(
            OrderColdCommon::new(Hash32([0; 32]), symbol, Signature::default(), TimestampMs(0)),
            OrderKind::Standard,
        ),
    )
}

/// Two trades of `symbol`, distinct in every column the schema stores.
fn trades(symbol: Symbol) -> Vec<Trade> {
    let first = Trade::new(
        order(0x11, 1, symbol),
        Quantity(3),
        order(0x22, 2, symbol),
        Price(100),
        Quantity(7),
    );
    let second = Trade::new(
        order(0x33, 3, symbol),
        Quantity(5),
        order(0x44, 4, symbol),
        Price(101),
        Quantity(9),
    );
    vec![first, second]
}

/// The settled shape: both trades settled in the block `batch_seq + 100`.
fn settled_result(batch_seq: u64, symbol: Symbol) -> SettlementResult {
    SettlementResult {
        batch_seq,
        symbol,
        outcome: SettlementOutcome::Settled,
        tx_hash: Some(Hash32([0xee; 32])),
        block: Some(batch_seq + 100),
        trades: trades(symbol),
    }
}

/// The reverted shape: the second trade failed, the maker at fault, the
/// decoded protocol code 2.
fn reverted_result(batch_seq: u64, symbol: Symbol) -> SettlementResult {
    SettlementResult {
        batch_seq,
        symbol,
        outcome: SettlementOutcome::Reverted {
            failed_trade: 1,
            at_fault: FaultSide::Maker,
            reason: SettlementFailure::Protocol(2),
        },
        tx_hash: Some(Hash32([0xdd; 32])),
        block: None,
        trades: trades(symbol),
    }
}

#[test]
#[ignore = "requires a local mysql server; set SVD_SQL_TEST_URL"]
fn test_settled_and_reverted_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    run_test(settled_and_reverted_roundtrip)
}

/// Writes one settled and one reverted batch and reads every column of their
/// settlement and trade rows back.
async fn settled_and_reverted_roundtrip(url: String) -> Result<(), Box<dyn std::error::Error>> {
    let symbol = Symbol([0x11; 32]);
    let batch_seq = unique_seq(ROUNDTRIP_OFFSET);
    let settled = settled_result(batch_seq, symbol);
    let reverted = reverted_result(batch_seq + 1, symbol);

    let urls = vec![url.clone()];
    let mut sql = MySqlSettlement::connect(&urls, 4).await?.with_symbol(symbol);
    sql.write_settled(&settled).await?;
    sql.write_reverted(&reverted).await?;

    let pool = MySqlPoolOptions::new().max_connections(2).connect(&url).await?;

    // The settlements: the settled batch first, the reverted one second.
    let rows = sqlx::query(
        "SELECT batch_seq, symbol, outcome, tx_hash, block, failed_trade, at_fault, reason, \
         CAST(created_at AS CHAR) AS created_at FROM settlements \
         WHERE batch_seq IN (?, ?) ORDER BY batch_seq",
    )
    .bind(batch_seq)
    .bind(reverted.batch_seq)
    .fetch_all(&pool)
    .await?;
    assert_eq!(rows.len(), 2, "one settlement row per batch");
    let (settled_row, reverted_row) = (&rows[0], &rows[1]);

    assert_eq!(settled_row.try_get::<u64, _>("batch_seq")?, batch_seq);
    assert_eq!(settled_row.try_get::<String, _>("symbol")?, symbol.hex());
    assert_eq!(settled_row.try_get::<String, _>("outcome")?, "settled");
    assert_eq!(
        settled_row.try_get::<Option<String>, _>("tx_hash")?,
        Some(Hash32([0xee; 32]).hex())
    );
    assert_eq!(settled_row.try_get::<Option<u64>, _>("block")?, Some(batch_seq + 100));
    assert_eq!(settled_row.try_get::<Option<u32>, _>("failed_trade")?, None);
    assert_eq!(settled_row.try_get::<Option<String>, _>("at_fault")?, None);
    assert_eq!(settled_row.try_get::<Option<String>, _>("reason")?, None);
    assert!(!settled_row.try_get::<String, _>("created_at")?.is_empty());

    assert_eq!(reverted_row.try_get::<u64, _>("batch_seq")?, reverted.batch_seq);
    assert_eq!(reverted_row.try_get::<String, _>("symbol")?, symbol.hex());
    assert_eq!(reverted_row.try_get::<String, _>("outcome")?, "reverted");
    assert_eq!(
        reverted_row.try_get::<Option<String>, _>("tx_hash")?,
        Some(Hash32([0xdd; 32]).hex())
    );
    assert_eq!(reverted_row.try_get::<Option<u64>, _>("block")?, None);
    assert_eq!(reverted_row.try_get::<Option<u32>, _>("failed_trade")?, Some(1));
    assert_eq!(reverted_row.try_get::<Option<String>, _>("at_fault")?, Some("maker".to_string()));
    assert_eq!(
        reverted_row.try_get::<Option<String>, _>("reason")?,
        Some("protocol:2".to_string())
    );

    // The trades of the settled batch, one row per trade in order.
    let rows = sqlx::query(
        "SELECT trade_index, symbol, tx_hash, block, price, traded_quantity, taker, taker_nonce, \
         taker_remaining, maker, maker_nonce, CAST(created_at AS CHAR) AS created_at FROM trades \
         WHERE batch_seq = ? ORDER BY trade_index",
    )
    .bind(batch_seq)
    .fetch_all(&pool)
    .await?;
    assert_eq!(rows.len(), 2, "one trade row per settled trade");
    for (index, (row, trade)) in rows.iter().zip(&settled.trades).enumerate() {
        assert_eq!(row.try_get::<u32, _>("trade_index")?, index as u32);
        assert_eq!(row.try_get::<String, _>("symbol")?, symbol.hex());
        assert_eq!(row.try_get::<Option<String>, _>("tx_hash")?, Some(Hash32([0xee; 32]).hex()));
        assert_eq!(row.try_get::<Option<u64>, _>("block")?, Some(batch_seq + 100));
        assert_eq!(row.try_get::<u64, _>("price")?, trade.price.0);
        assert_eq!(row.try_get::<u64, _>("traded_quantity")?, trade.traded_quantity.0);
        assert_eq!(row.try_get::<String, _>("taker")?, trade.taker.hot.user.hex());
        assert_eq!(row.try_get::<u64, _>("taker_nonce")?, trade.taker.hot.nonce.0);
        assert_eq!(row.try_get::<u64, _>("taker_remaining")?, trade.taker_remaining.0);
        assert_eq!(row.try_get::<String, _>("maker")?, trade.maker.hot.user.hex());
        assert_eq!(row.try_get::<u64, _>("maker_nonce")?, trade.maker.hot.nonce.0);
        assert!(!row.try_get::<String, _>("created_at")?.is_empty());
    }

    // The reverted batch writes no trade row: its failure is handled by the
    // pre-trade restore, not by a settled trade.
    let reverted_trades: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM trades WHERE batch_seq = ?")
            .bind(reverted.batch_seq)
            .fetch_one(&pool)
            .await?;
    assert_eq!(reverted_trades, 0);
    Ok(())
}

#[test]
#[ignore = "requires a local mysql server; set SVD_SQL_TEST_URL"]
fn test_replay_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    run_test(replay_is_idempotent)
}

/// Writes the same settled result three times (the replay shape: the journal
/// re-publishes after a crash) and asserts the unique keys absorbed the
/// repeats.
async fn replay_is_idempotent(url: String) -> Result<(), Box<dyn std::error::Error>> {
    let symbol = Symbol([0x22; 32]);
    let batch_seq = unique_seq(IDEMPOTENCE_OFFSET);
    let result = settled_result(batch_seq, symbol);

    // The second connect also exercises the idempotent DDL: the tables exist
    // by now and `CREATE TABLE IF NOT EXISTS` is a no-op.
    let urls = vec![url.clone()];
    let mut sql = MySqlSettlement::connect(&urls, 4).await?;
    sql.write_settled(&result).await?;
    let mut replay = MySqlSettlement::connect(&urls, 4).await?;
    replay.write_settled(&result).await?;
    replay.write_settled(&result).await?;

    let pool = MySqlPoolOptions::new().max_connections(2).connect(&url).await?;
    let settlements: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM settlements WHERE batch_seq = ?")
            .bind(batch_seq)
            .fetch_one(&pool)
            .await?;
    let trades: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trades WHERE batch_seq = ?")
        .bind(batch_seq)
        .fetch_one(&pool)
        .await?;
    assert_eq!(settlements, 1, "the replay wrote no second settlement row");
    assert_eq!(trades, 2, "the replay wrote no second trade rows");
    Ok(())
}
