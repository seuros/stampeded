//! Database setup shared by the integration test binaries.

use jiff::Timestamp;
use sqlx::{AssertSqlSafe, PgPool};
use stampeded::Queue;
use uuid::Uuid;

/// A queue on its own schema, so tests run in parallel; `None` without
/// `STAMPEDED_TEST_DATABASE_URL`.
pub async fn setup() -> Option<Queue> {
    let url = std::env::var("STAMPEDED_TEST_DATABASE_URL").ok()?;

    let schema = format!("t_{}", Uuid::now_v7().simple());
    let admin = PgPool::connect(&url).await.expect("connect");
    // Schema name is UUID-derived, not user input.
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create schema");
    admin.close().await;

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .after_connect(move |conn, _| {
            let search_path = schema.clone();
            Box::pin(async move {
                sqlx::query(AssertSqlSafe(format!("SET search_path TO {search_path}")))
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .expect("pool");

    stampeded::schema::migrate(&pool).await.expect("migrate");
    Some(Queue::new(pool))
}

/// The database's `now()`: a Docker VM's clock drifts from the host's.
#[allow(
    dead_code,
    reason = "each test binary compiles this module; not every one checks a timestamp"
)]
pub async fn db_now(q: &Queue) -> Timestamp {
    // Not via jiff-sqlx, so a decoding bug cannot shift both sides alike.
    let us: i64 = sqlx::query_scalar("SELECT (extract(epoch FROM now()) * 1000000)::int8")
        .fetch_one(q.pool())
        .await
        .expect("database clock");
    Timestamp::from_microsecond(us).expect("database clock in range")
}

/// `setup`, or skip the test; under CI a missing database fails instead.
#[macro_export]
macro_rules! q {
    () => {
        match common::setup().await {
            Some(q) => q,
            None => {
                assert!(
                    std::env::var_os("CI").is_none(),
                    "STAMPEDED_TEST_DATABASE_URL is not set and CI is; refusing to skip"
                );
                eprintln!("skipping: STAMPEDED_TEST_DATABASE_URL not set");
                return;
            }
        }
    };
}
