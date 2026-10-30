//! Database setup shared by the integration test binaries.

#![allow(
    dead_code,
    reason = "each test binary compiles this module and uses part of it"
)]

use jiff::Timestamp;
use sqlx::{AssertSqlSafe, PgPool};
use stampeded::Queue;
use std::sync::Arc;
use uuid::Uuid;

/// `STAMPEDED_TEST_DATABASE_URL`, or `None` to skip the test. Never under CI: a
/// suite that skips everything reports green.
pub fn database_url() -> Option<String> {
    let url = std::env::var("STAMPEDED_TEST_DATABASE_URL").ok();
    if url.is_none() {
        assert!(
            std::env::var_os("CI").is_none(),
            "STAMPEDED_TEST_DATABASE_URL is not set and CI is. Refusing to skip \
             the database tests and report success. Check the Postgres service \
             container."
        );
        eprintln!("skipping: STAMPEDED_TEST_DATABASE_URL not set");
    }
    url
}

/// An empty schema of its own, so tests run in parallel.
pub async fn fresh_schema(url: &str) -> String {
    let schema = format!("t_{}", Uuid::now_v7().simple());
    let admin = PgPool::connect(url).await.expect("connect");
    // UUID-derived, not user input.
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create schema");
    admin.close().await;
    schema
}

/// A pool whose every connection searches `search_path`.
pub async fn pool_on(url: &str, search_path: &str) -> PgPool {
    let set: Arc<str> = format!("SET search_path TO {search_path}").into();
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .after_connect(move |conn, _| {
            let set = Arc::clone(&set);
            Box::pin(async move {
                sqlx::query(AssertSqlSafe(&*set)).execute(conn).await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .expect("pool")
}

/// A migrated queue on a schema of its own; `None` to skip.
pub async fn setup() -> Option<Queue> {
    let url = database_url()?;
    let pool = pool_on(&url, &fresh_schema(&url).await).await;
    stampeded::schema::migrate(&pool).await.expect("migrate");
    Some(Queue::new(pool))
}

/// The database's `now()`: a Docker VM's clock drifts from the host's.
pub async fn db_now(q: &Queue) -> Timestamp {
    // Not via jiff-sqlx, so a decoding bug cannot shift both sides alike.
    let us: i64 = sqlx::query_scalar("SELECT (extract(epoch FROM now()) * 1000000)::int8")
        .fetch_one(q.pool())
        .await
        .expect("database clock");
    Timestamp::from_microsecond(us).expect("database clock in range")
}

/// A queue from `setup` (or the given setup future), or return: the test skips.
#[macro_export]
macro_rules! q {
    () => {
        q!(common::setup())
    };
    ($setup:expr) => {
        match $setup.await {
            Some(q) => q,
            None => return,
        }
    };
}
