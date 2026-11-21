//! Fixtures shared by the crate's unit tests.

use crate::queue::Queue;
use sqlx::{AssertSqlSafe, PgPool};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

// MOCKED: stands in for the embedder's subscriber; keeps each event's level, ignores spans.
#[derive(Default)]
pub struct Levels(Mutex<Vec<tracing::Level>>);

impl Levels {
    /// What `f` returned, and the level of every event it logged, in order.
    pub fn during<T>(f: impl FnOnce() -> T) -> (T, Vec<tracing::Level>) {
        let levels = Arc::new(Self::default());
        let out = tracing::subscriber::with_default(Arc::clone(&levels), f);
        let seen = levels.0.lock().expect("levels poisoned").clone();
        (out, seen)
    }
}

impl tracing::Subscriber for Levels {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        self.0
            .lock()
            .expect("levels poisoned")
            .push(*event.metadata().level());
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

pub async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("database regression exceeded five seconds")
}

/// A migrated schema of its own on STAMPEDED_TEST_DATABASE_URL, never DATABASE_URL.
pub struct Database {
    pub queue: Queue,
    pub admin: PgPool,
    pub schema: String,
}

impl Database {
    /// `None` skips the test: no database URL outside CI.
    pub async fn setup() -> Option<Self> {
        let Ok(url) = std::env::var("STAMPEDED_TEST_DATABASE_URL") else {
            assert!(
                std::env::var_os("CI").is_none(),
                "CI must provide STAMPEDED_TEST_DATABASE_URL"
            );
            eprintln!("skipping PostgreSQL regression: STAMPEDED_TEST_DATABASE_URL is unset");
            return None;
        };
        let admin = bounded(PgPool::connect(&url)).await.unwrap();
        let version: String = sqlx::query_scalar("SHOW server_version_num")
            .fetch_one(&admin)
            .await
            .unwrap();
        assert!(version.parse::<u32>().unwrap() >= 180_000);
        let schema = format!("unit_{}", Uuid::now_v7().simple());
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let options: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(3))
            .connect_with(options.options([
                ("search_path", schema.as_str()),
                ("application_name", schema.as_str()),
                ("statement_timeout", "3000"),
            ]))
            .await
            .unwrap();
        bounded(crate::schema::migrate(&pool)).await.unwrap();
        Some(Self {
            queue: Queue::new(pool),
            admin,
            schema,
        })
    }

    pub async fn close(self) {
        self.queue.pool().close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await
        .unwrap();
        self.admin.close().await;
    }
}
