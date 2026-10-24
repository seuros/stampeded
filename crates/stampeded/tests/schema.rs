//! `schema::migrate` on every kind of database it meets, each case in a
//! schema of its own:
//!
//!   STAMPEDED_TEST_DATABASE_URL=postgres://ubuntu:ubuntu@localhost/stampeded_test \
//!     cargo test -p stampeded --test schema

use sqlx::{AssertSqlSafe, PgPool};
use stampeded::schema::{self, VERSION};
use uuid::Uuid;

/// A pool on an empty schema of its own; `None` without the database.
async fn empty() -> Option<PgPool> {
    let Ok(url) = std::env::var("STAMPEDED_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "STAMPEDED_TEST_DATABASE_URL is not set and CI is; refusing to skip"
        );
        eprintln!("skipping: STAMPEDED_TEST_DATABASE_URL not set");
        return None;
    };
    let name = format!("t_{}", Uuid::now_v7().simple());
    let admin = PgPool::connect(&url).await.expect("connect");
    // UUID-derived, not user input.
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {name}")))
        .execute(&admin)
        .await
        .expect("create schema");
    admin.close().await;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .after_connect(move |conn, _| {
            let name = name.clone();
            Box::pin(async move {
                sqlx::query(AssertSqlSafe(format!("SET search_path TO {name}")))
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .expect("pool");
    Some(pool)
}

async fn version(pool: &PgPool) -> i32 {
    sqlx::query_scalar("SELECT version FROM stampeded_schema")
        .fetch_one(pool)
        .await
        .expect("schema version")
}

/// What an embedding app does: migrate on every start, several at once.
#[tokio::test]
async fn every_start_may_migrate_and_concurrent_starts_take_turns() {
    let Some(pool) = empty().await else { return };
    let (first, second) = tokio::join!(schema::migrate(&pool), schema::migrate(&pool));
    first.expect("first start installs");
    second.expect("second start finds it installed");
    schema::migrate(&pool)
        .await
        .expect("a later start changes nothing");
    assert_eq!(version(&pool).await, VERSION);
}

/// Installed by stampeded 0.3, before the schema recorded its version: that
/// is schema 1, so migrate marks it.
#[tokio::test]
async fn an_unversioned_0_3_install_is_marked() {
    let Some(pool) = empty().await else { return };
    schema::migrate(&pool).await.expect("install");
    sqlx::query("DROP TABLE stampeded_schema")
        .execute(&pool)
        .await
        .expect("a 0.3 install");

    schema::migrate(&pool).await.expect("mark");
    assert_eq!(version(&pool).await, 1);
}

#[tokio::test]
async fn schemas_it_cannot_use_are_refused_saying_why() {
    let Some(pool) = empty().await else { return };
    schema::migrate(&pool).await.expect("install");

    sqlx::query("UPDATE stampeded_schema SET version = version + 1")
        .execute(&pool)
        .await
        .expect("a newer stampeded's schema");
    let err = schema::migrate(&pool)
        .await
        .expect_err("newer than this stampeded");
    assert!(err.to_string().contains("upgrade stampeded"), "{err}");

    // 0.2: no version table, and no queue policies yet.
    sqlx::raw_sql("DROP TABLE stampeded_schema; DROP TABLE stampeded_queue_policies;")
        .execute(&pool)
        .await
        .expect("a 0.2 schema");
    let err = schema::migrate(&pool)
        .await
        .expect_err("0.2 has no upgrade path");
    assert!(err.to_string().contains("0.2"), "{err}");
}
