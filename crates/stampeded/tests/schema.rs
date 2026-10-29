//! `schema::migrate` on databases stampeded 0.2–0.4 left, and the dedup key
//! it builds, each case in a schema of its own:
//!
//!   STAMPEDED_TEST_DATABASE_URL=postgres://ubuntu:ubuntu@localhost/stampeded_test \
//!     cargo test -p stampeded --test schema

use sqlx::{AssertSqlSafe, PgPool};
use stampeded::schema;
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

/// Schema 1's dedup key, before it named its period.
const VERSION_1_DEDUP_KEY: &str =
    "ALTER TABLE stampeded_jobs ALTER COLUMN dedup_key SET EXPRESSION AS (
    sha256((dedup_scope || coalesce(':' || extract(
        epoch FROM (date_trunc(dedup_period, inserted_at, 'UTC') - to_timestamp(0))
    )::bigint::text, ''))::bytea))";

const VERSION_2_DEDUP_KEY: &str = include_str!("../schema/0002_dedup_key_period.sql");

const VERSION_3_DEDUP_KEY: &str = include_str!("../schema/0003_dedup_key_separator.sql");

/// Turns a fresh install into one stampeded 0.4 left at `version`, or 0.3
/// (unversioned) for `None`, with that version's dedup key.
async fn legacy(pool: &PgPool, dedup_key: &str, version: Option<i32>) {
    let recorded = version.map_or_else(
        || "DROP TABLE stampeded_schema".to_owned(),
        |v| format!("UPDATE stampeded_schema SET version = {v}"),
    );
    sqlx::raw_sql(AssertSqlSafe(format!(
        "{dedup_key}; DROP TABLE stampeded_migrations; {recorded}"
    )))
    .execute(pool)
    .await
    .expect("a legacy install");
}

/// An hourly and a daily unique job of one scope, both inserted at
/// 00:30 UTC: their buckets start together.
async fn hourly_and_daily_at_midnight(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(
        "INSERT INTO stampeded_jobs (queue, task, dedup_scope, dedup_period, inserted_at)
         VALUES ('default', 'report', 'acct:42', 'hour', '2026-10-24T00:30:00Z'),
                ('default', 'report', 'acct:42', 'day', '2026-10-24T00:30:00Z')",
    )
    .execute(pool)
    .await
    .map(drop)
}

/// The hourly job alone, so it actually commits instead of colliding.
async fn insert_hourly_at_midnight(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(
        "INSERT INTO stampeded_jobs (queue, task, dedup_scope, dedup_period, inserted_at)
         VALUES ('default', 'report', 'acct:42', 'hour', '2026-10-24T00:30:00Z')",
    )
    .execute(pool)
    .await
    .map(drop)
}

/// The daily job alone, to check it against an hourly row already stored.
async fn insert_daily_at_midnight(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(
        "INSERT INTO stampeded_jobs (queue, task, dedup_scope, dedup_period, inserted_at)
         VALUES ('default', 'report', 'acct:42', 'day', '2026-10-24T00:30:00Z')",
    )
    .execute(pool)
    .await
    .map(drop)
}

/// A database at schema 1 gets the migrations it lacks, and after them
/// behaves like a fresh install.
#[tokio::test]
async fn a_version_1_database_is_upgraded_in_place() {
    let Some(pool) = empty().await else { return };
    schema::migrate(&pool).await.expect("install");
    legacy(&pool, VERSION_1_DEDUP_KEY, Some(1)).await;

    // Commits under schema 1, so the rewrite below has a real row to touch.
    insert_hourly_at_midnight(&pool)
        .await
        .expect("the lone hourly job commits under schema 1");
    hourly_and_daily_at_midnight(&pool)
        .await
        .expect_err("schema 1 gives both one key");

    schema::migrate(&pool).await.expect("upgrade");

    // Still collides with its own scope+period: proof the ALTER COLUMN ...
    // SET EXPRESSION AS rewrite recomputed the pre-existing row's key under
    // the new expression, rather than leaving it stale.
    insert_hourly_at_midnight(&pool)
        .await
        .expect_err("the row stored under schema 1 keeps its key after the rewrite");
    // The daily job, against that same stored hourly row: no longer shares
    // its key now that the period is part of it.
    insert_daily_at_midnight(&pool)
        .await
        .expect("the period keeps the windows apart");
}

/// An unversioned 0.3 install is schema 1.
#[tokio::test]
async fn an_unversioned_0_3_install_is_adopted_and_upgraded() {
    let Some(pool) = empty().await else { return };
    schema::migrate(&pool).await.expect("install");
    legacy(&pool, VERSION_1_DEDUP_KEY, None).await;

    schema::migrate(&pool).await.expect("adopt and upgrade");
    hourly_and_daily_at_midnight(&pool).await.expect("upgraded");
}

#[tokio::test]
async fn a_0_2_schema_is_refused_saying_why() {
    let Some(pool) = empty().await else { return };
    schema::migrate(&pool).await.expect("install");

    // 0.2: no version table, and no queue policies yet.
    sqlx::raw_sql(
        "DROP TABLE stampeded_migrations; DROP TABLE stampeded_schema;
         DROP TABLE stampeded_queue_policies;",
    )
    .execute(&pool)
    .await
    .expect("a 0.2 schema");
    let err = schema::migrate(&pool)
        .await
        .expect_err("0.2 has no upgrade path");
    assert!(err.to_string().contains("0.2"), "{err}");
}

/// A forged, period-less scope shaped like `<scope>:<period>:<epoch>`, next
/// to the real periodic job it targets: same scope, same period, the same
/// hour -- `2025-01-01T00:00:00Z` truncates to itself, epoch `1735689600`.
async fn forged_scope_and_the_job_it_targets(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(
        "INSERT INTO stampeded_jobs (queue, task, dedup_scope, dedup_period, inserted_at)
         VALUES ('default', 'report', 'a:hour:1735689600', NULL, '2025-01-01T00:00:00Z'),
                ('default', 'report', 'a', 'hour', '2025-01-01T00:00:00Z')",
    )
    .execute(pool)
    .await
    .map(drop)
}

/// Schema 2 let a crafted scope forge another job's periodic key; schema 3's
/// zero-byte join closes it.
#[tokio::test]
async fn a_crafted_scope_cannot_forge_a_periodic_jobs_key() {
    let Some(pool) = empty().await else { return };
    schema::migrate(&pool).await.expect("install");

    sqlx::raw_sql(VERSION_2_DEDUP_KEY)
        .execute(&pool)
        .await
        .expect("schema 2's dedup key");
    forged_scope_and_the_job_it_targets(&pool)
        .await
        .expect_err("schema 2: the forged scope collides with the job it targets");

    sqlx::raw_sql(VERSION_3_DEDUP_KEY)
        .execute(&pool)
        .await
        .expect("schema 3's dedup key");
    forged_scope_and_the_job_it_targets(&pool)
        .await
        .expect("schema 3: the forgery no longer matches");
}

/// One job per scope, in one statement: any collision fails them all.
async fn insert_scopes(pool: &PgPool, scopes: &[&str]) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO stampeded_jobs (queue, task, dedup_scope)
         SELECT 'default', 'report', scope FROM unnest($1::text[]) AS scope",
    )
    .bind(scopes)
    .execute(pool)
    .await
    .map(drop)
}

/// Schema 3 read the scope as bytea escapes; schema 4 keys its text.
#[tokio::test]
async fn a_scope_is_keyed_by_its_text_not_its_bytea_escapes() {
    let Some(pool) = empty().await else { return };
    schema::migrate(&pool).await.expect("install");
    legacy(&pool, VERSION_3_DEDUP_KEY, Some(3)).await;

    // `\x62` is stored under `b`'s key.
    insert_scopes(&pool, &["a", r"\x62"])
        .await
        .expect("schema 3 stores both");
    for forged in [r"\x61", r"\141"] {
        insert_scopes(&pool, &[forged])
            .await
            .expect_err("schema 3: the escape forges a's key");
    }
    insert_scopes(&pool, &[r"C:\tmp"])
        .await
        .expect_err("schema 3: a stray backslash is invalid bytea");

    schema::migrate(&pool).await.expect("upgrade");

    insert_scopes(&pool, &[r"\x62"])
        .await
        .expect_err("the stored row is rekeyed by its text");
    insert_scopes(
        &pool,
        &["b", r"\x61", r"\141", r"C:\tmp", r"\", r"\\", r"\000"],
    )
    .await
    .expect("each distinct text is its own key");
}
