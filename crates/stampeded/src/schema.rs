//! The embedded database schema, so a host needs no `.sql` files.
//!
//! [`migrate`] is safe on every start: a fresh database gets [`SCHEMA`], an
//! older one the upgrades it lacks, a current one nothing.

use sqlx::{PgPool, Postgres, Transaction};

use crate::{Error, Result};

/// A fresh install, at [`VERSION`]. It records that version itself, so an
/// install by hand (`psql -f schema.sql`) is versioned too.
pub const SCHEMA: &str = include_str!("../schema/schema.sql");

/// The schema [`SCHEMA`] builds. A schema change adds its upgrade to
/// `UPGRADES` and raises this.
pub const VERSION: i32 = 1;

/// `UPGRADES[i]` takes a database from version `i + 1` to `i + 2`.
const UPGRADES: &[&str] = &[];

/// One installer at a time, across processes (`"STAMPEDE"`).
const LOCK: i64 = 0x5354_414d_5045_4445;

/// Installs the schema, or upgrades it to [`VERSION`], in one transaction.
/// Concurrent callers (several daemons starting) take turns.
///
/// # Errors
///
/// The database's schema is newer than this stampeded, or predates the 0.3
/// schema (stampeded 0.2 had no upgrade path: drop it and install again).
pub async fn migrate(pool: &PgPool) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(LOCK)
        .execute(&mut *tx)
        .await?;
    match installed(&mut tx).await? {
        None => {
            sqlx::raw_sql(SCHEMA).execute(&mut *tx).await?;
        }
        Some(version) if version > VERSION => {
            return Err(Error::Schema(format!(
                "the database holds schema {version}, newer than this stampeded's {VERSION}: \
                 upgrade stampeded"
            )));
        }
        Some(version) => {
            let pending = usize::try_from(version - 1).unwrap_or(0);
            for &upgrade in &UPGRADES[pending..] {
                sqlx::raw_sql(upgrade).execute(&mut *tx).await?;
            }
            sqlx::query("UPDATE stampeded_schema SET version = $1, changed_at = now()")
                .bind(VERSION)
                .execute(&mut *tx)
                .await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

/// The schema version the database holds; `None` on a database without
/// stampeded.
async fn installed(tx: &mut Transaction<'_, Postgres>) -> Result<Option<i32>> {
    let (jobs, versioned, policies): (bool, bool, bool) = sqlx::query_as(
        "SELECT to_regclass('stampeded_jobs') IS NOT NULL,
                to_regclass('stampeded_schema') IS NOT NULL,
                to_regclass('stampeded_queue_policies') IS NOT NULL",
    )
    .fetch_one(&mut **tx)
    .await?;
    if versioned {
        let version: i32 = sqlx::query_scalar("SELECT version FROM stampeded_schema")
            .fetch_one(&mut **tx)
            .await?;
        return Ok(Some(version));
    }
    match (jobs, policies) {
        (false, _) => Ok(None),
        // Installed by stampeded 0.3, before the schema recorded its version:
        // the same schema as version 1, so mark it.
        (true, true) => {
            sqlx::raw_sql(
                "CREATE TABLE stampeded_schema (
                     version     integer     NOT NULL CHECK (version > 0),
                     changed_at  timestamptz NOT NULL DEFAULT now()
                 );
                 CREATE UNIQUE INDEX stampeded_schema_one_row ON stampeded_schema ((true));
                 INSERT INTO stampeded_schema (version) VALUES (1);",
            )
            .execute(&mut **tx)
            .await?;
            Ok(Some(1))
        }
        (true, false) => Err(Error::Schema(
            "the database holds the stampeded 0.2 schema, which has no upgrade path: \
             drop its stampeded_* tables and functions, then migrate again"
                .into(),
        )),
    }
}
