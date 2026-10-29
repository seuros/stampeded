//! The embedded migrations, `schema/*.sql`, so a host needs no `.sql` files.
//!
//! [`migrate`] is safe on every start: it applies what the database lacks.

use sqlx::PgPool;
use sqlx::migrate::{Migrate, MigrateError, Migrator};

use crate::{Error, Result};

/// Applied migrations, apart from an app's own `_sqlx_migrations`.
const TABLE: &str = "stampeded_migrations";

/// One adopter at a time, across processes (`"STAMPEDE"`).
const LOCK: i64 = 0x5354_414d_5045_4445;

fn migrator() -> Migrator {
    let mut migrator = sqlx::migrate!("./schema");
    migrator.dangerous_set_table_name(TABLE);
    migrator
}

/// Installs the schema, or applies the migrations it lacks. Concurrent
/// callers (several daemons starting) take turns.
///
/// # Errors
///
/// A migration failed, the database is newer than this stampeded, or it
/// predates the 0.3 schema (stampeded 0.2 had no upgrade path: drop it and
/// install again).
pub async fn migrate(pool: &PgPool) -> Result<()> {
    let migrator = migrator();
    adopt(pool, &migrator).await?;
    migrator.run(pool).await.map_err(migrate_error)
}

/// Records the migrations a database installed before sqlx already holds.
async fn adopt(pool: &PgPool, migrator: &Migrator) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(LOCK)
        .execute(&mut *tx)
        .await?;
    let (tracked, versioned, jobs, policies): (bool, bool, bool, bool) = sqlx::query_as(
        "SELECT to_regclass($1) IS NOT NULL,
                to_regclass('stampeded_schema') IS NOT NULL,
                to_regclass('stampeded_jobs') IS NOT NULL,
                to_regclass('stampeded_queue_policies') IS NOT NULL",
    )
    .bind(TABLE)
    .fetch_one(&mut *tx)
    .await?;
    let applied: i64 = match (tracked, versioned, jobs, policies) {
        (true, ..) | (false, false, false, false) => return Ok(()),
        // stampeded 0.4: versions 1 to 3.
        (false, true, ..) => {
            sqlx::query_scalar("SELECT version::bigint FROM stampeded_schema")
                .fetch_one(&mut *tx)
                .await?
        }
        // stampeded 0.3: version 1, unrecorded.
        (false, false, true, true) => 1,
        (false, false, true, false) => {
            return Err(Error::Schema(
                "the database holds the stampeded 0.2 schema, which has no upgrade path: \
                 drop its stampeded_* tables and functions, then migrate again"
                    .into(),
            ));
        }
        (false, false, false, true) => {
            return Err(Error::Schema(
                "stampeded_queue_policies exists without stampeded_jobs: an unrelated or \
                 partially dropped install. Resolve it by hand before migrating"
                    .into(),
            ));
        }
    };
    if !(1..=3).contains(&applied) {
        return Err(Error::Schema(format!(
            "stampeded_schema holds version {applied}, but {TABLE} is missing"
        )));
    }
    tx.ensure_migrations_table(TABLE)
        .await
        .map_err(migrate_error)?;
    for migration in migrator.iter().filter(|m| m.version <= applied) {
        sqlx::query(
            "INSERT INTO stampeded_migrations
                 (version, description, success, checksum, execution_time)
             VALUES ($1, $2, true, $3, 0)",
        )
        .bind(migration.version)
        .bind(migration.description.as_ref())
        .bind(migration.checksum.as_ref())
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

fn migrate_error(e: MigrateError) -> Error {
    match e {
        MigrateError::Execute(e) => Error::Database(e),
        MigrateError::VersionMissing(version) => Error::Schema(format!(
            "the database holds migration {version}, which this stampeded lacks: \
             upgrade stampeded"
        )),
        e => Error::Schema(e.to_string()),
    }
}
