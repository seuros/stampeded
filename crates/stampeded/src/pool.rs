//! Opening a pool that reports an unreachable server at once.

use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{ConnectOptions, Connection};
use std::io;

/// `options.connect(url)`, with a probe connection first so a refused or misconfigured
/// server fails now instead of after `acquire_timeout`.
pub async fn connect(options: PgPoolOptions, url: &str) -> Result<PgPool, sqlx::Error> {
    let target: PgConnectOptions = url.parse()?;
    let timeout = options.get_acquire_timeout();
    let probe = tokio::time::timeout(timeout, target.connect())
        .await
        .map_err(|_| {
            sqlx::Error::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("connect timed out after {timeout:?}"),
            ))
        })??;
    probe.close().await?;
    Ok(options.connect_lazy_with(target))
}
