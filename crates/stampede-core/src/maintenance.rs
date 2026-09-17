//! Background maintenance: reaping orphans and pruning finished jobs.
//!
//! Both are singletons across the fleet. Running the reaper on every node would
//! be harmless but wasteful; running the pruner on every node would multiply
//! DELETE load on the one table that is already the bloat risk. Exclusion is a
//! Postgres advisory lock — no extra table, and it is released automatically if
//! the holder dies, which is the property a heartbeat table would have to
//! reimplement.

use crate::error::Result;
use crate::queue::Queue;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Advisory lock keys. Arbitrary but must be stable and distinct; sharing a key
/// with another subsystem would silently serialise unrelated work.
const LOCK_REAPER: i64 = 0x5741_4D50_0000_0001; // "STAMP" + 1
const LOCK_PRUNER: i64 = 0x5741_4D50_0000_0002;

#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// How often to look for orphaned jobs.
    pub reap_interval: Duration,
    /// A worker silent for longer than this has its jobs reclaimed. Must be
    /// comfortably more than the worker's heartbeat interval — see
    /// `WorkerConfig::heartbeat_interval`.
    pub dead_after: Duration,
    pub prune_interval: Duration,
    /// Finished jobs older than this are deleted.
    pub retain_for: Duration,
    /// Rows per prune batch. River's shape: bounded batches, looped, never one
    /// unbounded DELETE (plans/13).
    pub prune_batch: i32,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            reap_interval: Duration::from_secs(60),
            dead_after: Duration::from_secs(300),
            prune_interval: Duration::from_secs(60 * 60),
            retain_for: Duration::from_secs(14 * 24 * 60 * 60),
            prune_batch: 10_000,
        }
    }
}

pub struct Maintenance {
    queue: Queue,
    config: MaintenanceConfig,
}

impl Maintenance {
    pub fn new(queue: Queue, config: MaintenanceConfig) -> Self {
        Self { queue, config }
    }

    /// Run both loops until shutdown.
    pub async fn run(self, shutdown: watch::Receiver<bool>) -> Result<()> {
        let reaper = tokio::spawn(reap_loop(
            self.queue.clone(),
            self.config.clone(),
            shutdown.clone(),
        ));
        let pruner = tokio::spawn(prune_loop(self.queue.clone(), self.config, shutdown));

        let _ = tokio::try_join!(reaper, pruner);
        Ok(())
    }
}

async fn reap_loop(queue: Queue, config: MaintenanceConfig, mut shutdown: watch::Receiver<bool>) {
    let mut ticker = tokio::time::interval(config.reap_interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => break,
        }

        match with_advisory_lock(&queue, LOCK_REAPER, || queue.reap(config.dead_after)).await {
            Ok(Some(0)) => debug!("reaper: nothing to reclaim"),
            Ok(Some(n)) => info!(reclaimed = n, "reaper: reclaimed jobs from dead workers"),
            // Another node holds the lock. Normal, not an error.
            Ok(None) => debug!("reaper: another node is reaping"),
            Err(e) => warn!(error = %e, "reaper failed"),
        }
    }
}

async fn prune_loop(queue: Queue, config: MaintenanceConfig, mut shutdown: watch::Receiver<bool>) {
    let mut ticker = tokio::time::interval(config.prune_interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => break,
        }

        let result = with_advisory_lock(&queue, LOCK_PRUNER, || async {
            // Loop until a short batch. A single unbounded DELETE holds locks
            // and bloats worse than what it reclaims (plans/13).
            let mut total = 0u64;
            loop {
                let n = queue.prune(config.retain_for, config.prune_batch).await?;
                total += n;
                if n < config.prune_batch as u64 {
                    break;
                }
            }
            Ok(total)
        })
        .await;

        match result {
            Ok(Some(0)) => debug!("pruner: nothing to prune"),
            Ok(Some(n)) => info!(pruned = n, "pruner: deleted finished jobs"),
            Ok(None) => debug!("pruner: another node is pruning"),
            Err(e) => warn!(error = %e, "pruner failed"),
        }
    }
}

/// Run `f` while holding a session advisory lock, or return `Ok(None)` if
/// another node holds it.
///
/// **The connection is pinned deliberately.** `pg_try_advisory_lock` is
/// SESSION-scoped, and a pool hands out an arbitrary connection per query — so
/// locking on one connection and unlocking on another leaves the lock held for
/// the life of the pool, and every later attempt silently returns false. The
/// reaper would stop reaping and nothing would look broken. Hold one connection
/// for the whole critical section.
///
/// `pg_try_advisory_lock` rather than the blocking form: a node that cannot get
/// the lock should skip this tick, not queue behind the holder and then run
/// redundant work the moment it is released.
async fn with_advisory_lock<F, Fut, T>(queue: &Queue, key: i64, f: F) -> Result<Option<T>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    use sqlx::Acquire;

    let mut conn = queue.pool().acquire().await?;

    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(key)
        .fetch_one(conn.acquire().await?)
        .await?;

    if !acquired {
        return Ok(None);
    }

    let result = f().await;

    // Released on the SAME connection. Also released implicitly if this process
    // dies, which is why no liveness table is needed here.
    let _ = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(key)
        .fetch_one(conn.acquire().await?)
        .await;

    result.map(Some)
}

#[cfg(test)]
mod tests;
