//! Background maintenance: reaping orphans and pruning finished jobs.
//!
//! Both are singletons across the fleet. Running the reaper on every node would
//! be harmless but wasteful; running the pruner on every node would multiply
//! DELETE load on the one table that is already the bloat risk. Exclusion is a
//! Postgres advisory lock — no extra table, and it is released automatically if
//! the holder dies, which is the property a heartbeat table would have to
//! reimplement.

use crate::error::Result;
use crate::metrics::{Metrics, label, names};
use crate::queue::Queue;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Advisory lock keys. Arbitrary but must be stable and distinct; sharing a key
/// with another subsystem would silently serialise unrelated work.
const LOCK_REAPER: i64 = 0x5741_4D50_0000_0001; // "STAMP" + 1
const LOCK_PRUNER: i64 = 0x5741_4D50_0000_0002;
// NOTE: the scheduler needs NO advisory lock. Its exclusion is the row lock
// taken by stampeded_claim_due_schedules (FOR UPDATE SKIP LOCKED) and held for
// the whole tick transaction -- the schedule row IS the election. Every node
// may run the tick safely and concurrently.

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
    /// Cron occurrence rows older than this are deleted.
    ///
    /// Its OWN horizon, separate from job retention, which is the whole reason
    /// the ledger is a separate table (plans/11). Must stay comfortably longer
    /// than any window in which the same occurrence could be re-derived, or the
    /// idempotency backstop silently stops backstopping.
    pub retain_occurrences_for: Duration,
    /// Rows per prune batch. River's shape: bounded batches, looped, never one
    /// unbounded DELETE (plans/13).
    pub prune_batch: i32,
    /// How often to fire due schedules. Safe on every node concurrently.
    pub schedule_interval: Duration,

    /// How long a DISCARDED or CANCELLED job is kept.
    ///
    /// Separate from `retain_for`, which covers completed jobs. A completed job
    /// is evidence nobody will read; a discarded one is evidence somebody is
    /// about to be paged about. Oban's 60-second default deletes the failure
    /// before the alert fires; River's 7 days is the sane number.
    pub retain_failed_for: Duration,
    /// Ceiling on a single prune statement. Exceeding it halves the batch
    /// rather than letting a long DELETE hold locks.
    pub prune_statement_timeout: Duration,
    /// How often to expire jobs that blew their wall-clock budget.
    pub expire_interval: Duration,
    /// How often to check autovacuum standing and index density. Hourly is
    /// plenty: this diagnoses a slow drift, not an incident.
    pub health_interval: Duration,
    /// Rebuild indexes that measure sparse. `false` reports and leaves them,
    /// which is the right default for anyone who would rather schedule their
    /// own maintenance window.
    pub auto_reindex: bool,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            reap_interval: Duration::from_secs(60),
            dead_after: Duration::from_secs(300),
            prune_interval: Duration::from_secs(60 * 60),
            retain_for: Duration::from_secs(14 * 24 * 60 * 60),
            prune_batch: 10_000,
            schedule_interval: Duration::from_secs(5),
            retain_failed_for: Duration::from_secs(7 * 24 * 3600),
            retain_occurrences_for: Duration::from_secs(30 * 24 * 3600),
            prune_statement_timeout: Duration::from_secs(30),
            expire_interval: Duration::from_secs(30),
            health_interval: Duration::from_secs(3600),
            auto_reindex: false,
        }
    }
}

pub struct Maintenance {
    queue: Queue,
    config: MaintenanceConfig,
    metrics: Metrics,
}

impl Maintenance {
    pub fn new(queue: Queue, config: MaintenanceConfig) -> Self {
        Self {
            queue,
            config,
            metrics: Metrics::noop(),
        }
    }

    /// Send measurements somewhere. Defaults to nowhere.
    pub fn with_metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Run both loops until shutdown.
    pub async fn run(self, shutdown: watch::Receiver<bool>) -> Result<()> {
        let reaper = tokio::spawn(reap_loop(
            self.queue.clone(),
            self.config.clone(),
            shutdown.clone(),
        ));
        let scheduler = tokio::spawn(schedule_loop(
            self.queue.clone(),
            self.config.clone(),
            shutdown.clone(),
        ));
        let pruner = tokio::spawn(prune_loop(
            self.queue.clone(),
            self.config.clone(),
            self.metrics.clone(),
            shutdown.clone(),
        ));
        let expirer = tokio::spawn(expire_loop(
            self.queue.clone(),
            self.config.clone(),
            self.metrics.clone(),
            shutdown.clone(),
        ));
        let health = tokio::spawn(health_loop(
            self.queue.clone(),
            self.config,
            self.metrics,
            shutdown,
        ));

        let _ = tokio::try_join!(reaper, scheduler, pruner, expirer, health);
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

async fn prune_loop(
    queue: Queue,
    config: MaintenanceConfig,
    metrics: Metrics,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(config.prune_interval);
    // Survives across ticks: a table big enough to time out once will do it
    // again, and starting over at the full batch every time means never
    // finishing. River's cleaner does the same.
    let mut batch = config.prune_batch;

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => break,
        }

        let result = with_advisory_lock(&queue, LOCK_PRUNER, || async {
            let mut total = 0u64;
            loop {
                let n = match queue
                    .prune_batch(
                        config.retain_for,
                        config.retain_failed_for,
                        batch,
                        config.prune_statement_timeout,
                    )
                    .await
                {
                    Ok(n) => n,
                    Err(e) if is_statement_timeout(&e) => {
                        // Halve and retry rather than give up: the table is
                        // large enough that this batch size cannot finish
                        // inside the timeout, and a pruner that stops on the
                        // first timeout never catches up on the table that
                        // most needs it. Floored so it cannot reach zero.
                        batch = (batch / 2).max(100);
                        warn!(new_batch = batch, "prune timed out; halving batch");
                        break;
                    }
                    Err(e) => return Err(e),
                };

                total += n;
                if n < batch as u64 {
                    break;
                }

                // Jittered pause between batches. Deleting continuously at full
                // speed starves the workers of the same table; River sleeps
                // between batches for exactly this reason.
                tokio::time::sleep(jittered(Duration::from_millis(50), Duration::from_secs(1)))
                    .await;
            }
            Ok(total)
        })
        .await;

        match result {
            Ok(Some(0)) => debug!("pruner: nothing to prune"),
            Ok(Some(n)) => {
                info!(pruned = n, "pruner: deleted finished jobs");
                metrics.count(names::MAINTENANCE_JOBS, n, &[label("outcome", "pruned")]);
            }
            Ok(None) => debug!("pruner: another node is pruning"),
            Err(e) => warn!(error = %e, "pruner failed"),
        }

        // The cron occurrence ledger, on its own horizon. Until this was wired
        // in, `stampeded_prune_occurrences` existed and was called from nowhere,
        // so the ledger grew one row per firing forever -- half a million a
        // year for a schedule running every minute.
        match queue.prune_occurrences(config.retain_occurrences_for).await {
            Ok(0) => {}
            Ok(n) => {
                info!(pruned = n, "pruner: deleted old cron occurrences");
                metrics.count(
                    names::MAINTENANCE_JOBS,
                    n,
                    &[label("outcome", "occurrences")],
                );
            }
            Err(e) => warn!(error = %e, "occurrence pruner failed"),
        }
    }
}

/// Fail jobs that blew their wall-clock budget.
///
/// No advisory lock: `stampeded_expire` takes each row with FOR UPDATE SKIP
/// LOCKED, so concurrent nodes take different rows — the same argument as the
/// scheduler.
async fn expire_loop(
    queue: Queue,
    config: MaintenanceConfig,
    metrics: Metrics,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(config.expire_interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => break,
        }

        match queue.expire(100).await {
            Ok(0) => {}
            Ok(n) => {
                warn!(
                    expired = n,
                    "expirer: jobs exceeded their wall-clock budget"
                );
                metrics.count(names::MAINTENANCE_JOBS, n, &[label("outcome", "expired")]);
            }
            Err(e) => warn!(error = %e, "expirer failed"),
        }
    }
}

/// Autovacuum standing, index density, and the queue gauges.
///
/// The gauges live here rather than in the worker because they are per-queue
/// facts, not per-worker ones, and emitting them from every worker would
/// multiply them by the fleet size.
async fn health_loop(
    queue: Queue,
    config: MaintenanceConfig,
    metrics: Metrics,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(config.health_interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => break,
        }

        match queue.vacuum_health().await {
            Ok(rows) => {
                for r in rows {
                    metrics.gauge(
                        "stampeded.table.dead_tuples",
                        r.dead_tuples as f64,
                        &[label("table", r.relname.clone())],
                    );
                    if r.needs_vacuum {
                        // NAMED, with the numbers. This is the whole value of
                        // the check: the queue's death spiral otherwise
                        // presents as "mysteriously slow", and an operator who
                        // has not seen it before has no reason to suspect
                        // autovacuum.
                        warn!(
                            table = %r.relname,
                            dead = r.dead_tuples,
                            live = r.live_tuples,
                            threshold = r.autovac_threshold,
                            last_autovacuum = ?r.last_autovacuum,
                            "autovacuum is behind on this table; claim latency will drift up \
                             until it catches up"
                        );
                    }
                }
            }
            Err(e) => debug!(error = %e, "vacuum health check unavailable"),
        }

        match queue.sparse_indexes().await {
            Ok(idx) if !idx.is_empty() => {
                for name in idx {
                    if config.auto_reindex {
                        info!(index = %name, "rebuilding sparse index");
                        if let Err(e) = queue.reindex(&name).await {
                            warn!(index = %name, error = %e, "reindex failed");
                        }
                    } else {
                        info!(
                            index = %name,
                            "index is sparse enough to be worth rebuilding \
                             (set auto_reindex to do it automatically)"
                        );
                    }
                }
            }
            Ok(_) => debug!("no sparse indexes"),
            Err(e) => debug!(error = %e, "index density check unavailable"),
        }

        // Two signals nobody in the survey exports, both cheap, both failures
        // that are invisible until they are catastrophic.
        if let Ok(usage) = queue.notification_queue_usage().await {
            metrics.gauge(names::NOTIFY_QUEUE_USAGE, usage, &[]);
            if usage > 0.5 {
                // Past full, every NOTIFY errors -- and the notify trigger runs
                // inside the INSERT, so ENQUEUE ITSELF starts failing. The
                // cause is a LISTENing backend stuck in a long transaction,
                // which is nowhere near where the error appears.
                warn!(
                    usage,
                    "the async notification queue is filling; a LISTENing backend is probably \
                     stuck in a long transaction. If it fills, NOTIFY errors -- and because the \
                     notify trigger runs inside the INSERT, enqueueing stops working."
                );
            }
        }

        if let Ok(skew) = queue.clock_skew().await {
            metrics.gauge(names::CLOCK_SKEW_SECONDS, skew, &[]);
            if skew.abs() > 60.0 {
                // Not a correctness problem here -- retry delays are durations
                // applied by the database, and the scheduler compares
                // server-side -- but it makes logs and job timestamps
                // irreconcilable, which costs an hour during an incident.
                warn!(
                    skew_seconds = skew,
                    "this host's clock differs from the database's by more than a minute; \
                     job timestamps and log timestamps will not line up"
                );
            }
        }

        if let Ok(ages) = queue.oldest_available_age().await {
            for (q, age) in ages {
                // The number that says whether a queue is being WORKED, as
                // opposed to how much is in it.
                metrics.gauge(names::QUEUE_OLDEST_AGE_SECONDS, age, &[label("queue", q)]);
            }
        }
    }
}

/// Is this a `statement_timeout` firing (SQLSTATE 57014)?
fn is_statement_timeout(e: &crate::Error) -> bool {
    matches!(e, crate::Error::Database(db) if db.as_database_error()
        .and_then(|d| d.code())
        .as_deref() == Some("57014"))
}

/// A duration uniformly in `[min, max)`.
///
/// No `rand` dependency for this: the nanoseconds of the current instant are
/// plenty of entropy for spacing out delete batches, and a job queue does not
/// need a CSPRNG to decide when to sleep.
fn jittered(min: Duration, max: Duration) -> Duration {
    let span = max.saturating_sub(min);
    if span.is_zero() {
        return min;
    }
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    min + Duration::from_nanos(n % span.as_nanos().max(1) as u64)
}

/// Fire due recurring schedules.
///
/// No advisory lock and no leader election: `stampeded_claim_due_schedules`
/// takes each schedule row with FOR UPDATE SKIP LOCKED, so concurrent nodes
/// simply take different rows. That is the whole reason the schedule row
/// carries `next_run_at` instead of the schedule living in config (plans/11).
async fn schedule_loop(
    queue: Queue,
    config: MaintenanceConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(config.schedule_interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => break,
        }

        match queue.run_due_schedules(100).await {
            Ok(0) => {}
            Ok(n) => info!(fired = n, "scheduler: enqueued jobs from schedules"),
            Err(e) => warn!(error = %e, "scheduler failed"),
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
