//! Background maintenance: reaping orphans and pruning finished jobs.
//!
//! Reaper and pruner are fleet singletons via an advisory lock, released if the holder dies.

use crate::error::Result;
use crate::metrics::{Metrics, label, names};
use crate::planner::{Planner, Verdict};
use crate::queue::Queue;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

/// Advisory lock keys: must stay stable and distinct from any other subsystem's.
const LOCK_REAPER: i64 = 0x5741_4D50_0000_0001; // "STAMP" + 1
const LOCK_PRUNER: i64 = 0x5741_4D50_0000_0002;

#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// How often to look for orphaned jobs.
    pub reap_interval: Duration,
    /// A worker silent this long has its jobs reclaimed; keep well above its heartbeat interval.
    pub dead_after: Duration,
    pub prune_interval: Duration,
    /// Finished jobs older than this are deleted.
    pub retain_for: Duration,
    /// Cron occurrence rows older than this are deleted.
    ///
    /// Must outlast any window in which an occurrence could be re-derived, or dedup stops working.
    pub retain_occurrences_for: Duration,
    /// Rows per prune batch.
    pub prune_batch: i32,
    /// How often to fire due schedules. Safe on every node concurrently.
    pub schedule_interval: Duration,

    /// How long a discarded or cancelled job is kept; `retain_for` covers completed ones.
    pub retain_failed_for: Duration,
    /// Ceiling on a single prune statement; exceeding it halves the batch.
    pub prune_statement_timeout: Duration,
    /// How often to expire jobs that blew their wall-clock budget.
    pub expire_interval: Duration,
    /// How often to check autovacuum standing and index density.
    pub health_interval: Duration,
    /// How often the planner re-judges queues; keep well under the 30 s verdict staleness.
    pub plan_interval: Duration,
    /// Rebuild indexes that measure sparse; `false` only reports them.
    pub auto_reindex: bool,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            reap_interval: Duration::from_secs(60),
            dead_after: Duration::from_secs(300),
            prune_interval: Duration::from_hours(1),
            retain_for: Duration::from_hours(14 * 24),
            prune_batch: 10_000,
            schedule_interval: Duration::from_secs(5),
            retain_failed_for: Duration::from_hours(7 * 24),
            retain_occurrences_for: Duration::from_hours(30 * 24),
            prune_statement_timeout: Duration::from_secs(30),
            expire_interval: Duration::from_secs(30),
            health_interval: Duration::from_secs(3600),
            plan_interval: Duration::from_secs(1),
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
    #[must_use]
    pub fn with_metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Run both loops until shutdown.
    pub async fn run(self, shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut loops = tokio::task::JoinSet::new();
        loops.spawn(reap_loop(
            self.queue.clone(),
            self.config.clone(),
            shutdown.clone(),
        ));
        loops.spawn(schedule_loop(
            self.queue.clone(),
            self.config.clone(),
            shutdown.clone(),
        ));
        loops.spawn(prune_loop(
            self.queue.clone(),
            self.config.clone(),
            self.metrics.clone(),
            shutdown.clone(),
        ));
        loops.spawn(expire_loop(
            self.queue.clone(),
            self.config.clone(),
            self.metrics.clone(),
            shutdown.clone(),
        ));
        loops.spawn(plan_loop(
            self.queue.clone(),
            self.config.clone(),
            shutdown.clone(),
        ));
        loops.spawn(health_loop(
            self.queue.clone(),
            self.config,
            self.metrics,
            shutdown,
        ));

        // A panicking loop takes only itself down; log it so a dead loop is visible.
        while let Some(done) = loops.join_next().await {
            if let Err(e) = done {
                error!(error = %e, "maintenance loop died");
            }
        }
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
    // Survives across ticks: a table that timed out once will again at full size.
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
                        // Halve rather than give up; floored so it cannot reach zero.
                        batch = (batch / 2).max(100);
                        warn!(new_batch = batch, "prune timed out; halving batch");
                        break;
                    }
                    Err(e) => return Err(e),
                };

                total += n;
                if n < u64::from(batch.unsigned_abs()) {
                    break;
                }

                // Pause between batches so the workers on this table are not starved.
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

        // The cron occurrence ledger, on its own horizon.
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

/// Fail jobs that blew their wall-clock budget; SKIP LOCKED makes it safe on every node.
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

/// Autovacuum standing, index density, and the per-queue gauges.
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

        if let Ok(usage) = queue.notification_queue_usage().await {
            metrics.gauge(names::NOTIFY_QUEUE_USAGE, usage, &[]);
            if usage > 0.5 {
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
                // Not a correctness problem: delays and schedules are computed server-side.
                warn!(
                    skew_seconds = skew,
                    "this host's clock differs from the database's by more than a minute; \
                     job timestamps and log timestamps will not line up"
                );
            }
        }

        if let Ok(ages) = queue.oldest_available_age().await {
            for (q, age) in ages {
                // Whether the queue is being worked, as opposed to how much is in it.
                metrics.gauge(names::QUEUE_OLDEST_AGE_SECONDS, age, &[label("queue", q)]);
            }
        }
    }
}

/// Is this a `statement_timeout` firing (SQLSTATE 57014)?
fn is_statement_timeout(e: &crate::Error) -> bool {
    matches!(e, crate::Error::Database(db) if db.as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .as_deref() == Some("57014"))
}

/// A duration roughly uniform in `[min, max)`, seeded from the clock's nanoseconds.
fn jittered(min: Duration, max: Duration) -> Duration {
    let span = max.saturating_sub(min);
    if span.is_zero() {
        return min;
    }
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()));
    min + Duration::from_nanos_u128(u128::from(n) % span.as_nanos().max(1))
}

/// Fire due recurring schedules; SKIP LOCKED on the schedule row makes it safe on every node.
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

/// Judge every queue that has a policy and publish the verdicts.
///
/// No lock: every daemon computes the same verdicts, and writing one twice is harmless.
async fn plan_loop(queue: Queue, config: MaintenanceConfig, mut shutdown: watch::Receiver<bool>) {
    let planner = match Planner::new() {
        Ok(planner) => planner,
        Err(e) => {
            error!(error = %e, "planner: the queue policy does not lower; queues stay steady");
            return;
        }
    };
    let mut ticker = tokio::time::interval(config.plan_interval);
    let mut last: HashMap<String, Verdict> = HashMap::new();

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => break,
        }

        let snapshots = match queue.queue_snapshots().await {
            Ok(snapshots) => snapshots,
            Err(e) => {
                warn!(error = %e, "planner: could not read queue policies");
                continue;
            }
        };

        let decisions: Vec<_> = snapshots
            .iter()
            .map(|snapshot| (snapshot.queue.clone(), planner.decide(snapshot)))
            .collect();

        for (name, decision) in &decisions {
            // A queue first seen steady is the quiet default, not news.
            let changed = last
                .insert(name.clone(), decision.verdict)
                .map_or(decision.verdict != Verdict::Steady, |previous| {
                    previous != decision.verdict
                });
            if changed {
                info!(
                    queue = %name,
                    verdict = decision.verdict.as_str(),
                    reason = decision.reason.as_deref().unwrap_or(""),
                    "planner: verdict changed"
                );
            }
        }
        last.retain(|name, _| decisions.iter().any(|(q, _)| q == name));

        if let Err(e) = queue.record_verdicts(&decisions).await {
            warn!(error = %e, "planner: could not record verdicts");
        }
    }
}

/// Run `f` holding a session advisory lock, or return `Ok(None)` if another node holds it.
///
/// The lock is session-scoped, so lock and unlock must share one pinned connection.
async fn with_advisory_lock<F, Fut, T>(queue: &Queue, key: i64, f: F) -> Result<Option<T>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut conn = queue.pool().acquire().await?;

    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(key)
        .fetch_one(&mut *conn)
        .await?;

    if !acquired {
        return Ok(None);
    }

    let result = f().await;

    let released = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(key)
        .fetch_one(&mut *conn)
        .await;
    if !matches!(released, Ok(true)) {
        // Back in the pool still locked, it would stall this loop fleet-wide; closing releases it.
        warn!(
            key,
            ?released,
            "advisory unlock failed; closing the connection"
        );
        conn.close_on_drop();
    }

    result.map(Some)
}

#[cfg(test)]
mod tests;
