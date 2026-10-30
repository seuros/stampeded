//! Thin wrappers over the SQL functions in `schema/*.sql`, which own claim semantics
//! so every language's worker behaves the same; do not reimplement them here.

use crate::cron::{Cron, DstPolicy};
use crate::error::{Error, Result};
use crate::job::{FailureKind, Job, NewJob};
use crate::lifecycle::{self, Transitions};
use crate::planner::{Decision, QueueSnapshot};
use crate::schedule::Schedule;
use jiff::Timestamp;
use jiff::tz::TimeZone;
use jiff_sqlx::ToSqlx;
use sqlx::PgPool;
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Queue {
    pool: PgPool,
}

/// One partition's autovacuum standing.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct VacuumHealth {
    pub relname: String,
    pub live_tuples: i64,
    pub dead_tuples: i64,
    pub dead_ratio: f64,
    /// The threshold autovacuum uses for this table, from its own reloptions.
    pub autovac_threshold: f64,
    pub needs_vacuum: bool,
    #[sqlx(try_from = "crate::timestamp::NullableTimestamp")]
    pub last_autovacuum: Option<Timestamp>,
}

/// Ceiling on a dashboard query, which re-runs every second while a tab is open.
pub const DASHBOARD_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Per-destination failure evidence. See `destination_health`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DestinationHealth {
    pub destination: Option<String>,
    /// Jobs a worker DECLINED to run: its circuit was open.
    pub refused: i64,
    pub throttled: i64,
    pub failed: i64,
    pub waiting: i64,
    #[sqlx(try_from = "crate::timestamp::NullableTimestamp")]
    pub last_failure: Option<Timestamp>,
}

/// One tenant's live jobs. See `tenant_load`.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct TenantLoad {
    pub tenant: String,
    /// Available plus retryable.
    pub waiting: i64,
    /// Claimed plus executing.
    pub running: i64,
    /// Retryable because the fairness gate deferred them, not failed.
    pub throttled: i64,
}

/// One clustered failure signature.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FailureCluster {
    pub failure_kind: Option<String>,
    pub sample: Option<String>,
    pub destination: Option<String>,
    pub n: i64,
}

/// A schedule as the dashboard reads it.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ScheduleRow {
    pub key: String,
    pub queue: String,
    pub task: String,
    pub cron_expr: Option<String>,
    pub interval_secs: Option<i64>,
    pub timezone: String,
    pub dst_policy: String,
    pub paused: bool,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub next_run_at: Timestamp,
    #[sqlx(try_from = "crate::timestamp::NullableTimestamp")]
    pub last_run_at: Option<Timestamp>,
}

/// Quote an SQL identifier for interpolation.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Outcome of a terminal transition; `owned == false` means the job was taken away
/// and the worker must not write a result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub was: Option<String>,
    pub now: Option<String>,
    pub owned: bool,
}

impl Transition {
    /// From a `(was, now_state, owned)` row; zero rows means the fence did not match.
    fn from_row(row: Option<(Option<String>, Option<String>, Option<bool>)>) -> Self {
        match row {
            Some((was, now, owned)) => Self {
                was,
                now,
                owned: owned.unwrap_or(false),
            },
            None => Self {
                was: None,
                now: None,
                owned: false,
            },
        }
    }

    /// Warns when the database made a transition the lifecycle does not declare.
    pub fn verify(&self, event: crate::lifecycle::Transitions) {
        let (Some(was), Some(now)) = (&self.was, &self.now) else {
            return;
        };
        let Some(from) = parse_state(was) else { return };
        let Some(to) = parse_state(now) else { return };

        // Not Copy: describe the event before `apply` consumes it.
        let described = format!("{event:?}");
        match crate::lifecycle::apply(from, event) {
            Some(expected) if expected == to => {}
            _ => warn!(
                %was, %now, event = %described,
                "the database performed a transition the lifecycle does not declare; \
                 the schema and lifecycle.rs have drifted"
            ),
        }
    }
}

fn parse_state(s: &str) -> Option<crate::JobState> {
    use crate::JobState::{
        Available, Cancelled, Claimed, Completed, Discarded, Executing, Retryable,
    };
    Some(match s {
        "available" => Available,
        "claimed" => Claimed,
        "executing" => Executing,
        "retryable" => Retryable,
        "completed" => Completed,
        "discarded" => Discarded,
        "cancelled" => Cancelled,
        _ => return None,
    })
}

/// The database's clock, the one `next_run_at` is compared against (a transaction's start).
async fn db_now<'e, E>(executor: E) -> Result<Timestamp>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let now: jiff_sqlx::Timestamp = sqlx::query_scalar("SELECT now()")
        .fetch_one(executor)
        .await?;
    Ok(now.to_jiff())
}

impl Queue {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// A transaction with a LOCAL `statement_timeout`, so the setting never leaks to the pool.
    pub(crate) async fn begin_timed(
        &self,
        timeout: std::time::Duration,
    ) -> Result<sqlx::Transaction<'static, sqlx::Postgres>> {
        let mut tx = self.pool.begin().await?;
        // AssertSqlSafe: SET takes no bind parameter, and the value is our own integer.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET LOCAL statement_timeout = {}",
            timeout.as_millis().max(1)
        )))
        .execute(&mut *tx)
        .await?;
        Ok(tx)
    }

    /// Enqueue a job; `None` when a deduplicated job was already queued.
    pub async fn enqueue(&self, job: NewJob) -> Result<Option<Uuid>> {
        let id: Option<Uuid> = sqlx::query_scalar(
            r"
            INSERT INTO stampeded_jobs
                (queue, task, args, tenant_id, priority, max_attempts, scheduled_at,
                 dedup_scope, dedup_period, destination, dead_letter_queue, expire_seconds,
                 backoff_base_ms, backoff_max_ms, meta)
            VALUES ($1, $2, $3, $4, $5, $6, coalesce($7, now()),
                    $8, $9, $10, $11, coalesce($12, 3600),
                    coalesce($13, 1000), coalesce($14, 3600000), $15)
            ON CONFLICT DO NOTHING
            RETURNING id
            ",
        )
        .bind(&job.queue)
        .bind(&job.task)
        .bind(&job.args)
        .bind(&job.tenant_id)
        .bind(job.priority)
        .bind(job.max_attempts)
        .bind(job.scheduled_at.map(jiff_sqlx::Timestamp::from))
        .bind(&job.dedup_scope)
        .bind(&job.dedup_period)
        .bind(&job.destination)
        .bind(&job.dead_letter_queue)
        .bind(job.expire_seconds)
        .bind(job.backoff_base_ms)
        .bind(job.backoff_max_ms)
        .bind(&job.meta)
        .fetch_optional(&self.pool)
        .await?
        .flatten();

        Ok(id)
    }

    /// Claim up to `limit` jobs for this worker.
    pub async fn claim(&self, queue: &str, worker_id: Uuid, limit: i32) -> Result<Vec<Job>> {
        let jobs = sqlx::query_as::<_, Job>("SELECT * FROM stampeded_claim($1, $2, $3, NULL)")
            .bind(queue)
            .bind(worker_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        Ok(jobs)
    }

    /// Mark a job completed.
    pub async fn complete(&self, job: &Job, worker_id: Uuid) -> Result<Transition> {
        let row: Option<(Option<String>, Option<String>, Option<bool>)> =
            sqlx::query_as("SELECT was, now_state, owned FROM stampeded_complete($1, $2, $3, $4)")
                .bind(&job.queue)
                .bind(job.id)
                .bind(worker_id)
                .bind(job.attempt)
                .fetch_optional(&self.pool)
                .await?;

        Ok(Transition::from_row(row))
    }

    /// Give back a claimed job this worker never started; the claim is undone and no failure recorded.
    pub async fn release(&self, job: &Job, worker_id: Uuid) -> Result<Transition> {
        let row: Option<(Option<String>, Option<String>, Option<bool>)> =
            sqlx::query_as("SELECT was, now_state, owned FROM stampeded_release($1, $2, $3, $4)")
                .bind(&job.queue)
                .bind(job.id)
                .bind(worker_id)
                .bind(job.attempt)
                .fetch_optional(&self.pool)
                .await?;

        Ok(Transition::from_row(row))
    }

    /// Record a failure; `retry_delay` is a delay so the database applies its own clock.
    pub async fn fail(
        &self,
        job: &Job,
        worker_id: Uuid,
        kind: FailureKind,
        error: &str,
        retry_delay: Option<std::time::Duration>,
    ) -> Result<Transition> {
        self.fail_with_exit(job, worker_id, kind, error, retry_delay, None)
            .await
    }

    /// `fail`, plus the exit status of a worker process that died (137 is OOM).
    #[allow(clippy::too_many_arguments)]
    pub async fn fail_with_exit(
        &self,
        job: &Job,
        worker_id: Uuid,
        kind: FailureKind,
        error: &str,
        retry_delay: Option<std::time::Duration>,
        exit_status: Option<i32>,
    ) -> Result<Transition> {
        let row: Option<(Option<String>, Option<String>, Option<bool>)> = sqlx::query_as(
            "SELECT was, now_state, dead_lettered FROM stampeded_fail($1,$2,$3,$4,$5,$6,$7,$8)",
        )
        .bind(&job.queue)
        .bind(job.id)
        .bind(worker_id)
        .bind(job.attempt)
        .bind(kind.as_str())
        .bind(error)
        .bind(retry_delay.map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX)))
        .bind(exit_status)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some((was, now, _dlq)) => Transition {
                was,
                now,
                owned: true,
            },
            None => Transition {
                was: None,
                now: None,
                owned: false,
            },
        })
    }

    /// Tell the database this job, not just the process, is still progressing.
    pub async fn touch(&self, job: &Job, worker_id: Uuid) -> Result<bool> {
        // No row and a NULL result both mean this worker no longer owns the job.
        let ok: Option<Option<bool>> = sqlx::query_scalar("SELECT stampeded_touch($1, $2, $3, $4)")
            .bind(&job.queue)
            .bind(job.id)
            .bind(worker_id)
            .bind(job.attempt)
            .fetch_optional(&self.pool)
            .await?;
        Ok(ok.flatten().unwrap_or(false))
    }

    /// A handler context for this job, for callers driving the queue without `Worker`.
    pub fn context_for(&self, job: &Job, worker_id: Uuid) -> crate::worker::JobContext {
        crate::worker::JobContext::new(job.clone(), self.clone(), worker_id)
    }

    /// Save resume state; the new sequence number, or `None` if this worker no longer owns the job.
    /// Also a heartbeat, and resets the interruption budget but not `attempt`.
    pub async fn checkpoint<T: serde::Serialize + Sync>(
        &self,
        job: &Job,
        worker_id: Uuid,
        state: &T,
    ) -> Result<Option<i32>> {
        self.checkpoint_on(&self.pool, job, worker_id, state).await
    }

    /// `checkpoint` on a caller's executor, normally the transaction doing the work, so the
    /// cursor never covers work that was rolled back:
    ///
    /// ```ignore
    /// let mut tx = pool.begin().await?;
    /// import_chunk(&mut *tx, &rows).await?;
    /// queue.checkpoint_on(&mut *tx, &job, worker_id, &Cursor { at: n }).await?;
    /// tx.commit().await?;
    /// ```
    pub async fn checkpoint_on<'e, E, T>(
        &self,
        executor: E,
        job: &Job,
        worker_id: Uuid,
        state: &T,
    ) -> Result<Option<i32>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
        // Sync so the returned future is Send: `state` lives across the await.
        T: serde::Serialize + Sync,
    {
        let value = serde_json::to_value(state)?;
        let seq: Option<Option<i32>> =
            sqlx::query_scalar("SELECT stampeded_checkpoint($1, $2, $3, $4, $5)")
                .bind(&job.queue)
                .bind(job.id)
                .bind(worker_id)
                .bind(job.attempt)
                .bind(&value)
                .fetch_optional(executor)
                .await?;
        Ok(seq.flatten())
    }

    /// Fail jobs whose wall-clock budget ran out, even on a live worker. Returns how many.
    pub async fn expire(&self, limit: i32) -> Result<u64> {
        let rows: Vec<(Uuid, String, Option<Uuid>)> =
            sqlx::query_as("SELECT out_id, out_queue, out_worker FROM stampeded_expire($1)")
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.len() as u64)
    }

    /// Register this process as a worker; the id is its heartbeat identity.
    pub async fn register_worker(
        &self,
        hostname: &str,
        pid: i32,
        queues: &[String],
    ) -> Result<Uuid> {
        let id: Uuid = sqlx::query_scalar(
            r"
            INSERT INTO stampeded_workers (hostname, pid, queues)
            VALUES ($1, $2, $3)
            RETURNING id
            ",
        )
        .bind(hostname)
        .bind(pid)
        .bind(queues)
        .fetch_one(&self.pool)
        .await?;

        Ok(id)
    }

    pub async fn heartbeat(&self, worker_id: Uuid) -> Result<()> {
        sqlx::query("SELECT stampeded_heartbeat($1)")
            .bind(worker_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn deregister_worker(&self, worker_id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM stampeded_workers WHERE id = $1")
            .bind(worker_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Reclaim jobs whose worker stopped heartbeating. Returns how many.
    pub async fn reap(&self, dead_after: std::time::Duration) -> Result<u64> {
        let rows: Vec<(Uuid, String, Uuid)> = sqlx::query_as(
            "SELECT id, queue, worker_id FROM stampeded_reap(make_interval(secs => $1))",
        )
        .bind(dead_after.as_secs_f64())
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.len() as u64)
    }

    /// Delete a batch of finished jobs, completed and failed on separate clocks. Returns how many.
    /// `statement_timeout` bounds this statement alone; the caller halves the batch on timeout.
    pub async fn prune_batch(
        &self,
        retain_completed: std::time::Duration,
        retain_failed: std::time::Duration,
        limit: i32,
        statement_timeout: std::time::Duration,
    ) -> Result<u64> {
        let mut conn = self.begin_timed(statement_timeout).await?;
        let n: Option<i32> = sqlx::query_scalar(
            "SELECT stampeded_prune(make_interval(secs => $1), $2, make_interval(secs => $3))",
        )
        .bind(retain_completed.as_secs_f64())
        .bind(limit)
        .bind(retain_failed.as_secs_f64())
        .fetch_one(&mut *conn)
        .await?;

        // Must commit: sqlx rolls a dropped Transaction back, and the count would lie.
        conn.commit().await?;

        // A row count: never negative.
        Ok(u64::from(n.unwrap_or(0).unsigned_abs()))
    }

    /// Delete old cron occurrence rows. Returns how many.
    /// `older_than` must outlast any window in which an occurrence could be re-derived.
    pub async fn prune_occurrences(&self, older_than: std::time::Duration) -> Result<u64> {
        let n: Option<i32> =
            sqlx::query_scalar("SELECT stampeded_prune_occurrences(make_interval(secs => $1))")
                .bind(older_than.as_secs_f64())
                .fetch_one(&self.pool)
                .await?;
        // A row count: never negative.
        Ok(u64::from(n.unwrap_or(0).unsigned_abs()))
    }

    /// How full the async notification queue is, 0.0 to 1.0; once full, every enqueue fails.
    pub async fn notification_queue_usage(&self) -> Result<f64> {
        sqlx::query_scalar("SELECT pg_notification_queue_usage()::float8")
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    /// This process's clock minus the database's, in seconds; diagnostic only.
    pub async fn clock_skew(&self) -> Result<f64> {
        let db_now = db_now(&self.pool).await?;
        Ok(Timestamp::now().duration_since(db_now).as_secs_f64())
    }

    /// Autovacuum health per partition, against each table's own autovacuum threshold.
    pub async fn vacuum_health(&self) -> Result<Vec<VacuumHealth>> {
        let rows = sqlx::query_as::<_, VacuumHealth>(
            "SELECT relname, live_tuples, dead_tuples, dead_ratio, \
                    autovac_threshold, needs_vacuum, last_autovacuum \
               FROM stampeded_vacuum_health()",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Partitions without the wakeup trigger (made outside `stampeded_create_queue`), whose
    /// jobs are claimed only on the poll interval.
    pub async fn partitions_without_wakeup(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT partition FROM stampeded_partition_health() WHERE NOT has_notify",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(n,)| n).collect())
    }

    /// Indexes sparse enough to be worth rebuilding.
    pub async fn sparse_indexes(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT indexname FROM stampeded_index_density() WHERE sparse")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().map(|(n,)| n).collect())
    }

    /// Rebuild one index without blocking writes; CONCURRENTLY cannot run in a transaction.
    pub async fn reindex(&self, index: &str) -> Result<()> {
        // Identifier comes from pg_class, not from a caller.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "REINDEX INDEX CONCURRENTLY {}",
            quote_ident(index)
        )))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Per-destination failure evidence from the job rows, not any worker's circuit breaker.
    pub async fn destination_health(
        &self,
        window: std::time::Duration,
    ) -> Result<Vec<DestinationHealth>> {
        sqlx::query_as::<_, DestinationHealth>(
            "SELECT destination, \
                    count(*) FILTER (WHERE failure_kind = 'breaker_open')::bigint AS refused, \
                    count(*) FILTER (WHERE failure_kind = 'throttled')::bigint AS throttled, \
                    count(*) FILTER (WHERE failure_kind IN ('raised','timed_out'))::bigint AS failed, \
                    count(*) FILTER (WHERE state = 'retryable')::bigint AS waiting, \
                    max(finished_at) AS last_failure \
               FROM stampeded_jobs \
              WHERE destination IS NOT NULL \
                AND failure_kind IS NOT NULL \
                AND coalesce(finished_at, scheduled_at) > now() - make_interval(secs => $1) \
              GROUP BY destination \
              ORDER BY 2 DESC, 4 DESC \
              LIMIT 20",
        )
        .bind(window.as_secs_f64())
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Recent failures, clustered by fingerprint.
    pub async fn failure_clusters(&self, limit: i64) -> Result<Vec<FailureCluster>> {
        sqlx::query_as::<_, FailureCluster>(
            "SELECT failure_kind, min(left(last_error, 90)) AS sample, destination, \
                    count(*)::bigint AS n \
               FROM stampeded_jobs \
              WHERE error_fingerprint IS NOT NULL \
              GROUP BY failure_kind, error_fingerprint, destination \
              ORDER BY 4 DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// A bounded slice of the job table, newest first, under [`DASHBOARD_QUERY_TIMEOUT`].
    /// `state` is unindexed, so a rare state on a big partition scans most of it.
    pub async fn recent_jobs(
        &self,
        queue: &str,
        state: Option<&str>,
        limit: i64,
    ) -> Result<Vec<Job>> {
        let mut conn = self.begin_timed(DASHBOARD_QUERY_TIMEOUT).await?;
        let rows = sqlx::query_as::<_, Job>(
            "SELECT * FROM stampeded_jobs \
              WHERE queue = $1 AND ($2::text IS NULL OR state = $2) \
              ORDER BY id DESC LIMIT $3",
        )
        .bind(queue)
        .bind(state)
        .bind(limit)
        .fetch_all(&mut *conn)
        .await?;

        // Read-only, but commit so the connection returns cleanly instead of via an abort.
        conn.commit().await?;

        Ok(rows)
    }

    /// Live jobs per tenant, busiest backlog first, under the dashboard timeout.
    pub async fn tenant_load(&self, limit: i64) -> Result<Vec<TenantLoad>> {
        let mut conn = self.begin_timed(DASHBOARD_QUERY_TIMEOUT).await?;
        let rows = sqlx::query_as::<_, TenantLoad>(
            "SELECT tenant_id AS tenant, \
                    count(*) FILTER (WHERE state IN ('available','retryable'))::bigint AS waiting, \
                    count(*) FILTER (WHERE state IN ('claimed','executing'))::bigint AS running, \
                    count(*) FILTER (WHERE state = 'retryable' \
                                       AND failure_kind = 'throttled')::bigint AS throttled \
               FROM stampeded_jobs \
              WHERE tenant_id IS NOT NULL \
                AND state IN ('available','retryable','claimed','executing') \
              GROUP BY tenant_id \
              ORDER BY waiting DESC, tenant_id \
              LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&mut *conn)
        .await?;

        // Read-only, but commit so the connection returns cleanly instead of via an abort.
        conn.commit().await?;
        Ok(rows)
    }

    /// Every queue that has rows, from the counters.
    pub async fn queues(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT queue FROM stampeded_queue_counts WHERE n <> 0 ORDER BY 1",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(q,)| q).collect())
    }

    /// Schedules and when they next fire.
    pub async fn schedules(&self) -> Result<Vec<ScheduleRow>> {
        sqlx::query_as::<_, ScheduleRow>(
            "SELECT key, queue, task, cron_expr, interval_secs, timezone, \
                    dst_policy, paused, next_run_at, last_run_at \
               FROM stampeded_schedules ORDER BY paused, next_run_at LIMIT 50",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Age of the oldest job still waiting to run, per queue.
    pub async fn oldest_available_age(&self) -> Result<Vec<(String, f64)>> {
        let rows = sqlx::query_as::<_, (String, f64)>(
            "SELECT queue, extract(epoch FROM now() - min(scheduled_at))::float8 \
               FROM stampeded_jobs \
              WHERE state IN ('available','retryable') AND scheduled_at <= now() \
              GROUP BY queue",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Every queue with a policy, with what the planner judges it on.
    /// The wait is the claim order's head, one index entry per queue, not the oldest due job.
    pub async fn queue_snapshots(&self) -> Result<Vec<QueueSnapshot>> {
        let rows = sqlx::query_as::<_, QueueSnapshot>(
            "SELECT p.queue, p.max_running, p.max_wait_secs, \
                    coalesce((SELECT sum(c.n) FROM stampeded_queue_counts c \
                               WHERE c.queue = p.queue \
                                 AND c.state IN ('claimed','executing')), 0)::bigint AS running, \
                    extract(epoch FROM now() - h.scheduled_at)::float8 AS head_wait_secs \
               FROM stampeded_queue_policies p \
               LEFT JOIN LATERAL ( \
                    SELECT j.scheduled_at FROM stampeded_jobs j \
                     WHERE j.queue = p.queue \
                       AND j.state IN ('available','retryable') \
                       AND j.scheduled_at <= now() \
                       AND j.attempt < j.max_attempts \
                     ORDER BY j.priority, j.scheduled_at \
                     LIMIT 1) h ON true \
              ORDER BY p.queue",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Publish the planner's verdicts, rewriting a row only when it changed or nears staleness.
    pub async fn record_verdicts(&self, decisions: &[(String, Decision)]) -> Result<u64> {
        let queues: Vec<&str> = decisions.iter().map(|(q, _)| q.as_str()).collect();
        let verdicts: Vec<&str> = decisions.iter().map(|(_, d)| d.verdict.as_str()).collect();
        let reasons: Vec<Option<&str>> =
            decisions.iter().map(|(_, d)| d.reason.as_deref()).collect();

        let done = sqlx::query(
            "UPDATE stampeded_queue_policies p \
                SET verdict = d.verdict, reason = d.reason, decided_at = now() \
               FROM unnest($1::text[], $2::text[], $3::text[]) AS d(queue, verdict, reason) \
              WHERE p.queue = d.queue \
                AND (p.verdict IS DISTINCT FROM d.verdict \
                     OR p.reason IS DISTINCT FROM d.reason \
                     OR p.decided_at IS NULL \
                     OR p.decided_at < now() - interval '10 seconds')",
        )
        .bind(&queues)
        .bind(&verdicts)
        .bind(&reasons)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected())
    }

    /// Ensure a queue has its own partition, with the settings a partition cannot inherit.
    pub async fn create_queue(&self, queue: &str) -> Result<()> {
        sqlx::query("SELECT stampeded_create_queue($1)")
            .bind(queue)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Fire any due recurring schedules. Returns how many jobs were enqueued.
    /// Safe to call from every node at once: schedule rows are claimed SKIP LOCKED.
    pub async fn run_due_schedules(&self, limit: i32) -> Result<u64> {
        // One transaction: the SKIP LOCKED row locks must hold until the next run is written.
        let mut tx = self.pool.begin().await?;
        // The claim's clock, not ours: a lagging host clock would put the next run in the past.
        let now = db_now(&mut *tx).await?;

        // Cron rows never seeded (next_run_at = created_at) get their first real occurrence
        // instead of firing on creation.
        let unseeded: Vec<Schedule> = sqlx::query_as(
            "SELECT * FROM stampeded_schedules \
              WHERE cron_expr IS NOT NULL AND last_run_at IS NULL \
                AND next_run_at = created_at AND NOT paused \
              ORDER BY key LIMIT $1 \
              FOR UPDATE SKIP LOCKED",
        )
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;

        for schedule in unseeded {
            // A malformed expression stays due; the claim below warns about it.
            let Ok(first) = schedule.first_after(now) else {
                continue;
            };
            sqlx::query(
                "UPDATE stampeded_schedules SET next_run_at = $2, updated_at = now() \
                  WHERE key = $1",
            )
            .bind(&schedule.key)
            .bind(first.to_sqlx())
            .execute(&mut *tx)
            .await?;
        }

        let due: Vec<Schedule> = sqlx::query_as("SELECT * FROM stampeded_claim_due_schedules($1)")
            .bind(limit)
            .fetch_all(&mut *tx)
            .await?;

        let mut fired = 0u64;

        for schedule in due {
            let next = match schedule.next_after(now) {
                Ok(next) => next,
                Err(e) => {
                    // Leave a malformed row due: it warns every tick instead of pausing silently.
                    warn!(key = %schedule.key, error = %e, "cannot schedule next run");
                    continue;
                }
            };

            let job: Option<Uuid> = sqlx::query_scalar("SELECT stampeded_fire_schedule($1, $2)")
                .bind(&schedule.key)
                .bind(next.to_sqlx())
                .fetch_one(&mut *tx)
                .await?;

            if job.is_some() {
                fired += 1;
            }
        }

        tx.commit().await?;
        Ok(fired)
    }

    /// Register or update a fixed-interval schedule; a zero interval is an error, not clamped.
    pub async fn upsert_schedule(
        &self,
        key: &str,
        queue: &str,
        task: &str,
        every: std::time::Duration,
    ) -> Result<()> {
        if key.trim().is_empty() {
            return Err(Error::InvalidSchedule(
                "a schedule needs a key: it is the primary key and how you pause or remove it"
                    .into(),
            ));
        }
        if every.as_secs() == 0 {
            return Err(Error::InvalidSchedule(format!(
                "schedule {key:?}: the interval must be at least one second"
            )));
        }
        // Not `as i64`: a huge interval would wrap negative into an opaque CHECK violation.
        let secs = i64::try_from(every.as_secs()).map_err(|_| {
            Error::InvalidSchedule(format!("schedule {key:?}: the interval is too large"))
        })?;

        sqlx::query(
            "INSERT INTO stampeded_schedules (key, queue, task, interval_secs, cron_expr) \
             VALUES ($1, $2, $3, $4, NULL) \
             ON CONFLICT (key) DO UPDATE \
                SET queue = EXCLUDED.queue, task = EXCLUDED.task, \
                    interval_secs = EXCLUDED.interval_secs, cron_expr = NULL, \
                    updated_at = now()",
        )
        .bind(key)
        .bind(queue)
        .bind(task)
        .bind(secs)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Register or update a cron schedule, seeded to its first occurrence after now.
    /// `policy: None` uses [`Cron::suggested_dst_policy`].
    pub async fn upsert_cron_schedule(
        &self,
        key: &str,
        queue: &str,
        task: &str,
        expr: &str,
        timezone: &str,
        policy: Option<DstPolicy>,
    ) -> Result<()> {
        if key.trim().is_empty() {
            return Err(Error::InvalidSchedule(
                "a schedule needs a key: it is the primary key and how you pause or remove it"
                    .into(),
            ));
        }
        let cron = Cron::parse(expr).map_err(|e| Error::InvalidSchedule(e.to_string()))?;
        let tz = TimeZone::get(timezone)
            .map_err(|e| Error::InvalidSchedule(format!("unknown timezone {timezone:?}: {e}")))?;

        let policy = policy.unwrap_or_else(|| cron.suggested_dst_policy());

        // The database's now, which the claim will compare the seed against.
        let now = db_now(&self.pool).await?.to_zoned(tz.clone());
        let first = cron.next_after(&now, &tz, policy).ok_or_else(|| {
            Error::InvalidSchedule(format!("{expr:?} has no occurrence within four years"))
        })?;
        let first = first.timestamp();

        let dst_policy = match policy {
            DstPolicy::WallClock => "wall_clock",
            DstPolicy::Instant => "instant",
        };

        sqlx::query(
            "INSERT INTO stampeded_schedules \
                 (key, queue, task, cron_expr, interval_secs, timezone, dst_policy, next_run_at) \
             VALUES ($1, $2, $3, $4, NULL, $5, $6, $7) \
             ON CONFLICT (key) DO UPDATE \
                SET queue = EXCLUDED.queue, task = EXCLUDED.task, \
                    cron_expr = EXCLUDED.cron_expr, interval_secs = NULL, \
                    timezone = EXCLUDED.timezone, dst_policy = EXCLUDED.dst_policy, \
                    next_run_at = EXCLUDED.next_run_at, updated_at = now()",
        )
        .bind(key)
        .bind(queue)
        .bind(task)
        .bind(expr)
        .bind(timezone)
        .bind(dst_policy)
        .bind(first.to_sqlx())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// One job by id; the queue lets Postgres prune to a single partition.
    pub async fn fetch(&self, queue: &str, id: Uuid) -> Result<Option<Job>> {
        let job =
            sqlx::query_as::<_, Job>("SELECT * FROM stampeded_jobs WHERE queue = $1 AND id = $2")
                .bind(queue)
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(job)
    }

    /// Cancel a job that has not started, freeing its dedup slot. `false` when it is
    /// executing or finished: an executing job is its worker's, and flipping the row
    /// would not stop the work.
    pub async fn cancel(&self, queue: &str, id: Uuid) -> Result<bool> {
        let n = sqlx::query(
            "UPDATE stampeded_jobs \
                SET state = 'cancelled', failure_kind = 'cancelled', \
                    finished_at = now(), dedup_scope = NULL \
              WHERE queue = $1 AND id = $2 AND state = ANY($3)",
        )
        .bind(queue)
        .bind(id)
        .bind(lifecycle::sources(&Transitions::Cancel))
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n == 1)
    }

    /// Run a discarded or cancelled job again with every budget it was given up on
    /// reset: attempts, interruptions and deferrals. `false` in any other state.
    pub async fn resurrect(&self, queue: &str, id: Uuid) -> Result<bool> {
        let n = sqlx::query(
            "UPDATE stampeded_jobs \
                SET state = 'available', attempt = 0, interruptions = 0, deferrals = 0, \
                    failure_kind = NULL, last_error = NULL, finished_at = NULL, \
                    scheduled_at = now() \
              WHERE queue = $1 AND id = $2 AND state = ANY($3)",
        )
        .bind(queue)
        .bind(id)
        .bind(lifecycle::sources(&Transitions::Resurrect))
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n == 1)
    }

    /// Nonzero queue depth by state from the counters; a `count(*)` here would pin the xmin
    /// horizon every second the dashboard polls.
    pub async fn depth(&self, queue: &str) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query_as::<_, (String, i64)>(
            r"
            SELECT state, n
              FROM stampeded_queue_counts
             WHERE queue = $1 AND n <> 0
             ORDER BY state
            ",
        )
        .bind(queue)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Rebuild a queue's counters from the jobs table: an unbounded scan, so never on a timer.
    pub async fn recount(&self, queue: &str) -> Result<()> {
        sqlx::query("SELECT stampeded_recount_queue($1)")
            .bind(queue)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
