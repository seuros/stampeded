//! The queue — thin, deliberate wrappers over the SQL functions in
//! `schema/002_operations.sql`.
//!
//! These functions do NOT reimplement claim semantics in Rust. The fence, the
//! attempt accounting and the transition rules live in SQL precisely so that a
//! Python or Go worker calling `stampede_claim()` gets identical behaviour
//! (plans/05). Duplicating the logic here would create two implementations that
//! drift.

use crate::cron::{Cron, DstPolicy};
use crate::error::{Error, Result};
use crate::job::{FailureKind, Job, NewJob};
use crate::schedule::Schedule;
use chrono::{DateTime, Utc};
use jiff::Timestamp;
use jiff::tz::TimeZone;
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
    /// The threshold autovacuum will actually use for THIS table, computed from
    /// its own reloptions rather than the cluster defaults.
    pub autovac_threshold: f64,
    pub needs_vacuum: bool,
    pub last_autovacuum: Option<DateTime<Utc>>,
}

/// Ceiling on a dashboard query.
///
/// The console re-runs its panels every second, so a slow one is not a slow
/// page — it is a permanent load on the database for as long as a tab is open.
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
    pub last_failure: Option<DateTime<Utc>>,
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
    pub next_run_at: DateTime<Utc>,
    pub last_run_at: Option<DateTime<Utc>>,
}

/// Quote an SQL identifier for interpolation.
///
/// Only ever used on names read back out of `pg_class`, never on caller input —
/// but doubling embedded quotes anyway costs one line, and the alternative is a
/// function whose safety depends on where its argument came from.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Outcome of a terminal transition.
///
/// `owned == false` means the reaper (or an operator) took the job away while
/// we were running it. The worker must NOT write a result in that case — some
/// other worker now owns the outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub was: Option<String>,
    pub now: Option<String>,
    pub owned: bool,
}

impl Transition {
    /// Check what SQL actually did against the declared lifecycle.
    ///
    /// The SQL functions are authoritative — a polyglot worker calls them
    /// directly and never runs this code — so this cannot PREVENT anything. It
    /// exists to make drift loud: if a WHERE clause is edited and starts
    /// permitting a transition the table forbids, the alternative is finding
    /// out from a customer. `RETURNING OLD./NEW.` is what makes the check free;
    /// the before and after states come back from the same statement.
    pub fn verify(&self, event: crate::lifecycle::Transitions) {
        let (Some(was), Some(now)) = (&self.was, &self.now) else {
            return;
        };
        let Some(from) = parse_state(was) else { return };
        let Some(to) = parse_state(now) else { return };

        // The event enum carries payloads in general, so it is not Copy;
        // describe it before it is consumed.
        let described = format!("{event:?}");
        match crate::lifecycle::apply(from, event) {
            Some(expected) if expected == to => {}
            _ => warn!(
                %was, %now, event = %described,
                "the database performed a transition the lifecycle does not declare; \
                 schema/002_operations.sql and lifecycle.rs have drifted"
            ),
        }
    }
}

fn parse_state(s: &str) -> Option<crate::JobState> {
    use crate::JobState::*;
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

impl Queue {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Enqueue a job.
    ///
    /// Plain INSERT, no stored procedure: this is exactly what a caller in any
    /// language does with their own driver. `ON CONFLICT DO NOTHING` makes a
    /// duplicate enqueue a no-op rather than an error, so callers need not
    /// distinguish "already queued" from "failed".
    ///
    /// Returns `None` when a deduplicated job was already queued.
    pub async fn enqueue(&self, job: NewJob) -> Result<Option<Uuid>> {
        let id: Option<Uuid> = sqlx::query_scalar(
            r#"
            INSERT INTO stampede_jobs
                (queue, task, args, tenant_id, priority, max_attempts, scheduled_at,
                 dedup_scope, dedup_period, destination, dead_letter_queue, expire_seconds,
                 backoff_base_ms, backoff_max_ms)
            VALUES ($1, $2, $3, $4, $5, $6, coalesce($7, now()),
                    $8, $9, $10, $11, coalesce($12, 3600),
                    coalesce($13, 1000), coalesce($14, 3600000))
            ON CONFLICT DO NOTHING
            RETURNING id
            "#,
        )
        .bind(&job.queue)
        .bind(&job.task)
        .bind(&job.args)
        .bind(&job.tenant_id)
        .bind(job.priority)
        .bind(job.max_attempts)
        .bind(job.scheduled_at)
        .bind(&job.dedup_scope)
        .bind(&job.dedup_period)
        .bind(&job.destination)
        .bind(&job.dead_letter_queue)
        .bind(job.expire_seconds)
        .bind(job.backoff_base_ms)
        .bind(job.backoff_max_ms)
        .fetch_optional(&self.pool)
        .await?
        .flatten();

        Ok(id)
    }

    /// Claim up to `limit` jobs for this worker.
    pub async fn claim(&self, queue: &str, worker_id: Uuid, limit: i32) -> Result<Vec<Job>> {
        let jobs = sqlx::query_as::<_, Job>("SELECT * FROM stampede_claim($1, $2, $3, NULL)")
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
            sqlx::query_as("SELECT was, now_state, owned FROM stampede_complete($1, $2, $3)")
                .bind(&job.queue)
                .bind(job.id)
                .bind(worker_id)
                .fetch_optional(&self.pool)
                .await?;

        Ok(match row {
            Some((was, now, owned)) => Transition {
                was,
                now,
                owned: owned.unwrap_or(false),
            },
            // Zero rows means the WHERE clause did not match: we no longer own it.
            None => Transition {
                was: None,
                now: None,
                owned: false,
            },
        })
    }

    /// Record a failure.
    ///
    /// `retry_delay` comes from `chrono-machines` so the backoff formula lives
    /// in one place (plans/15) — but it is passed as a DELAY, not an instant,
    /// so the database applies its own clock and retry timing does not depend
    /// on app/DB clock agreement.
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

    /// `fail`, plus the exit status of a worker process that died.
    ///
    /// Separate entry point because only a supervising daemon has a status to
    /// report, and 137 (OOM) is the single highest-signal artefact in the
    /// survey: it turns "the job mysteriously stopped" into "the job needs more
    /// memory".
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
            "SELECT was, now_state, dead_lettered FROM stampede_fail($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(&job.queue)
        .bind(job.id)
        .bind(worker_id)
        .bind(kind.as_str())
        .bind(error)
        .bind(retry_delay.map(|d| d.as_millis() as i64))
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

    /// Tell the database a long-running job is still progressing.
    ///
    /// Layer 3 of the timeout stack. The worker heartbeat says the PROCESS is
    /// alive; this says this particular JOB is. Without it a legitimately slow
    /// job and a wedged one are indistinguishable, and the only available
    /// response is to guess a timeout.
    pub async fn touch(&self, job: &Job, worker_id: Uuid) -> Result<bool> {
        // Double Option: the outer is "no row came back", the inner is the
        // function returning NULL because the UPDATE matched nothing. Both mean
        // the same thing here -- this worker no longer owns the job.
        let ok: Option<Option<bool>> = sqlx::query_scalar("SELECT stampede_touch($1, $2, $3)")
            .bind(&job.queue)
            .bind(job.id)
            .bind(worker_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(ok.flatten().unwrap_or(false))
    }

    /// A handler context for this job, outside the worker loop.
    ///
    /// The worker builds one per job internally; this is for callers driving
    /// the queue directly — tests, and anyone embedding the claim loop rather
    /// than using `Worker`.
    pub fn context_for(&self, job: &Job, worker_id: Uuid) -> crate::worker::JobContext {
        crate::worker::JobContext::new(job.clone(), self.clone(), worker_id)
    }

    /// Save resume state.
    ///
    /// Returns the new sequence number, or `None` if this worker no longer owns
    /// the job — which is how a worker whose host wedged finds out the reaper
    /// reclaimed its work. That check is what stops a stale writer moving the
    /// cursor BACKWARDS over a newer worker's progress.
    ///
    /// Also counts as a heartbeat and resets the interruption budget: a job
    /// that just saved progress is demonstrably alive, and is demonstrably not
    /// the kind of job that kills workers (plans/17 §4). `attempt` is
    /// untouched, so a job that progresses one unit and then dies forever is
    /// still bounded by `max_attempts`.
    pub async fn checkpoint<T: serde::Serialize>(
        &self,
        job: &Job,
        worker_id: Uuid,
        state: &T,
    ) -> Result<Option<i32>> {
        self.checkpoint_on(&self.pool, job, worker_id, state).await
    }

    /// `checkpoint`, on a caller-supplied executor — normally a transaction.
    ///
    /// THIS IS THE ONE THAT CLOSES THE HAZARD. Saving the cursor as a separate
    /// write means a crash between the work and the cursor replays that chunk
    /// on resume, which for a job that is not idempotent is silent
    /// duplication. Run both in one transaction and the cursor can never claim
    /// work that was rolled back:
    ///
    /// ```ignore
    /// let mut tx = pool.begin().await?;
    /// import_chunk(&mut *tx, &rows).await?;
    /// queue.checkpoint_on(&mut *tx, &job, worker_id, &Cursor { at: n }).await?;
    /// tx.commit().await?;
    /// ```
    ///
    /// River offers `ResumableSetStepCursorTx` for exactly this reason, and
    /// arrived there first — this API exists because reading their
    /// implementation showed the plain version could not be made atomic.
    pub async fn checkpoint_on<'e, E, T>(
        &self,
        executor: E,
        job: &Job,
        worker_id: Uuid,
        state: &T,
    ) -> Result<Option<i32>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
        T: serde::Serialize,
    {
        let value = serde_json::to_value(state)?;
        let seq: Option<Option<i32>> =
            sqlx::query_scalar("SELECT stampede_checkpoint($1, $2, $3, $4)")
                .bind(&job.queue)
                .bind(job.id)
                .bind(worker_id)
                .bind(&value)
                .fetch_optional(executor)
                .await?;
        Ok(seq.flatten())
    }

    /// Fail jobs whose wall-clock budget ran out. Returns how many.
    ///
    /// Catches what neither other layer can: a worker that is alive and
    /// heartbeating normally but wedged on one job forever. The reaper only
    /// sees dead workers; the in-process timeout needs the worker to still be
    /// running our code and cooperating.
    pub async fn expire(&self, limit: i32) -> Result<u64> {
        let rows: Vec<(Uuid, String, Option<Uuid>)> =
            sqlx::query_as("SELECT out_id, out_queue, out_worker FROM stampede_expire($1)")
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.len() as u64)
    }

    /// Register this process as a worker. The returned id is the heartbeat
    /// identity and what the reaper checks.
    pub async fn register_worker(
        &self,
        hostname: &str,
        pid: i32,
        queues: &[String],
    ) -> Result<Uuid> {
        let id: Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO stampede_workers (hostname, pid, queues)
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
        )
        .bind(hostname)
        .bind(pid)
        .bind(queues)
        .fetch_one(&self.pool)
        .await?;

        Ok(id)
    }

    pub async fn heartbeat(&self, worker_id: Uuid) -> Result<()> {
        sqlx::query("SELECT stampede_heartbeat($1)")
            .bind(worker_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn deregister_worker(&self, worker_id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM stampede_workers WHERE id = $1")
            .bind(worker_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Reclaim jobs whose worker stopped heartbeating. Returns how many.
    pub async fn reap(&self, dead_after: std::time::Duration) -> Result<u64> {
        let rows: Vec<(Uuid, String, Uuid)> = sqlx::query_as(
            "SELECT id, queue, worker_id FROM stampede_reap(make_interval(secs => $1))",
        )
        .bind(dead_after.as_secs_f64())
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.len() as u64)
    }

    /// Delete one batch of finished jobs. Returns how many.
    ///
    /// TWO retention clocks (pg-boss's shape): a completed job is evidence
    /// nobody will read, a discarded one is evidence somebody is about to be
    /// paged about. One clock means either drowning in successes or deleting
    /// failures before anyone can look at them — and Oban's 60-second default
    /// does exactly the latter, destroying the evidence before the alert fires.
    ///
    /// `statement_timeout` is set for this statement alone. A prune that runs
    /// long on a huge table must be abandoned and retried smaller, not left to
    /// hold locks; the caller halves the batch when this returns a timeout.
    pub async fn prune_batch(
        &self,
        retain_completed: std::time::Duration,
        retain_failed: std::time::Duration,
        limit: i32,
        statement_timeout: std::time::Duration,
    ) -> Result<u64> {
        let mut conn = self.pool.acquire().await?;

        // LOCAL: reverts when the connection returns to the pool, so this
        // cannot leak onto an unrelated query later.
        // AssertSqlSafe: the only interpolated value is a u128 of our own
        // making. SET does not accept a bind parameter.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET LOCAL statement_timeout = {}",
            statement_timeout.as_millis().max(1)
        )))
        .execute(&mut *conn)
        .await?;

        let n: Option<i32> = sqlx::query_scalar(
            "SELECT stampede_prune(make_interval(secs => $1), $2, make_interval(secs => $3))",
        )
        .bind(retain_completed.as_secs_f64())
        .bind(limit)
        .bind(retain_failed.as_secs_f64())
        .fetch_one(&mut *conn)
        .await?;

        Ok(n.unwrap_or(0) as u64)
    }

    /// Delete old cron occurrence rows. Returns how many.
    ///
    /// The ledger is the idempotency backstop for recurring jobs: one row per
    /// firing, forever. A schedule running every minute writes 525,600 rows a
    /// year. It is deliberately a separate table from the jobs (plans/11) so it
    /// can have its OWN horizon -- good_job and solid_queue put the cron key on
    /// the jobs table and then had to guarantee those rows are never pruned,
    /// which couples cron correctness to the retention window.
    ///
    /// That horizon must stay comfortably longer than any window in which the
    /// same occurrence could be re-derived, or the backstop silently stops
    /// backstopping.
    pub async fn prune_occurrences(&self, older_than: std::time::Duration) -> Result<u64> {
        let n: Option<i32> =
            sqlx::query_scalar("SELECT stampede_prune_occurrences(make_interval(secs => $1))")
                .bind(older_than.as_secs_f64())
                .fetch_one(&self.pool)
                .await?;
        Ok(n.unwrap_or(0) as u64)
    }

    /// How full the async notification queue is, 0.0 to 1.0.
    ///
    /// plans/12 lists this as untested and notes that **none of the surveyed
    /// implementations export it**. The failure mode is specific and nasty: a
    /// backend that is LISTENing but stuck inside a long transaction pins the
    /// 8 GB queue, and once it fills, every `NOTIFY` starts erroring — which
    /// means every INSERT into the jobs table starts erroring, because the
    /// notify trigger runs inside it. Enqueue stops working, and the cause is
    /// nowhere near the symptom.
    ///
    /// Cheap: a single function call, no table access.
    pub async fn notification_queue_usage(&self) -> Result<f64> {
        sqlx::query_scalar("SELECT pg_notification_queue_usage()::float8")
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    /// Difference between this process's clock and the database's, in seconds.
    ///
    /// Positive means we are ahead. pg-boss measures this and warns past 60 s
    /// (`src/timekeeper.ts:298-337`), which is the right instinct: skew is
    /// invisible until it produces behaviour nobody can explain.
    ///
    /// Stampede is mostly IMMUNE by construction -- retry delays are passed as
    /// durations and the database applies its own clock (plans/15), and the
    /// scheduler compares `next_run_at` to `now()` server-side. This is
    /// therefore a diagnostic rather than a correctness input: a large skew
    /// means log timestamps will not line up with job timestamps, and any
    /// client-side reasoning about `scheduled_at` is wrong.
    pub async fn clock_skew(&self) -> Result<f64> {
        let db_now: DateTime<Utc> = sqlx::query_scalar("SELECT now()")
            .fetch_one(&self.pool)
            .await?;
        Ok((Utc::now() - db_now).num_milliseconds() as f64 / 1000.0)
    }

    /// Autovacuum health per partition.
    ///
    /// pg-boss's `#checkVacuum`, which plans/13 calls the most valuable
    /// operational feature in the survey: it turns the queue's characteristic
    /// death spiral from "mysterious slow backlog" into a named cause. The
    /// threshold compared against is the one autovacuum will ACTUALLY use for
    /// that table, from its own reloptions — comparing against the global
    /// default is why naive dead-tuple alerts cry wolf.
    pub async fn vacuum_health(&self) -> Result<Vec<VacuumHealth>> {
        let rows = sqlx::query_as::<_, VacuumHealth>(
            "SELECT relname, live_tuples, dead_tuples, dead_ratio, \
                    autovac_threshold, needs_vacuum, last_autovacuum \
               FROM stampede_vacuum_health()",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Partitions that would enqueue silently.
    ///
    /// The wakeup trigger is statement-level with a transition table, and
    /// Postgres does not clone those to partitions -- so a partition created
    /// with a raw `CREATE TABLE ... PARTITION OF` instead of
    /// `stampede_create_queue` has no trigger and notifies nobody.
    ///
    /// Worth surfacing because the symptom is silence rather than failure:
    /// jobs in that queue are still claimed correctly, just on the poll
    /// interval, so latency quietly becomes seconds for one queue and nothing
    /// anywhere says why.
    pub async fn partitions_without_wakeup(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT partition FROM stampede_partition_health() WHERE NOT has_notify",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(n,)| n).collect())
    }

    /// Indexes sparse enough to be worth rebuilding.
    ///
    /// Measured, not scheduled. River rebuilds everything at midnight; this
    /// reports what is actually sparse so the caller can rebuild only that.
    pub async fn sparse_indexes(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT indexname FROM stampede_index_density() WHERE sparse")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().map(|(n,)| n).collect())
    }

    /// Rebuild one index without blocking writes.
    ///
    /// CONCURRENTLY cannot run inside a transaction, hence a bare execute.
    /// A failure leaves an invalid `_ccnew` index behind, which
    /// `stampede_index_density` deliberately skips rather than retrying —
    /// retrying one is how a reindexer gets stuck in a loop.
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

    /// Per-destination health, as OBSERVED FROM THE JOB ROWS.
    ///
    /// NOT a read of any worker's circuit breaker. `breaker-machines` state
    /// lives in the worker process's memory, and the dashboard is normally
    /// served by a daemon running `--no-worker` -- a different process, often
    /// a different machine. Nothing can see another process's breaker.
    ///
    /// What this shows instead is the evidence the breaker would be reacting
    /// to: how many jobs for a destination failed recently and how. A run of
    /// `breaker_open` means some worker's circuit IS open; a run of `raised`
    /// against one host means one is probably about to be.
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
               FROM stampede_jobs \
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
    ///
    /// The same clustering `failure_summary` does over MCP: "47 jobs failed"
    /// is noise, "47 failed the same way against the same host" is a diagnosis.
    /// Uses the fingerprint index rather than scanning error text.
    pub async fn failure_clusters(&self, limit: i64) -> Result<Vec<FailureCluster>> {
        sqlx::query_as::<_, FailureCluster>(
            "SELECT failure_kind, min(left(last_error, 90)) AS sample, destination, \
                    count(*)::bigint AS n \
               FROM stampede_jobs \
              WHERE error_fingerprint IS NOT NULL \
              GROUP BY failure_kind, error_fingerprint, destination \
              ORDER BY 4 DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// A bounded slice of the job table, newest first.
    ///
    /// MEASURED COST, because the caveat deserved a number rather than a
    /// hedge. On a 200k-row partition holding 20 discarded jobs, asking for
    /// the discarded ones is a **parallel seq scan, 4,634 buffers**: `id` is
    /// uuidv7 so this walks the primary key backwards, but nothing indexes
    /// `state` outside the claim predicate, so a rare state means reading most
    /// of the partition to find 40 rows.
    ///
    /// No index was added. One on `(queue, state, id)` would make every insert
    /// and every transition pay, forever, for a dashboard that is usually not
    /// being watched — and the pathological case is specifically "rare state,
    /// huge partition", not the common one.
    ///
    /// Instead the damage is BOUNDED: a `statement_timeout`, because this
    /// query re-runs every second for as long as someone leaves the page open.
    /// A dashboard panel that cannot answer quickly should give up and say so,
    /// not quietly consume a core. The caller renders the timeout as an empty
    /// panel with an explanation.
    pub async fn recent_jobs(
        &self,
        queue: &str,
        state: Option<&str>,
        limit: i64,
    ) -> Result<Vec<Job>> {
        let mut conn = self.pool.acquire().await?;

        // LOCAL, so it reverts when the connection returns to the pool and
        // cannot leak onto an unrelated query.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET LOCAL statement_timeout = {}",
            DASHBOARD_QUERY_TIMEOUT.as_millis()
        )))
        .execute(&mut *conn)
        .await?;

        sqlx::query_as::<_, Job>(
            "SELECT * FROM stampede_jobs \
              WHERE queue = $1 AND ($2::text IS NULL OR state = $2) \
              ORDER BY id DESC LIMIT $3",
        )
        .bind(queue)
        .bind(state)
        .bind(limit)
        .fetch_all(&mut *conn)
        .await
        .map_err(Into::into)
    }

    /// Every queue that has rows, from the counters.
    pub async fn queues(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT queue FROM stampede_queue_counts WHERE n <> 0 ORDER BY 1",
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
               FROM stampede_schedules ORDER BY paused, next_run_at LIMIT 50",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Age of the oldest job still waiting to run, per queue.
    ///
    /// asynq's queue_latency, and the number that actually tells you whether a
    /// queue is healthy: depth says how much work there is, this says whether
    /// it is being done. 10,000 jobs with an oldest age of two seconds is fine;
    /// 3 jobs with an oldest age of an hour is broken, and depth alone reports
    /// the second as the quieter of the two.
    pub async fn oldest_available_age(&self) -> Result<Vec<(String, f64)>> {
        let rows = sqlx::query_as::<_, (String, f64)>(
            "SELECT queue, extract(epoch FROM now() - min(scheduled_at))::float8 \
               FROM stampede_jobs \
              WHERE state IN ('available','retryable') AND scheduled_at <= now() \
              GROUP BY queue",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Ensure a queue has its own partition, with the storage and autovacuum
    /// settings a partition cannot inherit (see `stampede_create_queue`).
    pub async fn create_queue(&self, queue: &str) -> Result<()> {
        sqlx::query("SELECT stampede_create_queue($1)")
            .bind(queue)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Fire any due recurring schedules. Returns how many jobs were enqueued.
    ///
    /// Safe to call concurrently from every node: the function claims schedule
    /// rows with FOR UPDATE SKIP LOCKED, so there is nothing to elect.
    pub async fn run_due_schedules(&self, limit: i32) -> Result<u64> {
        // ONE transaction for the whole tick. `stampede_claim_due_schedules`
        // takes FOR UPDATE SKIP LOCKED row locks, and those locks last exactly
        // as long as this transaction -- which is what stops two nodes firing
        // the same occurrence while Rust computes the next time. Committing
        // between the claim and the fire would give that guarantee away.
        let mut tx = self.pool.begin().await?;

        let due: Vec<Schedule> = sqlx::query_as("SELECT * FROM stampede_claim_due_schedules($1)")
            .bind(limit)
            .fetch_all(&mut *tx)
            .await?;

        let now = Utc::now();
        let mut fired = 0u64;

        for schedule in due {
            let next = match schedule.next_after(now) {
                Ok(next) => next,
                Err(e) => {
                    // A malformed cron expression must not take the other
                    // schedules down with it. Leave the row alone: it stays due,
                    // and the warning repeats every tick until someone fixes it,
                    // which is louder than silently pausing it.
                    warn!(key = %schedule.key, error = %e, "cannot schedule next run");
                    continue;
                }
            };

            let job: Option<Uuid> = sqlx::query_scalar("SELECT stampede_fire_schedule($1, $2)")
                .bind(&schedule.key)
                .bind(next)
                .fetch_one(&mut *tx)
                .await?;

            if job.is_some() {
                fired += 1;
            }
        }

        tx.commit().await?;
        Ok(fired)
    }

    /// Register or update a fixed-interval schedule.
    ///
    /// REJECTS a zero interval rather than clamping it. The first version did
    /// `.max(1)` to satisfy the CHECK, which meant asking for `every_secs: 0`
    /// silently produced a schedule firing EVERY SECOND -- an extreme value
    /// nobody asked for, from input that was plainly a mistake. Found by
    /// fuzzing the MCP tool, which then cheerfully reported "set to every 0s"
    /// while storing 1.
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

        sqlx::query(
            "INSERT INTO stampede_schedules (key, queue, task, interval_secs, cron_expr) \
             VALUES ($1, $2, $3, $4, NULL) \
             ON CONFLICT (key) DO UPDATE \
                SET queue = EXCLUDED.queue, task = EXCLUDED.task, \
                    interval_secs = EXCLUDED.interval_secs, cron_expr = NULL, \
                    updated_at = now()",
        )
        .bind(key)
        .bind(queue)
        .bind(task)
        .bind(every.as_secs() as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Register or update a cron schedule.
    ///
    /// The expression is parsed HERE rather than at fire time, so a typo is a
    /// rejected write instead of a row that warns once a second forever.
    ///
    /// `next_run_at` is seeded to the first occurrence after now — not to now —
    /// or a schedule created at 14:00 would fire its `0 3 * * *` immediately.
    ///
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

        // `None` takes the expression's own advice rather than guessing.
        let policy = policy.unwrap_or_else(|| cron.suggested_dst_policy());

        let now = Timestamp::from_second(Utc::now().timestamp())
            .unwrap_or(Timestamp::UNIX_EPOCH)
            .to_zoned(tz.clone());
        let first = cron.next_after(&now, &tz, policy).ok_or_else(|| {
            Error::InvalidSchedule(format!("{expr:?} has no occurrence within four years"))
        })?;
        let first = DateTime::from_timestamp(first.timestamp().as_second(), 0).unwrap_or_default();

        let dst_policy = match policy {
            DstPolicy::WallClock => "wall_clock",
            DstPolicy::Instant => "instant",
        };

        sqlx::query(
            "INSERT INTO stampede_schedules \
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
        .bind(first)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// One job by id.
    ///
    /// Takes the queue as well as the id because jobs are LIST-partitioned by
    /// queue: without it the planner cannot prune and reads every partition to
    /// find one row. The partition key is part of the primary key for the same
    /// reason.
    pub async fn fetch(&self, queue: &str, id: Uuid) -> Result<Option<Job>> {
        let job =
            sqlx::query_as::<_, Job>("SELECT * FROM stampede_jobs WHERE queue = $1 AND id = $2")
                .bind(queue)
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(job)
    }

    /// Queue depth by state — the dashboard and MCP `queue_status` tool.
    ///
    /// Reads `stampede_queue_counts`, NEVER `count(*)` over the job table.
    /// plans/13 item 6: an unbounded aggregate holds the xmin horizon open,
    /// which stops vacuum reclaiming dead tuples, which bloats the table, which
    /// makes the aggregate slower, which holds the horizon open longer. pg-boss
    /// documents that spiral (src/plans.ts:968-975). The dashboard runs this
    /// every second, so it is exactly the query that must not cause it.
    ///
    /// Zero rows are dropped: "0 discarded" is not information, and a counter
    /// row lingers after the last job of that state is pruned.
    pub async fn depth(&self, queue: &str) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query_as::<_, (String, i64)>(
            r#"
            SELECT state, n
              FROM stampede_queue_counts
             WHERE queue = $1 AND n <> 0
             ORDER BY state
            "#,
        )
        .bind(queue)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Rebuild a queue's counters from the jobs table.
    ///
    /// The one place an unbounded aggregate is allowed, because it is explicit,
    /// manual and rare. Do not put it on a timer.
    pub async fn recount(&self, queue: &str) -> Result<()> {
        sqlx::query("SELECT stampede_recount_queue($1)")
            .bind(queue)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
