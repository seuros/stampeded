//! The queue — thin, deliberate wrappers over the SQL functions in
//! `schema/002_operations.sql`.
//!
//! These functions do NOT reimplement claim semantics in Rust. The fence, the
//! attempt accounting and the transition rules live in SQL precisely so that a
//! Python or Go worker calling `stampede_claim()` gets identical behaviour
//! (plans/05). Duplicating the logic here would create two implementations that
//! drift.

use crate::error::Result;
use crate::job::{FailureKind, Job, NewJob};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Queue {
    pool: PgPool,
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
                (queue, task, args, tenant_id, priority, max_attempts, scheduled_at, dedup_scope)
            VALUES ($1, $2, $3, $4, $5, $6, coalesce($7, now()), $8)
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
        .fetch_optional(&self.pool)
        .await?
        .flatten();

        Ok(id)
    }

    /// Claim up to `limit` jobs for this worker.
    pub async fn claim(&self, queue: &str, worker_id: Uuid, limit: i32) -> Result<Vec<Job>> {
        let jobs = sqlx::query_as::<_, Job>(
            "SELECT * FROM stampede_claim($1, $2, $3, NULL)",
        )
        .bind(queue)
        .bind(worker_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(jobs)
    }

    /// Mark a job completed.
    pub async fn complete(&self, job: &Job, worker_id: Uuid) -> Result<Transition> {
        let row: Option<(Option<String>, Option<String>, Option<bool>)> = sqlx::query_as(
            "SELECT was, now_state, owned FROM stampede_complete($1, $2, $3)",
        )
        .bind(&job.queue)
        .bind(job.id)
        .bind(worker_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some((was, now, owned)) => Transition { was, now, owned: owned.unwrap_or(false) },
            // Zero rows means the WHERE clause did not match: we no longer own it.
            None => Transition { was: None, now: None, owned: false },
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
        let kind_str = match kind {
            FailureKind::Raised => "raised",
            FailureKind::Timeout => "timeout",
            FailureKind::Interrupted => "interrupted",
            FailureKind::Cancelled => "cancelled",
            FailureKind::Unrecoverable => "unrecoverable",
        };

        let row: Option<(Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT was, now_state FROM stampede_fail($1, $2, $3, $4, $5, $6)",
        )
        .bind(&job.queue)
        .bind(job.id)
        .bind(worker_id)
        .bind(kind_str)
        .bind(error)
        .bind(retry_delay.map(|d| d.as_millis() as i64))
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some((was, now)) => Transition { was, now, owned: true },
            None => Transition { was: None, now: None, owned: false },
        })
    }

    /// Register this process as a worker. The returned id is the heartbeat
    /// identity and what the reaper checks.
    pub async fn register_worker(&self, hostname: &str, pid: i32, queues: &[String]) -> Result<Uuid> {
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

    /// Delete finished jobs older than `older_than`, in bounded batches.
    ///
    /// Returns rows deleted. Callers loop until this returns less than `limit`
    /// — River's shape (plans/13). A single unbounded DELETE on a large table
    /// holds locks and bloats worse than what it reclaims.
    pub async fn prune(&self, older_than: std::time::Duration, limit: i32) -> Result<u64> {
        let n: Option<i32> = sqlx::query_scalar(
            "SELECT stampede_prune(make_interval(secs => $1), $2)",
        )
        .bind(older_than.as_secs_f64())
        .bind(limit)
        .fetch_one(&self.pool)
        .await?;

        Ok(n.unwrap_or(0) as u64)
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

    /// Queue depth by state — the dashboard and MCP `queue_status` tool.
    pub async fn depth(&self, queue: &str) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query_as::<_, (String, i64)>(
            r#"
            SELECT state::text, count(*)
              FROM stampede_jobs
             WHERE queue = $1 AND finished_at IS NULL
             GROUP BY state
             ORDER BY state
            "#,
        )
        .bind(queue)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }
}
