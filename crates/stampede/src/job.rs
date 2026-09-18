//! Job types and lifecycle states.
//!
//! The state set mirrors the CHECK constraint in `schema/001_initial.sql`. If
//! you add a variant here, add it there — the database is the authority, and a
//! state it rejects is a runtime error rather than a compile error.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Job lifecycle.
///
/// `Retryable` is claimable once its backoff has elapsed — see the claim
/// predicate in `schema/002_operations.sql`. Treating it as terminal is the
/// poison-job bug described there: a crashed job parks in `Retryable`, never
/// gets re-claimed, never spends its attempt budget, and never reaches
/// `max_attempts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Available,
    Claimed,
    Executing,
    Retryable,
    Completed,
    Discarded,
    Cancelled,
}

impl JobState {
    /// Terminal states never transition again and are eligible for pruning.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Discarded | Self::Cancelled)
    }

    /// States a worker may claim from.
    pub fn is_claimable(self) -> bool {
        matches!(self, Self::Available | Self::Retryable)
    }

    /// The name used in SQL, JSON and on the wire. One spelling everywhere.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Claimed => "claimed",
            Self::Executing => "executing",
            Self::Retryable => "retryable",
            Self::Completed => "completed",
            Self::Discarded => "discarded",
            Self::Cancelled => "cancelled",
        }
    }
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a job failed.
///
/// Kept separate from the attempt count on purpose. Of thirteen implementations
/// surveyed (plans/15) only BullMQ and solid_queue distinguish "the worker died"
/// from "the job raised", and that distinction is what makes a poison job
/// diagnosable instead of merely mysterious.
// snake_case, NOT lowercase: `TimedOut` must serialise as `timed_out` to match
// the CHECK constraint. `rename_all = "lowercase"` yields `timedout`, which the
// database rejects at write time rather than at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    /// The job function returned an error.
    Raised,
    /// Exceeded the in-process timeout or the database-enforced
    /// `expire_seconds`. Both land here: from the job's point of view they are
    /// the same event, and only the layer that noticed differs.
    TimedOut,
    /// A long-running job stopped calling `touch` while its worker stayed up.
    HeartbeatLost,
    /// The worker process died with a status we captured. 137 is OOM.
    WorkerExited,
    /// The worker vanished without exiting cleanly; found by the reaper.
    WorkerMissing,
    /// The circuit for this job's destination was open, so we declined to run
    /// it. OUR backpressure, not the job's failure — see `refunds_attempt`.
    BreakerOpen,
    /// The tenant was over its rate limit, so we declined to run it. Also ours,
    /// also refunded — kept distinct from `BreakerOpen` because "the API is
    /// down" and "this customer is flooding us" want different responses.
    Throttled,
    /// Explicitly cancelled by an operator.
    Cancelled,
    /// Will never succeed; skip remaining attempts.
    Unrecoverable,
}

impl FailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Raised => "raised",
            Self::TimedOut => "timed_out",
            Self::HeartbeatLost => "heartbeat_lost",
            Self::WorkerExited => "worker_exited",
            Self::WorkerMissing => "worker_missing",
            Self::BreakerOpen => "breaker_open",
            Self::Throttled => "throttled",
            Self::Cancelled => "cancelled",
            Self::Unrecoverable => "unrecoverable",
        }
    }

    /// Whether this failure should consume the remaining attempt budget
    /// immediately rather than retrying.
    pub fn is_fatal(self) -> bool {
        matches!(self, Self::Unrecoverable | Self::Cancelled)
    }

    /// Whether this means the WORKER died rather than the job failing.
    ///
    /// These charge `interruptions`, a much tighter budget than `attempt` —
    /// one free by default. A job that kills its worker must not be allowed to
    /// walk the whole fleet one process at a time.
    pub fn is_interruption(self) -> bool {
        matches!(
            self,
            Self::HeartbeatLost | Self::WorkerExited | Self::WorkerMissing
        )
    }

    /// Whether the attempt spent at claim is given back.
    ///
    /// True only for `BreakerOpen`, and it is a correctness rule rather than a
    /// nicety (plans/15): a failure caused by our own backpressure must not
    /// count against the job's retry budget. Without the refund, a downstream
    /// outage silently burns every queued job's attempts and a fleet-wide
    /// breaker trip discards the entire backlog — turning one dependency's bad
    /// afternoon into permanent data loss.
    pub fn refunds_attempt(self) -> bool {
        matches!(self, Self::BreakerOpen | Self::Throttled)
    }
}

impl std::fmt::Display for FailureKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A job row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Job {
    pub id: Uuid,
    pub queue: String,
    pub tenant_id: Option<String>,
    pub task: String,
    pub args: serde_json::Value,
    pub state: JobState,
    pub priority: i16,
    pub scheduled_at: DateTime<Utc>,
    pub attempt: i16,
    pub max_attempts: i16,
    pub attempted_by: Vec<String>,
    pub claimed_by: Option<Uuid>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub dedup_scope: Option<String>,
    pub dedup_period: Option<String>,
    pub destination: Option<String>,
    pub interruptions: i16,
    pub max_interruptions: i16,
    pub deferrals: i16,
    pub max_deferrals: i16,
    pub expire_seconds: i32,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub backoff_base_ms: i16,
    pub backoff_max_ms: i32,
    /// Resume state from a previous attempt. `None` on the first.
    pub checkpoint: Option<serde_json::Value>,
    pub checkpoint_at: Option<DateTime<Utc>>,
    pub checkpoint_seq: i32,
    pub failure_kind: Option<FailureKind>,
    pub last_error: Option<String>,
    /// Structured history, newest last, capped at 10 by the fail path.
    pub errors: serde_json::Value,
    pub error_fingerprint: Option<Vec<u8>>,
    pub dead_letter_queue: Option<String>,
    pub source_id: Option<Uuid>,
    pub source_queue: Option<String>,
    pub source_attempts: Option<i16>,
    pub inserted_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl Job {
    /// Attempts remaining before the job is discarded.
    ///
    /// Note `attempt` was already incremented at claim time, so a job currently
    /// executing has already spent the attempt it is using.
    pub fn attempts_remaining(&self) -> i16 {
        (self.max_attempts - self.attempt).max(0)
    }

    /// Resume state saved by a previous attempt, if any.
    ///
    /// `None` means this is the first run — or that the job has never
    /// checkpointed, which for most jobs is the same thing.
    pub fn resume_from<T: serde::de::DeserializeOwned>(&self) -> Option<T> {
        serde_json::from_value(self.checkpoint.clone()?).ok()
    }

    pub fn is_last_attempt(&self) -> bool {
        self.attempts_remaining() == 0
    }
}

/// A deduplication window.
///
/// Fixed buckets, not sliding windows: a sliding window needs
/// `ts >= now() - period`, which is not immutable and so cannot live in an
/// index or a generated column. The honest consequence is a boundary hole —
/// two jobs a minute apart can land either side of an hour boundary and both
/// run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Period {
    Minute,
    Hour,
    Day,
    Week,
    Month,
}

impl Period {
    /// Must match the CHECK constraint on `dedup_period`, and is passed
    /// straight to `date_trunc`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
        }
    }
}

/// A job to enqueue.
///
/// Deliberately constructible without a client library — this mirrors the
/// columns a plain `INSERT` would set from any language (plans/05).
#[derive(Debug, Clone)]
pub struct NewJob {
    pub queue: String,
    pub task: String,
    pub args: serde_json::Value,
    pub tenant_id: Option<String>,
    pub priority: i16,
    pub max_attempts: i16,
    pub scheduled_at: Option<DateTime<Utc>>,
    /// `None` means "this job is not deduplicated". When set, uniqueness is
    /// enforced per-queue by a partial unique index — see plans/10.
    pub dedup_scope: Option<String>,
    /// Optional bucket: `Some("hour")` means "one per hour", not "one ever".
    pub dedup_period: Option<String>,
    /// What this job talks to. Keys the circuit breaker, so it is a column
    /// rather than a tag inside `args`.
    pub destination: Option<String>,
    /// Where a terminal failure goes. The DLQ is a queue, so re-drive is just
    /// working it.
    pub dead_letter_queue: Option<String>,
    /// Database-enforced wall-clock budget. Fires even when the worker is dead,
    /// which is the case the in-process timeout cannot cover.
    pub expire_seconds: Option<i32>,
}

impl NewJob {
    pub fn new(queue: impl Into<String>, task: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            task: task.into(),
            args: serde_json::Value::Object(Default::default()),
            tenant_id: None,
            priority: 0,
            max_attempts: 25,
            scheduled_at: None,
            dedup_scope: None,
            dedup_period: None,
            destination: None,
            dead_letter_queue: None,
            expire_seconds: None,
        }
    }

    /// What this job talks to, for circuit breaking and per-destination health.
    pub fn destination(mut self, d: impl Into<String>) -> Self {
        self.destination = Some(d.into());
        self
    }

    /// Send terminal failures to this queue, with provenance.
    pub fn dead_letter(mut self, queue: impl Into<String>) -> Self {
        self.dead_letter_queue = Some(queue.into());
        self
    }

    /// Wall-clock budget enforced by the database.
    pub fn expires_in(mut self, d: std::time::Duration) -> Self {
        self.expire_seconds = Some(d.as_secs().max(1) as i32);
        self
    }

    pub fn args(mut self, args: serde_json::Value) -> Self {
        self.args = args;
        self
    }

    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }

    pub fn priority(mut self, priority: i16) -> Self {
        self.priority = priority;
        self
    }

    pub fn max_attempts(mut self, n: i16) -> Self {
        self.max_attempts = n;
        self
    }

    pub fn scheduled_at(mut self, at: DateTime<Utc>) -> Self {
        self.scheduled_at = Some(at);
        self
    }

    /// Enable per-queue deduplication on this scope string.
    /// One job per `scope` per bucket, e.g. `("report:42", Period::Hour)`.
    ///
    /// The bucket is computed by the database from `inserted_at`, so enqueuers
    /// with skewed clocks still agree on which window they are in.
    pub fn unique_per(mut self, scope: impl Into<String>, period: Period) -> Self {
        self.dedup_scope = Some(scope.into());
        self.dedup_period = Some(period.as_str().to_string());
        self
    }

    pub fn unique(mut self, scope: impl Into<String>) -> Self {
        self.dedup_scope = Some(scope.into());
        self
    }
}

#[cfg(test)]
mod tests;
