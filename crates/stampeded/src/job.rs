//! Job types and lifecycle states.
//!
//! The state set mirrors the CHECK constraint in the schema; change both together.

use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Job lifecycle. `Retryable` is claimable once its backoff has elapsed, not terminal.
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
    pub const ALL: [Self; 7] = [
        Self::Available,
        Self::Claimed,
        Self::Executing,
        Self::Retryable,
        Self::Completed,
        Self::Discarded,
        Self::Cancelled,
    ];

    /// The state named as in SQL.
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.as_str() == name)
    }

    /// Terminal states: no worker moves them again, only an operator's `resurrect`;
    /// eligible for pruning.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Discarded | Self::Cancelled)
    }

    /// States a worker may claim from.
    pub fn is_claimable(self) -> bool {
        matches!(self, Self::Available | Self::Retryable)
    }

    /// The name used in SQL, JSON and on the wire.
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
// snake_case, not lowercase: the CHECK constraint wants `timed_out`, not `timedout`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    /// The job function returned an error.
    Raised,
    /// Exceeded the in-process timeout or the database-enforced `expire_seconds`.
    TimedOut,
    /// A long-running job stopped calling `touch` while its worker stayed up.
    HeartbeatLost,
    /// The worker process died with a status we captured. 137 is OOM.
    WorkerExited,
    /// The worker vanished without exiting cleanly; found by the reaper.
    WorkerMissing,
    /// The destination's circuit was open, so we declined to run it.
    BreakerOpen,
    /// The tenant was over its rate limit, so we declined to run it.
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

    /// Whether this failure spends the remaining attempt budget at once.
    pub fn is_fatal(self) -> bool {
        matches!(self, Self::Unrecoverable | Self::Cancelled)
    }

    /// Whether the worker died rather than the job failing; charged to `interruptions`.
    pub fn is_interruption(self) -> bool {
        matches!(
            self,
            Self::HeartbeatLost | Self::WorkerExited | Self::WorkerMissing
        )
    }

    /// Whether we declined to run the job (our backpressure); charged to `deferrals`.
    pub fn is_deferral(self) -> bool {
        matches!(self, Self::BreakerOpen | Self::Throttled)
    }

    /// Whether the attempt spent at claim is given back.
    ///
    /// Our own backpressure must not burn the job's retry budget.
    pub fn refunds_attempt(self) -> bool {
        self.is_deferral()
    }
}

impl std::fmt::Display for FailureKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A job row.
#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub queue: String,
    pub tenant_id: Option<String>,
    pub task: String,
    /// The job's named arguments, one JSON object.
    pub args: serde_json::Value,
    /// Runtime context, namespaced by runtime (`"activejob"`, ...).
    pub meta: serde_json::Value,
    pub state: JobState,
    pub priority: i16,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub scheduled_at: Timestamp,
    pub attempt: i16,
    pub max_attempts: i16,
    pub attempted_by: Vec<String>,
    pub claimed_by: Option<Uuid>,
    #[sqlx(try_from = "crate::timestamp::NullableTimestamp")]
    pub claimed_at: Option<Timestamp>,
    pub dedup_scope: Option<String>,
    pub dedup_period: Option<String>,
    pub destination: Option<String>,
    pub interruptions: i16,
    pub max_interruptions: i16,
    pub deferrals: i16,
    pub max_deferrals: i16,
    pub expire_seconds: i32,
    #[sqlx(try_from = "crate::timestamp::NullableTimestamp")]
    pub heartbeat_at: Option<Timestamp>,
    pub backoff_base_ms: i16,
    pub backoff_max_ms: i32,
    /// Resume state from a previous attempt. `None` on the first.
    pub checkpoint: Option<serde_json::Value>,
    #[sqlx(try_from = "crate::timestamp::NullableTimestamp")]
    pub checkpoint_at: Option<Timestamp>,
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
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub inserted_at: Timestamp,
    #[sqlx(try_from = "crate::timestamp::NullableTimestamp")]
    pub finished_at: Option<Timestamp>,
}

impl Job {
    /// Attempts remaining before discard; `attempt` was already incremented at claim.
    pub fn attempts_remaining(&self) -> i16 {
        (self.max_attempts - self.attempt).max(0)
    }

    /// Resume state saved by a previous attempt, if any.
    pub fn resume_from<T: serde::de::DeserializeOwned>(&self) -> Option<T> {
        serde_json::from_value(self.checkpoint.clone()?).ok()
    }

    pub fn is_last_attempt(&self) -> bool {
        self.attempts_remaining() == 0
    }
}

/// A fixed deduplication bucket; jobs either side of a boundary both run.
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
    /// Must match the CHECK constraint on `dedup_period`; passed to `date_trunc`.
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

/// A job to enqueue; mirrors the columns a plain `INSERT` would set.
#[derive(Debug, Clone)]
pub struct NewJob {
    pub queue: String,
    pub task: String,
    pub args: serde_json::Value,
    pub meta: serde_json::Value,
    pub tenant_id: Option<String>,
    pub priority: i16,
    pub max_attempts: i16,
    pub scheduled_at: Option<Timestamp>,
    /// `None` means not deduplicated; otherwise unique per queue.
    pub dedup_scope: Option<String>,
    /// Optional bucket: `Some("hour")` means "one per hour", not "one ever".
    pub dedup_period: Option<String>,
    /// What this job talks to; keys the circuit breaker.
    pub destination: Option<String>,
    /// The queue a terminal failure goes to.
    pub dead_letter_queue: Option<String>,
    /// Database-enforced wall-clock budget; fires even when the worker is dead.
    pub expire_seconds: Option<i32>,
    /// First retry delay in ms, doubling from there; at most 32767 (`smallint`).
    pub backoff_base_ms: Option<i16>,
    /// Ceiling for the doubling.
    pub backoff_max_ms: Option<i32>,
}

impl NewJob {
    pub fn new(queue: impl Into<String>, task: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            task: task.into(),
            args: serde_json::Value::Object(serde_json::Map::new()),
            meta: serde_json::Value::Object(serde_json::Map::new()),
            tenant_id: None,
            priority: 0,
            max_attempts: 25,
            scheduled_at: None,
            dedup_scope: None,
            dedup_period: None,
            destination: None,
            dead_letter_queue: None,
            expire_seconds: None,
            backoff_base_ms: None,
            backoff_max_ms: None,
        }
    }

    /// Per-job retry backoff in ms; `base_ms` is clamped to 32767 (`smallint`).
    #[must_use]
    pub fn backoff(mut self, base_ms: i32, max_ms: i32) -> Self {
        self.backoff_base_ms = Some(i16::try_from(base_ms.max(1)).unwrap_or(i16::MAX));
        self.backoff_max_ms = Some(max_ms.max(1));
        self
    }

    /// What this job talks to, for circuit breaking and per-destination health.
    #[must_use]
    pub fn destination(mut self, d: impl Into<String>) -> Self {
        self.destination = Some(d.into());
        self
    }

    /// Send terminal failures to this queue, with provenance.
    #[must_use]
    pub fn dead_letter(mut self, queue: impl Into<String>) -> Self {
        self.dead_letter_queue = Some(queue.into());
        self
    }

    /// Wall-clock budget enforced by the database; saturates at `i32::MAX` seconds.
    #[must_use]
    pub fn expires_in(mut self, d: std::time::Duration) -> Self {
        self.expire_seconds = Some(i32::try_from(d.as_secs().max(1)).unwrap_or(i32::MAX));
        self
    }

    /// The job's named arguments; must be a JSON object. `null` is stored as `{}`.
    #[must_use]
    pub fn args(mut self, args: serde_json::Value) -> Self {
        self.args = if args.is_null() {
            serde_json::Value::Object(serde_json::Map::new())
        } else {
            args
        };
        self
    }

    /// Runtime context, namespaced by runtime. `null` is stored as `{}`.
    #[must_use]
    pub fn meta(mut self, meta: serde_json::Value) -> Self {
        self.meta = if meta.is_null() {
            serde_json::Value::Object(serde_json::Map::new())
        } else {
            meta
        };
        self
    }

    #[must_use]
    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }

    #[must_use]
    pub fn priority(mut self, priority: i16) -> Self {
        self.priority = priority;
        self
    }

    #[must_use]
    pub fn max_attempts(mut self, n: i16) -> Self {
        self.max_attempts = n;
        self
    }

    #[must_use]
    pub fn scheduled_at(mut self, at: Timestamp) -> Self {
        self.scheduled_at = Some(at);
        self
    }

    /// One job per `scope` per bucket, e.g. `("report:42", Period::Hour)`.
    ///
    /// The bucket comes from the database's `inserted_at`, so client clock skew is moot.
    #[must_use]
    pub fn unique_per(mut self, scope: impl Into<String>, period: Period) -> Self {
        self.dedup_scope = Some(scope.into());
        self.dedup_period = Some(period.as_str().to_string());
        self
    }

    #[must_use]
    pub fn unique(mut self, scope: impl Into<String>) -> Self {
        self.dedup_scope = Some(scope.into());
        self
    }
}

#[cfg(test)]
mod tests;
