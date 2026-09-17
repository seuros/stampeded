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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum FailureKind {
    /// The job function returned an error.
    Raised,
    /// The job exceeded its execution timeout.
    Timeout,
    /// The worker died mid-job; recovered by the reaper. Not the job's fault.
    Interrupted,
    /// Explicitly cancelled by an operator.
    Cancelled,
    /// Will never succeed; skip remaining attempts.
    Unrecoverable,
}

impl FailureKind {
    /// Whether this failure should consume the remaining attempt budget
    /// immediately rather than retrying.
    pub fn is_fatal(self) -> bool {
        matches!(self, Self::Unrecoverable | Self::Cancelled)
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
    pub failure_kind: Option<FailureKind>,
    pub last_error: Option<String>,
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

    pub fn is_last_attempt(&self) -> bool {
        self.attempts_remaining() == 0
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
        }
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
    pub fn unique(mut self, scope: impl Into<String>) -> Self {
        self.dedup_scope = Some(scope.into());
        self
    }
}

#[cfg(test)]
mod tests;
