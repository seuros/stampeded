//! Error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("database: {0}")]
    Database(#[from] sqlx::Error),

    #[error("job {id} is no longer owned by this worker (state is now {state})")]
    LostOwnership { id: uuid::Uuid, state: String },

    #[error("no handler registered for task {0:?}")]
    UnknownTask(String),

    #[error("job execution timed out after {0:?}")]
    Timeout(std::time::Duration),

    #[error("handler failed: {0}")]
    Handler(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),

    /// A schedule cannot produce a next run: a malformed cron expression, an
    /// unknown timezone, or one whose occurrences fall outside the search
    /// horizon. Distinct from `Handler` because it is a bad *definition*, not a
    /// failed execution — and it surfaces to users as a rejected write.
    #[error("{0}")]
    InvalidSchedule(String),

    /// The task's circuit is open — the downstream is failing and we are not
    /// adding to it. Not the job's fault, so it retries rather than discarding.
    #[error("circuit open: {0}")]
    CircuitOpen(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Whether this error means the job should not be retried.
    ///
    /// Losing ownership is deliberately NOT fatal to the job — it means the
    /// reaper already reclaimed it and someone else owns the outcome. The
    /// worker should drop it silently rather than write a failure over another
    /// worker's result.
    pub fn is_unrecoverable(&self) -> bool {
        matches!(self, Self::UnknownTask(_) | Self::Serde(_))
    }
}

/// A handler asking for a specific retry delay.
///
/// The Rust counterpart of Ruby's `Stampeded::RetryIn`, and the answer to
/// ActiveJob's `retry_on(wait:)` on both sides. Return it from a handler
/// (boxed, like any handler error) and the worker uses this delay instead of
/// the backoff policy:
///
/// ```ignore
/// Err(Box::new(RetryIn::secs(3600)))   // the server's Retry-After
/// ```
///
/// The row already carries `backoff_base_ms`/`backoff_max_ms`, which express
/// exponential-with-cap and nothing else. This covers the rest, and covers it
/// better than a schedule column could, because the delay is chosen at the
/// moment of failure and can use what the failure itself said.
///
/// It picks WHEN the next attempt happens, not whether: the attempt is already
/// charged at claim and `max_attempts` still bounds the job.
#[derive(Debug, Clone)]
pub struct RetryIn(pub std::time::Duration);

impl RetryIn {
    pub fn secs(s: u64) -> Self {
        Self(std::time::Duration::from_secs(s))
    }

    pub fn millis(ms: u64) -> Self {
        Self(std::time::Duration::from_millis(ms))
    }
}

impl std::fmt::Display for RetryIn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "retry in {:?}", self.0)
    }
}

impl std::error::Error for RetryIn {}
