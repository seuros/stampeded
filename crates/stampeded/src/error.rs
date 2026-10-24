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

    /// A schedule that cannot produce a next run; a bad definition, rejected at write.
    #[error("{0}")]
    InvalidSchedule(String),

    /// The task's circuit is open; not the job's fault, so it retries.
    #[error("circuit open: {0}")]
    CircuitOpen(String),

    /// The database holds a stampeded schema this version cannot use or
    /// upgrade.
    #[error("schema: {0}")]
    Schema(String),

    /// The HTTP edge could not listen on its address.
    #[error("could not bind {addr}: {source}")]
    Bind {
        addr: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Whether the job should not be retried.
    ///
    /// `LostOwnership` is not: another worker owns the outcome, so drop it silently.
    pub fn is_unrecoverable(&self) -> bool {
        matches!(self, Self::UnknownTask(_) | Self::Serde(_))
    }
}

/// A handler error that overrides the backoff policy with a specific delay.
///
/// ```ignore
/// Err(Box::new(RetryIn::secs(3600)))   // the server's Retry-After
/// ```
///
/// It picks when the next attempt runs, not whether: `max_attempts` still bounds the job.
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
