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
