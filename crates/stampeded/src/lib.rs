//! Stampeded: a Postgres-backed, polyglot job processor.
//!
//! The rules live in SQL (`schema/schema.sql`); this crate is a typed wrapper over them.

mod claim;
pub mod cron;
pub mod error;
pub mod fairness;
pub mod job;
pub mod lifecycle;
pub mod maintenance;
pub mod metrics;
pub mod planner;
mod pool;
pub mod pull;
pub mod queue;
pub mod queues;
pub mod schedule;
pub mod steps;
mod timestamp;
pub mod wakeup;
pub mod worker;

/// HTTP edge: the enqueue API, per-job status and SSE, and the dashboard.
#[cfg(feature = "http")]
pub mod http;

/// MCP control plane.
#[cfg(feature = "mcp")]
pub mod mcp;

pub use claim::VERDICT_TTL;
pub use cron::{Cron, DstPolicy};
pub use error::{Error, Result, RetryIn};
pub use fairness::{Admission, Fairness, TenantLimit};
pub use job::{FailureKind, Job, JobState, NewJob, Period};
pub use lifecycle::Transitions;
pub use maintenance::{Maintenance, MaintenanceConfig};
pub use metrics::{Metrics, Recorder, TracingRecorder};
pub use planner::{Decision, Planner, QueueSnapshot, Verdict};
pub use pool::connect;
pub use pull::{Lease, PullConfig, Puller};
pub use queue::{Queue, Transition};
pub use queues::Queues;
pub use schedule::Schedule;
pub use steps::StepState;
pub use wakeup::{NextPoll, Wakeup};
pub use worker::{Worker, WorkerConfig};

/// The embedded schema, so a host can migrate without shipping .sql files.
pub mod schema {
    /// Applied whole to a fresh database in one transaction, never incrementally.
    pub const SCHEMA: &str = include_str!("../schema/schema.sql");

    pub async fn migrate(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
        sqlx::raw_sql(SCHEMA).execute(pool).await?;
        Ok(())
    }
}

// Unreachable while `dashboard = ["http"]`; guards against dropping that implication.
#[cfg(all(feature = "dashboard", not(feature = "http")))]
compile_error!(
    "the `dashboard` feature requires `http`: the dashboard is served by the \
     same rama stack as the enqueue API. Enable `http`, or drop `dashboard`."
);
