//! Stampede — a Postgres-backed, polyglot job processor.
//!
//! The database is the authority. Claim semantics, attempt accounting and
//! transition rules live in SQL (`schema/002_operations.sql`) so that a worker
//! in any language gets identical behaviour; this crate is the Rust worker and
//! a thin typed wrapper, not a second implementation.
//!
//! See `plans/` for why each decision was made, and what was measured.

pub mod cron;
pub mod error;
pub mod fairness;
pub mod job;
pub mod maintenance;
pub mod metrics;
pub mod queue;
pub mod schedule;
pub mod wakeup;
pub mod worker;

pub use cron::{Cron, DstPolicy};
pub use error::{Error, Result};
pub use schedule::Schedule;
pub use fairness::{Admission, Fairness, TenantLimit};
pub use maintenance::{Maintenance, MaintenanceConfig};
pub use metrics::{Metrics, Recorder, TracingRecorder};
pub use job::{FailureKind, Job, JobState, NewJob};
pub use queue::{Queue, Transition};
pub use wakeup::{NextPoll, Wakeup};
pub use worker::{Worker, WorkerConfig};

/// The embedded schema, so a host application can migrate without shipping
/// .sql files alongside its binary.
pub mod schema {
    pub const INITIAL: &str = include_str!("../../../schema/001_initial.sql");
    pub const OPERATIONS: &str = include_str!("../../../schema/002_operations.sql");
    pub const SCHEDULES: &str = include_str!("../../../schema/003_schedules.sql");

    /// Apply both files in order.
    pub async fn migrate(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
        sqlx::raw_sql(INITIAL).execute(pool).await?;
        sqlx::raw_sql(OPERATIONS).execute(pool).await?;
        sqlx::raw_sql(SCHEDULES).execute(pool).await?;
        Ok(())
    }
}
