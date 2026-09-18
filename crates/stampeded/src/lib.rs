//! Stampeded — a Postgres-backed, polyglot job processor.
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
pub mod lifecycle;
pub mod maintenance;
pub mod metrics;
pub mod queue;
pub mod schedule;
pub mod steps;
pub mod wakeup;
pub mod worker;

/// HTTP edge: the enqueue API, per-job status and SSE, and the dashboard.
///
/// Behind a feature because it pulls rama. A process that only claims and runs
/// jobs -- which is most of them -- has no reason to carry an HTTP stack.
#[cfg(feature = "http")]
pub mod http;

/// MCP control plane.
#[cfg(feature = "mcp")]
pub mod mcp;

pub use cron::{Cron, DstPolicy};
pub use error::{Error, Result, RetryIn};
pub use fairness::{Admission, Fairness, TenantLimit};
pub use job::{FailureKind, Job, JobState, NewJob, Period};
pub use lifecycle::Transitions;
pub use maintenance::{Maintenance, MaintenanceConfig};
pub use metrics::{Metrics, Recorder, TracingRecorder};
pub use queue::{Queue, Transition};
pub use schedule::Schedule;
pub use steps::StepState;
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

// Feature invariant, enforced at COMPILE time.
//
// A runtime `assert!(cfg!(feature = "http"))` would be tautological: it can
// only observe the build it is part of, so it passes trivially whichever way
// that build went. `compile_error!` is the only thing that stops a bad
// combination being produced at all.
//
// UNREACHABLE TODAY, deliberately: `dashboard = ["http"]` in Cargo.toml
// already implies it, so this cannot fire as things stand. It is here for the
// edit that removes that implication -- at which point the dashboard would
// fail deep inside the module with a confusing error about missing rama items,
// instead of here, saying why.
#[cfg(all(feature = "dashboard", not(feature = "http")))]
compile_error!(
    "the `dashboard` feature requires `http`: the dashboard is served by the \
     same rama stack as the enqueue API. Enable `http`, or drop `dashboard`."
);
