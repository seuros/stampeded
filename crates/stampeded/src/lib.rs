//! Stampeded: a Postgres-backed, polyglot job processor.
//!
//! The rules live in SQL (`schema/*.sql`); this crate is a typed wrapper over them.

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
#[cfg(test)]
mod test_support;
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

pub mod schema;

// Unreachable while `dashboard = ["http"]`; guards against dropping that implication.
#[cfg(all(feature = "dashboard", not(feature = "http")))]
compile_error!(
    "the `dashboard` feature requires `http`: the dashboard is served by the \
     same rama stack as the enqueue API. Enable `http`, or drop `dashboard`."
);

// rustls panics if two crypto-provider backends link into the same binary (no
// single process-default CryptoProvider). Neither enabled is fine: the app
// embedding this crate may supply its own Postgres TLS backend.
#[cfg(all(feature = "tls-aws-lc", feature = "tls-ring"))]
compile_error!(
    "`tls-aws-lc` and `tls-ring` cannot both be enabled: rustls panics at the first TLS \
     connection when two crypto-provider backends link into one binary. Pick one."
);
