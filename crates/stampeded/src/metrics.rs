//! Instrumentation.
//!
//! **A trait, not an `opentelemetry` dependency.** plans/15 takes BullMQ's
//! position: a queue library that hard-depends on one telemetry vendor forces
//! that vendor on every application that uses it, and forces a major version
//! bump every time the vendor breaks its API. The core crate emits through this
//! trait; an OTel adapter is somebody's twenty-line impl, in their crate, on
//! their upgrade schedule.
//!
//! **The names are enumerated and stable.** Also BullMQ's call. A metric name
//! is an API — dashboards, alerts and runbooks are written against it — so they
//! live in one list that can be diffed, rather than being `format!`ed at a
//! hundred call sites where a rename is invisible until an alert stops firing.
//!
//! The set below merges asynq's gauges with Oban's per-job measures, plus three
//! nobody else has: `JOBS_INTERRUPTED` (because nobody else distinguishes a
//! worker dying from a job raising), `BREAKER_STATE`, and `THROTTLE_HEADROOM`.
//!
//! The one that matters most operationally is `QUEUE_OLDEST_AGE_SECONDS`.
//! Depth tells you how much work there is; age tells you whether it is being
//! *done*. A queue with 10,000 jobs and an oldest age of two seconds is
//! healthy; one with 3 jobs and an oldest age of an hour is broken, and depth
//! alone reports the second as quieter than the first.

use std::time::Duration;

/// Every metric Stampeded emits.
///
/// `&'static str` rather than an enum with a `name()`: these are used as map
/// keys and label values by recorders, and the string IS the identity.
pub mod names {
    /// Jobs by state. `{queue, state}` — from the counter table, never an
    /// aggregate over the job table.
    pub const QUEUE_DEPTH: &str = "stampeded.queue.depth";
    /// Age of the oldest unclaimed job. `{queue}`
    ///
    /// asynq's queue_latency, and the single most useful number here: it is
    /// the only one that distinguishes "lots of work, moving fine" from "a
    /// little work, stuck".
    pub const QUEUE_OLDEST_AGE_SECONDS: &str = "stampeded.queue.oldest_age_seconds";

    /// How long a job waited between enqueue and claim. `{queue, task}`
    pub const JOB_QUEUE_TIME_SECONDS: &str = "stampeded.job.queue_time_seconds";
    /// How long a job took to run. `{queue, task}`
    pub const JOB_DURATION_SECONDS: &str = "stampeded.job.duration_seconds";

    pub const JOBS_COMPLETED: &str = "stampeded.jobs.completed";
    /// `{queue, task, failure_kind}` — the label that makes "is this an outage
    /// or a bad deploy" answerable from a dashboard instead of from SQL.
    pub const JOBS_FAILED: &str = "stampeded.jobs.failed";
    pub const JOBS_RETRIED: &str = "stampeded.jobs.retried";
    pub const JOBS_DISCARDED: &str = "stampeded.jobs.discarded";
    /// `{queue, task, failure_kind}` — worker died rather than job failed.
    /// Nobody else separates these, so nobody else can chart them.
    pub const JOBS_INTERRUPTED: &str = "stampeded.jobs.interrupted";
    /// A job arrived for a task no worker handles. `{queue, task}`
    ///
    /// celery's task_unknown. A real production failure — a rename deployed to
    /// the enqueuer but not the worker — and invisible almost everywhere else:
    /// River discards such jobs with only a log line.
    pub const JOBS_UNKNOWN_TASK: &str = "stampeded.jobs.unknown_task";

    pub const WORKER_HEARTBEAT_AGE_SECONDS: &str = "stampeded.worker.heartbeat_age_seconds";

    /// Control plane, at job fidelity (Oban's engine events). `{queue}`
    pub const CLAIM_DURATION_SECONDS: &str = "stampeded.claim.duration_seconds";
    /// `{outcome}` — reaped, expired, pruned.
    pub const MAINTENANCE_JOBS: &str = "stampeded.maintenance.jobs";
    /// Whether the NOTIFY path is working. 0 = polling only.
    pub const WAKEUP_HEALTHY: &str = "stampeded.wakeup.healthy";
    /// Async notification queue fullness, 0.0-1.0. Nobody else exports this.
    pub const NOTIFY_QUEUE_USAGE: &str = "stampeded.notify.queue_usage";
    /// Seconds this process's clock differs from the database's.
    pub const CLOCK_SKEW_SECONDS: &str = "stampeded.clock.skew_seconds";
    pub const WAKEUPS_RECEIVED: &str = "stampeded.wakeups.received";

    /// `{destination}` — 0 closed, 1 half-open, 2 open. Ours alone.
    pub const BREAKER_STATE: &str = "stampeded.breaker.state";
    /// `{queue, tenant}` — how much of its rate a tenant has left. Ours alone.
    pub const THROTTLE_HEADROOM: &str = "stampeded.throttle.headroom";
}

/// A label attached to a measurement.
pub type Label = (&'static str, String);

/// Where measurements go.
///
/// Deliberately three methods. A recorder that has to implement a dozen kinds
/// of instrument is a recorder nobody writes, and every backend worth targeting
/// can express counters, gauges and durations.
pub trait Recorder: Send + Sync + 'static {
    /// A monotonically increasing count.
    fn count(&self, name: &'static str, value: u64, labels: &[Label]);
    /// A point-in-time value.
    fn gauge(&self, name: &'static str, value: f64, labels: &[Label]);
    /// A duration observation, for histograms and percentiles.
    fn duration(&self, name: &'static str, value: Duration, labels: &[Label]);
}

/// The default: measurements go nowhere.
///
/// Not an `Option<Recorder>` at every call site. A no-op impl means the
/// instrumentation reads identically whether or not anyone is collecting, and
/// the optimiser removes it.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopRecorder;

impl Recorder for NoopRecorder {
    fn count(&self, _: &'static str, _: u64, _: &[Label]) {}
    fn gauge(&self, _: &'static str, _: f64, _: &[Label]) {}
    fn duration(&self, _: &'static str, _: Duration, _: &[Label]) {}
}

/// A recorder that emits `tracing` events.
///
/// Useful on its own — `RUST_LOG=stampeded_core::metrics=debug` gives a working
/// metrics feed with no backend at all — and useful as the reference impl for
/// anyone writing a real one.
#[derive(Debug, Clone, Copy, Default)]
pub struct TracingRecorder;

impl Recorder for TracingRecorder {
    fn count(&self, name: &'static str, value: u64, labels: &[Label]) {
        tracing::debug!(metric = name, value, ?labels, "count");
    }
    fn gauge(&self, name: &'static str, value: f64, labels: &[Label]) {
        tracing::debug!(metric = name, value, ?labels, "gauge");
    }
    fn duration(&self, name: &'static str, value: Duration, labels: &[Label]) {
        tracing::debug!(metric = name, ?value, ?labels, "duration");
    }
}

/// Shared handle, cheap to clone.
#[derive(Clone)]
pub struct Metrics(std::sync::Arc<dyn Recorder>);

impl Metrics {
    pub fn new(recorder: impl Recorder) -> Self {
        Self(std::sync::Arc::new(recorder))
    }

    pub fn noop() -> Self {
        Self::new(NoopRecorder)
    }

    pub fn count(&self, name: &'static str, value: u64, labels: &[Label]) {
        self.0.count(name, value, labels);
    }

    pub fn gauge(&self, name: &'static str, value: f64, labels: &[Label]) {
        self.0.gauge(name, value, labels);
    }

    pub fn duration(&self, name: &'static str, value: Duration, labels: &[Label]) {
        self.0.duration(name, value, labels);
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::noop()
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Metrics(..)")
    }
}

/// Label helper, so call sites stay one line.
pub fn label(key: &'static str, value: impl Into<String>) -> Label {
    (key, value.into())
}

#[cfg(test)]
mod tests;
