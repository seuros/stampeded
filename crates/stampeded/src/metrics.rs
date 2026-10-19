//! Instrumentation through a [`Recorder`] trait, so no telemetry vendor is a dependency.

use std::time::Duration;

/// Every metric Stampeded emits; the names are a stable API.
pub mod names {
    /// Jobs by state, from the counter table. `{queue, state}`
    pub const QUEUE_DEPTH: &str = "stampeded.queue.depth";
    /// Age of the oldest unclaimed job; tells a stuck queue from a busy one. `{queue}`
    pub const QUEUE_OLDEST_AGE_SECONDS: &str = "stampeded.queue.oldest_age_seconds";

    /// How long a job waited between enqueue and claim. `{queue, task}`
    pub const JOB_QUEUE_TIME_SECONDS: &str = "stampeded.job.queue_time_seconds";
    /// How long a job took to run. `{queue, task}`
    pub const JOB_DURATION_SECONDS: &str = "stampeded.job.duration_seconds";

    pub const JOBS_COMPLETED: &str = "stampeded.jobs.completed";
    /// `{queue, task, failure_kind}`
    pub const JOBS_FAILED: &str = "stampeded.jobs.failed";
    pub const JOBS_RETRIED: &str = "stampeded.jobs.retried";
    pub const JOBS_DISCARDED: &str = "stampeded.jobs.discarded";
    /// Worker died rather than job failed. `{queue, task, failure_kind}`
    pub const JOBS_INTERRUPTED: &str = "stampeded.jobs.interrupted";
    /// A job arrived for a task no worker handles. `{queue, task}`
    pub const JOBS_UNKNOWN_TASK: &str = "stampeded.jobs.unknown_task";

    pub const WORKER_HEARTBEAT_AGE_SECONDS: &str = "stampeded.worker.heartbeat_age_seconds";

    /// Claim latency. `{queue}`
    pub const CLAIM_DURATION_SECONDS: &str = "stampeded.claim.duration_seconds";
    /// `{outcome}`: reaped, expired, pruned.
    pub const MAINTENANCE_JOBS: &str = "stampeded.maintenance.jobs";
    /// Whether the NOTIFY path is working. 0 = polling only.
    pub const WAKEUP_HEALTHY: &str = "stampeded.wakeup.healthy";
    /// Async notification queue fullness, 0.0-1.0.
    pub const NOTIFY_QUEUE_USAGE: &str = "stampeded.notify.queue_usage";
    /// Seconds this process's clock differs from the database's.
    pub const CLOCK_SKEW_SECONDS: &str = "stampeded.clock.skew_seconds";
    pub const WAKEUPS_RECEIVED: &str = "stampeded.wakeups.received";

    /// 0 closed, 1 half-open, 2 open. `{destination}`
    pub const BREAKER_STATE: &str = "stampeded.breaker.state";
    /// How much of its rate a tenant has left. `{queue, tenant}`
    pub const THROTTLE_HEADROOM: &str = "stampeded.throttle.headroom";
}

/// A label attached to a measurement.
pub type Label = (&'static str, String);

/// Where measurements go.
pub trait Recorder: Send + Sync + 'static {
    /// A monotonically increasing count.
    fn count(&self, name: &'static str, value: u64, labels: &[Label]);
    /// A point-in-time value.
    fn gauge(&self, name: &'static str, value: f64, labels: &[Label]);
    /// A duration observation, for histograms and percentiles.
    fn duration(&self, name: &'static str, value: Duration, labels: &[Label]);
}

/// The default: measurements go nowhere.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopRecorder;

impl Recorder for NoopRecorder {
    fn count(&self, _: &'static str, _: u64, _: &[Label]) {}
    fn gauge(&self, _: &'static str, _: f64, _: &[Label]) {}
    fn duration(&self, _: &'static str, _: Duration, _: &[Label]) {}
}

/// A recorder that emits `tracing` events at debug level.
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
