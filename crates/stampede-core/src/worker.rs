//! The worker: claim, execute, report, heartbeat.
//!
//! Three properties in here are load-bearing and easy to break by tidying:
//!
//! 1. **The connection is not held for the job's duration.** underway pins a
//!    pooled connection and an open transaction for the entire execution
//!    (`worker.rs:810-886`), which puts ~20 connections at rest on a 16-core box
//!    and exhausts a stock `max_connections=100` with five processes
//!    (plans/14 §5). We claim, release, execute, then re-acquire to report.
//!
//! 2. **Losing ownership is not a failure.** If the reaper reclaimed the job
//!    while we were running it, another worker owns the outcome. Writing our
//!    result would clobber theirs. We log and drop.
//!
//! 3. **Backoff comes from `chrono-machines`, not from SQL.** Full jitter by
//!    default (`jitter_factor: 1.0`) — of thirteen implementations surveyed
//!    (plans/15) not one defaults to full jitter. Putting the formula in SQL
//!    would fork it.

use crate::error::{Error, Result};
use crate::job::{FailureKind, Job};
use crate::queue::Queue;
use chrono_machines::{BackoffPolicy, BackoffStrategy, ExponentialBackoff};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// What a task handler returns.
pub type HandlerFuture =
    Pin<Box<dyn Future<Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>>;

/// A task handler: receives the job, does the work.
pub type Handler = Arc<dyn Fn(Job) -> HandlerFuture + Send + Sync>;

/// Worker configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub queue: String,
    /// How many jobs to claim per round trip.
    pub batch_size: i32,
    /// Poll floor. Never disabled, even with NOTIFY wired up: a redundant
    /// wakeup costs one empty query (~0.02 ms measured, plans/12); a missed one
    /// costs a stuck job. Nine of nine notifying implementations surveyed still
    /// poll unconditionally.
    pub poll_interval: Duration,
    /// Per-job execution timeout. underway is the only surveyed Postgres queue
    /// that enforces one at all (plans/15).
    pub job_timeout: Duration,
    /// Heartbeat cadence. Must be comfortably less than the reaper's
    /// `dead_after` or healthy workers get reaped mid-job.
    pub heartbeat_interval: Duration,
    pub max_concurrency: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            queue: "default".into(),
            batch_size: 5,
            poll_interval: Duration::from_secs(1),
            job_timeout: Duration::from_secs(15 * 60),
            heartbeat_interval: Duration::from_secs(30),
            max_concurrency: 10,
        }
    }
}

/// Used when a backoff policy declines to produce a delay. See `default_backoff`.
const FALLBACK_RETRY_DELAY_MS: u64 = 60 * 1_000;

/// The default retry policy: exponential, capped at one hour, **full jitter**.
///
/// Free function rather than inline so it is testable without a database pool,
/// and so the one value the whole field gets wrong is visible in one place.
/// Of thirteen implementations surveyed (plans/15) not one defaults to full
/// jitter; `jitter_factor(1.0)` is what stops a batch of jobs that failed
/// together from retrying together.
pub fn default_backoff() -> BackoffPolicy {
    BackoffPolicy::Exponential(
        ExponentialBackoff::new()
            .base_delay_ms(1_000)
            .max_delay_ms(60 * 60 * 1_000)
            .jitter_factor(1.0)
            // NOT the crate default of 3. The DATABASE owns the attempt limit
            // (`max_attempts` per job, default 25) — the policy's only job is to
            // say how long to wait. Leaving this at 3 is a live bug: delay()
            // returns None from attempt 3 on, report_failure turns that into
            // `retry_at: None`, `scheduled_at` is left unchanged, and the job
            // becomes immediately claimable again. A hot retry loop, produced by
            // a default that looks harmless. Caught by test, not by reading.
            .max_attempts(u8::MAX),
    )
}

pub struct Worker {
    queue: Queue,
    config: WorkerConfig,
    handlers: HashMap<String, Handler>,
    backoff: BackoffPolicy,
}

impl Worker {
    pub fn new(queue: Queue, config: WorkerConfig) -> Self {
        Self {
            queue,
            config,
            handlers: HashMap::new(),
            backoff: default_backoff(),
        }
    }

    /// Override the retry policy. Defaults to exponential with full jitter.
    pub fn backoff(mut self, policy: BackoffPolicy) -> Self {
        self.backoff = policy;
        self
    }

    /// Register a handler for a task name.
    pub fn register<F, Fut>(mut self, task: impl Into<String>, f: F) -> Self
    where
        F: Fn(Job) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>>
            + Send
            + 'static,
    {
        let task = task.into();
        self.handlers
            .insert(task, Arc::new(move |job| Box::pin(f(job)) as HandlerFuture));
        self
    }

    /// Run until `shutdown` fires. Returns once in-flight jobs have settled.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let hostname = hostname();
        let pid = std::process::id() as i32;
        let worker_id = self
            .queue
            .register_worker(&hostname, pid, std::slice::from_ref(&self.config.queue))
            .await?;

        info!(%worker_id, queue = %self.config.queue, "worker registered");

        // Heartbeat on its own task. If this stops, the reaper reclaims our
        // jobs — which is the intended behaviour, not a bug to paper over.
        let hb_queue = self.queue.clone();
        let hb_interval = self.config.heartbeat_interval;
        let mut hb_shutdown = shutdown.clone();
        let heartbeat = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(hb_interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = hb_queue.heartbeat(worker_id).await {
                            // Don't die: a transient DB blip should not take the
                            // worker down. The reaper is the backstop.
                            warn!(error = %e, "heartbeat failed");
                        }
                    }
                    _ = hb_shutdown.changed() => break,
                }
            }
        });

        let sem = Arc::new(tokio::sync::Semaphore::new(self.config.max_concurrency));
        let mut ticker = tokio::time::interval(self.config.poll_interval);

        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.changed() => {
                    info!(%worker_id, "shutdown requested");
                    break;
                }
            }

            // Only claim what we have capacity to run. Claiming more would park
            // jobs in `executing` behind a semaphore where the reaper cannot
            // tell them from genuinely running work.
            let available = sem.available_permits();
            if available == 0 {
                continue;
            }
            let want = (available as i32).min(self.config.batch_size);

            let jobs = match self.queue.claim(&self.config.queue, worker_id, want).await {
                Ok(j) => j,
                Err(e) => {
                    error!(error = %e, "claim failed");
                    continue;
                }
            };

            for job in jobs {
                let permit = sem.clone().acquire_owned().await.expect("semaphore closed");
                let queue = self.queue.clone();
                let handler = self.handlers.get(&job.task).cloned();
                let timeout = self.config.job_timeout;
                let backoff = self.backoff;

                tokio::spawn(async move {
                    let _permit = permit;
                    execute(queue, job, worker_id, handler, timeout, backoff).await;
                });
            }
        }

        // Drain: wait for in-flight jobs rather than abandoning them to the
        // reaper. Acquiring every permit means everything finished.
        let _ = sem.acquire_many(self.config.max_concurrency as u32).await;

        heartbeat.abort();
        self.queue.deregister_worker(worker_id).await?;
        info!(%worker_id, "worker stopped");
        Ok(())
    }
}

/// Execute one job and report the outcome.
async fn execute(
    queue: Queue,
    job: Job,
    worker_id: Uuid,
    handler: Option<Handler>,
    timeout: Duration,
    backoff: BackoffPolicy,
) {
    let job_id = job.id;
    let task = job.task.clone();

    let outcome: std::result::Result<(), Error> = match handler {
        None => Err(Error::UnknownTask(task.clone())),
        Some(h) => {
            // tokio::select over a sleep, underway's model (plans/15). This
            // CANCELS the future; it cannot kill a blocking thread. A handler
            // doing sync work must use spawn_blocking itself.
            tokio::select! {
                res = h(job.clone()) => res.map_err(Error::Handler),
                _ = tokio::time::sleep(timeout) => Err(Error::Timeout(timeout)),
            }
        }
    };

    let result = match outcome {
        Ok(()) => report_success(&queue, &job, worker_id).await,
        Err(e) => report_failure(&queue, &job, worker_id, e, backoff).await,
    };

    if let Err(e) = result {
        error!(%job_id, task = %task, error = %e, "failed to report job outcome");
    }
}

async fn report_success(queue: &Queue, job: &Job, worker_id: Uuid) -> Result<()> {
    let t = queue.complete(job, worker_id).await?;
    if !t.owned {
        // The reaper took it. Someone else owns the outcome now.
        warn!(job_id = %job.id, "completed a job we no longer owned; result discarded");
    } else {
        debug!(job_id = %job.id, task = %job.task, "completed");
    }
    Ok(())
}

async fn report_failure(
    queue: &Queue,
    job: &Job,
    worker_id: Uuid,
    err: Error,
    backoff: BackoffPolicy,
) -> Result<()> {
    let kind = match &err {
        Error::Timeout(_) => FailureKind::Timeout,
        e if e.is_unrecoverable() => FailureKind::Unrecoverable,
        _ => FailureKind::Raised,
    };

    // `attempt` was already incremented at claim, so it is 1-indexed here,
    // which is what chrono-machines expects.
    let retry_delay = if kind == FailureKind::Unrecoverable {
        None
    } else {
        let mut rng = chrono_machines::rand::rng();
        let ms = backoff
            .delay(job.attempt.clamp(0, u8::MAX as i16) as u8, &mut rng)
            // Defence in depth against the hot-loop described in
            // default_backoff: a policy that declines to produce a delay must
            // never mean "retry immediately". If the caller supplied a policy
            // whose budget is shorter than the job's, fall back to the cap.
            .unwrap_or(FALLBACK_RETRY_DELAY_MS);
        Some(Duration::from_millis(ms))
    };

    let t = queue
        .fail(job, worker_id, kind, &err.to_string(), retry_delay)
        .await?;

    if !t.owned {
        warn!(job_id = %job.id, "failed a job we no longer owned; result discarded");
    } else {
        warn!(
            job_id = %job.id, task = %job.task, attempt = job.attempt,
            ?kind, error = %err, "job failed"
        );
    }
    Ok(())
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests;
