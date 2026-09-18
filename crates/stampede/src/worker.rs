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
use crate::fairness::{Fairness, TenantLimit};
use crate::job::{FailureKind, Job};
use crate::metrics::{Metrics, label, names};
use crate::queue::Queue;
use crate::wakeup::{NextPoll, Wakeup};
use breaker_machines::async_circuit::AsyncCircuitBreaker;
use chrono::Utc;
use chrono_machines::{BackoffPolicy, BackoffStrategy, ExponentialBackoff};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use throttle_machines::gate::Gate;
use throttle_machines::gcra::{Gcra, GcraParams};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// What a task handler returns.
pub type HandlerFuture = Pin<
    Box<
        dyn Future<Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>>
            + Send,
    >,
>;

/// A task handler: receives the job, does the work.
pub type Handler = Arc<dyn Fn(JobContext) -> HandlerFuture + Send + Sync>;

/// What a handler gets: the job, plus the ability to save resume state.
///
/// Derefs to [`Job`], so `ctx.args` and `ctx.id` read as before. The addition
/// is [`checkpoint`](JobContext::checkpoint) — saving progress needs the queue
/// handle and this worker's id, and a handler has no other way to get either.
#[derive(Clone)]
pub struct JobContext {
    pub job: Job,
    queue: Queue,
    worker_id: Uuid,
}

impl std::ops::Deref for JobContext {
    type Target = Job;

    fn deref(&self) -> &Job {
        &self.job
    }
}

impl JobContext {
    /// Save resume state. Returns `false` if this worker no longer owns the
    /// job, which means the reaper reclaimed it and the work should stop.
    ///
    /// Checkpointing also counts as a heartbeat, so a job that saves progress
    /// regularly cannot be expired by the wall-clock timeout while it is
    /// plainly working.
    ///
    /// Two separate writes, so a crash between the work and the cursor replays
    /// that chunk on resume. For anything not idempotent, use
    /// [`checkpoint_on`](JobContext::checkpoint_on) with your transaction
    /// instead and the question does not arise.
    pub async fn checkpoint<T: serde::Serialize>(&self, state: &T) -> Result<bool> {
        Ok(self.queue.checkpoint(&self.job, self.worker_id, state).await?.is_some())
    }

    /// Save resume state inside the caller's transaction, so the cursor and
    /// the work it describes commit together or not at all.
    pub async fn checkpoint_on<'e, E, T>(&self, executor: E, state: &T) -> Result<bool>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
        T: serde::Serialize,
    {
        Ok(self
            .queue
            .checkpoint_on(executor, &self.job, self.worker_id, state)
            .await?
            .is_some())
    }

    /// The queue this job came from, for work that needs its own transaction.
    pub fn queue(&self) -> &Queue {
        &self.queue
    }

    /// Resume state from a previous attempt, if any.
    pub fn resume_from<T: serde::de::DeserializeOwned>(&self) -> Option<T> {
        self.job.resume_from()
    }

    /// Tell the database this job is still progressing, without saving state.
    pub async fn touch(&self) -> Result<bool> {
        self.queue.touch(&self.job, self.worker_id).await
    }
}

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
    /// Per-tenant admission rate. `None` disables fairness entirely.
    pub tenant_limit: Option<TenantLimit>,
    /// Trip a task's circuit after this many consecutive failures, so a failing
    /// downstream is not hammered by every retrying job. `None` disables it.
    pub breaker_failure_threshold: Option<usize>,
    /// How long to let in-flight jobs finish on shutdown.
    ///
    /// BOUNDED, deliberately. Draining is what stops a routine deploy charging
    /// every in-flight job an interruption (plans/15) — but an unbounded drain
    /// means one wedged job blocks the deploy forever, and the supervisor
    /// SIGKILLs the process anyway without ever deregistering the worker.
    /// Past this, the stragglers are left to the reaper, which is the honest
    /// outcome: from the queue's point of view the process did die on them.
    pub shutdown_timeout: Duration,

    /// Minimum gap between claim round trips, however many wakeups arrive.
    ///
    /// GCRA on the RECEIVER side. The sender is a trigger with no memory across
    /// transactions, so it cannot pace itself — and the transaction-level
    /// payload collapsing already does the sender's job for the case that
    /// matters. River puts a limiter on its client because River's client is
    /// what sends the notification; ours is the database.
    ///
    /// 5 ms is Oban's `dispatch_cooldown`, giving a ceiling of (1000/5) x batch
    /// jobs/s. River's 100 ms is 20x more conservative, but our claim is one
    /// statement rather than a CTE round trip.
    pub wakeup_cooldown: Duration,
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
            tenant_limit: Some(TenantLimit::default()),
            breaker_failure_threshold: Some(10),
            wakeup_cooldown: Duration::from_millis(5),
            shutdown_timeout: Duration::from_secs(25),
        }
    }
}

/// Monotonic-ish seconds for the GCRA gate. Same clock the fairness gate uses.
fn now_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
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
    /// `None` means poll-only, which is always correct and never wrong —
    /// just slower to start a job.
    wakeup: Option<Wakeup>,
    metrics: Metrics,
    handlers: HashMap<String, Handler>,
    backoff: BackoffPolicy,
    fairness: Option<Arc<Fairness>>,
    /// One breaker per task name. A failing `charge_card` must not open the
    /// circuit for `send_email`.
    breakers: Arc<std::sync::Mutex<HashMap<String, Arc<AsyncCircuitBreaker>>>>,
}

impl Worker {
    pub fn new(queue: Queue, config: WorkerConfig) -> Self {
        let fairness = config.tenant_limit.map(|l| Arc::new(Fairness::new(l)));
        Self {
            queue,
            config,
            wakeup: None,
            metrics: Metrics::noop(),
            handlers: HashMap::new(),
            backoff: default_backoff(),
            fairness,
            breakers: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Wake this worker on NOTIFY as well as on the poll interval.
    ///
    /// Strictly an accelerant. Without it the worker is correct and starts jobs
    /// within one poll interval; with it, within milliseconds.
    pub fn with_wakeup(mut self, wakeup: Wakeup) -> Self {
        self.wakeup = Some(wakeup);
        self
    }

    /// Send measurements somewhere. Defaults to nowhere.
    pub fn with_metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Get or create the circuit breaker for a task.
    fn breaker_for(&self, task: &str, threshold: usize) -> Arc<AsyncCircuitBreaker> {
        let mut map = self.breakers.lock().expect("breaker map poisoned");
        map.entry(task.to_string())
            .or_insert_with(|| {
                Arc::new(
                    AsyncCircuitBreaker::builder(format!("task:{task}"))
                        .failure_threshold(threshold)
                        // Jittered recovery: without it, every worker probes the
                        // recovering downstream at the same instant -- the same
                        // herd problem the retry backoff exists to avoid.
                        .jitter_factor(1.0)
                        .half_open_timeout_secs(30.0)
                        .build_async(),
                )
            })
            .clone()
    }

    /// Override the retry policy. Defaults to exponential with full jitter.
    pub fn backoff(mut self, policy: BackoffPolicy) -> Self {
        self.backoff = policy;
        self
    }

    /// Register a handler for a task name.
    pub fn register<F, Fut>(mut self, task: impl Into<String>, f: F) -> Self
    where
        F: Fn(JobContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>>
            + Send
            + 'static,
    {
        let task = task.into();
        self.handlers
            .insert(task, Arc::new(move |ctx| Box::pin(f(ctx)) as HandlerFuture));
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
        let mut wakeups = self
            .wakeup
            .as_ref()
            .map(|w| w.subscribe(&self.config.queue));

        // GCRA state for the wakeup gate. One tenant of one: this paces THIS
        // worker's claims, nothing else.
        let cooldown = self.config.wakeup_cooldown.as_secs_f64();
        let mut tat = 0.0f64;
        let mut next = NextPoll::Idle;

        loop {
            let delay = next.delay(self.config.poll_interval);

            // The poll floor is never disabled, notifications or not. A
            // redundant wakeup costs one empty query; a missed one costs a
            // stuck job, and nine of nine notifying implementations surveyed
            // still poll unconditionally.
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                // Only selected when notifications are wired up; otherwise this
                // branch is disabled and the sleep above is the only wakeup.
                _ = async {
                    match wakeups.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {}
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
                next = NextPoll::Saturated;
                continue;
            }

            // A burst of notifications must not become a burst of claims. The
            // payload collapsing already bounds one transaction; this bounds a
            // thousand small ones, which is the shape a busy enqueuer has.
            let decision = Gcra::check(
                tat,
                now_secs(),
                GcraParams {
                    emission_interval: cooldown,
                    delay_tolerance: cooldown * 4.0,
                },
            );
            tat = decision.state;
            if !decision.allowed {
                tokio::time::sleep(Duration::from_secs_f64(
                    decision.retry_after.clamp(0.0, 1.0),
                ))
                .await;
            }

            let want = (available as i32).min(self.config.batch_size);

            // Control plane at job fidelity (Oban's engine events). Claim
            // latency is the first thing to move when the database is in
            // trouble, and the last thing anybody thinks to measure.
            let claim_started = std::time::Instant::now();
            let jobs = match self.queue.claim(&self.config.queue, worker_id, want).await {
                Ok(j) => j,
                Err(e) => {
                    error!(error = %e, "claim failed");
                    next = NextPoll::Idle;
                    continue;
                }
            };
            self.metrics.duration(
                names::CLAIM_DURATION_SECONDS,
                claim_started.elapsed(),
                &[label("queue", self.config.queue.clone())],
            );

            // Decided BEFORE the jobs are consumed below. A full batch means
            // there is probably more waiting, so the next claim happens
            // immediately instead of a poll interval later — the difference
            // between draining a backlog at full speed and at one batch per
            // second.
            next = NextPoll::after_claim(jobs.len(), want as usize, sem.available_permits());

            for job in jobs {
                // Fairness is applied AFTER claiming, not before. Claiming is
                // ordered by priority and age, so a tenant's flood is
                // legitimately first in line — the only place to intervene is
                // at execution. A throttled job is released back to the queue
                // with a short delay rather than held, so another worker (or
                // another tenant's turn) can take it.
                if let Some(f) = &self.fairness
                    && let crate::fairness::Admission::Throttled { retry_after_secs } =
                        f.admit(job.tenant_id.as_deref())
                {
                    let delay = Duration::from_secs_f64(retry_after_secs.clamp(0.05, 60.0));
                    debug!(
                        job_id = %job.id, tenant = ?job.tenant_id,
                        ?delay, "tenant throttled; releasing job"
                    );
                    let _ = self
                        .queue
                        .fail(
                            &job,
                            worker_id,
                            FailureKind::Throttled,
                            "throttled: tenant rate limit",
                            Some(delay),
                        )
                        .await;
                    continue;
                }

                let permit = sem.clone().acquire_owned().await.expect("semaphore closed");
                let queue = self.queue.clone();
                let handler = self.handlers.get(&job.task).cloned();
                let timeout = self.config.job_timeout;
                let backoff = self.backoff;
                let breaker = self
                    .config
                    .breaker_failure_threshold
                    .map(|t| self.breaker_for(&job.task, t));
                let metrics = self.metrics.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    execute(
                        queue, job, worker_id, handler, timeout, backoff, breaker, metrics,
                    )
                    .await;
                });
            }
        }

        // Drain: let in-flight jobs finish and report their own outcome, so a
        // graceful shutdown never records a worker death. Acquiring every
        // permit means everything finished.
        let in_flight = self.config.max_concurrency - sem.available_permits();
        if in_flight > 0 {
            info!(%worker_id, in_flight, "draining");
        }

        let drained = tokio::time::timeout(
            self.config.shutdown_timeout,
            sem.acquire_many(self.config.max_concurrency as u32),
        )
        .await;

        if drained.is_err() {
            // Left to the reaper, which WILL charge them an interruption.
            // Correct: they outlived the shutdown window, so from the queue's
            // point of view this process died on them.
            warn!(
                %worker_id,
                timeout = ?self.config.shutdown_timeout,
                still_running = self.config.max_concurrency - sem.available_permits(),
                "jobs did not finish draining; the reaper will reclaim them"
            );
        }

        heartbeat.abort();
        self.queue.deregister_worker(worker_id).await?;
        info!(%worker_id, "worker stopped");
        Ok(())
    }
}

/// Execute one job and report the outcome.
/// Map an execution error onto the failure taxonomy.
///
/// One function so the metric label and the recorded `failure_kind` cannot
/// disagree — if they did, a dashboard filtered by kind would quietly miss
/// exactly the failures someone was looking for.
fn failure_kind_of(err: &Error) -> FailureKind {
    match err {
        Error::Timeout(_) => FailureKind::TimedOut,
        // The downstream is sick, not the job. `breaker_open` REFUNDS the
        // attempt spent at claim: the job never got a fair run, and burning its
        // budget on someone else's outage would mean a long enough downstream
        // failure discards the entire backlog rather than replaying it.
        Error::CircuitOpen(_) => FailureKind::BreakerOpen,
        e if e.is_unrecoverable() => FailureKind::Unrecoverable,
        _ => FailureKind::Raised,
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    queue: Queue,
    job: Job,
    worker_id: Uuid,
    handler: Option<Handler>,
    timeout: Duration,
    backoff: BackoffPolicy,
    breaker: Option<Arc<AsyncCircuitBreaker>>,
    metrics: Metrics,
) {
    let job_id = job.id;
    let task = job.task.clone();
    let labels = [
        label("queue", job.queue.clone()),
        label("task", task.clone()),
    ];

    // How long the job waited to start. Oban's :queue_time.
    metrics.duration(
        names::JOB_QUEUE_TIME_SECONDS,
        (Utc::now() - job.scheduled_at).to_std().unwrap_or_default(),
        &labels,
    );

    let started = std::time::Instant::now();

    let outcome: std::result::Result<(), Error> = match handler {
        None => {
            // celery's task_unknown. A rename deployed to the enqueuer but not
            // to the workers is a real and recurring production failure, and it
            // is invisible almost everywhere else -- River discards such jobs
            // with a log line and no counter, so nobody finds out until the
            // work silently stops happening.
            warn!(%job_id, task = %task, "no handler registered for this task");
            metrics.count(names::JOBS_UNKNOWN_TASK, 1, &labels);
            Err(Error::UnknownTask(task.clone()))
        }
        Some(h) => {
            let ctx = JobContext {
                job: job.clone(),
                queue: queue.clone(),
                worker_id,
            };

            let run = || async {
                // tokio::select over a sleep, underway's model (plans/15). This
                // CANCELS the future; it cannot kill a blocking thread. A
                // handler doing sync work must use spawn_blocking itself.
                tokio::select! {
                    res = h(ctx.clone()) => res.map_err(Error::Handler),
                    _ = tokio::time::sleep(timeout) => Err(Error::Timeout(timeout)),
                }
            };

            match breaker {
                None => run().await,
                // Per-task circuit breaking: when a downstream is failing,
                // retrying 1000 jobs against it makes the outage worse. An open
                // circuit fails fast WITHOUT calling the handler, and the job
                // goes back to retryable rather than being discarded — the
                // downstream being sick is not the job's fault.
                Some(b) => match b.call(run).await {
                    Ok(()) => Ok(()),
                    // The handler ran and failed: report the real error, so the
                    // failure taxonomy and the retry budget stay accurate.
                    Err(breaker_machines::CircuitError::Execution(e)) => Err(e),
                    // The handler never ran. Distinguished deliberately -- this
                    // is a downstream outage, not a defect in this job, and
                    // conflating them would make a sick dependency look like a
                    // thousand broken jobs in failure_summary.
                    Err(other) => Err(Error::CircuitOpen(other.to_string())),
                },
            }
        }
    };

    metrics.duration(names::JOB_DURATION_SECONDS, started.elapsed(), &labels);

    let result = match outcome {
        Ok(()) => {
            metrics.count(names::JOBS_COMPLETED, 1, &labels);
            report_success(&queue, &job, worker_id).await
        }
        Err(e) => {
            let kind = failure_kind_of(&e);
            let with_kind = [
                labels[0].clone(),
                labels[1].clone(),
                label("failure_kind", kind.as_str()),
            ];
            // Interruptions are counted separately from failures, because they
            // are a different question: "is our infrastructure killing jobs"
            // rather than "is this job broken". Nobody else can chart the
            // difference, because nobody else records it.
            if kind.is_interruption() {
                metrics.count(names::JOBS_INTERRUPTED, 1, &with_kind);
            } else {
                metrics.count(names::JOBS_FAILED, 1, &with_kind);
            }
            report_failure(&queue, &job, worker_id, e, backoff).await
        }
    };

    if let Err(e) = result {
        error!(%job_id, task = %task, error = %e, "failed to report job outcome");
    }
}

async fn report_success(queue: &Queue, job: &Job, worker_id: Uuid) -> Result<()> {
    let t = queue.complete(job, worker_id).await?;
    t.verify(crate::lifecycle::Transitions::Complete);
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
    let kind = failure_kind_of(&err);

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

    // Which transition SQL should have made depends on whether there was budget
    // left, and SQL decides that -- so check against what it reported rather
    // than against what we guessed.
    t.verify(if t.now.as_deref() == Some("discarded") {
        crate::lifecycle::Transitions::Discard
    } else {
        crate::lifecycle::Transitions::Retry
    });

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
