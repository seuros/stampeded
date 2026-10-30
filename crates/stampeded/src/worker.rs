//! The worker: claim, execute, report, heartbeat.
//! No connection is held while a job runs, and a job no longer owned is never written to.

use crate::claim::Claimer;
use crate::error::{Error, Result};
use crate::fairness::{Fairness, TenantLimit, now_secs};
use crate::job::{FailureKind, Job};
use crate::metrics::{Metrics, label, names};
use crate::queue::Queue;
use crate::queues::Queues;
use crate::wakeup::{NextPoll, Wakeup};
use breaker_machines::async_circuit::AsyncCircuitBreaker;
use chrono_machines::{BackoffPolicy, BackoffStrategy, ExponentialBackoff};
use serde::Serialize;
use serde::de::DeserializeOwned;
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

/// What a handler gets: the job (through `Deref`), plus checkpoints and steps.
#[derive(Clone)]
pub struct JobContext {
    pub job: Job,
    queue: Queue,
    worker_id: Uuid,
    /// Step record for this attempt; `job.checkpoint` is the claim-time snapshot and goes stale.
    steps: Arc<std::sync::Mutex<crate::steps::StepState>>,
}

impl std::ops::Deref for JobContext {
    type Target = Job;

    fn deref(&self) -> &Job {
        &self.job
    }
}

impl JobContext {
    pub(crate) fn new(job: Job, queue: Queue, worker_id: Uuid) -> Self {
        let steps = crate::steps::StepState::from_checkpoint(job.checkpoint.as_ref());
        Self {
            job,
            queue,
            worker_id,
            steps: Arc::new(std::sync::Mutex::new(steps)),
        }
    }

    /// Save resume state; `false` if the job was reclaimed and the work should stop.
    /// A separate write from the work: use `checkpoint_on` when replay is not idempotent.
    pub async fn checkpoint<T: serde::Serialize + Sync>(&self, state: &T) -> Result<bool> {
        Ok(self
            .queue
            .checkpoint(&self.job, self.worker_id, state)
            .await?
            .is_some())
    }

    /// Save resume state in the caller's transaction, so cursor and work commit together.
    pub async fn checkpoint_on<'e, E, T>(&self, executor: E, state: &T) -> Result<bool>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
        T: serde::Serialize + Sync,
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

    /// The worker that holds this job.
    pub const fn worker_id(&self) -> Uuid {
        self.worker_id
    }

    /// The step record: what earlier attempts completed, plus this attempt so far.
    pub fn steps(&self) -> crate::steps::StepState {
        self.steps.lock().expect("step state poisoned").clone()
    }

    /// Run a step once, ever; on resume a completed step is skipped and its value returned.
    ///
    /// ```ignore
    /// ctx.step("charge", async { charge(&order).await }).await?;
    /// let path: String = ctx.step("render", async { render(&order).await }).await?;
    /// ```
    pub async fn step<T, F>(&self, name: &str, f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Default,
        F: Future<Output = Result<T>>,
    {
        if let Some(done) = {
            let state = self.steps.lock().expect("step state poisoned");
            // No stored value returns the type's default: right for the common `()`.
            state
                .completed(name)
                .then(|| state.value(name).unwrap_or_default())
        } {
            return Ok(done);
        }

        let value = f.await?;

        // Snapshot under the lock and write outside it: the guard is not Send.
        let state = {
            let mut state = self.steps.lock().expect("step state poisoned");
            state.complete(name, Some(&value))?;
            state.clone()
        };
        self.checkpoint(&state).await?;
        Ok(value)
    }

    /// Record a cursor within a step, handed back by `steps` on the next attempt.
    /// Commit the work first, or use `checkpoint_on` so both land together.
    pub async fn advance<T: Serialize + Sync>(&self, name: &str, cursor: &T) -> Result<bool> {
        let state = {
            let mut state = self.steps.lock().expect("step state poisoned");
            state.set_cursor(name, cursor)?;
            state.clone()
        };
        self.checkpoint(&state).await
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
    /// One queue, several in turn, or every queue that has work.
    pub queues: Queues,
    /// How many jobs to claim per round trip.
    pub batch_size: i32,
    /// Poll floor, never disabled even with NOTIFY: a missed wakeup would strand a job.
    pub poll_interval: Duration,
    /// Per-job execution timeout.
    pub job_timeout: Duration,
    /// Heartbeat cadence; must stay well under the reaper's `dead_after`.
    pub heartbeat_interval: Duration,
    pub max_concurrency: usize,
    /// Per-tenant admission rate. `None` disables fairness entirely.
    pub tenant_limit: Option<TenantLimit>,
    /// Consecutive failures that trip a task's circuit. `None` disables it.
    pub breaker_failure_threshold: Option<usize>,
    /// How long in-flight jobs may finish on shutdown; stragglers are left to the reaper.
    pub shutdown_timeout: Duration,

    /// Minimum gap between claim round trips, however many wakeups arrive.
    /// The 1 ms default sits above one claim's cost, so it only coalesces bursts.
    pub wakeup_cooldown: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            queues: Queues::default(),
            batch_size: 5,
            poll_interval: Duration::from_secs(1),
            job_timeout: Duration::from_mins(15),
            heartbeat_interval: Duration::from_secs(30),
            max_concurrency: 10,
            tenant_limit: Some(TenantLimit::default()),
            breaker_failure_threshold: Some(10),
            wakeup_cooldown: Duration::from_millis(1),
            shutdown_timeout: Duration::from_secs(25),
        }
    }
}

/// Used when a backoff policy declines to produce a delay.
const FALLBACK_RETRY_DELAY_MS: u64 = 60 * 1_000;

/// The default retry policy: exponential, capped at one hour, full jitter.
pub fn default_backoff() -> BackoffPolicy {
    BackoffPolicy::Exponential(
        ExponentialBackoff::new()
            .base_delay_ms(1_000)
            .max_delay_ms(60 * 60 * 1_000)
            .jitter_factor(1.0)
            // Not the crate default of 3: the database owns the attempt limit, and a
            // `None` delay past it would mean an immediate retry.
            .max_attempts(u8::MAX),
    )
}

/// Schema defaults for `backoff_base_ms` / `backoff_max_ms`: a row carrying them defers to
/// the worker's policy; anything else overrides it.
const SCHEMA_BACKOFF_BASE_MS: i16 = 1_000;
const SCHEMA_BACKOFF_MAX_MS: i32 = 3_600_000;

/// The policy to retry this job with: its own backoff columns if set, as the Ruby worker does.
fn policy_for(job: &Job, configured: BackoffPolicy) -> BackoffPolicy {
    if job.backoff_base_ms == SCHEMA_BACKOFF_BASE_MS && job.backoff_max_ms == SCHEMA_BACKOFF_MAX_MS
    {
        return configured;
    }

    // Full jitter and the u8::MAX attempt budget, as in `default_backoff`.
    BackoffPolicy::Exponential(
        ExponentialBackoff::new()
            .base_delay_ms(u64::from(job.backoff_base_ms.max(1).unsigned_abs()))
            .max_delay_ms(u64::from(job.backoff_max_ms.max(1).unsigned_abs()))
            .jitter_factor(1.0)
            .max_attempts(u8::MAX),
    )
}

pub struct Worker {
    queue: Queue,
    config: WorkerConfig,
    /// `None` means poll-only: always correct, just slower to start a job.
    wakeup: Option<Wakeup>,
    metrics: Metrics,
    handlers: HashMap<String, Handler>,
    backoff: BackoffPolicy,
    fairness: Option<Arc<Fairness>>,
    /// One breaker per task name, so one failing task does not open another's circuit.
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

    /// Wake this worker on NOTIFY as well as on the poll interval; an accelerant only.
    #[must_use]
    pub fn with_wakeup(mut self, wakeup: Wakeup) -> Self {
        self.wakeup = Some(wakeup);
        self
    }

    /// Send measurements somewhere. Defaults to nowhere.
    #[must_use]
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
                        // Jittered, so workers do not all probe a recovering downstream at once.
                        .jitter_factor(1.0)
                        .half_open_timeout_secs(30.0)
                        .build_async(),
                )
            })
            .clone()
    }

    /// Override the retry policy. Defaults to exponential with full jitter.
    #[must_use]
    pub fn backoff(mut self, policy: BackoffPolicy) -> Self {
        self.backoff = policy;
        self
    }

    /// Register a handler for a task name.
    #[must_use]
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
        let worker_id = register_worker(&self.queue, &self.config.queues).await?;
        let queues_label = self.config.queues.label();

        info!(%worker_id, queues = %queues_label, "worker registered");

        // If the heartbeat stops, the reaper reclaims our jobs, as intended.
        let hb_queue = self.queue.clone();
        let hb_interval = self.config.heartbeat_interval;
        let mut hb_shutdown = shutdown.clone();
        let heartbeat = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(hb_interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = hb_queue.heartbeat(worker_id).await {
                            // Keep beating through a blip; the reaper is the backstop.
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
            .map(|w| w.subscribe_to(&self.config.queues));
        let mut claimer = Claimer::new(
            self.queue.clone(),
            self.config.queues.clone(),
            self.config.poll_interval,
        );

        // GCRA state for the wakeup gate, pacing only this worker's claims.
        let cooldown = self.config.wakeup_cooldown.as_secs_f64();
        let mut tat = 0.0f64;
        let mut next = NextPoll::Idle;

        loop {
            let delay = next.delay(self.config.poll_interval);

            // The poll floor stays on, notifications or not.
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = async {
                    match wakeups.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {}
                _ = shutdown.changed() => {
                    info!(%worker_id, "shutdown requested");
                    break;
                }
                // Saturated only: a freed permit wakes us, dropped right back as a signal.
                _ = sem.acquire(), if next == NextPoll::Saturated => {}
            }

            // Claim only what can run; a job parked behind the semaphore looks like running work.
            let available = sem.available_permits();
            if available == 0 {
                next = NextPoll::Saturated;
                continue;
            }

            // Pace claims so a burst of notifications does not become a burst of round trips.
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

            let want = i32::try_from(available)
                .unwrap_or(i32::MAX)
                .min(self.config.batch_size);

            let claim_started = std::time::Instant::now();
            let jobs = match claimer
                .round(worker_id, usize::try_from(want).unwrap_or(0))
                .await
            {
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
                &[label("queue", queues_label.clone())],
            );

            // Decided before the jobs are consumed: a full batch claims again immediately.
            next = NextPoll::after_claim(
                jobs.len(),
                usize::try_from(want).unwrap_or(0),
                sem.available_permits(),
            );

            for (job, held) in jobs {
                // Fairness after claiming, since a flood is legitimately first in line; a
                // throttled job goes back with a short delay rather than held.
                if let Some(f) = &self.fairness
                    && let crate::fairness::Admission::Throttled { retry_after_secs } =
                        f.admit(job.tenant_id.as_deref())
                {
                    let delay = Duration::from_secs_f64(retry_after_secs.clamp(0.05, 60.0));
                    if let Err(e) = record_failure(
                        &self.queue,
                        &job,
                        worker_id,
                        FailureKind::Throttled,
                        "throttled: tenant rate limit",
                        Some(delay),
                    )
                    .await
                    {
                        warn!(job_id = %job.id, error = %e, "could not release a throttled job");
                    }
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
                    // The slot and this queue's cap stay taken until the outcome is reported.
                    let _slot = (permit, held);
                    execute(
                        queue, job, worker_id, handler, timeout, backoff, breaker, metrics,
                    )
                    .await;
                });
            }
        }

        // Drain: in-flight jobs report their own outcome; every permit back means all finished.
        let in_flight = self.config.max_concurrency - sem.available_permits();
        if in_flight > 0 {
            info!(%worker_id, in_flight, "draining");
        }

        let drained = tokio::time::timeout(
            self.config.shutdown_timeout,
            sem.acquire_many(u32::try_from(self.config.max_concurrency).unwrap_or(u32::MAX)),
        )
        .await
        .is_ok();

        if !drained {
            // Left to the reaper, which charges an interruption: this process did die on them.
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

/// Map an execution error onto the failure taxonomy, shared by the metric label and the row.
fn failure_kind_of(err: &Error) -> FailureKind {
    match err {
        Error::Timeout(_) => FailureKind::TimedOut,
        // The downstream is sick, not the job: `breaker_open` refunds the claim's attempt.
        Error::CircuitOpen(_) => FailureKind::BreakerOpen,
        e if e.is_unrecoverable() => FailureKind::Unrecoverable,
        _ => FailureKind::Raised,
    }
}

/// How long a job waited to be claimed, both ends on the database clock; `None` if unclaimed.
fn queue_time(job: &Job) -> Option<Duration> {
    let claimed_at = job.claimed_at?;
    Some(Duration::try_from(claimed_at.duration_since(job.scheduled_at)).unwrap_or_default())
}

/// Execute one job and report the outcome.
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

    // Every job here came out of `stampeded_claim`, so `claimed_at` is set.
    if let Some(waited) = queue_time(&job) {
        metrics.duration(names::JOB_QUEUE_TIME_SECONDS, waited, &labels);
    }

    let started = std::time::Instant::now();

    let outcome: std::result::Result<(), Error> = match handler {
        None => {
            // A task renamed on the enqueuer but not the workers: count it, or nobody notices.
            warn!(%job_id, task = %task, "no handler registered for this task");
            metrics.count(names::JOBS_UNKNOWN_TASK, 1, &labels);
            Err(Error::UnknownTask(task.clone()))
        }
        Some(h) => {
            // expire_seconds is per job, set by the enqueuer; at or under job_timeout, the
            // database can expire a running job and let a second worker claim it.
            if timeout.as_secs() >= u64::from(job.expire_seconds.max(0).unsigned_abs()) {
                warn!(
                    %job_id,
                    task = %task,
                    job_timeout_secs = timeout.as_secs(),
                    expire_seconds = job.expire_seconds,
                    "job_timeout is not shorter than this job's expire_seconds: the \
                     database may expire it and let another worker claim it while this \
                     one is still running it"
                );
            }

            let ctx = JobContext::new(job.clone(), queue.clone(), worker_id);

            let run = || async {
                // Cancels the future but cannot kill a blocking thread; sync handlers
                // must use spawn_blocking.
                tokio::select! {
                    res = h(ctx.clone()) => res.map_err(Error::Handler),
                    () = tokio::time::sleep(timeout) => Err(Error::Timeout(timeout)),
                }
            };

            match breaker {
                None => run().await,
                // An open circuit fails fast without calling the handler, and the job
                // goes back to retryable rather than discarded.
                Some(b) => match b.call(run).await {
                    Ok(()) => Ok(()),
                    // The handler ran and failed: report the real error.
                    Err(breaker_machines::CircuitError::Execution(e)) => Err(e),
                    // The handler never ran: an outage, not a defect in this job.
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
            // Counted apart from failures: infrastructure killing jobs, not broken jobs.
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

/// Register this process as a worker serving `queues`.
pub(crate) async fn register_worker(queue: &Queue, queues: &Queues) -> Result<Uuid> {
    // PIDs fit in i32 on every platform Postgres runs on.
    let pid = std::process::id().cast_signed();
    queue
        .register_worker(&hostname(), pid, &queues.registered())
        .await
}

pub(crate) async fn report_success(queue: &Queue, job: &Job, worker_id: Uuid) -> Result<()> {
    let t = queue.complete(job, worker_id).await?;
    t.verify(crate::lifecycle::Transitions::Complete);
    if t.owned {
        debug!(job_id = %job.id, task = %job.task, "completed");
    } else {
        // The reaper took it. Someone else owns the outcome now.
        warn!(job_id = %job.id, "completed a job we no longer owned; result discarded");
    }
    Ok(())
}

/// A handler's own retry delay, if it asked for one.
fn retry_in_of(err: &Error) -> Option<Duration> {
    match err {
        Error::Handler(src) => src.downcast_ref::<crate::RetryIn>().map(|r| r.0),
        _ => None,
    }
}

async fn report_failure(
    queue: &Queue,
    job: &Job,
    worker_id: Uuid,
    err: Error,
    backoff: BackoffPolicy,
) -> Result<()> {
    let kind = failure_kind_of(&err);

    let delay = if kind == FailureKind::Unrecoverable {
        None
    } else if let Some(r) = retry_in_of(&err) {
        // The handler's own delay (`RetryIn`), as the Ruby worker honours it.
        Some(r)
    } else {
        Some(retry_delay(job, backoff))
    };

    record_failure(queue, job, worker_id, kind, &err.to_string(), delay).await
}

/// When this job should be tried again, from its own backoff columns or the worker's policy.
pub(crate) fn retry_delay(job: &Job, backoff: BackoffPolicy) -> Duration {
    // `attempt` was incremented at claim, so it is 1-indexed, as chrono-machines expects.
    let mut rng = chrono_machines::rand::rng();
    let ms = policy_for(job, backoff)
        .delay(
            u8::try_from(job.attempt.max(0)).unwrap_or(u8::MAX),
            &mut rng,
        )
        // A policy that declines a delay must never mean an immediate retry.
        .unwrap_or(FALLBACK_RETRY_DELAY_MS);
    Duration::from_millis(ms)
}

/// Log a failure SQL recorded; a deferral back in the queue is backpressure, so only debug.
fn log_failure(
    job: &Job,
    kind: FailureKind,
    message: &str,
    retry_in: Option<Duration>,
    discarded: bool,
) {
    if kind.is_deferral() && !discarded {
        debug!(
            job_id = %job.id, task = %job.task, tenant = ?job.tenant_id,
            ?kind, reason = %message, ?retry_in, "job deferred"
        );
    } else {
        warn!(
            job_id = %job.id, task = %job.task, attempt = job.attempt,
            ?kind, error = %message, "job failed"
        );
    }
}

/// Record a failure of `kind` and check the transition SQL made.
pub(crate) async fn record_failure(
    queue: &Queue,
    job: &Job,
    worker_id: Uuid,
    kind: FailureKind,
    message: &str,
    retry_delay: Option<Duration>,
) -> Result<()> {
    let t = queue
        .fail(job, worker_id, kind, message, retry_delay)
        .await?;

    // SQL decides whether budget was left, so verify against the state it reported.
    let discarded = t.now.as_deref() == Some("discarded");
    t.verify(if discarded {
        crate::lifecycle::Transitions::Discard
    } else {
        crate::lifecycle::Transitions::Retry
    });

    if t.owned {
        log_failure(job, kind, message, retry_delay, discarded);
    } else {
        warn!(job_id = %job.id, "failed a job we no longer owned; result discarded");
    }
    Ok(())
}

/// This machine's name, for the workers table and the dashboard.
fn hostname() -> String {
    pick_hostname(
        std::env::var("HOSTNAME").ok(),
        gethostname::gethostname().into_string().ok(),
    )
}

/// `HOSTNAME` first, so a container can name itself, then the OS, then "unknown".
fn pick_hostname(env: Option<String>, os: Option<String>) -> String {
    env.into_iter()
        .chain(os)
        .find(|h| !h.trim().is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests;
