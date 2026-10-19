//! Jobs handed to the caller instead of run in-process; the shape the C ABI binds to.

use crate::claim::{Claimer, Held};
use crate::error::Result;
use crate::fairness::{Admission, Fairness, TenantLimit};
use crate::job::{FailureKind, Job};
use crate::lifecycle::Transitions;
use crate::queue::Queue;
use crate::queues::Queues;
use crate::worker::{
    JobContext, default_backoff, record_failure, register_worker, report_success, retry_delay,
};
use chrono_machines::BackoffPolicy;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PullConfig {
    pub queues: Queues,
    /// Leases out at once, handed out or waiting; nothing more is claimed past this.
    pub max_leases: usize,
    /// How many jobs to claim per round trip.
    pub batch_size: usize,
    pub poll_interval: Duration,
    /// Must stay comfortably under the reaper's `dead_after`.
    pub heartbeat_interval: Duration,
    /// Per-tenant admission rate. `None` disables fairness.
    pub tenant_limit: Option<TenantLimit>,
}

impl Default for PullConfig {
    fn default() -> Self {
        Self {
            queues: Queues::default(),
            max_leases: 10,
            batch_size: 5,
            poll_interval: Duration::from_secs(1),
            heartbeat_interval: Duration::from_secs(30),
            tenant_limit: Some(TenantLimit::default()),
        }
    }
}

struct Claimed {
    job: Job,
    held: Held,
    permit: OwnedSemaphorePermit,
}

/// A registered worker whose jobs the caller runs.
pub struct Puller {
    queue: Queue,
    worker_id: Uuid,
    config: PullConfig,
    claimer: tokio::sync::Mutex<Claimer>,
    waiting: std::sync::Mutex<VecDeque<Claimed>>,
    leases: Arc<Semaphore>,
    fairness: Option<Fairness>,
    backoff: BackoffPolicy,
    stopping: AtomicBool,
    heartbeat: JoinHandle<()>,
}

impl Puller {
    /// Register a worker and start its heartbeat.
    pub async fn start(queue: Queue, config: PullConfig) -> Result<Self> {
        let worker_id = register_worker(&queue, &config.queues).await?;
        info!(%worker_id, queues = %config.queues.label(), "puller registered");

        let hb_queue = queue.clone();
        let hb_interval = config.heartbeat_interval;
        let heartbeat = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(hb_interval);
            loop {
                ticker.tick().await;
                if let Err(e) = hb_queue.heartbeat(worker_id).await {
                    // Keep beating through a blip; the reaper is the backstop.
                    warn!(error = %e, "heartbeat failed");
                }
            }
        });

        Ok(Self {
            claimer: tokio::sync::Mutex::new(Claimer::new(
                queue.clone(),
                config.queues.clone(),
                config.poll_interval,
            )),
            waiting: std::sync::Mutex::new(VecDeque::new()),
            leases: Arc::new(Semaphore::new(config.max_leases)),
            fairness: config.tenant_limit.map(Fairness::new),
            backoff: default_backoff(),
            stopping: AtomicBool::new(false),
            queue,
            worker_id,
            config,
            heartbeat,
        })
    }

    pub const fn worker_id(&self) -> Uuid {
        self.worker_id
    }

    /// The next job within `timeout`; `None` if none came, every lease is out, or stopping.
    pub async fn next(&self, timeout: Duration) -> Result<Option<Lease>> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.stopping.load(Ordering::Acquire) {
                return Ok(None);
            }
            if let Some(claimed) = self.pop() {
                if let Some(lease) = self.admit(claimed).await {
                    return Ok(Some(lease));
                }
                continue;
            }

            let room = self.leases.available_permits().min(self.config.batch_size);
            if room > 0 {
                self.claim(room).await?;
                if !self.is_empty() {
                    continue;
                }
            }

            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(None);
            }
            tokio::time::sleep(left.min(self.config.poll_interval)).await;
        }
    }

    /// Gives unstarted jobs back, waits up to `drain` for leases to settle, then deregisters.
    /// Leases still out after that are left to the reaper.
    pub async fn stop(&self, drain: Duration) -> Result<()> {
        self.stopping.store(true, Ordering::Release);

        // The claimer lock waits out a round in flight; later rounds see `stopping`,
        // so nothing reaches `waiting` after this.
        let unstarted: Vec<Claimed> = {
            let _no_claim_in_flight = self.claimer.lock().await;
            self.waiting
                .lock()
                .expect("waiting poisoned")
                .drain(..)
                .collect()
        };
        for claimed in unstarted {
            self.give_back(&claimed.job).await?;
        }

        let all = u32::try_from(self.config.max_leases).unwrap_or(u32::MAX);
        if tokio::time::timeout(drain, self.leases.acquire_many(all))
            .await
            .is_err()
        {
            warn!(worker_id = %self.worker_id, "leases still out after the drain; the reaper will reclaim them");
        }

        self.heartbeat.abort();
        self.queue.deregister_worker(self.worker_id).await?;
        info!(worker_id = %self.worker_id, "puller stopped");
        Ok(())
    }

    async fn give_back(&self, job: &Job) -> Result<()> {
        release_claim(&self.queue, job, self.worker_id).await
    }

    fn pop(&self) -> Option<Claimed> {
        self.waiting.lock().expect("waiting poisoned").pop_front()
    }

    fn is_empty(&self) -> bool {
        self.waiting.lock().expect("waiting poisoned").is_empty()
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the permits are moved into the claimed entries, not held"
    )]
    async fn claim(&self, room: usize) -> Result<()> {
        // Slots first, so every job this round claims has one despite concurrent callers.
        let slots: Vec<OwnedSemaphorePermit> = (0..room)
            .map_while(|_| Arc::clone(&self.leases).try_acquire_owned().ok())
            .collect();
        if slots.is_empty() {
            return Ok(());
        }

        // Held, with `stopping` read under it, until the round is in `waiting`: `stop`
        // takes this lock, so a round lands before it or not at all.
        let mut claiming = self.claimer.lock().await;
        if self.stopping.load(Ordering::Acquire) {
            return Ok(());
        }
        let claimed = claiming.round(self.worker_id, slots.len()).await?;

        // round never claims more than it had room for; spare slots go back.
        let entries = claimed
            .into_iter()
            .zip(slots)
            .map(|((job, held), permit)| Claimed { job, held, permit });
        self.waiting
            .lock()
            .expect("waiting poisoned")
            .extend(entries);
        drop(claiming);
        Ok(())
    }

    /// Tenant fairness, applied after claiming since a flood is legitimately first in line.
    /// A throttled job goes back with its attempt refunded.
    async fn admit(&self, claimed: Claimed) -> Option<Lease> {
        if let Some(f) = &self.fairness
            && let Admission::Throttled { retry_after_secs } =
                f.admit(claimed.job.tenant_id.as_deref())
        {
            let delay = Duration::from_secs_f64(retry_after_secs.clamp(0.05, 60.0));
            if let Err(e) = record_failure(
                &self.queue,
                &claimed.job,
                self.worker_id,
                FailureKind::Throttled,
                "throttled: tenant rate limit",
                Some(delay),
            )
            .await
            {
                warn!(job_id = %claimed.job.id, error = %e, "could not release a throttled job");
            }
            return None;
        }

        Some(Lease {
            ctx: JobContext::new(claimed.job, self.queue.clone(), self.worker_id),
            backoff: self.backoff,
            held: Some(claimed.held),
            permit: Some(claimed.permit),
            settled: false,
        })
    }
}

impl Drop for Puller {
    /// Stops heartbeating; the worker row and any leases still out are left to the reaper.
    fn drop(&mut self) {
        self.heartbeat.abort();
    }
}

/// One claimed job, owned by the caller until it reports an outcome; dropped without one,
/// it goes back to the queue as `heartbeat_lost`.
pub struct Lease {
    ctx: JobContext,
    backoff: BackoffPolicy,
    held: Option<Held>,
    permit: Option<OwnedSemaphorePermit>,
    settled: bool,
}

impl Lease {
    pub fn job(&self) -> &Job {
        &self.ctx.job
    }

    /// Checkpoints, steps and heartbeats, as an in-process handler has them.
    pub const fn context(&self) -> &JobContext {
        &self.ctx
    }

    /// The job succeeded.
    pub async fn complete(mut self) -> Result<()> {
        self.settled = true;
        report_success(self.ctx.queue(), &self.ctx.job, self.ctx.worker_id()).await
    }

    /// The job failed; retried after `retry_in`, or on its backoff, until `max_attempts`.
    pub async fn fail(mut self, message: &str, retry_in: Option<Duration>) -> Result<()> {
        self.settled = true;
        let delay = retry_in.unwrap_or_else(|| retry_delay(&self.ctx.job, self.backoff));
        record_failure(
            self.ctx.queue(),
            &self.ctx.job,
            self.ctx.worker_id(),
            FailureKind::Raised,
            message,
            Some(delay),
        )
        .await
    }

    /// Not a failure: back in the queue with the attempt refunded and its last real outcome kept.
    pub async fn requeue(mut self) -> Result<()> {
        self.settled = true;
        release_claim(self.ctx.queue(), &self.ctx.job, self.ctx.worker_id()).await
    }

    /// The job ran past the caller's time limit; retried on backoff, recorded as `timed_out`.
    pub async fn time_out(mut self, message: &str) -> Result<()> {
        self.settled = true;
        let delay = retry_delay(&self.ctx.job, self.backoff);
        record_failure(
            self.ctx.queue(),
            &self.ctx.job,
            self.ctx.worker_id(),
            FailureKind::TimedOut,
            message,
            Some(delay),
        )
        .await
    }

    /// The job can never succeed: discard it now, whatever attempts remain.
    pub async fn discard(mut self, message: &str) -> Result<()> {
        self.settled = true;
        record_failure(
            self.ctx.queue(),
            &self.ctx.job,
            self.ctx.worker_id(),
            FailureKind::Unrecoverable,
            message,
            None,
        )
        .await
    }
}

/// Undo a claim without recording an outcome.
async fn release_claim(queue: &Queue, job: &Job, worker_id: Uuid) -> Result<()> {
    let t = queue.release(job, worker_id).await?;
    t.verify(if t.now.as_deref() == Some("available") {
        Transitions::Release
    } else {
        Transitions::Retry
    });
    if !t.owned {
        warn!(job_id = %job.id, "gave back a job we no longer owned; left as it is");
    }
    Ok(())
}

impl Drop for Lease {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            warn!(job_id = %self.ctx.job.id, "lease dropped outside a runtime; the job waits for its expiry");
            return;
        };

        let ctx = self.ctx.clone();
        // Released once the report lands, so the slot and queue cap stay taken until then.
        let held = self.held.take();
        let permit = self.permit.take();
        warn!(job_id = %ctx.job.id, task = %ctx.job.task, "lease dropped without an outcome");
        runtime.spawn(async move {
            let _slot = (held, permit);
            if let Err(e) = record_failure(
                ctx.queue(),
                &ctx.job,
                ctx.worker_id(),
                FailureKind::HeartbeatLost,
                "lease dropped without an outcome",
                None,
            )
            .await
            {
                warn!(job_id = %ctx.job.id, error = %e, "could not report a dropped lease");
            }
        });
    }
}
