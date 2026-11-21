//! Probes the host on an interval, feeds the gate, and publishes the verdict: to the claim
//! loop, the metrics and the worker row.

use super::{Readings, VitalsConfig, VitalsGate, VitalsStatus, phase_code, phase_name};
use crate::error::Result;
use crate::metrics::{Metrics, label, names};
use crate::queue::Queue;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{OwnedRwLockReadGuard, RwLock, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};
use uuid::Uuid;

/// A wall-clock step past the monotonic one by more than this is time the host slept.
const SLEEP_TOLERANCE: Duration = Duration::from_secs(2);

/// Monotonic, and keeps counting while the host is suspended, which `Instant` does not on
/// every platform: an hour asleep must read as a gap, not as an hour of headroom.
#[derive(Debug)]
pub(super) struct HostClock {
    start: Instant,
    last_mono: Duration,
    last_wall: SystemTime,
    slept: Duration,
}

impl HostClock {
    pub(super) fn new() -> Self {
        Self::starting(Instant::now(), SystemTime::now())
    }

    pub(super) const fn starting(start: Instant, wall: SystemTime) -> Self {
        Self {
            start,
            last_mono: Duration::ZERO,
            last_wall: wall,
            slept: Duration::ZERO,
        }
    }

    fn now(&mut self) -> Duration {
        self.advance(self.start.elapsed(), SystemTime::now())
    }

    /// Time since the start, given the monotonic reading `mono` and the wall clock `wall`.
    /// A wall clock stepped forward reads as a sleep too, which only restarts a window.
    pub(super) fn advance(&mut self, mono: Duration, wall: SystemTime) -> Duration {
        let mono_step = mono.saturating_sub(self.last_mono);
        if let Ok(wall_step) = wall.duration_since(self.last_wall) {
            let missed = wall_step.saturating_sub(mono_step);
            if missed > SLEEP_TOLERANCE {
                self.slept += missed;
            }
        }
        self.last_mono = self.last_mono.max(mono);
        self.last_wall = wall;
        self.last_mono + self.slept
    }
}

/// The running gate. Dropping it stops the probing.
pub struct VitalsWatch {
    status: watch::Receiver<VitalsStatus>,
    /// Read-held by a claim from its admission check until its claim query returns;
    /// write-taken by a pause, so a pause waits out a claim already past the check.
    claims: Arc<RwLock<()>>,
    task: JoinHandle<()>,
}

impl VitalsWatch {
    /// Probe once before returning, so a worker started on a full disk claims nothing.
    pub async fn start(
        config: VitalsConfig,
        queue: Queue,
        worker_id: Uuid,
        metrics: Metrics,
    ) -> Result<Self> {
        config.validate()?;
        let interval = config.probe_interval;
        let gate = VitalsGate::new(&config);
        let (tx, status) = watch::channel(gate.status());
        let claims = Arc::new(RwLock::new(()));
        let mut driver = Driver {
            tx,
            claims: Arc::clone(&claims),
            gate,
            config: Arc::new(config),
            clock: HostClock::new(),
            queue,
            worker_id,
            metrics,
            pending: None,
            last: None,
            written: None,
        };
        driver.tick().await;
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            // The first tick is immediate, and the first probe already ran.
            ticker.tick().await;
            while !driver.tx.is_closed() {
                ticker.tick().await;
                driver.tick().await;
            }
        });
        Ok(Self {
            status,
            claims,
            task,
        })
    }

    pub fn admitting(&self) -> bool {
        self.status.borrow().admitting
    }

    /// Leave to start one claim, held until its claim query returns; `None` while paused.
    pub async fn claim_permit(&self) -> Option<OwnedRwLockReadGuard<()>> {
        let permit = Arc::clone(&self.claims).read_owned().await;
        self.admitting().then_some(permit)
    }

    pub fn status(&self) -> VitalsStatus {
        self.status.borrow().clone()
    }

    pub fn stop(&self) {
        self.task.abort();
    }
}

impl Drop for VitalsWatch {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Driver {
    tx: watch::Sender<VitalsStatus>,
    claims: Arc<RwLock<()>>,
    gate: VitalsGate,
    config: Arc<VitalsConfig>,
    clock: HostClock,
    queue: Queue,
    worker_id: Uuid,
    metrics: Metrics,
    /// A probe that outlived its timeout; no second one starts until it returns.
    pending: Option<JoinHandle<Readings>>,
    last: Option<VitalsStatus>,
    /// What the worker row holds; `None` until a write lands.
    written: Option<VitalsStatus>,
}

impl Driver {
    /// A pause reaches the claim loops before anything else, and is reported only once no
    /// claim is in flight: whoever sees it paused sees a worker that claims nothing more.
    async fn tick(&mut self) {
        let readings = self.read().await;
        let status = self.gate.observe(self.clock.now(), &readings);
        let pausing = !status.admitting && self.last.as_ref().is_none_or(|l| l.admitting);
        self.tx.send_replace(status.clone());
        if pausing {
            // Claims past their check before the send finish; later ones read it paused.
            drop(self.claims.write().await);
        }
        self.measure(&status);
        self.log_change(&status);
        self.write_row(&status).await;
        self.last = Some(status);
    }

    /// A probe hung on a dead mount reads as unreadable, never as the last good reading.
    async fn read(&mut self) -> Readings {
        if self.pending.as_ref().is_some_and(|p| !p.is_finished()) {
            return Readings::unreadable(&self.config, "the previous probe has not returned");
        }
        self.pending = None;
        let config = Arc::clone(&self.config);
        let mut probe = tokio::task::spawn_blocking(move || config.probe.read(&config));
        let timeout = self
            .config
            .probe_interval
            .clamp(Duration::from_secs(5), Duration::from_secs(30));
        match tokio::time::timeout(timeout, &mut probe).await {
            Ok(Ok(readings)) => readings,
            Ok(Err(e)) => Readings::unreadable(&self.config, &format!("the probe failed: {e}")),
            Err(_) => {
                self.pending = Some(probe);
                Readings::unreadable(
                    &self.config,
                    &format!("the probe did not return within {}s", timeout.as_secs()),
                )
            }
        }
    }

    fn measure(&self, status: &VitalsStatus) {
        self.metrics.gauge(
            names::VITALS_ADMITTING,
            f64::from(u8::from(status.admitting)),
            &[],
        );
        for v in &status.vitals {
            let labels = [label("vital", v.vital.to_string())];
            self.metrics
                .gauge(names::VITALS_PHASE, f64::from(phase_code(v.phase)), &labels);
            self.metrics.gauge(
                names::VITALS_READABLE,
                f64::from(u8::from(v.readable)),
                &labels,
            );
        }
    }

    fn log_change(&self, status: &VitalsStatus) {
        let worker_id = self.worker_id;
        let was_admitting = self.last.as_ref().is_none_or(|l| l.admitting);
        match (was_admitting, status.admitting) {
            (true, false) => warn!(
                %worker_id,
                vital = ?status.holding.as_ref().map(ToString::to_string),
                phase = phase_name(status.phase),
                reason = status.reason.as_deref().unwrap_or_default(),
                "host vitals: claiming paused; jobs already claimed keep running"
            ),
            (false, true) => info!(%worker_id, "host vitals: headroom held, claiming resumed"),
            _ => {}
        }
        for (i, v) in status.vitals.iter().enumerate() {
            let was_readable = self
                .last
                .as_ref()
                .and_then(|l| l.vitals.get(i))
                .is_none_or(|l| l.readable);
            if was_readable && !v.readable {
                warn!(
                    %worker_id, vital = %v.vital, detail = %v.detail,
                    "host vitals: cannot read this vital; it starts no pause, and ends none"
                );
            } else if !was_readable && v.readable {
                info!(%worker_id, vital = %v.vital, "host vitals: readable again");
            }
        }
    }

    /// On change only, and again after a failed write.
    async fn write_row(&mut self, status: &VitalsStatus) {
        if self.written.as_ref().is_some_and(|w| w.same_state(status)) {
            return;
        }
        let written = sqlx::query(
            "UPDATE stampeded_workers \
                SET metadata = jsonb_set(metadata, '{vitals}', \
                                         $2::jsonb || jsonb_build_object('since', now())) \
              WHERE id = $1",
        )
        .bind(self.worker_id)
        .bind(status.to_json())
        .execute(self.queue.pool())
        .await;
        match written {
            Ok(_) => self.written = Some(status.clone()),
            Err(e) => {
                warn!(worker_id = %self.worker_id, error = %e, "could not record host vitals");
            }
        }
    }
}
