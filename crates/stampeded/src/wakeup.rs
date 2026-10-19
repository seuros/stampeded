//! LISTEN/NOTIFY wakeup.
//!
//! A trigger notifies, so a plain `INSERT` from any language wakes a worker.
//! Polling stays the correctness mechanism; this only cuts latency.

use crate::error::Result;
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tracing::{debug, info, warn};

/// Queue names that may back up before a slow subscriber misses some; the poll floor covers it.
const WAKEUP_BUFFER: usize = 256;

/// How long the sonar waits for its own notification to come back.
const SONAR_TIMEOUT: Duration = Duration::from_secs(5);

/// Channel suffixes, prefixed with the schema because names are global per database.
const INSERT_CHANNEL: &str = "stampeded_insert";
const SONAR_CHANNEL: &str = "stampeded_sonar";

/// A daemon's single listener connection, fanned out to every worker.
#[derive(Clone)]
pub struct Wakeup {
    tx: broadcast::Sender<String>,
    healthy: Arc<AtomicBool>,
}

impl Wakeup {
    /// Start listening on a dedicated connection, outside the pool.
    pub async fn start(
        pool: &PgPool,
        database_url: &str,
        shutdown: watch::Receiver<bool>,
    ) -> Result<Self> {
        let schema: String = sqlx::query_scalar("SELECT current_schema()")
            .fetch_one(pool)
            .await?;

        let insert_channel = format!("{schema}.{INSERT_CHANNEL}");
        let sonar_channel = format!("{schema}.{SONAR_CHANNEL}");

        // Postgres truncates channel names past 63 bytes, which would stop matching the trigger.
        if insert_channel.len() > 63 {
            warn!(
                schema = %schema,
                "schema name too long for a notify channel; wakeups will be poll-only"
            );
            return Ok(Self {
                tx: broadcast::channel(1).0,
                healthy: Arc::new(AtomicBool::new(false)),
            });
        }

        let mut listener = PgListener::connect(database_url).await?;
        listener
            .listen_all([insert_channel.as_str(), sonar_channel.as_str()])
            .await?;

        let (tx, _) = broadcast::channel(WAKEUP_BUFFER);
        let healthy = Arc::new(AtomicBool::new(false));

        let handle = Self {
            tx: tx.clone(),
            healthy: healthy.clone(),
        };

        let sonar_probe = sonar_channel.clone();
        let probe_pool = pool.clone();
        let mut sd = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = sd.changed() => break,
                    notification = listener.recv() => match notification {
                        Ok(n) if n.channel() == sonar_probe => {
                            if !healthy.swap(true, Ordering::Relaxed) {
                                info!("notification path confirmed; wakeups are live");
                            }
                        }
                        Ok(n) => {
                            // Zero subscribers (a maintenance-only daemon) is not an error.
                            let _ = tx.send(n.payload().to_string());
                        }
                        Err(e) => {
                            // PgListener reconnects and re-LISTENs on its own.
                            warn!(error = %e, "listener error; polling covers the gap");
                            healthy.store(false, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    },
                }
            }
            debug!("listener stopped");
        });

        // Notify ourselves: tests the whole path, poolers included.
        let probe = sqlx::query("SELECT pg_notify($1, 'probe')")
            .bind(&sonar_channel)
            .execute(&probe_pool)
            .await;
        if let Err(e) = probe {
            warn!(error = %e, "sonar probe could not be sent");
        }

        let deadline = tokio::time::Instant::now() + SONAR_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            if handle.is_healthy() {
                return Ok(handle);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        warn!(
            "no notification came back within {SONAR_TIMEOUT:?}; falling back to polling only. \
             A connection pooler in transaction mode (PgBouncer) is the usual cause. \
             The queue is CORRECT either way -- jobs start within one poll interval instead \
             of immediately."
        );
        Ok(handle)
    }

    /// A wakeup path that never fires.
    pub fn disabled() -> Self {
        Self {
            tx: broadcast::channel(1).0,
            healthy: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Whether a notification has been seen coming back.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// Wakeups for one queue.
    pub fn subscribe(&self, queue: impl Into<String>) -> WakeupRx {
        self.subscribe_to(&crate::Queues::one(queue))
    }

    /// Woken by any queue the selection serves.
    pub fn subscribe_to(&self, queues: &crate::Queues) -> WakeupRx {
        WakeupRx {
            queues: queues.clone(),
            rx: self.tx.subscribe(),
        }
    }
}

/// One worker's view of the wakeup stream, filtered to the queues it serves.
pub struct WakeupRx {
    queues: crate::Queues,
    rx: broadcast::Receiver<String>,
}

impl WakeupRx {
    /// Wait until this queue is notified. Lag counts as a wakeup.
    pub async fn recv(&mut self) {
        loop {
            match self.rx.recv().await {
                Ok(q) if self.queues.serves(&q) => return,
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    debug!(queues = %self.queues.label(), skipped = n, "wakeups lagged");
                    return;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    // Shutting down: park so the caller's shutdown branch wins the select.
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}

/// How long to wait before the next claim attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextPoll {
    /// The last claim filled the batch, so go again without sleeping.
    Immediate,
    /// Nothing was there. Wait a poll interval, or until a notification.
    Idle,
    /// Every slot is busy; a finishing job or a notification cuts the sleep short.
    Saturated,
}

impl NextPoll {
    /// Decide from the last claim's outcome.
    pub fn after_claim(claimed: usize, wanted: usize, free_slots: usize) -> Self {
        if free_slots == 0 {
            Self::Saturated
        } else if claimed > 0 && claimed >= wanted {
            Self::Immediate
        } else {
            Self::Idle
        }
    }

    pub fn delay(self, poll_interval: Duration) -> Duration {
        match self {
            Self::Immediate => Duration::ZERO,
            Self::Idle => poll_interval,
            // Bounded, so a missed wakeup still recovers.
            Self::Saturated => poll_interval * 5,
        }
    }
}

#[cfg(test)]
mod tests;
