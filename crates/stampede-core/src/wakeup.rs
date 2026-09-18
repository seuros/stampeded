//! LISTEN/NOTIFY wakeup.
//!
//! The database notifies, not the client. That is what lets enqueue stay a
//! plain `INSERT` from any language (plans/05) while still waking a worker in
//! milliseconds: a Ruby script running `INSERT INTO stampede_jobs …` does not
//! run our code, so any design where the ENQUEUER sends the notification is
//! unavailable to us. Four of the surveyed projects moved NOTIFY into client
//! code; all four ship a client library that is the only supported way to
//! enqueue. We are structurally in the other group.
//!
//! **Polling is the correctness mechanism; this is a latency accelerant.** A
//! redundant wakeup costs one empty query (~0.02 ms measured); a missed one
//! costs a stuck job. The whole thing can be switched off with one `DROP
//! TRIGGER` and the queue keeps working, just less promptly — that is a
//! supported configuration, not breakage.
//!
//! Three things had to be right for this to be safe:
//!
//! 1. **One listener per daemon, fanned out in process.** Never one per worker.
//!    With `max_connections = 100`, per-worker LISTEN is not affordable.
//! 2. **The payload is a routing key** — the bare queue name. Postgres collapses
//!    identical `(channel, payload)` pairs within a transaction, so a 50k-row
//!    insert emits one notification per distinct queue. That makes it
//!    self-throttling for free.
//! 3. **Isolation is detected, not configured.** PgBouncer in transaction mode
//!    silently eats LISTEN, and the failure mode is a queue that looks fine and
//!    runs a second late forever. The sonar probe finds that rather than
//!    requiring the operator to already know.

use crate::error::Result;
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tracing::{debug, info, warn};

/// How many queue names may back up before a slow subscriber starts missing
/// them. Missing one is harmless — the poll floor catches the work — so this is
/// small on purpose rather than an unbounded buffer that could grow.
const WAKEUP_BUFFER: usize = 256;

/// How long the sonar waits for its own notification to come back.
///
/// Generous: this is diagnosing a broken path, not measuring a healthy one, and
/// a false "notifications are broken" would turn off an optimisation that works.
const SONAR_TIMEOUT: Duration = Duration::from_secs(5);

/// Channel suffixes. Names are global per database, so both are prefixed with
/// the schema — see the trigger in 001_initial.sql.
const INSERT_CHANNEL: &str = "stampede_insert";
const SONAR_CHANNEL: &str = "stampede_sonar";

/// A daemon's single listener connection, fanned out to every worker.
#[derive(Clone)]
pub struct Wakeup {
    tx: broadcast::Sender<String>,
    healthy: Arc<AtomicBool>,
}

impl Wakeup {
    /// Start listening. The returned handle is cheap to clone.
    ///
    /// Takes its own connection rather than one from the pool: a LISTEN
    /// connection is pinned for the lifetime of the daemon, and returning it to
    /// a pool would either leak the subscription onto an unrelated query or
    /// drop it silently. River's `MaxConns 1` hijack is the same shape.
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

        // 63 bytes is the hard cap on a channel name. Worth saying out loud
        // rather than letting Postgres truncate: a truncated channel still
        // works, it just silently stops matching the one the trigger emits on.
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
        listener.listen_all([insert_channel.as_str(), sonar_channel.as_str()]).await?;

        let (tx, _) = broadcast::channel(WAKEUP_BUFFER);
        let healthy = Arc::new(AtomicBool::new(false));

        let handle = Self { tx: tx.clone(), healthy: healthy.clone() };

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
                            // Subscribers may be zero (a maintenance-only
                            // daemon), which is not an error.
                            let _ = tx.send(n.payload().to_string());
                        }
                        Err(e) => {
                            // The connection dropped. PgListener reconnects and
                            // re-LISTENs on its own; the poll floor covers the
                            // gap, which is exactly why the floor exists.
                            warn!(error = %e, "listener error; polling covers the gap");
                            healthy.store(false, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    },
                }
            }
            debug!("listener stopped");
        });

        // Probe now. Oban's Sonar is the idea; pg-boss's pg_listening_channels
        // probe is the implementation. This combines them: notify ourselves and
        // see whether it comes back, which tests the WHOLE path rather than
        // asking the server whether it thinks it is listening.
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

        // Logged ONCE, at startup, with the likely cause named. The alternative
        // is a `poll_only` flag, which River offers and which requires the
        // operator to already know their pooler eats LISTEN — if they knew
        // that, they would not need the flag.
        warn!(
            "no notification came back within {SONAR_TIMEOUT:?}; falling back to polling only. \
             A connection pooler in transaction mode (PgBouncer) is the usual cause. \
             The queue is CORRECT either way — jobs start within one poll interval instead \
             of immediately."
        );
        Ok(handle)
    }

    /// A wakeup path that never fires. For daemons with notifications disabled,
    /// and for tests.
    pub fn disabled() -> Self {
        Self {
            tx: broadcast::channel(1).0,
            healthy: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Whether a notification has been seen coming back. Surfaced on the
    /// dashboard: "polling only" is worth knowing before someone spends an
    /// afternoon wondering why latency is a second.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// Wakeups for one queue.
    pub fn subscribe(&self, queue: impl Into<String>) -> WakeupRx {
        WakeupRx { queue: queue.into(), rx: self.tx.subscribe() }
    }
}

/// One worker's view of the wakeup stream, filtered to its own queue.
pub struct WakeupRx {
    queue: String,
    rx: broadcast::Receiver<String>,
}

impl WakeupRx {
    /// Wait until this queue is notified.
    ///
    /// Lag is deliberately not an error. `RecvError::Lagged` means notifications
    /// arrived faster than this worker drained them — which only happens when
    /// there is plenty of work, the case where an extra wakeup is worth nothing
    /// anyway. Treating it as a wakeup is both simpler and more useful than
    /// propagating it.
    pub async fn recv(&mut self) {
        loop {
            match self.rx.recv().await {
                Ok(q) if q == self.queue => return,
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    debug!(queue = %self.queue, skipped = n, "wakeups lagged");
                    return;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    // The daemon is shutting down. Park forever and let the
                    // caller's shutdown branch win the select.
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}

/// How long to wait before the next claim attempt.
///
/// Two mechanisms that are often confused and are not substitutes (plans/12):
/// the GCRA gate bounds how OFTEN we may fetch, and this decides when we WANT
/// to. solid_queue's poller returns its own next sleep, which is the shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextPoll {
    /// The last claim filled the batch, so there is probably more. Go again
    /// without sleeping — this is what keeps a backlog draining at full speed
    /// instead of one batch per poll interval.
    Immediate,
    /// Nothing was there. Wait a poll interval, or until a notification.
    Idle,
    /// Every execution slot is busy. Sleeping long here is safe *because* a
    /// finishing job or a notification interrupts it; without that it would be
    /// a latency bug.
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
            // Bounded rather than indefinite: a wakeup or a finishing job will
            // normally interrupt this, but if both are somehow missed the
            // worker must still recover on its own.
            Self::Saturated => poll_interval * 5,
        }
    }
}

#[cfg(test)]
mod tests;
