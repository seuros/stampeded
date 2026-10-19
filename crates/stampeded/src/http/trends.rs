//! Per-queue throughput, depth and backlog age, sampled in memory for the dashboard.
//!
//! Never an aggregate over finished job rows: counters and oldest ages only.

use crate::Queue;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::warn;

/// How often the sampler reads.
pub const SAMPLE_EVERY: Duration = Duration::from_secs(5);

/// How much history is kept and drawn.
pub const WINDOW: Duration = Duration::from_mins(10);

/// One queue at one moment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub at: Instant,
    /// Jobs completed per second since the previous sample; `None` on the first.
    pub rate: Option<f64>,
    /// Available plus retryable.
    pub waiting: i64,
    /// Age of the oldest waiting job, when any are waiting.
    pub oldest_secs: Option<f64>,
}

/// What one sample read for one queue.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Reading {
    pub completed: i64,
    pub waiting: i64,
    pub oldest_secs: Option<f64>,
}

#[derive(Default)]
struct Series {
    points: VecDeque<Point>,
    /// The completed count and when it was read, for the next rate.
    last: Option<(i64, Instant)>,
}

impl Series {
    fn last_rate(&self) -> Option<f64> {
        self.points.back().and_then(|p| p.rate)
    }
}

/// Per-queue history, shared by every dashboard viewer.
#[derive(Default)]
pub struct Trends {
    series: Mutex<BTreeMap<String, Series>>,
}

impl Trends {
    /// Fold one sample in. No I/O, so it is tested without a database.
    pub fn record(&self, now: Instant, readings: &BTreeMap<String, Reading>) {
        let mut series = self.lock();
        for (queue, reading) in readings {
            let s = series.entry(queue.clone()).or_default();
            let rate = match s.last {
                Some((prev, at)) if reading.completed >= prev => {
                    let secs = now.duration_since(at).as_secs_f64();
                    if secs > 0.0 {
                        Some((reading.completed - prev) as f64 / secs)
                    } else {
                        s.last_rate()
                    }
                }
                // A falling count means the pruner ran: carry the last rate.
                Some(_) => s.last_rate(),
                None => None,
            };
            s.last = Some((reading.completed, now));
            s.points.push_back(Point {
                at: now,
                rate,
                waiting: reading.waiting,
                oldest_secs: reading.oldest_secs,
            });
        }

        // Queues missing from this reading still age out, and go once empty.
        for s in series.values_mut() {
            while s
                .points
                .front()
                .is_some_and(|p| now.duration_since(p.at) > WINDOW)
            {
                s.points.pop_front();
            }
        }
        series.retain(|_, s| !s.points.is_empty());
    }

    /// Every queue's points, oldest first.
    pub fn snapshot(&self) -> Vec<(String, Vec<Point>)> {
        self.lock()
            .iter()
            .map(|(queue, s)| (queue.clone(), s.points.iter().copied().collect()))
            .collect()
    }

    /// Poisoning is recoverable: each sample is folded in whole under the lock.
    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, Series>> {
        self.series.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Sample until `shutdown` changes.
pub async fn run(queue: Arc<Queue>, trends: Arc<Trends>, mut shutdown: watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(SAMPLE_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            // Err means the sender is gone, which is a shutdown too.
            _ = shutdown.changed() => return,
        }
        match read(&queue).await {
            Ok(readings) => trends.record(Instant::now(), &readings),
            Err(e) => warn!(error = %e, "trend sample failed; the next one will retry"),
        }
    }
}

/// One sample: counts from the counter table, ages from the claim index.
async fn read(queue: &Queue) -> crate::Result<BTreeMap<String, Reading>> {
    let rows: Vec<(String, String, i64)> =
        sqlx::query_as("SELECT queue, state, n FROM stampeded_queue_counts WHERE n <> 0")
            .fetch_all(queue.pool())
            .await?;

    let mut readings: BTreeMap<String, Reading> = BTreeMap::new();
    for (q, state, n) in rows {
        let r = readings.entry(q).or_default();
        match state.as_str() {
            "completed" => r.completed = n,
            "available" | "retryable" => r.waiting += n,
            _ => {}
        }
    }
    for (q, age) in queue.oldest_available_age().await? {
        readings.entry(q).or_default().oldest_secs = Some(age);
    }
    Ok(readings)
}

#[cfg(test)]
mod tests;
