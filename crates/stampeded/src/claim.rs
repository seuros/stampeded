//! Choosing what to claim across queues, under the planner's verdicts.
//! Shared by the Worker and the Puller; must choose as the Ruby worker does.

use crate::error::Result;
use crate::job::Job;
use crate::planner::Verdict;
use crate::queue::Queue;
use crate::queues::Queues;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};
use uuid::Uuid;

/// How long a planner verdict is trusted; a stale `hold` would stop its queue for good.
pub const VERDICT_TTL: Duration = Duration::from_secs(30);

/// SQLSTATE undefined_table: a schema from before queue policies.
const UNDEFINED_TABLE: &str = "42P01";

/// Jobs this process holds per queue, claimed or running; checked against `max_running`.
#[derive(Debug, Clone, Default)]
pub struct Holdings(Arc<Mutex<HashMap<String, usize>>>);

impl Holdings {
    fn count(&self, queue: &str) -> usize {
        self.0
            .lock()
            .expect("holdings poisoned")
            .get(queue)
            .copied()
            .unwrap_or(0)
    }

    fn hold(&self, queue: &str) -> Held {
        *self
            .0
            .lock()
            .expect("holdings poisoned")
            .entry(queue.to_string())
            .or_default() += 1;
        Held {
            holdings: self.clone(),
            queue: queue.to_string(),
        }
    }
}

/// One claimed job's place in [`Holdings`], given back when dropped.
#[derive(Debug)]
pub struct Held {
    holdings: Holdings,
    queue: String,
}

impl Drop for Held {
    fn drop(&mut self) {
        let mut held = self.holdings.0.lock().expect("holdings poisoned");
        if let Some(n) = held.get_mut(&self.queue) {
            *n = n.saturating_sub(1);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct QueuePlan {
    verdict: Verdict,
    max_running: Option<usize>,
}

pub struct Claimer {
    queue: Queue,
    queues: Queues,
    refresh: Duration,
    rotation: usize,
    serving: Vec<String>,
    serving_read: Option<Instant>,
    plan: HashMap<String, QueuePlan>,
    plan_read: Option<Instant>,
    plan_missing: bool,
    holdings: Holdings,
}

impl Claimer {
    /// `refresh` bounds how often the queue list and the verdicts are re-read.
    pub fn new(queue: Queue, queues: Queues, refresh: Duration) -> Self {
        let serving = match &queues {
            Queues::Named(names) => names.clone(),
            Queues::All { .. } => Vec::new(),
        };
        Self {
            queue,
            queues,
            refresh,
            rotation: 0,
            serving,
            serving_read: None,
            plan: HashMap::new(),
            plan_read: None,
            plan_missing: false,
            holdings: Holdings::default(),
        }
    }

    /// Up to `room` jobs across served queues, rotating the start; rushed first, held skipped.
    /// A failure partway returns what was already claimed rather than strand it.
    pub async fn round(&mut self, worker_id: Uuid, room: usize) -> Result<Vec<(Job, Held)>> {
        if matches!(self.queues, Queues::All { .. }) {
            self.discover().await?;
        }
        if self.serving.is_empty() || room == 0 {
            return Ok(Vec::new());
        }
        self.read_plan().await?;

        let start = self.rotation % self.serving.len();
        self.rotation = start + 1;
        let mut rotated = self.serving.clone();
        rotated.rotate_left(start);

        let verdict = |name: &String| self.plan.get(name).map_or(Verdict::Steady, |p| p.verdict);
        let order: Vec<String> = rotated
            .iter()
            .filter(|n| verdict(n) == Verdict::Rush)
            .chain(rotated.iter().filter(|n| verdict(n) == Verdict::Steady))
            .cloned()
            .collect();

        let mut claimed = Vec::new();
        for name in order {
            let mut limit = room - claimed.len();
            if let Some(max) = self.plan.get(&name).and_then(|p| p.max_running) {
                limit = limit.min(max.saturating_sub(self.holdings.count(&name)));
            }
            if limit == 0 {
                continue;
            }

            let jobs = match self
                .queue
                .claim(&name, worker_id, i32::try_from(limit).unwrap_or(i32::MAX))
                .await
            {
                Ok(jobs) => jobs,
                Err(e) if claimed.is_empty() => return Err(e),
                Err(e) => {
                    warn!(queue = %name, error = %e, "claim failed; keeping this round's jobs");
                    break;
                }
            };
            claimed.extend(jobs.into_iter().map(|job| {
                let held = self.holdings.hold(&job.queue);
                (job, held)
            }));
            if claimed.len() >= room {
                break;
            }
        }
        Ok(claimed)
    }

    fn due(read: Option<Instant>, refresh: Duration) -> bool {
        read.is_none_or(|at| at.elapsed() >= refresh)
    }

    async fn discover(&mut self) -> Result<()> {
        if !Self::due(self.serving_read, self.refresh) {
            return Ok(());
        }
        self.serving_read = Some(Instant::now());

        let found: Vec<String> = self
            .queue
            .queues_with_work()
            .await?
            .into_iter()
            .filter(|q| self.queues.serves(q))
            .collect();
        if found != self.serving && !found.is_empty() {
            info!(queues = %found.join(", "), "serving");
        }
        self.serving = found;
        Ok(())
    }

    async fn read_plan(&mut self) -> Result<()> {
        if self.plan_missing || !Self::due(self.plan_read, self.refresh) {
            return Ok(());
        }
        self.plan_read = Some(Instant::now());

        match self.queue.queue_plan(&self.serving).await {
            Ok(rows) => {
                self.plan = rows
                    .into_iter()
                    .map(|(name, max_running, verdict)| {
                        let plan = QueuePlan {
                            verdict: Verdict::parse(&verdict),
                            max_running: max_running.and_then(|m| usize::try_from(m).ok()),
                        };
                        (name, plan)
                    })
                    .collect();
                Ok(())
            }
            Err(crate::Error::Database(e))
                if e.as_database_error()
                    .and_then(sqlx::error::DatabaseError::code)
                    .as_deref()
                    == Some(UNDEFINED_TABLE) =>
            {
                warn!("stampeded_queue_policies does not exist; claiming without queue policies");
                self.plan_missing = true;
                self.plan.clear();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

impl Queue {
    /// Every queue with claimable work, from the trigger-kept counter table, not a jobs scan.
    pub async fn queues_with_work(&self) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT queue FROM stampeded_queue_counts \
              WHERE state IN ('available', 'retryable') AND n > 0 \
              ORDER BY queue",
        )
        .fetch_all(self.pool())
        .await?)
    }

    /// Each queue's policy cap and verdict; `steady` once older than [`VERDICT_TTL`].
    pub async fn queue_plan(
        &self,
        queues: &[String],
    ) -> Result<Vec<(String, Option<i32>, String)>> {
        let rows = sqlx::query_as(
            "SELECT queue, max_running, \
                    CASE WHEN decided_at > now() - make_interval(secs => $2) \
                         THEN verdict ELSE 'steady' END \
               FROM stampeded_queue_policies \
              WHERE queue = ANY($1)",
        )
        .bind(queues)
        .bind(VERDICT_TTL.as_secs_f64())
        .fetch_all(self.pool())
        .await?;
        Ok(rows)
    }
}

#[cfg(test)]
mod tests;
