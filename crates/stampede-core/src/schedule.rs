//! Recurring schedules.
//!
//! The schedule row IS the due row, and its row lock IS the leader election
//! (`plans/11`). Postgres does the claiming; this module does the arithmetic
//! Postgres cannot, namely "given a cron expression, a timezone and a DST
//! policy, when is the next firing?".
//!
//! The whole tick is one transaction, so the locks taken by
//! `stampede_claim_due_schedules` are still held while `Cron` computes. Moving
//! the loop out of PL/pgSQL cost nothing in concurrency terms.

use crate::cron::{Cron, DstPolicy};
use crate::error::Result;
use chrono::{DateTime, Utc};
use jiff::tz::TimeZone;
use jiff::{Timestamp, ToSpan, Zoned};
use tracing::warn;

/// A schedule as stored. Mirrors `stampede_schedules`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Schedule {
    pub key: String,
    pub queue: String,
    pub task: String,
    pub interval_secs: Option<i64>,
    pub cron_expr: Option<String>,
    pub next_run_at: DateTime<Utc>,
    pub dst_policy: String,
    pub timezone: String,
    pub missed_policy: String,
}

impl Schedule {
    fn policy(&self) -> DstPolicy {
        match self.dst_policy.as_str() {
            "wall_clock" => DstPolicy::WallClock,
            _ => DstPolicy::Instant,
        }
    }

    /// When this schedule should fire after the occurrence at `next_run_at`.
    ///
    /// Computed from the PREVIOUS TARGET rather than from `now`: computing from
    /// now accumulates drift on every late run, which is how "every hour"
    /// silently becomes "every hour and four seconds".
    ///
    /// `now` is injected so the catch-up rule is testable without sleeping.
    pub fn next_after(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
        let tz = TimeZone::get(&self.timezone).unwrap_or(TimeZone::UTC);
        let from = to_zoned(self.next_run_at, &tz);
        let now_z = to_zoned(now, &tz);

        let mut next = self.step(&from, &tz)?;

        // A schedule that fell far behind catches up to the present rather than
        // replaying every missed slot. Nobody wants 400 backfilled runs after a
        // long outage, so the overdue occurrence runs ONCE (it already did, just
        // above) and the rest are abandoned.
        if self.missed_policy == "skip" || next.timestamp() <= now_z.timestamp() {
            // Bounded: a 1-second interval three days behind is 259k steps, and
            // an unbounded loop here would hold the row lock for all of them.
            for _ in 0..MAX_CATCHUP_STEPS {
                if next.timestamp() > now_z.timestamp() {
                    break;
                }
                next = self.step(&next, &tz)?;
            }
            if next.timestamp() <= now_z.timestamp() {
                // Further behind than we are willing to walk. Resume from now,
                // losing alignment but not the schedule.
                warn!(key = %self.key, "schedule too far behind to realign; resuming from now");
                next = self.step(&now_z, &tz)?;
            }
        }

        Ok(from_zoned(&next))
    }

    /// One step forward. The only place the two rule kinds differ.
    fn step(&self, from: &Zoned, tz: &TimeZone) -> Result<Zoned> {
        if let Some(expr) = &self.cron_expr {
            let cron = Cron::parse(expr).map_err(|e| {
                crate::Error::Handler(Box::new(SchedulingError(format!(
                    "schedule {:?}: {e}",
                    self.key
                ))))
            })?;
            return cron.next_after(from, tz, self.policy()).ok_or_else(|| {
                crate::Error::Handler(Box::new(SchedulingError(format!(
                    "schedule {:?}: {expr:?} has no occurrence within four years",
                    self.key
                ))))
            });
        }

        let secs = self.interval_secs.unwrap_or(60).max(1);
        from.checked_add(secs.seconds()).map_err(|e| {
            crate::Error::Handler(Box::new(SchedulingError(format!(
                "schedule {:?}: {e}",
                self.key
            ))))
        })
    }
}

/// 24h of one-second ticks. Past this, realigning costs more than it is worth.
const MAX_CATCHUP_STEPS: u32 = 86_400;

/// A schedule that cannot produce a next run: a malformed expression, or one
/// whose occurrences fall outside the search horizon.
#[derive(Debug)]
pub struct SchedulingError(String);

impl SchedulingError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl std::fmt::Display for SchedulingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SchedulingError {}

/// chrono is what sqlx binds (0.9 has no jiff support), jiff is what does the
/// arithmetic. These two functions are the entire boundary between them, and
/// both go through a UTC instant so nothing can be lost in translation.
fn to_zoned(dt: DateTime<Utc>, tz: &TimeZone) -> Zoned {
    Timestamp::from_second(dt.timestamp())
        .unwrap_or(Timestamp::UNIX_EPOCH)
        .to_zoned(tz.clone())
}

fn from_zoned(z: &Zoned) -> DateTime<Utc> {
    DateTime::from_timestamp(z.timestamp().as_second(), 0).unwrap_or_default()
}

#[cfg(test)]
mod tests;
