//! Recurring schedules: Postgres claims due rows, this module computes the next firing.
//!
//! The tick is one transaction, so the claimed row locks are held while `Cron` computes.

use crate::cron::{Cron, DstPolicy};
use crate::error::Result;
use jiff::tz::TimeZone;
use jiff::{Timestamp, ToSpan, Zoned};
use tracing::warn;

/// A schedule as stored. Mirrors `stampeded_schedules`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Schedule {
    pub key: String,
    pub queue: String,
    pub task: String,
    pub interval_secs: Option<i64>,
    pub cron_expr: Option<String>,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub next_run_at: Timestamp,
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

    /// When this schedule should fire after `next_run_at`; stepping from the target avoids drift.
    pub fn next_after(&self, now: Timestamp) -> Result<Timestamp> {
        let tz = TimeZone::get(&self.timezone).unwrap_or(TimeZone::UTC);
        let from = self.next_run_at.to_zoned(tz.clone());
        let now_z = now.to_zoned(tz.clone());

        let mut next = self.step(&from, &tz)?;

        // Far behind: the overdue occurrence ran once; the missed slots are not replayed.
        if self.missed_policy == "skip" || next.timestamp() <= now_z.timestamp() {
            // Bounded, since the row lock is held for every step.
            for _ in 0..MAX_CATCHUP_STEPS {
                if next.timestamp() > now_z.timestamp() {
                    break;
                }
                next = self.step(&next, &tz)?;
            }
            if next.timestamp() <= now_z.timestamp() {
                // Too far to walk: resume from now, losing alignment but not the schedule.
                warn!(key = %self.key, "schedule too far behind to realign; resuming from now");
                next = self.step(&now_z, &tz)?;
            }
        }

        Ok(next.timestamp())
    }

    /// The first firing after `now`, for a row written without a cron parser.
    pub fn first_after(&self, now: Timestamp) -> Result<Timestamp> {
        let tz = TimeZone::get(&self.timezone).unwrap_or(TimeZone::UTC);
        Ok(self.step(&now.to_zoned(tz.clone()), &tz)?.timestamp())
    }

    /// One step forward. The only place the two rule kinds differ.
    fn step(&self, from: &Zoned, tz: &TimeZone) -> Result<Zoned> {
        if let Some(expr) = &self.cron_expr {
            let cron = Cron::parse(expr).map_err(|e| {
                crate::Error::InvalidSchedule(format!("schedule {:?}: {e}", self.key))
            })?;
            return cron.next_after(from, tz, self.policy()).ok_or_else(|| {
                crate::Error::InvalidSchedule(format!(
                    "schedule {:?}: {expr:?} has no occurrence within four years",
                    self.key
                ))
            });
        }

        let secs = self.interval_secs.unwrap_or(60).max(1);
        from.checked_add(secs.seconds())
            .map_err(|e| crate::Error::InvalidSchedule(format!("schedule {:?}: {e}", self.key)))
    }
}

/// 24h of one-second ticks. Past this, realigning costs more than it is worth.
const MAX_CATCHUP_STEPS: u32 = 86_400;

#[cfg(test)]
mod tests;
