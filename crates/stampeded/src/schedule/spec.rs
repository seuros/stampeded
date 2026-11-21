//! Declared schedules, synced per owner by `stampeded_sync_schedules`.
//!
//! The ownership rules live in SQL so every binding applies the same ones;
//! this module validates a declaration and seeds each cron rule's first run,
//! which PL/pgSQL cannot compute.

use crate::cron::{Cron, DstPolicy};
use crate::error::{Error, Result};
use crate::queue::db_now;
use jiff::Timestamp;
use jiff::tz::TimeZone;
use serde::Serialize;
use serde_json::{Map, Value};
use sqlx::PgPool;
use std::time::Duration;

/// The SQLSTATE `stampeded_sync_schedules` raises for a key held by another owner.
const FOREIGN_OWNER: &str = "SD409";
/// `invalid_parameter_value`: a malformed declaration, refused by the function.
const INVALID_PARAMETER: &str = "22023";

#[derive(Debug, Clone)]
enum Rule {
    Cron {
        expr: String,
        policy: Option<DstPolicy>,
    },
    Every(Duration),
}

/// One schedule as its owner declares it, for [`crate::Queue::sync_schedules`].
///
/// Job limits left unset keep what the stored row holds (the column default
/// for a new row), so a limit tuned in SQL survives the next deploy.
#[derive(Debug, Clone)]
#[must_use]
pub struct ScheduleSpec {
    key: String,
    queue: String,
    task: String,
    rule: Rule,
    timezone: String,
    args: Value,
    meta: Value,
    tenant_id: Option<String>,
    priority: i16,
    max_attempts: Option<i16>,
    max_interruptions: Option<i16>,
    expire_seconds: Option<i32>,
}

impl ScheduleSpec {
    fn new(
        key: impl Into<String>,
        queue: impl Into<String>,
        task: impl Into<String>,
        rule: Rule,
    ) -> Self {
        Self {
            key: key.into(),
            queue: queue.into(),
            task: task.into(),
            rule,
            timezone: "UTC".into(),
            args: Value::Object(Map::new()),
            meta: Value::Object(Map::new()),
            tenant_id: None,
            priority: 0,
            max_attempts: None,
            max_interruptions: None,
            expire_seconds: None,
        }
    }

    /// A cron schedule, read in UTC unless [`Self::timezone`] says otherwise.
    pub fn cron(
        key: impl Into<String>,
        queue: impl Into<String>,
        task: impl Into<String>,
        expr: impl Into<String>,
    ) -> Self {
        Self::new(
            key,
            queue,
            task,
            Rule::Cron {
                expr: expr.into(),
                policy: None,
            },
        )
    }

    /// A fixed-interval schedule; a new one first fires at once.
    pub fn every(
        key: impl Into<String>,
        queue: impl Into<String>,
        task: impl Into<String>,
        every: Duration,
    ) -> Self {
        Self::new(key, queue, task, Rule::Every(every))
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    /// The zone cron fields are read in; an interval ignores it.
    pub fn timezone(mut self, timezone: impl Into<String>) -> Self {
        self.timezone = timezone.into();
        self
    }

    /// Unset, the expression decides: [`Cron::suggested_dst_policy`]. An interval ignores it.
    pub fn dst_policy(mut self, policy: DstPolicy) -> Self {
        if let Rule::Cron { policy: p, .. } = &mut self.rule {
            *p = Some(policy);
        }
        self
    }

    /// Each occurrence's named arguments; must be a JSON object. `null` is `{}`.
    pub fn args(mut self, args: Value) -> Self {
        self.args = object_or_empty(args);
        self
    }

    /// Each occurrence's runtime context. `null` is `{}`.
    pub fn meta(mut self, meta: Value) -> Self {
        self.meta = object_or_empty(meta);
        self
    }

    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }

    pub fn priority(mut self, priority: i16) -> Self {
        self.priority = priority;
        self
    }

    /// Attempts per occurrence, the first claim included; at least 1.
    pub fn max_attempts(mut self, n: i16) -> Self {
        self.max_attempts = Some(n);
        self
    }

    /// Worker deaths tolerated per occurrence; 0 makes the first terminal.
    pub fn max_interruptions(mut self, n: i16) -> Self {
        self.max_interruptions = Some(n);
        self
    }

    /// Executing-job inactivity before expiry; saturates at `i32::MAX` seconds.
    pub fn expires_in(mut self, d: Duration) -> Self {
        self.expire_seconds = Some(i32::try_from(d.as_secs().max(1)).unwrap_or(i32::MAX));
        self
    }

    /// The function's row for this schedule, its first run seeded against `now`.
    fn wire(&self, now: Timestamp) -> Result<Wire<'_>> {
        let key = &self.key;
        if key.trim().is_empty() {
            return Err(Error::InvalidSchedule(
                "a schedule needs a key: it is the primary key and how you pause or remove it"
                    .into(),
            ));
        }
        for (field, value) in [("args", &self.args), ("meta", &self.meta)] {
            if !value.is_object() {
                return Err(Error::InvalidSchedule(format!(
                    "schedule {key:?}: {field} must be a JSON object"
                )));
            }
        }
        let mut wire = Wire {
            key,
            queue: &self.queue,
            task: &self.task,
            args: &self.args,
            meta: &self.meta,
            tenant_id: self.tenant_id.as_deref(),
            priority: self.priority,
            cron_expr: None,
            interval_secs: None,
            timezone: &self.timezone,
            dst_policy: DstPolicy::Instant.as_str(),
            max_attempts: self.max_attempts,
            max_interruptions: self.max_interruptions,
            expire_seconds: self.expire_seconds,
            next_run_at: None,
        };
        match &self.rule {
            Rule::Every(every) => {
                if every.as_secs() == 0 {
                    return Err(Error::InvalidSchedule(format!(
                        "schedule {key:?}: the interval must be at least one second"
                    )));
                }
                wire.interval_secs = Some(i64::try_from(every.as_secs()).map_err(|_| {
                    Error::InvalidSchedule(format!("schedule {key:?}: the interval is too large"))
                })?);
            }
            Rule::Cron { expr, policy } => {
                let cron = Cron::parse(expr)
                    .map_err(|e| Error::InvalidSchedule(format!("schedule {key:?}: {e}")))?;
                let tz = TimeZone::get(&self.timezone).map_err(|e| {
                    Error::InvalidSchedule(format!(
                        "schedule {key:?}: unknown timezone {:?}: {e}",
                        self.timezone
                    ))
                })?;
                let policy = policy.unwrap_or_else(|| cron.suggested_dst_policy());
                let first = cron
                    .next_after(&now.to_zoned_datetime(tz.clone()), &tz, policy)
                    .ok_or_else(|| {
                        Error::InvalidSchedule(format!(
                            "schedule {key:?}: {expr:?} has no occurrence within four years"
                        ))
                    })?;
                wire.cron_expr = Some(expr);
                wire.dst_policy = policy.as_str();
                wire.next_run_at = Some(first.timestamp());
            }
        }
        Ok(wire)
    }
}

fn object_or_empty(value: Value) -> Value {
    if value.is_null() {
        Value::Object(Map::new())
    } else {
        value
    }
}

/// One element of `stampeded_sync_schedules`'s array, named as its columns.
#[derive(Serialize)]
struct Wire<'a> {
    key: &'a str,
    queue: &'a str,
    task: &'a str,
    args: &'a Value,
    meta: &'a Value,
    tenant_id: Option<&'a str>,
    priority: i16,
    cron_expr: Option<&'a str>,
    interval_secs: Option<i64>,
    timezone: &'a str,
    dst_policy: &'static str,
    max_attempts: Option<i16>,
    max_interruptions: Option<i16>,
    expire_seconds: Option<i32>,
    next_run_at: Option<Timestamp>,
}

/// What a sync did to one schedule. Unchanged schedules are not reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SyncChange {
    Created,
    Updated,
    /// An unowned row (written before owners, or by hand) now belongs to the owner.
    Adopted,
    /// The owner's row was no longer declared.
    Removed,
}

impl SyncChange {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "created" => Some(Self::Created),
            "updated" => Some(Self::Updated),
            "adopted" => Some(Self::Adopted),
            "removed" => Some(Self::Removed),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Updated => "updated",
            Self::Adopted => "adopted",
            Self::Removed => "removed",
        }
    }
}

/// One schedule a sync changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleChange {
    pub key: String,
    pub change: SyncChange,
}

pub async fn sync(
    pool: &PgPool,
    owner: &str,
    schedules: &[ScheduleSpec],
) -> Result<Vec<ScheduleChange>> {
    if owner.trim().is_empty() {
        return Err(Error::InvalidSchedule(
            "a schedule sync needs an owner: it is what keeps other programs' schedules apart"
                .into(),
        ));
    }
    // The database's clock, which the scheduler compares the seed against.
    let now = db_now(pool).await?;
    let rows = schedules
        .iter()
        .map(|s| s.wire(now))
        .collect::<Result<Vec<_>>>()?;

    let changed: Vec<(String, String)> =
        sqlx::query_as("SELECT schedule_key, change FROM stampeded_sync_schedules($1, $2)")
            .bind(owner)
            .bind(sqlx::types::Json(&rows))
            .fetch_all(pool)
            .await
            .map_err(refused)?;

    changed
        .into_iter()
        .map(|(key, change)| {
            let change = SyncChange::parse(&change).ok_or_else(|| {
                Error::Schema(format!("stampeded_sync_schedules reported {change:?}"))
            })?;
            Ok(ScheduleChange { key, change })
        })
        .collect()
}

/// The function's own refusals are a bad declaration, not a database fault.
fn refused(e: sqlx::Error) -> Error {
    if let Some(db) = e.as_database_error()
        && matches!(
            db.code().as_deref(),
            Some(FOREIGN_OWNER | INVALID_PARAMETER)
        )
    {
        return Error::InvalidSchedule(db.message().to_owned());
    }
    Error::Database(e)
}
