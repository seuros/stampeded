//! Per-tenant admission control (GCRA), so one noisy tenant cannot starve the rest.

use std::sync::LazyLock;
use std::time::{Duration, Instant};
use throttle_machines::{GcraPolicy, KeyedGcra};

/// How many jobs per second a single tenant may start, plus tolerated burst.
#[derive(Debug, Clone, Copy)]
pub struct TenantLimit {
    pub jobs_per_second: f64,
    /// Extra jobs a tenant may start back-to-back; zero means strictly paced.
    pub burst: f64,
}

impl Default for TenantLimit {
    fn default() -> Self {
        // Generous: fairness should only bite under genuine contention.
        Self {
            jobs_per_second: 100.0,
            burst: 50.0,
        }
    }
}

impl TenantLimit {
    /// The GCRA policy this limit maps onto.
    ///
    /// A rate that is not a positive number, or too small for a `Duration` to pace,
    /// admits nothing: a misconfigured limit must not admit everything. A rate past
    /// one job per nanosecond paces at one nanosecond. A negative burst is no burst,
    /// and one too large for the policy to count is capped at what it can.
    fn policy(&self) -> GcraPolicy {
        const NEVER: GcraPolicy = match GcraPolicy::new(Duration::MAX, Duration::ZERO) {
            Some(policy) => policy,
            None => panic!("a nonzero interval with no slack is a valid GCRA policy"),
        };
        let Ok(interval) = Duration::try_from_secs_f64(self.jobs_per_second.recip()) else {
            return NEVER;
        };
        let interval = interval.max(Duration::from_nanos(1));
        let slack = Duration::try_from_secs_f64(interval.as_secs_f64() * self.burst.max(0.0))
            .unwrap_or(Duration::MAX)
            .min(Duration::MAX.saturating_sub(interval));
        GcraPolicy::new(interval, slack)
            .or_else(|| GcraPolicy::new(interval, Duration::ZERO))
            .unwrap_or(NEVER)
    }
}

/// Admission control across tenants, with bounded state per limiter.
///
/// A tenant not yet tracked is throttled if its shard has no room.
#[derive(Debug)]
pub struct Fairness {
    limiter: KeyedGcra<String>,
}

impl Fairness {
    pub fn new(limit: TenantLimit) -> Self {
        Self {
            limiter: KeyedGcra::new(limit.policy()),
        }
    }

    /// Whether this tenant may start a job now; jobs with no tenant always may.
    pub fn admit(&self, tenant: Option<&str>) -> Admission {
        let Some(tenant) = tenant else {
            return Admission::Allowed;
        };
        self.admit_at(tenant, now())
    }

    /// Injectable clock, for tests: `now` is monotonic time since an epoch of the
    /// caller's choosing.
    pub fn admit_at(&self, tenant: &str, now: Duration) -> Admission {
        match self.limiter.check_with_retry_after(tenant, now) {
            Ok(_) => Admission::Allowed,
            Err((_, retry_after)) => Admission::Throttled { retry_after },
        }
    }

    /// Drop state for tenants that have gone quiet.
    pub fn evict_idle(&self, now: Duration) {
        self.limiter.evict(now);
    }

    /// Keys currently tracked; automatic eviction can remove recovered tenants.
    pub fn tracked_tenants(&self) -> usize {
        self.limiter.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Allowed,
    /// Retry hint, not a guarantee of admission; `Duration::MAX` when never.
    Throttled {
        retry_after: Duration,
    },
}

impl Admission {
    pub fn is_allowed(self) -> bool {
        matches!(self, Self::Allowed)
    }
}

/// Time on a monotonic clock, for the in-process GCRA gates. Not the wall clock:
/// NTP stepping it back would make every tenant, and the claim pacer, wait out the step.
pub(crate) fn now() -> Duration {
    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    START.elapsed()
}

#[cfg(test)]
mod tests;
