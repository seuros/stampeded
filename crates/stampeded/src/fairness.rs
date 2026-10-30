//! Per-tenant admission control (GCRA), so one noisy tenant cannot starve the rest.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Instant;
use throttle_machines::gate::Gate;
use throttle_machines::gcra::{Gcra, GcraParams};

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
    fn params(&self) -> GcraParams {
        let interval = if self.jobs_per_second > 0.0 {
            1.0 / self.jobs_per_second
        } else {
            f64::MAX
        };
        GcraParams {
            emission_interval: interval,
            delay_tolerance: interval * self.burst,
        }
    }
}

/// Admission control across tenants; holds each tenant's GCRA state.
#[derive(Debug)]
pub struct Fairness {
    limit: TenantLimit,
    /// tenant -> theoretical arrival time
    state: Mutex<HashMap<String, f64>>,
}

impl Fairness {
    pub fn new(limit: TenantLimit) -> Self {
        Self {
            limit,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Whether this tenant may start a job now; jobs with no tenant always may.
    pub fn admit(&self, tenant: Option<&str>) -> Admission {
        let Some(tenant) = tenant else {
            return Admission::Allowed;
        };
        self.admit_at(tenant, now_secs())
    }

    /// Injectable clock, for tests.
    pub fn admit_at(&self, tenant: &str, now: f64) -> Admission {
        let decision = {
            let mut state = self.state.lock().expect("fairness state poisoned");
            let tat = state.get(tenant).copied().unwrap_or(0.0);
            let decision = Gcra::check(tat, now, self.limit.params());
            state.insert(tenant.to_string(), decision.state);
            decision
        };

        if decision.allowed {
            Admission::Allowed
        } else {
            Admission::Throttled {
                retry_after_secs: decision.retry_after,
            }
        }
    }

    /// Drop state for tenants that have gone quiet.
    pub fn evict_idle(&self, now: f64) {
        let mut state = self.state.lock().expect("fairness state poisoned");
        state.retain(|_, tat| *tat > now);
    }

    pub fn tracked_tenants(&self) -> usize {
        self.state.lock().expect("fairness state poisoned").len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Admission {
    Allowed,
    Throttled { retry_after_secs: f64 },
}

impl Admission {
    pub fn is_allowed(self) -> bool {
        matches!(self, Self::Allowed)
    }
}

/// Seconds on a monotonic clock, for the in-process GCRA gates. Not the wall clock:
/// NTP stepping it back would make every tenant, and the claim pacer, wait out the step.
pub(crate) fn now_secs() -> f64 {
    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    START.elapsed().as_secs_f64()
}

#[cfg(test)]
mod tests;
