//! Per-tenant admission control.
//!
//! One noisy tenant enqueueing 100k jobs must not starve everyone else sharing
//! the queue. Claim order alone cannot fix that — jobs are ordered by priority
//! and age, so the flood is *correctly* first in line.
//!
//! Worth noting: rate limiting is a paid feature in both Sidekiq Enterprise and
//! Oban Pro. It is in the open-source core here because `throttle-machines`
//! already exists (plans/02).
//!
//! GCRA rather than a token bucket: it produces a smooth admission rate instead
//! of letting a full bucket drain instantly, which is what you want when the
//! thing being protected is a shared database rather than a burst-tolerant API.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use throttle_machines::gate::Gate;
use throttle_machines::gcra::{Gcra, GcraParams};

/// How many jobs per second a single tenant may start, and how much burst is
/// tolerated above that.
#[derive(Debug, Clone, Copy)]
pub struct TenantLimit {
    pub jobs_per_second: f64,
    /// Extra jobs a tenant may start back-to-back before the smooth rate
    /// applies. Zero means strictly paced.
    pub burst: f64,
}

impl Default for TenantLimit {
    fn default() -> Self {
        // Deliberately generous: fairness should only bite under genuine
        // contention. A limit low enough to shape normal traffic would be a
        // throughput cap wearing a fairness costume.
        Self { jobs_per_second: 100.0, burst: 50.0 }
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

/// Admission control across tenants.
///
/// `throttle-machines` gates are pure functions over caller-held state, so the
/// per-tenant TAT lives here rather than in the crate. That is what makes the
/// gate `no_std` and trivially testable — and it means this map is the only
/// thing that needs locking.
#[derive(Debug)]
pub struct Fairness {
    limit: TenantLimit,
    /// tenant -> theoretical arrival time
    state: Mutex<HashMap<String, f64>>,
}

impl Fairness {
    pub fn new(limit: TenantLimit) -> Self {
        Self { limit, state: Mutex::new(HashMap::new()) }
    }

    /// Whether this tenant may start a job now.
    ///
    /// Jobs with no tenant are always admitted: single-tenant deployments
    /// should not pay for a feature they are not using.
    pub fn admit(&self, tenant: Option<&str>) -> Admission {
        let Some(tenant) = tenant else {
            return Admission::Allowed;
        };
        self.admit_at(tenant, now_secs())
    }

    /// Injectable clock, for tests.
    pub fn admit_at(&self, tenant: &str, now: f64) -> Admission {
        let mut state = self.state.lock().expect("fairness state poisoned");
        let tat = state.get(tenant).copied().unwrap_or(0.0);

        let decision = Gcra::check(tat, now, self.limit.params());
        state.insert(tenant.to_string(), decision.state);

        if decision.allowed {
            Admission::Allowed
        } else {
            Admission::Throttled { retry_after_secs: decision.retry_after }
        }
    }

    /// Drop state for tenants that have gone quiet.
    ///
    /// Without this the map grows once per tenant seen, forever — a slow leak
    /// in any system where tenants are created dynamically.
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

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests;
