//! Tenant fairness tests. All use an injected clock -- a rate limiter tested
//! with real time is a flaky test generator.

use super::*;

fn limiter(rps: f64, burst: f64) -> Fairness {
    Fairness::new(TenantLimit {
        jobs_per_second: rps,
        burst,
    })
}

#[test]
fn jobs_without_a_tenant_are_always_admitted() {
    // Single-tenant deployments must not pay for a feature they do not use.
    let f = limiter(1.0, 0.0);
    for _ in 0..100 {
        assert!(f.admit(None).is_allowed());
    }
    assert_eq!(
        f.tracked_tenants(),
        0,
        "untenanted jobs should not allocate state"
    );
}

#[test]
fn a_tenant_is_paced_to_its_rate() {
    let f = limiter(10.0, 0.0); // one job per 100ms, no burst
    assert!(f.admit_at("acme", 1.0).is_allowed());
    // Immediately again: too soon.
    assert!(!f.admit_at("acme", 1.0).is_allowed());
    // After the emission interval: allowed.
    assert!(f.admit_at("acme", 1.1).is_allowed());
}

#[test]
fn burst_is_tolerated_then_the_smooth_rate_applies() {
    let f = limiter(10.0, 3.0); // 3 jobs of slack
    let mut allowed = 0;
    for _ in 0..10 {
        if f.admit_at("acme", 1.0).is_allowed() {
            allowed += 1;
        }
    }
    // The burst lets a few through at the same instant, but not all ten.
    assert!(
        (1..=5).contains(&allowed),
        "admitted {allowed} at one instant"
    );
}

#[test]
fn one_noisy_tenant_does_not_throttle_another() {
    // The entire point: tenants must have independent state.
    let f = limiter(10.0, 0.0);
    for _ in 0..50 {
        f.admit_at("noisy", 1.0);
    }
    assert!(
        f.admit_at("quiet", 1.0).is_allowed(),
        "a quiet tenant was starved by a noisy one"
    );
}

#[test]
fn throttled_reports_how_long_to_wait() {
    let f = limiter(1.0, 0.0);
    assert!(f.admit_at("acme", 1.0).is_allowed());
    match f.admit_at("acme", 1.0) {
        Admission::Throttled { retry_after_secs } => {
            assert!(retry_after_secs > 0.0, "retry_after should be positive");
            assert!(
                retry_after_secs <= 1.0,
                "retry_after {retry_after_secs} exceeds the interval"
            );
        }
        Admission::Allowed => panic!("second call at the same instant should be throttled"),
    }
}

#[test]
fn idle_tenants_are_evicted_so_state_does_not_leak() {
    // Without eviction the map grows once per tenant ever seen -- a slow leak
    // anywhere tenants are created dynamically.
    let f = limiter(10.0, 0.0);
    f.admit_at("gone", 1.0);
    assert_eq!(f.tracked_tenants(), 1);

    f.evict_idle(1000.0);
    assert_eq!(f.tracked_tenants(), 0);
}

#[test]
fn zero_rate_blocks_everything_after_the_first() {
    // A misconfigured 0 rps must not divide by zero or admit everything.
    let f = limiter(0.0, 0.0);
    f.admit_at("acme", 1.0);
    assert!(!f.admit_at("acme", 1.0).is_allowed());
}
