//! Tenant fairness tests, on an injected clock.

use super::*;

fn limiter(rps: f64, burst: f64) -> Fairness {
    Fairness::new(TenantLimit {
        jobs_per_second: rps,
        burst,
    })
}

#[test]
fn jobs_without_a_tenant_are_always_admitted() {
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
fn a_limit_is_an_interval_per_job_with_burst_jobs_of_slack() {
    // GCRA itself is throttle-machines'; what is ours is how a limit maps onto it.
    let cases = [
        // jobs_per_second, burst -> emission_interval, delay_tolerance
        ((4.0, 0.0), (0.25, 0.0)),
        ((4.0, 2.0), (0.25, 0.5)),
        // A misconfigured 0 rps must not divide by zero or admit everything.
        ((0.0, 0.0), (f64::MAX, 0.0)),
    ];
    for ((jobs_per_second, burst), (interval, tolerance)) in cases {
        let p = TenantLimit {
            jobs_per_second,
            burst,
        }
        .params();
        assert!(
            p.emission_interval.total_cmp(&interval).is_eq()
                && p.delay_tolerance.total_cmp(&tolerance).is_eq(),
            "{jobs_per_second} rps, burst {burst}: {} / {}",
            p.emission_interval,
            p.delay_tolerance
        );
    }
}

#[test]
fn one_noisy_tenant_does_not_throttle_another() {
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
fn idle_tenants_are_evicted_so_state_does_not_leak() {
    let f = limiter(10.0, 0.0);
    f.admit_at("gone", 1.0);
    assert_eq!(f.tracked_tenants(), 1);

    f.evict_idle(1000.0);
    assert_eq!(f.tracked_tenants(), 0);
}
