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
    let ms = Duration::from_millis;
    let cases = [
        // jobs_per_second, burst -> emission_interval, delay_tolerance
        ((4.0, 0.0), (ms(250), Duration::ZERO)),
        ((4.0, 2.0), (ms(250), ms(500))),
        // A negative burst is no burst.
        ((4.0, -1.0), (ms(250), Duration::ZERO)),
        // Past one job per nanosecond the gate paces at one.
        (
            (f64::INFINITY, 0.0),
            (Duration::from_nanos(1), Duration::ZERO),
        ),
        // A misconfigured rate must not divide by zero or admit everything,
        // whatever burst came with it.
        ((0.0, 0.0), (Duration::MAX, Duration::ZERO)),
        ((0.0, 50.0), (Duration::MAX, Duration::ZERO)),
        ((-1.0, 50.0), (Duration::MAX, Duration::ZERO)),
        ((f64::NAN, 50.0), (Duration::MAX, Duration::ZERO)),
    ];
    for ((jobs_per_second, burst), (interval, tolerance)) in cases {
        let p = TenantLimit {
            jobs_per_second,
            burst,
        }
        .policy();
        assert_eq!(
            (p.emission_interval(), p.delay_tolerance()),
            (interval, tolerance),
            "{jobs_per_second} rps, burst {burst}"
        );
    }
}

#[test]
fn a_zero_rate_admits_no_job() {
    let f = limiter(0.0, 50.0);
    assert_eq!(
        f.admit_at("t", Duration::from_secs(1)),
        Admission::Throttled {
            retry_after: Duration::MAX
        }
    );
}

#[test]
fn a_throttled_tenant_is_told_when_to_come_back() {
    let f = limiter(4.0, 0.0);
    let now = Duration::from_secs(1);
    assert!(f.admit_at("t", now).is_allowed());
    assert_eq!(
        f.admit_at("t", now),
        Admission::Throttled {
            retry_after: Duration::from_millis(250)
        }
    );
}

#[test]
fn one_noisy_tenant_does_not_throttle_another() {
    let f = limiter(10.0, 0.0);
    for _ in 0..50 {
        f.admit_at("noisy", Duration::from_secs(1));
    }
    assert!(
        f.admit_at("quiet", Duration::from_secs(1)).is_allowed(),
        "a quiet tenant was starved by a noisy one"
    );
}

#[test]
fn idle_tenants_are_evicted_so_state_does_not_leak() {
    let f = limiter(10.0, 0.0);
    f.admit_at("gone", Duration::from_secs(1));
    assert_eq!(f.tracked_tenants(), 1);

    f.evict_idle(Duration::from_secs(1000));
    assert_eq!(f.tracked_tenants(), 0);
}

#[test]
fn a_full_shard_defers_a_new_tenant_until_a_slot_recovers() {
    use std::hash::RandomState;
    use std::num::NonZeroUsize;

    let policy = TenantLimit {
        jobs_per_second: 1.0,
        burst: 0.0,
    }
    .policy();
    let f = Fairness {
        limiter: KeyedGcra::with_hasher(
            policy,
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            RandomState::new(),
        ),
    };

    let now = Duration::from_secs(1);
    assert!(f.admit_at("first", now).is_allowed());
    assert_eq!(
        f.admit_at("new", now),
        Admission::Throttled {
            retry_after: Duration::from_secs(1),
        }
    );
    assert!(f.admit_at("new", now + Duration::from_secs(1)).is_allowed());
}
