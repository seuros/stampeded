//! Worker configuration and policy tests.
//!
//! The claim/execute loop itself is covered by integration tests against a real
//! Postgres (tests/integration.rs) — mocking a queue would only prove the mock
//! works.

use super::*;

#[test]
fn default_config_heartbeats_well_inside_the_reap_window() {
    let c = WorkerConfig::default();
    // The reaper's default dead_after is 5 minutes. A heartbeat interval at or
    // above that reaps healthy workers mid-job; keep a wide margin.
    assert!(
        c.heartbeat_interval * 4 <= Duration::from_secs(300),
        "heartbeat {:?} too close to the 5m reap window",
        c.heartbeat_interval
    );
}

#[test]
fn default_backoff_uses_full_jitter() {
    // The differentiator: of thirteen surveyed implementations, not one
    // defaults to full jitter. If this regresses, a batch of jobs failing
    // together retries together -- the stampede the project is named for.
    //
    // Asserted behaviourally: a narrowing jitter (ActiveJob's 15%) would also
    // vary, so we require the spread to reach well below the deterministic
    // ceiling, which only full jitter does.
    let policy = default_backoff();
    let mut rng = chrono_machines::rand::rng();

    let (mut min, mut max) = (u64::MAX, 0u64);
    for _ in 0..200 {
        if let Some(d) = policy.delay(4, &mut rng) {
            min = min.min(d);
            max = max.max(d);
        }
    }

    assert!(max > 0, "backoff produced no delays");
    assert!(min < max / 2, "spread {min}..{max} is too narrow for full jitter");
}

#[test]
fn backoff_delay_is_bounded_and_varies() {
    let policy = BackoffPolicy::Exponential(
        ExponentialBackoff::new()
            .base_delay_ms(1_000)
            .max_delay_ms(60_000)
            .jitter_factor(1.0)
            .max_attempts(10),
    );
    let mut rng = chrono_machines::rand::rng();

    let mut seen = std::collections::HashSet::new();
    for _ in 0..50 {
        let d = policy.delay(5, &mut rng).expect("attempt 5 of 10 should retry");
        assert!(d <= 60_000, "delay {d} exceeded max_delay_ms");
        seen.insert(d);
    }
    // Full jitter samples over a range; identical values 50 times running would
    // mean jitter is not applied.
    assert!(seen.len() > 1, "full jitter produced a constant delay");
}

#[test]
fn backoff_stops_at_max_attempts() {
    let policy = BackoffPolicy::Exponential(ExponentialBackoff::new().max_attempts(3));
    let mut rng = chrono_machines::rand::rng();
    assert!(policy.delay(0, &mut rng).is_some());
    assert!(
        policy.delay(3, &mut rng).is_none(),
        "exhausted budget must return None so the job is discarded"
    );
}
