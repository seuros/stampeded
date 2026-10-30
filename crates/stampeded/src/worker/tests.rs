//! Worker configuration and policy tests; the claim loop is covered in tests/integration.rs.

use super::*;

#[test]
fn default_config_heartbeats_well_inside_the_reap_window() {
    let c = WorkerConfig::default();
    // The reaper's default dead_after is 5 minutes; keep a wide margin.
    assert!(
        c.heartbeat_interval * 4 <= Duration::from_secs(300),
        "heartbeat {:?} too close to the 5m reap window",
        c.heartbeat_interval
    );
}

#[test]
fn the_default_backoff_is_full_jitter_and_leaves_the_limit_to_the_database() {
    let BackoffPolicy::Exponential(b) = default_backoff() else {
        panic!("the default backoff is exponential");
    };
    // Full jitter spreads a retrying herd; a narrow band keeps it together.
    assert!((b.jitter_factor - 1.0).abs() < f64::EPSILON, "{b:?}");
    // A `None` delay past the crate's own limit would mean an immediate retry.
    assert_eq!(b.max_attempts, u8::MAX);
}

#[test]
fn queue_time_is_measured_on_the_databases_clock() {
    // Timestamps in 2001: the host clock for either end would give decades or zero, not 1.5s.
    let scheduled = jiff::Timestamp::from_second(1_000_000_000).unwrap();
    let mut job = job_with_backoff(SCHEMA_BACKOFF_BASE_MS, SCHEMA_BACKOFF_MAX_MS, 1);
    job.scheduled_at = scheduled;

    let cases = [
        (
            Some(scheduled + jiff::SignedDuration::from_millis(1_500)),
            Some(Duration::from_millis(1_500)),
        ),
        (
            Some(scheduled - jiff::SignedDuration::from_secs(1)),
            Some(Duration::ZERO),
        ),
        (None, None),
    ];
    for (claimed_at, expected) in cases {
        job.claimed_at = claimed_at;
        assert_eq!(queue_time(&job), expected, "claimed_at {claimed_at:?}");
    }
}

#[test]
fn circuit_open_retries_rather_than_discarding() {
    // An unrecoverable CircuitOpen would discard every job an outage touched.
    let e = crate::Error::CircuitOpen("task:charge is open".into());
    assert!(!e.is_unrecoverable());
}

#[test]
fn defaults_enable_fairness_and_breakers() {
    let c = WorkerConfig::default();
    assert!(c.tenant_limit.is_some());
    assert!(c.breaker_failure_threshold.is_some());
}

// connect_lazy still needs a reactor even though it never connects.
#[tokio::test]
async fn breakers_are_per_task_not_global() {
    let w = Worker::new(
        Queue::new(sqlx::PgPool::connect_lazy("postgres://invalid/db").unwrap()),
        WorkerConfig::default(),
    );
    let a = w.breaker_for("charge_card", 5);
    let b = w.breaker_for("send_email", 5);
    let a_again = w.breaker_for("charge_card", 5);

    assert!(!std::sync::Arc::ptr_eq(&a, &b), "tasks share a breaker");
    assert!(
        std::sync::Arc::ptr_eq(&a, &a_again),
        "same task got two breakers"
    );
}

#[test]
fn the_wakeup_cooldown_does_not_cap_the_claim() {
    // One stampeded_claim at batch 5 costs about 668 us on Postgres 18.
    const MEASURED_CLAIM: Duration = Duration::from_micros(668);
    let cfg = WorkerConfig::default();

    assert!(
        cfg.wakeup_cooldown >= MEASURED_CLAIM,
        "a cooldown below the claim cost paces nothing: {:?} < {MEASURED_CLAIM:?}",
        cfg.wakeup_cooldown
    );
    assert!(
        cfg.wakeup_cooldown <= Duration::from_millis(5),
        "and above a few ms it caps throughput instead of bounding bursts"
    );
}

#[test]
fn a_deferral_logs_at_debug_unless_it_discarded_the_job() {
    use crate::test_support::Levels;
    use tracing::Level;
    // Deferrals log at debug so they do not bury real failures; a discard still warns.
    let job = job_with_backoff(SCHEMA_BACKOFF_BASE_MS, SCHEMA_BACKOFF_MAX_MS, 1);
    let cases = [
        (FailureKind::Throttled, false, Level::DEBUG),
        (FailureKind::BreakerOpen, false, Level::DEBUG),
        (FailureKind::Throttled, true, Level::WARN),
        (FailureKind::BreakerOpen, true, Level::WARN),
        (FailureKind::Raised, false, Level::WARN),
        (FailureKind::TimedOut, false, Level::WARN),
        (FailureKind::HeartbeatLost, false, Level::WARN),
        (FailureKind::Unrecoverable, true, Level::WARN),
    ];
    for (kind, discarded, expected) in cases {
        let ((), seen) = Levels::during(|| {
            log_failure(&job, kind, "boom", Some(Duration::from_secs(1)), discarded);
        });
        assert_eq!(seen, [expected], "{kind}, discarded: {discarded}");
    }
}

/// Build a job row with the given per-job backoff columns.
fn job_with_backoff(base_ms: i16, max_ms: i32, attempt: i16) -> Job {
    Job {
        id: uuid::Uuid::now_v7(),
        queue: "default".into(),
        tenant_id: None,
        task: "t".into(),
        args: serde_json::json!({}),
        meta: serde_json::json!({}),
        state: crate::job::JobState::Executing,
        priority: 0,
        scheduled_at: jiff::Timestamp::now(),
        attempt,
        max_attempts: 25,
        attempted_by: vec![],
        claimed_by: None,
        claimed_at: None,
        dedup_scope: None,
        dedup_period: None,
        destination: None,
        interruptions: 0,
        max_interruptions: 1,
        deferrals: 0,
        max_deferrals: 100,
        expire_seconds: 3600,
        heartbeat_at: None,
        backoff_base_ms: base_ms,
        backoff_max_ms: max_ms,
        checkpoint: None,
        checkpoint_at: None,
        checkpoint_seq: 0,
        failure_kind: None,
        last_error: None,
        errors: serde_json::json!([]),
        error_fingerprint: None,
        dead_letter_queue: None,
        source_id: None,
        source_queue: None,
        source_attempts: None,
        inserted_at: jiff::Timestamp::now(),
        finished_at: None,
    }
}

/// Sample a policy repeatedly; full jitter means one draw proves nothing.
fn sample_delays(policy: BackoffPolicy, attempt: i16, n: usize) -> Vec<u64> {
    let mut rng = chrono_machines::rand::rng();
    (0..n)
        .map(|_| {
            policy
                .delay(u8::try_from(attempt.max(0)).unwrap_or(u8::MAX), &mut rng)
                .unwrap_or(FALLBACK_RETRY_DELAY_MS)
        })
        .collect()
}

#[test]
fn a_jobs_own_backoff_columns_beat_the_workers_configured_policy() {
    // Parity with Ruby's `backoff_ms`, which reads the row.
    let tight = job_with_backoff(2, 4, 8);

    // Attempt 8: 128s under the default policy, at most 4ms under the job's own.
    let delays = sample_delays(policy_for(&tight, default_backoff()), tight.attempt, 200);
    let worst = delays.iter().copied().max().unwrap();

    assert!(
        worst <= 4,
        "per-job cap of 4ms ignored: sampled a {worst}ms delay. The worker is \
         still using its own policy instead of the row."
    );
}

#[test]
fn a_row_carrying_the_schema_defaults_leaves_the_configured_policy_alone() {
    // A job that asked for nothing must not override a worker's `.backoff(...)`.
    let plain = job_with_backoff(SCHEMA_BACKOFF_BASE_MS, SCHEMA_BACKOFF_MAX_MS, 1);

    let configured = BackoffPolicy::Exponential(
        ExponentialBackoff::new()
            .base_delay_ms(30_000)
            .max_delay_ms(60_000)
            .jitter_factor(1.0)
            .max_attempts(u8::MAX),
    );

    let worst = sample_delays(policy_for(&plain, configured), plain.attempt, 200)
        .into_iter()
        .max()
        .unwrap();

    // The schema default caps attempt 1 at 1000ms; 200 draws over 0..30000 clear it.
    assert!(
        worst > 1_000,
        "worker's configured policy was overridden by a job that expressed no \
         preference: worst of 200 draws was {worst}ms, within the schema default"
    );
}

#[test]
fn hostname_falls_back_to_the_os_before_unknown() {
    const UNKNOWN: &str = "unknown";
    let s = |v: &str| Some(v.to_string());
    let cases = [
        (s("container-7"), s("box"), "container-7"),
        (None, s("box"), "box"),
        (s(""), s("box"), "box"),
        (s("  "), s("box"), "box"),
        (None, None, UNKNOWN),
        (s(""), s(""), UNKNOWN),
    ];
    for (env, os, expected) in cases {
        assert_eq!(
            pick_hostname(env.clone(), os.clone()),
            expected,
            "env {env:?}, os {os:?}"
        );
    }
    // The real lookup must name this machine whatever the shell exported.
    assert_ne!(hostname(), UNKNOWN);
}
