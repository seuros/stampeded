//! Tests for job lifecycle types.
use super::*;

#[test]
fn retryable_is_claimable_not_terminal() {
    // If terminal, a crashed job would never be re-claimed or spend its budget.
    assert!(JobState::Retryable.is_claimable());
    assert!(!JobState::Retryable.is_terminal());
}

#[test]
fn terminal_states_are_not_claimable() {
    for s in [
        JobState::Completed,
        JobState::Discarded,
        JobState::Cancelled,
    ] {
        assert!(s.is_terminal());
        assert!(!s.is_claimable());
    }
}

#[test]
fn worker_death_is_not_fatal() {
    // A worker dying is not the job's fault: it must retry, not discard.
    for k in [
        FailureKind::WorkerExited,
        FailureKind::WorkerMissing,
        FailureKind::HeartbeatLost,
    ] {
        assert!(!k.is_fatal(), "{k}");
        assert!(
            k.is_interruption(),
            "{k} must charge the interruptions budget"
        );
    }
    assert!(FailureKind::Unrecoverable.is_fatal());
}

#[test]
fn only_our_own_backpressure_refunds_the_attempt() {
    // A breaker trip that burned attempts would discard the backlog during an outage.
    assert!(FailureKind::BreakerOpen.refunds_attempt());
    assert!(FailureKind::Throttled.refunds_attempt());

    // Anything else refunding would let a broken job retry forever.
    for k in [
        FailureKind::Raised,
        FailureKind::TimedOut,
        FailureKind::WorkerExited,
        FailureKind::WorkerMissing,
        FailureKind::HeartbeatLost,
        FailureKind::Cancelled,
        FailureKind::Unrecoverable,
    ] {
        assert!(!k.refunds_attempt(), "{k} must not refund");
    }
}

#[test]
fn a_refunded_failure_is_never_also_an_interruption() {
    // The two budgets are disjoint.
    for k in [
        FailureKind::Raised,
        FailureKind::TimedOut,
        FailureKind::HeartbeatLost,
        FailureKind::WorkerExited,
        FailureKind::WorkerMissing,
        FailureKind::BreakerOpen,
        FailureKind::Throttled,
        FailureKind::Cancelled,
        FailureKind::Unrecoverable,
    ] {
        assert!(!(k.refunds_attempt() && k.is_interruption()), "{k}");
    }
}

#[test]
fn failure_kind_strings_match_the_schema_check_constraint() {
    // Validated by a CHECK at write time, so a mismatch is a runtime failure.
    assert_eq!(FailureKind::TimedOut.as_str(), "timed_out");
    assert_eq!(FailureKind::HeartbeatLost.as_str(), "heartbeat_lost");
    assert_eq!(FailureKind::WorkerExited.as_str(), "worker_exited");
    assert_eq!(FailureKind::WorkerMissing.as_str(), "worker_missing");
    assert_eq!(FailureKind::BreakerOpen.as_str(), "breaker_open");
}

#[test]
fn builder_limits_saturate_instead_of_wrapping() {
    use std::time::Duration;

    const CASES: &[(Duration, i32)] = &[
        (Duration::ZERO, 1),
        (Duration::from_secs(90), 90),
        (Duration::from_secs(i32::MAX as u64), i32::MAX),
        (Duration::from_secs(i32::MAX as u64 + 1), i32::MAX),
        (Duration::MAX, i32::MAX),
    ];
    for (d, expected) in CASES {
        let job = NewJob::new("q", "t").expires_in(*d);
        assert_eq!(job.expire_seconds, Some(*expected), "expires_in({d:?})");
    }

    for (base, expected) in [(-5, 1), (0, 1), (500, 500), (40_000, i16::MAX)] {
        let job = NewJob::new("q", "t").backoff(base, 1_000);
        assert_eq!(job.backoff_base_ms, Some(expected), "backoff({base}, _)");
    }
}

#[test]
fn null_args_are_stored_as_an_empty_object() {
    // The schema refuses JSON null; `{}` is the column's own "no arguments".
    let job = NewJob::new("q", "t").args(serde_json::Value::Null);
    assert_eq!(job.args, serde_json::json!({}));

    for args in [
        serde_json::json!({"a": 1}),
        serde_json::json!([1, 2]),
        serde_json::json!("s"),
    ] {
        let job = NewJob::new("q", "t").args(args.clone());
        assert_eq!(job.args, args, "non-null args pass through untouched");
    }
}

#[test]
fn every_state_parses_from_its_sql_name() {
    for state in JobState::ALL {
        assert_eq!(JobState::parse(state.as_str()), Some(state));
    }
    assert_eq!(JobState::parse("running"), None);
}
