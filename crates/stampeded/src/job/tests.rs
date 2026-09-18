//! Tests for job lifecycle types.
use super::*;

#[test]
fn retryable_is_claimable_not_terminal() {
    // The poison-job bug in one assertion: if Retryable were treated as
    // terminal, a crashed job would never be re-claimed and its attempt
    // budget would never be spent.
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
    // The correctness rule from plans/15. If a breaker trip burned attempts,
    // a long enough downstream outage would discard the whole backlog instead
    // of replaying it once the dependency came back.
    assert!(FailureKind::BreakerOpen.refunds_attempt());
    assert!(FailureKind::Throttled.refunds_attempt());

    // Everything else must NOT refund, or a genuinely broken job retries
    // forever and the attempt budget stops meaning anything.
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
    // The two budgets are disjoint by construction: refunding an attempt AND
    // charging an interruption would take with one hand and give with the
    // other, which is the kind of accounting nobody can reason about at 3am.
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
    // These strings are bound straight into stampeded_fail and validated by a
    // CHECK. A mismatch is a runtime write failure, not a compile error --
    // `rename_all = "lowercase"` would silently produce "timedout" here.
    assert_eq!(FailureKind::TimedOut.as_str(), "timed_out");
    assert_eq!(FailureKind::HeartbeatLost.as_str(), "heartbeat_lost");
    assert_eq!(FailureKind::WorkerExited.as_str(), "worker_exited");
    assert_eq!(FailureKind::WorkerMissing.as_str(), "worker_missing");
    assert_eq!(FailureKind::BreakerOpen.as_str(), "breaker_open");
}
