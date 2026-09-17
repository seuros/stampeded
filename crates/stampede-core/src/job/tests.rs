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
    for s in [JobState::Completed, JobState::Discarded, JobState::Cancelled] {
        assert!(s.is_terminal());
        assert!(!s.is_claimable());
    }
}

#[test]
fn interrupted_is_not_fatal() {
    // A worker dying is not the job's fault: it must retry, not discard.
    assert!(!FailureKind::Interrupted.is_fatal());
    assert!(FailureKind::Unrecoverable.is_fatal());
}
