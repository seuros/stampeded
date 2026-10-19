//! The transition table.

use super::*;

#[test]
fn a_retryable_job_can_be_claimed_again() {
    // Otherwise a crashed job parks forever and never reaches max_attempts.
    assert!(is_legal(JobState::Retryable, Transitions::Claim));
    assert_eq!(
        apply(JobState::Retryable, Transitions::Claim),
        Some(JobState::Executing)
    );
}

#[test]
fn a_finished_job_can_never_run_again() {
    for terminal in [
        JobState::Completed,
        JobState::Discarded,
        JobState::Cancelled,
    ] {
        assert!(
            !is_legal(terminal, Transitions::Claim),
            "{terminal} must not be claimable"
        );
        assert!(
            available_events(terminal).is_empty(),
            "{terminal} must be a dead end"
        );
    }
}

#[test]
fn an_executing_job_cannot_be_cancelled_out_from_under_its_worker() {
    // Flipping the row would not stop the work, only fail the worker's ownership check.
    assert!(!is_legal(JobState::Executing, Transitions::Cancel));
    assert!(is_legal(JobState::Available, Transitions::Cancel));
    assert!(is_legal(JobState::Retryable, Transitions::Cancel));
}

#[test]
fn only_a_running_job_can_complete() {
    assert!(is_legal(JobState::Executing, Transitions::Complete));
    for s in [
        JobState::Available,
        JobState::Claimed,
        JobState::Retryable,
        JobState::Completed,
    ] {
        assert!(!is_legal(s, Transitions::Complete), "{s} must not complete");
    }
}

#[test]
fn the_reaper_can_only_reclaim_jobs_that_were_held() {
    // Reclaiming an `available` job would charge an interruption it never had.
    assert!(is_legal(JobState::Executing, Transitions::Reclaim));
    assert!(is_legal(JobState::Claimed, Transitions::Reclaim));
    assert!(!is_legal(JobState::Available, Transitions::Reclaim));
    assert!(!is_legal(JobState::Completed, Transitions::Reclaim));
}

#[test]
fn a_job_can_be_discarded_from_either_live_state() {
    // Executing on a terminal failure; Retryable when out of interruptions.
    assert_eq!(
        apply(JobState::Executing, Transitions::Discard),
        Some(JobState::Discarded)
    );
    assert_eq!(
        apply(JobState::Retryable, Transitions::Discard),
        Some(JobState::Discarded)
    );
}

#[test]
fn every_state_round_trips_through_the_machine() {
    // The two From impls are hand-written.
    for s in [
        JobState::Available,
        JobState::Claimed,
        JobState::Executing,
        JobState::Retryable,
        JobState::Completed,
        JobState::Discarded,
        JobState::Cancelled,
    ] {
        let round: JobState = JobLifecycleState::from(s).into();
        assert_eq!(round, s);
    }
}

#[test]
fn claimable_states_agree_with_the_transition_table() {
    for s in [
        JobState::Available,
        JobState::Claimed,
        JobState::Executing,
        JobState::Retryable,
        JobState::Completed,
        JobState::Discarded,
        JobState::Cancelled,
    ] {
        assert_eq!(
            s.is_claimable(),
            is_legal(s, Transitions::Claim),
            "{s}: is_claimable and the transition table disagree"
        );
    }
}

#[test]
fn terminal_states_agree_with_the_transition_table() {
    for s in [
        JobState::Available,
        JobState::Claimed,
        JobState::Executing,
        JobState::Retryable,
        JobState::Completed,
        JobState::Discarded,
        JobState::Cancelled,
    ] {
        assert_eq!(
            s.is_terminal(),
            available_events(s).is_empty(),
            "{s}: is_terminal and the transition table disagree"
        );
    }
}

#[test]
fn only_a_running_job_can_be_given_back_unstarted() {
    assert_eq!(
        apply(JobState::Executing, Transitions::Release),
        Some(JobState::Available)
    );
    for s in [
        JobState::Available,
        JobState::Claimed,
        JobState::Retryable,
        JobState::Completed,
        JobState::Discarded,
        JobState::Cancelled,
    ] {
        assert!(
            !is_legal(s, Transitions::Release),
            "{s} must not be given back"
        );
    }
}

#[test]
fn every_event_round_trips_by_name_and_is_listed() {
    for event in EVENTS {
        assert!(
            event_named(event_name(&event)).is_some_and(|e| event_name(&e) == event_name(&event))
        );
    }
    assert!(event_named("explode").is_none());
    for state in JobState::ALL {
        for event in available_events(state) {
            assert!(
                event_named(event_name(&event)).is_some(),
                "{state}: {} is not in EVENTS",
                event_name(&event)
            );
        }
    }
}
