//! The transition table.
//!
//! These are the illegal transitions that a string-comparison lifecycle lets
//! through by accident. Each one is a specific bug, named.

use super::*;

#[test]
fn a_retryable_job_can_be_claimed_again() {
    // THE poison-job bug. If Retryable were not claimable, a crashed job would
    // park there forever, never spend its attempt budget, and never reach
    // max_attempts -- an unbounded retry loop that looks like a stalled queue.
    assert!(is_legal(JobState::Retryable, Transitions::Claim));
    assert_eq!(
        apply(JobState::Retryable, Transitions::Claim),
        Some(JobState::Executing)
    );
}

#[test]
fn a_finished_job_can_never_run_again() {
    // Re-running a completed job is the worst bug a queue can have: it charges
    // the card twice. Every terminal state is a dead end.
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
    // The worker owns it. Flipping the row underneath would not stop the work;
    // it would only make the worker's completion fail its ownership check,
    // turning a cancel into a silent no-op with extra steps.
    assert!(!is_legal(JobState::Executing, Transitions::Cancel));
    assert!(is_legal(JobState::Available, Transitions::Cancel));
    assert!(is_legal(JobState::Retryable, Transitions::Cancel));
}

#[test]
fn only_a_running_job_can_complete() {
    // `available -> completed` would mean reporting success for work never
    // done. Reachable in any implementation that compares strings and forgets
    // one of them.
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
    // Reclaiming an `available` job would reset a job nobody was running,
    // charging it an interruption it never had.
    assert!(is_legal(JobState::Executing, Transitions::Reclaim));
    assert!(is_legal(JobState::Claimed, Transitions::Reclaim));
    assert!(!is_legal(JobState::Available, Transitions::Reclaim));
    assert!(!is_legal(JobState::Completed, Transitions::Reclaim));
}

#[test]
fn a_job_can_be_discarded_from_either_live_state() {
    // From Executing when the worker reports a terminal failure, and from
    // Retryable when the reaper finds it has run out of interruptions.
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
    // The two From impls are hand-written, so a state added to one enum and
    // not the other would silently map to the wrong thing. This is the only
    // thing standing between that and a mislabelled job.
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
    // `JobState::is_claimable` is used by the SQL claim predicate and this
    // table is used by the Rust side. If they disagree, one of them is lying
    // about which jobs can run -- and the SQL one wins, silently.
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
