//! The job lifecycle, as a state machine.
//!
//! Mirrors the transitions the schema enforces, which stays authoritative.
//! Dynamic mode: a row's state is only known at runtime.

use crate::job::JobState;
use state_machines::state_machine;

// The macro emits `cfg(feature = "inspect")`, which is not a feature of this crate.
#[allow(unexpected_cfgs)]
mod machine {
    use super::state_machine;

    state_machine! {
        name: JobLifecycle,
        dynamic: true,

        initial: Available,
        states: [Available, Claimed, Executing, Retryable, Completed, Discarded, Cancelled],

        events {
            // `attempt` is incremented here, not on failure.
            claim {
                transition: { from: Available, to: Executing }
                // Once its backoff has elapsed.
                transition: { from: Retryable, to: Executing }
            }

            complete {
                transition: { from: Executing, to: Completed }
            }

            // Failed with budget left, or given back unstarted after an earlier failure.
            retry {
                transition: { from: Executing, to: Retryable }
            }

            // Given back unstarted by its worker, never having failed.
            release {
                transition: { from: Executing, to: Available }
            }

            // Failed with no budget left, or unrecoverable.
            discard {
                transition: { from: Executing, to: Discarded }
                transition: { from: Retryable, to: Discarded }
            }

            // Operator action; not from Executing, which belongs to its worker.
            cancel {
                transition: { from: Available, to: Cancelled }
                transition: { from: Retryable, to: Cancelled }
            }

            // The reaper reclaiming a job from a dead worker.
            reclaim {
                transition: { from: Claimed, to: Retryable }
                transition: { from: Executing, to: Retryable }
            }
        }
    }
}

pub use machine::*;

impl From<JobState> for JobLifecycleState {
    fn from(s: JobState) -> Self {
        match s {
            JobState::Available => Self::Available,
            JobState::Claimed => Self::Claimed,
            JobState::Executing => Self::Executing,
            JobState::Retryable => Self::Retryable,
            JobState::Completed => Self::Completed,
            JobState::Discarded => Self::Discarded,
            JobState::Cancelled => Self::Cancelled,
        }
    }
}

impl From<JobLifecycleState> for JobState {
    fn from(s: JobLifecycleState) -> Self {
        match s {
            JobLifecycleState::Available => Self::Available,
            JobLifecycleState::Claimed => Self::Claimed,
            JobLifecycleState::Executing => Self::Executing,
            JobLifecycleState::Retryable => Self::Retryable,
            JobLifecycleState::Completed => Self::Completed,
            JobLifecycleState::Discarded => Self::Discarded,
            JobLifecycleState::Cancelled => Self::Cancelled,
        }
    }
}

/// Where a transition can be attempted from a state read out of the database.
pub type Transitions = JobLifecycleEvent;

/// Every event, in declaration order.
pub const EVENTS: [Transitions; 7] = [
    Transitions::Claim,
    Transitions::Complete,
    Transitions::Retry,
    Transitions::Release,
    Transitions::Discard,
    Transitions::Cancel,
    Transitions::Reclaim,
];

/// The event's name, as other runtimes spell it.
pub const fn event_name(event: &Transitions) -> &'static str {
    match event {
        Transitions::Claim => "claim",
        Transitions::Complete => "complete",
        Transitions::Retry => "retry",
        Transitions::Release => "release",
        Transitions::Discard => "discard",
        Transitions::Cancel => "cancel",
        Transitions::Reclaim => "reclaim",
    }
}

pub fn event_named(name: &str) -> Option<Transitions> {
    EVENTS.into_iter().find(|e| event_name(e) == name)
}

/// Whether this transition is legal, without performing it.
pub fn is_legal(from: JobState, event: Transitions) -> bool {
    let mut m = DynamicJobLifecycle::new_init_state((), from.into());
    m.handle(event).is_ok()
}

/// Apply a transition, returning the resulting state.
pub fn apply(from: JobState, event: Transitions) -> Option<JobState> {
    let mut m = DynamicJobLifecycle::new_init_state((), from.into());
    m.handle(event).ok()?;
    Some(m.current_state().into())
}

/// Every event legal from this state.
pub fn available_events(from: JobState) -> Vec<Transitions> {
    DynamicJobLifecycle::new_init_state((), from.into()).get_available_events()
}

#[cfg(test)]
mod tests;
