//! The job lifecycle, as a state machine.
//!
//! plans/02 assigns this to `state-machines`, and the reason is worth stating
//! because it is the one piece of the design that is about MAINTENANCE rather
//! than correctness-under-load: every reference implementation surveyed stores
//! the lifecycle as a bare string column and compares it against string
//! literals scattered across the codebase. The legal transitions are then not
//! written down anywhere — they are whatever the set of comparisons happens to
//! permit, which is how `executing → available` (losing an attempt) or
//! `completed → retryable` (running a finished job again) get added by accident.
//!
//! Here the transitions are declared once, in the macro below, and the database
//! CHECK constraint plus the WHERE clauses in `002_operations.sql` enforce the
//! same shape. Two enforcement points is deliberate: the SQL functions are what
//! a polyglot worker calls, so SQL has to be authoritative, and this is what
//! keeps the Rust side honest about what those functions will accept.
//!
//! **Dynamic mode, not typestate.** The typestate pattern is the crate's
//! headline feature and it is the wrong tool here: a job arrives from a
//! database row carrying a state as text, so the state is not known at compile
//! time and cannot be a type parameter. The crate's dynamic dispatch exists for
//! exactly this — "events from external sources (queues)" — and it still checks
//! the transition table, just at runtime.

use crate::job::JobState;
use state_machines::state_machine;

// The macro emits `cfg(feature = "inspect")` for its own optional inspection
// API, which is not a feature of THIS crate — so rustc warns about an unknown
// cfg value on every build. Nothing we can fix from here.
#[allow(unexpected_cfgs)]
mod machine {
    use super::*;

    state_machine! {
        name: JobLifecycle,
        dynamic: true,

        initial: Available,
        states: [Available, Claimed, Executing, Retryable, Completed, Discarded, Cancelled],

        events {
            // A worker took it. `attempt` is incremented HERE, not on failure --
            // the plans/04 poison brake.
            claim {
                transition: { from: Available, to: Executing }
                // A retryable job whose backoff has elapsed is claimable. Leaving
                // this out is the poison-job bug: a crashed job parks in Retryable,
                // is never re-claimed, never spends its budget, and never reaches
                // max_attempts.
                transition: { from: Retryable, to: Executing }
            }

            complete {
                transition: { from: Executing, to: Completed }
            }

            // Failed with budget left.
            retry {
                transition: { from: Executing, to: Retryable }
            }

            // Failed with no budget left, or unrecoverable.
            discard {
                transition: { from: Executing, to: Discarded }
                transition: { from: Retryable, to: Discarded }
            }

            // Operator action. Deliberately NOT permitted from Executing: that job
            // belongs to a worker, and flipping the row underneath it would make
            // the worker's completion fail its ownership check rather than doing
            // anything useful.
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

/// Whether this transition is legal, without performing it.
///
/// Used to check what the database reported back: the SQL functions return the
/// before and after states via `RETURNING OLD./NEW.`, and a pair that is not in
/// the table means SQL and Rust have drifted — which is a bug worth shouting
/// about rather than silently accepting, since it means one of the two
/// enforcement points has stopped enforcing.
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
///
/// Surfaced by the MCP control plane so an operator — or a model — can be told
/// what it is allowed to do with a job, rather than trying something and
/// getting a refusal.
pub fn available_events(from: JobState) -> Vec<Transitions> {
    DynamicJobLifecycle::new_init_state((), from.into()).get_available_events()
}

#[cfg(test)]
mod tests;
