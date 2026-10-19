//! The job lifecycle: which event is legal from which state.

use crate::{STAMPEDED_ERROR, STAMPEDED_NONE, STAMPEDED_OK, arg, guarded};
use stampeded::JobState;
use stampeded::lifecycle::{EVENTS, apply, event_name, event_named};
use std::ffi::{CString, c_char};
use std::sync::LazyLock;

static STATES: LazyLock<Vec<(JobState, CString)>> = LazyLock::new(|| {
    JobState::ALL
        .into_iter()
        .map(|s| {
            (
                s,
                CString::new(s.as_str()).expect("state names have no NUL"),
            )
        })
        .collect()
});

static STATE_LIST: LazyLock<CString> = LazyLock::new(|| {
    let names: Vec<_> = JobState::ALL.iter().map(|s| s.as_str()).collect();
    CString::new(names.join(",")).expect("state names have no NUL")
});

static EVENT_LIST: LazyLock<CString> = LazyLock::new(|| {
    let names: Vec<_> = EVENTS.iter().map(event_name).collect();
    CString::new(names.join(",")).expect("event names have no NUL")
});

/// Every job state, comma-separated. Static.
#[must_use]
#[unsafe(no_mangle)]
pub extern "C" fn stampeded_lifecycle_states() -> *const c_char {
    STATE_LIST.as_ptr()
}

/// Every lifecycle event, comma-separated. Static.
#[must_use]
#[unsafe(no_mangle)]
pub extern "C" fn stampeded_lifecycle_events() -> *const c_char {
    EVENT_LIST.as_ptr()
}

/// Apply `event` to a job in `state`: STAMPEDED_OK with the next state in `*next_out` (static;
/// `next_out` may be NULL), STAMPEDED_NONE when the transition is illegal, STAMPEDED_ERROR for an
/// unknown state or event.
///
/// # Safety
///
/// `state` and `event` are NUL-terminated; `next_out` is NULL or writable.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lifecycle_apply(
    state: *const c_char,
    event: *const c_char,
    next_out: *mut *const c_char,
) -> i32 {
    guarded(STAMPEDED_ERROR, || {
        // SAFETY: the caller's contract.
        let state = unsafe { arg(state, "state") }?;
        // SAFETY: the caller's contract.
        let event = unsafe { arg(event, "event") }?;
        let from = JobState::parse(state).ok_or_else(|| format!("unknown state {state:?}"))?;
        let event = event_named(event).ok_or_else(|| format!("unknown event {event:?}"))?;

        let Some(to) = apply(from, event) else {
            return Ok(STAMPEDED_NONE);
        };
        if !next_out.is_null() {
            let name = STATES
                .iter()
                .find(|(s, _)| *s == to)
                .map(|(_, n)| n.as_ptr());
            // SAFETY: next_out is non-null and writable by the caller's contract.
            unsafe { *next_out = name.unwrap_or(std::ptr::null()) };
        }
        Ok(STAMPEDED_OK)
    })
}
