//! Named steps: the record a job keeps in its checkpoint, shared by every runtime.

use crate::{STAMPEDED_ERROR, STAMPEDED_NONE, STAMPEDED_OK, arg, guarded, opt_arg};
use stampeded::StepState;
use std::ffi::{CString, c_char};

/// A job's step record. Strings it returns live until the next call on it.
pub struct Steps {
    state: StepState,
    out: Option<CString>,
}

impl Steps {
    fn hand_out(&mut self, text: Option<String>) -> Result<*const c_char, String> {
        self.out = text
            .map(CString::new)
            .transpose()
            .map_err(|e| e.to_string())?;
        Ok(self.out.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()))
    }
}

fn json(text: &str, name: &str) -> Result<serde_json::Value, String> {
    serde_json::from_str(text).map_err(|e| format!("{name} is not valid JSON: {e}"))
}

/// Read the step record out of a checkpoint (JSON, or NULL for none); a plain cursor
/// checkpoint holds none. NULL when the JSON is invalid.
///
/// # Safety
///
/// `checkpoint_json` is NULL or NUL-terminated.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_steps_load(checkpoint_json: *const c_char) -> *mut Steps {
    guarded(std::ptr::null_mut(), || {
        // SAFETY: the caller's contract.
        let checkpoint = unsafe { opt_arg(checkpoint_json, "checkpoint_json") }?
            .map(|c| json(c, "checkpoint_json"))
            .transpose()?;
        let state = StepState::from_checkpoint(checkpoint.as_ref());
        Ok(Box::into_raw(Box::new(Steps { state, out: None })))
    })
}

/// Free a step record.
///
/// # Safety
///
/// `steps` came from `stampeded_steps_load`, or is NULL, and is not used again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_steps_free(steps: *mut Steps) {
    if !steps.is_null() {
        // SAFETY: the caller hands back the pointer stampeded_steps_load returned.
        drop(unsafe { Box::from_raw(steps) });
    }
}

/// STAMPEDED_OK when step `name` has completed, STAMPEDED_NONE when it has not.
///
/// # Safety
///
/// `steps` came from `stampeded_steps_load`; `name` is NUL-terminated.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_steps_completed(
    steps: *const Steps,
    name: *const c_char,
) -> i32 {
    guarded(STAMPEDED_ERROR, || {
        // SAFETY: the caller's contract.
        let steps = unsafe { steps.as_ref() }.ok_or("steps is NULL")?;
        // SAFETY: the caller's contract.
        let name = unsafe { arg(name, "name") }?;
        Ok(if steps.state.completed(name) {
            STAMPEDED_OK
        } else {
            STAMPEDED_NONE
        })
    })
}

/// What completed step `name` stored (JSON), or NULL if it stored nothing.
///
/// # Safety
///
/// `steps` came from `stampeded_steps_load` and no other thread is using it; `name` is
/// NUL-terminated.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_steps_value(
    steps: *mut Steps,
    name: *const c_char,
) -> *const c_char {
    guarded(std::ptr::null(), || {
        // SAFETY: the caller's contract.
        let steps = unsafe { steps.as_mut() }.ok_or("steps is NULL")?;
        // SAFETY: the caller's contract.
        let name = unsafe { arg(name, "name") }?;
        let value = steps.state.values.get(name).map(ToString::to_string);
        steps.hand_out(value)
    })
}

/// The position saved within unfinished step `name` (JSON), or NULL.
///
/// # Safety
///
/// As `stampeded_steps_value`.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_steps_cursor(
    steps: *mut Steps,
    name: *const c_char,
) -> *const c_char {
    guarded(std::ptr::null(), || {
        // SAFETY: the caller's contract.
        let steps = unsafe { steps.as_mut() }.ok_or("steps is NULL")?;
        // SAFETY: the caller's contract.
        let name = unsafe { arg(name, "name") }?;
        let cursor = steps.state.cursors.get(name).map(ToString::to_string);
        steps.hand_out(cursor)
    })
}

/// Record step `name` as done, with what it produced (JSON, or NULL for nothing).
/// STAMPEDED_ERROR when the result is too large to keep.
///
/// # Safety
///
/// As `stampeded_steps_value`; `value_json` is NULL or NUL-terminated.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_steps_complete(
    steps: *mut Steps,
    name: *const c_char,
    value_json: *const c_char,
) -> i32 {
    guarded(STAMPEDED_ERROR, || {
        // SAFETY: the caller's contract.
        let steps = unsafe { steps.as_mut() }.ok_or("steps is NULL")?;
        // SAFETY: the caller's contract.
        let name = unsafe { arg(name, "name") }?;
        // SAFETY: the caller's contract.
        let value = unsafe { opt_arg(value_json, "value_json") }?
            .map(|v| json(v, "value_json"))
            .transpose()?;
        steps
            .state
            .complete(name, value.as_ref())
            .map_err(|e| e.to_string())?;
        Ok(STAMPEDED_OK)
    })
}

/// Save a position within unfinished step `name` (JSON).
///
/// # Safety
///
/// As `stampeded_steps_value`; `cursor_json` is NUL-terminated.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_steps_set_cursor(
    steps: *mut Steps,
    name: *const c_char,
    cursor_json: *const c_char,
) -> i32 {
    guarded(STAMPEDED_ERROR, || {
        // SAFETY: the caller's contract.
        let steps = unsafe { steps.as_mut() }.ok_or("steps is NULL")?;
        // SAFETY: the caller's contract.
        let name = unsafe { arg(name, "name") }?;
        // SAFETY: the caller's contract.
        let cursor = json(unsafe { arg(cursor_json, "cursor_json") }?, "cursor_json")?;
        steps
            .state
            .set_cursor(name, &cursor)
            .map_err(|e| e.to_string())?;
        Ok(STAMPEDED_OK)
    })
}

/// The whole record as JSON, to save as the job's checkpoint.
///
/// # Safety
///
/// As `stampeded_steps_value`.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_steps_json(steps: *mut Steps) -> *const c_char {
    guarded(std::ptr::null(), || {
        // SAFETY: the caller's contract.
        let steps = unsafe { steps.as_mut() }.ok_or("steps is NULL")?;
        let text = serde_json::to_string(&steps.state).map_err(|e| e.to_string())?;
        steps.hand_out(Some(text))
    })
}
