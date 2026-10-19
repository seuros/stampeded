//! The pure exports: lifecycle, cron and step records, called by their C names.

#![allow(unsafe_code, reason = "calls the C ABI")]

use stampeded_ffi::{STAMPEDED_ERROR, STAMPEDED_NONE, STAMPEDED_OK, stampeded_last_error};
use std::ffi::{CStr, CString, c_char, c_void};

unsafe extern "C" {
    fn stampeded_lifecycle_states() -> *const c_char;
    fn stampeded_lifecycle_events() -> *const c_char;
    fn stampeded_lifecycle_apply(
        state: *const c_char,
        event: *const c_char,
        next_out: *mut *const c_char,
    ) -> i32;
    fn stampeded_cron_check(
        expr: *const c_char,
        timezone: *const c_char,
        dst_policy_out: *mut *const c_char,
    ) -> i32;
    fn stampeded_steps_load(checkpoint_json: *const c_char) -> *mut c_void;
    fn stampeded_steps_free(steps: *mut c_void);
    fn stampeded_steps_completed(steps: *const c_void, name: *const c_char) -> i32;
    fn stampeded_steps_value(steps: *mut c_void, name: *const c_char) -> *const c_char;
    fn stampeded_steps_cursor(steps: *mut c_void, name: *const c_char) -> *const c_char;
    fn stampeded_steps_complete(
        steps: *mut c_void,
        name: *const c_char,
        value_json: *const c_char,
    ) -> i32;
    fn stampeded_steps_set_cursor(
        steps: *mut c_void,
        name: *const c_char,
        cursor_json: *const c_char,
    ) -> i32;
    fn stampeded_steps_json(steps: *mut c_void) -> *const c_char;
}

fn c(s: &str) -> CString {
    CString::new(s).expect("no NUL")
}

fn read(p: *const c_char) -> Option<String> {
    // SAFETY: the library hands back NULL or a NUL-terminated string still alive here.
    (!p.is_null()).then(|| unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned())
}

fn last_error() -> String {
    read(stampeded_last_error()).unwrap_or_default()
}

fn apply(state: &str, event: &str) -> (i32, Option<String>) {
    let mut next: *const c_char = std::ptr::null();
    // SAFETY: valid strings and an out pointer on the stack.
    let status =
        unsafe { stampeded_lifecycle_apply(c(state).as_ptr(), c(event).as_ptr(), &raw mut next) };
    (status, read(next))
}

#[test]
fn the_lifecycle_answers_by_name() {
    // SAFETY: both return static strings.
    let (states, events) = unsafe { (stampeded_lifecycle_states(), stampeded_lifecycle_events()) };
    assert_eq!(
        read(states).unwrap(),
        "available,claimed,executing,retryable,completed,discarded,cancelled"
    );
    assert!(read(events).unwrap().split(',').any(|e| e == "release"));

    assert_eq!(
        apply("retryable", "claim"),
        (STAMPEDED_OK, Some("executing".into()))
    );
    assert_eq!(
        apply("executing", "release"),
        (STAMPEDED_OK, Some("available".into()))
    );
    assert_eq!(apply("completed", "claim").0, STAMPEDED_NONE);
    assert_eq!(apply("executing", "cancel").0, STAMPEDED_NONE);

    assert_eq!(apply("running", "claim").0, STAMPEDED_ERROR);
    assert!(last_error().contains("running"));
    assert_eq!(apply("available", "explode").0, STAMPEDED_ERROR);
    assert!(last_error().contains("explode"));
}

#[test]
fn cron_is_checked_as_the_daemon_reads_it() {
    let check = |expr: &str, tz: &str| {
        let mut policy: *const c_char = std::ptr::null();
        // SAFETY: valid strings and an out pointer on the stack.
        let status =
            unsafe { stampeded_cron_check(c(expr).as_ptr(), c(tz).as_ptr(), &raw mut policy) };
        (status, read(policy))
    };
    assert_eq!(
        check("30 1 * * *", "Africa/Casablanca"),
        (STAMPEDED_OK, Some("wall_clock".into()))
    );
    assert_eq!(
        check("*/5 * * * *", "UTC"),
        (STAMPEDED_OK, Some("instant".into()))
    );
    assert_eq!(check("61 * * * *", "UTC").0, STAMPEDED_ERROR);
    assert!(last_error().contains("cron"));
    assert_eq!(check("0 3 * * *", "Mars/Olympus").0, STAMPEDED_ERROR);
    assert!(last_error().contains("Mars/Olympus"));
}

#[test]
fn a_step_record_round_trips_through_a_checkpoint() {
    // SAFETY: every pointer below is valid for the call that takes it.
    unsafe {
        let steps = stampeded_steps_load(std::ptr::null());
        assert!(!steps.is_null());
        assert_eq!(
            stampeded_steps_completed(steps, c("charge").as_ptr()),
            STAMPEDED_NONE
        );

        assert_eq!(
            stampeded_steps_set_cursor(steps, c("email").as_ptr(), c("2").as_ptr()),
            STAMPEDED_OK
        );
        assert_eq!(
            stampeded_steps_complete(steps, c("charge").as_ptr(), std::ptr::null()),
            STAMPEDED_OK
        );
        assert_eq!(
            stampeded_steps_complete(steps, c("render").as_ptr(), c("\"/tmp/r.pdf\"").as_ptr()),
            STAMPEDED_OK
        );
        let saved = read(stampeded_steps_json(steps)).unwrap();
        stampeded_steps_free(steps);

        let resumed = stampeded_steps_load(c(&saved).as_ptr());
        assert_eq!(
            stampeded_steps_completed(resumed, c("charge").as_ptr()),
            STAMPEDED_OK
        );
        assert_eq!(
            read(stampeded_steps_value(resumed, c("charge").as_ptr())),
            None
        );
        assert_eq!(
            read(stampeded_steps_value(resumed, c("render").as_ptr())),
            Some("\"/tmp/r.pdf\"".into())
        );
        assert_eq!(
            read(stampeded_steps_cursor(resumed, c("email").as_ptr())),
            Some("2".into())
        );

        let large = format!("\"{}\"", "x".repeat(9 * 1024));
        assert_eq!(
            stampeded_steps_complete(resumed, c("big").as_ptr(), c(&large).as_ptr()),
            STAMPEDED_ERROR
        );
        assert!(last_error().contains("reference"));
        stampeded_steps_free(resumed);

        let plain = stampeded_steps_load(c("{\"cursor\":50000}").as_ptr());
        assert_eq!(
            stampeded_steps_completed(plain, c("cursor").as_ptr()),
            STAMPEDED_NONE
        );
        stampeded_steps_free(plain);

        assert!(stampeded_steps_load(c("{nope").as_ptr()).is_null());
        assert!(last_error().contains("JSON"));
    }
}
