//! stampeded's C ABI: enqueue and pull jobs from any language with a C FFI.
//! Calls block the calling thread; nothing calls back into the host, and no panic crosses.

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char};
use std::io::IsTerminal;
use std::panic::{AssertUnwindSafe, catch_unwind};

#[allow(
    unsafe_code,
    reason = "C ABI exports: they dereference caller pointers"
)]
mod client;
#[allow(
    unsafe_code,
    reason = "C ABI exports: they dereference caller pointers"
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "flags borrowed handles and leases already moved out; each call holds them exactly as long as it runs"
)]
mod puller;

#[allow(
    unsafe_code,
    reason = "C ABI exports: they dereference caller pointers"
)]
mod cron;
#[allow(
    unsafe_code,
    reason = "C ABI exports: they dereference caller pointers"
)]
mod lifecycle;
#[allow(
    unsafe_code,
    reason = "C ABI exports: they dereference caller pointers"
)]
mod steps;

pub use client::{Client, EnqueueOptions};
pub use puller::{LeaseHandle, PullerHandle, PullerOptions};
pub use steps::Steps;

/// The call did what was asked.
pub const STAMPEDED_OK: i32 = 0;
/// Nothing to do: no job came, the job was already queued, or the lease is no longer ours.
pub const STAMPEDED_NONE: i32 = 1;
/// The call failed; `stampeded_last_error` says why.
pub const STAMPEDED_ERROR: i32 = -1;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_error(message: &str) {
    let message = CString::new(message.replace('\0', " ")).unwrap_or_default();
    LAST_ERROR.with(|e| *e.borrow_mut() = Some(message));
}

/// Runs one ABI call: clears this thread's error, and turns an `Err` or a panic into `fallback`.
fn guarded<T>(fallback: T, call: impl FnOnce() -> Result<T, String>) -> T {
    LAST_ERROR.with(|e| *e.borrow_mut() = None);
    match catch_unwind(AssertUnwindSafe(call)) {
        Ok(Ok(value)) => value,
        Ok(Err(message)) => {
            set_error(&message);
            fallback
        }
        Err(panic) => {
            let what = panic
                .downcast_ref::<&str>()
                .map(ToString::to_string)
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown".into());
            set_error(&format!("stampeded panicked: {what}"));
            fallback
        }
    }
}

/// A required C string argument, as UTF-8.
///
/// # Safety
///
/// `ptr` is NULL or points to a NUL-terminated string that outlives the call.
#[allow(unsafe_code, reason = "reads a caller's C string")]
unsafe fn arg<'a>(ptr: *const c_char, name: &str) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err(format!("{name} is NULL"));
    }
    // SAFETY: non-null, and the caller guarantees NUL termination and lifetime.
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|_| format!("{name} is not UTF-8"))
}

/// An optional C string argument: NULL is `None`.
///
/// # Safety
///
/// As [`arg`].
#[allow(unsafe_code, reason = "reads a caller's C string")]
unsafe fn opt_arg<'a>(ptr: *const c_char, name: &str) -> Result<Option<&'a str>, String> {
    if ptr.is_null() {
        Ok(None)
    } else {
        // SAFETY: forwarded from the caller.
        unsafe { arg(ptr, name) }.map(Some)
    }
}

/// This library's version, e.g. `"0.1.0"`. Static; never freed.
#[must_use]
#[allow(unsafe_code, reason = "exported under its C name")]
#[unsafe(no_mangle)]
pub extern "C" fn stampeded_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr().cast()
}

/// Log to stderr at `filter`, e.g. `"stampeded=debug"`, NULL for `"warn"`; silent until called.
/// STAMPEDED_NONE when this process already has a logger.
///
/// # Safety
///
/// `filter` is NULL or NUL-terminated.
#[must_use]
#[allow(
    unsafe_code,
    reason = "exported under its C name; reads a caller's C string"
)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_log_init(filter: *const c_char) -> i32 {
    guarded(STAMPEDED_ERROR, || {
        // SAFETY: the caller's contract.
        let spec = unsafe { opt_arg(filter, "filter") }?.unwrap_or("warn");
        let filter = tracing_subscriber::EnvFilter::try_new(spec)
            .map_err(|e| format!("log filter {spec:?}: {e}"))?;
        let installed = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .with_ansi(std::io::stderr().is_terminal())
            .try_init();
        Ok(if installed.is_ok() {
            STAMPEDED_OK
        } else {
            STAMPEDED_NONE
        })
    })
}

/// Why this thread's last call failed, or NULL; valid until the thread's next call.
#[must_use]
#[allow(unsafe_code, reason = "exported under its C name")]
#[unsafe(no_mangle)]
pub extern "C" fn stampeded_last_error() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ref().map_or(std::ptr::null(), |m| m.as_ptr()))
}
