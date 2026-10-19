//! Cron schedules: the daemon's own parser, for writers in other runtimes.

use crate::{STAMPEDED_ERROR, STAMPEDED_OK, arg, guarded};
use stampeded::{Cron, DstPolicy};
use std::ffi::{CString, c_char};
use std::sync::LazyLock;

static POLICIES: LazyLock<[(DstPolicy, CString); 2]> = LazyLock::new(|| {
    [DstPolicy::WallClock, DstPolicy::Instant].map(|p| {
        (
            p,
            CString::new(p.as_str()).expect("policy names have no NUL"),
        )
    })
});

/// Check a cron expression and its timezone as the daemon will read them. STAMPEDED_OK with the
/// DST policy to use when none is given in `*dst_policy_out` (static; may be NULL), or
/// STAMPEDED_ERROR with the reason.
///
/// # Safety
///
/// `expr` and `timezone` are NUL-terminated; `dst_policy_out` is NULL or writable.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_cron_check(
    expr: *const c_char,
    timezone: *const c_char,
    dst_policy_out: *mut *const c_char,
) -> i32 {
    guarded(STAMPEDED_ERROR, || {
        // SAFETY: the caller's contract.
        let expr = unsafe { arg(expr, "expr") }?;
        // SAFETY: the caller's contract.
        let timezone = unsafe { arg(timezone, "timezone") }?;
        let policy = Cron::check(expr, timezone)?;
        if !dst_policy_out.is_null() {
            let name = POLICIES
                .iter()
                .find(|(p, _)| *p == policy)
                .map(|(_, n)| n.as_ptr());
            // SAFETY: dst_policy_out is non-null and writable by the caller's contract.
            unsafe { *dst_policy_out = name.unwrap_or(std::ptr::null()) };
        }
        Ok(STAMPEDED_OK)
    })
}
