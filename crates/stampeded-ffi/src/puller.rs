//! Pulling jobs, and settling them.

use crate::client::Client;
use crate::{STAMPEDED_ERROR, STAMPEDED_NONE, STAMPEDED_OK, arg, guarded, opt_arg};
use stampeded::{Lease, PullConfig, Puller, Queues};
use std::ffi::{CString, c_char};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

/// A registered worker whose jobs the caller runs.
pub struct PullerHandle {
    runtime: Arc<Runtime>,
    puller: Puller,
    worker_id: CString,
}

/// One pulled job. Its strings live as long as it does.
pub struct LeaseHandle {
    runtime: Arc<Runtime>,
    lease: Option<Lease>,
    id: CString,
    queue: CString,
    task: CString,
    args: CString,
    meta: CString,
    job: CString,
}

/// Settings for `stampeded_puller_start`. Zero a field for its default.
#[repr(C)]
pub struct PullerOptions {
    /// `"mail"`, `"default,mail"` or `"*"`. NULL for `"default"`.
    pub queues: *const c_char,
    /// With `"*"`: queues to leave alone, e.g. `"dlq"`. NULL for none.
    pub except_queues: *const c_char,
    /// Leases out at once. 0 for 10.
    pub max_leases: u32,
    /// Jobs claimed per round trip. 0 for 5.
    pub batch_size: u32,
    /// How often to look for work when there is none. 0 for 1000.
    pub poll_interval_ms: u32,
    /// How often the worker reports alive; keep well under the reaper's `dead_after`. 0 for 30000.
    pub heartbeat_interval_ms: u32,
}

/// Register a worker and start its heartbeat; `options` may be NULL. NULL on failure.
///
/// # Safety
///
/// `client` came from `stampeded_connect`; `options` is NULL or valid, with
/// NULL or NUL-terminated strings.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_puller_start(
    client: *const Client,
    options: *const PullerOptions,
) -> *mut PullerHandle {
    guarded(std::ptr::null_mut(), || {
        // SAFETY: the caller's contract, for every pointer below.
        let client = unsafe { client.as_ref() }.ok_or("client is NULL")?;
        let mut config = PullConfig::default();

        if let Some(options) = unsafe { options.as_ref() } {
            let spec = unsafe { opt_arg(options.queues, "queues") }?.unwrap_or("default");
            config.queues = Queues::parse(spec)?;
            if let Some(except) = unsafe { opt_arg(options.except_queues, "except_queues") }? {
                config.queues = config.queues.except(except)?;
            }
            if options.max_leases > 0 {
                config.max_leases = options.max_leases as usize;
            }
            if options.batch_size > 0 {
                config.batch_size = options.batch_size as usize;
            }
            if options.poll_interval_ms > 0 {
                config.poll_interval = Duration::from_millis(options.poll_interval_ms.into());
            }
            if options.heartbeat_interval_ms > 0 {
                config.heartbeat_interval =
                    Duration::from_millis(options.heartbeat_interval_ms.into());
            }
        }

        let puller = client
            .runtime
            .block_on(Puller::start(client.queue.clone(), config))
            .map_err(|e| e.to_string())?;
        let worker_id =
            CString::new(puller.worker_id().hyphenated().to_string()).map_err(|e| e.to_string())?;
        Ok(Box::into_raw(Box::new(PullerHandle {
            runtime: client.runtime.clone(),
            puller,
            worker_id,
        })))
    })
}

/// Wait up to `timeout_ms` for a job; safe from many threads. STAMPEDED_OK sets `*lease_out`;
/// STAMPEDED_NONE when no job came, every lease is out, or the puller is stopping.
///
/// # Safety
///
/// `puller` came from `stampeded_puller_start`; `lease_out` points to a
/// writable `stampeded_lease *`.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_puller_next(
    puller: *const PullerHandle,
    timeout_ms: u32,
    lease_out: *mut *mut LeaseHandle,
) -> i32 {
    guarded(STAMPEDED_ERROR, || {
        // SAFETY: the caller's contract.
        let handle = unsafe { puller.as_ref() }.ok_or("puller is NULL")?;
        if lease_out.is_null() {
            return Err("lease_out is NULL".into());
        }

        let next = handle
            .runtime
            .block_on(handle.puller.next(Duration::from_millis(timeout_ms.into())))
            .map_err(|e| e.to_string())?;
        let Some(lease) = next else {
            return Ok(STAMPEDED_NONE);
        };

        let job = lease.job();
        let text = |s: String| CString::new(s).map_err(|e| e.to_string());
        let leased = LeaseHandle {
            id: text(job.id.hyphenated().to_string())?,
            queue: text(job.queue.clone())?,
            task: text(job.task.clone())?,
            args: text(job.args.to_string())?,
            meta: text(job.meta.to_string())?,
            job: text(serde_json::to_string(job).map_err(|e| e.to_string())?)?,
            runtime: handle.runtime.clone(),
            lease: Some(lease),
        };
        // SAFETY: lease_out is non-null and writable by the caller's contract.
        unsafe { *lease_out = Box::into_raw(Box::new(leased)) };
        Ok(STAMPEDED_OK)
    })
}

/// Stop handing out jobs, give unstarted ones back, and wait up to `drain_ms` for leases still out.
/// The puller still needs `stampeded_puller_free`.
///
/// # Safety
///
/// `puller` came from `stampeded_puller_start`.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_puller_stop(puller: *const PullerHandle, drain_ms: u32) -> i32 {
    guarded(STAMPEDED_ERROR, || {
        // SAFETY: the caller's contract.
        let handle = unsafe { puller.as_ref() }.ok_or("puller is NULL")?;
        handle
            .runtime
            .block_on(handle.puller.stop(Duration::from_millis(drain_ms.into())))
            .map_err(|e| e.to_string())?;
        Ok(STAMPEDED_OK)
    })
}

/// This puller's `stampeded_workers` id, for SQL `stampeded_checkpoint`/`stampeded_touch` calls.
/// Valid until the puller is freed.
///
/// # Safety
///
/// `puller` came from `stampeded_puller_start`, or is NULL.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_puller_worker_id(puller: *const PullerHandle) -> *const c_char {
    // SAFETY: the caller's contract.
    unsafe { puller.as_ref() }.map_or(std::ptr::null(), |p| p.worker_id.as_ptr())
}

/// Free a puller; without `stampeded_puller_stop` first, its worker and leases go to the reaper.
///
/// # Safety
///
/// `puller` came from `stampeded_puller_start`, or is NULL, and is not used
/// again; no other thread is inside a call on it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_puller_free(puller: *mut PullerHandle) {
    if !puller.is_null() {
        // SAFETY: the caller hands back the pointer stampeded_puller_start returned.
        drop(unsafe { Box::from_raw(puller) });
    }
}

/// The job's id. Every lease string is valid until the lease is settled or freed.
///
/// # Safety
///
/// `lease` came from `stampeded_puller_next` and is not yet settled or freed.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_id(lease: *const LeaseHandle) -> *const c_char {
    // SAFETY: the caller's contract.
    unsafe { lease.as_ref() }.map_or(std::ptr::null(), |l| l.id.as_ptr())
}

/// The job's queue.
///
/// # Safety
///
/// As `stampeded_lease_id`.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_queue(lease: *const LeaseHandle) -> *const c_char {
    // SAFETY: the caller's contract.
    unsafe { lease.as_ref() }.map_or(std::ptr::null(), |l| l.queue.as_ptr())
}

/// The job's task.
///
/// # Safety
///
/// As `stampeded_lease_id`.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_task(lease: *const LeaseHandle) -> *const c_char {
    // SAFETY: the caller's contract.
    unsafe { lease.as_ref() }.map_or(std::ptr::null(), |l| l.task.as_ptr())
}

/// The job's arguments, a JSON object.
///
/// # Safety
///
/// As `stampeded_lease_id`.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_args(lease: *const LeaseHandle) -> *const c_char {
    // SAFETY: the caller's contract.
    unsafe { lease.as_ref() }.map_or(std::ptr::null(), |l| l.args.as_ptr())
}

/// The job's metadata, a JSON object.
///
/// # Safety
///
/// As `stampeded_lease_id`.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_meta(lease: *const LeaseHandle) -> *const c_char {
    // SAFETY: the caller's contract.
    unsafe { lease.as_ref() }.map_or(std::ptr::null(), |l| l.meta.as_ptr())
}

/// The whole job row as a JSON object: every column, timestamps in RFC 3339.
///
/// # Safety
///
/// As `stampeded_lease_id`.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_job(lease: *const LeaseHandle) -> *const c_char {
    // SAFETY: the caller's contract.
    unsafe { lease.as_ref() }.map_or(std::ptr::null(), |l| l.job.as_ptr())
}

/// This attempt's number, from 1. 0 for a NULL lease.
///
/// # Safety
///
/// As `stampeded_lease_id`.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_attempt(lease: *const LeaseHandle) -> i32 {
    // SAFETY: the caller's contract.
    unsafe { lease.as_ref() }
        .and_then(|l| l.lease.as_ref())
        .map_or(0, |l| i32::from(l.job().attempt))
}

/// Save `state_json`, resume state the next attempt reads back from the job's checkpoint.
/// STAMPEDED_NONE when the job is no longer this worker's: stop working on it.
///
/// # Safety
///
/// As `stampeded_lease_id`; `state_json` is NUL-terminated.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_checkpoint(
    lease: *const LeaseHandle,
    state_json: *const c_char,
) -> i32 {
    guarded(STAMPEDED_ERROR, || {
        // SAFETY: the caller's contract.
        let handle = unsafe { lease.as_ref() }.ok_or("lease is NULL")?;
        let state: serde_json::Value =
            serde_json::from_str(unsafe { arg(state_json, "state_json") }?)
                .map_err(|e| format!("state_json is not valid JSON: {e}"))?;
        let lease = handle.lease.as_ref().ok_or("lease is already settled")?;

        let still_ours = handle
            .runtime
            .block_on(lease.context().checkpoint(&state))
            .map_err(|e| e.to_string())?;
        Ok(if still_ours {
            STAMPEDED_OK
        } else {
            STAMPEDED_NONE
        })
    })
}

/// The job succeeded. Settles and frees the lease, whatever it returns.
///
/// # Safety
///
/// `lease` came from `stampeded_puller_next` and is not used again.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_complete(lease: *mut LeaseHandle) -> i32 {
    // SAFETY: the caller's contract.
    unsafe { settle(lease, |runtime, lease| runtime.block_on(lease.complete())) }
}

/// The job failed: retried after `retry_in_ms`, or on its backoff if negative, while attempts last.
/// Settles and frees the lease, whatever it returns.
///
/// # Safety
///
/// `lease` came from `stampeded_puller_next` and is not used again; `message` is NUL-terminated.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_fail(
    lease: *mut LeaseHandle,
    message: *const c_char,
    retry_in_ms: i64,
) -> i32 {
    let retry_in = u64::try_from(retry_in_ms).ok().map(Duration::from_millis);
    // SAFETY: the caller's contract.
    unsafe {
        settle_saying(lease, message, |rt, lease, m| {
            rt.block_on(lease.fail(m, retry_in))
        })
    }
}

/// Not a failure: back in the queue with the attempt refunded, to run again from its checkpoint.
/// Settles and frees the lease, whatever it returns.
///
/// # Safety
///
/// `lease` came from `stampeded_puller_next` and is not used again.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_requeue(lease: *mut LeaseHandle) -> i32 {
    // SAFETY: the caller's contract.
    unsafe { settle(lease, |runtime, lease| runtime.block_on(lease.requeue())) }
}

/// The job ran past the caller's time limit: retried on its backoff, recorded as `timed_out`.
/// Settles and frees the lease, whatever it returns.
///
/// # Safety
///
/// As `stampeded_lease_fail`.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_timed_out(
    lease: *mut LeaseHandle,
    message: *const c_char,
) -> i32 {
    // SAFETY: the caller's contract.
    unsafe {
        settle_saying(lease, message, |rt, lease, m| {
            rt.block_on(lease.time_out(m))
        })
    }
}

/// The job can never succeed: discard it whatever attempts remain.
/// Settles and frees the lease, whatever it returns.
///
/// # Safety
///
/// As `stampeded_lease_fail`.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_discard(
    lease: *mut LeaseHandle,
    message: *const c_char,
) -> i32 {
    // SAFETY: the caller's contract.
    unsafe { settle_saying(lease, message, |rt, lease, m| rt.block_on(lease.discard(m))) }
}

/// Give a lease back without an outcome: the job returns to the queue as an interruption.
///
/// # Safety
///
/// `lease` came from `stampeded_puller_next`, or is NULL, and is not used again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_lease_free(lease: *mut LeaseHandle) {
    if lease.is_null() {
        return;
    }
    // SAFETY: the caller hands back the pointer stampeded_puller_next returned.
    let handle = unsafe { Box::from_raw(lease) };
    // An unsettled lease reports itself from its Drop, which needs the runtime.
    let _entered = handle.runtime.enter();
    drop(handle.lease);
}

/// As [`settle`], with the caller's message; an unreadable one settles under a placeholder.
///
/// # Safety
///
/// As [`settle`]; `message` is NULL or NUL-terminated.
unsafe fn settle_saying(
    lease: *mut LeaseHandle,
    message: *const c_char,
    report: impl FnOnce(&Runtime, Lease, &str) -> stampeded::Result<()>,
) -> i32 {
    // SAFETY: forwarded from the caller.
    let message = unsafe { arg(message, "message") }.map_or_else(
        |e| format!("(message unreadable: {e})"),
        ToString::to_string,
    );
    // SAFETY: forwarded from the caller.
    unsafe { settle(lease, |runtime, lease| report(runtime, lease, &message)) }
}

/// Take ownership of a lease, report it with `report`, free it.
///
/// # Safety
///
/// `lease` came from `stampeded_puller_next` and is not used again.
unsafe fn settle(
    lease: *mut LeaseHandle,
    report: impl FnOnce(&Runtime, Lease) -> stampeded::Result<()>,
) -> i32 {
    guarded(STAMPEDED_ERROR, || {
        if lease.is_null() {
            return Err("lease is NULL".into());
        }
        // SAFETY: the caller hands back the pointer stampeded_puller_next returned, once.
        let mut handle = unsafe { Box::from_raw(lease) };
        let lease = handle.lease.take().ok_or("lease is already settled")?;
        report(&handle.runtime, lease).map_err(|e| e.to_string())?;
        Ok(STAMPEDED_OK)
    })
}
