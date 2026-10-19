//! Connecting, and enqueueing.

use crate::{STAMPEDED_ERROR, STAMPEDED_NONE, STAMPEDED_OK, arg, guarded, opt_arg};
use stampeded::{NewJob, Queue};
use std::ffi::c_char;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// A connection pool, and the runtime every call on it blocks on.
pub struct Client {
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) queue: Queue,
}

/// Optional settings for `stampeded_enqueue`. Zero every field you do not set.
#[repr(C)]
pub struct EnqueueOptions {
    /// Runtime context as a JSON object, or NULL for `{}`.
    pub meta_json: *const c_char,
    /// Refuses a second job with this scope in the queue until the first finishes. NULL for none.
    pub unique: *const c_char,
    pub priority: i16,
    /// 0 for the default, 25.
    pub max_attempts: i16,
    /// When to run, in Unix milliseconds by the caller's clock. 0 for now.
    pub scheduled_at_unix_ms: i64,
}

/// Connect to a Postgres URL (its `options` may set a `search_path`); NULL on failure.
/// Tries a connection first: a bad host, login or database fails at once, a silent host after 30 s.
///
/// # Safety
///
/// `database_url` is a NUL-terminated string.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_connect(database_url: *const c_char) -> *mut Client {
    guarded(std::ptr::null_mut(), || {
        // SAFETY: the caller's contract.
        let url = unsafe { arg(database_url, "database_url") }?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("stampeded")
            .enable_all()
            .build()
            .map_err(|e| format!("could not start a runtime: {e}"))?;
        let pool = runtime
            .block_on(stampeded::connect(
                sqlx::postgres::PgPoolOptions::new().max_connections(5),
                url,
            ))
            .map_err(|e| format!("could not connect: {e}"))?;

        Ok(Box::into_raw(Box::new(Client {
            runtime: Arc::new(runtime),
            queue: Queue::new(pool),
        })))
    })
}

/// Free a client. Pullers started from it keep working until freed.
///
/// # Safety
///
/// `client` came from `stampeded_connect`, or is NULL, and is not used again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_client_free(client: *mut Client) {
    if !client.is_null() {
        // SAFETY: the caller hands back the pointer stampeded_connect returned.
        drop(unsafe { Box::from_raw(client) });
    }
}

/// Enqueue a job: `args_json` is a JSON object of named arguments, `options` may be NULL.
/// STAMPEDED_NONE when its `unique` scope already waits; else a non-NULL `id_out` gets the id.
///
/// # Safety
///
/// `client` came from `stampeded_connect`; the strings are NUL-terminated;
/// `options` is NULL or valid; `id_out` is NULL or has room for 37 bytes.
#[must_use]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stampeded_enqueue(
    client: *const Client,
    queue: *const c_char,
    task: *const c_char,
    args_json: *const c_char,
    options: *const EnqueueOptions,
    id_out: *mut c_char,
) -> i32 {
    guarded(STAMPEDED_ERROR, || {
        // SAFETY: the caller's contract, for every pointer below.
        let client = unsafe { client.as_ref() }.ok_or("client is NULL")?;
        let queue = unsafe { arg(queue, "queue") }?;
        let task = unsafe { arg(task, "task") }?;
        let args = json(unsafe { arg(args_json, "args_json") }?, "args_json")?;

        let mut job = NewJob::new(queue, task).args(args);
        if let Some(options) = unsafe { options.as_ref() } {
            if let Some(meta) = unsafe { opt_arg(options.meta_json, "meta_json") }? {
                job = job.meta(json(meta, "meta_json")?);
            }
            if let Some(unique) = unsafe { opt_arg(options.unique, "unique") }? {
                job = job.unique(unique);
            }
            job = job.priority(options.priority);
            if options.max_attempts > 0 {
                job = job.max_attempts(options.max_attempts);
            }
            if options.scheduled_at_unix_ms != 0 {
                let at = jiff::Timestamp::from_millisecond(options.scheduled_at_unix_ms)
                    .map_err(|e| format!("scheduled_at_unix_ms: {e}"))?;
                job = job.scheduled_at(at);
            }
        }

        let Some(id) = client
            .runtime
            .block_on(client.queue.enqueue(job))
            .map_err(|e| e.to_string())?
        else {
            return Ok(STAMPEDED_NONE);
        };

        if !id_out.is_null() {
            let id = id.hyphenated().to_string();
            // SAFETY: id_out holds 37 bytes by contract; a hyphenated UUID is 36 ASCII bytes.
            unsafe {
                std::ptr::copy_nonoverlapping(id.as_ptr(), id_out.cast::<u8>(), id.len());
                *id_out.add(id.len()) = 0;
            }
        }
        Ok(STAMPEDED_OK)
    })
}

fn json(text: &str, name: &str) -> Result<serde_json::Value, String> {
    serde_json::from_str(text).map_err(|e| format!("{name} is not valid JSON: {e}"))
}
