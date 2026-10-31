//! Typed jobs: the struct is the payload, `run` is the handler, `TASK` names it.

use crate::error::{Error, Result};
use crate::job::NewJob;
use crate::worker::JobContext;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;

/// What `run` fails with. `RetryIn` picks the delay; a `stampeded::Error` keeps its kind.
pub type JobError = Box<dyn std::error::Error + Send + Sync>;

pub type JobResult<T> = std::result::Result<T, JobError>;

/// Stored as `args`, so the payload must serialize to a JSON object (named fields).
/// A payload that no longer decodes is discarded as unrecoverable, not retried.
pub trait AsyncJob: Serialize + DeserializeOwned + Send + Sync + 'static {
    const TASK: &'static str;

    const QUEUE: &'static str = "default";

    /// Logged at debug, never stored: the schema keeps no results.
    type Output: Serialize + Send;

    fn run(&self, ctx: &JobContext) -> impl Future<Output = JobResult<Self::Output>> + Send;

    /// Override to add dedup, priority or a destination; start from `default_new_job`.
    fn new_job(&self) -> Result<NewJob> {
        default_new_job(self)
    }
}

pub fn default_new_job<T: AsyncJob>(job: &T) -> Result<NewJob> {
    let args = serde_json::to_value(job)?;
    if !args.is_object() {
        // `NewJob::args` stores null as `{}`, which would never decode back.
        return Err(Error::Serde(serde::ser::Error::custom(format!(
            "task {:?}: the payload must serialize to a JSON object, not {}",
            T::TASK,
            json_type(&args)
        ))));
    }
    Ok(NewJob::new(T::QUEUE, T::TASK).args(args))
}

fn json_type(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

pub(crate) async fn run_typed<T: AsyncJob>(ctx: JobContext) -> JobResult<()> {
    // One `?`, one box: a `Box<Error>` inside the `dyn` would not downcast to `Error::Serde`.
    let job: T = ctx.job.payload()?;
    let output = job.run(&ctx).await?;

    match serde_json::to_value(&output) {
        Ok(output) => tracing::debug!(
            job_id = %ctx.job.id, task = T::TASK, %output, "typed job finished"
        ),
        // The work is done; do not redo it over an unprintable result.
        Err(e) => tracing::warn!(
            job_id = %ctx.job.id, task = T::TASK, error = %e,
            "typed job finished, but its output does not serialize"
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
