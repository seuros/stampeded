//! MCP control plane.
//!
//! Read tools are unconditional; write tools are invisible to non-operator sessions.

use crate::Queue;
use mcp_host::prelude::*;
use mcp_host::registry::router::{
    McpPromptRouter, McpResourceRouter, McpResourceTemplateRouter, McpRouter,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

mod tools;

/// The MCP server, holding a queue handle.
#[derive(Clone)]
pub struct StampededServer {
    queue: Arc<Queue>,
}

impl StampededServer {
    pub fn new(queue: Queue) -> Self {
        Self {
            queue: Arc::new(queue),
        }
    }

    pub fn queue(&self) -> &Queue {
        &self.queue
    }
}

/// What the process allows, as visibility predicates see it.
pub struct StampededEnvironment {
    writes: bool,
    cwd: PathBuf,
}

impl StampededEnvironment {
    pub fn new(writes: bool) -> Self {
        Self {
            writes,
            cwd: std::env::current_dir().unwrap_or_default(),
        }
    }
}

impl Environment for StampededEnvironment {
    fn cwd(&self) -> &Path {
        &self.cwd
    }

    fn has_custom(&self, key: &str) -> bool {
        key == "writes" && self.writes
    }
}

/// Write tools: a process that allows writes, and a session with the `operator` role.
pub fn operator_may_write(ctx: &VisibilityContext<'_>) -> bool {
    ctx.has_role("operator") && ctx.environment.is_some_and(|e| e.has_custom("writes"))
}

/// Arguments shared by the search-shaped tools.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct QueueArgs {
    /// Queue name.
    pub queue: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct JobIdArgs {
    /// Queue the job belongs to (jobs are partitioned by queue).
    pub queue: String,
    /// Job UUID.
    pub id: String,
}

impl JobIdArgs {
    /// The id, or the caller's error naming what was passed.
    pub fn uuid(&self) -> Result<uuid::Uuid, ToolError> {
        uuid::Uuid::parse_str(&self.id)
            .map_err(|_| ToolError::InvalidArguments(format!("{:?} is not a UUID", self.id)))
    }
}

/// No parameters; the tool contract still takes an object.
#[derive(Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct EmptyArgs {}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ScheduleUpsertArgs {
    /// Stable identifier. Upserting the same key edits the schedule in place.
    pub key: String,
    /// Queue the fired jobs land in.
    pub queue: String,
    /// Task name to enqueue.
    pub task: String,
    /// Five-field cron expression: minute hour day-of-month month day-of-week.
    /// Mutually exclusive with `every_secs`.
    #[serde(default)]
    pub cron: Option<String>,
    /// Fixed interval in seconds. Mutually exclusive with `cron`.
    #[serde(default)]
    pub every_secs: Option<u64>,
    /// IANA timezone for a cron schedule (default UTC).
    #[serde(default)]
    pub timezone: Option<String>,
    /// "wall_clock" fires once per local time (a daily report runs once on a
    /// fall-back day); "instant" fires per real instant (so it runs twice).
    /// Omit to let the expression decide.
    #[serde(default)]
    pub dst_policy: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ScheduleKeyArgs {
    /// Schedule key.
    pub key: String,
    /// True to pause, false to resume.
    #[serde(default)]
    pub paused: bool,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SearchArgs {
    /// Queue to search.
    pub queue: String,
    /// Optional task name filter.
    #[serde(default)]
    pub task: Option<String>,
    /// Optional state filter (available, executing, retryable, discarded, ...).
    #[serde(default)]
    pub state: Option<String>,
    /// Substring to match against the recorded error text.
    #[serde(default)]
    pub error_contains: Option<String>,
    /// Max rows (default 20, capped at 100).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Every tool; the write tools are hidden, and refused, unless [`operator_may_write`].
pub fn router() -> McpRouter<StampededServer> {
    McpRouter::new(
        tools::router(),
        McpPromptRouter::new(),
        McpResourceRouter::new(),
        McpResourceTemplateRouter::new(),
    )
}

/// A configured MCP server; `allow_writes` lets operator sessions see the write tools.
pub fn server(name: &str, version: &str, allow_writes: bool) -> mcp_host::server::ServerBuilder {
    Server::builder(name, version)
        .with_instructions(
            "Stampeded job queue control plane. Read tools describe queue depth, \
             individual jobs, failure patterns and worker liveness. Prefer \
             failure_summary over job_search when diagnosing: it clusters errors \
             so you see the pattern rather than one instance. Note that `attempt` \
             is incremented at CLAIM, so a job currently executing has already \
             spent the attempt it is using, and failure_kind 'interrupted' means \
             the worker died rather than the job raising.",
        )
        .with_tools(true)
        .with_environment(StampededEnvironment::new(allow_writes))
        .with_circuit_breaker(ToolBreakerConfig {
            failure_threshold: 5,
            failure_window_secs: 60.0,
            half_open_timeout_secs: 15.0,
            success_threshold: 1,
            call_timeout_secs: 30.0,
        })
        .with_runtime_config(ServerRuntimeConfig {
            notification_queue_capacity: 256,
            client_request_queue_capacity: 32,
            max_in_flight_requests: 16,
            transport_write_timeout: Duration::from_secs(5),
            transport_shutdown_timeout: Duration::from_secs(2),
            response_drain_timeout: Duration::from_secs(2),
            max_client_request_timeout: Duration::from_mins(1),
            max_task_ttl: Duration::from_mins(10),
        })
}

#[cfg(test)]
mod tests;
