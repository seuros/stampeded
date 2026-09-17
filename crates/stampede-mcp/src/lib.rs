//! MCP control plane.
//!
//! The differentiator (plans/00, plans/06). Nothing else in this space has a
//! first-class MCP interface, and debugging a job queue is inherently a
//! conversational task: "why does this keep failing", "what changed at 3am",
//! "retry everything that hit the payment API".
//!
//! Built on `mcp-host`, which shares `breaker-machines`, `chrono-machines` and
//! `throttle-machines` with the job core — so a tool call and a job execution
//! are protected by one set of primitives, not two (plans/02).
//!
//! **Read tools are unconditional; write tools are gated on an operator flag.**
//! `mcp-host` evaluates `visible` per session, so a read-only session does not
//! merely get refused — it never sees the destructive tools at all.

use mcp_host::prelude::*;
use mcp_host::registry::router::McpToolRouter;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stampede_core::Queue;
use std::sync::Arc;

mod tools;

/// The MCP server, holding a queue handle.
#[derive(Clone)]
pub struct StampedeServer {
    queue: Arc<Queue>,
    /// Whether this process exposes destructive tools at all. A deployment that
    /// only wants observability leaves this false and the write tools are not
    /// advertised.
    allow_writes: bool,
}

impl StampedeServer {
    pub fn new(queue: Queue) -> Self {
        Self { queue: Arc::new(queue), allow_writes: false }
    }

    /// Expose the guarded write tools (`job_retry`, `job_cancel`).
    pub fn with_writes(mut self) -> Self {
        self.allow_writes = true;
        self
    }

    pub fn queue(&self) -> &Queue {
        &self.queue
    }

    pub fn writes_allowed(&self) -> bool {
        self.allow_writes
    }
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

/// Build the tool router.
///
/// `allow_writes` mounts the destructive tools; they are additionally gated on
/// the session carrying an `operator` role. See `tools::mount`.
pub fn tool_router(allow_writes: bool) -> McpToolRouter<StampedeServer> {
    tools::mount(McpToolRouter::new(), allow_writes)
}

/// The full MCP router. Stampede exposes tools only — no prompts, resources or
/// templates yet, so those registries are empty.
pub fn router(allow_writes: bool) -> mcp_host::registry::router::McpRouter<StampedeServer> {
    mcp_host::registry::router::McpRouter::new(
        tool_router(allow_writes),
        Default::default(),
        Default::default(),
        Default::default(),
    )
}

/// Build a configured MCP server.
pub fn server(name: &str, version: &str) -> mcp_host::server::ServerBuilder {
    Server::builder(name, version)
        .with_instructions(
            "Stampede job queue control plane. Read tools describe queue depth, \
             individual jobs, failure patterns and worker liveness. Prefer \
             failure_summary over job_search when diagnosing: it clusters errors \
             so you see the pattern rather than one instance. Note that `attempt` \
             is incremented at CLAIM, so a job currently executing has already \
             spent the attempt it is using, and failure_kind 'interrupted' means \
             the worker died rather than the job raising.",
        )
        .with_tools(true)
}
