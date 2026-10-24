use crate::mcp::{QueueArgs, StampededServer};
use mcp_host::prelude::*;
// `let _ = write!`/`writeln!` into a String below: fmt::Write on String never errors.
use std::fmt::Write as _;

impl StampededServer {
    /// Queue depth by state.
    #[mcp_tool(
        name = "queue_status",
        description = "Queue depth by state: how many jobs are available, executing, retryable or discarded",
        read_only = true,
        idempotent = true
    )]
    async fn queue_status(&self, _ctx: Ctx<'_>, params: Parameters<QueueArgs>) -> ToolResult {
        let rows = self
            .queue()
            .depth(&params.0.queue)
            .await
            .map_err(|e| ToolError::Internal(format!("depth query failed: {e}")))?;

        if rows.is_empty() {
            return Ok(ToolOutput::text(format!(
                "queue {:?} has no unfinished jobs",
                params.0.queue
            )));
        }

        let mut out = format!("queue {:?}\n", params.0.queue);
        for (state, n) in rows {
            let _ = writeln!(out, "  {state:<12} {n}");
        }
        Ok(ToolOutput::text(out))
    }
}

pub fn mount(router: McpToolRouter<StampededServer>) -> McpToolRouter<StampededServer> {
    router.with_tool(
        StampededServer::queue_status_tool_info(),
        StampededServer::queue_status_handler,
        None,
    )
}
