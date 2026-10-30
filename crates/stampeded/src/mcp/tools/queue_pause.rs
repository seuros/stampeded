use crate::mcp::{QueuePauseArgs, StampededServer};
use mcp_host::prelude::*;

impl StampededServer {
    /// Pause or resume handing out a queue's jobs.
    #[mcp_tool(
        name = "queue_pause",
        description = "Pause or resume a queue: no worker claims from a paused queue, \
                       running jobs finish, and new jobs still queue up",
        idempotent = true,
        visible = "crate::mcp::operator_may_write(ctx)"
    )]
    async fn queue_pause(&self, _ctx: Ctx<'_>, params: Parameters<QueuePauseArgs>) -> ToolResult {
        let QueuePauseArgs { queue, paused } = &params.0;
        let done = if *paused {
            self.queue().pause_queue(queue).await
        } else {
            self.queue().resume_queue(queue).await
        };
        done.map_err(|e| ToolError::Internal(format!("pause failed: {e}")))?;

        Ok(ToolOutput::text(format!(
            "queue {queue:?} {}",
            if *paused { "paused" } else { "resumed" }
        )))
    }
}

pub fn mount(router: McpToolRouter<StampededServer>) -> McpToolRouter<StampededServer> {
    router.with_tool(
        StampededServer::queue_pause_tool_info(),
        StampededServer::queue_pause_handler,
        Some(StampededServer::queue_pause_visibility),
    )
}
