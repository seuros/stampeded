use crate::mcp::{JobIdArgs, StampededServer};
use mcp_host::prelude::*;

impl StampededServer {
    /// Make a discarded job claimable again.
    #[mcp_tool(
        name = "job_retry",
        description = "Reset a discarded job so it can be claimed again (resets attempt count)",
        idempotent = true,
        visible = "crate::mcp::operator_may_write(ctx)"
    )]
    async fn job_retry(&self, _ctx: Ctx<'_>, params: Parameters<JobIdArgs>) -> ToolResult {
        let id = params.0.uuid()?;
        let resurrected = self
            .queue()
            .resurrect(&params.0.queue, id)
            .await
            .map_err(|e| ToolError::Internal(format!("retry failed: {e}")))?;

        Ok(ToolOutput::text(if resurrected {
            format!("job {id} reset to available")
        } else {
            format!("job {id} was not in a retryable terminal state; nothing changed")
        }))
    }
}

pub fn mount(router: McpToolRouter<StampededServer>) -> McpToolRouter<StampededServer> {
    router.with_tool(
        StampededServer::job_retry_tool_info(),
        StampededServer::job_retry_handler,
        Some(StampededServer::job_retry_visibility),
    )
}
