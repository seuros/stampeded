use crate::mcp::{JobIdArgs, StampededServer};
use mcp_host::prelude::*;

impl StampededServer {
    /// Cancel a job that has not started.
    #[mcp_tool(
        name = "job_cancel",
        description = "Cancel a job that has not started executing",
        destructive = true,
        idempotent = true,
        visible = "crate::mcp::operator_may_write(ctx)"
    )]
    async fn job_cancel(&self, _ctx: Ctx<'_>, params: Parameters<JobIdArgs>) -> ToolResult {
        let id = params.0.uuid()?;
        let cancelled = self
            .queue()
            .cancel(&params.0.queue, id)
            .await
            .map_err(|e| ToolError::Internal(format!("cancel failed: {e}")))?;

        Ok(ToolOutput::text(if cancelled {
            format!("job {id} cancelled")
        } else {
            format!(
                "job {id} is not cancellable -- it is already executing or finished. \
                 An executing job is owned by its worker; cancelling the row would \
                 not stop the work."
            )
        }))
    }
}

pub fn mount(router: McpToolRouter<StampededServer>) -> McpToolRouter<StampededServer> {
    router.with_tool(
        StampededServer::job_cancel_tool_info(),
        StampededServer::job_cancel_handler,
        Some(StampededServer::job_cancel_visibility),
    )
}
