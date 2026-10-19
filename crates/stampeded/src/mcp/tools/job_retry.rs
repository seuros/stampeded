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
        let id = uuid::Uuid::parse_str(&params.0.id)
            .map_err(|_| ToolError::InvalidArguments(format!("{:?} is not a UUID", params.0.id)))?;

        let n = sqlx::query(
            "UPDATE stampeded_jobs \
                SET state = 'available', attempt = 0, failure_kind = NULL, \
                    last_error = NULL, finished_at = NULL, scheduled_at = now() \
              WHERE queue = $1 AND id = $2 AND state IN ('discarded','cancelled')",
        )
        .bind(&params.0.queue)
        .bind(id)
        .execute(self.queue().pool())
        .await
        .map_err(|e| ToolError::Internal(format!("retry failed: {e}")))?
        .rows_affected();

        Ok(ToolOutput::text(if n == 1 {
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
