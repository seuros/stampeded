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
        let id = uuid::Uuid::parse_str(&params.0.id)
            .map_err(|_| ToolError::InvalidArguments(format!("{:?} is not a UUID", params.0.id)))?;

        // Never an executing job: the worker owns that row.
        let n = sqlx::query(
            "UPDATE stampeded_jobs \
                SET state = 'cancelled', failure_kind = 'cancelled', \
                    finished_at = now(), dedup_scope = NULL \
              WHERE queue = $1 AND id = $2 AND state IN ('available','retryable')",
        )
        .bind(&params.0.queue)
        .bind(id)
        .execute(self.queue().pool())
        .await
        .map_err(|e| ToolError::Internal(format!("cancel failed: {e}")))?
        .rows_affected();

        Ok(ToolOutput::text(if n == 1 {
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
