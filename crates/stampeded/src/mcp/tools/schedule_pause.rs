use crate::mcp::{ScheduleKeyArgs, StampededServer};
use mcp_host::prelude::*;

impl StampededServer {
    /// Pause or resume a schedule; the row and its occurrence ledger stay.
    #[mcp_tool(
        name = "schedule_pause",
        description = "Pause or resume a schedule by key without deleting it",
        idempotent = true,
        visible = "crate::mcp::operator_may_write(ctx)"
    )]
    async fn schedule_pause(
        &self,
        _ctx: Ctx<'_>,
        params: Parameters<ScheduleKeyArgs>,
    ) -> ToolResult {
        let n = sqlx::query(
            "UPDATE stampeded_schedules SET paused = $2, updated_at = now() WHERE key = $1",
        )
        .bind(&params.0.key)
        .bind(params.0.paused)
        .execute(self.queue().pool())
        .await
        .map_err(|e| ToolError::Internal(format!("pause failed: {e}")))?
        .rows_affected();

        Ok(ToolOutput::text(if n == 1 {
            format!(
                "schedule {:?} {}",
                params.0.key,
                if params.0.paused { "paused" } else { "resumed" }
            )
        } else {
            format!("no schedule named {:?}", params.0.key)
        }))
    }
}

pub fn mount(router: McpToolRouter<StampededServer>) -> McpToolRouter<StampededServer> {
    router.with_tool(
        StampededServer::schedule_pause_tool_info(),
        StampededServer::schedule_pause_handler,
        Some(StampededServer::schedule_pause_visibility),
    )
}
