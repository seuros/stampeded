use crate::mcp::{SearchArgs, StampededServer};
use mcp_host::prelude::*;
use std::fmt::Write as _;

impl StampededServer {
    /// Search jobs.
    #[mcp_tool(
        name = "job_search",
        description = "Find jobs by task, state or error substring. Use failure_summary first when diagnosing a pattern",
        read_only = true,
        idempotent = true
    )]
    async fn job_search(&self, _ctx: Ctx<'_>, params: Parameters<SearchArgs>) -> ToolResult {
        let a = params.0;
        let limit = a.limit.unwrap_or(20).clamp(1, 100);

        let rows: Vec<(uuid::Uuid, String, String, i16, Option<String>)> = sqlx::query_as(
            "SELECT id, task, state::text, attempt, left(last_error, 120) \
               FROM stampeded_jobs \
              WHERE queue = $1 \
                AND ($2::text IS NULL OR task = $2) \
                AND ($3::text IS NULL OR state::text = $3) \
                AND ($4::text IS NULL OR last_error ILIKE '%' || $4 || '%') \
              ORDER BY inserted_at DESC LIMIT $5",
        )
        .bind(&a.queue)
        .bind(&a.task)
        .bind(&a.state)
        .bind(&a.error_contains)
        .bind(limit)
        .fetch_all(self.queue().pool())
        .await
        .map_err(|e| ToolError::Internal(format!("search failed: {e}")))?;

        if rows.is_empty() {
            return Ok(ToolOutput::text("no matching jobs"));
        }

        let mut out = format!("{} job(s)\n", rows.len());
        for (id, task, state, attempt, err) in rows {
            let _ = write!(out, "  {id}  {task:<20} {state:<10} attempt {attempt}");
            if let Some(e) = err {
                let _ = write!(out, "  {e}");
            }
            out.push('\n');
        }
        Ok(ToolOutput::text(out))
    }
}

pub fn mount(router: McpToolRouter<StampededServer>) -> McpToolRouter<StampededServer> {
    router.with_tool(
        StampededServer::job_search_tool_info(),
        StampededServer::job_search_handler,
        None,
    )
}
