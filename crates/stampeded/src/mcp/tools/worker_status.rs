use crate::mcp::{EmptyArgs, StampededServer};
use mcp_host::prelude::*;
// `let _ = write!`/`writeln!` into a String below: fmt::Write on String never errors.
use std::fmt::Write as _;

impl StampededServer {
    /// Worker liveness.
    #[mcp_tool(
        name = "worker_status",
        description = "Registered workers and how long since each last heartbeat",
        read_only = true,
        idempotent = true
    )]
    async fn worker_status(&self, _ctx: Ctx<'_>, _params: Parameters<EmptyArgs>) -> ToolResult {
        let rows: Vec<(String, i32, Vec<String>, f64)> = sqlx::query_as(
            "SELECT hostname, pid, queues, extract(epoch from now() - last_heartbeat)::float8 \
               FROM stampeded_workers ORDER BY last_heartbeat",
        )
        .fetch_all(self.queue().pool())
        .await
        .map_err(|e| ToolError::Internal(format!("worker query failed: {e}")))?;

        if rows.is_empty() {
            return Ok(ToolOutput::text(
                "no workers registered -- nothing is claiming jobs",
            ));
        }

        let mut out = format!("{} worker(s)\n", rows.len());
        for (host, pid, queues, age) in rows {
            let flag = if age > 300.0 {
                "  STALE -- jobs will be reclaimed"
            } else {
                ""
            };
            let _ = writeln!(
                out,
                "  {host}:{pid:<8} queues={:<20} heartbeat {age:.0}s ago{flag}",
                queues.join(",")
            );
        }
        Ok(ToolOutput::text(out))
    }
}

pub fn mount(router: McpToolRouter<StampededServer>) -> McpToolRouter<StampededServer> {
    router.with_tool(
        StampededServer::worker_status_tool_info(),
        StampededServer::worker_status_handler,
        None,
    )
}
