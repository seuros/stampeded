use crate::mcp::{EmptyArgs, StampededServer};
use mcp_host::prelude::*;
use std::fmt::Write as _;

/// One schedule row as `schedule_list` reads it.
type ScheduleRow = (
    String,         // key
    String,         // queue
    String,         // task
    Option<String>, // cron_expr
    Option<i64>,    // interval_secs
    String,         // timezone
    String,         // dst_policy
    bool,           // paused
    String,         // next_run_at
    Option<String>, // last_run_at
);

impl StampededServer {
    /// Every schedule and when it next fires.
    #[mcp_tool(
        name = "schedule_list",
        description = "Every recurring schedule: its rule, timezone, DST policy, and when it next fires",
        read_only = true,
        idempotent = true
    )]
    async fn schedule_list(&self, _ctx: Ctx<'_>, _params: Parameters<EmptyArgs>) -> ToolResult {
        let rows: Vec<ScheduleRow> = sqlx::query_as(
            "SELECT key, queue, task, cron_expr, interval_secs, timezone, dst_policy, \
                    paused, next_run_at::text, last_run_at::text \
               FROM stampeded_schedules ORDER BY next_run_at",
        )
        .fetch_all(self.queue().pool())
        .await
        .map_err(|e| ToolError::Internal(format!("schedule query failed: {e}")))?;

        if rows.is_empty() {
            return Ok(ToolOutput::text("no schedules registered"));
        }

        let mut out = String::new();
        for (key, queue, task, cron, every, tz, dst, paused, next, last) in rows {
            let rule = match (cron, every) {
                (Some(c), _) => format!("{c:?} {tz} ({dst})"),
                (_, Some(n)) => format!("every {n}s"),
                _ => "<no rule>".into(),
            };
            let _ = write!(
                out,
                "{key}\n  {rule}\n  -> {queue}/{task}{}\n  next {next}  last {}\n",
                if paused { "  [PAUSED]" } else { "" },
                last.as_deref().unwrap_or("never")
            );
        }
        Ok(ToolOutput::text(out))
    }
}

pub fn mount(router: McpToolRouter<StampededServer>) -> McpToolRouter<StampededServer> {
    router.with_tool(
        StampededServer::schedule_list_tool_info(),
        StampededServer::schedule_list_handler,
        None,
    )
}
