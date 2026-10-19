use crate::DstPolicy;
use crate::mcp::{ScheduleUpsertArgs, StampededServer};
use mcp_host::prelude::*;

impl StampededServer {
    /// Create or edit a schedule.
    #[mcp_tool(
        name = "schedule_upsert",
        description = "Create or edit a recurring schedule by key, using either a cron expression or a fixed interval",
        idempotent = true,
        visible = "crate::mcp::operator_may_write(ctx)"
    )]
    async fn schedule_upsert(
        &self,
        _ctx: Ctx<'_>,
        params: Parameters<ScheduleUpsertArgs>,
    ) -> ToolResult {
        let a = params.0;

        let policy = match a.dst_policy.as_deref() {
            None => None,
            Some("wall_clock") => Some(DstPolicy::WallClock),
            Some("instant") => Some(DstPolicy::Instant),
            Some(other) => {
                return Err(ToolError::InvalidArguments(format!(
                    "dst_policy must be \"wall_clock\" or \"instant\", got {other:?}"
                )));
            }
        };

        match (a.cron, a.every_secs) {
            (Some(expr), None) => {
                let tz = a.timezone.as_deref().unwrap_or("UTC");
                self.queue()
                    .upsert_cron_schedule(&a.key, &a.queue, &a.task, &expr, tz, policy)
                    .await
                    // Parsed on write, so a typo is reported here, not stored.
                    .map_err(|e| ToolError::InvalidArguments(format!("{e}")))?;
                Ok(ToolOutput::text(format!(
                    "schedule {:?} set to {expr:?} ({tz})",
                    a.key
                )))
            }
            (None, Some(secs)) => {
                self.queue()
                    .upsert_schedule(
                        &a.key,
                        &a.queue,
                        &a.task,
                        std::time::Duration::from_secs(secs),
                    )
                    .await
                    // A zero interval or empty key is the caller's mistake.
                    .map_err(|e| ToolError::InvalidArguments(format!("{e}")))?;
                Ok(ToolOutput::text(format!(
                    "schedule {:?} set to every {secs}s",
                    a.key
                )))
            }
            (Some(_), Some(_)) => Err(ToolError::InvalidArguments(
                "give either cron or every_secs, not both".into(),
            )),
            (None, None) => Err(ToolError::InvalidArguments(
                "give either cron or every_secs".into(),
            )),
        }
    }
}

pub fn mount(router: McpToolRouter<StampededServer>) -> McpToolRouter<StampededServer> {
    router.with_tool(
        StampededServer::schedule_upsert_tool_info(),
        StampededServer::schedule_upsert_handler,
        Some(StampededServer::schedule_upsert_visibility),
    )
}
