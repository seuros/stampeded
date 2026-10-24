use crate::mcp::{QueueArgs, StampededServer};
use mcp_host::prelude::*;
// `let _ = write!`/`writeln!` into a String below: fmt::Write on String never errors.
use std::fmt::Write as _;

/// One clustered failure signature as `failure_summary` reads it.
type FailureCluster = (
    Option<String>, // failure_kind
    Option<String>, // a representative error message
    Option<String>, // destination
    i64,            // how many
);

impl StampededServer {
    /// Cluster recent failures by error signature.
    #[mcp_tool(
        name = "failure_summary",
        description = "Cluster recent failures by error text so the pattern is visible rather than individual instances",
        read_only = true,
        idempotent = true
    )]
    async fn failure_summary(&self, _ctx: Ctx<'_>, params: Parameters<QueueArgs>) -> ToolResult {
        // Group on the indexed fingerprint (UUIDs, hex, numbers stripped), not a prefix.
        let rows: Vec<FailureCluster> = sqlx::query_as(
            "SELECT failure_kind::text, min(left(last_error, 80)), destination, count(*) \
               FROM stampeded_jobs \
              WHERE queue = $1 AND error_fingerprint IS NOT NULL \
              GROUP BY failure_kind, error_fingerprint, destination \
              ORDER BY 4 DESC LIMIT 20",
        )
        .bind(&params.0.queue)
        .fetch_all(self.queue().pool())
        .await
        .map_err(|e| ToolError::Internal(format!("summary failed: {e}")))?;

        if rows.is_empty() {
            return Ok(ToolOutput::text(format!(
                "no failures recorded in queue {:?}",
                params.0.queue
            )));
        }

        let mut out = String::from("failures by signature (most common first)\n");
        for (kind, err, dest, n) in rows {
            let _ = writeln!(
                out,
                "  {n:>5}  {:<15} {}{}",
                kind.unwrap_or_default(),
                err.unwrap_or_default(),
                dest.map(|d| format!("  -> {d}")).unwrap_or_default()
            );
        }
        out.push_str(
            "\nThe kind is the diagnosis, so read it first:\n\
             \x20 worker_exited / worker_missing / heartbeat_lost -- the WORKER died, not the \
             job. Usually infrastructure: OOM, a deploy, a node going away.\n\
             \x20 breaker_open / throttled -- WE declined to run these. A downstream is sick or \
             a tenant is over its limit. No attempt was charged, so they will run once the \
             cause clears; nothing needs retrying by hand.\n\
             \x20 raised / timed_out -- the job itself. These are the ones that are a bug.\n",
        );
        Ok(ToolOutput::text(out))
    }
}

pub fn mount(router: McpToolRouter<StampededServer>) -> McpToolRouter<StampededServer> {
    router.with_tool(
        StampededServer::failure_summary_tool_info(),
        StampededServer::failure_summary_handler,
        None,
    )
}
