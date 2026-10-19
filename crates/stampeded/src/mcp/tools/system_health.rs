use crate::mcp::{EmptyArgs, StampededServer};
use mcp_host::prelude::*;
use std::fmt::Write as _;

impl StampededServer {
    /// Whether the queue's own machinery is healthy.
    #[mcp_tool(
        name = "system_health",
        description = "Autovacuum standing, sparse indexes and queue backlog age -- the slow degradations that never raise an error",
        read_only = true,
        idempotent = true
    )]
    async fn system_health(&self, _ctx: Ctx<'_>, _params: Parameters<EmptyArgs>) -> ToolResult {
        let mut out = String::new();

        // A lagging autovacuum never errors; the queue just gets slower.
        match self.queue().vacuum_health().await {
            Ok(rows) => {
                let behind: Vec<_> = rows.iter().filter(|r| r.needs_vacuum).collect();
                if behind.is_empty() {
                    out.push_str("autovacuum: keeping up\n");
                } else {
                    out.push_str("autovacuum: BEHIND\n");
                    for r in behind {
                        let _ = writeln!(
                            out,
                            "  {} -- {} dead tuples vs a threshold of {:.0}; last ran {}",
                            r.relname,
                            r.dead_tuples,
                            r.autovac_threshold,
                            r.last_autovacuum
                                .map_or_else(|| "never".into(), |t| t.to_string())
                        );
                    }
                    out.push_str(
                        "  Claim latency drifts up until this catches up. Usually means \
                         autovacuum_vacuum_cost_limit is too low for the write rate.\n",
                    );
                }
            }
            Err(e) => {
                let _ = writeln!(out, "autovacuum: unavailable ({e})");
            }
        }

        match self.queue().sparse_indexes().await {
            Ok(idx) if idx.is_empty() => out.push_str("indexes: dense\n"),
            Ok(idx) => {
                let _ = writeln!(
                    out,
                    "indexes: sparse, worth rebuilding -- {}",
                    idx.join(", ")
                );
            }
            Err(e) => {
                let _ = writeln!(out, "indexes: unavailable ({e})");
            }
        }

        // Age, not depth: whether work is being done, not how much there is.
        match self.queue().oldest_available_age().await {
            Ok(ages) if ages.is_empty() => out.push_str("backlog: nothing waiting\n"),
            Ok(ages) => {
                out.push_str("backlog age (oldest waiting job per queue)\n");
                for (q, secs) in ages {
                    let _ = writeln!(out, "  {q:<20} {secs:.0}s");
                }
            }
            Err(e) => {
                let _ = writeln!(out, "backlog: unavailable ({e})");
            }
        }

        Ok(ToolOutput::text(out))
    }
}

pub fn mount(router: McpToolRouter<StampededServer>) -> McpToolRouter<StampededServer> {
    router.with_tool(
        StampededServer::system_health_tool_info(),
        StampededServer::system_health_handler,
        None,
    )
}
