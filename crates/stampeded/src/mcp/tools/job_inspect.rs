use crate::mcp::{JobIdArgs, StampededServer};
use mcp_host::prelude::*;
use std::fmt::Write as _;

/// One job row as `job_inspect` reads it; named fields bind by column name.
#[derive(sqlx::FromRow)]
struct JobRow {
    task: String,
    state: String,
    attempt: i16,
    max_attempts: i16,
    failure_kind: Option<String>,
    last_error: Option<String>,
    attempted_by: Vec<String>,
    args: serde_json::Value,
    interruptions: i16,
    max_interruptions: i16,
    destination: Option<String>,
    deferrals: i16,
    checkpoint: Option<serde_json::Value>,
    checkpoint_seq: i32,
    dead_letter_queue: Option<String>,
    source_queue: Option<String>,
    errors: serde_json::Value,
}

impl StampededServer {
    /// Full detail for one job.
    #[mcp_tool(
        name = "job_inspect",
        description = "Everything known about one job: state, attempts, which workers ran it, and why it last failed",
        read_only = true,
        idempotent = true
    )]
    async fn job_inspect(&self, _ctx: Ctx<'_>, params: Parameters<JobIdArgs>) -> ToolResult {
        let id = uuid::Uuid::parse_str(&params.0.id)
            .map_err(|_| ToolError::InvalidArguments(format!("{:?} is not a UUID", params.0.id)))?;

        let row: Option<JobRow> = sqlx::query_as(
            "SELECT task, state::text AS state, attempt, max_attempts, \
                        failure_kind::text AS failure_kind, last_error, attempted_by, args, \
                        interruptions, max_interruptions, destination, deferrals, \
                        checkpoint, checkpoint_seq, dead_letter_queue, source_queue, errors \
                   FROM stampeded_jobs WHERE queue = $1 AND id = $2",
        )
        .bind(&params.0.queue)
        .bind(id)
        .fetch_optional(self.queue().pool())
        .await
        .map_err(|e| ToolError::Internal(format!("query failed: {e}")))?;

        let Some(JobRow {
            task,
            state,
            attempt,
            max_attempts,
            failure_kind: kind,
            last_error: error,
            attempted_by: workers,
            args,
            interruptions,
            max_interruptions,
            destination,
            deferrals,
            checkpoint,
            checkpoint_seq,
            dead_letter_queue: dlq,
            source_queue,
            errors,
        }) = row
        else {
            return Ok(ToolOutput::text(format!(
                "no job {id} in queue {:?}",
                params.0.queue
            )));
        };

        let mut out = format!(
            "job {id}\n  task        {task}\n  state       {state}\n  \
             attempt     {attempt} of {max_attempts}\n"
        );
        // Two budgets; the tighter one usually decides.
        if interruptions > 0 {
            let _ = writeln!(
                out,
                "  interrupts  {interruptions} of {max_interruptions} (worker deaths)"
            );
        }
        if let Some(d) = destination {
            let _ = writeln!(out, "  destination {d}");
        }
        // Deferrals count how often we declined to run it, not failures.
        if deferrals > 0 {
            let _ = writeln!(
                out,
                "  deferred    {deferrals}x (we declined to run it; no attempt charged)"
            );
        }
        if let Some(cp) = checkpoint {
            let _ = writeln!(out, "  resume from {cp} (checkpoint {checkpoint_seq})");
        } else if checkpoint_seq > 0 {
            let _ = writeln!(out, "  checkpointed {checkpoint_seq}x (cleared on success)");
        }
        if let Some(src) = source_queue {
            let _ = writeln!(out, "  dead letter from queue {src}");
        } else if let Some(q) = dlq {
            let _ = writeln!(out, "  on failure  dead-letters to {q}");
        }
        if let Some(k) = kind {
            let gloss = match k.as_str() {
                "worker_exited" => " (the worker process died; not the job's fault)",
                "worker_missing" => " (the worker vanished; not the job's fault)",
                "heartbeat_lost" => " (stopped reporting progress; not the job's fault)",
                "timed_out" => " (exceeded its time budget)",
                "breaker_open" => {
                    " (WE declined to run it: downstream circuit open; \
                                   no attempt was charged)"
                }
                "throttled" => {
                    " (WE declined to run it: tenant over its rate limit; \
                                no attempt was charged)"
                }
                "unrecoverable" => " (will never succeed; retries skipped)",
                _ => "",
            };
            let _ = writeln!(out, "  failure     {k}{gloss}");
        }
        if let Some(e) = error {
            let _ = writeln!(out, "  last error  {e}");
        }
        if !workers.is_empty() {
            let _ = writeln!(out, "  ran on      {}", workers.join(", "));
        }

        if let Some(history) = errors.as_array().filter(|h| h.len() > 1) {
            out.push_str("  history\n");
            for e in history {
                let _ = writeln!(
                    out,
                    "    attempt {} {}: {}{}",
                    e["attempt"].as_i64().unwrap_or(0),
                    e["kind"].as_str().unwrap_or("?"),
                    e["message"].as_str().unwrap_or(""),
                    match e["exit_status"].as_i64() {
                        Some(137) => " [exit 137 = OOM]".to_string(),
                        Some(c) => format!(" [exit {c}]"),
                        None => String::new(),
                    }
                );
            }
        }
        let _ = writeln!(out, "  args        {args}");
        Ok(ToolOutput::text(out))
    }
}

pub fn mount(router: McpToolRouter<StampededServer>) -> McpToolRouter<StampededServer> {
    router.with_tool(
        StampededServer::job_inspect_tool_info(),
        StampededServer::job_inspect_handler,
        None,
    )
}
