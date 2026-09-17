//! Tool implementations.
//!
//! Every tool answers an operational question someone actually asks at 3am.
//! `failure_summary` is the one worth highlighting: it clusters errors by
//! signature rather than listing instances, because "47 jobs failed" is noise
//! and "47 jobs failed with the same connection timeout" is a diagnosis.

use crate::{EmptyArgs, JobIdArgs, QueueArgs, ScheduleKeyArgs, ScheduleUpsertArgs, SearchArgs, StampedeServer};
use stampede_core::DstPolicy;
use mcp_host::prelude::*;
use mcp_host::registry::router::McpToolRouter;

/// One job row as `job_inspect` reads it.
///
/// Named rather than inlined: the tuple is wide enough that a mis-ordered
/// column in the SELECT would be a silent type-checks-but-wrong bug.
type JobRow = (
    String,              // task
    String,              // state
    i16,                 // attempt
    i16,                 // max_attempts
    Option<String>,      // failure_kind
    Option<String>,      // last_error
    Vec<String>,         // attempted_by
    serde_json::Value,   // args
);

impl StampedeServer {
    /// Queue depth by state.
    #[mcp_tool(
        name = "queue_status",
        description = "Queue depth by state: how many jobs are available, executing, retryable or discarded",
        read_only = true
    )]
    async fn queue_status(&self, _ctx: Ctx<'_>, params: Parameters<QueueArgs>) -> ToolResult {
        let rows = self
            .queue()
            .depth(&params.0.queue)
            .await
            .map_err(|e| ToolError::Internal(format!("depth query failed: {e}")))?;

        if rows.is_empty() {
            return Ok(ToolOutput::text(format!(
                "queue {:?} has no unfinished jobs",
                params.0.queue
            )));
        }

        let mut out = format!("queue {:?}\n", params.0.queue);
        for (state, n) in rows {
            out.push_str(&format!("  {state:<12} {n}\n"));
        }
        Ok(ToolOutput::text(out))
    }

    /// Full detail for one job.
    #[mcp_tool(
        name = "job_inspect",
        description = "Everything known about one job: state, attempts, which workers ran it, and why it last failed",
        read_only = true
    )]
    async fn job_inspect(&self, _ctx: Ctx<'_>, params: Parameters<JobIdArgs>) -> ToolResult {
        let id = uuid::Uuid::parse_str(&params.0.id)
            .map_err(|_| ToolError::InvalidArguments(format!("{:?} is not a UUID", params.0.id)))?;

        let row: Option<JobRow> = sqlx::query_as(
                "SELECT task, state::text, attempt, max_attempts, failure_kind::text, \
                        last_error, attempted_by, args \
                   FROM stampede_jobs WHERE queue = $1 AND id = $2",
            )
            .bind(&params.0.queue)
            .bind(id)
            .fetch_optional(self.queue().pool())
            .await
            .map_err(|e| ToolError::Internal(format!("query failed: {e}")))?;

        let Some((task, state, attempt, max_attempts, kind, error, workers, args)) = row else {
            return Ok(ToolOutput::text(format!(
                "no job {id} in queue {:?}",
                params.0.queue
            )));
        };

        let mut out = format!(
            "job {id}\n  task        {task}\n  state       {state}\n  \
             attempt     {attempt} of {max_attempts}\n"
        );
        if let Some(k) = kind {
            // Spell out the distinction most implementations conflate.
            let gloss = match k.as_str() {
                "interrupted" => " (the worker died; not the job's fault)",
                "timeout" => " (exceeded its execution timeout)",
                "unrecoverable" => " (will never succeed; retries skipped)",
                _ => "",
            };
            out.push_str(&format!("  failure     {k}{gloss}\n"));
        }
        if let Some(e) = error {
            out.push_str(&format!("  last error  {e}\n"));
        }
        if !workers.is_empty() {
            out.push_str(&format!("  ran on      {}\n", workers.join(", ")));
        }
        out.push_str(&format!("  args        {args}\n"));
        Ok(ToolOutput::text(out))
    }

    /// Search jobs.
    #[mcp_tool(
        name = "job_search",
        description = "Find jobs by task, state or error substring. Use failure_summary first when diagnosing a pattern",
        read_only = true
    )]
    async fn job_search(&self, _ctx: Ctx<'_>, params: Parameters<SearchArgs>) -> ToolResult {
        let a = params.0;
        let limit = a.limit.unwrap_or(20).clamp(1, 100);

        let rows: Vec<(uuid::Uuid, String, String, i16, Option<String>)> = sqlx::query_as(
            "SELECT id, task, state::text, attempt, left(last_error, 120) \
               FROM stampede_jobs \
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
            out.push_str(&format!("  {id}  {task:<20} {state:<10} attempt {attempt}"));
            if let Some(e) = err {
                out.push_str(&format!("  {e}"));
            }
            out.push('\n');
        }
        Ok(ToolOutput::text(out))
    }

    /// Cluster recent failures by error signature.
    #[mcp_tool(
        name = "failure_summary",
        description = "Cluster recent failures by error text so the pattern is visible rather than individual instances",
        read_only = true
    )]
    async fn failure_summary(&self, _ctx: Ctx<'_>, params: Parameters<QueueArgs>) -> ToolResult {
        // Group on a truncated prefix: full error text usually carries an id or
        // a timestamp that would make every row its own "cluster".
        let rows: Vec<(Option<String>, Option<String>, i64)> = sqlx::query_as(
            "SELECT failure_kind::text, left(last_error, 80), count(*) \
               FROM stampede_jobs \
              WHERE queue = $1 AND last_error IS NOT NULL \
              GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20",
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
        for (kind, err, n) in rows {
            out.push_str(&format!(
                "  {n:>5}  {:<14} {}\n",
                kind.unwrap_or_default(),
                err.unwrap_or_default()
            ));
        }
        out.push_str(
            "\nfailure_kind 'interrupted' means the worker died mid-job, not that \
             the job raised — those are usually an infrastructure problem, not a bug.\n",
        );
        Ok(ToolOutput::text(out))
    }

    /// Worker liveness.
    #[mcp_tool(
        name = "worker_status",
        description = "Registered workers and how long since each last heartbeat",
        read_only = true
    )]
    async fn worker_status(&self, _ctx: Ctx<'_>, _params: Parameters<()>) -> ToolResult {
        let rows: Vec<(String, i32, Vec<String>, f64)> = sqlx::query_as(
            "SELECT hostname, pid, queues, extract(epoch from now() - last_heartbeat)::float8 \
               FROM stampede_workers ORDER BY last_heartbeat",
        )
        .fetch_all(self.queue().pool())
        .await
        .map_err(|e| ToolError::Internal(format!("worker query failed: {e}")))?;

        if rows.is_empty() {
            return Ok(ToolOutput::text(
                "no workers registered — nothing is claiming jobs",
            ));
        }

        let mut out = format!("{} worker(s)\n", rows.len());
        for (host, pid, queues, age) in rows {
            let flag = if age > 300.0 { "  STALE — jobs will be reclaimed" } else { "" };
            out.push_str(&format!(
                "  {host}:{pid:<8} queues={:<20} heartbeat {age:.0}s ago{flag}\n",
                queues.join(",")
            ));
        }
        Ok(ToolOutput::text(out))
    }

    /// Make a discarded job claimable again.
    ///
    /// Gated: invisible unless the server was built `.with_writes()`.
    #[mcp_tool(
        name = "job_retry",
        description = "Reset a discarded job so it can be claimed again (resets attempt count)",
        visible = "ctx.has_role(\"operator\")"
    )]
    async fn job_retry(&self, _ctx: Ctx<'_>, params: Parameters<JobIdArgs>) -> ToolResult {
        let id = uuid::Uuid::parse_str(&params.0.id)
            .map_err(|_| ToolError::InvalidArguments(format!("{:?} is not a UUID", params.0.id)))?;

        let n = sqlx::query(
            "UPDATE stampede_jobs \
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

    /// Cancel a job that has not started.
    #[mcp_tool(
        name = "job_cancel",
        description = "Cancel a job that has not started executing",
        visible = "ctx.has_role(\"operator\")"
    )]
    async fn job_cancel(&self, _ctx: Ctx<'_>, params: Parameters<JobIdArgs>) -> ToolResult {
        let id = uuid::Uuid::parse_str(&params.0.id)
            .map_err(|_| ToolError::InvalidArguments(format!("{:?} is not a UUID", params.0.id)))?;

        // Deliberately refuses to cancel an executing job: the worker owns it,
        // and flipping the row underneath would make the worker's completion
        // silently fail the ownership check rather than doing anything useful.
        let n = sqlx::query(
            "UPDATE stampede_jobs \
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
                "job {id} is not cancellable — it is already executing or finished. \
                 An executing job is owned by its worker; cancelling the row would \
                 not stop the work."
            )
        }))
    }

    /// Every schedule and when it next fires.
    #[mcp_tool(
        name = "schedule_list",
        description = "Every recurring schedule: its rule, timezone, DST policy, and when it next fires",
        read_only = true
    )]
    async fn schedule_list(&self, _ctx: Ctx<'_>, _params: Parameters<EmptyArgs>) -> ToolResult {
        let rows: Vec<ScheduleRow> = sqlx::query_as(
            "SELECT key, queue, task, cron_expr, interval_secs, timezone, dst_policy, \
                    paused, next_run_at::text, last_run_at::text \
               FROM stampede_schedules ORDER BY next_run_at",
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
            out.push_str(&format!(
                "{key}\n  {rule}\n  -> {queue}/{task}{}\n  next {next}  last {}\n",
                if paused { "  [PAUSED]" } else { "" },
                last.as_deref().unwrap_or("never")
            ));
        }
        Ok(ToolOutput::text(out))
    }

    /// Create or edit a schedule.
    ///
    /// Runtime-mutable schedules are the point: Oban, River and good_job keep
    /// theirs in config, so changing one is a deploy. Here it is a tool call.
    #[mcp_tool(
        name = "schedule_upsert",
        description = "Create or edit a recurring schedule by key, using either a cron expression or a fixed interval",
        visible = "ctx.has_role(\"operator\")"
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
                )))
            }
        };

        match (a.cron, a.every_secs) {
            (Some(expr), None) => {
                let tz = a.timezone.as_deref().unwrap_or("UTC");
                self.queue()
                    .upsert_cron_schedule(&a.key, &a.queue, &a.task, &expr, tz, policy)
                    .await
                    // Parsing happens on write, so a typo comes back here as a
                    // usable message rather than becoming a row that fails
                    // silently once a minute forever.
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
                    .map_err(|e| ToolError::Internal(format!("upsert failed: {e}")))?;
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

    /// Pause or resume a schedule.
    ///
    /// Pausing rather than deleting: a paused row keeps its key, its history in
    /// the occurrence ledger, and its next_run_at, so resuming is not a
    /// re-creation and the ledger still refuses a duplicate occurrence.
    #[mcp_tool(
        name = "schedule_pause",
        description = "Pause or resume a schedule by key without deleting it",
        visible = "ctx.has_role(\"operator\")"
    )]
    async fn schedule_pause(&self, _ctx: Ctx<'_>, params: Parameters<ScheduleKeyArgs>) -> ToolResult {
        let n = sqlx::query(
            "UPDATE stampede_schedules SET paused = $2, updated_at = now() WHERE key = $1",
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

/// One schedule row as `schedule_list` reads it. Named for the same reason as
/// `JobRow`: wide enough that a mis-ordered column would type-check.
type ScheduleRow = (
    String,          // key
    String,          // queue
    String,          // task
    Option<String>,  // cron_expr
    Option<i64>,     // interval_secs
    String,          // timezone
    String,          // dst_policy
    bool,            // paused
    String,          // next_run_at
    Option<String>,  // last_run_at
);

/// Mount the read tools, and optionally the guarded write tools.
///
/// Two independent gates, deliberately:
///
/// 1. `allow_writes` decides whether the destructive tools are mounted at all,
///    so a read-only deployment cannot expose them by misconfiguration.
/// 2. `visible = "ctx.has_role(\"operator\")"` is evaluated PER SESSION, so
///    even where they are mounted, a non-operator session never sees them in
///    tools/list. Being invisible is better than being refused: a model cannot
///    be argued into calling a tool it was never shown.
pub fn mount(
    router: McpToolRouter<StampedeServer>,
    allow_writes: bool,
) -> McpToolRouter<StampedeServer> {
    let router = router
        .with_tool(
            StampedeServer::queue_status_tool_info(),
            StampedeServer::queue_status_handler,
            None,
        )
        .with_tool(
            StampedeServer::job_inspect_tool_info(),
            StampedeServer::job_inspect_handler,
            None,
        )
        .with_tool(
            StampedeServer::schedule_list_tool_info(),
            StampedeServer::schedule_list_handler,
            None,
        )
        .with_tool(
            StampedeServer::job_search_tool_info(),
            StampedeServer::job_search_handler,
            None,
        )
        .with_tool(
            StampedeServer::failure_summary_tool_info(),
            StampedeServer::failure_summary_handler,
            None,
        )
        .with_tool(
            StampedeServer::worker_status_tool_info(),
            StampedeServer::worker_status_handler,
            None,
        );

    if !allow_writes {
        return router;
    }

    router
        .with_tool(
            StampedeServer::job_retry_tool_info(),
            StampedeServer::job_retry_handler,
            Some(StampedeServer::job_retry_visibility),
        )
        .with_tool(
            StampedeServer::job_cancel_tool_info(),
            StampedeServer::job_cancel_handler,
            Some(StampedeServer::job_cancel_visibility),
        )
        .with_tool(
            StampedeServer::schedule_upsert_tool_info(),
            StampedeServer::schedule_upsert_handler,
            Some(StampedeServer::schedule_upsert_visibility),
        )
        .with_tool(
            StampedeServer::schedule_pause_tool_info(),
            StampedeServer::schedule_pause_handler,
            Some(StampedeServer::schedule_pause_visibility),
        )
}
