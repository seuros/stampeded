//! Tool implementations.
//!
//! Every tool answers an operational question someone actually asks at 3am.
//! `failure_summary` is the one worth highlighting: it clusters errors by
//! signature rather than listing instances, because "47 jobs failed" is noise
//! and "47 jobs failed with the same connection timeout" is a diagnosis.

use super::{
    EmptyArgs, JobIdArgs, QueueArgs, ScheduleKeyArgs, ScheduleUpsertArgs, SearchArgs,
    StampedeServer,
};
use crate::DstPolicy;
use mcp_host::prelude::*;
use mcp_host::registry::router::McpToolRouter;

/// One clustered failure signature as `failure_summary` reads it.
type FailureCluster = (
    Option<String>, // failure_kind
    Option<String>, // a representative error message
    Option<String>, // destination
    i64,            // how many
);

/// One job row as `job_inspect` reads it.
///
/// A NAMED STRUCT, not a tuple. It started as a tuple with a comment saying a
/// mis-ordered column would be a silent type-checks-but-wrong bug; by the time
/// resume state was added it had seventeen fields, which is past sqlx's tuple
/// FromRow limit and well past the point a human can check the ordering by eye.
/// Named fields bind by NAME, so the class of bug the comment warned about
/// cannot happen at all.
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
                "SELECT task, state::text AS state, attempt, max_attempts, \
                        failure_kind::text AS failure_kind, last_error, attempted_by, args, \
                        interruptions, max_interruptions, destination, deferrals, \
                        checkpoint, checkpoint_seq, dead_letter_queue, source_queue, errors \
                   FROM stampede_jobs WHERE queue = $1 AND id = $2",
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
        // Two budgets, and the tighter one usually decides. Showing only
        // attempts would make a job discarded after two claims look like a
        // mystery.
        if interruptions > 0 {
            out.push_str(&format!(
                "  interrupts  {interruptions} of {max_interruptions} (worker deaths)\n"
            ));
        }
        if let Some(d) = destination {
            out.push_str(&format!("  destination {d}\n"));
        }
        // Deferrals are neither attempts nor interruptions: they are how many
        // times WE declined to run it. A job with a high count is waiting on a
        // sick dependency, not failing.
        if deferrals > 0 {
            out.push_str(&format!(
                "  deferred    {deferrals}x (we declined to run it; no attempt charged)\n"
            ));
        }
        // Resume state. Often the single most useful thing about a long job:
        // "where did it get to" answers "should I wait or should I worry".
        if let Some(cp) = checkpoint {
            out.push_str(&format!(
                "  resume from {cp} (checkpoint {checkpoint_seq})\n"
            ));
        } else if checkpoint_seq > 0 {
            out.push_str(&format!(
                "  checkpointed {checkpoint_seq}x (cleared on success)\n"
            ));
        }
        if let Some(src) = source_queue {
            out.push_str(&format!("  dead letter from queue {src}\n"));
        } else if let Some(q) = dlq {
            out.push_str(&format!("  on failure  dead-letters to {q}\n"));
        }
        if let Some(k) = kind {
            // Spell out the distinctions most implementations conflate. This is
            // the whole reason the taxonomy exists: "it failed" is not
            // actionable, "our breaker refused to run it" is.
            let gloss = match k.as_str() {
                "worker_exited" => " (the worker process died; not the job's fault)",
                "worker_missing" => " (the worker vanished; not the job's fault)",
                "heartbeat_lost" => " (stopped reporting progress; not the job's fault)",
                "timed_out" => " (exceeded its time budget)",
                "breaker_open" => " (WE declined to run it: downstream circuit open; \
                                   no attempt was charged)",
                "throttled" => " (WE declined to run it: tenant over its rate limit; \
                                no attempt was charged)",
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

        // The structured history, which is what makes a flaky job
        // distinguishable from a consistently broken one at a glance.
        if let Some(history) = errors.as_array().filter(|h| h.len() > 1) {
            out.push_str("  history\n");
            for e in history {
                out.push_str(&format!(
                    "    attempt {} {}: {}{}\n",
                    e["attempt"].as_i64().unwrap_or(0),
                    e["kind"].as_str().unwrap_or("?"),
                    e["message"].as_str().unwrap_or(""),
                    match e["exit_status"].as_i64() {
                        // The single highest-signal artefact in the survey, and
                        // thrown away everywhere else.
                        Some(137) => " [exit 137 = OOM]".to_string(),
                        Some(c) => format!(" [exit {c}]"),
                        None => String::new(),
                    }
                ));
            }
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
        // Groups on the ERROR FINGERPRINT, not on a text prefix. A prefix
        // clusters "timed out after 30012ms" and "timed out after 29998ms"
        // separately, which is the same incident reported as two -- and if the
        // varying part comes first, as it does with a host or an id, a prefix
        // gives every single row its own "cluster" and the tool is useless
        // precisely when there is an incident. The fingerprint strips UUIDs,
        // hex and numbers before hashing, and it is indexed, so this is an
        // index scan rather than a seq scan over every error string.
        let rows: Vec<FailureCluster> = sqlx::query_as(
            "SELECT failure_kind::text, min(left(last_error, 80)), destination, count(*) \
               FROM stampede_jobs \
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
            out.push_str(&format!(
                "  {n:>5}  {:<15} {}{}\n",
                kind.unwrap_or_default(),
                err.unwrap_or_default(),
                dest.map(|d| format!("  -> {d}")).unwrap_or_default()
            ));
        }
        out.push_str(
            "\nThe kind is the diagnosis, so read it first:\n\
             \x20 worker_exited / worker_missing / heartbeat_lost — the WORKER died, not the \
             job. Usually infrastructure: OOM, a deploy, a node going away.\n\
             \x20 breaker_open / throttled — WE declined to run these. A downstream is sick or \
             a tenant is over its limit. No attempt was charged, so they will run once the \
             cause clears; nothing needs retrying by hand.\n\
             \x20 raised / timed_out — the job itself. These are the ones that are a bug.\n",
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

    /// Whether the queue's own machinery is healthy.
    #[mcp_tool(
        name = "system_health",
        description = "Autovacuum standing, sparse indexes and queue backlog age — the slow degradations that never raise an error",
        read_only = true
    )]
    async fn system_health(&self, _ctx: Ctx<'_>, _params: Parameters<EmptyArgs>) -> ToolResult {
        let mut out = String::new();

        // The death spiral, named. A queue table is the pathological
        // autovacuum case, and when it falls behind nothing errors -- things
        // just get slower, which presents as "the queue is mysteriously slow"
        // and sends people looking at their job code.
        match self.queue().vacuum_health().await {
            Ok(rows) => {
                let behind: Vec<_> = rows.iter().filter(|r| r.needs_vacuum).collect();
                if behind.is_empty() {
                    out.push_str("autovacuum: keeping up\n");
                } else {
                    out.push_str("autovacuum: BEHIND\n");
                    for r in behind {
                        out.push_str(&format!(
                            "  {} — {} dead tuples vs a threshold of {:.0}; last ran {}\n",
                            r.relname,
                            r.dead_tuples,
                            r.autovac_threshold,
                            r.last_autovacuum
                                .map(|t| t.to_string())
                                .unwrap_or_else(|| "never".into())
                        ));
                    }
                    out.push_str(
                        "  Claim latency drifts up until this catches up. Usually means \
                         autovacuum_vacuum_cost_limit is too low for the write rate.\n",
                    );
                }
            }
            Err(e) => out.push_str(&format!("autovacuum: unavailable ({e})\n")),
        }

        match self.queue().sparse_indexes().await {
            Ok(idx) if idx.is_empty() => out.push_str("indexes: dense\n"),
            Ok(idx) => out.push_str(&format!("indexes: sparse, worth rebuilding — {}\n", idx.join(", "))),
            Err(e) => out.push_str(&format!("indexes: unavailable ({e})\n")),
        }

        // Backlog AGE, not depth. Depth says how much work there is; this says
        // whether it is being done.
        match self.queue().oldest_available_age().await {
            Ok(ages) if ages.is_empty() => out.push_str("backlog: nothing waiting\n"),
            Ok(ages) => {
                out.push_str("backlog age (oldest waiting job per queue)\n");
                for (q, secs) in ages {
                    out.push_str(&format!("  {q:<20} {secs:.0}s\n"));
                }
            }
            Err(e) => out.push_str(&format!("backlog: unavailable ({e})\n")),
        }

        Ok(ToolOutput::text(out))
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
            StampedeServer::system_health_tool_info(),
            StampedeServer::system_health_handler,
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
