# Failure handling, retries, and observability

> **IMPLEMENTATION FINDING 2026-09-17 — retry delay must be a DELAY, not an instant.**
>
> The first cut of `stampede_fail` took `p_retry_at timestamptz`, computed on
> the worker as `Utc::now() + backoff`. An integration test failed: the job was
> not claimable on its next round. Cause: the worker's clock was **3 ms ahead**
> of Postgres, so `scheduled_at <= now()` was false.
>
> 3 ms is harmless. The flaw is not: taking an absolute instant makes retry
> timing depend on the app server and the database agreeing about the time. A
> worker 30 s ahead delays every retry by 30 s; a fleet with mixed skew retries
> inconsistently; and it is invisible in development, where skew is milliseconds.
>
> `stampede_fail` now takes `p_retry_delay_ms bigint` and the database applies
> its own clock (`now() + make_interval(...)`). `chrono-machines` already returns
> a delay in ms, so this is also less code — the conversion to an absolute time
> was work that introduced a bug.
>
> None of the thirteen surveyed implementations documents this. Most compute
> backoff in the worker and write an absolute time; they are all carrying the
> same latent skew dependency.



Source study of retry formulas, error capture, dead-lettering, poison-job
handling, timeouts and instrumentation across the permissively-licensed
references. All `file:line` citations are from cloned source at
`~/Projects/ecosystem/reference/`, read 2026-09-16.

**Licence compliance:** nothing in this file derives from `faktory`, `dramatiq`,
`hangfire`, `jobrunr` or `sidekiq`. Sidekiq's formula appears only because
`asynq` and `neoq` cite it in their own comments — quoted from *their* MIT
source, not from Sidekiq.

**Two corrections to existing notes are recorded at the bottom. Read them.**

---

## 1. Retry and backoff formulas

### The table

`n` = attempt / error count. "jitter" = does the *default* configuration
decorrelate two jobs that failed at the same instant.

| impl | default formula (seconds unless noted) | jitter | kind | file:line |
|---|---|---|---|---|
| **Oban** | `15 + 2^n`, `n` clamped to 20 via `round(attempt/max_attempts*20)` | **yes** | additive, `+0..10%` (`mode: :inc`) | `postgres/oban/lib/oban/worker.ex:509-520`; `lib/oban/backoff.ex:33-39,59-80` |
| **River** | `n^4` where `n = len(errors)+1` | **yes** | multiplicative `±10%` | `postgres/river/internal/retrypolicy/default.go:37,49-67,74-76` |
| **asynq** | `n^4 + 15 + rand(0..29)*(n+1)` | **yes** | additive, unbounded-ish | `redis/asynq/server.go:401-404` |
| **bullmq** | `2^(n-1) * delay`; `jitter` option defaults **0** | **no** | opt-in fraction | `redis/bullmq/src/classes/backoffs.ts:23-34`; `src/interfaces/backoff-options.ts` (`@defaultValue 0`) |
| **celery** | `default_retry_delay = 180`, **constant**; `retry_backoff` off by default | **no** (default) / **yes** (opt-in) | AWS full jitter when enabled | `other/celery/celery/app/task.py:241,245,843`; `celery/utils/time.py:467-480`; `celery/app/autoretry.py:21-30,49-55` |
| **graphile-worker** | `e^min(n,10)` — computed in SQL | **no** | none available | `postgres/graphile-worker/src/sql/failJobs.ts:46,79,123` |
| **pg-boss** | `retry_backoff=false` → constant `retry_delay` (**default 0**); when on: `LEAST(max, GREATEST(delay,1) * (2^k/2 + 2^k/2*random()))`, `k=LEAST(16,n+1)` | **no** (default) / **yes** (opt-in) | AWS *equal* jitter, in SQL | `postgres/pg-boss/src/plans.ts:105-113,2431-2438` |
| **que** | `n^4 + 3` | **no** | none | `postgres/que/lib/que/job.rb:255` |
| **neoq** | `n^4 + 15 + rand(0..29)*n + 1` | **yes, broken** | see below | `postgres/neoq/internal/internal.go:22-33` |
| **db-scheduler** | `initialDelay * 1.5^consecutiveFailures` | **no** | none in repo | `postgres/db-scheduler/.../task/FailureHandler.java:112,130-134` |
| **procrastinate** | `wait + linear*n + exponential^(n+1)`, **all defaults 0** | **no** | none | `postgres/procrastinate/procrastinate/retry.py:190-206` |
| **underway** (Rust) | `min(1000ms * 2^(n-1), 60_000ms)`, then jitter | **yes** | fraction `0.5` = equal jitter; `1.0` would be full | `postgres/underway/src/task/retry_policy.rs:42-59` |
| **ActiveJob** (solid_queue / good_job substrate) | `n^4 + jitter + 2` | **yes** | additive `0..15%` | `activejob/lib/active_job/exceptions.rb:30,161-184`; `railties/.../configuration.rb:189` |

### Who gets jitter wrong

Ranked by how badly a synchronised batch stays synchronised.

1. **graphile-worker** — worst. `e^min(attempts,10)` seconds, no jitter, no
   knob. 1,000 jobs that fail together against a downed API retry in the same
   second, forever, all 25 default attempts. (`src/sql/failJobs.ts:46`)
2. **que** (`n^4+3`), **db-scheduler** (`1.5^n`), **procrastinate**
   (defaults to 0 = immediate) — same class, no jitter anywhere in the repo
   (verified by repo-wide grep on db-scheduler: zero hits for `jitter`).
3. **pg-boss** and **celery** — jitter exists and is correct *when enabled*,
   but both default to a constant delay. pg-boss's default `retry_delay: 0`
   means the default behaviour is an **immediate** retry loop
   (`src/plans.ts:110`). Correct primitive, wrong default.
4. **bullmq** — `jitter` defaults to `0`. Exponential without jitter.
5. **neoq** — *intends* to have jitter and fails twice:
   - transcription bug: Sidekiq's `rand(30) * (count + 1)` was written as
     `RandInt(maxInt)*retryCount+1`, so the jitter term is multiplied by
     `retryCount` instead of `retryCount+1` and **vanishes entirely on the
     first retry** (`internal/internal.go:26`).
   - `rand.NewSource(time.Now().UnixNano())` is re-seeded on **every call**
     (`internal/internal.go:31-33`). Jobs failing inside the same clock tick
     draw the same "random" number. This is a thundering-herd generator
     wearing a jitter costume.
6. **Oban / ActiveJob** — jitter present but **additive and small**: Oban is
   `+0..10%` one-sided (`backoff.ex:61`, `mode: :inc` at `worker.ex:519`),
   ActiveJob `+0..15%` (`exceptions.rb:182-184`). A 10% spread on a 1,000-job
   herd still lands 1,000 jobs inside a 10%-wide window. It thins the spike, it
   does not break the correlation.
7. **River / asynq** — decent. `±10%` multiplicative (River) and an additive
   `rand(30)*(n+1)` term that grows with the attempt (asynq) — asynq's spread
   is genuinely wide by attempt 3+.
8. **underway** — best of the survey. `target*(1-j) + rand(0..target*j)` with
   `j = 0.5` default, bounded by `max_interval_ms`, and the coefficient and
   jitter factor are **persisted per task in the DB**
   (`src/queue.rs:782-783,845-846`). At `j = 1.0` this *is* AWS full jitter.

**Nobody in the survey defaults to full jitter.** Underway comes closest and
stops at equal jitter. That is the gap `chrono-machines` walks into.

Verified: every formula above was read from source. Inferred: nothing in this
section.

---

## 2. Error capture

### What is stored

| impl | shape | history | bounded? |
|---|---|---|---|
| **Oban** | `jsonb[]`-ish array appended with `\|\|`, each entry `{attempt, at, error}` where `error` is `Exception.format/3` output — message **and** stacktrace, one flat string | **all attempts** | **no** — `basic.ex:25-29` appends forever, no cap, no truncation |
| **River** | `errors jsonb[]`, each `AttemptError{at, attempt, error, trace}` — `trace` populated **only on panic** | **all attempts** | **no** on `errors`; **yes** on `attempted_by` |
| **solid_queue** | separate `failed_executions` row, `error` JSON `{exception_class, message, backtrace}` | last only (one row per failure, job destroyed on retry) | **yes** — the only backtrace-aware truncator |
| **que** | `last_error_message`, `last_error_backtrace` (two columns) | **last only** | **yes** — `left($2,500)` / `left($3,10000)` in SQL |
| **good_job** | `error` text + `error_backtrace` array + **`error_event` enum** | one row per execution in `good_job_executions` | **no** |
| **graphile-worker** | `last_error text` = `err.message` only. No class, no stack. | **last only, overwritten** | **no** |
| **pg-boss** | `output jsonb` — whatever the handler returns; `serialize-error` used for thrown errors | last only | **no** |
| **celery** | result backend `{exc_type, exc_message, exc_module}` + traceback string | last only | **no** |

Citations: `oban/lib/oban/job.ex:582-590` (`format_attempt`),
`oban/lib/oban/engines/basic.ex:25-29,246,264,296`;
`river/rivertype/river_type.go:343-363`,
`river/riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql:19,482`;
`solid_queue/app/models/solid_queue/failed_execution.rb:7,38-78`;
`que/lib/que/job_methods.rb:28-31`;
`good_job/app/models/concerns/good_job/error_events.rb:7-12`,
`good_job/app/models/good_job/job.rb:822-857`;
`graphile-worker/src/sql/failJobs.ts:45`;
`pg-boss/src/manager.ts:4,344`.

### The three things worth stealing

**River's bounded `attempted_by` ring buffer** — already in `04-`:

```
attempted_by = array_append(
  CASE WHEN array_length(attempted_by,1) >= @max_attempted_by
       THEN attempted_by[array_length(attempted_by,1)+2-@max_attempted_by:]
       ELSE attempted_by END,
  @attempted_by)
```
(`river_job.sql:224-231`)

Note the asymmetry: River bounds *who attempted* but leaves *what went wrong*
(`errors`) unbounded (`river_job.sql:482`). A job with `max_attempts: 25`
retrying against a service returning a 4 KB error body accumulates 100 KB of
`jsonb[]` on a single row. Both need the ring buffer.

**que truncates in SQL, not in the client** (`job_methods.rb:30-31`). The bound
holds no matter which language enqueued or acked the job. For a polyglot
processor this is the only place the bound *can* live — a Python worker will
not honour a Rust-side constant.

**solid_queue computes the backtrace budget from the column limit**
(`failed_execution.rb:60-78`): it reads `columns_hash["error"].limit`,
subtracts the exception class name, the message and 256 bytes of JSON overhead,
then appends backtrace lines until the JSON exceeds the remainder. Nobody else
in the survey reasons about the error column as a *budget*.

**good_job's `error_event` enum** (`error_events.rb:7-12`) —
`interrupted / unhandled / handled / retried / retry_stopped / discarded`,
stored as a smallint on the row. This is the single most query-friendly error
artefact in the survey and is exactly what an MCP `failure_summary` tool needs
to cluster on. Everyone else forces a `LIKE '%...%'` over free text.

---

## 3. Dead letter / discarded jobs

Three models. Only one is a real DLQ.

**(a) Terminal state + TTL delete** — Oban, River, graphile-worker, que,
good_job, procrastinate.

- Oban: `state = 'discarded'`, `discarded_at` set
  (`basic.ex:237-252`). The `Oban.Pruner` is a **default** service plugin
  (`config.ex:52-57`) and hard-`DELETE`s completed/cancelled/discarded rows
  older than `max_age`, whose **default is 60 seconds**
  (`lib/oban/pruner.ex:75-80`, delete at `basic.ex:169-191`). Out of the box,
  Oban's dead-letter retention is one minute. Re-drive is `Oban.retry_job/1`
  against a row that has probably already been deleted.
- River: `state = 'discarded'`, cleaner deletes after
  `DiscardedJobRetentionPeriodDefault = 7 * 24h`
  (`rivershared/riversharedmaintenance/river_shared_maintenance.go:37`;
  cancelled/completed 24h at `:35-36`). Sane default. Re-drive via
  `JobRetry` / River UI.
- graphile-worker: no discarded state at all — a dead job is one where
  `attempts >= max_attempts`, which drops out of the `is_available` generated
  column (`sql/000011.sql:67`). It sits in the jobs table forever. Re-drive is
  `reschedule_jobs(..., attempts => 0)` (`sql/000011.sql:247-256`). Simple, but
  the live table and the graveyard are the same table and the same index.

**(b) Separate table** — solid_queue.
`SolidQueue::FailedExecution` (`app/models/solid_queue/failed_execution.rb`).
Re-drive is first-class: `#retry` resets the counters, re-prepares the job and
destroys the failed row (`:21-30`); `.retry_all` batches it (`:14-19`). The
failed rows are not pruned by default. This keeps the hot table small.

**(c) A real dead-letter queue** — pg-boss, and only pg-boss.
A queue may declare `dead_letter text REFERENCES queue(name) CHECK (dead_letter
IS DISTINCT FROM name)` (`src/plans.ts:312`). On terminal failure the job is
**re-enqueued as a new job on the DLQ**, carrying provenance columns
`source_name`, `source_id`, `source_created_on`, `source_retry_count`
(`src/plans.ts:2533-2559`). There is also `deadLetterJobsByIdWithOutputs`
(`:2581-2591`) to force a job straight to the DLQ, bypassing remaining retries.

This is the right design and the reason is structural: **the DLQ is a queue, so
re-driving is just working it.** Every consumer, dashboard, filter, rate limit
and alert that already exists for queues applies to the graveyard for free. The
other ten implementations all invent a second, weaker vocabulary for
"look at the dead ones".

Note the documented caveat at `src/plans.ts:2782-2784`: the DLQ copy inherits
the *destination queue's* options as configured now; per-job overrides from the
original `send()` are not preserved.

---

## 4. Poison jobs — the job that kills the worker

The question: a job that OOMs or segfaults the worker process rather than
raising. How is it detected, and what stops it being re-claimed forever?

**Most do conflate it with a normal exception. Verified.** But not all — and
the exceptions are more interesting than the rule.

### Conflated (rescue is indistinguishable from a raise)

- **Oban (OSS).** `Oban.Lifeline` transitions `executing` rows whose
  `attempted_at < now() - rescue_after` (default 1 hour) back to `available`,
  or to `discarded` if `attempt >= max_attempts`
  (`lib/oban/engines/basic.ex:194-214`, config at `lib/oban/lifeline.ex:33-40`).
  It writes **no error entry at all** — the `errors` array is untouched, so
  after the fact there is no record that the job was rescued rather than
  retried. Bounding comes only from `attempt`, which the claim query already
  incremented. The moduledoc is candid: *"Rescuing is purely based on time,
  rather than any heuristic about the job's expected execution time or whether
  the node is still alive"* and *"This plugin may transition jobs that are
  genuinely executing and cause duplicate execution"*
  (`lib/oban/lifeline.ex:3-17`).
- **graphile-worker.** `resetLockedAt.ts:23` nulls `locked_at`. No error, no
  counter, no distinction. Already covered in `04-`.
- **que, procrastinate, db-scheduler.** No worker-death path in source; a dead
  worker's row is recovered by advisory-lock release or a lease sweep and the
  next attempt looks like any other.

### Distinguished in the error text only

- **River.** The rescuer writes an `AttemptError` with
  `Error: "Stuck job rescued by JobRescuer"` and an empty `Trace`
  (`internal/maintenance/job_rescuer.go:241-246`), and increments
  `river:rescue_count` in the job's metadata
  (`river_job.sql:483-495`). Bounding is still `attempt < max_attempts`
  (`job_rescuer.go:367`). So a job that kills the worker every time gets
  discarded after `max_attempts` rescues — correct, but on the same budget as
  ordinary failures, and the `rescue_count` is recorded and then never read.
  Rescue is also gated on the per-kind timeout having actually elapsed
  (`job_rescuer.go:357-360`), which avoids rescuing a legitimately slow job.
- **celery.** `WorkerLostError` is a distinct exception type raised by the
  parent when a prefork child dies (`celery/worker/request.py:695`), and
  `task_reject_on_worker_lost` (default unset/false, `celery/app/defaults.py:308`)
  decides requeue-vs-fail. **Default is not to requeue** — the task is recorded
  as `FAILURE` with `WorkerLostError`. Safe against poison loops by default; it
  also silently loses legitimate OOM victims. Worse, `acks_late` defaults to
  `False` (`celery/app/defaults.py:267`), so in the default configuration the
  broker message is acked *before* execution and a hard worker kill loses the
  task with no row anywhere.

### Genuinely separated — two implementations, and they are the state of the art

**BullMQ keeps a second counter.** `stc` (stalled count) is incremented by
`HINCRBY` in the stall sweeper, entirely separately from `attemptsMade`, and
the comment states the intent outright: *"If this job has been stalled too many
times, such as if it crashes the worker, then fail it"*
(`src/commands/moveStalledJobsToWait-9.lua:74-97`). `maxStalledCount` defaults
to **1** (`src/classes/worker.ts:248`, documented at
`src/interfaces/worker-options.ts:63-70`) — one free crash, then
`failedReason = "job stalled more than allowable limit"`. A stall is only
counted if the job's lock key is actually gone (`:69`), so a live worker holding
its lock is never mistaken for a dead one. There is also `maxStartedAttempts`
(`worker-options.ts:54-61`, default unset), a hard cap on *starts* regardless
of outcome — a direct poison-job kill switch.

**solid_queue has a three-way worker-death taxonomy and refuses to retry any of
it.** Three distinct error classes, each carrying different evidence:

| class | meaning | evidence carried | file:line |
|---|---|---|---|
| `ProcessExitError` | supervisor reaped a fork with a bad exit status | **the exit status** (137 = OOM-kill) | `lib/solid_queue/fork_supervisor.rb:60,76` |
| `ProcessPrunedError` | heartbeat expired (`process_alive_threshold`, default 5 min; heartbeat every 60 s) | `last_heartbeat_at` | `app/models/solid_queue/process/prunable.rb:25`; `lib/solid_queue.rb:31-32` |
| `ProcessMissingError` | a claimed execution whose process row no longer exists (`scope :orphaned, -> { where.missing(:process) }`) | — | `app/models/solid_queue/claimed_execution.rb:7`; `lib/solid_queue/supervisor/maintenance.rb:32` |

And the policy, stated explicitly in `README.md:689`:

> Solid Queue deliberately doesn't retry these automatically: the job itself
> might be what's killing the process (for example, a job that exhausts the
> container's memory), and retrying it blindly would just kill the next worker
> too.

It also notes that `retry_on`/`rescue_from` *cannot* see these, because the
process that was running the job is gone — the error is recorded by a
**different** process after the fact. Recovery is opt-in, via a
`fail_many_claimed.solid_queue` subscription whose payload carries the error and
the affected job IDs (`app/models/solid_queue/claimed_execution.rb:55`).

**Conclusion for #4.** The suspicion in the brief is correct for the majority —
Oban OSS, graphile, que, procrastinate and db-scheduler conflate "worker died"
with "job raised". River and celery distinguish it in the error payload but
spend the same attempt budget. Only **BullMQ** (separate `stc` counter,
`maxStalledCount: 1`) and **solid_queue** (three typed process errors,
no auto-retry) treat it as a different failure class with a different policy.
`ProcessExitError` carrying the raw exit status is the single best artefact —
exit 137 is an unambiguous OOM signal and nothing else in the survey captures it.

---

## 5. Timeouts

| impl | enforced by | default | what happens to the row |
|---|---|---|---|
| **Oban** | worker process — `:timer.exit_after(timeout, TimeoutError)` kills the job process | `:infinity` (`worker.ex:523-525`) | normal failure path: `error_job`, `errors` appended with an `Oban.TimeoutError` entry, state → `retryable` |
| **River** | worker — Go `context` deadline | `JobTimeoutDefault = 1 * time.Minute` (`client.go:53,218-224`) | context cancelled; if the job ignores `ctx` nothing happens until `RescueStuckJobsAfter` |
| **asynq** | worker — context deadline | `defaultTimeout = 30 * time.Minute` (`client.go:340,408`) | same cooperative-cancellation caveat |
| **celery** | worker — **hard**: `task_time_limit` SIGKILLs the prefork child; `task_soft_time_limit` raises `SoftTimeLimitExceeded` inside it | both unset (`celery/app/defaults.py:317-321`) | `TimeLimitExceeded`; `acks_on_timeout` decides ack-vs-reject (`worker/request.py:696-702`) |
| **pg-boss** | **the database** — `failJobsByTimeout` sweeps `state='active' AND started_on + expire_seconds*interval '1s' < now()`, default `expire_seconds = 15 min` | 15 min (`src/plans.ts:106,2312-2320`) | row fails through the *normal* fail path — consumes a retry, backs off, and routes to the DLQ on exhaustion. `output` set to `{"value":{"message":"job timed out"}}` |
| **pg-boss (heartbeat)** | **the database** — `failJobsByHeartbeat` on `heartbeat_on + heartbeat_seconds` | opt-in (`heartbeat_seconds` nullable) | `output = {"value":{"message":"job heartbeat timeout"}}`; worker refreshes via `touchJobs` (`src/plans.ts:2333-2345`) |
| **bullmq** | neither — lock renewal + stall detection stand in (`stalledInterval` 30 s) | — | job moves back to `wait`, `stc` incremented |
| **graphile, que, solid_queue, procrastinate, db-scheduler, good_job, underway** | **not at all** | — | — |

Two things matter here.

**Worker-side timeouts in Go and Rust are cooperative and therefore advisory.**
River and asynq cancel a `context`; a job that does not select on `ctx.Done()`
runs to completion regardless. Only Oban (BEAM process kill) and celery
(SIGKILL of the prefork child) can actually *stop* work. Rust has the same
limitation as Go: `tokio` cannot preempt a blocking task. Any timeout Stampede
enforces in-process is a request, not a guarantee. The guarantee has to come
from the database sweep or from killing the OS process.

**River is the only one that documents the invariant.**
`client.go:364-366`: *"RescueStuckJobsAfter must be greater than JobTimeout.
Otherwise, jobs would become eligible for rescue while they're still running"* —
default 1 hour, or `JobTimeout + 1 hour` when `JobTimeout` exceeds an hour, with
a separate `JobStuckThreshold` (default 10 s past `JobTimeout`) driving a
`JobStuckHandler` callback (`client.go:208-216,364-368`). Nobody else states the
ordering constraint, and violating it is exactly how a lease-based system
double-executes.

**pg-boss's DB-side expiry is the only mechanism that is correct when the worker
is gone.** A worker-side timeout cannot fire if the worker was SIGKILLed. That
is why `failJobsByTimeout` and `failJobsByHeartbeat` route through the *same*
fail path as an exception — they consume a retry, they back off, and they
dead-letter on exhaustion. That is the behaviour to copy.

---

## 6. Instrumentation

### What each emits

| impl | mechanism | built-in metrics | built-in tracing |
|---|---|---|---|
| **Oban** | `:telemetry` — the most complete event taxonomy in the survey | no (bring `telemetry_metrics`) | no |
| **River** | typed Go subscription channel (`Subscribe`) + `riverlog` structured logs | **no** | **no** |
| **asynq** | — | **Prometheus collector**, `x/metrics/metrics.go` | no |
| **bullmq** | vendor-neutral `Telemetry` interface + `MetricNames`/`TelemetryAttributes` enums | **yes, named enum** | **yes, OTel-shaped** (`SpanKind`) |
| **celery** | signals (`celery/signals.py`) + the events protocol | no | no |
| **solid_queue** | `ActiveSupport::Notifications` + `LogSubscriber` | no | no |
| **good_job** | `ActiveSupport::Notifications` + the `error_event` enum column | no | no |
| **graphile, que, pg-boss, procrastinate, db-scheduler, underway** | logs / JS events / `tracing` spans | no | no |

### Oban's event taxonomy — the shape to copy

`lib/oban/telemetry.ex`. Job events (`:20-66`):

| event | measures | metadata |
|---|---|---|
| `[:oban, :job, :start]` | `:system_time` | `:conf`, `:job` |
| `[:oban, :job, :stop]` | `:duration`, `:memory`, `:queue_time`, `:reductions` | `:conf`, `:job`, `:state`, `:result` |
| `[:oban, :job, :exception]` | same | + `:kind`, `:reason`, `:stacktrace` |

`:state` is one of `:success / :failure / :cancelled / :discard / :snoozed`
(`telemetry.ex:47`) and `:kind` one of `:error / :exit / :throw`
(`telemetry.ex:51-57`). **`:queue_time` as a first-class measurement on every
job is the one to steal** — queue latency is the metric that answers "are we
falling behind" and almost nobody emits it per-job.

Beyond jobs, Oban emits `start|stop|exception` for every engine operation
individually — `fetch_jobs`, `insert_job`, `error_job`, `retry_job`,
`prune_jobs`, `rescue_jobs`, `stage_jobs`, `cancel_all_jobs`, … —
(`telemetry.ex:70-108`), plus notifier (`:128,147`), plugin (`:171-174`),
peer election (`:200`), queue shutdown (`:220`) and stager switch (`:236`)
events. The lesson is not the specific names; it is that **the control plane's
own operations are instrumented at the same fidelity as user jobs.** When
claiming is slow, Oban can tell you it is claiming that is slow. River cannot.

### BullMQ's metric names — the canonical list

`src/enums/telemetry-attributes.ts`, comment: *"Standard metric names for BullMQ
telemetry"*:

```
bullmq.queue.jobs            bullmq.jobs.completed
bullmq.jobs.failed           bullmq.jobs.delayed
bullmq.jobs.retried          bullmq.jobs.waiting
bullmq.jobs.waiting_children bullmq.job.duration
```

and ~38 span attributes (`bullmq.queue.name`, `bullmq.job.id`,
`bullmq.job.attempts.made`, `bullmq.job.failed.reason`,
`bullmq.worker.stalled.jobs`, `bullmq.worker.failed.jobs`, …). This is the only
documented, enumerated, stable metric-name list in the survey. Two observations:

- BullMQ deliberately does **not** depend on `@opentelemetry/api` — it defines
  its own `Telemetry`/`Tracer` interface with an OTel-compatible `SpanKind`
  (`src/enums/telemetry-attributes.ts:56-61`) and leaves the adapter to the
  user. Correct call for a library; correct call for us too.
- It uses a `bullmq.*` namespace, **not** the OTel `messaging.*` semantic
  convention. Repo-wide grep for `messaging.system` across all Postgres
  references, asynq, bullmq-core and celery returns hits **only** in BullMQ's
  Elixir port (`redis/bullmq/elixir/lib/bullmq/telemetry/opentelemetry.ex:83-86`,
  `queue.ex:1990-1992`, `worker.ex:1977-1980`). The OTel messaging convention
  exists and essentially nobody in this space implements it.

### asynq's Prometheus metrics — the essential set

`x/metrics/metrics.go:43-83`:

```
asynq_tasks_enqueued_total          {queue,state}
asynq_queue_size                    {queue}
asynq_queue_latency_seconds         {queue}   <- age of the oldest pending task
asynq_queue_memory_usage_approx_bytes {queue}
asynq_tasks_processed_total         {queue}
asynq_tasks_failed_total            {queue}
asynq_queue_paused_total            {queue}
```

`asynq_queue_latency_seconds` — *"Number of seconds the oldest pending task is
waiting in pending state to be processed"* (`metrics.go:55-59`) — is the single
most operationally useful metric in the survey and asynq is the only project
that ships it as a default gauge. Queue *depth* is ambiguous (10k fast jobs is
fine, 10 stuck jobs is not); queue *age* is not.

### solid_queue's event list

`SolidQueue.instrument(:...)` call sites: `claim`, `release_claimed`,
`release_many_claimed`, `fail_many_claimed`, `release_blocked`,
`release_many_blocked`, `retry`, `retry_all`, `discard`, `discard_all`,
`dispatch_scheduled`, `enqueue_recurring_task`, `start_process`,
`shutdown_process`, `replace_fork`, `replace_thread`, `fork_boot_timeout`,
`graceful_termination`, `immediate_termination`, `thread_error`,
`prune_processes`, `deregister_process`, `finish_batch`.

Worth noting: `replace_fork`, `replace_thread` and `fork_boot_timeout` are
**supervisor-health** events, not job events. Nothing else instruments
"a worker died and I replaced it" as a first-class signal.

### celery's signal list

`celery/signals.py:31-115`: `before_task_publish`, `after_task_publish`,
`task_received`, `task_prerun`, `task_postrun`, `task_success`, `task_retry`,
`task_failure`, `task_internal_error`, `task_revoked`, `task_rejected`,
`task_unknown`, `task_sent`, plus worker lifecycle. `task_rejected` and
`task_unknown` are unusual and useful — they cover "a message arrived that no
worker can handle", a failure mode invisible in every other system.

---

## 7. Where observability is genuinely weak

Verified gaps, in descending order of how badly they hurt an on-call engineer.

1. **Retry *schedule* is invisible.** Every implementation stores
   `scheduled_at`/`run_at` for the next attempt and *nothing* about the
   remaining schedule. "When will this stop retrying, and what is the total
   wall-clock before it dead-letters?" requires re-implementing the backoff
   formula by hand. Underway is the only one that even persists the policy
   parameters on the row (`src/queue.rs:845-846`), and it still does not expose
   the resulting schedule.

2. **Failure clustering does not exist.** The universal question is "what are
   my top 10 failure signatures right now". Every system except good_job stores
   failures as free text, so answering it means `LIKE '%...%'` over an
   unindexed `text` or `jsonb[]` column across the whole jobs table.
   good_job's `error_event` enum (`error_events.rb:7-12`) is a 6-value coarse
   classification, not a signature. **Nobody fingerprints errors.**

3. **The control plane is uninstrumented.** Oban is the exception
   (`telemetry.ex:70-108`); River, graphile, pg-boss, que, solid_queue and
   procrastinate emit nothing about claim latency, rescuer throughput, or
   scheduler lag. When the *queue* is the problem rather than the jobs, there is
   no data.

4. **Queue age is not a default metric.** asynq ships
   `asynq_queue_latency_seconds`; nobody else does. Everyone ships depth,
   which is the less useful of the two.

5. **Worker death is not a metric.** solid_queue instruments `replace_fork` /
   `fork_boot_timeout`; River counts rescues in `river:rescue_count` metadata
   and never surfaces it; everyone else drops it. "Are my workers being
   OOM-killed" is unanswerable from inside every one of these systems.

6. **No system correlates failures with a downstream dependency.** Jobs fail
   because a third-party API is down. Not one of the twelve models
   "destination" as a first-class concept, so the dashboard cannot say
   *"87% of current failures are the payments provider"* — which is the actual
   answer the operator needs.

7. **No natural-language / programmatic interrogation surface.** Confirms
   `06-`. Every system's answer to "why does this keep failing" is a web UI and
   ad-hoc SQL.

Gaps 1, 2, 5 and 6 are where the MCP control plane in `06-` differentiates, and
they are differentiators precisely because they need schema support that has to
be designed in from the start, not bolted on.

---

## What Stampede should do

### Retry and backoff — `chrono-machines`

- **Full jitter as the shipped default.** `delay = rand(0, min(cap, base*2^n))`.
  Nobody in the survey defaults to it; underway's equal jitter is the closest.
  This is a real, defensible, one-line-of-marketing advantage and it is free
  because the crate already implements it.
- **Cap the backoff.** Every non-capped formula (`n^4` in River/asynq/que/neoq,
  `e^n` in graphile) produces absurd tails. Underway's `max_interval_ms`
  (default 60 s, `retry_policy.rs:56`) and Oban's `max_pow` clamp
  (`backoff.ex:34`) are the right shape. Cap, then jitter — in that order,
  which is what underway does (`retry_policy.rs:45-48`).
- **Persist the policy on the row**, underway-style — coefficient, cap, jitter
  factor, strategy — so the dashboard and MCP can *render the remaining
  schedule* without re-deriving it. This closes weakness (1) and is nearly free
  once the columns exist.
- **The clock is injectable**, River-style (`coalesce($now, now())`), already
  decided in `04-`. Extend it to backoff computation so the schedule is
  testable.

### Circuit breaking — `breaker-machines`

This maps onto weakness (6), and it is the piece no competitor has.

- **`destination` is a first-class column on the job row**, not a tag inside
  `args`. A breaker keyed on destination gives: per-destination failure rates
  in the dashboard, `failure_summary` grouped by destination in MCP, and
  automatic suppression of retries against a dependency that is already down.
- When a breaker is **open**, do not burn attempts. Jobs bound for an open
  destination should be re-scheduled without incrementing `attempt` — the
  semantics of Oban's `snooze` (`basic.ex:272-283`, which *decrements* attempt)
  and River's `EventKindJobInterrupted`, which explicitly *"does not consume an
  attempt or add an attempt error"* (`event.go:26-29`). **A failure caused by
  our own backpressure must not count against the job's retry budget.** This is
  the correctness rule that makes breakers safe to enable by default.
- Breaker state transitions are first-class dashboard and MCP surfaces
  (already in `06-`).

### Rate limiting — `throttle-machines`

- Rescue and retry **storms** need throttling as much as normal work does.
  River's rescuer already sleeps a random `BatchBackoffMin..Max` between
  batches (`job_rescuer.go:295`) — the primitive belongs in the crate, applied
  uniformly to claim, rescue, prune and re-drive.
- A DLQ re-drive of 100k jobs is a self-inflicted stampede. `job_retry` over a
  matched set (`06-`) must be rate-limited by construction, not by operator
  discipline.

### Lifecycle — `state-machines`

Model the *cause* of a transition, not just the transition. This is the thing
the survey is weakest on and it costs one column.

- `available → claimed → executing → {completed, retryable, discarded, cancelled}`
  as already decided in `04-`, plus a **typed `failure_kind`** on every
  transition into `retryable`/`discarded`:

  | kind | analogue |
  |---|---|
  | `raised` | ordinary exception |
  | `timed_out` | pg-boss `failJobsByTimeout`; Oban `TimeoutError` |
  | `heartbeat_lost` | pg-boss `failJobsByHeartbeat`; solid_queue `ProcessPrunedError` |
  | `worker_exited` | solid_queue `ProcessExitError` — **carry the exit status**, 137 = OOM |
  | `worker_missing` | solid_queue `ProcessMissingError`; River rescuer |
  | `breaker_open` | ours; does not consume an attempt |
  | `cancelled` | operator action |

  good_job's `error_event` enum proves the query-ability is worth it;
  solid_queue proves the taxonomy is derivable in practice. Nobody has both.

- **Two counters, not one.** `attempt` for raises; `interruptions` for the
  worker-death kinds — BullMQ's `stc` (`moveStalledJobsToWait-9.lua:75`) with
  `maxStalledCount: 1` (`worker.ts:248`). Default: **one free interruption,
  then discard**, with solid_queue's reasoning (`README.md:689`) written into
  the schema comment. A job that kills workers must not be allowed to walk the
  whole fleet.
- Guards on transitions out of `executing` are the natural home for
  "cannot complete a job whose lease expired" — the bug graphile guards against
  by hand in three separate predicates (`04-`).

### Error capture — schema

- **Structured, not formatted.** `{at, attempt, kind, class, message,
  backtrace[], failure_kind, destination}`. Oban's `Exception.format/3` blob
  (`job.ex:587`) is human-readable and machine-useless.
- **Bounded, and bounded in SQL.** que proves it
  (`left($2,500)` / `left($3,10000)`, `job_methods.rb:30-31`); solid_queue
  proves the budget should be computed rather than guessed
  (`failed_execution.rb:60-78`). For a polyglot processor where a Python worker
  might ack the job, the truncation **must** live in the claim/ack SQL function
  — a Rust-side constant is unenforceable. This is a concrete argument for the
  "claim/ack as SQL functions" option still open in `04-`.
- **Ring-buffer the history**, River-style (`river_job.sql:224-231`), and apply
  it to `errors` as well as `attempted_by` — River bounds only the latter
  (`river_job.sql:482` appends errors unbounded), which is half a fix.
- **Store an error fingerprint** (normalised class + message with numbers/UUIDs
  stripped, hashed) as an indexed column. This is weakness (2) and it is the
  difference between `failure_summary` being instant and being a seq scan.
  Nobody does this.

### Dead letter

**Copy pg-boss: the DLQ is a queue** (`plans.ts:312,2533-2559`). A terminal
failure re-enqueues onto the configured dead-letter queue with provenance
(`source_id`, `source_queue`, `source_attempts`). Re-drive is then just working
that queue — every existing tool applies, and `job_retry` in MCP becomes
"move from DLQ back to origin" rather than a bespoke mutation.

Retention: River's defaults are the sane ones — 24 h completed / 24 h cancelled
/ **7 days discarded** (`river_shared_maintenance.go:35-37`). Emphatically not
Oban's 60-second `max_age` (`pruner.ex:79`), which deletes the evidence before
anyone is paged.

### Timeouts

Three layers, because the first two cannot fire when the worker is dead.

1. **Cooperative, in-process** — a tokio cancellation token per job, River's
   `JobTimeout` (`client.go:53`, 1 min default). Advisory: Rust cannot preempt
   a blocking task any more than Go can.
2. **Database-enforced expiry** — pg-boss's `expire_seconds` +
   `failJobsByTimeout` (`plans.ts:2312-2320`). Fires regardless of worker
   liveness, routes through the **normal** fail path so it consumes a retry,
   backs off and dead-letters. Set `failure_kind = timed_out`.
3. **Heartbeat** — pg-boss's `touchJobs`/`failJobsByHeartbeat`
   (`plans.ts:2322-2345`) for long jobs, plus the `stampede_workers` process
   heartbeat from `04-` for the worker itself (solid_queue:
   `process_heartbeat_interval` 60 s, `process_alive_threshold` 5 min,
   `lib/solid_queue.rb:31-32`). Set `failure_kind = heartbeat_lost`.

**Enforce River's invariant in the schema, with a CHECK constraint if
possible:** `rescue_after > job_timeout` (`client.go:364-366`). Violating it is
the mechanism by which lease systems double-execute, and it is currently
enforced nowhere except a Go doc comment.

If the daemon supervises worker processes, capture the **exit status** on
abnormal exit (`fork_supervisor.rb:60,76`) and store it. It is the cheapest,
highest-signal artefact in the entire survey.

### Instrumentation

- **Vendor-neutral trait, OTel adapter behind a feature flag.** BullMQ's call
  (`src/enums/telemetry-attributes.ts:56-61`) — do not take a hard
  `opentelemetry` dependency in the core crate.
- **Publish an enumerated, stable metric-name list**, BullMQ-style
  (`MetricNames` enum), in the schema comments and the docs. Proposed essential
  set, merging asynq's gauges with Oban's per-job measures:

  ```
  stampede.queue.depth              {queue,state}
  stampede.queue.oldest_age_seconds {queue}        <- asynq's queue_latency; the one that matters
  stampede.job.queue_time_seconds   {queue,task}   <- Oban's :queue_time
  stampede.job.duration_seconds     {queue,task}
  stampede.jobs.completed           {queue,task}
  stampede.jobs.failed              {queue,task,failure_kind}
  stampede.jobs.retried             {queue,task}
  stampede.jobs.discarded           {queue,task}
  stampede.jobs.interrupted         {queue,task,failure_kind}   <- nobody has this
  stampede.worker.heartbeat_age_seconds {worker}
  stampede.worker.replaced          {reason}       <- solid_queue's replace_fork
  stampede.claim.duration_seconds   {queue}        <- control-plane, Oban-style
  stampede.rescuer.jobs             {outcome}
  stampede.breaker.state            {destination}  <- ours alone
  stampede.throttle.headroom        {queue,tenant} <- ours alone
  ```

  `failure_kind` as a label on `jobs.failed` is what makes "is this an outage or
  a bad deploy" answerable from a dashboard rather than from SQL.
- **Instrument the control plane at job fidelity** (Oban's engine events,
  `telemetry.ex:70-108`). Claim, stage, rescue, prune, notify.
- Emit `task_rejected` / `task_unknown` equivalents (`celery/signals.py:80-88`)
  — "a job arrived for a task no worker handles" is a real production failure
  and it is invisible everywhere else. River discards such jobs with only a log
  line (`job_rescuer.go:337-340`).

---

## Corrections to existing notes

Both verified against source; both change an argument currently in the plans.

**1. `02-crate-family.md` — "GoodJob's `:polynomially_longer` has no jitter, so
a batch of jobs that fail together retry together" is wrong.**

ActiveJob applies jitter: `delay + (Kernel.rand * delay * jitter) + 2`
(`activejob/lib/active_job/exceptions.rb:161-184`) with `retry_jitter` set to
`0.15` by `config.load_defaults 6.1` and later
(`railties/lib/rails/application/configuration.rb:189`). The docstring at
`exceptions.rb:30` states the formula outright.

The claim should be replaced with the accurate and still-strong version:
**additive 15% jitter is a narrowing, not a decorrelation.** A thousand jobs
that fail at t=0 with `executions=3` retry across `[83s, 95s]` — a 12-second
window. Full jitter spreads the same herd across `[0s, 83s]`. The argument for
`chrono-machines` survives; the factual claim as written does not, and it is the
kind of error a competent reader will catch.

**2. `04-job-lifecycle-and-claiming.md` — the Oban row in the orphan-recovery
table describes Oban *Pro*, not Oban OSS.**

The table says Oban uses *"`attempted_by = [node, uuid]` + producer heartbeat
table; rescuer requeues jobs whose producer stopped reporting"*. In the cloned
Apache-licensed source, `Oban.Lifeline` is a **pure time lease** on
`attempted_at`, functionally identical to the Graphile mechanism the same table
criticises:

```elixir
where([j], j.state == "executing" and j.attempted_at < ^cut)   # basic.ex:202
```
(`lib/oban/engines/basic.ex:194-214`; `rescue_after` default 1 hour,
`lib/oban/lifeline.ex:37-40`). The moduledoc says so: *"Rescuing is purely based
on time, rather than any heuristic about ... whether the node is still alive"*
and *"This plugin may transition jobs that are genuinely executing and cause
duplicate execution"* — pointing at `Oban.Pro.Lifeline` for the accurate version
(`lib/oban/lifeline.ex:3-17`).

The **decision in `04-` is still correct** — heartbeat over lease — but the
provenance is wrong, and it matters because the OSS prior art for the chosen
design is **solid_queue**, not Oban:

- `solid_queue_processes` with `last_heartbeat_at`, heartbeat every 60 s,
  alive threshold 5 min (`lib/solid_queue.rb:31-32`)
- `scope :prunable, -> { where(last_heartbeat_at: ..threshold.ago) }`
  (`app/models/solid_queue/process/prunable.rb:9`)
- `scope :orphaned, -> { where.missing(:process) }` — a claimed execution whose
  process row is gone (`app/models/solid_queue/claimed_execution.rb:7`)

That is the heartbeat model `04-` decided on, implemented in MIT-licensed
source we can read. It should be cited there instead of Oban.

Corollary worth carrying into `04-`: the *claim* is not where orphan recovery
lives. It lives in the relationship between `stampede_workers` and the claim
row, and solid_queue's `where.missing(:process)` is the cheapest expression of
it — a foreign key and a `LEFT JOIN ... IS NULL`, no time arithmetic, no
wall-clock guess.

---

## Still open

- **Error fingerprinting algorithm.** Normalise-and-hash is easy to describe
  and easy to get wrong (numbers, UUIDs, IPs, file paths, line numbers). Does
  it live in the daemon or in a SQL function? The polyglot argument says SQL;
  the "strip a UUID with a regex" argument says the daemon. Unresolved.
- **Is `destination` one column or a set?** A job may call two services. One
  column is cheap and covers the 90% case; an array or a join table is correct
  and complicates the breaker key and the index. Leaning one column plus an
  escape hatch.
- **Does a `breaker_open` reschedule count toward any budget at all?** If it
  counts toward nothing, a permanently-open breaker parks jobs forever and the
  queue silently grows. Probably needs a separate `deferrals` counter with its
  own (generous) cap, which is a third counter — is that one too many?
- **Interruption budget default.** BullMQ ships `maxStalledCount: 1`. Is one
  free interruption right when the most common cause is a routine deploy
  rather than an OOM? A graceful shutdown should not consume the budget at all
  (River's `EventKindJobInterrupted`, `event.go:26-29`) — so the budget only
  applies to *ungraceful* death, which argues for keeping it at 1. Needs the
  shutdown path designed first.
- **Error-column budget.** que picks 500 / 10000 bytes
  (`job_methods.rb:30-31`). solid_queue computes it from the column limit.
  We have no column limit on `text`/`jsonb`, so the budget is a policy choice.
  Default? Configurable per queue?
- **Ring-buffer depth for `errors`.** River bounds `attempted_by` only, and
  `max_attempted_by` is caller-supplied. Keep the first N and the last M
  (the first failure and the recent ones are what matter; the middle is
  noise) — or a flat last-N? The first-error-plus-last-N shape is not used by
  anyone in the survey, which is either an opportunity or a warning.
- **Does the DLQ share the jobs table or get its own?** pg-boss re-inserts into
  the same partitioned table; solid_queue uses a separate table. Partitioning
  is already blocked on the `pg_partman` question in `04-`, and this decision
  depends on it.
- **Do we ship a Prometheus endpoint, an OTel exporter, or both?** asynq ships
  a separate `x/metrics` module rather than baking it in
  (`redis/asynq/x/metrics/metrics.go`) — a good pattern for keeping the core
  dependency-light, and it interacts with the "does the daemon serve the
  dashboard directly" question still open in `06-`.
