# Stampede

A polyglot job processor. Rust core, Postgres 18+ substrate, Datastar dashboard,
MCP control plane. BSD-3.

**Status: working end to end.** Schema, worker, daemon, HTTP edge, live
dashboard and MCP control plane are built and verified against Postgres 18.
Not yet production-hardened — see *Not done yet* below.

## Why

Every serious job processor is locked to one runtime — Sidekiq is Ruby, Oban is
Elixir, Celery is Python, BullMQ is Node. Stampede's substrate is Postgres, so
**enqueue is a plain `INSERT` and every language already has a client**:

```sql
INSERT INTO stampede_jobs (queue, task, args)
VALUES ('mail', 'send_welcome', '{"user_id": 42}'::jsonb);
```

No wire protocol, no client library to maintain, and the job commits in the same
transaction as the business data that caused it.

## What is different

Stampede was designed after surveying thirteen job processors in six languages
(`plans/08-reference-survey.md`). Most of the design is a response to something
measured in one of them:

- **Attempt counter increments at claim, not at failure.** Half the surveyed
  implementations have an unbounded poison loop because a job that kills its
  worker never reports, so its attempt budget never advances. (`plans/04`)
- **Full jitter by default.** Of thirteen implementations, *none* defaults to
  full jitter; Celery's default retry is a flat 180 s with no randomness at all.
  A batch of jobs failing together retries together — the stampede this is named
  for. (`plans/15`)
- **Heartbeat, not a time lease.** Every other Postgres queue surveyed reclaims
  orphans on an elapsed-time guess (Oban 60 min, Graphile 4 h, pg-boss 15 min).
  A job slower than the guess runs twice. (`plans/04`)
- **"Worker died" is a distinct failure kind** from "the job raised". Only two of
  thirteen make that distinction, and it is what makes a poison job diagnosable.
- **Retry delays are delays, not timestamps**, so retry timing does not depend on
  the app server's clock agreeing with the database's. (`plans/15`)

## Running it

```bash
createdb stampede
stampede --migrate --database-url postgres://localhost/stampede \
         --queue default --http 127.0.0.1:7878
```

That single process runs the worker, the reaper, the pruner, the HTTP API and
the dashboard. Split them later by running the same binary with different flags
(`--no-worker`, `--no-maintenance`) — there is no second binary to deploy.

- **dashboard** — <http://127.0.0.1:7878/dashboard>, live over SSE
- **API** — `POST /jobs`, `GET /queues/{queue}/depth`, `GET /health`
- **MCP** — `stampede-mcp` over stdio; point any MCP client at it

Handlers are registered in Rust (`crates/stampede-core/examples/worker.rs`), but
**enqueueing needs no Rust and no client library** — any language with a
Postgres driver can `INSERT`.

## Layout

```
plans/     design notes — what was measured, and why each decision was made
schema/    001_initial.sql, 002_operations.sql  (PG18+ only)
crates/
  stampede-core     worker, queue, job types, maintenance loops
  stampede-daemon   the `stampede` binary
  stampede-http     enqueue API + Datastar dashboard, on rama
  stampede-mcp      MCP control plane, on mcp-host
```

## MCP control plane

Five read tools — `queue_status`, `job_inspect`, `job_search`,
`failure_summary`, `worker_status` — plus two guarded write tools, `job_retry`
and `job_cancel`.

`failure_summary` clusters by error signature rather than listing instances:
"47 jobs failed" is noise, "47 failed with the same connection timeout" is a
diagnosis.

Write tools have two independent gates. They are only mounted when writes are
enabled, *and* `mcp-host` evaluates visibility per session against an `operator`
role — so a read-only session never sees them in `tools/list` at all. Being
invisible beats being refused: a model cannot be argued into calling a tool it
was never shown.

## Recurring schedules

The schedule row *is* the due row. `next_run_at` lives on the row and is claimed
with `FOR UPDATE SKIP LOCKED`, so **there is no leader election for cron** — the
row lock is the election, and every node can tick concurrently.

That model also gives, for free: a missed occurrence runs **late, once** (the
overdue row is still there); **no drift**, because the next time is computed
from the previous target rather than from `now()`; and schedules that are
queryable and mutable at runtime, which Oban, River, good_job and procrastinate
cannot offer because their schedules live in config.

**DST is a per-schedule column, not an algorithm.** Every surveyed
implementation keys occurrences on a UTC instant, so a 02:30 job runs *twice* on
a fall-back day. Set `dst_policy = 'wall_clock'` and the duplicated hour
collapses into one token, so it runs once.

A separate occurrence ledger is the idempotency backstop. It is deliberately its
own narrow table: good_job and solid_queue put the cron key on the jobs table
and then had to guarantee those rows are never pruned, which couples cron
correctness to your retention horizon.

## Fairness and circuit breaking

Both on by default.

**Per-tenant GCRA admission** (`throttle-machines`) so one tenant enqueueing
100k jobs cannot starve the rest. Applied *after* claiming, because claim order
is by priority and age — the flood is legitimately first in line, so execution
is the only honest place to intervene. Rate limiting is a paid feature in both
Sidekiq Enterprise and Oban Pro.

**Per-task circuit breakers** (`breaker-machines`) so a failing downstream is
not hammered by every retrying job. An open circuit fails fast *without* calling
the handler, and the job is recorded `interrupted` rather than `raised` — a sick
dependency is not a defect in the job, and conflating them would make one outage
look like a thousand broken jobs.

## Not done yet

- Cron *expressions* — schedules are interval-based today (the parser belongs in
  Rust; it will set `next_run_at` the same way)
- MCP tools for managing schedules
- Batched `UNWIND` writes — deliberately deferred, see `plans/14`

## Requirements

Postgres **18 or newer**, deliberately. Stampede uses `uuidv7()`,
`RETURNING OLD./NEW.` and generated columns; there are no compatibility shims.

## Testing

```bash
cargo test                                   # unit
createdb stampede_test
STAMPEDE_TEST_DATABASE_URL=postgres://localhost/stampede_test \
  cargo test --test integration              # against real Postgres
```

Integration tests run in parallel, each in its own schema.

## Licence

BSD-3-Clause.
