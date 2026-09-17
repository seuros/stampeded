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

**Workers are polyglot too.** Claiming carries the fence, the attempt accounting
and the ownership rules, so it must not be reimplemented per language — but it
does not have to be, because it is a SQL function. A complete worker in Ruby,
with no Stampede client library anywhere:

```ruby
require 'pg'
require 'json'

db = PG.connect(ENV['DATABASE_URL'])

worker = db.exec_params(
  "INSERT INTO stampede_workers (hostname, pid, queues) VALUES ($1,$2,$3) RETURNING id",
  ['ruby-worker', Process.pid, '{default}']
).getvalue(0, 0)

loop do
  db.exec_params("SELECT stampede_heartbeat($1)", [worker])
  db.exec_params("SELECT * FROM stampede_claim($1,$2,$3)", ['default', worker, 5]).each do |job|
    handle(JSON.parse(job['args']))
    db.exec_params("SELECT stampede_complete($1,$2,$3)", ['default', job['id'], worker])
  end
  sleep 0.5
end
```

That is the whole integration. The pitch is not "our protocol is better than
Faktory's" — it is that **there is no protocol**.

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
- **MCP** — `stampede-mcp` over stdio; point any MCP client at it
- **API** — for callers that cannot hold a Postgres connection (browsers, edge
  functions, third parties). Everyone else should just `INSERT`.

  | | |
  |---|---|
  | `POST /jobs` | enqueue |
  | `GET /queues/{queue}/jobs/{id}` | job state |
  | `GET /queues/{queue}/jobs/{id}/events` | state transitions over SSE, ends when the job finishes |
  | `GET /queues/{queue}/depth` | depth by state |
  | `GET /health` | |

Rust handlers are registered in-process
(`crates/stampede-core/examples/worker.rs`); workers in other languages use
`stampede_claim` as shown above. Neither needs a client library.

## Layout

```
plans/     design notes — what was measured, and why each decision was made
schema/    001_initial, 002_operations, 003_schedules  (PG18+ only)
crates/
  stampede-core     worker, queue, job types, maintenance loops
  stampede-daemon   the `stampede` binary
  stampede-http     enqueue API + Datastar dashboard, on rama
  stampede-mcp      MCP control plane, on mcp-host
```

## MCP control plane

Six read tools — `queue_status`, `job_inspect`, `job_search`,
`failure_summary`, `worker_status`, `schedule_list` — plus four guarded write
tools: `job_retry`, `job_cancel`, `schedule_upsert` and `schedule_pause`.

Schedules being editable at runtime is the point of putting them in a table:
elsewhere changing one is a deploy, here it is a tool call.

`failure_summary` clusters by error signature rather than listing instances:
"47 jobs failed" is noise, "47 failed with the same connection timeout" is a
diagnosis.

Write tools have two independent gates. They are only mounted when writes are
enabled, *and* `mcp-host` evaluates visibility per session against an `operator`
role — so a read-only session never sees them in `tools/list` at all. Being
invisible beats being refused: a model cannot be argued into calling a tool it
was never shown.

## Recurring schedules

Cron expressions or fixed intervals:

```sql
INSERT INTO stampede_schedules (key, queue, task, cron_expr, timezone, dst_policy)
VALUES ('nightly', 'reports', 'daily_summary', '0 3 * * *', 'America/New_York', 'wall_clock');
```

The schedule row *is* the due row. `next_run_at` lives on the row and is claimed
with `FOR UPDATE SKIP LOCKED`, so **there is no leader election for cron** — the
row lock is the election, and every node can tick concurrently. The whole tick
is one transaction, so the lock is still held while Rust computes the next
occurrence.

That model also gives, for free: a missed occurrence runs **late, once** (the
overdue row is still there); **no drift**, because the next time is computed
from the previous target rather than from `now()`; and schedules that are
queryable and mutable at runtime, which Oban, River, good_job and procrastinate
cannot offer because their schedules live in config.

**DST is a per-schedule column, not an algorithm.** Every surveyed
implementation keys occurrences on a UTC instant, so a 01:30 job runs *twice* on
a fall-back day. Stampede makes it a choice:

| | fall-back (01:30 happens twice) | spring-forward (02:30 does not exist) |
|---|---|---|
| `wall_clock` | runs **once** | runs at 03:30, when the gap closes |
| `instant` | runs **twice**, an hour of real time apart | — |

`wall_clock` is what you want for "the daily report at 02:30"; `instant` for
"every two hours", which should run 13 times on a 25-hour day, not 12. Omit the
column and the expression decides: one that *names* its hours gets `wall_clock`,
one using `*` or a step gets `instant`.

Timezone arithmetic is [jiff](https://docs.rs/jiff), not a cron crate. The
popular ones are chrono-based, which would mean jiff never touches the
arithmetic — and none of them implement the split above. The expression parser
is ~300 lines; the hard part was never the grammar.

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

- Batched `UNWIND` writes — deliberately deferred, see `plans/14`
- Daemon-mediated polyglot workers (claim handed out over HTTP/SSE, for workers
  across a trust boundary). The SQL-function path above covers in-cluster
  workers, which is the common case.
- Not production-hardened: no load testing at scale, no TLS on the HTTP edge,
  no auth on the API — put it behind something that has both.

## Requirements

Postgres **18 or newer**, deliberately. Stampede uses `uuidv7()`,
`RETURNING OLD./NEW.` and generated columns; there are no compatibility shims.

## Testing

```bash
cargo test                                   # unit only
createdb stampede_test
STAMPEDE_TEST_DATABASE_URL=postgres://localhost/stampede_test cargo test
```

With the env var set that is 88 tests: unit, integration against real Postgres,
and wire-level tests that run a real server on an ephemeral port. Integration
tests run in parallel, each in its own schema.

## Licence

BSD-3-Clause.
