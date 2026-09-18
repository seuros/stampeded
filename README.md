# Stampede

A polyglot job processor. Rust core, Postgres 18+ substrate, Datastar dashboard,
MCP control plane. BSD-3.

**Status: working end to end, and every design decision in `plans/` is
implemented.** Verified against Postgres 18 with 141 tests, including
integration tests against a real database and wire-level tests against a real
server. Not yet load-tested at scale — see *Not done yet* below.

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
- **Our own backpressure does not burn retries.** When a circuit is open or a
  tenant is throttled, the attempt spent at claim is *refunded*. Without that, a
  long enough downstream outage walks every queued job's budget down and
  discards the whole backlog instead of replaying it. (`plans/15`)
- **Two budgets, not one.** `attempt` counts raises; `interruptions` counts
  worker deaths, defaulting to one free. A job that segfaults its worker stops
  after the second claim instead of walking twenty-five workers down.
- **Jobs resume.** A job that dies at row 50,000 of 100,000 gets its cursor
  back instead of starting again at row 1. Of the Postgres cohort only River
  has this, and [River's version is richer](plans/17-job-resume.md) — named
  steps, fast-forwarded on resume. Ours is a single cursor. (`plans/17`)
- **Failures cluster by fingerprint**, with numbers, UUIDs and hex stripped
  before hashing — so "timed out after 30012ms" and "timed out after 29998ms"
  are one incident rather than two. Nobody else does this, and it is the
  difference between a failure summary being useful during an incident and
  being a list. (`plans/15`)

## Running it

```bash
createdb stampede
stampede --migrate --database-url postgres://localhost/stampede \
         --queue default --http 127.0.0.1:7878
```

That single process runs the worker, the reaper, the pruner, the HTTP API and
the dashboard. Split them later by running the same binary with different flags
(`--no-worker`, `--no-maintenance`) — there is no second binary to deploy.

- **dashboard** — <http://127.0.0.1:7878/dashboard>, live over SSE.
  **Off unless you pass `--dashboard-token`**, deliberately: a job dashboard
  exposes customer data in job args and offers a retry button, and opt-in auth
  is how dashboards end up open on production.
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

## Rails

```ruby
gem "stampede"
gem "stampede-daemon"   # optional: the precompiled binary
```

```ruby
config.active_job.queue_adapter = :stampede
```

Two processes: `bin/rails stampede:work` runs your jobs, `bin/rails
stampede:daemon` runs the scheduler, reaper, pruner and dashboard. They are
split because the Rust daemon cannot execute an ActiveJob job — an ActiveJob
job is a Ruby class — while every other role operates on *rows* and so does not
care what language wrote them. See `ruby/README.md` and `plans/16`.

`retry_on` raises: it re-enqueues as a new row while Stampede counts attempts
at claim, which would make every attempt column read 1. Use
`stampede max_attempts: 5` instead.

## Layout

```
plans/     design notes — what was measured, and why each decision was made
schema/    001_initial, 002_operations, 003_schedules  (PG18+ only)
crates/stampede/  one crate, features gate the weight
  src/            queue, worker, lifecycle, cron, wakeup, maintenance, metrics
  src/http/       enqueue API + Datastar dashboard, on rama   [feature: http]
  src/mcp/        MCP control plane, on mcp-host              [feature: mcp]
  src/bin/        `stampede` (daemon) and `stampede-mcp`
ruby/             the `stampede` gem: ActiveJob adapter, worker, schedules
ruby-daemon/      the `stampede-daemon` gem: one binary per platform
```

One crate rather than a workspace of four, because the four bought nothing:
nobody had asked to depend on the pieces separately, and it cost four
publishes and four version numbers. Features carry the weight instead, and
they carry a lot of it — the queue core alone locks **374** crates, everything
locks **958**. An application embedding the queue should not compile rama:

```bash
cargo install stampede                       # the daemon, batteries included
cargo add stampede --no-default-features     # just the queue
```

All four `*-machines` crates are load-bearing: `state-machines` holds the
lifecycle transition table, `chrono-machines` the backoff, `breaker-machines` a
circuit per destination, and `throttle-machines` both tenant fairness and
wakeup pacing.

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

## Resumable jobs

```ruby
def perform(file)
  cursor = stampede_checkpoint&.dig("cursor") || 0

  rows[cursor..].each_slice(1_000) do |chunk|
    import(chunk)                          # commit the work FIRST
    cursor += chunk.size
    stampede_checkpoint!(cursor: cursor)   # then record it
  end
end
```

Or by **named step**, which is what you want when a job has phases that must
not be repeated:

```ruby
def perform(order)
  stampede_step("charge") { Stripe.charge(order.total) }    # never charged twice
  path = stampede_step("render") { render_pdf(order) }      # value kept on resume

  stampede_step("email", cursor: 0) do |step|
    recipients[step.cursor..].each do |to|
      Mailer.receipt(to, path).deliver_now
      step.advance!(step.cursor + 1)
    end
  end
end
```

On resume, completed steps are skipped and the one that was running picks up
from its cursor. This is River's model, which is the prior art and better than a
single blob — with one departure: a step's return value is kept and handed back
when the step is skipped, so `path = step { ... }` is not silently nil the
second time round. Results over 8 KB raise; store the big thing yourself and
return a reference.

A checkpoint is otherwise opaque to Stampede — your cursor, your meaning. It
rides on the row `stampede_claim` already returns, so resuming costs no extra
round trip.

Steps work the same from Rust:

```rust
ctx.step("charge", async { charge(&order).await }).await?;
let path: String = ctx.step("render", async { render(&order).await }).await?;
```

Both workers write the **same** record, so a job checkpointed by the Ruby
worker resumes in the Rust one and the reverse — which is what "the workers are
peers" has to mean if it means anything.

Deliberately **not** durable execution (Temporal, DBOS, restate). Replaying a
function means intercepting every side effect, which means a per-language SDK
— the client-library tax this project exists to avoid. A cursor is a column and
works from any language.

Two things worth knowing:

- **A checkpoint is also a heartbeat.** A job saving progress is demonstrably
  alive, so it cannot be expired by the wall-clock timeout while it is plainly
  working.
- **Progress resets the *interruption* budget, not the attempt budget.** A job
  that checkpointed and then died is not the kind of job that kills workers, so
  it is not charged as one — but `attempt` still counts, so a job that
  progresses one unit and then dies forever is still discarded. Progress buys
  tolerance of infrastructure failure and buys nothing against a broken job.

**Checkpoint inside your transaction** and the cursor can never claim work that
was rolled back:

```ruby
ActiveRecord::Base.transaction do
  import(chunk)
  stampede_checkpoint!(cursor: n)   # commits with the work, or not at all
end
```

Under Rails this is automatic, because the checkpoint runs on ActiveRecord's
connection. In Rust, pass the transaction to `checkpoint_on`. Outside both, pass
the connection explicitly — River's `ResumableSetStepCursorTx` exists for the
same reason and got there first.

## Operational visibility

Three things degrade a Postgres queue without ever raising an error, and all
three are surfaced on the dashboard and through the `system_health` MCP tool:

- **Autovacuum falling behind.** A queue table is the pathological case —
  constant `UPDATE`s over a small hot set. When vacuum falls behind, nothing
  fails; things just get slower, which presents as "the queue is mysteriously
  slow" and sends people reading their job code. Stampede compares dead tuples
  against the threshold autovacuum will *actually* use for that table, from its
  own reloptions. (pg-boss's `#checkVacuum`, which `plans/13` rates the most
  valuable operational feature in the survey.)
- **Notifications being eaten.** PgBouncer in transaction mode silently drops
  `LISTEN`, and the queue then runs one poll interval late, forever. A sonar
  probe notifies our own daemon and checks it comes back.
- **Backlog age.** Depth says how much work there is; age says whether it is
  being *done*. 10,000 jobs with an oldest age of two seconds is healthy; three
  jobs aged an hour is broken, and depth alone makes the second look quieter.

**No unbounded aggregates.** `SELECT count(*) … GROUP BY state` over the job
table looks harmless and is pg-boss's documented death spiral: the aggregate
holds the xmin horizon, vacuum cannot reclaim below it, the table bloats, the
aggregate slows, and it holds the horizon longer. Depth comes from a counter
table maintained by statement-level triggers, because the dashboard runs it
every second.

## Not done yet

- Batched `UNWIND` writes — deliberately deferred, see `plans/14`
- Daemon-mediated polyglot workers (claim handed out over HTTP/SSE, for workers
  across a trust boundary). The SQL-function path above covers in-cluster
  workers, which is the common case.
- **Not load-tested at scale.** Every design decision here is informed by
  measurements against a real Postgres 18, but nothing has been run at
  production volume for a sustained period. The autovacuum numbers in
  particular are Oban's, adopted as a starting point, and `plans/13` flags them
  as "to be measured, not a result".
- No TLS on the HTTP edge and no auth on the enqueue API — put it behind
  something that has both. The *dashboard* is authenticated and off by default.

## Requirements

Postgres **18 or newer**, deliberately. Stampede uses `uuidv7()`,
`RETURNING OLD./NEW.` and generated columns; there are no compatibility shims.

## Testing

```bash
cargo test                                   # unit only
createdb stampede_test
STAMPEDE_TEST_DATABASE_URL=postgres://localhost/stampede_test cargo test
```

With the env var set that is 141 tests: unit, integration against real
Postgres, and wire-level tests that run a real server on an ephemeral port.
Integration tests run in parallel, each in its own schema.

The integration tests are worth reading as documentation of the awkward cases:
a breaker trip refunding an attempt, a job that kills workers being discarded
by the *interruptions* budget long before `max_attempts`, a rescheduled job not
vacating its own dedup window, a bulk insert of 500 rows producing exactly one
notification, and both DST transitions against `America/New_York`.

## Licence

BSD-3-Clause.
