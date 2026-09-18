# Stampeded

A polyglot job processor. Rust core, Postgres 18+ substrate, Datastar dashboard,
MCP control plane. BSD-3.

**Status: working end to end, and every design decision in `plans/` is
implemented.** Verified against Postgres 18 with 260 tests — unit, integration
against a real database, wire-level against a real server, a real Rails app,
and fault injection. Not yet load-tested at scale; see *Not done yet*.

The tests that found the most were the ones that did what the system is
actually *for*, rather than what is convenient to assert: four workers
completing 200 jobs at once found a deadlock in the counter trigger; killing a
worker's database connection found that it never reconnected; fuzzing the
enqueue endpoint found that `max_attempts: 0` created a job nothing could ever
claim.

## Why

Every serious job processor is locked to one runtime — Sidekiq is Ruby, Oban is
Elixir, Celery is Python, BullMQ is Node. Stampeded's substrate is Postgres, so
**enqueue is a plain `INSERT` and every language already has a client**:

```sql
INSERT INTO stampeded_jobs (queue, task, args)
VALUES ('mail', 'send_welcome', '{"user_id": 42}'::jsonb);
```

No wire protocol, no client library to maintain, and the job commits in the same
transaction as the business data that caused it.

**Workers are polyglot too.** Claiming carries the fence, the attempt accounting
and the ownership rules, so it must not be reimplemented per language — but it
does not have to be, because it is a SQL function. A complete worker in Ruby,
with no Stampeded client library anywhere:

```ruby
require 'pg'
require 'json'

db = PG.connect(ENV['DATABASE_URL'])

worker = db.exec_params(
  "INSERT INTO stampeded_workers (hostname, pid, queues) VALUES ($1,$2,$3) RETURNING id",
  ['ruby-worker', Process.pid, '{default}']
).getvalue(0, 0)

loop do
  db.exec_params("SELECT stampeded_heartbeat($1)", [worker])
  db.exec_params("SELECT * FROM stampeded_claim($1,$2,$3)", ['default', worker, 5]).each do |job|
    handle(JSON.parse(job['args']))
    db.exec_params("SELECT stampeded_complete($1,$2,$3)", ['default', job['id'], worker])
  end
  sleep 0.5
end
```

That is the whole integration. The pitch is not "our protocol is better than
Faktory's" — it is that **there is no protocol**.

## What is different

Stampeded was designed after surveying thirteen job processors in six languages
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
createdb stampeded
stampeded --migrate --database-url postgres://localhost/stampeded \
         --queue default --http 127.0.0.1:7878
```

That single process runs the worker, the reaper, the pruner, the HTTP API and
the dashboard. Split them later by running the same binary with different flags
(`--no-worker`, `--no-maintenance`) — there is no second binary to deploy.

- **dashboard** — <http://127.0.0.1:7878/dashboard>, live over SSE.
  **Off unless you pass `--dashboard-token`**, deliberately: a job dashboard
  exposes customer data in job args and offers a retry button, and opt-in auth
  is how dashboards end up open on production.
- **MCP** — `stampeded-mcp` over stdio; point any MCP client at it
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
(`crates/stampeded-core/examples/worker.rs`); workers in other languages use
`stampeded_claim` as shown above. Neither needs a client library.

## Rails

```ruby
gem "stampeded"
gem "stampeded-daemon"   # optional: the precompiled binary
```

```ruby
config.active_job.queue_adapter = :stampeded
```

Two processes: `bin/rails stampeded:work` runs your jobs, `bin/rails
stampeded:daemon` runs the scheduler, reaper, pruner and dashboard. They are
split because the Rust daemon cannot execute an ActiveJob job — an ActiveJob
job is a Ruby class — while every other role operates on *rows* and so does not
care what language wrote them. See `ruby/README.md` and `plans/16`.

`retry_on` raises: it re-enqueues as a new row while Stampeded counts attempts
at claim, which would make every attempt column read 1. Use
`stampeded max_attempts: 5` instead.

## Layout

```
plans/     design notes — what was measured, and why each decision was made
schema/    001_initial, 002_operations, 003_schedules  (PG18+ only)
crates/stampeded/  one crate, features gate the weight
  src/            queue, worker, lifecycle, cron, wakeup, maintenance, metrics
  src/http/       enqueue API + Datastar dashboard, on rama   [feature: http]
  src/mcp/        MCP control plane, on mcp-host              [feature: mcp]
  src/bin/        `stampeded` (daemon) and `stampeded-mcp`
ruby/             the `stampeded` gem: ActiveJob adapter, worker, schedules
ruby-daemon/      the `stampeded-daemon` gem: one binary per platform
```

One crate rather than a workspace of four, because the four bought nothing:
nobody had asked to depend on the pieces separately, and it cost four
publishes and four version numbers. Features carry the weight instead, and
they carry a lot of it — the queue core alone locks **374** crates, everything
locks **958**. An application embedding the queue should not compile rama:

```bash
cargo install stampeded                       # the daemon, batteries included
cargo add stampeded --no-default-features     # just the queue
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
INSERT INTO stampeded_schedules (key, queue, task, cron_expr, timezone, dst_policy)
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
a fall-back day. Stampeded makes it a choice:

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

## Claim order

Jobs are claimed by `priority`, then `scheduled_at`. **Ties are not broken.**

Jobs bulk-enqueued in one transaction all share the same `scheduled_at` — one
`now()` for the whole statement — so a batch is exactly such a tie, and its
members are claimed in index-scan order rather than insertion order. That order
is stable for a given index state, so nothing starves; it is simply not
defined, and you should not build on it.

This is deliberate. A `uuidv7` tiebreaker would give insertion order, but
adding any unique column to the claim index makes every entry distinct and
leaves btree deduplication nothing to collapse: **85 MB against 10 MB at a
million rows**, on the index every claim walks and the one most wanted resident
in cache. Claim latency was identical either way.

Strict FIFO is not on offer across concurrent workers regardless: `FOR UPDATE
SKIP LOCKED` takes the next *unlocked* row, not the next row. If you need
ordering guarantees, encode them in `priority` or `scheduled_at`, which are the
columns the claim actually orders by.

## Unique jobs

Set `dedup_scope` and the database refuses a second job with the same one:

```sql
INSERT INTO stampeded_jobs (queue, task, dedup_scope)
VALUES ('default', 'SyncAccount', 'acct:42');
```

It is a partial unique index, not a check in application code, so it holds
against concurrent enqueuers in separate transactions — including ones that
never run any of our code.

**Uniqueness is per queue.** The index is `(queue, dedup_key)`, so the same
`dedup_scope` in two different queues is two different jobs:

```
('default', 'acct:42')   -> inserted
('default', 'acct:42')   -> rejected, duplicate key
('mail',    'acct:42')   -> inserted
```

This follows from partitioning by queue — a unique index on a partitioned
table must contain the partition key — and it is the same choice pg-boss
makes. If you want an account synced once across the whole system, enqueue it
to one queue; if you put the queue name in the scope you get the per-queue
behaviour twice over, not global uniqueness.

`dedup_period` ('hour', 'day', …) adds a time bucket to the key, so the same
scope becomes enqueueable again once the bucket rolls over.

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
  cursor = stampeded_checkpoint&.dig("cursor") || 0

  rows[cursor..].each_slice(1_000) do |chunk|
    import(chunk)                          # commit the work FIRST
    cursor += chunk.size
    stampeded_checkpoint!(cursor: cursor)   # then record it
  end
end
```

Or by **named step**, which is what you want when a job has phases that must
not be repeated:

```ruby
def perform(order)
  stampeded_step("charge") { Stripe.charge(order.total) }    # never charged twice
  path = stampeded_step("render") { render_pdf(order) }      # value kept on resume

  stampeded_step("email", cursor: 0) do |step|
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

A checkpoint is otherwise opaque to Stampeded — your cursor, your meaning. It
rides on the row `stampeded_claim` already returns, so resuming costs no extra
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
  stampeded_checkpoint!(cursor: n)   # commits with the work, or not at all
end
```

Under Rails this is automatic, because the checkpoint runs on ActiveRecord's
connection. In Rust, pass the transaction to `checkpoint_on`. Outside both, pass
the connection explicitly — River's `ResumableSetStepCursorTx` exists for the
same reason and got there first.

## Connection pooling

If you run behind PgBouncer in **transaction** mode — the recommended mode, and
the common one in front of Rails — `LISTEN`/`NOTIFY` does not survive it. The
pooler accepts the `LISTEN` without error and the connection will even tell you
it is subscribed; the notifications simply never arrive.

Stampeded detects this rather than letting you find out from latency graphs. The
dashboard says:

```
polling only
```

Nothing is broken when it does: jobs are still claimed correctly, just on the
poll interval instead of within milliseconds. If you want wakeups, give the
daemon a **direct** connection (or a `session`-mode pool) — it is the only
connection that needs one. Workers can stay pooled.

## Checkpoint cost

A checkpoint is a round trip — measured at **~1.7 ms against a local database,
about 600/second**. Checkpointing every row of a large job therefore costs more
than the work it records. Batch it:

```ruby
steps.step("rows", cursor: 0) do |c|
  rows.each_with_index do |row, i|
    process(row)
    c.advance!(i, every: 1000)   # one write per 1000 rows
  end
end
```

`every:` is opt-in because it has a price: a hard kill loses up to N units of
progress, and surviving a hard kill is what checkpoints are *for*. The default
writes every time. Stating `every: 1000` at the call site is how the trade
stays visible.

A buffered cursor is deliberately not written when the block raises — the
pending value may describe work that never committed, and persisting it would
make the retry skip that work rather than redo it.

## Custom retry delays

The row carries `backoff_base_ms` and `backoff_max_ms`, which give exponential
backoff with a cap. When a failure knows better than a formula, the handler can
say so:

```ruby
rescue RateLimited => e
  raise Stampeded::RetryIn.new(e.retry_after)   # honour the server
end
```

```rust
Err(Box::new(RetryIn::secs(3600)))
```

This is Stampeded's answer to ActiveJob's `retry_on(wait:)`, which the adapter
refuses — see *Rails* above. The objection to `retry_on` is not custom backoff;
it is that `retry_on` re-enqueues the job as a **new row**, which resets every
counter and makes the dashboard lie about how many times anything has run.

`RetryIn` chooses *when* the next attempt happens, not *whether*. The attempt
was charged at claim, and `max_attempts` still discards the job on schedule.

## Timeouts

Three layers, because the first cannot fire when the worker is dead and the
second cannot tell a slow job from a dead one on its own.

| layer | where | default | fires when |
|---|---|---|---|
| 1 | in the worker | `job_timeout`, 15 min | the handler runs too long |
| 2 | in the database | `expire_seconds`, 1 h | nothing has been heard since `heartbeat_at` |
| 3 | in the database | worker heartbeat + `dead_after`, 5 min | the whole worker process died |

**Layer 1 must be shorter than layer 2.** This is River's
`rescue_after > job_timeout` invariant, and getting it backwards is not a
tuning mistake — it is how a queue double-executes. Layer 2 fires whether or
not the worker is alive and returns the job to `retryable`, where another
worker claims it; if layer 1 has not already stopped the first execution, both
run at once.

`expire_seconds` is per job and set by the enqueuer, so it cannot be validated
against `job_timeout` by a constant. Both workers check it against the row they
were handed and warn if the layers are inverted:

```
job_timeout (900s) is not shorter than Report's expire_seconds (60s): the
database may expire this job and let another worker claim it while this one is
still running it
```

Layer 1 is advisory on both sides, and for the same reason: Ruby cannot
interrupt a blocking C call and tokio cannot preempt a blocking task. A handler
doing synchronous work should use `spawn_blocking` (Rust) or expect the timeout
to be approximate (Ruby). Layers 2 and 3 do not care what the handler is doing.

A job that heartbeats — including via `checkpoint`, which counts as one —
cannot be expired by layer 2 while it keeps doing so. That is deliberate: a job
saving progress is demonstrably working. It means a job stuck in a loop *that
checkpoints* is bounded only by layer 1.

## Operational visibility

Three things degrade a Postgres queue without ever raising an error, and all
three are surfaced on the dashboard and through the `system_health` MCP tool:

- **Autovacuum falling behind.** A queue table is the pathological case —
  constant `UPDATE`s over a small hot set. When vacuum falls behind, nothing
  fails; things just get slower, which presents as "the queue is mysteriously
  slow" and sends people reading their job code. Stampeded compares dead tuples
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

## Retention

Completed jobs are pruned after an hour, discarded ones after seven days.

A completed job costs **~330 bytes** (heap plus every index), so an hour at a
sustained 100 jobs/s is about 120 MB. What it does *not* cost is claim
latency: the claim index is partial, so finished jobs are not in it at all.
Keeping them longer makes the table bigger and gives autovacuum more to do —
it does not make claiming slower, which is usually the fear behind shortening
retention.

| jobs/sec | 1 hour | 24 hours | 7 days |
|---|---|---|---|
| 10 | 12 MB | 285 MB | 2.0 GB |
| 100 | 119 MB | 2.9 GB | 20 GB |
| 1,000 | 1.2 GB | 29 GB | 200 GB |

## What this does not do

**Every worker needs database credentials.** The tables are the protocol: a
worker claims by calling a SQL function, so a worker you would not trust with
the jobs table cannot be a Stampeded worker. There is no broker handing work out
over HTTP, which rules out browser-side or customer-hosted workers. That is a
consequence of the design rather than a missing feature — see
`plans/05-polyglot-protocol.md`, which weighs the alternative and declines it.

**Strict FIFO is not offered.** See *Claim order*.

**`retry_on` is refused, not supported.** ActiveJob's retry mechanism
re-enqueues as a new row, which would give two independent retry budgets and
make every counter on the dashboard wrong. Use `max_attempts`,
`backoff_base_ms`/`backoff_max_ms`, or `RetryIn` — see *Custom retry delays*.

## Requirements

Postgres **18 or newer**, deliberately. Stampeded uses `uuidv7()`,
`RETURNING OLD./NEW.` and generated columns; there are no compatibility shims.

**Building on Linux needs `libclang`** (`apt install libclang-dev`). A
dependency runs `bindgen` in a build script there; macOS has it already, so
this only bites the first time someone builds on Linux — and the error names
neither the crate that wanted it nor the package that provides it.

## Testing

```bash
cargo test                                   # unit only
createdb stampeded_test
STAMPEDED_TEST_DATABASE_URL=postgres://localhost/stampeded_test cargo test
```

With the env var set that is 183 Rust tests — unit, integration against real
Postgres, and wire-level tests that run a real server on an ephemeral port —
plus 78 Ruby tests (245 assertions) under `ruby/`. Integration tests run in
parallel, each in its own schema.

Without the env var the database tests skip, which is a courtesy to anyone
without a Postgres to hand. Under `CI` they refuse to skip and fail instead:
123 of those tests would otherwise return early and report success, and a
green build that tested nothing is worse than a red one.

The integration tests are worth reading as documentation of the awkward cases:
a breaker trip refunding an attempt, a job that kills workers being discarded
by the *interruptions* budget long before `max_attempts`, a rescheduled job not
vacating its own dedup window, a bulk insert of 500 rows producing exactly one
notification, and both DST transitions against `America/New_York`.

## Licence

BSD-3-Clause.
