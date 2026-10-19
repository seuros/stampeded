# Stampeded

A job processor on Postgres 18+. A Rust daemon (scheduler, reaper, pruner,
dashboard, HTTP API), a Rust worker, a Ruby gem with an ActiveJob adapter, and
an MCP control plane. BSD-3.

## How it works

Jobs are rows. Enqueueing is a plain `INSERT`, from any language, inside the
same transaction as the data that caused it:

```sql
INSERT INTO stampeded_jobs (queue, task, args)
VALUES ('mail', 'send_welcome', '{"user_id": 42}'::jsonb);
```

`args` is always a JSON object of named arguments, so any language decodes it
into a struct. `meta` holds one runtime's context under its name, e.g.
ActiveJob's `job_class` and `job_id` under `"activejob"`; other workers ignore it.

Claiming, completing and failing are SQL functions, so a worker in any language
needs only a Postgres driver:

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
    db.exec_params("SELECT stampeded_complete($1,$2,$3,$4)", ['default', job['id'], worker, job['attempt']])
  end
  sleep 0.5
end
```

## Running it

```bash
cargo install stampeded
createdb stampeded
stampeded --migrate --database-url postgres://localhost/stampeded \
         --queue default --http 127.0.0.1:7878
```

One process runs the worker, the reaper, the pruner, the scheduler, the HTTP API
and the dashboard. Split roles with `--no-worker` and `--no-maintenance`;
`stampeded --help` lists every flag. `--queue` takes one queue, a list
(`default,mail`), or `'*'` for every queue with work (`--except-queues dlq`).

The URL takes libpq's `sslmode` and `sslrootcert`. TLS is rustls with Mozilla's
roots built in; a private CA goes in `sslrootcert`, so on RDS:
`?sslmode=verify-full&sslrootcert=/path/to/global-bundle.pem`.

- **Dashboard** -- `http://127.0.0.1:7878/dashboard`, live over SSE. Only
  served when you pass `--dashboard-token`.
- **MCP** -- `stampeded-mcp` over stdio.
- **HTTP API** -- for callers that cannot reach Postgres:

  | | |
  |---|---|
  | `POST /jobs` | enqueue |
  | `GET /queues/{queue}/jobs/{id}` | job state |
  | `GET /queues/{queue}/jobs/{id}/events` | state changes over SSE, until the job finishes |
  | `GET /queues/{queue}/depth` | depth by state |
  | `GET /health` | |

Rust handlers are registered in-process (`crates/stampeded/examples/worker.rs`).
Or pull jobs and run them yourself: `Puller::next` hands out a `Lease`, which
you `complete`, `fail` or `discard`; claiming, heartbeats and queue policies stay
in the library. To embed only the queue in a Rust app:
`cargo add stampeded --no-default-features`.

## Rails

```ruby
gem "stampeded"
config.active_job.queue_adapter = :stampeded
```

`bin/rails stampeded:work` runs your jobs; `bin/rails stampeded:daemon` runs the
scheduler, reaper, pruner and dashboard. ActiveJob's `retry_on` is refused --
use `stampeded max_attempts:` or `Stampeded::RetryIn`.

A job with `stampeded task: "invoice.parse"` can be enqueued from Rails and run
by any worker that handles `invoice.parse`, in any language; its keyword
arguments become `args`. See [ruby/README.md](https://github.com/seuros/stampeded/blob/master/ruby/README.md).

## Other languages

`libstampeded_ffi` exposes enqueueing and pulling over a C ABI, for any
language with a C FFI. The header is
[`crates/stampeded-ffi/include/stampeded.h`](https://github.com/seuros/stampeded/blob/master/crates/stampeded-ffi/include/stampeded.h).

```c
stampeded_client *client = stampeded_connect("postgres://localhost/app");
stampeded_puller_options options = {.queues = "invoice"};
stampeded_puller *puller = stampeded_puller_start(client, &options);

stampeded_lease *lease = nullptr;
while (stampeded_puller_next(puller, 5000, &lease) != STAMPEDED_ERROR) {
  if (lease == nullptr) continue;
  const char *args = stampeded_lease_args(lease);   /* a JSON object */
  if (parse_invoice(args)) (void)stampeded_lease_complete(lease);
  else (void)stampeded_lease_fail(lease, "unreadable invoice", -1);
  lease = nullptr;
}
```

The Ruby gem's worker is built on it. Calls block the calling thread, so call
`stampeded_puller_next` from as many threads as you want jobs in flight. A
lease freed without an outcome goes back to the queue as an interruption. On
`STAMPEDED_ERROR`, `stampeded_last_error()` says why.

### Go

`github.com/seuros/stampeded/go`, linked with
`CGO_LDFLAGS="-L<dir> -Wl,-rpath,<dir>"` where `<dir>` holds the library:

```go
client, err := stampeded.Connect("postgres://localhost/app")
puller, err := client.Pull(stampeded.PullerOptions{Queues: "invoice", MaxLeases: 8})

for range 8 {
	go func() {
		for {
			lease, err := puller.Next(5 * time.Second)
			if err != nil || lease == nil {
				continue
			}
			if err := parseInvoice(lease.Args); err != nil {
				lease.Fail(err.Error(), stampeded.Backoff)
			} else {
				lease.Complete()
			}
		}
	}()
}
```

A lease dropped without an outcome goes back to the queue once it is garbage
collected; `Release` gives it back at once.

## Retries and failures

- **Attempts are counted when a job is claimed**, so a job that crashes its
  worker still uses up its budget. Default `max_attempts` is 25.
- **Backoff is exponential with full jitter**, from `backoff_base_ms` (1 s) up
  to `backoff_max_ms` (1 h). A handler can pick its own delay instead:

  ```ruby
  rescue RateLimited => e
    raise Stampeded::RetryIn.new(e.retry_after)
  ```

  ```rust
  Err(Box::new(RetryIn::secs(3600)))
  ```

  `RetryIn` chooses when, not whether: `max_attempts` still applies.
- **Worker deaths have their own budget** (`max_interruptions`, default 1). A
  job that keeps killing its worker is discarded after the second claim.
- **Failures are recorded by kind** -- `raised`, `timed_out`,
  `worker_missing`, `breaker_open`, ... -- and fingerprinted with the numbers
  and ids stripped, so the same error repeated is one cluster.
- **Dead-letter queues**: set `dead_letter_queue` and a discarded job is copied
  there.

## Recurring schedules

Cron expressions or fixed intervals, stored as rows and editable at runtime:

```sql
INSERT INTO stampeded_schedules (key, queue, task, cron_expr, timezone, dst_policy)
VALUES ('nightly', 'reports', 'daily_summary', '0 3 * * *', 'Africa/Casablanca', 'wall_clock');
```

Any number of daemons can run the scheduler; each occurrence fires once. A
missed occurrence (daemon down) runs once, late. A new schedule first fires at
its next occurrence, not on creation.

DST is chosen per schedule:

| | fall-back (01:30 happens twice) | spring-forward (02:30 does not exist) |
|---|---|---|
| `wall_clock` | runs once | runs at 03:30 |
| `instant` | runs twice, an hour apart | -- |

Omit `dst_policy` and the expression decides: one that names its hours
(`0 3 * * *`) gets `wall_clock`, one with `*` or a step in the hour gets
`instant`.

## Unique jobs

```sql
INSERT INTO stampeded_jobs (queue, task, dedup_scope)
VALUES ('default', 'SyncAccount', 'acct:42');
```

A second job with the same `dedup_scope` **in the same queue** is refused until
the first finishes. `dedup_period` (`minute`, `hour`, `day`, `week`, `month`)
allows one per time bucket instead.

## Claim order

Jobs are claimed by `priority`, then `scheduled_at`. Ties -- such as a batch
inserted in one statement -- come out in no defined order, and with concurrent
workers there is no strict FIFO. Encode ordering you depend on in `priority` or
`scheduled_at`.

## Queue policies

Per-queue limits, judged by the daemon every second:

```sql
INSERT INTO stampeded_queue_policies (queue, max_running) VALUES ('extraction', 2);
INSERT INTO stampeded_queue_policies (queue, max_wait_secs) VALUES ('mail', 60);
```

Each queue with a policy gets a verdict, with a reason, in the same row:
`hold` at `max_running` ("2 running, max 2"), `rush` once its next job has
waited past `max_wait_secs` ("next job waited 122s, max 60s"), otherwise
`steady`. Workers claim rushed queues first and skip held ones.

- A verdict older than 30 seconds is ignored, so without a daemon workers fall
  back to plain round robin.
- `max_running` is approximate across processes. Each worker also enforces it
  for what it holds itself, daemon or not.

The policy is a behavior tree from
[behavior-machines](https://github.com/seuros/behavior-machines).

## Fairness and circuit breaking

On by default in the Rust worker:

- **Per-tenant admission** (`tenant_id`): one tenant enqueueing 100k jobs does
  not starve the others.
- **A circuit breaker per task**: a failing downstream is not hammered by every
  retry. Jobs refused by an open circuit or by tenant throttling are recorded
  as `breaker_open` / `throttled` and get their attempt back.

## Resumable jobs

Save a cursor as the job progresses; a retry picks up from it:

```ruby
def perform(file)
  cursor = stampeded_checkpoint&.dig("cursor") || 0

  rows[cursor..].each_slice(1_000) do |chunk|
    import(chunk)                          # do the work first
    cursor += chunk.size
    stampeded_checkpoint!(cursor: cursor)  # then record it
  end
end
```

Or split a job into named steps; completed steps are skipped on retry and their
return values are kept:

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

```rust
ctx.step("charge", async { charge(&order).await }).await?;
let path: String = ctx.step("render", async { render(&order).await }).await?;
```

- Every runtime reads and writes the step record through the same Rust code, so
  any worker can resume another's job.
- Checkpoint inside your transaction and the cursor commits with the work. Under
  Rails that happens automatically; in Rust use `checkpoint_on`.
- A checkpoint is a round trip (about 1.7 ms locally). For large loops write
  every N units: `step.advance!(i, every: 1000)`. A hard kill then loses up to N.
- A checkpoint counts as a heartbeat, so a job making progress is not expired.
- Step results are limited to 8 KB; store anything bigger yourself and return a
  reference.

## Timeouts

| | where | default | fires when |
|---|---|---|---|
| `job_timeout` | in the worker | 15 min | the handler runs too long |
| `expire_seconds` | in the database, per job | 1 h | nothing heard from the job |
| `dead_after` | in the database | 5 min | the worker stopped heartbeating |

Keep `job_timeout` shorter than `expire_seconds`, or an expired job can be
claimed by a second worker while the first is still running it. Workers warn
when a job's settings are the wrong way round. `job_timeout` cannot interrupt a
blocking C call (Ruby) or a blocking task (tokio).

## Connection pooling

Behind PgBouncer in transaction mode, `LISTEN`/`NOTIFY` does not work, so jobs
start on the poll interval (1 s) instead of immediately; the dashboard shows
"polling only". Give the daemon a direct or session-mode connection to get
instant wakeups. Workers can stay pooled.

## Operational visibility

The dashboard and the `system_health` MCP tool report:

- autovacuum falling behind on the queue tables,
- `LISTEN`/`NOTIFY` not getting through,
- backlog age per queue -- whether work is being done, not just how much is
  waiting.

## Retention

Completed jobs are kept 14 days (`--retain-for`), discarded ones 7 days. A
completed job costs about 330 bytes; finished jobs never slow down claiming.

| jobs/sec | 1 hour | 24 hours | 7 days |
|---|---|---|---|
| 10 | 12 MB | 285 MB | 2.0 GB |
| 100 | 119 MB | 2.9 GB | 20 GB |
| 1,000 | 1.2 GB | 29 GB | 200 GB |

## Performance

One million jobs against a real 8.7 GB database:

| job body | threads | durable commits | jobs/sec |
|---|---|---|---|
| nothing | 8 | no | 5,887 |
| one `SELECT` | 8 | no | 3,971 |
| 10 ms of IO | 64 | no | 4,890 |
| 10 ms of IO | 64 | yes | 825 |

| | jobs/sec |
|---|---|
| 1 worker × 8 threads | 3,472 |
| 4 workers × 8 threads | 8,849 |

- Queue overhead is about 0.17 ms per job.
- **With durable commits, your storage's commit rate is the ceiling**, not the
  worker. To see what one commit costs, time a single `INSERT` in `psql` with
  `\timing on`. Faster storage, `synchronous_commit = off`, or an unlogged queue
  (`SELECT stampeded_create_queue('metrics', p_unlogged => true)`) raise it.
- IO-bound jobs scale with threads; CPU-bound jobs need more processes.

## Limitations

- Every worker needs database credentials; there is no broker for workers
  outside your network.
- No strict FIFO (see *Claim order*).
- No TLS and no auth on the HTTP API -- put it behind something that has both.
- No claiming over HTTP yet, for workers across a trust boundary.

## Requirements

- Postgres 18 or newer.
- Building on Linux needs `libclang` (`upkg install libclang-dev`).
- The C ABI test compiles C23: GCC 14 or newer (`STAMPEDED_CC` picks the
  compiler; `gcc-16` is used when on `PATH`).

## Testing

```bash
cargo test                                   # unit only
createdb stampeded_test
STAMPEDED_TEST_DATABASE_URL=postgres://localhost/stampeded_test cargo test
```

The database tests skip without `STAMPEDED_TEST_DATABASE_URL`, except under
`CI`, where they fail instead. `STAMPEDED_TEST_TLS_DATABASE_URL` names a server
with a certificate for the end-to-end TLS test, which skips without one, CI
included. The Ruby suite lives in `ruby/`.

## Layout

```
crates/stampeded/  queue, worker, scheduler, maintenance, planner
  schema/          schema.sql, NOTES.md (design notes)
  src/http/        HTTP API and dashboard    [feature: http]
  src/mcp/         MCP control plane         [feature: mcp]
  src/bin/         stampeded, stampeded-mcp
crates/stampeded-ffi/  C ABI: libstampeded_ffi and stampeded.h
ruby/              the stampeded gem
go/                the Go package
```

## FAQ

**Can I use another database?**
No. Stampeded is a PostgreSQL application; there will be no SQLite, MySQL or
Redis backend. If you want Redis, use Sidekiq, which I use for other kinds of
workloads.

**Can I use Ruby 3.4?**
Upgrade, or fork. It might work on 3.4, but I don't test it.

**Can I use Rails 7.2?**
Same: upgrade, or fork.

**I don't like the dashboard.**
Consume the HTTP API with whatever you want.

**Will there be PHP or JS bindings?**
Build one on the C ABI ([`stampeded.h`](https://github.com/seuros/stampeded/blob/master/crates/stampeded-ffi/include/stampeded.h))
and open an issue linking to it; you can be listed as its maintainer. I can't
maintain bindings for languages I don't use, even with AI in the loop.

## Credits

The dashboard palette is from
[cybercore-css](https://github.com/sebyx07/cybercore-css) v0.3.0 (MIT,
© sebyx07) -- the design tokens only.

## Licence

BSD-3-Clause.
