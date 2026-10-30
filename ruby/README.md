# stampeded (Ruby)

Rails integration for [Stampeded](https://github.com/seuros/stampeded): an
ActiveJob adapter, a worker, and a launcher for the daemon.

```ruby
gem "stampeded"
```

```ruby
# config/application.rb
config.active_job.queue_adapter = :stampeded
```

```bash
bin/rails stampeded:install   # apply the schema (Postgres 18+)
bin/rails stampeded:work      # run your jobs
bin/rails stampeded:daemon    # scheduler, reaper, pruner, dashboard
```

## Two processes

`stampeded:work` runs your jobs. `stampeded:daemon` runs the Rust daemon:
scheduler, reaper, pruner, expirer, health checks, HTTP API and dashboard. It
never runs a job itself. Without the daemon, jobs still run, but there is no
cron, no recovery of jobs from dead workers, no retention and no dashboard.

The daemon gets the database URL through its environment, never its command
line. Extra flags go in `STAMPEDED_ARGS`, which `ps` shows to everyone on the
host, so the dashboard token goes in `STAMPEDED_DASHBOARD_TOKEN` instead:

```bash
STAMPEDED_DASHBOARD_TOKEN=... STAMPEDED_ARGS="--http 127.0.0.1:7878" bin/rails stampeded:daemon
```

## Transactional enqueue

```ruby
Order.transaction do
  order.save!
  ShipOrderJob.perform_later(order)   # same transaction
end                                    # rolled back? the job is gone too
```

The enqueue is an `INSERT` on your own connection. This only holds when the
jobs live in the same database as your data; with a separate database
(`config.connection_class`), the job commits on its own.

## Jobs other runtimes can run

```ruby
class ParseInvoiceJob < ApplicationJob
  queue_as :parsing
  stampeded task: "invoice.parse"

  def perform(document_id:) = ...
end

ParseInvoiceJob.perform_later(document_id: document.id)
# task "invoice.parse", args {"document_id": "..."}
```

Any worker that handles `invoice.parse` can run it: Rust, Go, or this gem's
worker. Its arguments must be keywords with plain JSON values (no records, times
or symbols); anything else raises `Stampeded::PortableArgumentsError` at
enqueue. A job another runtime enqueues as `invoice.parse` runs
`ParseInvoiceJob` here, with `args` as its keywords.

Every other job works as before. Keyword calls with plain values are stored the
same standard way; anything else (records, positional arguments) keeps
ActiveJob's own format, which only a Ruby worker can run.

## Retries

ActiveJob's `retry_on` raises `Stampeded::RetryConflict` when the job class
loads: it re-enqueues retries as new jobs, while stampeded counts attempts on
the job itself. Declare stampeded's options instead:

```ruby
class ChargeCard < ApplicationJob
  stampeded max_attempts: 5,
            backoff_base_ms: 1_000, backoff_max_ms: 300_000,
            dead_letter_queue: "payments_dlq",
            expire_seconds: 120
end
```

- `raise Stampeded::RetryIn.new(seconds)` picks the next delay, e.g. a
  server's `Retry-After`.
- `stampeded_attempt` is the attempt number. ActiveJob's `executions` is always
  1 here.
- `discard_on` works as usual.
- Jobs inside installed gems (Rails' `ActiveStorage::AnalyzeJob`, for one) may
  keep `retry_on`; they get a warning at load instead.

Other options: `unique:` / `unique_period:` (one waiting job per key) and
`destination:`.

## Continuations

Rails 8.1 continuations work as written:

```ruby
class ImportJob < ApplicationJob
  include ActiveJob::Continuable

  def perform(import_id)
    step :rows, start: 0 do |rows|
      Row.where(import_id:).find_each(start: rows.cursor) do |row|
        row.process!
        rows.advance!(from: row.id)
      end
    end
  end
end
```

- Progress is kept on the job's row, so a crashed worker's job resumes too.
- A deploy interrupts the job at its next checkpoint; it goes back to the queue
  without spending an attempt and resumes on the same row.
- An error after progress is an attempt: `max_attempts` and backoff apply, and
  the next attempt resumes from the last checkpoint.

## Recurring jobs

```ruby
Stampeded::Schedule.recurring(
  key:  "nightly-report",
  job:  ReportJob,
  cron: "0 3 * * *",
  args: [Date.current],
  timezone: "America/New_York"
)
```

The daemon fires it; a new schedule first runs at its next occurrence. An
expression that names its hours runs once on a DST fall-back day; one using `*`
or a step runs on real elapsed time. The queue is `queue:`, or the job's
`queue_as` evaluated with `args`. `Stampeded::Schedule.pause`, `.remove` and
`.all` manage them.

## Workers

The pool is threads, which suits IO-bound jobs; for CPU-bound jobs run more
processes.

```bash
QUEUE=mail CONCURRENCY=10 bin/rails stampeded:work
QUEUES=default,mail,reports bin/rails stampeded:work    # one pool, several queues
QUEUES='*' QUEUES_EXCEPT=payments_dlq bin/rails stampeded:work
```

Or configure it once:

```ruby
config.queues = %w[default mail reports]   # or :all
config.except_queues = %w[payments_dlq]    # with :all
```

- With named queues, name every queue your jobs use: a job in a queue nobody
  polls waits forever.
- With `:all`, the worker serves every queue that has work. Leave out
  dead-letter queues and queues other languages' workers own: this worker runs
  everything as ActiveJob and discards a job whose class it cannot find.
- `SIGTERM` lets in-flight jobs finish before exiting. Jobs still running after
  `shutdown_timeout` are reclaimed by the daemon.
- The worker runs on `libstampeded_ffi`, the same core every runtime's worker
  uses; see [The binaries](#the-binaries). Failures to claim or report go to
  stderr always; `STAMPEDED_LOG=1` also logs each job, and
  `STAMPEDED_LOG=debug` (or `info`, `warn`, ...) sets the core's level.

## Queue policies

```ruby
Stampeded::QueuePolicy.set("extraction", max_running: 2)  # at most two at a time
Stampeded::QueuePolicy.set("mail", max_wait: 60)          # first in line once its next job waits a minute
```

The daemon turns these into a verdict per queue every second; the worker claims
rushed queues first and skips held ones. `max_running` also limits what one
worker process holds from that queue, with or without the daemon.

```ruby
Stampeded::QueuePolicy.pause("mail")   # no worker in any language claims from it
Stampeded::QueuePolicy.resume("mail")
```

Jobs already running finish, and `perform_later` and schedules keep queueing
behind the pause. `set` keeps a pause; `remove` lifts it.

## Without Rails

```ruby
Stampeded.config.database_url = ENV["DATABASE_URL"]

Stampeded.enqueue(queue: "mail", task: "welcome", args: { user_id: 42 })

worker = Stampeded::Worker.new(queue: "mail")
worker.register("welcome") { |job| Mailer.welcome(job.args["user_id"]).deliver_now }
worker.run
```

## The binaries

Platform gems include the daemon and the worker's library, `libstampeded_ffi`.
Otherwise (a platform without them, or the gem from git), build both and point
at them:

```bash
cargo build --release -p stampeded -p stampeded-ffi --bin stampeded --lib
export STAMPEDED_INSTALL_DIR=/path/to/stampeded/target/release
```

The worker, schedules, steps and job state checks load the library; a process
that only enqueues needs neither.

## Tests

```bash
createdb stampeded_test
STAMPEDED_TEST_DATABASE_URL=postgres://localhost/stampeded_test bundle exec rake test
```

`rake test` builds the library first, unless `STAMPEDED_INSTALL_DIR` names one.
