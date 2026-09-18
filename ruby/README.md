# stampede (Ruby)

Rails integration for [Stampede](https://github.com/seuros/stampede): an
ActiveJob adapter, a worker, and a launcher for the precompiled daemon.

```ruby
gem "stampede"
gem "stampede-daemon"   # optional: the precompiled binary
```

```ruby
# config/application.rb
config.active_job.queue_adapter = :stampede
```

```bash
bin/rails stampede:install   # apply the schema (Postgres 18+)
bin/rails stampede:work      # run a worker
bin/rails stampede:daemon    # scheduler, reaper, pruner, dashboard
```

## Two processes, and why

`bin/rails stampede:work` runs your jobs. `bin/rails stampede:daemon` runs
everything else — scheduler, reaper, pruner, expirer, health checks, HTTP API,
dashboard.

They are split because **the Rust daemon cannot execute an ActiveJob job**: an
ActiveJob job is a Ruby class, and the daemon would have to boot your entire
Rails app to run one. Every other role operates on *rows* rather than on job
bodies, so those stay in the binary where they cost nothing.

The two are peers on the same tables, not client and server. Neither talks to
the other. That is the same arrangement a Python or Go worker would have.

If you skip the daemon you lose cron, orphan recovery, retention and the
dashboard — everything else works.

## This is not a client library

Enqueueing is an `INSERT`. Claiming goes through SQL functions that the Rust
worker calls too, so there is exactly one implementation of the attempt
accounting, the fence and the ownership rules. If this gem vanished you could
do everything it does with `connection.execute` and a downloaded binary. What
it sells is the glue that makes `perform_later` work, and not needing a Rust
toolchain.

## Transactional enqueue

```ruby
Order.transaction do
  order.save!
  ShipOrderJob.perform_later(order)   # same transaction
end                                    # rolls back? the job is gone too
```

Rails 8 defaults `enqueue_after_transaction_commit` to `false`, which for a
Redis-backed queue is a hazard — the job can be picked up before the
transaction commits and find no row, which is why Sidekiq has warned about this
for a decade. Here it is free and correct: the enqueue is an `INSERT` on your
own connection.

Keep jobs in the same database as your data. A separate queue database puts the
`INSERT` in a different transaction and gives away the one thing Redis cannot
do.

## `retry_on` is refused

```ruby
class ChargeCard < ApplicationJob
  retry_on Net::OpenTimeout   # => Stampede::RetryConflict
end
```

ActiveJob's `retry_on` re-enqueues the job as a **new row**. Stampede counts
attempts **at claim** — that is what stops a job which kills its worker from
retrying forever. With both active, `retry_on attempts: 5` inside a job with
`max_attempts: 25` can run 125 times, and every row reports `attempt 1`, so the
dashboard, the metrics and `failure_summary` all lie.

It raises at class-load rather than warning, because a warning gets filtered
out of production logs. Let Stampede own retries instead:

```ruby
class ChargeCard < ApplicationJob
  stampede max_attempts: 5,
           destination: "stripe.com",        # keys the circuit breaker
           dead_letter_queue: "payments_dlq",
           expire_seconds: 120                # enforced by the database
end
```

`discard_on` is fine and needs no change — a discarded exception never reaches
us, so the job simply completes.

## Recurring jobs

```ruby
Stampede::Schedule.recurring(
  key:  "nightly-report",
  job:  ReportJob,
  cron: "0 3 * * *",
  args: [Date.current],
  timezone: "America/New_York"
)
```

No leader election and no separate scheduler process: the schedule row *is* the
due row, and its row lock is the election, so every daemon can tick
concurrently.

DST is a per-schedule choice. An expression that **names** its hours gets
`wall_clock` — "the 03:00 report" runs *once* on a fall-back day, where every
surveyed implementation runs it twice. One using `*` or a step gets `instant`,
so "every two hours" runs 13 times on a 25-hour day rather than 12.

## Worker concurrency

Threads. Rails jobs are overwhelmingly IO-bound and the GVL is released for
database calls, HTTP and file IO.

**CPU-bound jobs will not scale on this pool.** Run more processes with a
smaller pool each — the same advice Sidekiq gives. solid_queue forks instead
and pays for a supervisor process.

```bash
QUEUE=mail CONCURRENCY=10 bin/rails stampede:work
```

A `SIGTERM` drains: in-flight jobs are allowed to finish and report their own
outcome, so a routine deploy never charges a job an *interruption*. Jobs that
outlive `shutdown_timeout` are reclaimed by the reaper and do get charged,
which is correct — from the queue's point of view the process did die on them.

## Without Rails

```ruby
Stampede.config.database_url = ENV["DATABASE_URL"]

Stampede.enqueue(queue: "mail", task: "welcome", args: { user_id: 42 })

worker = Stampede::Worker.new(queue: "mail")
worker.register("welcome") { |job| Mailer.welcome(job.args["user_id"]).deliver_now }
worker.run
```

## The daemon binary

`stampede-daemon` ships one precompiled binary per platform. For a platform
that has no gem, build it and point at it:

```bash
cargo build --release
export STAMPEDE_INSTALL_DIR=/path/to/stampede/target/release
```

## Tests

```bash
createdb stampede_test
STAMPEDE_TEST_DATABASE_URL=postgres://localhost/stampede_test bundle exec rake test
```

Each test class gets its own Postgres schema, so these run alongside the Rust
suite against the same database.
