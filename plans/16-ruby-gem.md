# The Ruby gem

Two things ship as `stampede` on RubyGems: an **ActiveJob adapter** and the
**precompiled daemon binary**. This file works out how they fit together, and
resolves the one question that looks fatal at first glance.

Everything marked VERIFIED was read out of an installed gem or an API response
on 2026-09-18, not recalled.

## 1. Does this contradict `05-polyglot-protocol.md`?

No, and the distinction matters enough to state up front, because `05-` is the
load-bearing argument of the whole project:

> Enqueue is an `INSERT`. That is the whole client library. […] Zero client
> libraries to write or maintain.

Faktory's ongoing cost is ~10 client libraries, each of which must track a wire
protocol. **A gem that ships a binary is not that.** There is no wire format to
version, no serialisation to keep in sync across languages, and no second
implementation of the claim semantics. If the gem disappeared tomorrow, a Rails
app could do everything it does with `ActiveRecord::Base.connection.execute` and
a downloaded binary — which is precisely the test `05-` sets.

What the gem sells is **not having to have a Rust toolchain**, plus the ten
lines of glue that make `perform_later` work. Those are ergonomics over SQL, not
a protocol.

The honest cost, recorded here so it is not discovered later: the gem is a
second place where queue semantics are expressed, and it *can* drift from the
SQL functions. §5 is about keeping that surface as small as possible.

## 2. The problem that decides the architecture

**The Rust daemon cannot execute an ActiveJob job.**

An ActiveJob job is a Ruby class. `stampede-daemon` executes handlers registered
in Rust via `Worker::register`. There is no arrangement in which a Rust process
calls `UserMailer.welcome(42).deliver_now`.

Three ways out, and only one survives:

| | how | verdict |
|---|---|---|
| Rust shells out per job | `bin/rails runner` per job | **no.** Rails boot is seconds; this is a job queue |
| Rust embeds a Ruby VM | magnus / rb-sys in the daemon | **no.** One VM per daemon, GVL-bound, and it drags the entire Rails app into a process that also runs the reaper and the dashboard |
| **Split by role** | Ruby runs workers; Rust runs everything else | **yes** |

The third is not a compromise — the daemon was already built for it.
`stampede --no-worker` runs the scheduler, reaper, pruner, expirer, health
checks, HTTP API and dashboard, all of which are language-agnostic because they
operate on rows rather than on job bodies. The only role that has to know what a
job *means* is the executor.

So:

```
 stampede --no-worker          (Rust, one per deployment)
   scheduler · reaper · pruner · expirer · health · HTTP · dashboard

 stampede work --queue=mail    (Ruby, one per app server)
   stampede_claim → ActiveJob::Base.execute → stampede_complete/fail
```

Both talk to the same tables. Neither talks to the other. That is the polyglot
claim doing real work rather than being a slogan: the Ruby worker is not a
client of the Rust daemon, it is a peer.

**Consequence to accept knowingly:** a Rails-only deployment runs two processes
where Sidekiq runs one. The second is small and stateless and its roles are
exactly the ones you would otherwise have to reimplement in Ruby (cron with no
leader election, orphan reaping, retention). solid_queue ships all of that *in*
Ruby and pays for it in a supervisor, a dispatcher and a scheduler process.

## 3. Gem layout

Two gems, following the `tailwindcss-rails` / `tailwindcss-ruby` split
(VERIFIED: both installed locally; `tailwindcss-ruby` 4.3.3 ships 9 platform
gems — `arm64-darwin`, `x86_64-darwin`, `{aarch64,x86_64}-linux-{gnu,musl}`,
`x64-mingw-ucrt`, `x64-mingw32`, and a plain `ruby` fallback).

- **`stampede`** — pure Ruby, no platform. ActiveJob adapter, `Stampede.enqueue`,
  the worker loop, the Rails generator. Depends on `pg` (or `activerecord`).
- **`stampede-daemon`** — platform gems, one precompiled `stampede` binary each,
  resolved at runtime by `Gem::Platform.local`.

Why split rather than fatten `stampede` into platform variants: a deployment
that already runs the daemon under systemd or in its own container should not
download a 10 MB binary per app server to get `perform_later`. `tailwindcss-rails`
makes exactly this split and `tailwindcss-ruby` is an optional dependency for
the same reason.

**The binary-resolution shape is `tailwindcss-ruby`'s** (VERIFIED, read from
`lib/tailwindcss/ruby.rb`): `exe/<platform>/<binary>` plus a shim, an
`ExecutableNotFoundException` naming the platform when it is missing, and a
`TAILWINDCSS_INSTALL_DIR` escape hatch. We want the escape hatch —
`STAMPEDE_INSTALL_DIR` — because anyone building for a platform we do not ship
needs a supported way in, and because it is what makes the gem testable against
a locally-built debug binary.

**Cross-compilation:** the owner's own `breaker_machines` and `chrono_machines`
already ship `aarch64-linux`, `arm64-darwin` and `x86_64-linux` platform gems
(VERIFIED in this repo's `Gemfile.lock`), so the pipeline exists. Note the
difference: those are **linked extensions** (`.bundle` per Ruby ABI under
`lib/<name>_native/<abi>/`, VERIFIED — 3.4 and 4.0 side by side), whereas this
is a **standalone executable** and therefore has no Ruby ABI dimension at all.
One binary per platform, not per platform × Ruby version.

## 4. The ActiveJob adapter

The contract is small (VERIFIED against `activejob-8.1.3.1`,
`abstract_adapter.rb`): `enqueue(job)`, `enqueue_at(job, timestamp)`, and
`stopping?`. `enqueue_all(jobs)` is optional and present on the Sidekiq adapter.

```ruby
def enqueue(job)
  job.provider_job_id = insert(job, scheduled_at: nil)
end

def enqueue_at(job, timestamp)
  job.provider_job_id = insert(job, scheduled_at: Time.at(timestamp).utc)
end
```

`provider_job_id` is the row's uuid, which is what makes an ActiveJob job
inspectable through the MCP tools and the dashboard.

### 4.1 Transactional enqueue is the selling point, not a footnote

Rails 8 defaults `enqueue_after_transaction_commit` to `false` (VERIFIED,
`enqueuing.rb:53`). For a Redis-backed queue that default is a hazard: the job
can be picked up before the transaction commits and find no row. Sidekiq's
docs have warned about this for a decade.

For Stampede the same default is *correct and free*, because the enqueue IS an
`INSERT` in the caller's own transaction. Roll back, and the job is gone. There
is nothing to reconcile.

**The adapter must therefore not opt into after-commit**, and the README should
say why, because "we work correctly with the framework default" is a weaker
claim than the truth, which is that the problem does not arise.

### 4.2 The retry conflict — the one real semantic clash

ActiveJob's `retry_on` is **in-process**: the job rescues, and re-enqueues
*itself as a new job*. Stampede's attempt accounting is in SQL and increments
**at claim** (`04-`), which is the poison-job brake the whole schema is built
around.

Run both and you get two independent budgets: `retry_on attempts: 5` inside a
job with `max_attempts: 25` can produce 125 executions, and the Stampede-side
`attempt` column will read 1 for each of them because every ActiveJob retry is
a brand new row. Every dashboard number becomes a lie.

**Decision: Stampede owns retries; `retry_on` is documented as incompatible and
detected at boot.** The adapter should raise — not warn — if a job class
declares `retry_on`, naming the Stampede equivalent (`max_attempts`,
`backoff_base_ms`, `backoff_max_ms` on the row). A warning gets filtered out of
production logs; a raise at class-load is seen once, by the person who wrote it.

`discard_on` is fine and needs no special handling: a discarded exception never
reaches us, so the job simply completes.

**Open:** `retry_on` with a `wait:` lambda is genuinely expressive and has no
Stampede equivalent yet. The row carries `backoff_base_ms`/`backoff_max_ms`,
which covers exponential-with-cap but not arbitrary schedules. Decide whether
to grow the row or to accept the gap.

### 4.3 Mapping

| ActiveJob | Stampede | note |
|---|---|---|
| `queue_name` | `queue` | needs `stampede_create_queue` for a dedicated partition |
| `priority` | `priority` | ActiveJob is nullable; default 0 |
| `scheduled_at` | `scheduled_at` | |
| `job.serialize` | `args` jsonb | inspectable in psql and in `job_search` — the reason `05-` chose jsonb over bytea |
| `provider_job_id` | `id` | uuidv7 |
| — | `destination` | no ActiveJob concept; expose as a class-level DSL |
| — | `dead_letter_queue` | ditto |

## 5. Keeping the drift surface small

The gem is a second expression of queue semantics (§1). Two rules hold it down:

1. **The worker calls the SQL functions and nothing else.** No Ruby-side claim
   query, no Ruby-side attempt arithmetic, no Ruby-side state transitions.
   `stampede_claim` / `stampede_complete` / `stampede_fail` / `stampede_touch`
   are the entire interface, exactly as for the Rust worker. This is why those
   are functions rather than daemon-side SQL (`05-`).
2. **No schema in the gem.** The gem does not carry a migration that recreates
   the tables. It ships `schema/*.sql` verbatim and a rake task that applies
   them, so there is one definition. A gem-maintained Rails migration would be
   a second schema that drifts on its own release cycle.

## 6. Still open

- **Worker concurrency model.** Threads (Sidekiq), processes (solid_queue's
  forks), or one job at a time? Ruby's GVL makes threads fine for IO-bound jobs
  and useless for CPU-bound ones. solid_queue forks and pays for a supervisor;
  Sidekiq threads and documents the caveat. Not decided.
- **Does `stampede work` boot Rails?** It must, to execute a job class. That
  makes it a Rails command (`bin/rails stampede:work`) rather than a bare
  binary, which is a different shape from the Rust worker and worth being
  deliberate about.
- **Graceful shutdown and the interruption budget.** `15-` says a graceful
  shutdown must not consume the budget. The Ruby worker needs the same drain
  semantics the Rust one has, and nothing currently tests that property in
  either.
- **`retry_on(wait:)`**, per §4.2.
- **Which platforms to ship.** The owner's existing gems do three; tailwindcss
  does nine. musl matters for Alpine containers, which is most of them.
- **Whether `stampede-daemon` should be a hard or optional dependency.** Optional
  matches `tailwindcss-rails`, but optional means a `gem install stampede` that
  appears to work and then cannot start a scheduler.
