# The machines crate family

All four are published, in use, and owned by us. Stampede is largely an
integration of these rather than new resilience code.

| crate | version | downloads | repo |
|---|---|---|---|
| `state-machines` | 0.21.0 | 15,346 | github.com/state-machines/state-machines-rs |
| `chrono-machines` | 0.8.0 | 6,365 | github.com/seuros/chrono_machines |
| `breaker-machines` | 0.17.1 | 4,166 | github.com/seuros/breaker_machines |
| `throttle-machines` | 0.2.0 | 2,184 | github.com/seuros/throttle_machines |

(Verified against crates.io 2026-09-16. `state-machines-rs` is ~31k lines: a
workspace of core/macro/cli with `no_std`, async, guards, callbacks and
hierarchical states. `libstatemachines` is the C sibling.)

All four cloned to `~/Projects/ecosystem/`.

## How each maps onto the job processor

### state-machines → job lifecycle

```
available → claimed → executing → completed
                   ↘ retryable → available
                   ↘ discarded
                   ↘ cancelled
```

Every reference implementation stores this as a bare string column and compares
it with string literals scattered across the codebase. Making transitions
type-checked, with guards, is a real advantage — illegal transitions become
compile errors instead of a production incident.

> **IMPLEMENTED, in both languages.** `crates/stampede/src/lifecycle.rs` uses
> `state-machines` (dynamic mode — a job arrives as a database row carrying its
> state as text, so the state is not known at compile time and cannot be a type
> parameter). `ruby/lib/stampede/lifecycle.rb` uses the `state_machines` gem to
> declare the same table.
>
> The Ruby side spent a while as exactly the thing this paragraph criticises —
> `def state = row["state"]` and string comparisons — which is worth recording,
> because writing the criticism did not stop me doing it.
>
> SQL remains authoritative: a polyglot worker calls the functions directly and
> loads neither file. Both machines are used to VERIFY what the database
> reported, so drift between `002_operations.sql` and the declared table is
> logged rather than discovered from a customer. A test parses the Rust
> declaration and asserts the Ruby one matches it, event by event.

Pairs with PG18 `RETURNING OLD./NEW.`, which hands the guard the before and
after state atomically in one statement. See `03-postgres18.md`.

### chrono-machines → retry and backoff

Exponential, constant and Fibonacci backoff with **full jitter**.

Full jitter as the default is a genuine correctness advantage — but the claim
needs stating precisely.

**CORRECTION.** An earlier draft said ActiveJob's `:polynomially_longer` (used
by good_job and solid_queue) "has no jitter". **It does** — 15% additive, since
Rails 6.1 sets `active_job.retry_jitter = 0.15` via `load_defaults`. The
formula is `executions**4 + rand*executions**4*0.15 + 2`. On a bare ActiveJob
without Rails defaults the class attribute is `0.0` and there genuinely is none.

The real criticism is sharper: **15% additive jitter narrows the herd, it does
not decorrelate it.** Every job in a batch still retries within a 15% window of
the same instant. Full jitter samples uniformly over `[0, cap]`, which actually
spreads them. Surveyed across 13 implementations, **not one defaults to full
jitter** — underway's 0.5 equal jitter is the closest anyone gets, pg-boss,
BullMQ and Celery implement correct jitter but default it off, and graphile,
que, db-scheduler and procrastinate have none at all.

That is the gap `chrono-machines` fills by default.

### breaker-machines → protecting the work itself

Jobs call external services. When a downstream dependency is failing, retrying
1,000 jobs against it makes the outage worse. Circuit breaking per-destination
belongs in the executor, not in user code.

### throttle-machines → queue fairness and rate limits

GCRA, token bucket, fixed window.

Worth noting: **rate limiting is a paid feature in both Sidekiq Enterprise and
Oban Pro.** Having it in the open-source core, backed by a crate that already
exists, is a positioning advantage as well as a technical one.

Uses: per-queue concurrency caps, per-tenant fairness, global rate limits
against a downstream API, backpressure when the queue is growing faster than it
drains.

## Not just crates — an ecosystem that already composes

`mcp-host` 0.3.2 (`~/Projects/ecosystem/mcp/mcphost-rs`) is the MCP
implementation (`06-dashboard-and-mcp.md`, `14-rust-stack.md`) and it is
**already built on `breaker-machines`, `chrono-machines` and
`throttle-machines`** — with a rama-backed HTTP transport.

That matters more than a dependency list. It means the control plane and the job
core are not two systems wired together: a tool call and a job execution are
guarded by the same circuit breaker, the same backoff policy and the same GCRA
rate limiter, with one set of semantics to reason about and one dashboard
surface to render them. The integration work most projects would spend a quarter
on is already done, by prior choices rather than by luck.

`state-machines` is the one not yet used there — and it is the one Stampede adds.

## What is genuinely missing

Nothing in the resilience layer. The gaps are all substrate and edges:

- job storage, claiming, visibility (`04-job-lifecycle-and-claiming.md`)
- the polyglot enqueue boundary (`05-polyglot-protocol.md`)
- dashboard (`06-dashboard-and-mcp.md`)
- MCP control plane (`06-dashboard-and-mcp.md`)

## Note

The earlier assumption that "none of my crates have proper state machines or
retry" turned out to be wrong — all four exist and are published. What is true
is that none of the *application* crates consume them. For Stampede they are
ready to use as-is.
