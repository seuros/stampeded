# Stampede — Vision

## What it is

A job processor with a Rust core, a Postgres 18+ substrate, a Datastar dashboard,
and MCP as a first-class control plane. Jobs can be enqueued from any language.

## Why it exists

Every serious job processor is locked to one runtime. Sidekiq is Ruby. Oban is
Elixir. Celery is Python. BullMQ is Node. The only real polyglot prior art is
Faktory (Mike Perham wrote it precisely because Sidekiq could not cross
languages) and Temporal, which is a workflow engine carrying far more weight
than "run this job".

That is a genuine gap, not a crowded market.

## The three bets

**1. Postgres is the protocol.**
If the substrate is Postgres, every language already has a client, because every
language already has a Postgres driver. Enqueue is `INSERT`. No custom TCP
protocol, no wire format to evangelise, and no fleet of half-maintained client
libraries — which is Faktory's real ongoing cost, not its server.

It also means enqueue is transactional for free: the job and the business data
it refers to commit together, or neither does. That property is worth more than
it sounds; see `04-job-lifecycle-and-claiming.md`.

**2. The resilience primitives already exist.**
The four `*-machines` crates (state, chrono, breaker, throttle) are published,
used, and cover exactly the cross-cutting concerns a job processor is made of.
Stampede is substantially an integration of crates that already work, not a
greenfield build. See `02-crate-family.md`.

**3. MCP is the differentiator.**
No job processor has a first-class MCP interface. "Why does this job keep
failing", "retry everything matching X", "what is my queue depth trend" are
natural tool calls. Everything else in Stampede can be copied in a quarter by a
motivated competitor. This cannot, quickly.

## Non-goals

- Not a workflow engine. No durable execution, no saga orchestration, no
  Temporal-style replay. If that is wanted later it is a layer, not the core.
- Not backwards compatible with older Postgres. PG18+ only, deliberately. See
  `03-postgres18.md` for what that buys.
- Not a broker. The Rust daemon is a dispatcher and supervisor. Postgres holds
  the truth.

## Licence and naming

BSD-3. Clean-room: reference implementations are read for design and protocol
documentation only, never for code. Several references are GPL/LGPL, which makes
this rule load-bearing rather than decorative — see `08-reference-survey.md`.

Name status in `09-naming.md`. Short version: `stampede` is squatted on both
registries, both plausibly reclaimable, and renaming later is cheap.
