# Stampede

A polyglot job processor. Rust core, Postgres 18+ substrate, Datastar dashboard,
MCP control plane. BSD-3.

**Status: early. Schema and worker core are built and tested; the daemon, HTTP
edge, dashboard and MCP surface are not yet wired up.**

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

## Layout

```
plans/     design notes — what was measured, and why each decision was made
schema/    001_initial.sql, 002_operations.sql  (PG18+ only)
crates/    stampede-core — worker, queue, job types
```

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
