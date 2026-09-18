# Job lifecycle, claiming, and orphan recovery

The core design decision. Everything else hangs off the answer here.

Source study of Oban (Elixir), River (Go) and Graphile Worker (JS), read from
cloned source at `~/Projects/ecosystem/reference/postgres/`. All three are
permissively licensed (Apache / MPL / MIT), so reading them is safe under our
clean-room rule.

## How the three claim

### Oban — state column + CTE optimisation fence

`oban/lib/oban/engines/basic.ex:108-145`

```sql
WITH subset AS (
  SELECT id FROM oban_jobs
  WHERE state = 'available' AND queue = $1
  ORDER BY priority, scheduled_at, id
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
UPDATE oban_jobs SET state = 'executing', attempt = attempt + 1,
       attempted_at = now(), attempted_by = ARRAY[node, uuid]
FROM subset
WHERE oban_jobs.id = subset.id AND attempt < max_attempts
RETURNING *;
```

**The CTE is not stylistic.** From the comment at `basic.ex:119`:

> The Postgres planner may choose to generate a plan that executes a nested loop
> over the LIMITing subquery, causing more UPDATEs than LIMIT. That could cause
> unexpected updates, including attempts > max_attempts in some cases. The
> solution is to use a CTE as an "optimization fence".

This is the single most valuable finding of the survey. It is a silent
correctness bug that you would never predict, and it is invisible until a job
exceeds `max_attempts` in production.

### River — same shape, bounded attempt history

`river/riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql`, `JobGetAvailable`

Structurally identical (CTE + `FOR UPDATE SKIP LOCKED` + `UPDATE ... FROM`),
with two differences worth stealing:

- `SELECT *` in the CTE rather than `SELECT id`
- `attempted_by` maintained as a **bounded ring buffer**: `array_append` with a
  slice when length exceeds `max_attempted_by`. Unbounded attempt history is a
  slow leak on heavily-retried jobs.

It also filters `scheduled_at <= coalesce($now, now())`, threading an injectable
clock through the query — good for testing, worth copying.

### Graphile Worker — lease-based, no state column

`graphile-worker/sql/000011.sql`, `graphile-worker/src/generated/sql.ts:95-125`

```sql
WHERE (locked_at IS NULL OR locked_at < now() - job_expiry)
  AND run_at <= now()
  AND attempts < max_attempts
ORDER BY priority, run_at, id
LIMIT 1
FOR UPDATE OF job_queues
SKIP LOCKED
```

Fundamentally different model:

- no `state` column at all — availability is derived from `locked_at`/`locked_by`
- `is_available` is a **stored generated column**:
  `(locked_at IS NULL) AND (attempts < max_attempts)`
- `FOR UPDATE OF job_queues` locks the *queue* row, not the job row, which buys
  per-queue serialisation for free
- validates `worker_id` length (>= 10) defensively inside the function

## Orphan recovery — where they actually diverge

| | mechanism | failure mode |
|---|---|---|
| **Oban (OSS)** | **pure time lease.** `Oban.Lifeline`, `rescue_after: :timer.minutes(60)` (`lib/oban/lifeline.ex:75`) rescues on elapsed `attempted_at` | **same as Graphile, 60 min instead of 4 h** |
| **River** | `attempted_by` + a separate rescuer | needs a rescue process |
| **Graphile** | pure time lease: `locked_at < now() - interval '4 hours'` resets it (`resetLockedAt.ts:23-28`) | **a job slower than the lease runs twice** |
| **solid_queue** | `solid_queue_processes` heartbeat + three typed process errors; crash victims are **never auto-re-dispatched** | needs a liveness table and a reaper |

**CORRECTION.** An earlier draft of this file credited Oban with a producer
heartbeat table. That is **Oban Pro**. OSS Oban uses a flat 60-minute time
lease — the exact mechanism this file criticises Graphile for, just with a
shorter default. Verified: no heartbeat anywhere in OSS `lib/`, and no liveness
table in its migrations.

**The OSS prior art for the heartbeat design is solid_queue**, not Oban:
`solid_queue_processes` with `last_heartbeat_at`, a 5-minute alive threshold,
and three *distinct* named errors — `ProcessPrunedError`, `ProcessExitError`
(carrying the signal/exit status), `ProcessMissingError` — with an explicit
refusal to auto-re-dispatch a job whose worker died.

Graphile's is the simplest and the most dangerous: correctness depends on a
wall-clock guess about maximum job duration, and the default is a flat 4 hours.
A job that legitimately runs longer gets picked up by a second worker while the
first is still going. That is silent at-least-twice delivery.

Note Graphile also guards `complete_jobs` and `permanently_fail_jobs` with the
same `locked_at IS NULL OR locked_at < now() - 4h` predicate, so a worker that
overran its lease cannot even complete its own job.

## Decision for Stampede

**Take solid_queue's heartbeat model, not a time lease.** A lease turns a slow
job into duplicate execution, and no Postgres version fixes that. A heartbeat
costs a `stampede_workers` table and a reaper, which is a known, bounded cost.

Worth stating plainly: **every Postgres queue surveyed except solid_queue uses a
time lease** — Oban OSS 60 min, Graphile 4 h, pg-boss 15 min, underway a
30-second renewable lease. The lease is the popular choice because it is cheap,
and it is wrong for the same reason in all of them: correctness depends on a
wall-clock guess about maximum job duration. underway's renewable lease is the
best of the leases because the heartbeat pushes the expiry forward, which makes
it a heartbeat in all but name.

Concretely:

- `stampede_workers` heartbeat table; rescuer requeues jobs whose worker went
  silent
- `attempted_by` as a **bounded** array (River's trick)
- claim via `UPDATE ... RETURNING OLD.state, NEW.state`, with `state-machines`
  validating the transition from the atomically-returned pair
- `uuidv7()` primary keys — ordering cursor for free
- ~~`GENERATED ... VIRTUAL` for derived predicates~~ **WITHDRAWN.** Verified on
  18.6: generation expressions must be IMMUTABLE (so anything with `now()` is
  rejected) *and* virtual generated columns cannot be indexed at all. Graphile's
  `is_available` is STORED precisely because it backs the fetch indexes. Claim
  predicates go inline in a partial index instead. See `03-postgres18.md`.
- injectable clock in the claim query (River's `coalesce($now, now())`)

### Increment `attempts` AT CLAIM, not at failure

The single most consequential line in the schema, and it is cheap to get wrong.

Surveyed across twelve implementations, **half have an unbounded poison loop**,
and the distinguishing factor is exactly this:

| bounded — increments at claim/fetch | unbounded — increments only on a path requiring a live worker |
|---|---|
| graphile-worker (`getJobs.ts:246`), pg-boss (`plans.ts:1979`), underway (`attempt_count < max_attempts` in the claim predicate), bullmq (separate `stc` counter + `defa` breaker), asynq, solid_queue (never re-dispatches a crash victim) | procrastinate (`schema.sql:254-258` sets only `status`/`worker_id`), celery, que, neoq, db-scheduler (default handler *revives* without counting), good_job |

If the counter only advances when the worker survives to report, a job that
segfaults its worker is re-claimed **forever** — `max_attempts` is never reached
because the counter never moves. Procrastinate compounds it: a stuck `doing` row
holds the unique index `procrastinate_jobs_lock_idx_v1 (lock) WHERE
status='doing'`, so one poison job blocks every other job sharing that lock.

Stampede increments at claim. A worker that dies has already spent the attempt.

**The CTE fence becomes unnecessary** — but only because `RETURNING OLD.`
removes the need for the LIMIT subquery at all. That reasoning MUST be written
into the schema comments, or someone will eventually "simplify" the claim query
back into Oban's bug without knowing why the fence was there.

## Still open

- ~~claim batch size / per-queue vs global~~ — **per-queue, configurable
  batch**, with GCRA fairness applied AFTER claiming: claim order is by
  priority and age, so a tenant's flood is legitimately first in line and
  execution is the only honest place to intervene.
- ~~SQL function vs daemon-side SQL~~ — **function.** It is what makes a Ruby
  or Python worker a peer rather than a client, and it is why there is exactly
  one implementation of the fence and the attempt accounting.
- ~~`LISTEN/NOTIFY` vs polling~~ — **both** (`12-`). Polling is the correctness
  mechanism and never switches off; the trigger is a latency accelerant that
  can be dropped with one `DROP TRIGGER`.
- ~~completed-job retention and partitioning (blocked on `pg_partman` absence)~~
  **answered in `13-retention-and-bloat.md`**: LIST-partition by state (two
  leaves), no `pg_partman`, no time partitioning. Note that completion becomes
  a cross-partition row move, which can raise SQLSTATE 40001 and needs a retry.
