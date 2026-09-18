# Unique jobs and deduplication

> **RESOLVED 2026-09-17 — see `README.md`.** Partitioning is by **queue**, not
> state, so the recommended index becomes `UNIQUE (queue, dedup_key) WHERE
> dedup_key IS NOT NULL` and is legal. Uniqueness is therefore **per-queue**.
>
> Also corrected: `sha256(convert_to(dedup_scope,'UTF8'))` as written in this
> file **cannot be used in a generated column** — `convert_to` is STABLE, not
> IMMUTABLE. Verified working alternatives: `sha256(dedup_scope::bytea)` or
> `decode(md5(dedup_scope),'hex')`.



Source study of Oban, River, Graphile Worker and eight other permissively
licensed queues, plus **sixteen experiments executed against live PostgreSQL
18.6** (`commonsharecore-db-1`). Facts marked *verified* were executed; anything
else is inference and is labelled as such.

Experiments 12-16 were added in a second pass. Two of them change §7: the
recommended `dedup_period` bucket was specified on the wrong timestamp column,
and the recommended unique index is **illegal on a partitioned table** unless the
partition key is folded into it. Both are corrected in place below.

Reading rule from `08-reference-survey.md` observed: nothing here came from
Faktory, Sidekiq, dramatiq, Hangfire or JobRunr source.

## 1. Oban — query + advisory lock, not a constraint

**Expression.** `unique:` is a keyword list: `fields` (`:args | :meta | :queue |
:worker`, default `[:args, :queue, :worker]`), `keys` (a subset of args/meta
keys), `period` (seconds or `:infinity`, default 60), `states` (default
`:successful`), `timestamp` (`:inserted_at | :scheduled_at`). Defaults at
`oban/lib/oban/job.ex:228-234`. State groups resolve at `job.ex:431-436`:
`:successful` = `suspended available scheduled executing retryable completed`,
`:incomplete` drops `completed`, `:all` is all eight.

**Enforcement — application-side, inside a transaction.**
`oban/lib/oban/engines/basic.ex:74-88` branches: if the changeset carries
`unique`, it opens a transaction and calls `insert_unique/5`
(`basic.ex:447-466`), which is `advisory lock → SELECT → INSERT or UPDATE`:

- `acquire_lock/2` (`basic.ex:551-559`) runs `pg_try_advisory_xact_lock($1)`
  where the key is `:erlang.phash2([keys, states, dynamic])` plus a hash of the
  schema prefix (`basic.ex:493`, `551-553`). **`try`, not blocking.**
- `fetch_job/3` (`basic.ex:563-568`) runs the uniqueness query built at
  `basic.ex:487-503`: `state IN (...)` + `since_period` +
  a dynamic predicate per field. For `args`/`meta` it uses jsonb containment —
  `@>` and `<@` both ways for an exact match, `@>` alone when `keys` narrows it
  (`basic.ex:507-523`).
- `since_period/3` (`basic.ex:545-549`) is a **true sliding window**:
  `timestamp >= now() - period`. Not a bucket.

**There is no unique index anywhere in Oban's migrations.** Grepped
`oban/lib/oban/migrations/postgres/v01..v14.ex` — the only `create index` calls
are on `scheduled_at` (`v02.ex:12,41`, `v05.ex:22`).

**On conflict.** Returns the *existing* job with the virtual field
`conflict?: true` (`job.ex:156`, `basic.ex:453`). If `:replace` was given
(`job.ex:252`), `resolve_conflict/4` (`basic.ex:468-485`) updates the listed
keys on the existing row, per its current state. So: returns existing, or
replaces — never raises, never silently drops. Except:

**Two real holes.**
1. `pg_try_advisory_xact_lock` failing means *another enqueuer holds the same
   lock right now*. Oban's handler (`basic.ex:455-458`) applies the changeset
   in memory and returns a job struct with `conflict?: true` — **and no `id`,
   because nothing was written**. Under concurrent enqueue of the same key the
   loser's job is dropped and the caller gets something that looks like a job.
2. Bulk insert has no uniqueness at all. `insert_all_jobs/3`
   (`basic.ex:91-96`) forces `on_conflict: :nothing` and never consults
   `unique`; `oban/lib/oban.ex:819-820` says bulk unique is an Oban Pro feature.

**Verdict:** correct only because every enqueuer runs the same Elixir code. Not
portable to a plain INSERT. Rules Oban's approach out for us — see §7.

## 2. River — hashed key column + partial unique index, and *why it moved*

You remembered right. River shipped uniqueness twice.

**v1 (removed in 0.13.0):** advisory-lock based, same shape as Oban.
`river/CHANGELOG.md:621` and `:631`: *"Version 0.13.0 removes the original
advisory lock based unique jobs implementation that was deprecated in v0.12.0."*

**v2 (migration 005, River 0.10.0):** a `unique_key bytea` column plus
`CREATE UNIQUE INDEX river_job_kind_unique_key_idx ON river_job (kind,
unique_key) WHERE unique_key IS NOT NULL`
(`river/riverdriver/riverpgxv5/migration/main/005_migration_unique_client.up.sql`).

**v3 (migration 006, River 0.12.0):** replaced `(kind, unique_key)` with
`(unique_key)` alone, and moved "which states count" into a per-row `bit(8)`
mask evaluated *inside the index predicate*
(`riverdriver/riverpgxv5/migration/main/006_bulk_unique.up.sql`):

```sql
CREATE UNIQUE INDEX river_job_unique_idx ON river_job (unique_key)
  WHERE unique_key IS NOT NULL AND unique_states IS NOT NULL
    AND river_job_state_in_bitmask(unique_states, state);
```

with `river_job_state_in_bitmask` declared `IMMUTABLE` in the same file so it is
legal in a predicate.

**The stated migration reasons** (`CHANGELOG.md:709-726`, PR #590):
- bulk insert (`InsertMany`) could not use advisory locks at all — index-backed
  uniqueness is the only thing that works for multi-row INSERT;
- *"maintains the speed advantage of the newer index-backed uniqueness system"* —
  the advisory-lock path is explicitly described as slow;
- customisable `ByState` (add `cancelled`/`discarded`, drop `retryable`) became
  possible only once the state set was a per-row value rather than a query;
- key stability: `CHANGELOG.md:720` — *"`encoding/json` encodes struct fields in
  their defined order, so merely reordering struct fields would previously have
  been enough to cause a new job to not be considered identical"*. Hence the
  alphabetical-sort dance in `internal/dbunique/db_unique.go:96-109`.

**Key construction.** `db_unique.go:47-54`: build a string
`&kind=…&args=…&period=…&queue=…`, then `sha256`. Period is
`ScheduledAt.Truncate(ByPeriod)` (`db_unique.go:117`) — a **bucket**, not a
sliding window. `ExcludeKind` drops kind from the key (`db_unique.go:61-63`).
Args narrowing is by `river:"unique"` struct tags (`insert_opts.go:113-121`).

**On conflict.** `riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql:315-321`:

```sql
ON CONFLICT (unique_key) WHERE <the index predicate, repeated verbatim>
  -- Something needs to be updated for a row to be returned on a conflict.
  DO UPDATE SET kind = EXCLUDED.kind
RETURNING sqlc.embed(river_job), (xmax != 0) AS unique_skipped_as_duplicate;
```

Two tricks worth naming: the **no-op `DO UPDATE`** exists only because
`DO NOTHING` returns zero rows, so you lose the existing job's id; and
`xmax != 0` is how you tell an insert from a conflict-update. PG18 makes both
obsolete — see §6.

The bulk path uses `DO NOTHING` and returns nothing (`river_job.sql:356-360`).

**The cost River pays for a state-dependent predicate** is in
`river_job.sql:539-618` (`JobSchedule`): when a `scheduled`/`retryable` job
transitions to `available` it *enters* the index, so the transition itself can
violate the constraint. The scheduler therefore pre-resolves conflicts with a
`ROW_NUMBER() OVER (PARTITION BY unique_key)` plus a lookup of already-indexed
rows, and **discards the losers**, stamping
`metadata || '{"unique_key_conflict": "scheduler_discarded"}'`
(`river_job.sql:606`). This code has already produced one shipped bug:
`CHANGELOG.md:646` — jobs *"considered in conflict with themselves and moved to
`discarded` by mistake"*, fixed by the `river_job.id != jobs_with_rownum.id`
guard at `river_job.sql:578`.

`ByState` cannot omit `pending`, `scheduled`, `available`, `running`
(`insert_opts.go:276-284`) — those are what make the scheduler's reasoning sound.

## 3. Graphile Worker — one nullable `key` column, no state predicate

Not a partial index and not a hash. `graphile-worker/sql/000011.sql:62`:

```sql
key text unique constraint jobs_key_check check (length(key) > 0 and length(key) <= 512)
```

A **plain, full `UNIQUE` constraint on a nullable column.** Availability is not
in the predicate at all. Release is by setting `key = null`.

**`job_key_mode` has three values, not four.** `src/interfaces.ts:634`:
`"replace" | "preserve_run_at" | "unsafe_dedupe"`. There is no `unlisted` mode —
grepped `src/`, `sql/`, `website/`; the only hits for `unlisted` are zero.
`sql/000011.sql:365` raises `GWBKM` for anything else.

**Queued vs locked.** This is the interesting part. `add_jobs`
(`sql/000011.sql:94-178`, current version `sql/000020.sql`) does two things in
order:

1. **Pre-clear the key on jobs that are no longer available**
   (`000011.sql:119-127`): `set key = null, attempts = jobs.max_attempts` where
   `jobs.key = spec.job_key and is_available is not true`. The comment at
   `:114-118` states the semantics: a locked job *"must have already started
   executing (i.e. its world state is out of date, and the fact add_job has been
   called again implies there's new information that needs to be acted upon)"*.
   So a running job is orphaned from its key and a **new** job is created
   alongside it. `is_available` is the stored generated column
   `(locked_at is null) and (attempts < max_attempts)` (`000011.sql:67`).
2. `ON CONFLICT (key) DO UPDATE SET … attempts = 0, last_error = null,
   revision = revision + 1 … WHERE jobs.locked_at is null`
   (`000011.sql:159-176`). `preserve_run_at` is a single CASE:
   `when job_key_preserve_run_at is true and jobs.attempts = 0 then jobs.run_at`
   (`000011.sql:164-167`) — i.e. preserve only if it has not yet been tried.

`unsafe_dedupe` (`000011.sql:310-363`) skips step 1 entirely and does
`ON CONFLICT (key) DO UPDATE SET revision = revision + 1` — which is a pure
"return the existing row" no-op. The docstring at `:323-329` is unusually
honest: *"This is dangerous because it means that whatever state change
triggered this add_job may not be acted upon … If in doubt, DO NOT USE THIS."*

**Residual race (inference, not verified):** between the pre-clear and the
INSERT, a worker can lock the row. The `ON CONFLICT … WHERE jobs.locked_at is
null` then matches nothing, the statement returns no row, and `add_job` returns
NULL — the enqueue is silently lost. Graphile's own source flags uncertainty
here: `000011.sql:129-130` *"TODO: is there a risk that a conflict could occur
depending on the isolation level?"*

`remove_job` (`000011.sql:213-241`) uses the same two-step: delete if unlocked,
otherwise null the key and burn the attempts.

New in `sql/000020.sql`: on conflict, if both payloads are JSON arrays they are
**concatenated** rather than replaced. That is coalescing-by-key — N enqueues
become one job carrying N payloads. Genuinely different from everything else in
the survey.

## 4. The rest — only what is genuinely new

| system | mechanism | new idea |
|---|---|---|
| **pg-boss** | six policy-scoped partial unique indexes, one per queue partition (`src/plans.ts:885-899, 929-935`) | uniqueness is a **queue policy**, not a per-job flag; a `standard` queue carries no index at all. `stately` puts `state` in the index *key* (`plans.ts:894`) so one job per key **per state** may coexist. `COALESCE(singleton_key,'')` (`plans.ts:886`) collapses NULL so "no key" = queue-wide singleton via the same index. |
| **pg-boss** | time slot computed **inside the INSERT** (`plans.ts:2209-2213`): `'epoch' + 1s * (secs * floor((epoch(now()) + offset)/secs))` | clients never compute the bucket → no clock-skew bucket splitting. Cron overrides it via a private `__singletonSlot` column (`plans.ts:2166-2173`). Debounce = retry with `offset = singletonSeconds` to land in the next bucket (`src/manager.ts:1390-1393`). Conflict → `DO NOTHING`, `createJob` returns `null` (`plans.ts:2261`, `manager.ts:1409`). |
| **procrastinate** | **two** orthogonal columns (`procrastinate/sql/schema.sql:99-102`): `queueing_lock` UNIQUE `WHERE status='todo'`, and `lock` UNIQUE `WHERE status='doing'` | queue-depth ≤ 1 and parallelism ≤ 1 are *separate, composable* guarantees. Most systems fuse them. Conflict is a raw 23505; the client matches `constraint_name` and regexes the key out of the error detail (`sync_psycopg_connector.py:29-42`) to raise `AlreadyEnqueued`. |
| **procrastinate** | periodic dedup in a **separate ledger table**, `UNIQUE (task_name, periodic_id, defer_timestamp)` (`schema.sql:85-87`, used `schema.sql:160-170`) | dedup lifetime decoupled from job-row lifetime; self-GC'ing. **good_job** (`create_good_jobs.rb.erb:88`, unique on `(cron_key, cron_at)`) and **solid_queue** (`queue_schema.rb:85`, unique on `(task_key, run_at)`) independently converged on the same shape for cron. Three projects agreeing is a signal. |
| **neoq** | content-addressed by default: `md5(queue ‖ json(payload))` (`jobs/jobs.go:50-76`), index `(queue, fingerprint, status) WHERE NOT status='processed'` | caller supplies **nothing**; a user key is an *override*. Caveat: `status` is an index **key column**, so the same fingerprint in `new` *and* `failed` both survive. |
| **underway** | `UNIQUE (task_queue_name, concurrency_key) WHERE concurrency_key IS NOT NULL AND state IN ('pending','in_progress')` (`migrations/20240921151751_0.sql:62-64`) | the cleanest pure-constraint design in the set: bare INSERT, no `ON CONFLICT`, no client handling (`src/queue.rs:549-579`). 23505 goes straight to the caller. |
| **good_job** | `pg_advisory_xact_lock(hashtext(key))` → `COUNT(*)` → compare → `raise ActiveRecord::Rollback` (`concurrency.rb:104-156`) | at *perform* time it does not count — it selects the N oldest running with that key and asks "am I in that set?" (`concurrency.rb:167-171`). Deterministic and FIFO-fair. Also: throttle-rejected jobs are excluded from the throttle window (`concurrency.rb:186`) or the throttle deadlocks itself. |
| **solid_queue** | counting semaphore **rows**, mutated by guarded UPDATE (`app/models/solid_queue/semaphore.rb:77-83`) | `UPDATE … SET value = value-1 WHERE key=? AND value>0`, checking affected rows — lock-free, and expresses N > 1, which no unique index can. `expires_at` (`semaphore.rb:88-90`) recovers from crashed workers with no liveness protocol. `on_conflict: :discard | :block` is a first-class knob (`lib/active_job/concurrency_controls.rb:11`). |
| **bullmq** | `SET <prefix>:de:<id> <jobId> PX ttl NX` (`src/commands/includes/setDeduplicationKey.lua:5-12`) | `replace: true` (`deduplicateJob.lua:30-59`) evicts the pending job and takes its place — **last-write-wins**. Every SQL system here is first-write-wins. If you want "coalesce N updates, keep the latest payload", first-write-wins is the wrong primitive. |
| **asynq** | `SETNX` on `asynq:{queue}:unique:<type>:<md5(payload)>` (`internal/rdb/rdb.go:269-284`) | stores `unique_key` back onto the task hash (`rdb.go:281`) so the worker can release it on completion without re-deriving. "unique until done" rather than "unique for TTL". |
| **que** | none | README delegates to a third-party gem (`README.md:195`); docs advocate idempotent job bodies (`docs/README.md:667`). |
| **celery** | none at enqueue | `worker_deduplicate_successful_tasks` (`celery/app/defaults.py:341`, default off) is *redelivery* dedup, requires `acks_late`, checks the result backend (`celery/app/trace.py:508-522`). Not uniqueness. |

## 5. The hard cases

**Against executing jobs.** Only expressible if the predicate references live
state. Oban includes `executing` in `:successful` by default (`job.ex:435-436`);
River includes `running` (`rivertype/river_type.go:633-642`); underway includes
`in_progress`; procrastinate deliberately does **not** — `queueing_lock` covers
only `todo`, because the intent there is queue depth, not exclusion. Graphile
goes the opposite way and actively *releases* the key when a job starts
executing (`sql/000011.sql:114-127`). Both are defensible; they are different
products. Verified in §6 that a state-predicated index does enforce this.

**Against completed jobs in a window.** Two incompatible techniques:

| | window | index-able | boundary behaviour |
|---|---|---|---|
| Oban `since_period` (`basic.ex:545-549`) | true sliding: `ts >= now() - period` | **no** — `now()` is not immutable | exact |
| River `Truncate(ByPeriod)` (`db_unique.go:117`), pg-boss `floor(epoch/secs)` (`plans.ts:2211`) | fixed bucket | **yes** | 10:59 and 11:01 are different buckets; two jobs one minute apart both run |

You cannot have both. A sliding window forces a query, a query forces a lock,
a lock forces client code. This is the single biggest fork in the design.

Secondary point: with a bucket in the key, uniqueness against *completed* jobs
also depends on retention — once the completed row is pruned the key is free
regardless of the bucket. River's docs (`insert_opts.go:188-193`) say this
plainly.

**Two simultaneous enqueuers.** Verified, §6. Summary: with an index, the
database resolves it and the loser learns the winner's id. With an advisory
`try`-lock, the loser's job is dropped (Oban, `basic.ex:455-458`). With
SELECT-then-INSERT, the loser re-SELECTs and infers (db-scheduler,
`JdbcTaskRepository.java:173-194`, which does not even inspect the SQLSTATE).

## 6. What PG18 actually does — executed on 18.6

Eleven experiments. All results below were run, not recalled.

**Virtual generated columns cannot be indexed.**
```
CREATE TABLE v1(a int, b int GENERATED ALWAYS AS (a*2) VIRTUAL);
CREATE UNIQUE INDEX v1_b ON v1(b);
ERROR:  indexes on virtual generated columns are not supported
```
This kills the obvious idea from `03-postgres18.md` — a virtual `unique_key`.
A derived, indexed dedup key **must be `STORED`**, and therefore costs disk.

**Stored generated columns need a strictly IMMUTABLE expression, and two
obvious building blocks are not.** `convert_to(text, name)` is **stable**
(depends on server encoding) → `sha256(convert_to(…))` is rejected with
*"generation expression is not immutable"*. `extract(epoch from timestamptz)`
and `date_trunc(text, timestamptz)` (2-arg) are **stable** too; the 3-arg
`date_trunc(text, timestamptz, text)` **is immutable** (verified via
`pg_proc.provolatile`). Workarounds that were verified to work: wrap
`convert_to` in a one-line `IMMUTABLE` SQL function; bucket with
`date_trunc(unit, ts, 'UTC') AT TIME ZONE 'UTC'` then `extract(epoch …)`.
`text::bytea` is **not** a safe substitute — verified `'\x41'::bytea` yields one
byte, i.e. it is an I/O conversion that interprets escapes.

**`jsonb` canonicalises the key for us — with one hole.** Verified:
`'{"b":1,"a":2,"a":3}'::jsonb::text` → `{"a": 3, "b": 1}`, and
`'{"b":1,"a":2}'::jsonb = '{"a":2,"b":1}'::jsonb` → `t`. Verified end to end
through a generated column: `{"b":2,"a":1}` and `{"a":1,"b":2}` produce the
identical hash. Because `05-polyglot-protocol.md` already chose `jsonb` for args,
**River's entire sjson key-sorting routine (`db_unique.go:96-109`) is unnecessary
for us.** The database normalises order and duplicate keys before the generated
column ever sees them.

The hole: **jsonb preserves numeric scale.** Same experiment, third row —
`{"a": 1.0, "b": 2}` hashed differently from `{"a": 1, "b": 2}`. So client-side
number formatting still leaks into the key: a Ruby enqueuer sending `1` and a JS
enqueuer sending `1.0` will not deduplicate against each other. `jsonb` solves
key order and duplicate keys, not value rendering. Cheap to fix in a
canonicalising generated column if it ever bites; not worth pre-empting.

**A state-predicated partial unique index works exactly as River describes.**
Built the River shape (stored `unique_key` + `bit(8)` mask + `IMMUTABLE`
bitmask function) and verified: duplicate plain INSERT → 23505; duplicate with
different jsonb key order → 23505; insert blocked while the holder is
`executing`; transition of the holder to a state outside the mask frees the key
immediately; period bucketing dedupes within a bucket and allows the next one.

**And it has the failure mode River has to work around.** Verified: moving a row
*into* the masked set raises
`ERROR: duplicate key value violates unique constraint` **on the UPDATE**. So a
retry, a reschedule, or an un-cancel can fail with 23505. That is why
`river_job.sql:539-618` exists. Any design with `state` in the predicate
inherits that CTE and its bug history.

**`ON CONFLICT` cannot infer a partial index without repeating the predicate.**
Verified: `ON CONFLICT (unique_key) DO NOTHING` →
*"there is no unique or exclusion constraint matching the ON CONFLICT
specification"*. The full `WHERE …` clause must be restated verbatim, and
`ON CONFLICT ON CONSTRAINT` is unavailable because partial indexes are not
constraints. **This matters directly for the polyglot pitch**: whatever goes in
the predicate is text every client in every language must copy correctly.

**`RETURNING OLD./NEW.` replaces River's `xmax != 0` hack.** Verified on a real
conflict and on a fresh key in the same statement shape:

| case | `OLD.id` | `NEW.id` | `xmax <> 0` |
|---|---|---|---|
| conflict | `01a0…abe2` | `01a0…abe2` | `t` |
| fresh insert | *null* | `01a0…f7c0` | `f` |

`OLD.id IS NOT NULL` is an exact, documented substitute for the undocumented
`xmax` trick, and it also hands back the winner's row. River cannot use it
because it supports PG12+. **Second clear place where PG18-only pays**, after
`RETURNING OLD./NEW.` in the claim query (`03-postgres18.md`).

**Concurrency, four enqueuers, same key** (verified):

| isolation | conflict action | winner | losers |
|---|---|---|---|
| READ COMMITTED | `DO NOTHING` | 1 row | 0 rows, **no error** |
| READ COMMITTED | `DO UPDATE` | 1 row | **the same id**, `was_duplicate = t` |
| READ COMMITTED | none (plain INSERT) | 1 row | `23505` each |
| REPEATABLE READ | `DO NOTHING` | 1 row | **`could not serialize access due to concurrent update` (40001)** |
| SERIALIZABLE | `DO NOTHING` | 1 row | **40001** |

The last two rows are a trap for us specifically. `05-polyglot-protocol.md`
sells "enqueue is transactional for free — the job and the business data commit
together". If the caller's business transaction is `REPEATABLE READ` or
`SERIALIZABLE`, a deduplicated enqueue **aborts their whole transaction** with
40001, even with `DO NOTHING`. That has to be documented, not discovered.

**Measured cost of putting `state` in the index predicate: every state
transition loses HOT.** Two identical tables, `fillfactor=50`, one with a
state-predicated partial unique index:

| | update `note` (unindexed) | update `state` (in predicate) |
|---|---|---|
| no unique index | 1 upd / 1 HOT | 2 upd / **2 HOT** |
| state-predicated index | 1 upd / 1 HOT | 2 upd / **1 HOT** |

Non-obvious consequence, also verified: the test row had `unique_key IS NULL`
and was **not in the index at all**, and still lost HOT. The decision is made
from the relation's index attribute bitmap, not per-row membership — so
**non-unique jobs pay the cost too**, on the hottest write path in the system.

**A partial index avoids indexing the NULLs; a full UNIQUE constraint does
not.** 200 000 rows, all with a NULL dedup key:

| | index size |
|---|---|
| `UNIQUE (dedup_key)` constraint | **1272 kB** |
| `UNIQUE (dedup_key) WHERE dedup_key IS NOT NULL` | **16 kB** |

So the ergonomic win of a real constraint (`ON CONFLICT ON CONSTRAINT`, no
predicate to repeat — verified working) costs a full-table-sized index that is
almost entirely NULLs. Not worth it.

**Release-by-regeneration works.** Verified: a `STORED` generated `dedup_key`
recomputes on UPDATE, so `UPDATE … SET dedup_scope = NULL` sets `dedup_key` to
NULL and frees the slot; re-inserting the same scope then succeeds. Writing the
generated column directly is correctly refused
(*"column can only be updated to DEFAULT"*), which means the daemon cannot
corrupt the key — it can only change the inputs. That is the property we want.

### Second pass — five more experiments

Run later, same 18.6 instance, schema dropped afterwards. Three of these bear
directly on §7 and one of them contradicts it.

**12. A `STORED` key recomputes when *any* source column changes — including
`scheduled_at`.** Key bucketed on `scheduled_at` with a 1-hour period, then the
row rescheduled `+2 hours`:

```
c2e640bff5e6e09733f91e2443391b855dcfc927c233140a43c3b415ba6a0d1f
  ->  33f5d87311edf1e281f9d3dc9e9eaf75529312811bde75752608505cb617ff5b
```

**A snooze, a retry backoff, or any reschedule silently vacates the throttle
slot.** §7 proposes exactly this — `date_trunc(unit, scheduled_at, 'UTC')` folded
into the generated expression — so §7 has this bug as written. River has the same
exposure, deliberately: it buckets on `ScheduledAt` if set (`CHANGELOG.md:563`,
PR #734). Oban does not: `timestamp:` defaults to `:inserted_at` (`job.ex:233`).

**Bucket on `inserted_at`.** It is a column nothing ever updates, so the key is
immutable for the row's whole life, which is the property the whole
release-by-regeneration design (experiment 11) depends on. The cost is a real
semantic difference — a job enqueued at 10:59 for execution at 15:00 buckets at
10:00, not 15:00 — and that is the *less* surprising of the two behaviours
anyway.

**13. `ON CONFLICT DO UPDATE` still cannot handle duplicates inside one
statement.**

```
INSERT ... VALUES (<same key>), (<same key>) ON CONFLICT ... DO UPDATE ...;
ERROR:  ON CONFLICT DO UPDATE command cannot affect row a second time
HINT:  Ensure that no rows proposed for insertion within the same command have
       duplicate constrained values.
```

`DO NOTHING` is immune — verified, it inserted 1 of the 2. So the recommended
"return the winner's id" clause in §7 works for single-row enqueue and breaks for
batch enqueue. Inference, not verified against River: `JobInsertFastMany` uses
`DO UPDATE` and its CTE has no `DISTINCT` (`river_job.sql:265-321`), so River
should be exposed to this too; the `NoReturning` variant uses `DO NOTHING`
(`river_job.sql:356-360`) and is not.

**14. A short predicate is achievable even with `state` in it** — relevant if the
§7 decision is ever revisited. A *second* `STORED` generated column
`dedupe_active boolean GENERATED ALWAYS AS (state_in_mask(dedupe_states, state))`
is accepted (user-defined `IMMUTABLE` functions are legal in generation
expressions, verified), reducing the index to `WHERE dedupe_active` and the
client's clause to `ON CONFLICT (unique_key) WHERE dedupe_active`. Verified end
to end including `RETURNING NEW.id, (OLD.id IS NULL)`. This neutralises §7's
"predicate every client must repeat" argument — but not the HOT argument, which
still decides it. Also verified: River's required-states rule is enforceable in
the schema rather than in client validation —
`CHECK (dedupe IS NULL OR (dedupe_states & B'11100000') = B'11100000')` correctly
rejects a non-closed mask.

**15. Partitioning and uniqueness are coupled. This is the sharpest constraint
in the file and it is not mentioned anywhere above.**

```
CREATE UNIQUE INDEX pjobs_dedupe_idx ON pjobs (dedup_key) WHERE dedup_active;
ERROR:  unique constraint on partitioned table must include all partitioning columns
DETAIL:  ... lacks column "queue" which is part of the partition key.
```

```
PARTITION BY LIST (is_final);
ERROR:  cannot use generated column in partition key
```

So: **every partition-key column must appear in the unique index, and the
partition key cannot itself be a generated column.** Consequences:

- Cross-queue and cross-task uniqueness (River's `ExcludeKind`) become impossible
  the moment queue or task is the partition key.
- The tempting "partition finished jobs away from live ones" scheme is dead as
  stated — `is_final` would have to be a plain column maintained by the daemon,
  not derived.
- This is exactly why pg-boss partitions by queue *and* leads every one of its six
  unique indexes with `name` (`plans.ts:780-781`, `:886-934`). That was not a
  style choice.

`03-postgres18.md` flagged partitioning as blocked on `pg_partman` being absent
from the image. It is now also blocked on the uniqueness-scope decision, and the
two have to be made together. `ON CONFLICT` itself works normally on a
partitioned table once the index is legal (verified).

**16. Two smaller facts, recorded so nobody re-derives them.**

- `UNIQUE ... NULLS NOT DISTINCT` works on 18.6 and would remove the need for
  pg-boss's `COALESCE(singleton_key,'')` (`plans.ts:886`). **We do not want it
  here** — it turns "no dedup key" from *exempt* to *one global slot*, i.e. every
  non-unique job would collide with every other. Noted because it looks like a
  tidy-up and is not.
- `sha256(<text>::bytea)` *is* accepted in a generated column (the I/O cast is
  immutable), which is a working alternative to wrapping `convert_to` in a custom
  `IMMUTABLE` function. It is still the wrong choice for the reason already given
  above — the cast interprets `\x` escapes, so any args text containing a
  backslash-x sequence is silently reinterpreted. Use the wrapper.
- Index footprint of a `sha256` key: 100k rows → 7848 kB (~80 B/row), table
  15 MB. Not a factor in any decision here.

## 7. Decision for Stampeded

**Hard constraint first.** `05-polyglot-protocol.md` says enqueue is a plain
INSERT from any Postgres driver. That **rules out**, flatly:

- **Oban's model.** Advisory lock + SELECT + conditional insert is four
  statements of Elixir. A Ruby enqueuer that skips them silently defeats
  uniqueness for everyone. Not a criticism of Oban — it is a criticism of the
  model under our constraint.
- **Any sliding-window period** (`ts >= now() - period`). It requires a query,
  which requires the lock, which requires the client. Verified above:
  non-immutable, so it cannot live in an index or a generated column.
- **good_job's count-based concurrency** and **solid_queue's semaphore table**
  for the same reason — both need a read-modify-write per enqueue. Note they
  buy something a constraint cannot express (N > 1), so keep them in mind for
  the *throttling* design (`throttle-machines`), not for uniqueness.
- **db-scheduler's SELECT-then-INSERT.** Same reason.

What survives is: **a database-derived key plus a unique index.** Within that,
the real choice is whether `state` appears in the index predicate.

**Recommendation: it must not.** Take Graphile's *shape* with River's
*key derivation*, and PG18's `RETURNING OLD.`:

- `dedup_scope text` — plain, nullable, client-supplied. NULL means "this job is
  not unique". Contains whatever the enqueuer wants to dedupe on, as text.
- `dedup_key bytea GENERATED ALWAYS AS (… sha256 of dedup_scope …) STORED` —
  must be STORED (virtual cannot be indexed, verified). The hash exists only to
  bound the index key length; `dedup_scope` itself stays readable in `psql` and
  in the MCP control plane, which is the same inspectability argument that chose
  `jsonb` over `bytea` for args.
- `UNIQUE INDEX (dedup_key) WHERE dedup_key IS NOT NULL` — partial, because the
  full constraint costs 1272 kB per 200k non-unique jobs (verified) and the
  predicate is short enough to copy. **Legal only on an unpartitioned table.**
  Experiment 15: if `stampeded_jobs` is ever partitioned, every partition-key
  column must appear in this index, which forcibly scopes uniqueness to the
  partition key. Either the jobs table stays unpartitioned and retention is
  handled by deletion, or uniqueness becomes per-queue and the index is
  `(queue, dedup_key)`. `03-postgres18.md` wants partitioning; this is the bill.
- **Release is an explicit write**: the daemon sets `dedup_scope = NULL` as part
  of the same UPDATE that performs the terminal transition. Zero extra
  statements.

Why this and not River's mask:

| | `state` in predicate (River) | `dedup_scope` nulled explicitly (recommended) |
|---|---|---|
| HOT on state transitions | **lost, for every job incl. non-unique ones** (verified) | preserved (verified) |
| transition can raise 23505 | **yes** (verified) — needs River's `JobSchedule` CTE, which has already shipped one bug (`CHANGELOG.md:646`) | **impossible**: `dedup_key` only ever goes non-NULL → NULL, so no row can ever *enter* the index |
| "which states count" | declarative, DB-enforced | daemon policy in `state-machines` |
| failure mode if daemon is buggy | under- or over-blocking | **over-blocking only** — a forgotten release holds the key; the rescuer already exists to clean it |
| predicate every client must repeat | `state_in_bitmask(unique_states, state)` | `dedup_key IS NOT NULL` |

The last two rows decide it. Over-blocking is the safe direction, and the
polyglot INSERT snippet has to stay short enough that people copy it correctly.
The cost is honest and should be written down: **the set of states in which a
job is unique is no longer visible in the schema.** It lives in Rust. That is
the same trade `04-job-lifecycle-and-claiming.md` already made for lifecycle
("lifecycle stays authoritative in Rust"), so at least it is consistent.

> **IMPLEMENTATION NOTE.** The bucket is implemented, and needed three
> immutability facts none of the analysis above had. `date_trunc(text,
> timestamptz)` is STABLE (it reads the session TimeZone); the three-argument
> `date_trunc(text, timestamptz, text)` with an explicit zone IS immutable; and
> casting a `timestamptz` to text is STABLE again, via DateStyle. So the
> truncated time cannot simply be concatenated — it is turned into a number by
> subtracting the epoch, which yields an interval, and `extract` on an INTERVAL
> is immutable. Postgres reports any of these as just "generation expression is
> not immutable", with no indication of which sub-expression is at fault.

**Time windows: bucket, and say so.** `dedup_scope` is text the client composes,
so the bucket is the client's to include — but that reintroduces clock skew
between enqueuers, which pg-boss deliberately avoids by computing the slot
server-side (`plans.ts:2209-2213`). Compromise: a nullable `dedup_period
interval` column folded into the generated expression via
`date_trunc(unit, inserted_at, 'UTC')` (verified immutable).

**Bucket on `inserted_at`, not `scheduled_at`** — experiment 12 showed a `STORED`
key bucketed on `scheduled_at` silently changes when a job is snoozed or
rescheduled, vacating its own throttle slot. `inserted_at` is never updated, so
the key is stable for the row's whole life, which is also what
release-by-regeneration assumes. River accepts this exposure by design
(`CHANGELOG.md:563`); Oban avoids it by defaulting to `:inserted_at`
(`job.ex:233`).

The boundary hole is real and must be documented: two jobs 60 seconds apart
across a bucket edge both run. Anyone who needs an exact sliding window needs
Oban, and should be told so rather than sold a bucket.

**Conflict semantics: offer both, default to returning the existing job.**
- `ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING` → 0 rows,
  no error. Fire-and-forget.
- `… DO UPDATE SET dedup_scope = jobs.dedup_scope RETURNING NEW.id,
  (OLD.id IS NOT NULL) AS was_duplicate` → every concurrent enqueuer gets the
  winner's id (verified). This is River's `unique_skipped_as_duplicate` without
  the `xmax` hack and without a no-op column write that means nothing.
  **Single-row only** — experiment 13: `DO UPDATE` raises *"cannot affect row a
  second time"* if one statement proposes the same key twice, so a batch enqueue
  must use `DO NOTHING` or de-duplicate before it is sent.
- Bare INSERT with no clause → 23505. Legitimate for callers who want a hard
  error, and it is what underway does. Document the SQLSTATE and put the scope
  in the error detail — do not make people regex it out like procrastinate has
  to (`sync_psycopg_connector.py:29-42`).

**Do not build the Graphile pre-clear.** Graphile nulls the key of an already
executing job so a new one can be enqueued (`sql/000011.sql:114-127`). That is
"replace a running job's future self", and it comes with a lost-enqueue race
(`000011.sql:129-130`). We already rejected Graphile's lease model in
`04-job-lifecycle-and-claiming.md`; rejecting this for the same reason
(correctness by hopeful timing) is consistent. Uniqueness against an executing
job should mean *blocked*, and the caller decides what to do about it.

**Worth stealing, not yet designed:** pg-boss's queue-level policy
(`plans.ts:57-64`) — expressing "this whole queue is a singleton" once at queue
creation instead of on every enqueue is strictly better ergonomics than a
per-job flag, and costs nothing for queues that do not opt in.

## 8. Still open

- `dedup_scope` composition: does the daemon offer a helper, or is "you compose
  the string" the whole API? A helper means client code, which is the thing we
  are avoiding. Leaning: document conventions (`task|arg|arg`), ship nothing.
- Whether `dedup_period` is worth a column at all, given the bucket hole. The
  alternative is procrastinate's separate ledger table
  (`schema.sql:85-87`), which three projects independently converged on and
  which would keep periodic dedup entirely out of the jobs table.
- Should the release be automatic via a trigger on terminal transitions?
  `03-postgres18.md` restricts triggers to "what the database alone can do
  atomically". A release is derivable from `state`, so by that rule it belongs
  in the daemon — but that means a language-native worker calling
  `stampeded_claim()` directly must also release, and will forget.
- ~~**Uniqueness scope vs partitioning — blocking, and it blocks `03-` too.**~~
  **RESOLVED, and it was resolved by the partitioning decision itself** — this
  entry simply outlived it. `plans/README` §1 chose `PARTITION BY LIST (queue)`
  precisely because uniqueness won the argument, and a unique index on a
  partitioned table must contain the partition key. So the scope was decided
  the moment the key was: `UNIQUE (queue, dedup_key) WHERE dedup_key IS NOT
  NULL` (`001_initial.sql:335`). That is the coherent pg-boss shape this entry
  asked for — both together, not neither.

  Verified against a real database on 2026-09-18, because the consequence is
  user-visible and worth stating rather than deriving:

  ```
  ('default', 'acct:42')   -> inserted
  ('default', 'acct:42')   -> rejected, duplicate key
  ('mail',    'acct:42')   -> inserted          <-- different queue, different job
  ```

  **Uniqueness is per queue, not global.** Someone deduplicating by account id
  who routes two jobs to two queues gets both. Now documented in the README
  under *Unique jobs* rather than left to be discovered. `dedup_period` was
  verified in the same run: same hour rejected, next hour accepted.
- Batch enqueue: does Stampeded offer one, and does it dedupe? `DO UPDATE` cannot
  (§6, experiment 13), `DO NOTHING` can but returns nothing. Oban punts this to
  Oban Pro (`oban.ex:819-820`); River made it the reason for the whole migration
  (`CHANGELOG.md:709`). If we want it, the multi-row INSERT must either use
  `DO NOTHING` or be de-duplicated before it is sent.
- The 40001 under REPEATABLE READ / SERIALIZABLE: document only, or have the
  HTTP enqueue endpoint retry on the caller's behalf?
- Coalescing (Graphile `sql/000020.sql` payload array-concat, bullmq `replace`).
  Both are "N enqueues become one job carrying the latest/all data". Neither
  fits first-write-wins. Defer, but do not design it out.
- Interaction with `throttle-machines`: good_job's ordered-set membership test
  (`concurrency.rb:167-171`) and solid_queue's guarded-decrement semaphore
  (`semaphore.rb:77-83`) both express N > 1, which no unique index can. That is
  a separate mechanism and a separate plan file.
