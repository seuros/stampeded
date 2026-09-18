# Retention, table bloat, and partitioning

> **SUPERSEDED IN PART 2026-09-17 — see `README.md`.** The LIST-by-state
> partitioning recommended here was **not** adopted: it is mutually exclusive
> with DB-enforced uniqueness (`10-unique-jobs.md`), and uniqueness won.
> Partitioning is by **queue**.
>
> Everything else in this file stands and matters MORE, not less: with no
> state-based partition, completed jobs share a partition with live ones, so the
> pruner shape, the autovacuum settings and the bloat detection here are now the
> only defence against unbounded growth. The cross-partition row-move cost is no
> longer on the critical path, since completion no longer moves a row.



The long-term failure mode. Claiming decides whether the queue is *correct*
(`04-`); this decides whether it is still alive in a year.

Source study of eleven Postgres-backed queues at `~/Projects/ecosystem/reference/postgres/`,
all permissively licensed. Plus eight facts executed against live PG 18.6
(`commonsharecore-db-1`), marked **VERIFIED 18.6**.

Everything below separates **verified** (read in source / executed) from
**inferred** (my reasoning, stated as such).

---

## 1. What happens to a completed job

Three distinct answers, and the choice cascades into everything else.

| | completed job fate | citation |
|---|---|---|
| **Graphile Worker** | **DELETE immediately** | `graphile-worker/sql/000011.sql:180-191` (`delete … returning *`); hot path is JS, not the fn: `graphile-worker/src/sql/completeJobs.ts:35,55,75` |
| **solid_queue** | **row moves between tables** — `DELETE` from `solid_queue_ready_executions`, `INSERT` into `solid_queue_claimed_executions`; job row keeps `finished_at` | `solid_queue/app/models/solid_queue/ready_execution.rb:37-44`, `claimed_execution.rb:27-39` |
| **neoq** | dead jobs **moved to a second table** `neoq_dead_jobs`, original DELETEd | `neoq/backends/postgres/postgres_backend.go:646-657` |
| **Oban** | kept as `state='completed'`, pruned later | `oban/lib/oban/engines/basic.ex:169-191` |
| **River** | kept as `state='completed'`, `finalized_at` set, cleaned later | `river/internal/maintenance/job_cleaner.go:182-236` |
| **pg-boss** | kept as `state='completed'`, deleted later on two independent clocks | `pg-boss/src/plans.ts:2827-2840` |
| **que** | `UPDATE … SET finished_at = now()` — kept forever, **no built-in pruner at all** | `que/lib/que/job_methods.rb:4-9`; deletion is a doc snippet, `que/docs/README.md:875-884` |
| **underway** | `UPDATE … state='succeeded'`, kept; TTL delete only if you start the routine | `underway/src/queue.rs:1492-1510`, `1926-1942` |
| **good_job** | kept (`preserve_job_records` default `true`), cleaned inline by workers | `good_job/lib/good_job.rb:229-273` |
| **procrastinate** | kept; optional `delete_job` flag on finish | `procrastinate/sql/schema.sql:264-280` |
| **db-scheduler** | one-shot tasks DELETEd on completion | `db-scheduler/.../jdbc/JdbcTaskRepository.java:510` |

**Nobody archives to a history table.** The closest are neoq's dead-letter
table and solid_queue's execution-table split — and neither is an archive, both
are working state. The industry answer is delete-or-keep, not archive.

### The three architectures, named

1. **Delete-on-success** (Graphile). Table stays tiny. You lose all history.
   Permafailed rows are the only residue, and they are kept **forever** by
   default — `DELETE_PERMAFAILED_JOBS` is opt-in and not even in the default
   cleanup set (`graphile-worker/src/cleanup.ts:4-8` vs `:30`).
2. **Keep-and-prune** (Oban, River, pg-boss, good_job, underway, que). History
   for free, unbounded growth by construction, correctness of the whole system
   now depends on a background process nobody watches.
3. **Table-per-state** (solid_queue). State is *which table the row is in*.
   `solid_queue_ready_executions` rows are only ever INSERTed and DELETEd —
   **never UPDATEd**. Zero in-place churn on the hot table. The cost is a
   multi-table transaction per claim and six tables instead of one.

Architecture 3 is the one nobody talks about and it is the most interesting
for us. See §7.

---

## 2. Pruning mechanics

| | interval | batch | loop? | long-txn safe? |
|---|---|---|---|---|
| **Oban** `Pruner` | 30 s | 10,000 | **no** — one pass per tick | one txn, leader-elected; `Repo.transaction` wraps the whole delete |
| **River** `JobCleaner` | 30 s | 10,000 (→1,000 degraded) | **yes**, until `n < batch` | 30 s per-query timeout, 50 ms–1 s jittered sleep between batches |
| **pg-boss** `deletion()` | 24 h | **unbounded** | n/a | advisory-locked, no LIMIT |
| **good_job** | every 1,000 jobs *or* 600 s, inline in the worker | 1,000 | yes, until empty | no wrapping txn; two `delete_all` per batch |
| **solid_queue** | hourly recurring task, `:12` past | 500 | yes, until 0 | optional `sleep_between_batches` |
| **underway** | 1 h | **unbounded** | n/a | no LIMIT, FK cascade into `task_attempt` |
| **graphile** | **manual CLI, one-shot, then exits** | **unbounded** | n/a | three unbounded DELETEs on one connection |
| **procrastinate** | scheduled task | **unbounded** | n/a | DELETE … WHERE id IN (`DISTINCT ON` over a job⋈events join) |
| **que** | — | — | — | none shipped |

Citations: `oban/lib/oban/pruner.ex:70-76` (defaults `interval: 30s, limit: 10_000,
max_age: 60`), `:162-175` (leader gate + txn); `oban/lib/oban/engines/basic.ex:169-191`
(join-to-limited-subquery `DELETE`). River:
`river/rivershared/riversharedmaintenance/river_shared_maintenance.go:30,35-40,58,60`
(`Completed/Cancelled = 24h`, `Discarded = 7*24h`, `Interval = 30s`,
`Timeout = 30s`, `BatchSizeDefault = 10_000`, `BatchSizeReduced = 1_000`) and
`:17-18` (`BatchBackoffMin = 50ms`, `BatchBackoffMax = 1s`);
`river/internal/maintenance/job_cleaner.go:184-236` (the loop, `break if
numDeleted < batchSize`, and the **circuit breaker that halves the batch on
`DeadlineExceeded`** at `:214-217`). pg-boss:
`pg-boss/src/plans.ts:2827-2840`, `src/attorney.ts:758` (`maintenanceIntervalSeconds
|| 24h`). good_job: `good_job/lib/good_job.rb:229` (`in_batches_of: 1_000`),
`good_job/README.md:315-317` (`cleanup_preserved_jobs_before_seconds_ago = 1209600`
i.e. **14 days**, `cleanup_interval_jobs = 1000`, `cleanup_interval_seconds = 600`),
`good_job/lib/good_job/job_performer.rb:89` (**cleanup runs inside the worker
after N jobs — no separate process**). solid_queue:
`solid_queue/lib/solid_queue.rb:42-43` (`preserve_finished_jobs = true`,
`clear_finished_jobs_after = 1.day`),
`solid_queue/app/models/solid_queue/job/clearable.rb:13-19`. underway:
`underway/src/queue.rs:1922-1942`. graphile: `graphile-worker/src/cleanup.ts:36-64`,
`graphile-worker/src/cli.ts:152`. procrastinate:
`procrastinate/sql/queries.sql:55-69`, `procrastinate/builtin_tasks.py:9-45`.

### Defaults worth stealing, verbatim

| constant | Oban | River | pg-boss | good_job | solid_queue | underway |
|---|---|---|---|---|---|---|
| completed retention | **60 s** | **24 h** | **7 d** (`deletion_seconds`) | **14 d** | **1 d** | **14 d** (`ttl`) |
| discarded/failed retention | 60 s | **7 d** | 14 d (`keep_until`) | 14 d | kept | 14 d |
| sweep interval | 30 s | 30 s | 24 h | 600 s / 1000 jobs | 1 h | 1 h |
| batch | 10,000 | 10,000 | ∞ | 1,000 | 500 | ∞ |

`pg-boss/src/plans.ts:105-114` — note pg-boss carries **two** clocks:
`retention_seconds` (14 d, → `keep_until`, applies to *unstarted* jobs) and
`deletion_seconds` (7 d, → from `completed_on`). Two different questions,
correctly given two different knobs: "how long may a job sit unclaimed" vs
"how long do I keep the corpse". Everyone else conflates them.

**Oban's 60-second default is the outlier and it is the right instinct.** The
docs say so directly — `oban/guides/advanced/scaling.md:151-153`: *"prune
*aggressively* … Aim to retain as few jobs as necessary for uniqueness and
historic introspection."*

### Who actually avoids the long DELETE

Only **River**. It is the only implementation with all four of: a `LIMIT`, a
loop-until-drained, a per-statement timeout, *and* adaptive batch reduction
under load (`job_cleaner.go:171-176`, `:214-217`). Everyone else is either
unbounded or single-shot. Copy River's shape.

Oban's `prune_jobs` is subtler than it looks: the `LIMIT` lives in a subquery
that is `join`ed, not a bare `DELETE … LIMIT` (which Postgres does not have) —
`basic.ex:174-188`. Same trick as the claim fence.

---

## 3. Partitioning

**Exactly one of eleven partitions the jobs table: pg-boss.** Verified negative
for Oban, River, Graphile Worker, underway, que, solid_queue, good_job,
procrastinate, neoq, db-scheduler (grep for `partition` across each tree
returns nothing in schema/migrations).

### pg-boss: `PARTITION BY LIST (name)` — by queue

`pg-boss/src/plans.ts:542` — `PARTITION BY LIST (name)`, where `name` is the
queue name.

- Default install attaches a single catch-all `job_common` as `DEFAULT`
  partition (`plans.ts:616-633`). Every queue shares it.
- A queue created with `partition: true` gets a **dedicated physical table**,
  named `'j' || encode(sha224(queue_name::bytea),'hex')`
  (`plans.ts:712-714`), created inside the `create_queue()` PL/pgSQL function
  (`plans.ts:767-793`).
- **Dropping a queue drops the partition**: `DROP TABLE IF EXISTS …`
  (`plans.ts:809-811`) rather than a DELETE. Instant reclamation.
- **The ATTACH trick worth stealing** (`plans.ts:792-793`):
  ```
  ADD CONSTRAINT cjc CHECK (name = <queue>)   -- then
  ATTACH PARTITION … FOR VALUES IN (<queue>)
  ```
  The proving CHECK is added *before* ATTACH so Postgres can skip the full
  validation scan.
- Index set is chosen **per partition by queue policy** (`plans.ts:775-790`) —
  a `short`-policy queue does not carry the `key_strict_fifo` indexes. A single
  shared table must carry all of them. This is the underrated benefit of
  partition-by-queue and it is a *write-amplification* argument, not a read one.

Rationale in their own words, `pg-boss/docs/introduction.md:8-10`: shared table
by default, dedicated table when *"1 or more of your queues grows significantly
or experiences an unexpected backlog"*; *"Postgres should scale to thousands of
queues in a partitioning hierarchy quite well."*

### pg-boss also range-partitions its stats table — and this is the recipe

`queue_stats` is `PARTITION BY RANGE (captured_on)` with **daily** partitions
(`plans.ts:1406-1419`). Creation and dropping are hand-rolled PL/pgSQL `DO`
blocks, **no `pg_partman`**:

- `ensureQueueStatsPartitions` (`plans.ts:1439-1467`) — creates today's and
  tomorrow's, idempotently, `pg_class`-guarded, bounds emitted as explicit
  `+00` timestamptz literals *because a bare date literal is cast in the
  session TZ and rows near UTC midnight would fall outside every partition*
  (`plans.ts:1432-1437`). That is a genuine trap, verified-by-comment.
- `dropOldQueueStatsPartitions` (`plans.ts:1468-1494`) — walks `pg_inherits`,
  parses the `YYYYMMDD` suffix, `DROP TABLE IF EXISTS` below the cutoff.
- Driven from the supervise pass (`pg-boss/src/boss.ts:297-299`), deliberately
  in the **public** `supervise()` path so manual callers also get partitions
  provisioned.

~55 lines of PL/pgSQL replaces the extension. This is the existence proof that
manual partition management is tractable.

### Why the other ten don't partition — inferred

Partition pruning only works on the partition key. A queue's hot query filters
on `state` and `queue` and orders by `priority, scheduled_at` — none of which
is the dimension you want to *drop*. You want to drop by **time**, and time is
not in the claim predicate. So time-range partitioning buys retention and pays
for it on every single claim. Measured below (§7); it is not a small price.

The one dimension that is both in the claim predicate *and* a natural
drop-unit is the **queue** — which is exactly what pg-boss chose.

---

## 4. VACUUM / autovacuum pressure

Four of eleven say anything at all. The distribution is stark.

### Oban — the only concrete, complete recommendation anywhere

`oban/guides/advanced/scaling.md:48-70`, verbatim:

| setting | value |
|---|---|
| `autovacuum_vacuum_scale_factor` | **0.02** |
| `autovacuum_vacuum_threshold` | **50** |
| `autovacuum_analyze_scale_factor` | **0.02** |
| `autovacuum_analyze_threshold` | **100** |
| `autovacuum_vacuum_insert_scale_factor` | **0.02** |
| `autovacuum_vacuum_insert_threshold` | **1000** |
| `autovacuum_vacuum_cost_limit` | **2000** |
| `autovacuum_vacuum_cost_delay` | **1** |
| `fillfactor` | **85** |

Applied per-table (`ALTER TABLE oban_jobs SET (…)`), not globally — the guide
says so explicitly at `:50-51`. Stated reasoning at `:43-47`: *"Default
autovacuum settings are sized for general purpose tables and may fall behind on
`oban_jobs`, where state transitions update a row and pruning deletes in bulk."*
Verification advice at `:73-74`: watch `n_dead_tup` and `last_autovacuum` in
`pg_stat_user_tables`.

Defaults these replace: scale_factor 0.2 (10×), cost_limit 200 (10×),
cost_delay 2 ms (2×), fillfactor 100.

### que — `fillfactor = 90`, set in the migration, no guidance

`que/lib/que/migrations/4/up.sql:1` — `ALTER TABLE que_jobs SET (fillfactor = 90);`
and `:102` for `que_values`. Reverted on down (`4/down.sql:1`). **No comment
explaining it** — it is the only fillfactor anyone ships in DDL rather than
documents.

que's prose is the bluntest statement of the failure mode, `que/README.md:30`:
*"Que's job table undergoes a lot of churn … susceptible to bloat and slowness
if Postgres isn't able to clean it up. The most common cause of this is
long-running transactions."* And `que/docs/README.md:821-836` recommends a
**recurring `VACUUM VERBOSE ANALYZE que_jobs` job at `INTERVAL = 300`
(5 minutes), at the highest job priority**, with the reasoning that manual
vacuum does not back off and sleep the way autovacuum does. That is an
unusually aggressive recommendation and it comes from production scars.

### pg-boss — does not *recommend* settings, it *detects the failure*

pg-boss 12 is the state of the art here and it is worth reading in full.

**`#checkVacuum`** (`pg-boss/src/boss.ts:508-620`) reads `pg_stat_user_tables`
and emits two distinct diagnoses because the fixes are opposite:

- `xmin_horizon` — a vacuum ran and dead tuples did not fall → something pins
  the MVCC horizon.
- `autovacuum_disabled` — no vacuum ran and the pile grew.

It requires **two consecutive observations** before warning (`boss.ts:539-542`)
and honours per-table storage parameters when computing the budget
(`autovacuum_vacuum_threshold + scale_factor * live`, `boss.ts:530`).

The measured justification for not using a transaction-age proxy
(`boss.ts:522-527`): *"the transaction count that corresponds to a given amount
of unreclaimable garbage moves by ~9x with producer batching alone … **the same
40,000 jobs left the same 120,000 unreclaimable rows** across both producer
shapes."* Dead tuples are the stable signal; `age()` is cluster-global and
useless.

**The monitoring query is itself the bloat cause.** `pg-boss/src/plans.ts:968-975`:
*"a measured **200,000 deleted rows stayed 'dead but not yet removable' for
exactly as long as one such snapshot was open**, and were removed by the very
next vacuum after it closed. Run it densely enough and the job table can never
be reclaimed — the failure PlanetScale documents as a queue's death spiral."*

Their fix is a **vacuum-safety backoff** (`plans.ts:1020-1088`):
- `MONITOR_PIN_BUDGET_RATIO = 0.1` — the stats aggregate may pin the horizon
  for at most 10% of `autovacuum_naptime` (6 s at the default 60 s).
- Backoff length = `LEAST(GREATEST(2 * naptime, LEAST(elapsed, 600)), 3600)`.
- `MONITOR_BACKOFF_MAX_SECONDS = 600`, `MONITOR_BACKOFF_CAP_SECONDS = 3600`.
- `naptime` is read from `current_setting('autovacuum_naptime')`, not assumed.
- Reasoning at `plans.ts:1035-1053`: *"OldestXmin is computed once, when the
  worker starts on the relation, and never revisited. So the requirement is not
  that a vacuum finish between two aggregates — it is that the instant a worker
  computes its cutoff falls in a window with no aggregate running."*
- Measured aggregate cost: **0.19 s at 1M rows; 2.1–3.6 s at 10M rows / 3 GB**
  (`plans.ts:1021-1025`).
- Written with `GREATEST` so the deadline only moves outward across instances
  (`plans.ts:1055-1061`).
- Critically: the backoff suppresses **only the aggregate**, never the
  timeout/heartbeat expiry in the same pass, *"the vacuum-safety valve silently
  becoming a job-expiry outage"* (`plans.ts:985-991`).

### Everyone else

- **graphile-worker** — the entire vacuum strategy of a queue that advertises
  180k jobs/s, `graphile-worker/website/docs/scaling.md:72-77`: *"The jobs
  table has extremely high churn; find a quiet period and give it a nice
  `VACUUM` from time to time. **TODO: which `VACUUM` options should we
  recommend?**"*
- **River, underway, solid_queue, good_job, procrastinate, neoq, db-scheduler**
  — zero hits for `autovacuum`/`fillfactor` across their trees. No storage
  parameters on any table in any migration.

**Only Oban ships numbers. Only que ships a fillfactor. Only pg-boss ships
detection.** Nobody ships all three.

---

## 5. Index bloat and HOT updates

### Does claiming break HOT? Yes, in every single implementation.

A HOT update requires that **no indexed column changes**. Claiming, by
definition, changes `state` (or `locked_at`), and every implementation indexes
that column — usually as a partial-index predicate, which is worse than a key
column because the row *leaves one index and enters another*.

| | claim writes | indexed? | HOT-eligible |
|---|---|---|---|
| Oban | `state`, `attempt`, `attempted_at`, `attempted_by` | `state` leads `oban_jobs (state, queue, priority, scheduled_at, id)` | **no** |
| River | `state`, `attempt`, `attempted_at`, `attempted_by` | `state` leads `river_job_prioritized_fetching_index`; also in `river_job_state_and_finalized_at_index` | **no** |
| Graphile | `attempts`, `locked_by`, `locked_at` | both partial indexes are `WHERE is_available` and `is_available` is a **STORED generated column over `locked_at` and `attempts`** | **no** |
| pg-boss | `state`, `started_on` | `state` is in the predicate of `job_i1..i4, i6, i8, i9, i10, i11` | **no** |
| underway | `state`, `run_at`, `lease_expires_at`, `attempt_count` | three indexes touched at once | **no** |
| que | `finished_at` / `error_count` | `que_poll_idx … WHERE finished_at IS NULL AND expired_at IS NULL` | **no** |
| **solid_queue** | **nothing — the row is DELETEd from one table and INSERTed into another** | n/a | **question does not arise** |

Index citations: `oban/lib/oban/migrations/postgres/v09.ex:14`, `v13.ex:7-8`,
`v10.ex:39-40` (GIN on `args`, `meta`);
`river/riverdriver/riverpgxv5/migration/main/002_initial_schema.up.sql:52-60`,
`005_migration_unique_client.up.sql:45`, `006_bulk_unique.up.sql:30`;
`graphile-worker/sql/000011.sql:67` (the generated column), `:71-81` (both
partial indexes); `pg-boss/src/plans.ts:885-983`;
`underway/migrations/20260213190000_9.sql:40-46`;
`que/lib/que/migrations/4/up.sql:45-47`.

**So `fillfactor` on the job table does not buy HOT updates for claiming.** It
buys new-tuple locality (fewer page splits, the updated tuple stays on the same
page for the pruning path). Oban's comment says exactly that —
`scaling.md:68`: *"Leave headroom on pages for locality and fewer page splits."*
Not "preserve HOT". Nobody claims to have achieved HOT on the claim path.

The one place fillfactor *would* buy real HOT updates is a **heartbeat**
column, if it is unindexed. underway's heartbeat is the counterexample: it
writes `lease_expires_at`, which **is** an index key
(`underway/src/queue.rs:1826-1841`, index at
`underway/migrations/20260213190000_9.sql:44-46`), on a **30-second** timer per
in-flight task (`underway/migrations/20241110164319_3.sql:5`). Every heartbeat
of every long-running task is a non-HOT row rewrite carrying the full `input
jsonb`. That is a designed-in leak and we must not copy it.

### The evidence that index bloat is the real problem, not heap bloat

pg-boss issue #876, written up in `pg-boss/src/plans.ts:3650-3682`. This is the
best data in the entire survey:

> *"Autovacuum reclaims heap space but never returns btree pages to the OS, so
> a job index sizes itself to the largest backlog its queue has ever held and
> stays there. A **drained 1M-job peak leaves `job_common_i11` at 38 MB and
> `job_common_pkey` at 51 MB holding 1k live rows** — and every subsequent
> vacuum with dead tuples to clean does a full physical scan of every one of
> those pages (**measured: 17,969 buffer hits + 3,892 reads vs 750 hits / 0
> reads after a rebuild**), which is what drains IO burst credits on managed
> Postgres."*

Their detection heuristic, `REINDEX_DEFAULTS` at `plans.ts:3662-3682`:

| knob | value | why |
|---|---|---|
| `minPages` | **128** (1 MB) | below this the waste is irrelevant and `reltuples/relpages` is noisy |
| `maxEntriesPerPage` | **5** | a freshly rebuilt job index packs **140–170 entries/page**; a bloated one holds **~0.2** |
| `minSizeRatio` | **4** | size vs. what the live entries need, from `pg_stats.avg_width`; guards wide-key indexes |
| `maxIndexBytes` | **2 GiB** | above this the rebuild itself is the incident; leave it to a human |

Their two calibration fixtures (`plans.ts:3672-3679`):
- reported bloat, drained 1M peak: **0 live entries, 4,861 pages → 4,861×**
- healthy wide-key index: **150k × 1,291-byte keys, 37,502 pages → 1.6×**

Three orders of magnitude apart. They deliberately avoid `pgstattuple` because
*"it is an extension that may not be installed"* (`plans.ts:3735-3737`).
**It is installed on our image** — see §7.

### Who reindexes, and what

| | targets | schedule | concurrency |
|---|---|---|---|
| **Oban** `Reindexer` | only `oban_jobs_args_index`, `oban_jobs_meta_index` (the two GINs) | `@midnight` | `REINDEX CONCURRENTLY` |
| **River** `Reindexer` | **all seven** incl. `river_job_pkey` | midnight UTC daily, 1 min timeout | `REINDEX CONCURRENTLY` |
| **pg-boss** | measured-bloated leaf indexes only | 24 h claim via `version.reindex_on` | `REINDEX CONCURRENTLY` |

`oban/lib/oban/reindexer.ex:59-65`; `river/client.go:63-71`,
`river/internal/maintenance/reindexer.go:27,290-297`;
`pg-boss/src/plans.ts:272-291`, `pg-boss/src/boss.ts:341`.

Oban's reasoning, `oban/guides/advanced/scaling.md:101-104`: *"The Postgres
transactional model applies to indexes as well as tables. That leaves bloat
from inserting, updating, and deleting jobs that auto-vacuuming won't always
fix."*

River's `reindexOne` has the operational detail everyone else misses
(`reindexer.go:222-247`): an interrupted `REINDEX CONCURRENTLY` leaves
`_ccnew` / `_ccold` catalog artifacts, and River **detects them and skips**
rather than retrying, on the grounds that a retry is slated for the same fate.
It only cleans them up on graceful shutdown, with a separate 15 s budget
(`reindexer.go:265-285`). pg-boss independently hit the same problem —
`pg-boss/src/plans.ts:3398-3410`, *"CREATE INDEX CONCURRENTLY leaves an INVALID
index in the catalog."*

Three independent implementations converged on "reindex nightly, concurrently,
and handle the artifacts". That is a strong signal.

### The uuidv7 bonus nobody else can have

`pg-boss/src/plans.ts:916-923` rejected `id` as a key column in its fetch index:

> *"`id` is a random uuid, so it **defeats btree deduplication** and dominates
> size: **24 MB against 19 MB** for this shape at **500k rows**, on the hot
> insert path and on every vacuum's bulkdelete. It buys nothing, because
> `fetchNextJob` dropped `id` from its ORDER BY for the same reason (a random
> uuid was never creation order)."*

pg-boss uses `gen_random_uuid()` (v4). **We use `uuidv7()`, which is
time-ordered**, so both halves of that objection invert: the index is
append-mostly (no random-insert page splits), and `id` *is* creation order, so
it can serve as the ordering tiebreaker for free. This is a concrete,
quantified benefit of the PG18 bet that was not previously written down.

---

## 6. Real-world scaling reports

Concrete numbers only, with sources. **Read the caveats; they matter more than
the headlines.**

| claim | source |
|---|---|
| **~183,000 jobs/s executed**; 4.16 ms avg add→start latency; ~202,000 jobs/s queued from one batched `add_jobs` | `graphile-worker/website/docs/performance.md:6-13` |
| *"over 180,000 trivial jobs per second … around fifteen billion jobs per day"* | `graphile-worker/website/docs/scaling.md:9-10` |
| **15,606 jobs/s with the shipped defaults** (`localQueue.size = -1`, batch delays `-1`) vs 183,895 with batching on — **11.8×** | `graphile-worker/website/docs/performance.md:152-236`; defaults at `graphile-worker/src/main.ts:586-588` |
| Graphile's own advice: *"generally start thinking about [another queue] when you're getting to **5k+ jobs per second**"* — **36× below the headline** | `graphile-worker/website/docs/scaling.md:81-83` |
| **183,000/s on an empty table → 843/s with 150k rows present** (100k stuck across two named queues + 50k ready). Alternative strategies measured at 40/s and 600/s. | `graphile-worker/src/sql/getJobs.ts:128-137`; benchmark resets the DB every run, `graphile-worker/perfTest/run.js:45-49` |
| *"Significantly improved 'large jobs table' performance … **Around 20x improvement** in this 'worst case'"* | `graphile-worker/CHANGELOG.md:496-499` |
| pg-boss: use a DB queue **under ~10,000 jobs/s**; consider Kafka/Redis **above ~50,000/s** | `pg-boss/docs/database-backends.md:550-560` |
| pg-boss's cited reference band for Postgres queues generally: **7–30k jobs/s** | `pg-boss/docs/database-backends.md:563` |
| pg-boss stats aggregate: **0.19 s at 1M rows, 2.1–3.6 s at 10M rows / 3 GB** | `pg-boss/src/plans.ts:1021-1025` |
| pg-boss: **40,000 jobs → 120,000 unreclaimable rows** under a pinned horizon | `pg-boss/src/boss.ts:526-527` |
| pg-boss: drained 1M-job peak → 38 MB + 51 MB of index for 1k live rows; 17,969 buffer hits vs 750 after rebuild | `pg-boss/src/plans.ts:3652-3658` |
| pg-boss fetch index: `start_after` as trailing key column, **0.04 ms → 18 ms at 50k deferred** if omitted | `pg-boss/src/plans.ts:906-911` |
| pg-boss `job_i10` `INCLUDE (start_after)`: **47 ms → 111 ms, ~400k extra buffer hits** at 200k queued / 2,000 keys if dropped | `pg-boss/src/plans.ts:938-945` |
| que on dead tuples: *"acquiring a job to work will start taking longer and longer … the jobs are being added faster than they can be worked"* | `que/docs/README.md:821-823`, citing brandur.org/postgres-queues |

**The single most important number in this table is 183,000 → 843.** Every
headline throughput figure in this ecosystem is measured on a freshly-created,
zero-bloat, near-empty table. The realistic steady state is two to three orders
of magnitude below it, and the variable that moves it is *how many rows are in
the table*. That is the entire thesis of this document.

---

## 7. What Stampede should do

### PG18 facts, executed on 18.6 today

**VERIFIED 18.6** (`commonsharecore-db-1`, Debian 18.6-1.pgdg12+2):

1. **`pg_partman` is absent. So is `pg_repack`. So is `pg_squeeze`.**
   `pg_cron` 1.6 and **`pgstattuple` 1.5** are available (neither installed).
   Confirms and extends `03-postgres18.md`.
2. **Virtual generated columns cannot be indexed.**
   `ERROR: indexes on virtual generated columns are not supported`.
   → `03-postgres18.md:39-49` needs a correction: `GENERATED … VIRTUAL` is
   *not* a free replacement for Graphile's stored `is_available`, because
   Graphile's whole point is that it is the **index predicate**. Virtual is
   fine for dashboard-facing derived fields; it cannot carry the claim path.
3. **`uuid_extract_timestamp(uuidv7())` works; `uuidv7(interval)` computes a
   bound UUID from a time offset.** Both are needed to derive range-partition
   boundaries from dates.
4. **RANGE partitioning on a `uuidv7()` column works** and rows land in the
   correct time bucket.
5. **A unique constraint on a partitioned table must include every partition
   column.** `UNIQUE constraint on table "j" lacks column "id" which is part of
   the partition key.` → range-by-`id` makes any global unique key (idempotency
   key, unique-job key) impossible.
6. **Storage parameters cannot be set on a partitioned parent.** `cannot
   specify storage parameters for a partitioned table / Specify storage
   parameters for its leaf partitions instead.` They work per-leaf.
7. **`CREATE TABLE … (LIKE t INCLUDING ALL)` does NOT copy reloptions.** So
   every partition-creation path must re-apply `fillfactor` and the autovacuum
   settings explicitly. (pg-boss's `create_queue` uses `LIKE job INCLUDING
   DEFAULTS` with no storage params — **every pg-boss per-queue partition runs
   stock autovacuum**, `pg-boss/src/plans.ts:767`.)
8. **`ALTER TABLE … DETACH PARTITION … CONCURRENTLY` works.**

### The measurement that decides it

Built a 13-partition table range-partitioned on `uuidv7()` (weekly buckets +
head), one partial index per partition, 5,000 available rows, `ANALYZE`d, then
ran the claim query. **VERIFIED 18.6**:

```
Limit → LockRows → Merge Append
          Sort Key: prio DESC, run_at
          → Index Scan on jp_0 … jp_11, jp_head   (13 scans)
```

**Thirteen index scans for one claim.** No pruning is possible — `state` and
`queue` are not the partition key, so every retained week must be opened on
every claim. Claim latency grows linearly with retention. Twelve of those
thirteen partitions contain nothing but completed jobs.

That kills time-range partitioning of the live job table. Not "adds overhead" —
it makes the thing we optimise hardest scale with the thing we are trying to
retain.

Then the counter-design. `PARTITION BY LIST (state)`, live states in one
partition, terminal states in another. **VERIFIED 18.6**:

```
Limit → LockRows → Index Scan using js_live_q_prio_id_idx on js_live
```

**One scan. Pruned.** And:

```
UPDATE js SET state='completed' WHERE id=… RETURNING OLD.state, NEW.state, tableoid;
  was       |    now    |   tableoid
 available  | completed | spike.js_done
```

Cross-partition row movement works, `RETURNING OLD./NEW.` survives it, and the
completed row **physically leaves the hot partition at the moment of
completion**. The hot table never accumulates a tail. The pruner's job shrinks
from "keep the queue alive" to "keep disk usage bounded" — an ordinary
housekeeping task instead of a load-bearing one.

The known hazard, also **VERIFIED 18.6**: a concurrent update racing a
cross-partition move raises

```
ERROR: 40001: tuple to be locked was already moved to another partition
       due to concurrent update
```

SQLSTATE **40001** — a serialization failure, i.e. retryable, i.e. exactly what
`chrono_machines` exists for. `FOR UPDATE SKIP LOCKED` narrows the window
(the mover holds the lock, so the claimer skips) but does not close it.

### Evaluating the three options honestly

| | pros | cons | verdict |
|---|---|---|---|
| **Add `pg_partman` to the image** | battle-tested; retention, pre-creation, and retention-by-drop for free | a second image to maintain forever; every operator must install it; a *polyglot* queue whose selling point is "just Postgres" now needs an extension; and it solves the wrong problem — see the Merge Append measurement | **no** |
| **Manual partitions, by time** | no extension; pg-boss proves ~55 lines of PL/pgSQL suffices (`plans.ts:1439-1494`) | the Merge Append result; no global unique constraint (fact 5); reloptions per leaf (facts 6, 7); partition DDL inside a queue-creation transaction is a documented footgun (`pg-boss/docs/database-backends.md:268-292`) | **no** |
| **Manual partitions, by state (LIST)** | measured: claim prunes to one partition; completion self-evicts from the hot set; a fixed, small, known partition count — no lifecycle DDL at all | cross-partition row movement is DELETE+INSERT (never HOT, index churn in two partitions); 40001 races; unique constraints must include `state`, which breaks unique-across-states job keys | **yes, for the hot/cold split** |
| **No partitioning** | simplest; ten of eleven references do this and ship | the completed tail sits in the same heap and the same indexes as the hot set, forever, and retention becomes load-bearing | **fallback** |

### Recommendation

**Two partitions by state, plus River's cleaner, plus Oban's autovacuum
settings, plus pg-boss's detection. No `pg_partman`, no time partitioning, no
per-queue partitioning at v1.**

1. **`stampede_jobs PARTITION BY LIST (state)`, exactly two leaves.**
   - `stampede_jobs_live` — `available, scheduled, executing, retryable`
   - `stampede_jobs_done` — `completed, discarded, cancelled`
   - Fixed at install. No partition is ever created or dropped at runtime, so
     none of pg-boss's DDL-in-transaction hazards apply.
   - Every claim-path index lives only on `_live`. `_done` carries a pkey and
     one `(finished_at)` index for the pruner and the dashboard, and nothing
     else. The hot index set never sees a completed row, which is precisely the
     bloat pg-boss issue #876 documents.
   - Uniqueness constraints must include `state`. Design unique job keys as
     unique-among-live (a partial unique index on `_live` alone), which is what
     pg-boss already does anyway (`job_i1..i3, i6, i8` are all
     `WHERE state <= …`).
   - Completion is a row move. Wrap it in `chrono_machines` and retry on
     SQLSTATE 40001. Write the SQLSTATE into the schema comment.

2. **Storage parameters on both leaves explicitly** (fact 6, fact 7). Start
   from Oban's numbers (`scaling.md:48-70`), diverged by role — *inferred, not
   verified; these are a starting point to be measured, not a result*:

   | | `_live` | `_done` |
   |---|---|---|
   | `fillfactor` | 85 | 100 (insert-then-delete only) |
   | `autovacuum_vacuum_scale_factor` | 0.01 | 0.05 |
   | `autovacuum_vacuum_threshold` | 50 | 1000 |
   | `autovacuum_analyze_scale_factor` | 0.02 | 0.1 |
   | `autovacuum_vacuum_cost_limit` | 2000 | 1000 |
   | `autovacuum_vacuum_cost_delay` | 1 | 2 |

   Lower than Oban's 0.02 on `_live` because the live partition is *small* —
   scale_factor on a small table triggers rarely in absolute terms, which is
   the case the `threshold` is for.

3. **Retention: River's loop shape, Oban's aggression.**
   - Default retention **1 hour** for `completed` (between Oban's 60 s and
     River's 24 h; 60 s is too short to debug with, 24 h is a lot of rows),
     **7 days** for `discarded`/`cancelled` (River's number,
     `river_shared_maintenance.go:37`).
   - Two knobs, not one, following pg-boss (`plans.ts:106-108`): a *retention*
     clock from completion and an *expiry* clock for never-claimed jobs.
   - Batch **10,000**, loop until `n < batch`, jittered 50 ms–1 s sleep between
     batches, per-statement timeout, halve the batch on timeout
     (`river/internal/maintenance/job_cleaner.go:184-236`, `:214-217`).
     This is the only correct pruner in the survey; copy it wholesale.
   - Leader-elected off the `stampede_workers` heartbeat table that `04-`
     already requires. No second coordination mechanism.

4. **Reindex nightly, concurrently, measured — not scheduled blindly.** Take
   pg-boss's density heuristic (`plans.ts:3662-3682`: `minPages 128`,
   `maxEntriesPerPage 5`, `minSizeRatio 4`, `maxIndexBytes 2 GiB`) rather than
   River's rebuild-everything-at-midnight. Handle `_ccnew`/`_ccold` artifacts
   by skipping, not retrying (`river/internal/maintenance/reindexer.go:222-247`).
   Re-evaluate `maxEntriesPerPage` against **uuidv7** — pg-boss calibrated
   140–170 entries/page against v4 UUIDs and explicitly notes that random UUIDs
   defeat btree deduplication (`plans.ts:916-923`); time-ordered keys
   deduplicate better, so healthy density should be *higher* and the gate
   should hold with more margin. **Inferred — measure before trusting it.**

5. **Ship `#checkVacuum` from day one.** pg-boss's two-observation,
   two-diagnosis detector (`pg-boss/src/boss.ts:508-620`) is the single most
   valuable operational feature in the survey, because it converts the queue's
   characteristic death spiral from "mysterious slow backlog" into a named
   cause. The dashboard (`06-`) should surface `n_dead_tup` vs the computed
   autovacuum budget, `last_autovacuum`, and index density per index.
   **`pgstattuple` is available on our image** (verified), so where pg-boss had
   to infer bloat from `relpages`/`reltuples`, we can measure it exactly — and
   fall back to their heuristic when the extension is absent.

6. **Never run an unbounded aggregate over the job table.** This is the lesson
   of `plans.ts:968-975`. Any dashboard count, any metric, any `SELECT count(*)`
   over `_live` holds the horizon and stops vacuum from reclaiming. Maintain
   counters on a small `stampede_queues` row via trigger — which is exactly the
   narrow, invariant-maintaining trigger use that `03-postgres18.md:81-86`
   already permits — and read the counter, never the table. If an aggregate is
   ever unavoidable, port pg-boss's naptime-proportional backoff verbatim.

### What we are deliberately not doing

- **Not deleting on success** (Graphile). It makes the table trivially small
  and it is the reason Graphile's numbers look the way they do, but it
  forecloses the dashboard and MCP introspection that `06-` is built on. The
  LIST-by-state split gets most of the benefit — the hot partition is as small
  as Graphile's whole table — without losing history.
- **Not per-queue partitioning** (pg-boss). It is a real win for
  write-amplification (per-policy index sets, `plans.ts:775-790`) and for
  instant reclamation via `DROP TABLE`, but it requires runtime DDL, it
  composes badly with the state split (two-level partitioning), and it is an
  optimisation for a problem we do not have yet. Revisit if a single queue
  dominates.
- **Not the execution-table split** (solid_queue). Architecturally the
  cleanest answer to bloat in the survey — INSERT/DELETE only, no in-place
  churn, HOT is moot — but it costs a multi-table transaction per claim and
  discards the entire `RETURNING OLD./NEW.` claim design from `04-`, which is
  the central PG18 bet. Noted as the road not taken; if the state-partition
  approach disappoints under load, this is the fallback to re-evaluate, not
  a bigger pruner.

---

## Found by building it

**The counter trigger deadlocked under concurrency**, and it is worth recording
because the fix is a one-line `ORDER BY` that looks like noise.

A transition statement touches several `(queue, state)` counter rows at once —
one decrement, one increment. Without an ordering, concurrent transactions lock
them in whatever order the CTE happened to emit: worker A takes
`(default, executing)` then `(default, completed)` while worker B does the
reverse, and Postgres resolves it by killing one with SQLSTATE 40P01.

Reproduced reliably: four workers completing 200 jobs concurrently deadlocked
inside `stampede_complete` on every run. In production that is a worker failing
to report a *finished* job, which then sits in `executing` until the reaper
notices — a silent stall under exactly the load this design exists for, and one
that would have looked like a reaper bug rather than a counter bug.

`ORDER BY queue, state` on all three upserts fixes it, because a consistent
lock order cannot deadlock. This is the cost of item 6's counter table that the
original analysis did not anticipate.

## Still open

- ~~**Measure the row-move cost.**~~ **MOOT.** This assumed
  `PARTITION BY LIST (state)`, which `plans/README` §1 superseded with
  partitioning by QUEUE — uniqueness won. Completion is an in-place UPDATE, so
  there are no cross-partition row moves to measure.
- ~~**40001 retry budget.**~~ **MOOT**, same reason: no row movement, no
  "tuple to be locked was already moved to another partition" race.
- **Is `fillfactor = 85` cargo?** Still open, and now applies to every queue
  partition rather than a `_live` one. Oban justifies 85 as locality rather
  than HOT-eligibility, and claiming is never HOT-eligible here either. Worth
  an A/B; it costs 15% of every partition's pages to be wrong.
- **Retention default.** 1 hour for completed and 7 days for discarded, both
  triangulated from Oban's 60 s and River's 24 h rather than measured. The
  console can now show what it can see, so the introspection window is at least
  observable — but nobody has run it long enough to say whether an hour is
  right.
- **`uuidv7` and btree deduplication.** pg-boss's 24 MB vs 19 MB at 500k rows
  is a v4-UUID number (`plans.ts:916-923`). Re-measure with v7; if
  deduplication works as expected, `id` becomes a free ordering tiebreaker and
  the fetch index can be simpler than pg-boss's.
- ~~**`pg_cron` for the sweep?**~~ **No, decided.** It would split the
  retention policy between the daemon and the database, and the daemon already
  survives every worker being down — the pruner is a maintenance role, not a
  worker role, and `--no-worker` runs it.
- **`pgstattuple` cost.** It does a full relation scan. That is precisely the
  horizon-pinning pattern §7.6 forbids. Either restrict it to `pgstatindex` on
  small indexes, gate it behind the same naptime backoff, or keep it as an
  operator-invoked diagnostic rather than a background pass.
- **Correction needed in `03-postgres18.md`.** The `GENERATED … VIRTUAL`
  section (`:39-49`) implies virtual can replace Graphile's stored
  `is_available`. It cannot — virtual generated columns are not indexable on
  18.6 (verified). Fix that note before someone builds on it.
