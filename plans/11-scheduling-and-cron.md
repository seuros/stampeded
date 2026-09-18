# Scheduling, cron, and time

Delayed jobs, recurring jobs, leader election, DST, and whether to lean on
`pg_cron`.

Source study of nine permissively-licensed implementations read from
`~/Projects/ecosystem/reference/`. Restricted repos (Faktory, Sidekiq, Dramatiq,
Hangfire, JobRunr) were not opened — see `08-reference-survey.md`.

Measurements marked **verified** were executed against the live 18.6 instance
(`commonsharecore-db-1`). Everything else is cited to a file and line. Claims
marked *(inference)* are reasoned from source, not executed.

---

## 1. Delayed jobs — staging step vs claim predicate

Three distinct architectures, not two.

### (a) Predicate in the claim query

`run_at <= now()` lives in the fetch query; there is no promotion step and often
no state column distinguishing "scheduled" from "ready".

| impl | predicate | index backing it |
|---|---|---|
| Graphile Worker | `run_at <= now()` (`graphile-worker/src/generated/sql.ts:95-125`) | — see `04-` |
| que | `run_at <= now()`, twice, in a recursive CTE (`que/lib/que/poller.rb:74`, `:95`) | `que_poll_idx_with_job_schema_version (job_schema_version, queue, priority, run_at, id) WHERE finished_at IS NULL AND expired_at IS NULL` (`que/lib/que/migrations/5/up.sql:10-12`) |
| good_job | `only_scheduled` scope (`good_job/app/models/good_job/job.rb:123`) | seven partial indexes on `(…, scheduled_at, …) WHERE finished_at IS NULL` (`good_job/demo/db/schema.rb:110-117`) |
| procrastinate | `scheduled_at IS NULL OR scheduled_at <= now()` (`procrastinate/procrastinate/sql/schema.sql:250`) | **none** — fetch index is `(priority desc, id asc) WHERE status = 'todo'` (`schema.sql:109`) |
| pg-boss | `start_after <= job_now()` (`pg-boss/src/plans.ts:1877`) | `job_i11 (name, priority DESC, created_on, start_after) WHERE state < 'active' AND NOT blocked` (`pg-boss/src/plans.ts:926`) |
| underway | `state = 'pending' and run_at <= now()` (`underway/src/queue.rs:1054-1077`) | `idx_task_claim_pending (task_queue_name, priority desc, run_at, id) WHERE state = 'pending'` (`underway/migrations/20260213190000_9.sql:40-42`) |

pg-boss documents the failure mode for this family better than anyone. The
comment at `pg-boss/src/plans.ts:904-918` explains that `start_after` is
deliberately a **trailing key column** rather than `INCLUDE` or absent: as a key
column it is still evaluated as an Index Cond, so not-yet-due rows are discarded
inside the index. Without it the predicate degrades to a heap Filter and an idle
queue with a scheduled backlog goes *"0.04 ms -> 18 ms at 50k deferred, growing
linearly"*. `INCLUDE` cannot substitute because `FOR UPDATE` forces an Index Scan.

That is the whole tradeoff in one sentence: this family works only if
`scheduled_at` sits in a usable index position on the **hot claim path**, and it
makes every claim pay for rows that are not due yet.

### (b) A promotion pass (staging)

| impl | mechanism |
|---|---|
| Oban | `stage_jobs` moves `scheduled`/`retryable` → `available` (`oban/lib/oban/engines/basic.ex:147-166`); driven by `Oban.Stager`, 1 s interval, 5 000 rows (`oban/lib/oban/stager.ex:16-17`), leader-gated (`stager.ex:64`) |
| River | `JobSchedule` CTE (`river/riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql:539-557`), 5 s interval (`river/internal/maintenance/job_scheduler.go:26`) |
| solid_queue | two tables: `solid_queue_scheduled_executions` → `solid_queue_ready_executions`, by a separate Dispatcher process (`solid_queue/app/models/solid_queue/scheduled_execution.rb:7-24`) |
| asynq | Redis ZSET forwarder, `ZRANGEBYSCORE … LIMIT 0 100` in Lua, looped to exhaustion (`asynq/internal/rdb/rdb.go:1072-1090`, `:1117-1129`) |

solid_queue shows the architectural payoff most clearly: `ready_executions` has
**no time column at all** — its indexes are `(priority, job_id)` and
`(queue_name, priority, job_id)`
(`solid_queue/lib/generators/solid_queue/install/templates/db/queue_schema.rb:79-87`).
The worker's claim never evaluates a time predicate. That is the tightest hot
path in the survey.

### (c) Both, with a look-ahead — River

River is the only one that does both, and the reason is worth stealing.
`job_scheduler.go:175` promotes using `now + Interval` rather than `now`:

```
nowWithLookAhead := now.Add(s.config.Interval)
```

so a job is already `available` *before* it is due. The claim query then
re-checks `scheduled_at <= coalesce($now, now())`
(`river_job.sql:209`), which is what stops the early promotion from causing an
early run. Consequence: **promotion latency is removed from job latency**, and
the promoter can be slow, leader-gated, or briefly dead without adding delay.
The NOTIFY is separately gated on a 5 ms horizon so only genuinely-due jobs wake
a worker (`job_scheduler.go:194-202`).

### At scale — verified

Built a 2.2 M-row table on 18.6 (1 M future-scheduled, 1 M completed, 200 k
available, 500 due), 12 queues × 4 priorities, and ran Oban's exact `stage_jobs`
shape (`state IN ('scheduled','retryable') AND scheduled_at <= now() ORDER BY
scheduled_at DESC, id DESC LIMIT 5000`).

| index available | plan | buffers | time | index size |
|---|---|---|---|---|
| Oban's `(state, queue, priority, scheduled_at, id)` | **Parallel Seq Scan**, 2.2 M rows filtered | 18 430 | 54.2 ms | 124 MB |
| purpose-built `(scheduled_at, id) WHERE state IN ('scheduled','retryable')` | Index Scan Backward, 508 rows | 11 | 0.165 ms | 39 MB |

**1 675× fewer buffers, 328× faster, one third the index size.**

The composite index is not merely slower — the planner declines it outright. Its
`scheduled_at` is the *fourth* key column, behind `queue` and `priority`, which
the staging query does not constrain. With a single queue and a single priority
the same query does get a Parallel Index Only Scan (172 buffers, 3.02 ms,
verified), so the degradation tracks `queue × priority` cardinality. PG18's
B-tree skip scan makes the one-queue case survivable and the twelve-queue case
not.

Caveat: `state` was `text`, Oban uses an enum, and the row was narrower than a
real `oban_jobs`. Directionally sound; not a benchmark of Oban.

This is also why Oban's own history is instructive. It *had* the right index —
`(scheduled_at) WHERE state IN ('available','scheduled')`
(`oban/lib/oban/migrations/postgres/v02.ex:12-15`) — and **dropped it in V05**
(`v05.ex:7`) in favour of `(queue, state, scheduled_at, id)`, later reshuffled to
`(state, queue, priority, scheduled_at, id)` (`v08.ex:18-22`, `v09.ex:14-16`).
The claim path won the index argument and the staging path was left to share.

**Conclusion for #1:** the staging query and the claim query want *different*
indexes. Every implementation that tries to serve both from one composite index
pays for it on whichever path lost.

---

## 2. Recurring jobs — where the schedule lives

| impl | schedule lives in | shape |
|---|---|---|
| **db-scheduler** | **the jobs table itself** | PK `(task_name, task_instance)`, recurring instance literally named `"recurring"` (`db-scheduler/.../task/helper/RecurringTask.java:25`); schema at `db-scheduler/db-scheduler/src/test/resources/postgresql_tables.sql:1-16` |
| solid_queue | DB table, seeded from `config/recurring.yml` | `solid_queue_recurring_tasks`, unique `key` (`queue_schema.rb:88-102`); upserted on boot (`recurring_schedule.rb:85-88`) |
| pg-boss | DB table | `schedule`, PK `(name, key)`, columns `cron`, `kind`, `timezone` (`pg-boss/src/plans.ts:348-359`) |
| underway | DB table, **one schedule per queue** | `task_schedule`, PK = queue name (`underway/migrations/20240921151751_0.sql:19-27`) |
| Oban | **app config, parsed at boot** | `crontab:` list (`oban/lib/oban/cron.ex:263-276`); the moduledoc at `cron.ex:9-12` says runtime/global cron is a Pro feature |
| River | **app code**, `PeriodicSchedule` interface | `periodic_job.go:15-19`; River ships no cron parser — `PeriodicInterval` is `t.Add(interval)` (`:104-106`) |
| good_job | **app config** (hash / ENV / CLI / **lambda**) | `configuration.rb:227-239`; `CronEntry` is `ActiveModel`, not `ActiveRecord` (`cron_entry.rb:11-12`); only per-key enable/disable is in the DB |
| procrastinate | **app config**, `@app.periodic` decorator registry | `periodic.py:47-49`, `:76-109` |
| asynq | app config, or a `PeriodicTaskConfigProvider` re-read every 3 min | `periodic_task_manager.go:45`, `:79-88` |
| neoq | **in-process `cron.Cron` only** | `postgres_backend.go:95`, `:141` — queue name derived from the *English description* of the spec (`:537-563`) |

### db-scheduler's design deserves its own paragraph

There is no schedule table, no cron plugin, no leader, and no misfire policy —
because **the schedule row *is* the due row**. A recurring task is one row in
`scheduled_tasks` whose `execution_time` is advanced on completion
(`RecurringTask.java:88`, `OnCompleteReschedule`). The next time is computed
forward from `executionComplete.getTimeDone()` (`CronSchedule.java:71-90`).

Consequences, all free:

- Downtime does not skip a run. The row is simply overdue and runs **late, once**,
  as soon as any node picks it up. No backfill logic exists because none is needed.
- No two nodes fire it, because claiming the row is the same `picked`/`version`
  optimistic-lock claim used for every other job (`JdbcTaskRepository.java:417`).
- No occurrence key, no leader election, no `ON CONFLICT`.
- Drift does not accumulate: next is computed from *done*, not from *scheduled*.

The registration race is resolved by the primary key. `scheduleIfNotExists`
(`ScheduleRecurringOnStartup.java:79` → `JdbcTaskRepository.java:171-195`) does a
plain insert and treats the constraint violation as "another node created it".

### Missed runs — the actual spread

| policy | who | mechanism |
|---|---|---|
| **runs late, once** | db-scheduler | structural; the overdue row is still there |
| **skipped** | Oban, River, solid_queue, underway, asynq, neoq | Oban evaluates `now?` against the current minute only (`cron.ex:339-347`); River documents it at `periodic_job.go:65-73` — *"each scheduler only retains in-memory state, so anytime a process quits or a new leader is elected, the whole process starts over without regard for the state of the last scheduler"*; underway has no last-fired state at all (`task_schedule` has only `created_at`/`updated_at`) |
| **one job for the whole gap** | pg-boss | `SCHEDULE_MISSED_POLICIES = {skip, once}` (`src/plans.ts:100-103`); the 17-line rationale at `:83-99` — *"a nightly report that missed three nights is one report, not three"* — and per-occurrence backfill is explicitly rejected as unimplementable because the forwarded job carries only the schedule's `data` |
| **per-occurrence backfill, opt-in** | good_job | `cron_graceful_restart_period` (`configuration.rb:241-245`); `create_graceful_tasks` replays `period.ago..Time.current` (`cron_manager.rb:118-132`) |
| **per-occurrence backfill, bounded** | procrastinate | warm restart backfills every intervening tick (`periodic.py:171-177`); cold start takes only the last occurrence and drops it if older than `MAX_DELAY = 600 s` (`:20-22`, `:179-198`) |

Only River's and good_job's mitigations are honest about the cost. River's
`RunOnStart` is described as *"a hedge to make sure that jobs with long run
durations are guaranteed to occasionally run"* (`periodic_job.go:42-51`) — an
admission that the schedule itself is not durable.

---

## 3. Leader election — the crux

Two families, and they are not equally good.

### Family A — one writer, elected

| impl | mechanism | failure mode |
|---|---|---|
| **Oban** | `oban_peers` table, PK on `name` (`oban/lib/oban/migrations/postgres/v11.ex:7-12`, `SET UNLOGGED` at `:15`). `INSERT … ON CONFLICT (name) DO UPDATE SET expires_at WHERE node = $node` (`peers/database.ex:204-216`) — the `WHERE node =` makes the update affect 0 rows for a non-owner, and `{0, nil}` means "not leader". 30 s lease, re-elect at interval/2 with jitter (`:35-37`, `:150-155`); expired peers deleted first (`:157-166`). | `leader?` is a **cached GenServer boolean** (`:140-142`) — it can be stale for up to 15 s after the lease is actually lost. Nothing fences the cron insert against that. |
| **River** | `river_leader`, `name text PRIMARY KEY CHECK (name = 'default')` — a one-row table by construction (`riverdriver/riverpgxv5/internal/dbsqlc/river_leader.sql:1-8`, `CREATE UNLOGGED TABLE`). `LeaderAttemptElect` is `INSERT … ON CONFLICT (name) DO NOTHING RETURNING *` (`:10-23`); `LeaderAttemptReelect` renews only if `elected_at` and `leader_id` still match and `expires_at >= now()` (`:25-32`). Resignation deletes *and* `pg_notify`s in one statement (`:53-69`). | Materially stricter than Oban: `newLeadershipTerm` computes `trustedUntil = attemptStarted + ttl - 1 s` from the time *before* the DB round-trip (`internal/leadership/elector.go:244-258`, margin at `:31`). The leader distrusts itself before the DB-side lease expires. This is the right shape for a local fencing deadline. |
| **underway** | session-level `PgAdvisoryLock("{queue}-scheduler")` (`src/scheduler.rs:149-151`), acquired once at `:357-361`. | The loser **returns `Ok(())` and exits permanently** — no retry, no standby, no failover. And there is **no database-side dedup backstop**: `process_next_schedule` (`:432-445`) enqueues unconditionally with nothing keyed to the occurrence. The lock is load-bearing and single-point. |
| **asynq** | **none.** `enqueueJob.Run` calls `client.Enqueue` unconditionally (`scheduler.go:181-204`). | Two scheduler processes = two jobs, every tick. The operator is expected to run exactly one. No election primitive exists anywhere in the source *(inference: grep of the package)*. |

### Family B — no leader; a unique index arbitrates

Every node computes the same occurrences and races to insert. The database picks
the winner.

| impl | key | how the loser finds out |
|---|---|---|
| **solid_queue** | `UNIQUE (task_key, run_at)` (`queue_schema.rb:85`) | `insert(attributes, unique_by: [:task_key, :run_at])` → `ON CONFLICT DO NOTHING RETURNING id`; empty result raises `AlreadyRecorded` (`recurring_execution.rb:10-23`). **The ledger insert and the job enqueue are one transaction** (`:25-33`), so a lost race rolls the job back too. |
| **good_job** | `UNIQUE (cron_key, cron_at) WHERE cron_key IS NOT NULL` (`demo/db/schema.rb:101`; made partial in `demo/db/migrate/20231128075428_recreate_good_job_cron_indexes_with_conditional.rb:13-16`) | attempts the insert, catches `RecordNotUnique`, and re-raises if `cron_key` is absent (`job.rb:526-534`) — so a unique violation on a non-cron job still propagates. |
| **procrastinate** | `UNIQUE (task_name, periodic_id, defer_timestamp)` on a **separate ledger table** `procrastinate_periodic_defers` (`sql/schema.sql:81-88`) | inside plpgsql: `INSERT … ON CONFLICT DO NOTHING RETURNING id INTO _defer_id; IF _defer_id IS NULL THEN RETURN NULL` (`schema.sql:162-170`) — the job is never created for the loser. |
| **pg-boss** | `UNIQUE (name, singleton_on, COALESCE(singleton_key,'')) WHERE state <> 'cancelled' AND singleton_on IS NOT NULL` (`src/plans.ts:897-898`) | `ON CONFLICT DO NOTHING` (`:2261`). Layered *on top of* an interval claim on a single-row `version` table (`:1129-1136`), so pg-boss is the only one with both. |
| **neoq** | `UNIQUE (queue, fingerprint, status) WHERE status <> 'processed'` (`migrations/20240302172413_make_fingerprints_unique_to_each_queue.up.sql:1-2`) | `pgerrcode.UniqueViolation` → `ErrDuplicateJob` (`postgres_backend.go:475-483`), swallowed by the cron closure (`:573-578`). |

**The discriminator inside Family B is whether the key contains the occurrence
instant.** solid_queue, good_job, procrastinate and pg-boss all include it and
get true once-per-occurrence semantics. neoq does not — its fingerprint is
`md5(queue || json(payload))` (`jobs/jobs.go:51-76`) and a cron job has a nil
payload, so the fingerprint is a *constant*. That gives "at most one outstanding
job per cron queue", which **coalesces**: a tick that fires while the previous
occurrence is still `new` or `failed` is dropped, not queued.

### Two dependencies that are easy to miss

1. **Family B's guarantee dies when you prune completed jobs.** good_job never
   destroys cron-keyed rows (`job.rb:894`: `if finished_at.blank? ||
   cron_key.present? || preserve_job_record?(result)`) and documents the
   requirement at `README.md:690`. solid_queue's README says the same: the
   guarantee holds *"as long as you keep the jobs around"*, and its
   `RecurringExecution` rows are `clearable` once the job is gone
   (`recurring_execution.rb:7`, swept at `:35-40`). **This collides head-on with
   the open retention/partitioning question in `04-`.** procrastinate is the only
   one that dodges it — its ledger is a separate narrow table, garbage-collected
   on its own schedule (`schema.sql:193-204`).

2. **Identity by hash is a bad idea.** Oban's cron entry name is
   `:erlang.phash2/1` over `{expr, worker, opts}` (`cron.ex:200-210`) — 27 bits.
   It is only used for `meta.cron_name`, not for dedup, which is fortunate.
   neoq's derivation of the queue name from the *English description* of the cron
   spec (`postgres_backend.go:537-563`) means two different specs whose
   descriptions normalise identically collide onto one queue.

### Which family is better

Family B, decisively. Family A makes correctness depend on a lease being
correct under clock skew, GC pauses and network partitions; Family B makes
duplicate enqueue **structurally impossible** and degrades to wasted work, not
wrong work. Oban's stale cached `leader?` and underway's no-failover exit are
both real bugs-in-waiting that Family B cannot have.

---

## 4. Timezones and DST

| impl | library | zone storage | spring-forward (02:30 does not exist) | fall-back (02:30 twice) |
|---|---|---|---|---|
| **db-scheduler** | `java.time` / cron-utils | `ZoneId` on the schedule (`Daily.java:53-59`, `CronSchedule.java:56-68`) | **runs once** — `ZonedDateTime.of(date, time, zone)` (`Daily.java:67`, `:73`) shifts a gap forward by its length | **runs once** — `ZonedDateTime.of` resolves an overlap to the earlier offset *(inference: documented `java.time` semantics)* |
| **underway** | jiff + jiff-cron | IANA name asserted at construction (`src/scheduler.rs:478-489`); wire format `"<expr>[<IANA>]"` (`:538-557`) | delegated to jiff `Zoned` arithmetic; iteration is genuinely zoned, not UTC-then-convert (`:388-390`, `:502-507`) | same |
| **pg-boss** | cron-parser + rrule-temporal | `timezone text DEFAULT 'UTC'` per row (`src/plans.ts:352`), read back as `COALESCE(timezone,'UTC')` (`:1255`) | delegated | **fires twice** — the unique slot is a UTC instant, so the two 02:30s are different keys |
| **solid_queue** | Fugit | trailing zone in the cron string, or `SolidQueue.time_zone` normalised to IANA via `tzinfo.name` (`lib/solid_queue.rb:46-53`, `recurring_task.rb:182-188`) | delegated | **fires twice** — `(task_key, run_at)` holds distinct UTC instants |
| **good_job** | Fugit | trailing zone only; **no** default-zone injection, so process-local by default (`cron_entry.rb:216-218`) | delegated | **fires twice** — distinct `cron_at` |
| **Oban** | own parser | `:timezone` per crontab entry or globally (`cron.ex:49-52`, `:270`) | **skipped** — `now?` is pure field matching on the current wall-clock minute (`cron/expression.ex:91-99`) against `DateTime.shift_zone!(now, tz)` (`cron.ex:344`), and 02:30 local never occurs | **fires twice** — 02:30 local occurs at two UTC instants and there is no occurrence key |
| **River** | none shipped | whatever you plug into `PeriodicSchedule` | delegated to the user's library; `ScheduleFunc` is called with the *previous target* (`periodic_job_enqueuer.go:479`), so a DST-aware library gets a correct input | same |
| **asynq** | robfig/cron v3, `cron.WithLocation` (`scheduler.go:108`) | `SchedulerOpts.Location`, default UTC (`:98-101`, `:147-150`) | skipped *(inference: robfig v3 does wall-clock field search in the location)* | fires twice, and asynq has no dedup at all |
| **neoq** | robfig/cron **v1** | **none** — bare `cron.New()` (`postgres_backend.go:141`); v1 has no `WithLocation`, so `time.Local` | skipped *(inference)* | second fire swallowed as `ErrDuplicateJob` if the first is still unprocessed — accidental, not designed |
| **procrastinate** | croniter, **no `tz` argument** (`periodic.py:39-41`) | none; naive epoch floats throughout (`:135`, `:139-198`) | n/a — effectively UTC-relative | n/a |

**Nobody has explicit DST code.** All nine delegate to a library. Only
db-scheduler is correct-by-construction on both transitions, and only because
`java.time`'s gap/overlap resolution happens to be right; there is no test or
comment showing it was intended.

The fall-back double-fire is the most widespread silent bug in the survey. It is
emergent: everyone keys the occurrence on a **UTC instant**, and fall-back
produces two distinct UTC instants for one wall-clock time, so a correctly
implemented unique index dutifully lets both through. For a "daily at 02:30" job
that is wrong. For an "every 30 minutes" job it is right. Nobody distinguishes.

pg-boss gets closest to caring: it validates the IANA zone eagerly
(`src/timezone.ts:19-36`) because cron-parser validates `tz` **lazily** and
accepts `'America/New_Yrok'`, exploding later as an opaque "CronDate: unhandled
timestamp" (`timezone.ts:3-17`); and it rejects a non-string zone outright
because cron-parser reads that as "unset" and silently uses host-local time
(`:27-29`). It also anchors rrules at a **floating** `19700101T000000`
(`src/rrule.ts:19`) so `INTERVAL=2` phase does not move on restart (`:6-18`).

---

## 5. `pg_cron` 1.6 — evaluation

**Verified on 18.6:** `pg_available_extensions` reports `pg_cron | 1.6 |` — the
null third column confirms *available, not installed*, as `03-` states.

### What it would replace

The in-daemon tick loop and, if we chose Family A, the leader election behind it.
Nothing else. It does not store jobs, claim them, retry them, or know anything
about Stampeded's schema.

### Limitations, in order of how much they hurt

1. **Install is an operator action, not a migration.** Requires
   `shared_preload_libraries = 'pg_cron'`, a cluster restart, and superuser to
   `CREATE EXTENSION`. A self-hoster cannot get it from `stampeded migrate`.
2. **Metadata lives in exactly one database per cluster.** `cron.database_name`
   defaults to `postgres`; the extension may only be installed once, and
   `cron.schedule_in_database()` is how you reach the others. So Stampeded's
   schema would live in `myapp` while its schedule rows live in `postgres` —
   split across backup, restore, migration and `pg_dump` boundaries. For a
   product whose pitch is "Postgres is the protocol" (`00-vision.md`), that is a
   bad seam.
3. **One timezone for the entire cluster.** `cron.timezone` is a GUC, default
   `GMT`. Per-schedule zones — which pg-boss, solid_queue, underway and
   db-scheduler all have — are not expressible.
4. **Granularity.** Minute-resolution cron, plus `'N seconds'` intervals
   (1–59) since 1.5, which cannot be combined with cron fields.
5. **Concurrency is a hard cap, not a queue.** `cron.max_running_jobs` defaults
   to 32, drawn from `max_worker_processes` (8 by default with
   `cron.use_background_workers = on`). Hitting the cap fails or skips the tick.
6. **`cron.job_run_details` grows unbounded** unless pruned or
   `cron.log_run = off`.
7. **It does not fix the reliability property people credit it with.** If the
   database is down, pg_cron misses the occurrence exactly as a daemon would.
   Its advantage is narrower than "the schedule survives": it survives the
   *workers* being down, not the *cluster* being down.
8. Managed-provider availability varies, and 1.6.0 requires
   `ALTER EXTENSION pg_cron UPDATE` after a binary upgrade.

### Verdict

**Do not depend on it. Support it as an optional tick source.**

The reliability argument — the schedule fires even with zero workers running —
is real but small, and it is almost entirely recoverable in the daemon. With a
durable `next_run_at` and an overdue row (db-scheduler's model, §2), a daemon
that comes back finds the occurrence still waiting and runs it late. The only
genuine delta is the *latency* of an occurrence during an outage, not its
existence. Trading a restart-and-superuser install requirement for that is a bad
trade when self-hosting is a goal.

---

## What Stampeded should do

### Delayed jobs — staging, with River's look-ahead and a claim-side fence

- Keep the `state` column decided in `04-`. `scheduled`/`retryable` → `available`
  is a promotion pass, not a claim predicate.
- Promote with a **look-ahead of one promoter interval** (River,
  `job_scheduler.go:175`), so promotion latency never appears in job latency.
- **Retain `scheduled_at <= now()` in the claim query anyway** (River,
  `river_job.sql:209`). It costs an Index Cond and it is what makes the
  look-ahead safe. Thread an injectable clock through it, per `04-`.
- **Give staging its own partial index.** `(scheduled_at, id) WHERE state IN
  ('scheduled','retryable')` — verified 1 675× fewer buffers and one third the
  size of sharing the claim's composite index. Do not repeat Oban's V05 mistake
  of deleting it to save space.
- Promotion is **leader-gated for efficiency, never for correctness**: the
  promotion `UPDATE` is idempotent, so a leader-election bug costs duplicated
  work, not duplicated jobs. Write that in the schema comment.
- If a leader is used at all, use River's lease shape, not Oban's: a local
  `trusted_until` computed from *before* the round-trip
  (`elector.go:244-258`), not a cached boolean (`peers/database.ex:140-142`).

### Recurring jobs — the schedule row *is* the due row, plus an occurrence ledger

Two mechanisms, each doing one job.

**Primary — db-scheduler's model.** One row per schedule in
`stampeded_schedules` carrying `next_run_at`, claimed with the same
`FOR UPDATE SKIP LOCKED` used for jobs. The claimer enqueues the job and advances
`next_run_at` **in the same transaction**. This buys, for free:

- no leader election for cron at all;
- a missed occurrence runs **late, once**, because the overdue row is still there;
- no drift, because the next time is computed from the previous target;
- schedules are queryable and mutable at runtime, which the MCP control plane in
  `06-` needs and which Oban's, River's, good_job's and procrastinate's
  config-only schedules cannot offer.

**Secondary — an occurrence ledger as an idempotency backstop and audit trail.**
`stampeded_cron_occurrences (cron_key, occurrence_token, job_id)` with
`UNIQUE (cron_key, occurrence_token)`, inserted `ON CONFLICT DO NOTHING
RETURNING id` in the same transaction. Verified on 18.6: the winner gets one row,
the loser gets zero.

Put it in a **separate narrow table**, not on the jobs table.
good_job and solid_queue both put the key on the jobs table and both had to
guarantee cron rows are never pruned (`job.rb:894`; solid_queue README). `04-`
already has an unresolved retention/partitioning question and `03-` records that
`pg_partman` is absent — coupling cron correctness to job retention would make
that question load-bearing for correctness. procrastinate's separate ledger
(`sql/schema.sql:81-88`) is the right precedent; it is prunable on its own
horizon.

### DST — make the occurrence token the policy

This is where Stampeded can be straightforwardly better than all nine, because the
fix is a column, not an algorithm.

`occurrence_token` is **text**, and what goes in it is a per-schedule policy:

- `instant` — the UTC instant. Fall-back fires twice. Correct for sub-daily
  schedules ("every 30 minutes" should run 25 times on a fall-back day).
- `wall_clock` — the local wall-clock datetime plus zone. Fall-back's two 02:30s
  collapse to one token and the second is dropped. Correct for schedules that pin
  an hour ("daily at 02:30" is one report, not two).

Default: `wall_clock` when the cron expression constrains the hours field,
`instant` otherwise. One column, one index, no branching in the firing path.

Spring-forward is the easier half: fire once, at the instant the gap ends, and
record that real instant.

> **CORRECTED IN IMPLEMENTATION.** An earlier line in this file, and the first
> draft of the schema comment, said `wall_clock` should SKIP a spring-forward
> occurrence. It does not: skipping silently loses one run a year, and a daily
> report that does not happen on one day in March is worse than one that happens
> an hour late. This paragraph is what shipped, and it is exactly jiff's
> `Compatible` disambiguation, which is also Vixie cron's behaviour. This is what `java.time` does for db-scheduler
(`Daily.java:67`) and what jiff's `Compatible` disambiguation does for underway.
Oban skips the day entirely (`expression.ex:91-99`); that is worse and avoidable.

Mechanics:

- `timezone text NOT NULL DEFAULT 'UTC'` per schedule, **not nullable** —
  pg-boss's reasoning at `src/plans.ts:1245-1249`: a null read back evaluates in
  the *host's* zone, which differs per node.
- Validate the IANA name **eagerly** at write time, and assert
  `TimeZone::get(name).iana_name().is_some()` the way underway does
  (`src/scheduler.rs:478-489`). pg-boss documents what lazy validation costs
  (`src/timezone.ts:3-17`).
- Use **jiff** for the arithmetic. It is already in
  `~/Projects/ecosystem/reference/jiff`, underway proves `jiff-cron` works
  against this exact problem, and its `Zoned` type makes the gap/overlap
  decision explicit rather than emergent.

### Missed runs — a policy, defaulted to `once`

The occurrence ledger makes all three policies safe to implement, because replay
is idempotent across N booting nodes — which is exactly good_job's argument for
why its backfill is safe (`cron_manager.rb:115-116`).

| policy | behaviour |
|---|---|
| `late` | run the overdue row whenever a node arrives (the free default from the primary mechanism) |
| `once` | collapse a whole outage to a single run (pg-boss `src/plans.ts:83-103`) |
| `backfill(window)` | one job per missed occurrence within the window (good_job `cron_manager.rb:118-132`) |
| `skip` | advance `next_run_at` past the gap without firing |

Default `once`. pg-boss's rationale is right and its warning matters: a
per-occurrence backfill is only meaningful if the job can tell *which* occurrence
it is, so Stampeded must pass `occurrence_at` in the job args — procrastinate does
this (`periodic.py:213`) and pg-boss could not.

### `pg_cron` — optional ticker, never a dependency

Because correctness lives in the occurrence key and the overdue row, *who ticks*
is a deployment choice. Ship a `stampeded.tick()` SQL function that performs the
same claim-and-enqueue, and document
`cron.schedule_in_database('stampeded-tick', '* * * * *', 'SELECT stampeded.tick()', 'myapp')`
as a supported alternative for operators who already run pg_cron. Never require
it, never test only that path, and never let a schedule column exist that only
pg_cron can populate.

### `uuidv7()` for scheduled jobs — rejected, with reason

**Verified on 18.6:** `uuidv7(shift interval)` and `uuid_extract_timestamp()`
both work, and round-trip exactly —
`uuid_extract_timestamp(uuidv7(timestamptz '2026-09-17 02:00+00' - now()))`
returns `2026-09-17 02:00:00+00`. So minting a PK whose embedded timestamp *is*
`scheduled_at` is possible, and would make `ORDER BY id` equal `ORDER BY
scheduled_at` for free.

**Do not.** It inverts the one property uuidv7 exists for. Time-ordered keys give
index locality because inserts land at the right-hand edge of the B-tree. Shift
the timestamp to a future `scheduled_at` and inserts scatter across the whole
future range — a job scheduled six months out writes into a page nothing has
touched, and a mixed workload of immediate jobs, retries and long-delayed jobs is
the worst case. The partial index measured in §1 costs 39 MB and 11 buffers;
degrading PK locality across the entire table to avoid it is not a trade.

Do exploit the read side: `uuid_extract_timestamp(id)` is a free `created_at`,
so that column need not exist.

### Verified correction to `03-postgres18.md`

`GENERATED … VIRTUAL` **cannot** express a time-dependent predicate. Verified:

```
CREATE TABLE g1(scheduled_at timestamptz,
  is_ready bool GENERATED ALWAYS AS (scheduled_at <= now()) VIRTUAL);
ERROR:  generation expression is not immutable
```

Generation expressions must be `IMMUTABLE`; `now()` is `STABLE`. `03-` at lines
47–49 proposes exactly `is_ready` / `is_stuck` / `is_retryable` as virtual
columns — none of those are expressible, because all three are time-relative.
Virtual columns remain useful for genuinely immutable derivations
(`attempt >= max_attempts`, verified working), and that is the whole of their
applicability here. Graphile Worker's stored `is_available` is
`(locked_at IS NULL) AND (attempts < max_attempts)` — note it deliberately
contains no time term, for this reason.

---

## Still open

- ~~Promoter interval and batch size.~~ **Moot: there is no promoter.** The
  claim predicate reads `scheduled_at <= now()` directly, so a delayed job
  becomes claimable without a separate pass that can fall behind or die. That
  was the whole reason for choosing the predicate over a promotion pass.
- ~~One table or two.~~ **Two.** The cron row would otherwise compete with jobs
  for the claim index, and the dashboard's extra join is cheap by comparison.
- ~~Occurrence-ledger retention.~~ **Done, and it was a live leak**:
  `stampeded_prune_occurrences` existed from the start and was called from
  NOWHERE, so the ledger grew one row per firing forever — 525,600 a year for a
  schedule running every minute. Now pruned on its own 30-day horizon, separate
  from job retention, which is the entire reason it is a separate table.
- ~~`LISTEN/NOTIFY` on promotion.~~ **Falls out of the design.** There is no
  promotion step; the scheduler ENQUEUES, and the insert trigger notifies like
  any other insert. pg-boss's `scheduled_at <= now()` gate is on our trigger
  too, so a future-dated job wakes nobody — there is a test for it.
- ~~`wall_clock` default for all explicit-hour schedules?~~ **Sharper rule
  found:** it keys off whether the hour field is EXPLICIT (`3`, `3,15`, `9-17`)
  rather than on the period. `0 */6 * * *` uses a step, so it gets `instant` —
  which is right, because "every six hours" should run 5 times on a 25-hour day
  rather than 4. That resolves the `*/6` case the note could not.
- ~~Clock skew.~~ **Both**: the database is authoritative (retry delays are
  durations it applies; the scheduler compares server-side), AND the skew is
  measured and warned on past a minute, pg-boss style. It is a diagnostic here
  rather than a correctness input — past a minute, job timestamps and log
  timestamps stop lining up, which costs an hour during an incident.
