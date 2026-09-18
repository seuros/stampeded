# Wakeup — polling vs LISTEN/NOTIFY

Resolves the open item in `04-job-lifecycle-and-claiming.md:125`.

The question: after an enqueue commits, how long before a worker starts the job,
and what does that cost when idle?

Everything under **Verified on 18.6** was executed against `commonsharecore-db-1`
(PostgreSQL 18.6, Debian 18.6-1.pgdg12+2, aarch64) and the output is quoted.
Everything with a `file:line` was read from cloned source. Inference is labelled.

Trees read: `graphile-worker` v0.18.0 (`4cda192`), `pg-boss` v12.33.0 (`d63ae11`,
upstream `timgit/pg-boss` master), `que` 2.4.1, plus `oban`, `river`,
`procrastinate`, `good_job`, `solid_queue`, `underway`, `asynq` at survey HEAD.
**The "pg-boss is pure polling" reputation is stale** — this tree ships a full
LISTEN/NOTIFY path; `08-reference-survey.md:56` should be read with that in mind.

---

## Verified on 18.6 — the Postgres primitives

```
select pg_notify('c', repeat('x',7999)) --> ok
select pg_notify('c', repeat('x',8000)) --> ERROR:  payload string too long
select pg_notify(repeat('c',64),'x')    --> ERROR:  channel name too long
show max_notify_queue_pages            --> 1048576   (8 GB async queue)
select pg_notification_queue_usage()   --> 0
```

So: **payload ≤ 7999 bytes, channel name ≤ 63 bytes.** The 63-byte channel cap is
`NAMEDATALEN-1` and is the reason both River and pg-boss compute channel names
carefully (below).

### Deduplication inside a transaction — the load-bearing property

```
LISTEN c;
BEGIN;
  SELECT pg_notify('c','same');
  SELECT pg_notify('c','same');
  SELECT pg_notify('c','diff');
COMMIT;
--> Asynchronous notification "c" with payload "same" received ...
--> Asynchronous notification "c" with payload "diff" received ...
```

Identical `(channel, payload)` pairs within one transaction collapse to one
delivery. This is what makes a wakeup-only payload self-limiting: 50,000 inserts
across 5 queues in one transaction emit **5** notifications, not 50,000.
Confirmed with a real trigger:

```
BEGIN;
  INSERT INTO nt_jobs(queue) SELECT 'default' FROM generate_series(1,1000);
  INSERT INTO nt_jobs(queue) SELECT 'mailer'  FROM generate_series(1,500);
COMMIT;
--> 2 notifications: "default", "mailer"
```

River's schema says exactly this in a comment
(`riverdriver/riverdatabasesql/migration/main/002_initial_schema.up.sql:68-70`):

> Notify will coalesce duplicate notifications within a transaction, so keep
> these payloads generalized

### Trigger cost, measured

50,000-row single-statement `INSERT ... SELECT`, 3 runs each, same session,
warm cache:

| variant | run times (ms) | mean | delta |
|---|---|---|---|
| no trigger | 102.9 / 94.0 / 90.1 | 95.7 | — |
| `FOR EACH STATEMENT` + `REFERENCING NEW TABLE`, `PERFORM pg_notify(...) FROM (SELECT DISTINCT queue ...)` | 98.4 / 94.9 / 97.0 | 96.8 | **+1.2 %** |
| `FOR EACH ROW`, `pg_notify('…', NEW.queue)` | 161.6 / 167.9 / 162.1 | 163.9 | **+71 %** |

Both trigger variants delivered exactly 5 notifications (one per distinct
queue). **The statement-level transition-table trigger is free; the per-row
trigger costs ~70 % on bulk insert** even though the *notifications* dedup
identically — the cost is 50,000 plpgsql invocations, not 50,000 notifications.

Not measured cleanly: the single-row-insert case. Transition-table setup is
per-statement, so it does not amortise there; the box was too noisy to quote a
number. **Open.**

### Partitioned tables — three results that change the design

`13-retention-and-bloat.md` decides LIST partitioning by `state`. That interacts
with the trigger directly. Tested on 18.6 against
`CREATE TABLE pj(...) PARTITION BY LIST (state)` with partitions `pj_avail`
(`'available'`) and `pj_done` (`'completed'`), trigger on the **parent**:

**1. A statement-level trigger with `REFERENCING NEW TABLE` works on a
partitioned parent.** A three-row multi-queue insert through the parent produced
three notifications, one per distinct queue. No error, no caveat.

**2. Inserting directly into a partition does NOT fire the parent's trigger.**

```
INSERT INTO pj_avail(queue,state) VALUES ('direct','available');  --> no notification
```

**This is a live hazard for the polyglot story.** `05-polyglot-protocol.md:22-27`
tells people to `INSERT INTO stampede_jobs`. Nothing stops them — or a `COPY`, or
a migration tool, or a well-meaning DBA — from targeting the partition, and the
job then waits out the poll interval with no error anywhere. Mitigation is to put
the trigger on **each partition that can hold `available` rows**, not on the
parent (see 3), and to keep the poll floor so the failure is latency rather than
a stuck job.

**3. Duplicate triggers on parent *and* partition still deliver exactly one
notification.**

```
-- triggers on both pj and pj_avail
INSERT INTO pj(queue,state) VALUES ('viaparent','available');
--> Asynchronous notification ... payload "viaparent"    (once, not twice)
```

The transaction-level dedup absorbs it, because the payload is the bare queue
name. Belt-and-braces trigger placement is therefore free — **but only for a
payload with no nonce.** Graphile's `{"r":random()}` (`sql/000020.sql:86`) would
double-fire here. This is the third independent reason to keep the payload a bare
routing key, and the strongest one.

**4. A cross-partition `UPDATE` does NOT fire the `AFTER INSERT` trigger.**

```
UPDATE pj SET state='completed' WHERE queue='default';  --> no notification
```

Row movement between partitions is internally DELETE+INSERT, but it does not
present as an INSERT to triggers. So claiming and completing generate no spurious
wakeups, and we avoid underway's problem (`after insert or update`,
`20241024174106_1.sql:19-22`) without having to think about it.

### NOTIFY fires at COMMIT

```
txn_start 18:31:09.564   (pg_notify called here)
commit_at 18:31:13.572   (4 s later)
listener observed the notification only after the commit
```

Delivery latency has a hard floor of "time remaining in the enclosing
transaction". For Stampede this is *correct* — `05-polyglot-protocol.md:33-37`
sells transactional enqueue, and you must not wake a worker for a row that has
not committed — but it means **notify latency is bounded below by business
transaction length, not by queue machinery**. A 30-second business transaction
that enqueues at its start gives a 30-second notify latency no matter what.

### Idle poll cost, measured

200,000-row table, 0 rows `available`, partial index
`(queue, priority, scheduled_at, id) WHERE state='available'`:

```
Limit (actual time=0.008..0.009 rows=0.00 loops=1)  Buffers: shared hit=5
Execution Time: 0.020 ms
```

**0.02 ms and 5 buffer hits per empty poll.** 50 workers × 5 queues × 1 Hz =
250 polls/s = **5 ms of CPU per second, ~0.5 % of one core**. Polling against a
correct partial index is not a database-CPU problem. The cost of polling is
round-trips and connection occupancy, not query execution. (Inference: the
"polling is expensive" folklore comes from implementations that poll without a
partial index, or that poll a hot unpartitioned table.)

---

## 1. Who uses LISTEN/NOTIFY, and how

### Oban (Elixir, Apache) — notifier is pluggable, NOTIFY is one option

- Dedicated `Postgrex.SimpleConnection`, `auto_reconnect: true`
  (`lib/oban/notifiers/postgres.ex:55-63`). Not from the Ecto pool.
- Channels are `"#{prefix}.oban_#{channel}"` (`postgres.ex:196-198`), four of
  them: `insert`, `leader`, `signal`, `sonar` (`lib/oban/notifier.ex:33-41`).
- Re-LISTENs everything on reconnect from a registry that is the source of truth
  (`postgres.ex:103-121`): *"Every connection, including a reconnect after a
  crash, listens on all channels that have registered listeners."*
- Publish is `SELECT pg_notify($1, payload) FROM json_array_elements_text($2::json)`
  (`postgres.ex:93-97`) — one row per payload, so a batch is one round trip.
- **Payload is a wakeup signal, not job data.** `%{queue: q}` only, and only for
  queues that actually have `available` jobs (`lib/oban/engine.ex:425-429`,
  `lib/oban/stager.ex:126-135`).

### River (Go, MPL) — started with a trigger, deliberately removed it

Original design, `002_initial_schema.up.sql:61-82`:

```sql
CREATE OR REPLACE FUNCTION river_job_notify() RETURNS TRIGGER AS $$
BEGIN
  IF NEW.state = 'available' THEN
    payload = json_build_object('queue', NEW.queue);
    PERFORM pg_notify('river_insert', payload::text);
  END IF;
  RETURN NULL;
END; $$ LANGUAGE plpgsql;
CREATE TRIGGER river_notify AFTER INSERT ON river_job FOR EACH ROW ...
```

Dropped in `004_pending_and_more.up.sql:23-24`. The rationale is the single most
relevant paragraph in the whole survey (`CHANGELOG.md:920-924`):

> The initial design for River utilized a trigger on job insert that issued
> notifications (`NOTIFY`) … While this is good for lowering latency, it does
> have the side effect of emitting a large amount of notifications any time
> there are lots of jobs being inserted. This adds overhead, particularly to
> high-throughput installations.
>
> To improve this situation … the notifications have been refactored to be
> emitted at the application level. A client-level debouncer ensures that these
> notifications are not emitted more often than they could be useful. If a queue
> is due for an insert notification (on a particular Postgres schema), the
> notification is piggy-backed onto the insert query within the transaction.

And, separately, the channel-namespace bug they hit (`CHANGELOG.md:926`):
initial releases did not scope topics, so two River installs in different
schemas on one database collided on a global channel name. Fixed by prefixing
`{schema}.`, which is why `notifier.go:38-46` computes the longest topic name to
bound how long a custom schema name may be — *"Postgres enforces a maximum topic
length of 63 characters"*.

Current emission: `client.go:2188-2215`, gated by
`insertNotifyLimiter.ShouldTrigger(queue)` — a per-queue leaky-bucket keyed on
last-sent time (`internal/notifylimiter/limiter.go:31-43`), width =
`FetchCooldown` = 100 ms (`client.go:865`, `client.go:46`).

Channels: `river_control`, `river_insert`, `river_leadership`
(`internal/notifier/notifier.go:27-31`). Insert payload `{"queue": "..."}`
(`client.go:2202`). Control payload *does* carry data —
`{'action':'cancel','job_id':id,'queue':queue}` (`river_job.sql:51-54`) — but
that is a command, not a wakeup, and it is one row.

### Graphile Worker (TS, MIT) — also started with a trigger, also removed it

`sql/000011.sql:267-277` had `_900_after_insert … FOR EACH STATEMENT` calling
`pg_notify('jobs:insert','')`. Dropped in `sql/000018.sql:1-2`. NOTIFY now lives
inside the `add_job`/`add_jobs` SQL functions (`sql/000020.sql:86`, `:136`):

```sql
perform pg_notify('jobs:insert', '{"r":' || random()::text || ',"count":1}');
```

Payload is a **count hint plus a random nonce**. (Inference: the `r` field exists
to *defeat* the transaction-level dedup, so N `add_job` calls in one transaction
produce N wakeups instead of collapsing to one. That is the exact opposite of
what Stampede wants.)

Consequence of moving NOTIFY into the function: a raw
`INSERT INTO _private_jobs` no longer wakes anybody. Graphile can afford that —
its client library *is* `add_job`. **Stampede cannot** (`05-polyglot-protocol.md:22-27`).

Listener: dedicated client checked out of the pool
(`src/main.ts:544-546`, `// Create a client dedicated to listening for new jobs.`),
`LISTEN "jobs:insert"; LISTEN "worker:migrate";` (`src/main.ts:537`), reconnect
with jittered exponential backoff capped at 60 s (`src/main.ts:400-430`,
`src/main.ts:59-60`).

### pg-boss (JS, MIT) — NOTIFY as an explicit "latency hint", off by default

Tree read: `timgit/pg-boss` master `d63ae11`, 2026-09-16, v12.33.0. This is much
newer than the "pg-boss is pure polling" reputation; that reputation is stale.

Design statement, `src/notifier.ts:13-17`:

> Owns the LISTEN/NOTIFY listener lifecycle. A NOTIFY is only ever a latency
> hint: it wakes workers so they run their normal locking fetch sooner than the
> polling interval. If the listener can't be established (custom adapter,
> PgBouncer transaction pooling, dropped connection), fall back to polling after
> emitting a warning.

- Default **off**: `src/attorney.ts:507`, plus a second per-queue opt-in
  (`src/manager.ts:996`).
- Channel is derived, not literal: `'pgboss_' || left(encode(sha224(schema),'hex'),24)`
  (`src/plans.ts:856-858`) — explicitly to stay inside 63 bytes and to avoid
  cross-install collision in the global channel namespace. Same bug River hit.
- Payload = the queue name (`src/plans.ts:862-864`).
- Emitted **inside the INSERT statement**, gated on runnability
  (`src/plans.ts:2266-2284`): a `notified` CTE does
  `SELECT pg_notify(ch, name) FROM ins WHERE start_after <= job_now() LIMIT 1`,
  referenced from the final `WHERE` so the planner actually evaluates it.
  Future-dated jobs deliberately emit nothing.

### procrastinate (Python, MIT) — per-row trigger, two channels, still shipping

`procrastinate/sql/schema.sql:404-416`:

```sql
SELECT json_build_object('type','job_inserted','job_id', NEW.id)::text INTO payload;
PERFORM pg_notify('procrastinate_queue_v1#' || NEW.queue_name, payload);
PERFORM pg_notify('procrastinate_any_queue_v1', payload);
```

`FOR EACH ROW WHEN (new.status = 'todo')` (`schema.sql:567-570`) — the state gate
is on the **trigger**, not inside the function, so the plpgsql body is never
entered for irrelevant rows. Two channels: per-queue and a global fan-out. Payload
carries `job_id` but the worker re-queries anyway.

Note the v0→v1 migration renamed the channels
(`sql/migrations/03.00.00_01_pre_cancel_notification.sql:267-268`) — the third
project in this survey to have to fix its channel namespace after shipping.

Listener is a **standalone connection outside the pool**, `autocommit=True`
(`procrastinate/psycopg_connector.py:305-307`), reconnect loop at `:314-326`,
`LISTEN_TIMEOUT = 30.0` (`procrastinate/connector.py:13`) after which it issues
`SELECT 1` as a liveness probe and breaks to reconnect on `OperationalError`
(`psycopg_connector.py:331-353`).

### que 2.4.1 (Ruby, MIT) — the one design that *routes* the notification

Que is the outlier and the most interesting result in the survey. Its trigger
does not broadcast — it picks **one specific worker process** and notifies only
that process's private channel.

`lib/que/migrations/5/up.sql:36-45` selects a `locker_pid` from the `que_lockers`
registry where `listening AND queues @> ARRAY[NEW.queue] AND job_schema_version
matches`, sharded by `WHERE NEW.id % count = row_number` over
`ORDER BY md5(pid::text || id::text)`. Then `up.sql:65`:

```sql
PERFORM pg_notify('que_listener_' || locker_pid::text, sort_key::text);
```

The listener's channel is its own backend pid (`lib/que/listener.rb:11`:
`@channel = channel || "que_listener_#{connection.backend_pid}"`).

**This is a thundering-herd mitigation implemented entirely in SQL, at the
sender, with zero client cooperation.** N idle workers, one insert, exactly one
wakeup. Nothing else in the survey achieves that.

And Que states the payload-size rationale outright (`up.sql:48-52`):

> There's a size limit to what can be broadcast via LISTEN/NOTIFY, so rather
> than throw errors when someone enqueues a big job, just broadcast the most
> pertinent information, and let the locker query for the record after it's
> taken the lock. The worker will have to hit the DB in order to make sure the
> job is still visible anyway.

Payload is `{message_type, queue, priority, id, run_at}` (`up.sql:53-63`) —
metadata for a priority decision, deliberately **not** `klass` or `args`.
(The literal `8000` appears nowhere in `lib/`; the comment says "a size limit".)

Intervals: `DEFAULT_POLL_INTERVAL = 5.0`, `DEFAULT_WAIT_PERIOD = 50` ms,
`DEFAULT_MAXIMUM_BUFFER_SIZE = 8` (`lib/que/locker.rb:50-53`), both `listen:` and
`poll:` default `true` (`locker.rb:58-60`). One connection serves both LISTEN and
polling (`locker.rb:196-216`). Listener thread gets `Thread.current.priority = 1`
— *"Give this thread priority, so it can promptly respond to NOTIFYs"*
(`locker.rb:185-186`).

### good_job (Ruby) — no trigger at all, and the connection accounting is explicit

Application-level NOTIFY, not a trigger: `NOTIFY good_job, <json>` through
`GoodJob::Job.connection_pool.with_connection` (`lib/good_job/notifier.rb:56-62`).
Single channel `good_job`, `WAIT_INTERVAL = 1`, `RECONNECT_INTERVAL = 5`,
`KEEPALIVE_INTERVAL = 10` (`notifier.rb:23-32`).

The connection handling is the part worth stealing (`notifier.rb:257-269`):

```ruby
self.connection = ::GoodJob::Job.connection_pool.checkout.tap do |conn|
  ::GoodJob::Job.connection_pool.remove(conn)
end
connection.execute("SET application_name = #{connection.quote(self.class.name)}")
```

It **permanently removes** the connection from the pool rather than holding a
checkout, and names it in `pg_stat_activity`. The cost is documented as a number
(`README.md:1237-1239`): 1 connection per execution thread plus *"2 additional
connections that GoodJob uses for utility functionality (e.g. LISTEN/NOTIFY,
cron, etc.)"*.

Two separate readiness signals — `connected?` ("has acquired a dedicated database
connection", `notifier.rb:98-104`) and `listening?` ("LISTEN actually issued",
`:109-115`) — which is the right granularity: a pooler failure leaves you
connected but not listening.

Fallback is capability-based, not error-based (`notifier.rb:273`, `:291-292`): if
`raw_connection` does not `respond_to?(:wait_for_notify)`, the loop degrades to
`sleep WAIT_INTERVAL`.

Poller is fully independent: `DEFAULT_POLL_INTERVAL = 10` seconds
(`lib/good_job/configuration.rb:19`), a plain `Concurrent::TimerTask`, **no
jitter** (`lib/good_job/poller.rb:12-15`).

### solid_queue (Ruby, MIT) — pure polling, and the interval is adaptive

No `pg_notify` anywhere. Defaults (`lib/solid_queue/configuration.rb:21-39`):
Worker **0.1 s**, Dispatcher **1 s**, Scheduler **5 s**.

The poll method *returns the next sleep duration*, which is genuine adaptivity —
the only clean example in the survey:

- Worker: `pool.idle? ? polling_interval : 10.minutes` (`lib/solid_queue/worker.rb:36-44`)
  — saturated pool sleeps ten minutes and waits to be woken.
- Dispatcher: `batch.zero? ? polling_interval : 0.seconds` (`lib/solid_queue/dispatcher.rb:36-40`)
  — drain at full speed while batches come back full.
- Scheduler: `dynamic_tasks_enabled? ? polling_interval : 60` (`lib/solid_queue/scheduler.rb:31,69`).

And the wakeup primitive is an in-process self-pipe, not the database
(`lib/solid_queue/processes/interruptible.rb:10-32`), wired to pool idleness at
`worker.rb:25` (`on_idle: -> { wake_up }`). Structurally the same idea as LISTEN,
scoped to one OS process — no cross-process wakeup exists at all.

No jitter on any interval.

### underway (Rust, Apache) — per-row trigger, 1-minute poll, `tokio::select!`

Closest to our stack. `migrations/20241024174106_1.sql:5-22`:

```sql
if (new.state = 'pending') then
  perform pg_notify('task_change',
    json_build_object('task_queue_name', new.task_queue_name)::text);
```

`after insert or update ... for each row` (`:19-22`). Payload is the queue name
only; the worker filters on it (`src/worker.rs:678-690`) and re-queries.

Note the guard is **inside the function**, not a trigger-level `WHEN` — so the
plpgsql body runs on every task UPDATE, including every claim and completion, and
only the `if` suppresses the notify. procrastinate's trigger-level `WHEN`
(`schema.sql:567-570`) is strictly cheaper. (Inference, but it follows from the
per-row measurement above.)

Poll default is **1 minute** (`src/worker.rs:524-526`), documented at `:471-472`:
*"Tasks are processed via a subscription to a Postgres channel and polling in a
loop. A one-minute sleep occurs between polls."* Both race in one `tokio::select!`
(`src/worker.rs:577-641`), with the poll arm literally commented
`// Pending task polling fallback` (`:635-638`) and listener-error reconnect at
`:628-631`.

Shutdown uses a per-process randomised channel,
`underway_shutdown_{uuid}` (`src/queue.rs:2179-2181`) — a clean way to address
one process without a registry table, at the cost of the address not surviving a
restart.

---

## 2. The known problems, and where each is worked around

### (a) 8000-byte payload cap

**Nobody sends job data.** Every implementation read sends a wakeup token:

| impl | payload | bytes |
|---|---|---|
| Oban | `{"queue":"default"}` | ~20 |
| River | `{"queue": "default"}` | ~21 |
| pg-boss | `default` | ~7 |
| Graphile Worker | `{"r":0.83…,"count":1}` | ~30 |
| procrastinate | `{"type":"job_inserted","job_id":…}` | ~60 |
| underway | `{"task_queue_name":"default"}` | ~30 |
| que | `{message_type,queue,priority,id,run_at}` | ~110 |
| good_job | queue name + `scheduled_at` JSON | ~60 |

**Que is the only project in the survey that documents why**
(`lib/que/migrations/5/up.sql:48-52`): it carries enough metadata for a priority
decision and nothing more, because *"rather than throw errors when someone
enqueues a big job, just broadcast the most pertinent information, and let the
locker query for the record after it's taken the lock."* Every implementation
re-queries after waking; none of them trusts the payload as the job.

Oban is the only one that *can* carry a large payload — user-facing
`Oban.Notifier.notify/3` accepts arbitrary maps for `signal`/custom channels —
and it deals with the cap by compressing above 512 bytes
(`lib/oban/notifier.ex:339-353`):

```
@compress_threshold 512
"Z:" <> Base.encode64(:zlib.gzip(iodata))
```

with a decoder that still accepts the legacy `H4sI` gzip-magic form
(`notifier.ex:363-369`). Note base64 inflates 4/3, so the effective pre-compression
budget is ~5999 bytes of gzipped data.

**Reading: the cap is a non-issue if and only if the payload is a routing key.**
The moment a payload carries job args, you inherit a compression scheme, a legacy
decoder, and a size class of jobs that silently cannot be notified.

### (b) NOTIFY fires on COMMIT — long transactions

Verified above: 4-second transaction → 4-second delivery delay. Oban documents
the flip side as a *feature* (`lib/oban/notifiers/postgres.ex:23-26`):

> Notifications are only delivered **after a transaction completes** and are
> de-duplicated before publishing. This means that notifications sent during a
> transaction will not be sent if the transaction is rolled back, providing
> consistency; this is the only notifier which provides that guarantee.

The practical fallout is in testing, not production: Ecto sandbox mode wraps each
test in a rolled-back transaction, so **no notification ever fires under test**
(`postgres.ex:30-35`). Oban ships `Oban.Notifiers.Isolated`
(`lib/oban/notifiers/isolated.ex`) purely to relay in-process during tests.

Second-order hazard (**documented behaviour, not executed here**): the 8 GB async
queue (`max_notify_queue_pages = 1048576`, verified) cannot be trimmed past the
oldest LISTENing backend's read position. A backend that is LISTENing but stuck
in a long transaction pins the queue; if it fills, transactions issuing NOTIFY
start failing. `pg_notification_queue_usage()` exists and returned `0` — that is
the monitoring hook, and nothing in the surveyed implementations monitors it.

### (c) It does not survive transaction-mode poolers

This is the one that actually bites, and the responses differ enormously.

**Oban — detection, then automatic degradation.** This is the best design read
in the survey. `Oban.Sonar` (`lib/oban/sonar.ex`) listens on `:sonar` and
periodically NOTIFYs its own node name (`sonar.ex:50-56`, `:84`). It then
classifies (`sonar.ex:153-168`):

- no nodes seen → `:isolated` (its own ping did not come back — pooler eats NOTIFY)
- only itself → `:solitary`
- others → `:clustered`

Ping interval adapts: 5 s minimum, 60 s maximum, doubling toward a
status-dependent target, reset to minimum on any change, jittered
(`sonar.ex:14-17`, `:127-151`).

The Stager consumes that status every tick (`lib/oban/stager.ex:155-169`):

```
:clustered -> :global
:isolated  -> :local
:solitary  -> if leader, :global, else :local
```

`:global` = leader notifies queues over pubsub. `:local` = every producer polls
on its own (`stager.ex:137-145`, sending the same `{:notification, :insert, …}`
message in-process). There is also a belt-and-braces path: if the staging
transaction itself throws, it force-notifies, falling back to `:local` on
exception (`stager.ex:106-124`), and `notify_queues/1` demotes to `:local` if
`Notifier.notify` returns an error (`stager.ex:131-134`).

Documented, with the operator-visible symptom (`guides/advanced/troubleshooting.md:105-120`):

> ## Database Queries from Every Queue Every Second
> If every queue is polling the database every second, job staging has switched
> from efficient **global mode** to **local mode**. … 2. Using PgBouncer with
> transaction pooling (disables pubsub notifications)

and the log line to grep for:
`"job staging switched to local mode. local mode polls for jobs for every queue"`.

The second-order cost is also documented: leadership handover goes from
immediate to "up to 30 seconds" when the leader's resignation broadcast cannot
travel (`guides/learning/clustering.md:34-40`).

**River — an explicit opt-out flag, no detection.** `Config.PollOnly`
(`client.go:309-322`):

> The upside is that it makes River compatible with systems where listen/notify
> isn't available. For example, PgBouncer in transaction pooling mode.
> … A new job in the queue may have to wait up to FetchPollInterval to be locked
> for work. When a leader resigns, it will be up to five seconds before a new one
> elects itself.

Also an escape hatch for mixed setups: `NewWithPgxListener(dbPool, listenerPool)`
puts *only* the listener on a second pool
(`riverdriver/riverdatabasesql/river_database_sql_driver.go:56-77`) —
*"Applications using PgBouncer must configure the listener pool to use session
pooling or connect it directly to Postgres."*

**pg-boss — detection with a typed warning event.** Three fallback branches
(`src/notifier.ts:43-77`) emitting `listen_notify_unavailable`, plus a
compatibility flag `noListenNotify` (`src/attorney.ts:23`) honoured
producer-side so the inlined `pg_notify` is never even emitted
(`src/manager.ts:1214-1218`). Plus a **liveness probe** nobody else has —
every 10 s it asks the listening connection whether its own subscription still
exists (`src/db.ts:180-213`):

```sql
SELECT EXISTS (SELECT 1 FROM pg_listening_channels() AS active(channel)
               WHERE channel = $1) AS listening
```

and throws `'LISTEN/NOTIFY channel registration was lost'` (`src/db.ts:209`).
That catches the silent-subscription-loss case where the TCP connection survives
but the LISTEN registration does not.

**Graphile Worker — a candid FAQ and nothing else** (`website/docs/faq.md:9-20`):

> Yes, it's on by default. Yes, pgbouncer _might_ pose an issue if it's not done
> in "connection" mode. We could probably disable it and do polling only (but we
> don't currently have an option for that) …

No detection, no flag, no fallback toggle. (Inference: it degrades silently to a
2-second-worst-case polling queue, which is survivable precisely *because*
polling was never turned off.)

**good_job — a documented four-setting recipe.** `README.md:1570`:

> GoodJob is not compatible with PgBouncer in _transaction_ mode, but is
> compatible with PgBouncer's _connection_ mode. GoodJob uses connection-based
> advisory locks and LISTEN/NOTIFY, both of which require full database
> connections.

and the escape hatch at `README.md:1547-1556`: switch `lock_strategy` to
`:skiplocked`, then *"you must also disable the `LISTEN/NOTIFY` notifier (which
requires a persistent connection) and rely on polling instead"* —
`enable_listen_notify = false`, `advisory_lock_heartbeat = false`,
`poll_interval = 5`. Toggle at `lib/good_job/configuration.rb:366-372`.
Note the coupling: good_job's pooler problem is *advisory locks* as much as
NOTIFY. Stampede does not have that problem — `04-…` claims with
`FOR UPDATE SKIP LOCKED`, not advisory locks, so only the notifier is at risk.

**procrastinate — a flag, and no claim about poolers.** `listen_notify: False`
worker default / `--no-listen-notify` (`docs/howto/production/connections.md:7-19`),
justified by connection count rather than pooler compatibility. The pooler
documentation (`docs/howto/basics/connector.md:111-119`) recommends a null pool
under PgBouncer without ever stating that LISTEN breaks there.

### (d) A LISTEN connection cannot be pooled

Universal mitigation: **one listener connection per process, fanned out
in-process** — never one per worker.

- River hijacks exactly one connection from a dedicated pool and never returns
  it (`river_database_sql_driver.go:68-74`): *"Listener connections are hijacked
  from listenerPool and never returned. A pool dedicated to one River client can
  generally set MinConns to zero and MaxConns to one. Each concurrently running
  client still needs its own listener connection."* Producers subscribe
  in-process via `Notifier.Listen(ctx, topic, callback)` (`producer.go:372`).
- Oban runs one `Postgrex.SimpleConnection` per Oban instance; producers call
  `Notifier.listen(conf.name, [:insert, :signal])` (`producer.go:64`) and are
  relayed to by `send/2` (`notifier.ex:297-305`).
- Graphile Worker: one client per worker pool (`src/main.ts:544-546`).
- procrastinate opens a connection *outside* the pool entirely
  (`psycopg_connector.py:305-307`), and documents the saving from turning it off:
  *"Disabling the `LISTEN/NOTIFY` feature … will use one less connection per
  worker"* (`docs/howto/production/connections.md:7-13`).
- good_job checks out and then **removes** the connection from the ActiveRecord
  pool (`lib/good_job/notifier.rb:257-269`), so it cannot be accidentally handed
  to a query, and sets `application_name` so it is identifiable in
  `pg_stat_activity`.
- que is the exception: **one connection serves both LISTEN and polling** in the
  locker thread (`lib/que/locker.rb:196-216`), which works because the listen
  block is only 50 ms and polling happens between blocks.

Cost at scale: N application processes = N permanent backends *before any work
connection*. `03-postgres18.md:93-96` records the dev instance at
`max_connections=100`. 40 worker processes would spend 40 % of the connection
budget on doing nothing. (Inference, but arithmetic.) good_job budgets it
explicitly at *"2 additional connections … for utility functionality (e.g.
LISTEN/NOTIFY, cron, etc.)"* per process (`README.md:1237-1239`).

### (e) Thundering herd

Three distinct mitigations exist; they are not equivalent.

**0. Address exactly one worker — que only.** The trigger consults the
`que_lockers` registry, shards by `NEW.id % count` over an
`md5(pid || id)`-ordered list of processes that are `listening` and subscribed to
that queue, and notifies `que_listener_<that pid>`
(`lib/que/migrations/5/up.sql:36-45`, `:65`). N idle workers, one insert, **one**
wakeup. No client cooperation, no debouncer, no jitter — the herd is prevented
rather than absorbed. Nothing else in the survey does this.

Its failure mode is the mirror image (inference, consistent with
`locker.rb:287-295`): the `listening` flag is written once at registration and
never updated, so a saturated or dying-but-unreaped locker still receives the
notification, drops it (`job_buffer.accept?`, `lib/que/job_buffer.rb:74-80`), and
the job waits out that locker's 5-second poll instead of being re-routed.
**Unicast trades tail latency for herd elimination.**

**1. Scope the channel/payload so only interested workers wake.** Every
implementation does this. River's producer drops the notification if
`decoded.Queue != p.config.Queue` (`producer.go:365-367`) — but note the wake
has already happened at the OS level; only the fetch is suppressed. Oban
likewise (`producer.go:131-135`), underway likewise (`src/worker.rs:678-690`).
procrastinate goes further and puts the queue name in the **channel**
(`procrastinate_queue_v1#<queue>`, `schema.sql:411`) so the uninterested worker
is never woken at all — cheaper than payload filtering, at the cost of one
`LISTEN` per subscribed queue.

**2. Debounce the fetch.** Oban's producer has `dispatch_cooldown: 5` ms
(`producer.go:20`) and an idempotent scheduler (`producer.go:235-244`):

```
if is_reference(state.dispatch_timer) do state   # already scheduled: drop
else Backoff.jitter(state.dispatch_cooldown, mode: :both, mult: 1) ... end
```

Oban documents the throughput consequence directly (`lib/oban.ex:525-530`):

> average jobs per second for a single queue is approximately
> `(1000 / cooldown) * limit`. For example, with a `5ms` cooldown and a queue
> limit of `25` a single queue averages 5,000 jobs/sec.

River's equivalent is `DebouncedChan` (`internal/util/chanutil/debounced_chan.go:9-12`):
*"a channel that will only fire once per cooldown period, at the leading edge.
If it is called again during the cooldown, the subsequent calls are delayed until
the cooldown period has elapsed and are also coalesced into a single call."*
Leading-edge-plus-trailing-edge, width `FetchCooldown` = 100 ms.

**3. Throttle the sender.** Only River. `notifylimiter` (above) caps NOTIFY
emission at one per queue per 100 ms *at the producer of the notification*, which
is strictly better than debouncing at 50 receivers — the message is never sent.

**4. Only wake idle workers.** Graphile Worker's `nudge` walks the pool and stops
at the first genuinely idle worker (`src/main.ts:788-796`,
`src/worker.ts:109-119`); a busy worker returns `false` and the walk continues.
Count-scaled: a `count:1` notification wakes exactly one worker.

**5. Jitter the polling phase.** River (`producer.go:634-637`):

> jitteredFetchPollInterval returns FetchPollInterval with random jitter in
> [0, 10% of FetchPollInterval) added (minimum 10ms). This prevents multiple
> producers from synchronizing their fetches after a transient event (e.g. GC
> pause, network blip), which would cause periodic DB load spikes.

asynq jitters fully: `time.Sleep(interval/2 + rand.N(interval))` (`processor.go:184-185`).
Oban jitters both the dispatch cooldown (`producer.go:239`) and the refresh
(`producer.go:247`).

**Everybody else does not.** Graphile Worker does not jitter `pollInterval` at
either call site (`src/localQueue.ts:718`, `src/worker.ts:222`); good_job uses a
fixed `Concurrent::TimerTask` interval (`lib/good_job/poller.rb:12-15`);
solid_queue uses the interval verbatim; procrastinate uses a bare
`asyncio.sleep`; underway a bare `tokio::time::interval`. Pools started together
poll in phase forever. Graphile *does* jitter the `resetLocked` sweep for exactly
this reason (`src/main.ts:384-389`): *"not immediately because we don't want to
cause a thundering herd"* — which makes the omission on the hot path look like an
oversight rather than a decision.

**6. Deliberately ignore notifications while backing off.** Graphile Worker's
`refetchDelay` is the most developed anti-herd design found. Rationale
(`src/index.ts:315-324`):

> When running at very high scale … each time a new job comes in, each pool may
> request it, multiplying up the load. To reduce this impact, when a pool
> receives no (or few) results … we can instigate a 'refetch delay' to cause the
> pool to wait before issuing its next poll for jobs, **even when new job
> notifications come in**.

With doubled jitter — on the delay itself (`src/localQueue.ts:669-670`,
`(0.5 + Math.random()) * durationMs`) and on the abort threshold
(`src/localQueue.ts:674-681`), justified at `src/index.ts:350-377`:

> because you may have many different workers in refetch delay we take a random
> number up to this threshold, this means that different workers will abort
> refetch delay at different times which a) helps avoid the thundering herd
> problem, and b) helps to reduce the latency of executing a new job when all
> workers are in refetch delay.

Caveat: `refetchDelay` only applies when `localQueue` is enabled, and
`localQueue.size` defaults to `-1` (`src/main.ts:586`, `:1208-1216`) — so **the
whole mechanism is off by default.**

**Nobody mitigates herd in pg-boss**: `Manager.notifyQueue(name)`
(`src/manager.ts:1223-1229`) wakes every worker on the queue with no idle guard
and no jitter. The mitigation is structural — `SKIP LOCKED` means losers just get
an empty result.

---

## 3. Pure polling

### asynq (Go, MIT) — Redis, and *chose* polling over a blocking primitive

`processor.go:176-187`:

```go
case errors.Is(err, errors.ErrNoProcessableTask):
    // Queues are empty, this is a normal behavior.
    // Sleep to avoid slamming redis and let scheduler move tasks into queues.
    // Note: We are not using blocking pop operation and polling queues instead.
    // This adds significant load to redis.
    jitter := rand.N(p.taskCheckInterval)
    time.Sleep(p.taskCheckInterval/2 + jitter)
```

`defaultTaskCheckInterval = 1 * time.Second` (`server.go:414`),
`defaultDelayedTaskCheckInterval = 5 * time.Second` (`server.go:420`).

Sleep is uniform in `[0.5 s, 1.5 s)` — mean 1 s, **full jitter, no adaptivity**.
Not exponential; it does not back off further when idle. The only "adaptive"
behaviour is that the sleep is skipped entirely when a task *was* found, so a
busy queue runs a tight loop.

This is a useful data point because Redis *has* `BRPOPLPUSH` and asynq declined
to use it. (Inference: with N queues and priority ordering, a blocking pop on one
key does not compose; they traded latency for priority correctness.)

### River poll-only mode

`FetchPollIntervalDefault = 1 * time.Second`, min 1 ms (`client.go:49-50`).
Jittered `[interval, interval * 1.1)` (`producer.go:634-637`). Not adaptive by
interval — adaptivity is on the *cooldown* side instead (below).

### solid_queue (Ruby, MIT) — the only genuinely adaptive interval

No NOTIFY at all; cross-process wakeup does not exist. But the poll loop returns
its own next sleep (`lib/solid_queue/processes/poller.rb:24-33`), and that return
value is state-dependent:

```
Worker:     pool.idle? ? polling_interval : 10.minutes     # worker.rb:36-44
Dispatcher: batch.zero? ? polling_interval : 0.seconds     # dispatcher.rb:36-40
Scheduler:  dynamic_tasks_enabled? ? polling_interval : 60 # scheduler.rb:31,69
```

Tighten-when-busy (`0.seconds` on a full batch) and back-off-when-blocked
(`10.minutes` when the thread pool is saturated) in four lines. The 10-minute
sleep is safe only because the in-process self-pipe
(`lib/solid_queue/processes/interruptible.rb:10-32`) interrupts it the instant a
thread frees up (`worker.rb:25`, `on_idle: -> { wake_up }`).

Worker interval default is **0.1 s** (`lib/solid_queue/configuration.rb:21`) —
10× tighter than anyone else, which is what you set when polling is your *only*
wakeup. Note the generator template ships `1` instead
(`lib/generators/solid_queue/install/templates/config/queue.yml:3`).

This is the cleanest prior art for adaptive polling, and it is worth noting it
comes from the project with no notification mechanism at all.

---

## 4. Hybrid — notify with polling fallback

Every serious implementation is a hybrid. **Nobody turns polling off.** The two
never double-process because neither is the correctness mechanism: both merely
*trigger a claim*, and the claim is `FOR UPDATE SKIP LOCKED` (`04-…:12-29`).
A duplicated wakeup costs one wasted query, not one wasted execution. That is
the whole reconciliation story, and it is why the hybrid is cheap.

**Nine of nine implementations that notify also poll unconditionally.** Oban
(stager, 1 s), River (`fetchPollLoop`, 1 s), Graphile Worker (2 s), pg-boss
(2 s / 30 s), que (`poll: true` default, `locker.rb:58-60`), procrastinate (5 s,
*"Worker polls the database regardless of this setting"*, `app.py:325`), good_job
(independent `Poller`, 10 s), underway (`// Pending task polling fallback`,
`src/worker.rs:635-638`). solid_queue skips NOTIFY entirely. There is no
production-grade counter-example.

Four shapes:

**River — poll is a stated backup, cooldown is the real regulator.**
`producer.go:76-84`:

> FetchCooldown is the minimum amount of time to wait between fetches of new
> jobs. Jobs will only be fetched *at most* this often, but if no new jobs are
> coming in via LISTEN/NOTIFY then fetches may be delayed as long as
> FetchPollInterval.

and `producer.go:606-610`:

> Loops every FetchPollInterval to check for jobs. This is meant as a back up in
> case something with listen/notify didn't work, or the fetch limiter was
> limited so there's still jobs to pick up, and it's also important in poll-only
> mode.

Both the notification handler and the poll timer call the *same*
`fetchLimiter.Call()` (`producer.go:369`, `:624`). One debouncer, two triggers —
so notify and poll cannot compound. River also self-accelerates: a full batch
sets `fetchWhenSlotsAreAvailable`, which re-triggers the limiter the moment a
slot frees (`producer.go:618-622`, `:594-601`), documented at `CHANGELOG.md:613`
as a deliberate throughput-for-load trade.

**Oban — the poll is the stager, and it is the only thing that can stage.**
`Stager` ticks every 1 s (`lib/oban/stager.ex:16`), moves `scheduled → available`,
then notifies only the queues that have work (`stager.ex:92-135`). Insert-time
notification is a separate, *optional* accelerant — `insert_trigger`, default
`true` (`lib/oban/config.ex:33`), documented at `lib/oban.ex:533-541`:

> The trigger mechanism is designed to make jobs execute immediately after
> insert, rather than up to the stager's `:interval` (1 second) afterwards, and
> it can safely be disabled to improve insert throughput.
> … At high load, e.g. thousands or more job inserts per second, notifications
> may become a bottleneck.

**pg-boss — NOTIFY relaxes the poll rather than replacing it.** Base poll 2000 ms
(`src/attorney.ts:717-719`, floor 500 ms at `:8`); when NOTIFY is live the poll
stretches to 30 000 ms (`src/attorney.ts:729-735`), never off. Stated at
`docs/api/workers.md:294`:

> This is an **opt-in optimization on top of polling, not a replacement for it.**
> Polling always keeps running as a safety net, so jobs are never lost if a
> notification is missed.

Three-tier interval resolution (`src/manager.ts:1001-1019`): burst (0 ms) when
the last fetch filled the batch, else notify-backstop (30 s), else base (2 s).
The `fullBatch` requirement is the anti-hot-loop guard. Plus
`forceFetchLnWorkers()` on listener reconnect (`src/manager.ts:1233-1239`) to
close the gap window.

**underway / procrastinate — race them in one select.** underway puts both in a
single `tokio::select!` (`src/worker.rs:577-641`), procrastinate in a
`wait_any(notification, sleep(interval), stop)` (`procrastinate/worker.py:694-697`)
where the notification handler just sets an `Event` (`worker.py:473-477`). No
debouncer at all — whichever fires first triggers a claim, and `SKIP LOCKED`
absorbs the duplicate. This is the minimum viable hybrid, and it is the one to
copy if the GCRA turns out to be over-engineering. underway's 60-second poll is
the loosest backstop in the survey, which is only defensible because the notify
path is a trigger that cannot be bypassed.

---

## 5. Latency vs load, as claimed by the repos

| impl | claimed / configured enqueue→start | idle load |
|---|---|---|
| Graphile Worker | **4.16 ms mean, 13.84 ms max**, ~183 k jobs/s executed (`website/docs/performance.md:8-12`, config at `:17-30`) | 2 s poll per pool, unjittered, *plus* LISTEN |
| Oban | notify path is immediate; without it, "up to the stager's `:interval` (1 second)" (`lib/oban.ex:536-538`). Throughput ceiling `(1000/cooldown) * limit` (`lib/oban.ex:525-528`) | global mode: **1 query/s total** (leader only). local mode: **1 query/s per queue per node** (`troubleshooting.md:105-110`) |
| River | notify path bounded below by `FetchCooldown` 100 ms; poll-only "may have to wait up to FetchPollInterval" = 1 s (`client.go:316-318`) | 1 query/s/producer, jittered +0–10 % |
| pg-boss | notify → next fetch immediately; otherwise 2 s, or 30 s when notify is live (`attorney.ts:719`, `:734`) | 0.5 polls/s/worker, or 0.033/s with notify |
| asynq | mean 1 s, worst 1.5 s (`processor.go:184-185`) | 1 poll/s/processor, fully jittered |
| que | notify → next 50 ms listen block; else 5 s poll (`locker.rb:50-53`) | 0.2 polls/s/locker, unicast NOTIFY |
| procrastinate | notify immediate; else 5 s (`worker.py:29`) | 0.2 polls/s/worker, no jitter |
| good_job | notify → ≤1 s (`WAIT_INTERVAL`); else 10 s (`configuration.rb:19`) | 0.1 polls/s/process, no jitter |
| solid_queue | no notify; **0.1 s** worker interval (`configuration.rb:21`) | 10 polls/s/worker, no jitter |
| underway | notify immediate; else **60 s** (`src/worker.rs:524-526`) | 0.017 polls/s/worker |

Note the shape: **the notify path's floor is the debounce, not the network.**
River cannot beat 100 ms without retuning `FetchCooldown`; Oban's 5 ms cooldown is
why it can claim 4-digit jobs/sec per queue. Graphile's 4 ms figure is measured
with `localQueue.size: 500` and `completeJobBatchDelay: 0`, i.e. batching on —
and it warns that batching *hurts* latency in the sparse case
(`website/docs/performance.md:100-102`): "execution latency may increase if jobs
exist in one worker's local queue whilst another worker sits idle."

Also note: **nobody's polling number is large.** 1 query/s/producer against a
partial index is 0.02 ms of database time (measured above). The load argument
against polling is about round-trips and pooled-connection churn at high process
counts, not about the database.

---

## The tradeoff, condensed

| option | enqueue→start | idle DB load | works behind txn pooler | works for anonymous `INSERT` | herd | cost to us |
|---|---|---|---|---|---|---|
| **poll only** | ½·interval mean, 1·interval worst | 0.02 ms × workers × queues × rate | yes | yes | mitigated by jitter alone | none; already needed |
| **client-side NOTIFY** (River, pg-boss, Graphile, good_job) | ms | ~zero | no (detect + degrade) | **no** | sender-side limiter possible | **disqualified** by `05-…:22-27` |
| **per-row trigger NOTIFY** (procrastinate, underway, River v0) | ms | ~zero | no (detect + degrade) | yes | dedup + receiver debounce | **+71 % bulk insert** (measured) |
| **statement-level trigger NOTIFY** | ms | ~zero | no (detect + degrade) | yes | dedup + receiver debounce | **+1.2 % bulk insert** (measured) |
| **unicast trigger NOTIFY** (que) | ms; full poll interval on a stale target | ~zero | no (detect + degrade) | yes | **eliminated at source** | registry read on hot insert path; routing policy in plpgsql |
| **statement trigger + poll floor** ← recommended | ms; degrades to 1 s | 0.02 ms × workers × queues | **yes, degraded** | yes | dedup + GCRA | +1.2 % insert, 1 conn/daemon, a sonar channel |

Rows 2–5 all require row 1 underneath them. Nothing in the survey ships a notify
path without a poll floor, and the reason is not caution: `SKIP LOCKED` makes a
redundant wakeup cost one empty query, so the floor is nearly free while a missed
wakeup is a stuck job.

## What Stampede should do

### The constraint that decides it

`05-polyglot-protocol.md:22-27`: enqueue is a plain `INSERT` from any language,
and *"Every language already has a client, because every language already has a
Postgres driver."* That kills two of the three options outright:

- **Client-side NOTIFY (River/pg-boss/Graphile/good_job current design) is
  unavailable.** All four moved NOTIFY into application code or into an
  `add_job()` function. Both require the enqueuer to run our code. A Ruby script
  doing `INSERT INTO stampede_jobs …` runs neither.
- **NOTIFY inside `stampede_claim()` is nonsense** — that is the consumer side.

Leaving: a trigger, or polling, or both.

The survey splits cleanly along exactly this line. **The four projects that kept
a trigger (que, procrastinate, underway, and Graphile before 000018) are the four
whose enqueue path the library does not control or does not want to control.**
The four that moved NOTIFY into client code all ship a client library that is the
only supported way to enqueue. Stampede is structurally in the first group and
should stop trying to inherit the second group's conclusions.

### Recommendation

**Poll as the correctness mechanism. Add a statement-level NOTIFY trigger as a
latency accelerant that can be dropped with one DDL statement.**

Concretely:

1. **Poll is never off.** Default `fetch_poll_interval = 1 s`, jittered
   `[t, t·1.1)` (River's constant, River's jitter — `producer.go:634-637`).
   Measured cost: 0.02 ms/poll. This is the safety net and the only thing that
   has to be correct.

2. **One trigger, statement-level, transition table:**

   ```
   AFTER INSERT ON stampede_jobs
     REFERENCING NEW TABLE AS inserted
     FOR EACH STATEMENT
   ```
   emitting `pg_notify('<schema>.stampede_insert', queue)` over
   `SELECT DISTINCT queue FROM inserted WHERE state='available' AND scheduled_at <= now()`.

   Measured: **+1.2 % on a 50 k-row insert**, versus +71 % for the per-row form
   River and procrastinate shipped. River's stated reason for ripping the trigger
   out (`CHANGELOG.md:922`) was *"emitting a large amount of notifications any
   time there are lots of jobs being inserted"* — and that is a per-row-trigger
   problem. Statement-level plus transaction-level dedup means a 50,000-row
   insert emits one notification per distinct queue. **River's objection does not
   transfer to the statement-level form.** This is the single finding that lets
   us keep the trigger.

   The `scheduled_at <= now()` gate is pg-boss's (`src/plans.ts:2281`): a
   future-dated job has nothing to wake anybody for; the scheduler handles it.

   Put the state gate on the **trigger** (`WHEN (new.state = 'available')`), not
   inside the function — procrastinate's form (`schema.sql:567-570`), not
   underway's (`20241024174106_1.sql:19-22`, guard inside the body, so the
   plpgsql body runs on every claim and every completion too).

   **Attach it to the partition(s) that hold `available`, not to the parent**, and
   attach it to the parent as well. Verified above: direct-to-partition inserts
   bypass a parent-only trigger, and duplicate parent+partition triggers still
   deliver exactly one notification because the payload has no nonce. Belt and
   braces costs nothing here and closes the `INSERT INTO stampede_jobs_available`
   hole. If `13-retention-and-bloat.md` changes the partition key, this needs
   re-deriving — note it there.

3. **Payload is a routing key, never job data.** The queue name, bare. No JSON,
   no compression scheme, no legacy decoder. 8000 bytes stops being a
   consideration; the transaction-level dedup does the throttling for free; and
   duplicate triggers become harmless (verified above). Explicitly *not*
   Graphile's `{"r":random()}` nonce (`sql/000020.sql:86`), whose entire purpose
   is to defeat that dedup, and which would break both properties. Que's
   metadata payload (`migrations/5/up.sql:53-63`) is the other end of the
   spectrum and only pays off because Que routes to one worker that needs to make
   a priority decision before claiming — we broadcast, so there is nothing to
   decide.

4. **Channel name is `{schema}.stampede_insert`.** Both River (`CHANGELOG.md:926`)
   and pg-boss (`src/plans.ts:841-858`) shipped the un-namespaced version first
   and had to fix it. The channel namespace is global per database; two Stampede
   installs in different schemas will collide. Budget: 63 bytes total
   (verified), so the schema name is capped at `63 - len('.stampede_insert')` = 46.

5. **This trigger is the narrow exception `03-postgres18.md:81-86` permits.**
   That section says triggers fight `state-machines` because lifecycle rules end
   up in two places. This trigger enforces no lifecycle and makes no decision —
   it reads a column and emits a routing key. It is "what the database alone can
   do atomically" in the exact sense used there: *only* the database knows an
   anonymous INSERT happened. The `DROP TRIGGER` escape hatch must be documented
   as supported, not as breakage; `GRANT`ing enqueue does not grant knowledge of
   our wakeup protocol.

6. **One listener connection per daemon, fanned out in-process.** Never one per
   worker. River's `MinConns 0 / MaxConns 1` hijack model
   (`river_database_sql_driver.go:68-74`) is the shape. With
   `max_connections = 100` on the dev box (`03-postgres18.md:93`), per-worker
   LISTEN is not affordable.

7. **Detect isolation, do not require configuration.** Oban's Sonar
   (`lib/oban/sonar.ex:50-56`, `:153-168`) is the right idea and pg-boss's
   `pg_listening_channels()` probe (`src/db.ts:190-196`) is the right
   implementation. Combine: NOTIFY our own daemon id on a `stampede_sonar`
   channel; if it does not come back, log once, set status, and stop relying on
   notifications. Do not offer a `PollOnly` flag as the *primary* answer — River
   does, and it requires the operator to already know their pooler eats NOTIFY.
   Ship the flag as an override, not as the mechanism.

8. **`throttle-machines` paces wakeups — receiver side, per (queue, daemon).**
   `Gcra::check(tat, now, GcraParams { emission_interval, delay_tolerance })`
   returns `Decision { allowed, state, retry_after }`
   (`ext/throttle_machines_native/core/src/gcra.rs:28-45`,
   `src/gate.rs:7-15`). That maps exactly: `allowed=false` → do not fetch,
   arm a timer for `retry_after`. `delay_tolerance` is the burst allowance,
   which is precisely Oban's leading-edge dispatch and River's `sendLeading`
   (`debounced_chan.go:29-31`) expressed as a parameter instead of a boolean.

   `emission_interval = 5 ms` matches Oban's `dispatch_cooldown`
   (`producer.go:20`) and its documented ceiling of `(1000/5)·limit` jobs/s
   (`lib/oban.ex:525-528`). River's 100 ms is 20× more conservative; start at
   Oban's number since our claim is one statement (`04-…:108-110`) rather than a
   CTE round-trip.

   **Do not** put a limiter on the sender side. River can (`notifylimiter`,
   `client.go:2200`) because River's client sends the NOTIFY. Ours is a trigger
   with no memory across transactions, and the transaction-level dedup already
   does the sender-side job for the case that matters.

9. **Back the poll interval off adaptively, solid_queue style.** The poll
   function returns its own next sleep
   (`lib/solid_queue/processes/poller.rb:24-33`): `0` when the last claim
   returned a full batch, `poll_interval` when it returned nothing, and a long
   sleep when every worker slot is full — safe because the in-process wakeup
   interrupts it (`lib/solid_queue/processes/interruptible.rb:10-32`). This is
   orthogonal to the GCRA: GCRA bounds how *often* we may fetch, adaptive
   interval decides when we *want* to. Both are needed; neither substitutes.

### Closing the open item in `11-scheduling-and-cron.md:493-497`

That file asks whether the scheduler should NOTIFY when it promotes
`scheduled → available`. It should, and the answer is already implied by point 2:
**the promotion `UPDATE` must not fire the insert trigger** (our trigger is
`AFTER INSERT` only, so it does not — unlike underway's
`after insert or update`, `20241024174106_1.sql:19-22`), and the promoter should
emit the NOTIFY itself, filtered.

River's filter is the one to copy (`internal/maintenance/job_scheduler.go:190-202`):

> Notify about scheduled jobs with a scheduled_at in the past, or just slightly
> in the future (this loop, the notify, and tx commit will take a small amount
> of time). … `notificationHorizon := s.Time.Now().Add(5 * time.Millisecond)`

Promoting a job that is due in 40 seconds (which `11-…:70-76` says River does
deliberately, to take promotion latency off the critical path) must not wake
anybody for 40 seconds. The 5 ms horizon is the slack for the loop plus commit.

Consequence for the daemon: the promoter is the **only** component that calls
`pg_notify` from Rust. Everything else on the wakeup path is the trigger. Keep
that boundary — two writers to one channel is how River's schema-namespace bug
(`CHANGELOG.md:926`) stayed invisible.

### The alternative worth taking seriously: que's unicast

Que's trigger addresses one specific worker by shelling out to a registry table
(`lib/que/migrations/5/up.sql:36-45`). **We already have that table** —
`04-job-lifecycle-and-claiming.md:104-106` commits to a `stampede_workers`
heartbeat table with a rescuer. The trigger could shard `uuidv7` → one live
worker and notify only that worker's private channel. Herd eliminated at the
source, not absorbed at 50 receivers. No debouncer, no GCRA, no jitter.

Reasons not to do it *yet*:

- It puts routing policy in plpgsql, which is the line `03-postgres18.md:81-90`
  draws (*"Business logic in PL/pgSQL: no"*). Fairness across queues and tenants
  is `throttle-machines`' job (`05-polyglot-protocol.md:92-94`), and a modulo in
  a trigger is a second, divergent implementation of it.
- It reads `stampede_workers` on every insert statement. Que gets away with a
  few processes; at 200 workers the shard query is on the hot enqueue path.
- Que's own failure mode (above): a stale `listening` flag sends the wakeup to a
  dead worker and the job waits out a full poll interval. Our heartbeat table
  makes staleness *detectable*, which Que's does not — so we could do this
  better than Que. That is an argument for revisiting, not for shipping first.

**Decision: broadcast per-queue now, keep unicast as the escape hatch if herd
measurements justify it.** The two differ only in the trigger body; the
worker-side code is identical. Record it so the option is not forgotten.

### What this buys

- Anonymous polyglot `INSERT` wakes a worker in single-digit milliseconds. That
  is what `05-polyglot-protocol.md:22-27` promises, and only three projects in
  the survey deliver it today (que, procrastinate, underway) — all three by
  keeping a trigger, all three at the per-row cost we measured at +71 %.
- Behind a transaction-mode pooler, latency degrades to ~1 s and nothing breaks;
  the daemon says so in a log line instead of the operator discovering it.
- Trigger overhead is +1.2 % measured on bulk insert, and removable by one
  `DROP TRIGGER` — which is a supported configuration, not breakage.
- The herd is bounded by a GCRA whose parameters are config values, not by a
  hardcoded debouncer (Oban's 5 ms, River's 100 ms, Graphile's `refetchDelay`
  default of `-1`/off are each a different unbendable guess).
- Unicast (que's design) remains reachable later by editing one trigger body,
  because the worker side does not know how it was woken.

### What this costs

- A trigger, in a design that said "triggers narrowly"
  (`03-postgres18.md:81-86`). Justified above; the justification must live in the
  schema comment next to the CTE-fence reasoning (`README.md:35-39`), or someone
  will delete it — this is the second load-bearing comment in the schema and both
  protect against the same failure mode.
- One dedicated connection per daemon, permanently outside the pool, named in
  `pg_stat_activity` (good_job's `notifier.rb:257-269` shape).
- A sonar channel, a status field, and the discipline that the promoter is the
  only Rust code allowed to `pg_notify`.
- A second thing that can be true but useless: `connected? && !listening?`
  (good_job's distinction, `notifier.rb:98-115`) must be a real state in the
  daemon, not a boolean.

---

## Still open

- ~~**Single-row insert trigger overhead is unmeasured.**~~ **MEASURED:
  +3.5 %**, median of four alternating 20k-row runs on 18.6, against +1.2 % for
  the 50k bulk case. The hypothesis in this note was right — transition-table
  setup is per-statement and does not amortise over a single row — and the cost
  is still small enough to keep the trigger. Caveat: the inserts ran in a
  plpgsql loop on one connection, which isolates the trigger but is not quite
  the polyglot shape of separate statements from a client.
- ~~**Decide whether the daemon exports queue usage as a health metric.**~~
  **It does**, as `stampede.notify.queue_usage`, with a warning past 50%. None
  of the surveyed implementations export it, and the failure is worth the
  gauge: once the queue fills every `NOTIFY` errors, and because the notify
  trigger runs INSIDE the insert, enqueueing itself stops working — a symptom
  nowhere near its cause. **Still untested**: reproducing it needs a backend
  deliberately wedged in a long transaction.
- ~~**GCRA parameters are guesses.**~~ **MEASURED, and the guess was 5x too
  conservative.** One `stampede_claim` at batch 5 costs **668 us** on 18.6 with
  60k available rows, so a worker sustains roughly **7,500 jobs/s**. A 5 ms
  cooldown caps that at 1,000 — about a seventh. Exactly the effect this note
  predicted: Oban's number paces Oban's CTE round trip, ours is a single
  `UPDATE … RETURNING`. Default lowered to **1 ms**, which sits above the
  measured claim cost so it does not bind in steady state while still bounding
  a notification burst. One machine, one workload shape; a test pins the
  relationship rather than the number.
- ~~**Does the sonar channel justify itself?**~~ **YES — settled against a real
  pooler, and the cheaper alternative provably cannot do the job.** See §
  "The sonar, tested against pgbouncer" below.
- ~~**Interaction with `pg_cron`.**~~ Recurring jobs are not `pg_cron`'s; the
  schedule row is the due row and its row lock is the election. Superseded, and
  the original note follows for context. If recurring jobs are
  inserted by `pg_cron` inside the database, the trigger fires for them too and
  the wakeup works with no daemon involvement at all. That is a genuine argument
  for the trigger over any client-side scheme, but it is unverified — `pg_cron`
  is available and not installed.
- **Unicast vs broadcast is deferred, not resolved.** See "the alternative worth
  taking seriously" above. Needs a herd measurement at realistic worker counts
  before it can be closed either way, and that measurement does not exist because
  nothing is built.
- ~~**Direct-to-partition inserts are unenforceable.**~~ **Confirmed
  unenforceable-by-inheritance, and now DETECTED.** Verified on 18.6, which
  also settles the mechanism the entry left implicit:

  | trigger kind | cloned to partitions? |
  |---|---|
  | `FOR EACH STATEMENT` + `REFERENCING NEW TABLE` (ours) | **no** |
  | `FOR EACH ROW` | **yes**, including partitions created later |

  A raw `CREATE TABLE ... PARTITION OF stampede_jobs` gets **zero** triggers,
  and an INSERT straight into it notifies nobody — tested with a real listener.
  So the hole is structural: it cannot be closed by inheritance without moving
  to a row-level trigger, which costs +3.5% per row against +1.2% for the
  statement form (measured above) — paying a permanent throughput tax to
  prevent a mistake that has to be made by hand.

  The third option in the entry ("accept it and rely on the poll floor") was
  closest, but incomplete: accepting it silently is what makes it dangerous.
  The symptom is not failure, it is **silence** — jobs in that queue are still
  claimed correctly, just on the poll interval, so one queue is quietly slow
  for ever and nothing says why.

  `stampede_partition_health()` reports every partition and whether it carries
  the notify and counter triggers; the dashboard health panel shows any that do
  not, saying what it means. Detection rather than the event trigger, because
  an event trigger runs plpgsql on every DDL statement in the database
  (`03-postgres18.md:88-90` resists exactly that) to prevent something one
  query reports.
- **Channel-per-queue vs payload-per-queue.** procrastinate puts the queue name
  in the channel (`schema.sql:411`), so uninterested workers are never woken at
  the OS level; we put it in the payload, so they wake and discard. Channel-per-
  queue costs one `LISTEN` per subscribed queue on the single listener connection
  and makes dynamic queue creation a runtime `LISTEN`. Probably right at high
  queue counts, probably not worth it at low ones. Unmeasured.
- **Do we notify on anything other than insert?** Every project surveyed notifies
  on state transitions too — que has a broadcast `que_state` channel
  (`migrations/4/up.sql:257`), River a `river_control` channel for cancellation
  (`river_job.sql:51-54`), underway fires on `insert or update`
  (`20241024174106_1.sql:19-22`). `06-dashboard-and-mcp.md` will want a live
  transition feed, and `05-polyglot-protocol.md:51` already promises
  `GET /jobs/:id/events` as SSE. That feed is a *different* problem from wakeup
  — high fanout, low value per message, and it is the one place a payload
  actually wants to be large. Do not let it share the wakeup channel.

## The badge needed a third state

The dashboard reported wakeups from a boolean: live, or amber "polling only".
An end-to-end run of the **recommended Rails shape** — `--no-worker`, where the
workers are Ruby — showed amber forever.

Nothing was wrong. That daemon starts no listener *because it has no workers to
wake*; there is no degradation to report. But the operator sees a permanent
warning on a healthy system, and a warning that is always on is one people
learn to scroll past — which costs them the real one later.

So three states, not two: `Live`, `PollingOnly` (a listener is running and
nothing came back — usually a pooler in transaction mode eating `LISTEN`), and
`NotApplicable` (no workers here). Only the middle one is amber.

Worth noting how this surfaced: every unit test passed, because each asserted
the boolean it was given. The defect was that the boolean could not express the
system's most common configuration, and that is only visible from outside —
in this case from reading the badge on a daemon started the way the README
tells people to start it.


## The sonar, tested against pgbouncer

The open item above asked whether the sonar earns its keep, and named the
cheaper alternative: probe `pg_listening_channels()` on the listener connection,
as pg-boss does (`src/db.ts:190-196`). It noted that this "catches subscription
loss but *not* a pooler that accepts `LISTEN` and silently drops deliveries",
and left it unresolved which failure we actually care about.

**Resolved by installing pgbouncer and trying it.** Transaction pooling mode,
one listener and one notifier through the pooler:

```
LISTEN through pgbouncer (transaction mode): accepted, no error raised
pg_listening_channels() reports 1 channel(s)
NOTIFY sent through the same pooler
notification delivered: false
```

Every cheap signal says healthy. `LISTEN` is accepted without error. The
connection reports itself subscribed to exactly one channel. **The notification
never arrives.** `pg_listening_channels()` would report a problem: *no*. The
sonar would: *yes*.

Confirmed end to end against the real daemon — same binary, same database, only
the URL differs:

| connection | badge |
|---|---|
| `…@127.0.0.1:6432` (pgbouncer, transaction mode) | `polling only` |
| `…@localhost:5432` (direct) | `wakeups live` |

### Why this is the failure that matters

Transaction-mode pooling is not an exotic deployment — it is the *recommended*
PgBouncer mode and the common one in front of Rails. So the population most
likely to hit this is the population the Ruby gem exists to serve, and the
symptom is invisible: the queue is still correct, jobs still run, they are just
polled rather than woken. Latency quietly becomes `poll_interval` instead of
milliseconds, and nothing anywhere says so.

That is precisely the condition the open item assumed "the operator usually
already knows about". They do not, because there is nothing to know it by. A
second channel and a timer is a small price for the only signal that can tell
the difference.

**The cheaper alternative is not merely weaker, it is actively misleading here**
— it returns a confident healthy answer in the exact case we care about. Ship
the sonar.