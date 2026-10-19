# Schema notes
The SQL in `schema/` is deliberately plain: DDL, constraints, no prose. This
file is where the prose went.

Every block below was a comment in the schema, kept with the statement it
introduced. It is the *why* -- the measurements, the failure modes, the things
that were tried and rejected. The SQL tells you what the database does; this
tells you why it does it that way, which is the part that is expensive to
rediscover.

## Tables, indexes and triggers

### `CREATE TABLE stampeded_workers (`

Stampeded -- initial schema
Postgres 18+ ONLY. No compatibility shims.

Where a line looks odd, the comment says why, because the non-obvious ones
are non-obvious for a reason and will otherwise be "simplified" into a bug.
Workers -- heartbeat liveness.

Orphan recovery is heartbeat-based, NOT a time lease. A lease is cheap and
wrong: correctness depends on guessing the maximum job duration, and a job
that outlives the guess runs twice.

### `CREATE INDEX stampeded_workers_heartbeat_idx ON stampeded_workers (last_heartbeat)`

The reaper's only index.

### `CREATE TABLE stampeded_jobs (`

Jobs -- partitioned by QUEUE.

Partitioning by `state` is mutually exclusive with a unique index on the
dedup key (Postgres requires a unique constraint on a partitioned table to
contain every partition-key column, and a generated column cannot BE a
partition key). Uniqueness won; partitioning moved to queue.

Accepted consequence: no state-based pruning. Completed jobs share a
partition with live ones, so the pruner and autovacuum settings below are the
ONLY defence against unbounded growth.

### `args            jsonb       NOT NULL DEFAULT '{}'::jsonb`

jsonb not bytea: inspectable in psql and searchable from the MCP control
plane. An opaque payload would gut `job_search`.

BOUNDED AT 1 MB, generously. Postgres will happily TOAST a huge value
out of line, so the row stays small -- but `stampeded_claim` returns
`stampeded_jobs.*`, which detoasts it on every single claim. Someone
stuffing a PDF in here would move megabytes per claim through a query
that is otherwise a few hundred bytes, on the hot path this whole schema
is shaped around.

1 MB is far above any sane payload and far below a file. The guidance is
the usual one: pass an id, not an object. Enforced in SQL rather than in
the client because it is the only bound that survives a polyglot
enqueuer.

### `destination     text`

What this job talks to: an API host, a database, a third party. A
FIRST-CLASS COLUMN, not a tag inside args, because it is the circuit
breaker's key and therefore has to be queryable: per-destination
failure rates in the dashboard, failure_summary grouped by destination,
and suppression of retries against a dependency already known to be down.
Buried in args it could do none of that.

### `expire_seconds  integer     NOT NULL DEFAULT 3600 CHECK (expire_seconds > 0)`

Database-enforced timeout. The in-process tokio timeout cannot fire when
the worker is DEAD, which is precisely the case that needs it; this one
does not care whether anybody is alive. Layer 2 of the three-layer
timeout stack.

### `heartbeat_at    timestamptz`

Long-running jobs touch this so a slow job is distinguishable from a dead
one. Set at claim; NULL means never touched.

### `attempt         smallint    NOT NULL DEFAULT 0`

Attempt accounting. `attempt` is incremented AT CLAIM, never at failure.
This single line is the difference between a bounded budget and an
unbounded poison loop: if the counter only advances when the worker
survives to report, a job that segfaults its worker is re-claimed forever
and max_attempts is never reached.

### `max_attempts    smallint    NOT NULL DEFAULT 25 CHECK (max_attempts >= 1)`

MUST BE AT LEAST 1. The claim predicate is `attempt < max_attempts`, so
a job with max_attempts = 0 can never be claimed: it sits in `available`
forever, counts toward depth, and nothing ever runs it or reports why.
The API accepted it happily until a fuzz of the enqueue endpoint found
the black hole.

### `attempted_by    text[]      NOT NULL DEFAULT '{}'`

Bounded ring, trimmed on write. Unbounded attempt history is a slow leak
on heavily-retried jobs.

### `interruptions       smallint NOT NULL DEFAULT 0`

TWO counters, not one. `attempt` counts raises; this counts the
worker-death kinds.

One free interruption, then discard. A job that kills its worker gets
exactly one more chance, because otherwise it walks the entire fleet:
claimed, kills a worker, reclaimed by the next, kills that one too. The
attempt counter cannot express this, since a job that dies mid-execution
is not the same event as a job that raised, and charging both to one
budget makes a crash-looping job indistinguishable from a flaky one.

### `deferrals           smallint NOT NULL DEFAULT 0`

A THIRD counter, which looks like one too many. It is not, because
without it the refund rule above has no floor:
`breaker_open` and `throttled` give the attempt back, so a permanently
open circuit re-claims and re-defers the same job forever. Verified: 12
cycles against a max_attempts of 3 left attempt at 0. The queue grows
silently and nothing ever reports a failure.

So deferrals are counted, with a DELIBERATELY GENEROUS cap. This is not
a retry budget and must not behave like one: at the capped backoff of an
hour, 100 deferrals is days of downstream outage before anything is
given up on. A real outage must never reach it; a destination that is
permanently gone must.

### `checkpoint      jsonb CHECK (checkpoint IS NULL OR pg_column_size(checkpoint) <= 65536)`

Resume state.

Opaque to us: the job's own cursor, handed back verbatim on the next
attempt so a job that died at row 50,000 of 100,000 does not start again
at row 1.

The database stores one opaque value. Named steps are a shape the
workers write into it (the step record both the Rust and Ruby workers
share), so the schema needs nothing more for them.

Deliberately NOT durable execution: replaying a function requires
intercepting every side effect, which requires a per-language SDK, which
is the client-library tax the polyglot design exists to avoid. A cursor
is a column and works from any language.

BOUNDED IN SQL, for the same reason error text is: a Python worker will
not run our truncation logic. Someone will try to store the 100,000
processed ids here instead of a cursor, and the row will be megabytes on
the one table that already has the bloat problem.

### `checkpoint_seq  integer     NOT NULL DEFAULT 0`

Monotonic, so a caller can tell whether the state it is looking at is the
one it wrote.

### `backoff_base_ms smallint    NOT NULL DEFAULT 1000 CHECK (backoff_base_ms > 0)`

Backoff policy on the ROW, so the dashboard and MCP can render the
remaining retry schedule without re-deriving it from a Rust constant they
cannot see. Cap first, then jitter.

### `dedup_scope     text`

Deduplication.
dedup_scope is client-supplied and nullable; NULL means "not unique".
It stays readable text so psql and the MCP tools can explain WHY two jobs
collided -- the hash exists only to bound the index key.

NOT sha256(convert_to(...)): convert_to is STABLE, not IMMUTABLE, and is
rejected in a generated column. The ::bytea cast IS immutable. Verified.

### `dedup_period    text        CHECK (dedup_period IN`

Optional time bucket: "one of these per hour", not "one of these ever".

Computed SERVER-SIDE. The client could compose the bucket into
dedup_scope itself, but then two enqueuers with skewed clocks land in
different buckets and the limit quietly stops holding.

BUCKETED ON inserted_at, NOT scheduled_at. The trap is verified: a
STORED key bucketed on scheduled_at silently CHANGES when a job is
snoozed or rescheduled, vacating its own throttle slot and letting a
duplicate in. inserted_at is never updated, so the key is stable for the
row's whole life.

Honest limitation: buckets are fixed windows, not sliding ones. Two jobs
60 seconds apart can land either side of an hour boundary and both run. A
sliding window needs `ts >= now() - period`, which is not immutable, so
it cannot live in an index or a generated column at all -- it needs a
read-modify-write per enqueue, and that needs the client to run our code,
which is exactly what the polyglot INSERT rules out.

### `dedup_key       bytea       GENERATED ALWAYS AS (`

NOT sha256(convert_to(...)): convert_to is STABLE, not IMMUTABLE, and is
rejected in a generated column. The ::bytea cast IS immutable. Verified.

The bucket chain is fussier than it looks, and every step was checked:
two-argument date_trunc is STABLE (it reads the session TimeZone), the
three-argument form with an explicit zone is IMMUTABLE, and casting a
timestamptz to text is STABLE again (DateStyle) -- so the truncated time
is turned into a number by subtracting the epoch, which yields an
interval, and extract on an INTERVAL is immutable. Skipping any of that
gets "generation expression is not immutable" with no hint as to which
part.

### `failure_kind    text`

Failure taxonomy. Distinguishing "the worker died" from "the job raised"
is what makes poison jobs diagnosable.

### `last_error      text        CHECK (length(last_error) <= 4000)`

Truncated in SQL, not in client code: it is the only bound that survives
a polyglot worker -- a Python or Go worker will not run our Rust
truncation logic.

### `errors          jsonb       NOT NULL DEFAULT '[]'::jsonb`

Structured error history, not a formatted blob: a formatted blob is
readable and machine-useless.
Each element: {at, attempt, kind, class, message, backtrace, destination}.

Bounded like attempted_by. Bounding attempted_by alone would be half a
fix, and the half that leaks, since error text dwarfs a worker id.

### `error_fingerprint bytea`

Normalised error class + message with numbers, UUIDs and hex stripped,
then hashed. Indexed. This is the difference between `failure_summary`
being an index scan and being a seq scan over every error string in the
table.

### `dead_letter_queue text`

Dead letter. The DLQ is a QUEUE, so re-drive is just working that queue
and every existing tool applies, rather than a bespoke mutation path
nothing else understands.

### `source_id       uuid`

Provenance, set on the job that LANDS in the dead-letter queue.

### `PRIMARY KEY (queue, id)`

The PK must contain the partition key. `id` alone is not permitted on a
partitioned table, so uuidv7's ordering is carried by the index below
rather than by the PK.

### `CONSTRAINT stampeded_jobs_failure_kind_valid CHECK (`

The failure taxonomy. The distinctions that earn their keep: worker_exited
carries an exit status (137 = OOM, the single highest-signal artefact a
dead worker leaves), and breaker_open means WE refused to run the job, so
it must not be charged for it.

### `CONSTRAINT stampeded_jobs_dlq_not_self CHECK (`

A job cannot dead-letter onto its own queue: that is an infinite loop
wearing a feature's clothing.

### `CONSTRAINT stampeded_jobs_args_not_json_null CHECK (jsonb_typeof(args) <> 'null')`

`NOT NULL` stops SQL NULL; it does not stop the JSON value `null`, which
is a perfectly valid jsonb. Both clients produced it: `POST /jobs`
without `args`, and the Ruby `enqueue(args: nil)`. Every worker then read
back nil/None where it indexes a map, and the Ruby handler raised on
`args["x"]`. "No arguments" is `{}`, the column's own default.

Both clients now normalise null to `{}` before the INSERT, so this only
fires for a caller writing its own INSERT -- and for that caller a
loud constraint violation at enqueue beats a NoMethodError in a worker
later. Rejected rather than rewritten by a trigger: a BEFORE INSERT
trigger would put a function call on every enqueue to fix input that is
a bug at the source, and silently coercing it is how the caller never
learns.

### `CREATE TABLE stampeded_jobs_default PARTITION OF stampeded_jobs DEFAULT`

NO storage parameters on the parent: Postgres rejects them outright
("cannot specify storage parameters for a partitioned table"), and they are
NOT inherited by partitions nor copied by LIKE INCLUDING ALL. Verified.

OPERATIONAL CONSEQUENCE: fillfactor and autovacuum settings must be applied
to EVERY partition individually, including every queue partition created at
runtime. A queue provisioned without them silently runs on defaults
(autovacuum_vacuum_scale_factor 0.2), which on a large partition means vacuum
waits for 20% dead tuples. That is the bloat failure mode, reintroduced one
queue at a time. The partition-creation path MUST set them;
see stampeded_create_queue_partition below.
Catches any queue not explicitly provisioned. Queues are created at runtime,
so each new queue wants its own partition; DEFAULT means an unprovisioned
queue still works.

### `CREATE INDEX stampeded_jobs_claim_idx`

Indexes
The claim path. Partial, because only `available` rows are ever claimed and
the completed tail lives in the same partition (see the partitioning note).
Measured without it: a parallel seq scan, 18,430 buffers / 54 ms, versus 11
buffers / 0.165 ms with the partial index.

scheduled_at is deliberately NOT in a generated `is_ready` column: generation
expressions must be IMMUTABLE and anything with now() is rejected; virtual
generated columns also cannot be indexed at all. Verified twice.
Must cover BOTH claimable states. A retryable job whose backoff has elapsed
is claimable (see stampeded_claim); indexing only 'available' would
make every retry a seq scan.

`id` is deliberately NOT a trailing tiebreaker. It would make every index
tuple unique, which leaves btree deduplication nothing to collapse:
MEASURED at 85 MB against 10 MB at a million rows, 8.5x, on the index every
claim on every queue walks and the one most wanted resident in
shared_buffers. Claim time is indifferent (0.13 ms vs 0.11 ms for a 100-row
claim, no sort either way), so the tiebreaker bought only determinism.

WHAT THAT COSTS: jobs sharing an exact (priority, scheduled_at) are claimed
in index-scan order rather than insertion order. A bulk enqueue produces
such a tie by construction -- one transaction, one now(), every row the
same. The order is stable for a given index state, so nothing starves; it
is simply not defined. Strict FIFO was never on offer across workers anyway,
since FOR UPDATE SKIP LOCKED takes the next UNLOCKED row, not the next row.

stampeded_claim's ORDER BY must match this index exactly. Adding a column
there without adding it here reintroduces a sort: measured 0.26 ms and 244
buffers against 0.13 ms and 212.

### `CREATE UNIQUE INDEX stampeded_jobs_dedup_idx`

Uniqueness. Partial: a full constraint costs 1272 kB per 200k non-dedup rows
versus 16 kB partial. Led by `queue` because the partition key
must be present -- which is also what makes uniqueness per-queue.

### `CREATE INDEX stampeded_jobs_claimed_idx`

Orphan reaping: find rows whose worker stopped heartbeating.

### `CREATE INDEX stampeded_jobs_tenant_idx`

Tenant fairness and MCP queries.

### `CREATE INDEX stampeded_jobs_finished_idx`

Pruning and the "what failed recently" MCP tool.

### `CREATE INDEX stampeded_jobs_fingerprint_idx`

`failure_summary` clusters by fingerprint. Without this index that tool is a
seq scan over every error string in the table, which is exactly the query an
operator runs while the incident is ongoing and the table is at its largest.

### `CREATE INDEX stampeded_jobs_destination_idx`

Per-destination health, for the breaker and the dashboard.

### `CREATE INDEX stampeded_jobs_expiry_idx`

The expiry sweep: executing jobs whose wall-clock budget has run out.
Partial, because only executing rows can expire.

### `ALTER TABLE stampeded_jobs_default SET (`

Autovacuum

A queue table is the pathological autovacuum case -- constant UPDATEs over
a small hot working set plus a large finished tail. Defaults
(scale_factor 0.2) mean vacuum waits for 20% of the table to be dead, which
on a large table is far too late.

fillfactor = 85 is NOT a number taken on faith -- it was measured here,
and the measurement contradicted the reasoning that nearly removed it.
20k jobs through claim + 10 heartbeats + complete:

fillfactor   heap after churn   HOT updates
100            88 MB             0.1%
85            37 MB            60.7%
70            22 MB            77.1%

The reason is heartbeats, not claims. Claiming updates `state` and can never
be HOT -- that much was known, and was the argument for calling this cargo.
But stampeded_touch writes `heartbeat_at` ALONE, which is in no index, so
every heartbeat is HOT-eligible and a long job emits a great many of them.
At fillfactor 100 there is no room on the page for the new version, so each
one writes a fresh tuple and an index entry: 2.4x the heap.

85 rather than 70 because a fresh, idle backlog spreads over 18% more pages
at 70, and a deep idle queue is a real state. The claim path itself is
indifferent (208 buffers either way).

### `CREATE FUNCTION stampeded_notify_insert() RETURNS trigger`

Wakeup notification

STATEMENT-level, not per-row. Measured on 18.6: the per-row form
costs +71% on a 50k-row insert; this costs +1.2%. The notification-storm
objection to insert triggers is specific to the per-row form.

Payload is the bare queue name: Postgres collapses identical
(channel, payload) pairs within a transaction, so a 1500-row insert across
two queues emits exactly two notifications. That makes it self-throttling and
makes the 8000-byte payload cap irrelevant. A random nonce in the payload
would DEFEAT that collapsing and break both properties.

This is why enqueue can stay a plain INSERT from any language:
the database notifies, not the client.
The channel is SCHEMA-NAMESPACED. Channel names are global per database, so
two Stampeded installs in different schemas of one database would wake each
other's workers for queues they do not serve.

Budget: a channel name is capped at 63 bytes, so the schema name has
63 - length('.stampeded_insert') = 46 to play with. Verified.

### `CREATE TRIGGER stampeded_notify_insert_trg`

NOTE: the trigger must exist on the parent AND on every partition. Inserting
directly into a partition silently bypasses a parent-only trigger -- a real
hole in the "enqueue is a plain INSERT" promise. Duplicate
delivery is free because of the payload collapsing described above.

### `CREATE TABLE stampeded_queue_counts (`

Queue counters

The single most important performance rule: NEVER run an unbounded aggregate
over the job table.

`SELECT count(*) FROM stampeded_jobs GROUP BY state` looks harmless and is a
death spiral. A long aggregate holds the xmin horizon open; vacuum cannot
reclaim dead tuples below that horizon; the table bloats; the aggregate
gets slower; it holds the horizon longer. The
queue does not fall over, it degrades -- which is worse, because it presents as
"mysteriously slow backlog" rather than as an error.

Anything that wants a count reads THIS table: the dashboard refreshing every
second, `queue_status` over MCP, the depth API, every metric.

Rows are (queue, state) pairs maintained by a statement-level trigger. Small,
fixed cardinality: queues x 7 states.

### `n       bigint   NOT NULL DEFAULT 0`

Signed: a counter is allowed to go transiently negative if a row is
deleted by a path the trigger did not see. Clamping it to zero would hide
exactly the drift that stampeded_recount_queue exists to repair.

### `CREATE FUNCTION stampeded_recount_queue(p_queue text) RETURNS void`

Counters drift. Any correction is a bug, but bugs happen, and a counter that
cannot be repaired without downtime is worse than one that can.

THIS is the unbounded aggregate the rest of the system is forbidden from
running. It is acceptable here because it is explicit, manual and rare --
not because it is cheap. Do not call it on a timer.

### `CREATE FUNCTION stampeded_count_delta() RETURNS trigger`

Maintained by transition deltas, statement-level, using the same transition
tables as the notify trigger. Per-row would put a plpgsql call on every row of
every bulk insert, which is the +71% figure measured for the notify trigger.

Branches on TG_OP because a transition table only EXISTS for the operation it
belongs to: an INSERT trigger may declare NEW TABLE and not OLD TABLE, a
DELETE trigger the reverse. Referencing the absent one is a create-time error.

### `ORDER BY queue, state`

Deterministic lock order, same reason as the UPDATE branch below.

### `ORDER BY queue, state`

Deterministic lock order, same reason as the UPDATE branch below.

### `WITH delta AS (`

Both directions netted before they touch the table, so a 10k-row
state change is one UPSERT per (queue, state) rather than 10k. The
HAVING drops rows whose state did not change, which is most of them:
a heartbeat UPDATE moves no job between states and should cost
nothing here.

### `ORDER BY queue, state`

ORDER BY IS LOAD-BEARING: it prevents a deadlock.

A transition statement touches several (queue, state) rows at once --
one decrement, one increment -- and without an order, concurrent
transactions lock them in whatever order the CTE happened to emit.
Worker A locks (default, executing) then (default, completed) while
worker B does the reverse, and Postgres kills one of them with
40P01.

Reproduced: four workers completing 200 jobs concurrently deadlocked
inside stampeded_complete every run. In production that is a worker
failing to report a finished job, which then sits until the reaper
notices -- a silent stall under exactly the load this design is for.

### `CREATE FUNCTION stampeded_attach_counters(p_table text) RETURNS void`

Attached per PARTITION, not to the parent: a statement-level trigger with
transition tables on a partitioned root does not see rows routed to leaves,
and a direct INSERT into a partition would bypass a parent-only trigger
anyway -- the same hole the notify trigger closes the same way.

### `DO $$ BEGIN`

BOTH, exactly like the notify trigger, and for the same reason with the
opposite failure mode. A statement-level trigger fires for the table the
statement NAMED: inserting into the parent fires only the parent's, inserting
straight into a partition fires only that partition's. Attach one and the
other path is silently uncounted.

Verified that this does NOT double-count: 5 rows through the parent then 3
straight into the partition gave 8, not 13.

### `CREATE FUNCTION stampeded_create_queue(p_queue text) RETURNS void`

Queue provisioning

Queues are created at runtime, and a partition needs THREE things applied
that are not inherited from the parent: storage parameters, autovacuum
settings, and the notify trigger. Forgetting any one of them fails silently --
the queue works, and then bloats, or stops waking workers.

So provisioning is a function, not a runbook step. Callable from any
language, which keeps it consistent with the polyglot stance.

### `EXECUTE format(`

Not inherited from the parent. See the note on the parent table.

### `EXECUTE format(`

Direct inserts into a partition bypass a parent-only trigger.

### `PERFORM stampeded_attach_counters(part_name)`

Counters are per-partition for the same reason the notify trigger is.

## Operations

### `CREATE FUNCTION stampeded_claim(`

Stampeded -- operations

The semantics live here. The tables encode the decisions; these functions
encode the rules that make them correct.

Everything is a FUNCTION rather than daemon-side SQL, deliberately: one round
trip, atomic, and callable from any language with a Postgres driver. That is
the polyglot story -- a Python worker calls stampeded_claim() and
gets the same fence, the same attempt accounting and the same transition
rules as the Rust daemon, because there is only one implementation.
CLAIM

Two things in here are load-bearing and will look like noise to a reader:

(1) `AS MATERIALIZED` on the CTE. This is an optimization fence: without it
the planner may nested-loop the LIMIT subquery and update MORE ROWS THAN
LIMIT, pushing attempt past max_attempts. Since PG12 a plain CTE is INLINED
by default, which defeats the fence entirely. On 18 the keyword is
required, not optional. An earlier draft claimed RETURNING OLD./NEW.
made the fence unnecessary; that was wrong. You still need to pick N rows,
which still needs a LIMIT subquery, which still needs the fence.

(2) `attempt = attempt + 1` HERE, at claim, not at failure. This is the
difference between a bounded budget and an unbounded poison loop. A job
that segfaults its worker has already spent the attempt, so max_attempts
is eventually reached. If the increment moves to the failure path, a
worker-killing job is re-claimed forever.

### `p_now       timestamptz DEFAULT NULL`

Injectable clock, `coalesce(p_now, now())`. Makes scheduling testable
without sleeping.

### `AND state        IN ('available','retryable')`

'retryable' is claimable once its backoff has elapsed. That can be
handled by a claim predicate, a separate promotion pass, or both. The
predicate is chosen here for having no moving parts -- a promotion pass
is another process that can fall behind or die.

Omitting 'retryable' here is not a missing feature, it is a silent
poison-job bug: the reaper parks a crashed job in 'retryable', a
claim that only reads 'available' never picks it up again, the
attempt budget never walks down, and max_attempts is never
reached. Found by test, not by reading.

### `ORDER BY priority, scheduled_at`

Must match stampeded_jobs_claim_idx EXACTLY. `id` is not a
tiebreaker here: adding one reintroduces a sort unless it is also
added to the index, and in the index it costs 8.5x the size by
defeating btree deduplication. See the stampeded_jobs_claim_idx note.

### `heartbeat_at = coalesce(p_now, now())`

Seed the job heartbeat at claim so stampeded_expire has a baseline
for a job that never calls stampeded_touch. Left NULL it would fall
back to claimed_at, which is the same thing -- but only by accident,
and the expiry predicate should not depend on that coincidence.

### `attempted_by = array_append(`

Bounded ring buffer. Unbounded attempt history is a slow leak on
heavily-retried jobs; keep the last 10.

### `CREATE FUNCTION stampeded_complete(`

COMPLETE

Returns the prior state via PG18 `RETURNING OLD.`. This is where that feature
earns its place: the caller learns atomically whether it actually
owned the transition, so a worker that lost its job to the reaper mid-flight
finds out instead of silently completing a job someone else is running.

`dedup_scope = NULL` releases the uniqueness slot as part of the SAME
update. Keys can only LEAVE the index, so no transition can ever raise
23505, and none needs a conflict-handling path.

### `checkpoint  = NULL`

Dead weight once the job is done, on the one table that already
has the bloat problem. Consistent with the fire-and-forget
decision: we do not keep results either.

### `CREATE FUNCTION stampeded_fingerprint(p_error text) RETURNS bytea`

FAIL

One entry point for every failure, with the KIND recorded separately from the
count. Distinguishing "the worker died" from "the job raised" is the thing
that makes poison jobs diagnosable.

Backoff is NOT computed here. chrono-machines owns it (full jitter by
default) and passes the resulting instant in. Putting the formula in SQL
would fork it.
Error fingerprint: the normalisation that makes failures CLUSTER.

"connection to 10.0.0.7:5432 timed out after 30012ms" and
"connection to 10.0.0.9:5432 timed out after 29998ms" are one incident, but
as raw strings they are two. Strip the parts that vary per occurrence --
UUIDs, hex blobs, numbers -- and hash what survives. Without this, a
`failure_summary` lists instances instead of causes.

IMMUTABLE so it can be computed in an index or a generated column later.

### `CREATE FUNCTION stampeded_fail(`

FAIL

Three outcomes, decided by WHY the job failed rather than only by how many
times it has.

breaker_open      -- our backpressure. REFUNDS the attempt.
worker-death      -- charges `interruptions`, a separate and much tighter
budget than `attempt`.
everything else   -- charges `attempt`, which was already spent at claim.

On a terminal outcome the job dead-letters if a queue is configured, because
the DLQ should be a QUEUE: re-drive is then just working it, and every tool
that understands jobs already understands it.

### `p_retry_delay_ms bigint DEFAULT NULL`

A DELAY, not an instant.

Taking an absolute timestamptz computed on the caller makes retry timing
depend on the app server and the database agreeing about the time. A
server 30s ahead delays every retry by 30s; a fleet with mixed skew
behaves inconsistently; and it is invisible in development, where skew is
a few milliseconds. Measured at 3ms locally -- enough to make a retry
scheduled for "now" not ready yet.

The database applies its own clock, so there is nothing to agree about.

### `p_exit_status  integer DEFAULT NULL`

Exit status, when the daemon supervises the worker process and saw it
die. 137 is OOM. This is the cheapest, highest-signal artefact a dead
worker leaves.

### `RETURN`

Not ours: the reaper already reclaimed it and somebody else owns the
outcome. Writing a failure over another worker's result would be
worse than doing nothing.

### `v_interrupt := p_kind IN ('heartbeat_lost', 'worker_exited', 'worker_missing')`

Did the worker die, rather than the job fail?

### `v_refund    := p_kind IN ('breaker_open', 'throttled')`

Did WE refuse to run it?
Both are US declining to run the job: one because the downstream is
sick, one because the tenant is over its share. Kept as separate kinds
because the operator response differs -- "the API is down" and "this
customer is flooding us" want different people woken up.

### `v_terminal := CASE`

The correctness rule: a failure caused by our own backpressure must not
count against the job's retry budget. The attempt was spent at claim, so
declining to run the job has to give it back -- otherwise a downstream
outage silently burns every job's budget and a fleet-wide breaker trip
discards the entire backlog.

### `WHEN v_refund    THEN j.deferrals + 1 > j.max_deferrals`

A deferral is terminal only when the job has been waiting on a
dependency for so long that the dependency is evidently not coming
back. Everything else about the refund still applies: the attempt is
returned, because none of this was the job's doing.

### `last_error   = left(p_error, 4000)`

Truncated in SQL, not in client code: it is the only bound that
survives a polyglot worker.

### `errors = (`

Ring buffer, newest last, capped at 10. Bounding attempted_by
alone would be half a fix, and the half that actually leaks,
since error text dwarfs a worker id.

### `dedup_scope  = CASE WHEN v_terminal THEN NULL ELSE dedup_scope END`

Release the dedup slot only on a terminal outcome; a retryable job
keeps its slot so a duplicate cannot be enqueued while it waits.

### `IF v_terminal AND j.dead_letter_queue IS NOT NULL THEN`

Dead letter, with provenance. The copy carries where it came from and how
hard it tried, because a DLQ row with no history is an alert nobody can
action. Its own dead_letter_queue is left NULL: a dead letter that can
itself dead-letter is a loop.

### `CREATE FUNCTION stampeded_release(`

RELEASE

A worker that claimed a batch and is stopping hands back the jobs it never
started. That is not a failure, so this is not `stampeded_fail`: nothing is
recorded. It used to be `stampeded_fail(... 'throttled' ...)`, which wrote
over the job's last real failure -- a job that raised came back as
"throttled: worker stopping", with a throttled entry in its history, a
deferral charged, and a fingerprint that made every deploy a failure
cluster in `failure_summary`.

The claim is undone and nothing else: the attempt is refunded,
`failure_kind`, `last_error`, `errors` and `deferrals` stay as they were,
and `scheduled_at` is kept -- the job was already due, so it keeps its
place in line. It goes back to `available` if it never failed and to
`retryable` if it had, which is where it was claimed from; `failure_kind`
is NULL exactly for jobs that have never failed (or were reset by an
operator), so it tells the two apart. `attempted_by` keeps the entry the
claim added, as every other refund does.

Fenced like `stampeded_complete`, and on `state = 'executing'` too: a job
the reaper or the expiry already took back is not this worker's to return.

### `CREATE FUNCTION stampeded_touch(p_queue text, p_id uuid, p_worker_id uuid)`

TOUCH -- a long job proving it is alive

Layer 3 of the timeout stack. The worker heartbeat says the PROCESS is
alive; this says this particular JOB is still progressing. Without it a
legitimately long job and a wedged one look identical, and the only
available response is to guess a timeout.

### `CREATE FUNCTION stampeded_checkpoint(`

CHECKPOINT -- save resume state

Returns the new sequence number, or NULL if this worker no longer owns the
job.

THE OWNERSHIP CHECK IS NOT A FORMALITY HERE. The sequence that makes it
matter: worker A checkpoints at 50k and its host wedges; the reaper decides A
is dead and returns the job; worker B claims it, resumes from 50k and
checkpoints at 60k; then A WAKES UP and writes 51k. Without the predicate,
A's stale write moves the cursor BACKWARDS and B's next attempt redoes 9,000
units of work -- or does them twice, if the job is not perfectly idempotent.

It also sets heartbeat_at: a job that just saved progress is demonstrably
alive, and requiring a separate stampeded_touch would be asking the caller to
say the same thing twice. Forgetting that second call would let a job which
is plainly working get expired out from under itself.

AND IT RESETS `interruptions`, which is the interesting decision. The two
counters mean different things: `interruptions` exists to stop a job that
KILLS WORKERS from walking the fleet one process at a time, and a
job that checkpointed at 50k and then died is demonstrably not that job -- it
ran, it worked, something else killed it. `attempt` is untouched and still
increments at claim, so a job that progresses one unit and then dies forever
is still discarded at max_attempts. Progress buys tolerance of
INFRASTRUCTURE failure and buys nothing against a job that is simply broken.
Warn when a checkpoint looks like a result set rather than a cursor.

The 64 KB CHECK is a hard stop that catches only the pathological case, and
it catches it by failing the job -- at which point the work is already lost.
The misuse it exists for is gradual: storing the 100,000 processed ids
instead of "I have done 100,000". That has a SHAPE, and the shape is
detectable. A cursor ADVANCES, so it stays about the same size while its
value changes. A result set ACCUMULATES, so it grows on every write.

So: warn when the checkpoint is both large and still growing. Neither
condition alone means anything -- a big constant checkpoint is fine, and a
small growing one has room -- but together they are a job heading for the
cap, and the warning arrives with thousands of writes left to act on.

A WARNING, never an ERROR. The job is working, and the 64 KB CHECK is
already the enforcement; turning a heuristic into a hard failure would
discard jobs over a guess about intent.

### `c_warn_bytes constant int := 8192`

An eighth of the 64 KB cap: enough runway that a job growing steadily
has thousands of writes left when it is told.

### `v_new_size   integer := length(p_state::text)`

length(::text), NOT pg_column_size.

pg_column_size reports the STORED size, which for a stored jsonb is
TOAST-compressed and for a parameter is not -- so comparing the two
compares compressed against uncompressed. A stable 20 KB checkpoint of
repetitive data compresses to a few hundred bytes on disk and then looks
like it grew 70x on every single write, warning forever about a job
doing nothing wrong. Found by testing the large-but-stable case, which
is the one that distinguishes this heuristic from the size cap.

The text length is the logical size on both sides.

### `CREATE FUNCTION stampeded_expire(p_limit integer DEFAULT 100)`

EXPIRE -- the timeout that fires when nobody is alive to fire it

Layer 2 of the three. The in-process tokio timeout cannot fire if the worker
is dead, and the reaper only helps once the worker has stopped heartbeating
entirely -- a worker that is alive and healthy but stuck on one job is
invisible to both. This catches exactly that.

Routes through the NORMAL fail path so it consumes a retry, backs off and
dead-letters like any other failure.

### `AND coalesce(heartbeat_at, claimed_at) < now() - make_interval(secs => expire_seconds)`

From the last sign of life, not from claim: a job that is touching
is working, however long it has been running.

### `CREATE FUNCTION stampeded_reap(`

REAP -- orphan recovery

Heartbeat, NOT a time lease. A lease turns a slow job into duplicate
execution because correctness depends on guessing maximum job duration.

The attempt was already spent at claim, so a job whose worker keeps dying
walks its attempt budget down and eventually discards itself. That is the
poison-job brake, and it costs nothing here.

### `SET state        = CASE WHEN j.interruptions + 1 > j.max_interruptions`

TWO budgets now, and the tighter one usually wins. `attempt` was spent
at claim (the poison brake); `interruptions` is the brake specific to
jobs that KILL their worker, defaulting to one free one. A job that
segfaults every worker it touches hits max_interruptions after two
claims instead of walking 25 workers down first.

### `failure_kind  = 'worker_missing'`

Named distinctly from a raise: conflating the two is what makes "why
did this run twice" unanswerable.

### `CREATE FUNCTION stampeded_prune(`

PRUNE

Bounded batch, never an unbounded DELETE. With partitioning by queue there
is no state partition to drop, so pruning and autovacuum are the ONLY
defence against unbounded growth. A single unbounded DELETE on a large
table holds locks and bloats worse than what it reclaims.

Returns the count so the caller can loop until it returns < p_limit, which
avoids both a long transaction and an unfinished sweep.
TWO retention clocks, not one.

A completed job is evidence nobody will read; a discarded one is evidence
somebody is about to be paged about. Keeping them for the same duration means
either drowning in successes or deleting failures before anyone can look at
them, destroying the evidence before the alert has fired.

Defaults: 1 hour for completed, 7 days for discarded and cancelled.

### `deleted AS (`

COUNTED, not `RETURNING 1`.

A SQL function declared RETURNS integer yields the first column of the
FIRST ROW, so `DELETE ... RETURNING 1` returns 1 whether it deleted one
row or ten thousand (and NULL for none). The caller loops until the
returned count is less than the batch size -- with a constant 1 that
condition is always true, so the loop always exited after one batch and
a large backlog was pruned p_limit rows per TICK instead of per
iteration. The pruner looked correct and quietly did a fraction of the
work.

### `CREATE FUNCTION stampeded_vacuum_health()`

VACUUM HEALTH

This converts the queue's characteristic death spiral from "mysterious
slow backlog" into a named cause. A queue table is the pathological
autovacuum case -- constant UPDATEs over a small hot set -- and when
autovacuum falls behind, everything simply gets slower with no error
anywhere.

Returns one row per partition: dead tuples, the threshold autovacuum will
actually use for THAT table (its own reloptions, not the global defaults --
which is the whole reason a naive n_dead_tup alert misfires), and when it
last ran.

### `threshold.base + threshold.factor * s.n_live_tup`

The formula autovacuum itself uses, with this table's own settings
where they are set and the cluster defaults where they are not.

### `AND s.schemaname = current_schema()`

SCOPED TO THIS SCHEMA. pg_stat_user_tables spans the whole database,
so without this a second Stampeded install -- or, here, a few hundred
leftover test schemas -- reports as this one's partitions. Found by
running it and getting 200 rows for a 2-partition install.

### `CREATE FUNCTION stampeded_index_density()`

INDEX DENSITY

Reindex on a MEASUREMENT, not on a schedule: compute entries-per-page and
only rebuild what is actually sparse. The heuristic: at least 128 pages,
and an index is suspect below ~5 entries per page.

Those numbers were calibrated against v4 UUIDs, whose random keys defeat
btree deduplication. Ours are uuidv7 and time-ordered, so healthy density
should be HIGHER and this gate should hold with more margin -- inferred,
not measured, so treat a positive as a prompt to look rather than as a
verdict.

### `AND c.relname NOT LIKE '%\_ccnew%'`

Skip REINDEX CONCURRENTLY's leftovers rather than trying to rebuild
them: _ccnew/_ccold are transient artefacts and retrying one is how a
reindexer gets stuck in a loop.

### `AND t.relnamespace = current_schema()::regnamespace`

SCOPED TO THIS SCHEMA. pg_class spans the whole database, so without
this a second Stampeded install's indexes -- or, when this was first
run, a few hundred leftover test schemas -- are reported as ours.
1167 indexes for a two-partition install is how it was noticed.

### `CREATE FUNCTION stampeded_heartbeat(p_worker_id uuid) RETURNS void`

HEARTBEAT

### `CREATE FUNCTION stampeded_partition_health()`

Partitions that would enqueue silently.

The wakeup trigger is FOR EACH STATEMENT with a transition table, and
Postgres does NOT clone statement-level triggers to partitions -- verified
on 18.6: a `CREATE TABLE ... PARTITION OF stampeded_jobs` gets zero triggers,
and an INSERT straight into it notifies nobody. (Row-level triggers ARE
cloned, including to partitions created later, but a row-level wakeup
measured +3.5% per row against +1.2% for the statement form, which is why
the statement trigger was chosen.)

`stampeded_create_queue` attaches the trigger, so every partition made the
supported way is covered. This catches the unsupported way. The symptom it
prevents is nasty and quiet: jobs enqueued into that queue are CORRECT and
claimed on the poll interval, so nothing breaks -- latency just silently
becomes seconds instead of milliseconds, for one queue, for ever.

Detection rather than prevention, deliberately. An event trigger could
enforce it, at the cost of more plpgsql running on every DDL statement in
the database, to stop a mistake this reports in one query.

### `coalesce(`

The bound is `FOR VALUES IN ('name')`; pull the queue name back
out rather than assuming the table is named for its queue.

stampeded_jobs_default is the DEFAULT partition, not a partition
for a queue called "default" -- it catches every queue that has
none of its own. Labelling it "default" would be wrong in the
confusing direction: it would read as one queue among many when
it is the fallback for all of them.

## Recurring schedules

### `CREATE TABLE stampeded_schedules (`

Stampeded -- recurring schedules

Two mechanisms, each doing exactly one job.
Schedules -- the schedule row IS the due row.

`next_run_at` lives on the row and is claimed with the same
FOR UPDATE SKIP LOCKED used for jobs. The claimer enqueues the job and
advances next_run_at IN THE SAME TRANSACTION. That buys, for free:

* no leader election for cron at all -- the row lock IS the election;
* a missed occurrence runs late, once, because the overdue row is still
there when the process comes back;
* no drift, because the next time is computed from the previous target
rather than from "now";
* schedules queryable and mutable at runtime, rather than living in config.

### `interval_secs   bigint      CHECK (interval_secs > 0)`

Exactly one of the two. A cron expression is parsed in Rust (jiff does
the timezone arithmetic -- see crates/stampeded/src/cron.rs), which is
why the firing loop lives there rather than in PL/pgSQL; a fixed interval
needs no parser at all and is kept for the common "every N seconds" case.

### `dst_policy      text        NOT NULL DEFAULT 'instant'`

DST policy, per schedule.

If occurrences are keyed on a UTC instant, a wall-clock time that occurs
twice on a fall-back day produces two valid occurrences and the job runs
twice. Making this a column rather than an algorithm lets each schedule choose:

'instant'    -- key on the UTC instant. A 01:30 job runs twice on the
fall-back day, an hour of real time apart. Correct for
"every N hours" work.
'wall_clock' -- key on local date+time, which collapses the repeated
hour into one token. A 01:30 job runs ONCE on the
fall-back day; a 02:30 job on a spring-forward day runs
at 03:30, the instant the gap closes, rather than being
lost. Correct for "the daily report at 2:30am".

### `missed_policy   text        NOT NULL DEFAULT 'once'`

What to do about occurrences missed while nothing was running.
'once' runs the overdue occurrence a single time, however far behind it
is. 'skip' abandons it and waits for the next. Nobody wants 400 backfilled
runs after a long outage, so 'once' is the default and backfill is not
offered at all.

### `CONSTRAINT stampeded_schedules_args_not_json_null CHECK (jsonb_typeof(args) <> 'null')`

The same rule as `stampeded_jobs_args_not_json_null`, one hop earlier.
The scheduler copies these args onto every firing, and it fires every due
schedule in ONE transaction: a null here would trip the jobs constraint
mid-tick and roll back every other schedule's firing with it. Rejected at
write time instead, where it names the schedule that is wrong.

### `CREATE INDEX stampeded_schedules_due_idx`

The claim path: due, unpaused schedules, oldest first.

### `CREATE TABLE stampeded_cron_occurrences (`

Occurrence ledger -- idempotency backstop and audit trail.

Deliberately a SEPARATE NARROW TABLE, not a column on stampeded_jobs.
A cron key on the jobs table means guaranteeing cron rows are never
pruned. Coupling cron correctness to job retention would make the
retention horizon load-bearing for correctness. This table is prunable on
its own schedule.

### `occurrence_token  text        NOT NULL`

The DST policy made concrete. For 'instant' this is the UTC epoch
second; for 'wall_clock' it is the local 'YYYY-MM-DD HH24:MI' string,
which collapses the duplicated hour into one token.

### `UNIQUE (cron_key, occurrence_token)`

The backstop: two processes racing to enqueue the same occurrence, one
wins and one gets zero rows. Verified on 18.6.

### `CREATE FUNCTION stampeded_claim_due_schedules(p_limit integer DEFAULT 100)`

Firing, in two halves.

It used to be one PL/pgSQL loop. Cron expressions broke that: computing the
next occurrence needs a parser and a timezone database, and PL/pgSQL has
neither. Rather than write a cron parser in SQL, the LOOP moved to Rust and
the transaction stayed put:

BEGIN
stampeded_claim_due_schedules(n)   -- takes the row locks
...Rust computes each next_run_at with jiff...
stampeded_fire_schedule(key, next) -- once per schedule
COMMIT

The row lock is still the leader election, and it is held across the round
trip because it is all one transaction. Nothing about the concurrency
argument changed; only who does the arithmetic.
Take the locks. MUST be called inside a transaction: the locks are what stop
two nodes firing the same occurrence, and they last exactly as long as it.

### `CREATE FUNCTION stampeded_fire_schedule(p_key text, p_next_run_at timestamptz)`

Fire one claimed schedule and advance it. `p_next_run_at` comes from Rust.

Returns the job id, or NULL if this occurrence was already enqueued by
someone else -- in which case the schedule is still advanced, or we would spin
on it forever.

### `v_occ    bigint`

Separate from v_job: the ledger's id is bigserial, the job's is uuid.
Reusing one variable for both is a runtime cast error, not a compile one.

### `CREATE FUNCTION stampeded_prune_occurrences(p_older_than interval DEFAULT interval '30 days')`

Prune the ledger on its own horizon, independent of job retention.

### `, deleted AS (`

Counted, for the same reason as stampeded_prune: RETURNING 1 through a
RETURNS integer function is a constant, not a count.
