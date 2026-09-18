-- Stampede — initial schema
-- Postgres 18+ ONLY. No compatibility shims.
--
-- Every design choice here traces to a decision in ../plans/. Where a line
-- looks odd, the comment says why, because the non-obvious ones are non-obvious
-- for a reason and will otherwise be "simplified" into a bug.

-- ---------------------------------------------------------------------------
-- Workers — heartbeat liveness.
--
-- plans/04: orphan recovery is heartbeat-based, NOT a time lease. Every other
-- Postgres queue surveyed uses a lease (Oban OSS 60 min, Graphile 4 h, pg-boss
-- 15 min); solid_queue is the sole exception and the prior art here. A lease is
-- cheap and wrong the same way in all of them: correctness depends on guessing
-- the maximum job duration, and a job that outlives the guess runs twice.
-- ---------------------------------------------------------------------------
CREATE TABLE stampede_workers (
    id              uuid        PRIMARY KEY DEFAULT uuidv7(),
    hostname        text        NOT NULL,
    pid             integer     NOT NULL,
    queues          text[]      NOT NULL DEFAULT '{}',
    metadata        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    started_at      timestamptz NOT NULL DEFAULT now(),
    last_heartbeat  timestamptz NOT NULL DEFAULT now()
);

-- The reaper's only index.
CREATE INDEX stampede_workers_heartbeat_idx ON stampede_workers (last_heartbeat);

-- ---------------------------------------------------------------------------
-- Jobs — partitioned by QUEUE.
--
-- plans/README §1: partitioning by `state` is mutually exclusive with a unique
-- index on the dedup key (Postgres requires a unique constraint on a
-- partitioned table to contain every partition-key column, and a generated
-- column cannot BE a partition key). Uniqueness won; partitioning moved to
-- queue. This is pg-boss's model and why it leads all six of its indexes with
-- `name`.
--
-- Accepted consequence: no state-based pruning. Completed jobs share a
-- partition with live ones, so the pruner and autovacuum settings below are the
-- ONLY defence against unbounded growth. See plans/13.
-- ---------------------------------------------------------------------------
CREATE TABLE stampede_jobs (
    id              uuid        NOT NULL DEFAULT uuidv7(),
    queue           text        NOT NULL,
    tenant_id       text,

    task            text        NOT NULL,
    -- jsonb not bytea: inspectable in psql and searchable from the MCP control
    -- plane (plans/06). An opaque payload would gut `job_search`.
    args            jsonb       NOT NULL DEFAULT '{}'::jsonb,

    state           text        NOT NULL DEFAULT 'available',
    priority        smallint    NOT NULL DEFAULT 0,

    -- What this job talks to: an API host, a database, a third party. A
    -- FIRST-CLASS COLUMN, not a tag inside args (plans/15), because it is the
    -- circuit breaker's key and therefore has to be queryable: per-destination
    -- failure rates in the dashboard, failure_summary grouped by destination,
    -- and suppression of retries against a dependency already known to be down.
    -- Buried in args it could do none of that.
    destination     text,

    scheduled_at    timestamptz NOT NULL DEFAULT now(),

    -- Database-enforced timeout (pg-boss's expire_seconds). The in-process
    -- tokio timeout cannot fire when the worker is DEAD, which is precisely the
    -- case that needs it; this one does not care whether anybody is alive.
    -- Layer 2 of the three in plans/15.
    expire_seconds  integer     NOT NULL DEFAULT 3600 CHECK (expire_seconds > 0),
    -- Long-running jobs touch this so a slow job is distinguishable from a dead
    -- one (pg-boss touchJobs). Set at claim; NULL means never touched.
    heartbeat_at    timestamptz,

    -- Attempt accounting. plans/04: `attempt` is incremented AT CLAIM, never at
    -- failure. Half the twelve implementations surveyed have an unbounded
    -- poison loop, and this single line is the difference: if the counter only
    -- advances when the worker survives to report, a job that segfaults its
    -- worker is re-claimed forever and max_attempts is never reached.
    attempt         smallint    NOT NULL DEFAULT 0,
    max_attempts    smallint    NOT NULL DEFAULT 25,
    -- Bounded ring, trimmed on write (River's trick). Unbounded attempt history
    -- is a slow leak on heavily-retried jobs.
    attempted_by    text[]      NOT NULL DEFAULT '{}',

    -- TWO counters, not one (plans/15). `attempt` counts raises; this counts
    -- the worker-death kinds — BullMQ's `stc` with maxStalledCount: 1.
    --
    -- One free interruption, then discard. A job that kills its worker gets
    -- exactly one more chance, because otherwise it walks the entire fleet:
    -- claimed, kills a worker, reclaimed by the next, kills that one too. The
    -- attempt counter cannot express this, since a job that dies mid-execution
    -- is not the same event as a job that raised, and charging both to one
    -- budget makes a crash-looping job indistinguishable from a flaky one.
    interruptions       smallint NOT NULL DEFAULT 0,
    max_interruptions   smallint NOT NULL DEFAULT 1,

    -- A THIRD counter, and plans/15 asks whether that is one too many. It is
    -- not, because without it the refund rule above has no floor:
    -- `breaker_open` and `throttled` give the attempt back, so a permanently
    -- open circuit re-claims and re-defers the same job forever. Verified: 12
    -- cycles against a max_attempts of 3 left attempt at 0. The queue grows
    -- silently and nothing ever reports a failure.
    --
    -- So deferrals are counted, with a DELIBERATELY GENEROUS cap. This is not
    -- a retry budget and must not behave like one: at the capped backoff of an
    -- hour, 100 deferrals is days of downstream outage before anything is
    -- given up on. A real outage must never reach it; a destination that is
    -- permanently gone must.
    deferrals           smallint NOT NULL DEFAULT 0,
    max_deferrals       smallint NOT NULL DEFAULT 100,

    -- Backoff policy on the ROW (underway's shape), so the dashboard and MCP
    -- can render the remaining retry schedule without re-deriving it from a
    -- Rust constant they cannot see. Cap first, then jitter.
    backoff_base_ms smallint    NOT NULL DEFAULT 1000 CHECK (backoff_base_ms > 0),
    backoff_max_ms  integer     NOT NULL DEFAULT 3600000 CHECK (backoff_max_ms > 0),

    claimed_by      uuid,
    claimed_at      timestamptz,

    -- Deduplication. plans/10 + README §1.
    -- dedup_scope is client-supplied and nullable; NULL means "not unique".
    -- It stays readable text so psql and the MCP tools can explain WHY two jobs
    -- collided — the hash exists only to bound the index key.
    --
    -- NOT sha256(convert_to(...)): convert_to is STABLE, not IMMUTABLE, and is
    -- rejected in a generated column. The ::bytea cast IS immutable. Verified.
    dedup_scope     text,

    -- Optional time bucket: "one of these per hour", not "one of these ever".
    --
    -- Computed SERVER-SIDE. The client could compose the bucket into
    -- dedup_scope itself, but then two enqueuers with skewed clocks land in
    -- different buckets and the limit quietly stops holding -- which is why
    -- pg-boss computes its slot server-side too.
    --
    -- BUCKETED ON inserted_at, NOT scheduled_at. plans/10 verified the trap: a
    -- STORED key bucketed on scheduled_at silently CHANGES when a job is
    -- snoozed or rescheduled, vacating its own throttle slot and letting a
    -- duplicate in. inserted_at is never updated, so the key is stable for the
    -- row's whole life. River is exposed to this by design; Oban avoids it by
    -- defaulting to inserted_at.
    --
    -- Honest limitation: buckets are fixed windows, not sliding ones. Two jobs
    -- 60 seconds apart can land either side of an hour boundary and both run. A
    -- sliding window needs `ts >= now() - period`, which is not immutable, so
    -- it cannot live in an index or a generated column at all -- it needs a
    -- read-modify-write per enqueue, and that needs the client to run our code,
    -- which is exactly what the polyglot INSERT rules out.
    dedup_period    text        CHECK (dedup_period IN
                                  ('minute','hour','day','week','month')),

    -- NOT sha256(convert_to(...)): convert_to is STABLE, not IMMUTABLE, and is
    -- rejected in a generated column. The ::bytea cast IS immutable. Verified.
    --
    -- The bucket chain is fussier than it looks, and every step was checked:
    -- two-argument date_trunc is STABLE (it reads the session TimeZone), the
    -- three-argument form with an explicit zone is IMMUTABLE, and casting a
    -- timestamptz to text is STABLE again (DateStyle) -- so the truncated time
    -- is turned into a number by subtracting the epoch, which yields an
    -- interval, and extract on an INTERVAL is immutable. Skipping any of that
    -- gets "generation expression is not immutable" with no hint as to which
    -- part.
    dedup_key       bytea       GENERATED ALWAYS AS (
        sha256((
            dedup_scope || coalesce(
                ':' || extract(
                    epoch FROM (date_trunc(dedup_period, inserted_at, 'UTC') - to_timestamp(0))
                )::bigint::text,
                ''
            )
        )::bytea)
    ) STORED,

    -- Failure taxonomy, borrowed from good_job's error_event enum — the best in
    -- the survey. Distinguishing "the worker died" from "the job raised" is the
    -- thing almost nobody does (only BullMQ and solid_queue), and it is what
    -- makes poison jobs diagnosable.
    failure_kind    text,
    -- Truncated in SQL, not in client code. plans/15: que is the only surveyed
    -- implementation that bounds error text database-side, and it is the only
    -- bound that survives a polyglot worker — a Python or Go worker will not
    -- run our Rust truncation logic.
    last_error      text        CHECK (length(last_error) <= 4000),

    -- Structured error history, not a formatted blob. Oban stores
    -- `Exception.format/3` output, which is readable and machine-useless.
    -- Each element: {at, attempt, kind, class, message, backtrace, destination}.
    --
    -- Bounded like attempted_by. River bounds attempted_by but appends errors
    -- UNBOUNDED (river_job.sql:482) — half a fix, and the half that leaks, since
    -- error text dwarfs a worker id.
    errors          jsonb       NOT NULL DEFAULT '[]'::jsonb,

    -- Normalised error class + message with numbers, UUIDs and hex stripped,
    -- then hashed. Indexed. This is the difference between `failure_summary`
    -- being an index scan and being a seq scan over every error string in the
    -- table, and nobody in the survey does it.
    error_fingerprint bytea,

    -- Dead letter. pg-boss's design: the DLQ is a QUEUE, so re-drive is just
    -- working that queue and every existing tool applies, rather than a bespoke
    -- mutation path nothing else understands.
    dead_letter_queue text,
    -- Provenance, set on the job that LANDS in the dead-letter queue.
    source_id       uuid,
    source_queue    text,
    source_attempts smallint,

    inserted_at     timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,

    -- The PK must contain the partition key. `id` alone is not permitted on a
    -- partitioned table, so uuidv7's ordering is carried by the index below
    -- rather than by the PK.
    PRIMARY KEY (queue, id),

    CONSTRAINT stampede_jobs_state_valid CHECK (
        state IN ('available','claimed','executing','retryable','completed','discarded','cancelled')
    ),
    -- The taxonomy from plans/15. good_job's error_event enum proves the
    -- queryability is worth having; solid_queue proves the categories are
    -- derivable in practice; nobody has both. The distinctions that earn their
    -- keep: worker_exited carries an exit status (137 = OOM, the single
    -- highest-signal artefact in the survey), and breaker_open is ours alone —
    -- it means WE refused to run the job, so it must not be charged for it.
    CONSTRAINT stampede_jobs_failure_kind_valid CHECK (
        failure_kind IS NULL OR failure_kind IN (
            'raised',          -- ordinary exception
            'timed_out',       -- exceeded expire_seconds or the in-process timeout
            'heartbeat_lost',  -- stopped touching a long-running job
            'worker_exited',   -- process died with a status we captured
            'worker_missing',  -- worker vanished without exiting cleanly
            'breaker_open',    -- downstream is sick; our backpressure, NOT the job's fault
            'throttled',       -- tenant over its rate limit; also ours, also not a fault
            'cancelled',       -- operator action
            'unrecoverable'    -- will never succeed: unknown task, bad args
        )
    ),
    CONSTRAINT stampede_jobs_attempt_sane CHECK (attempt >= 0 AND attempt <= max_attempts),
    CONSTRAINT stampede_jobs_interruptions_sane CHECK (interruptions >= 0),
    CONSTRAINT stampede_jobs_deferrals_sane CHECK (deferrals >= 0),
    -- A job cannot dead-letter onto its own queue: that is an infinite loop
    -- wearing a feature's clothing.
    CONSTRAINT stampede_jobs_dlq_not_self CHECK (
        dead_letter_queue IS NULL OR dead_letter_queue <> queue
    )
)
PARTITION BY LIST (queue);
-- NO storage parameters on the parent: Postgres rejects them outright
-- ("cannot specify storage parameters for a partitioned table"), and they are
-- NOT inherited by partitions nor copied by LIKE INCLUDING ALL. Verified.
--
-- OPERATIONAL CONSEQUENCE: fillfactor and autovacuum settings must be applied
-- to EVERY partition individually, including every queue partition created at
-- runtime. A queue provisioned without them silently runs on defaults
-- (autovacuum_vacuum_scale_factor 0.2), which on a large partition means vacuum
-- waits for 20% dead tuples. That is the bloat failure mode in plans/13,
-- reintroduced one queue at a time. The partition-creation path MUST set them;
-- see stampede_create_queue_partition below.

-- Catches any queue not explicitly provisioned. Queues are created at runtime,
-- so each new queue wants its own partition (pg-boss does this in
-- src/plans.ts:1439-1494); DEFAULT means an unprovisioned queue still works.
CREATE TABLE stampede_jobs_default PARTITION OF stampede_jobs DEFAULT;

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

-- The claim path. Partial, because only `available` rows are ever claimed and
-- the completed tail lives in the same partition (see the partitioning note).
-- plans/11 measured the cost of NOT having this on Oban: a parallel seq scan,
-- 18,430 buffers / 54 ms, versus 11 buffers / 0.165 ms with the partial index.
--
-- scheduled_at is deliberately NOT in a generated `is_ready` column: generation
-- expressions must be IMMUTABLE and anything with now() is rejected; virtual
-- generated columns also cannot be indexed at all. Verified twice. plans/03.
-- Must cover BOTH claimable states. A retryable job whose backoff has elapsed
-- is claimable (see stampede_claim in 002); indexing only 'available' would
-- make every retry a seq scan.
CREATE INDEX stampede_jobs_claim_idx
    ON stampede_jobs (queue, priority, scheduled_at, id)
    WHERE state IN ('available', 'retryable');

-- Uniqueness. Partial: a full constraint costs 1272 kB per 200k non-dedup rows
-- versus 16 kB partial (plans/10). Led by `queue` because the partition key
-- must be present — which is also what makes uniqueness per-queue.
CREATE UNIQUE INDEX stampede_jobs_dedup_idx
    ON stampede_jobs (queue, dedup_key)
    WHERE dedup_key IS NOT NULL;

-- Orphan reaping: find rows whose worker stopped heartbeating.
CREATE INDEX stampede_jobs_claimed_idx
    ON stampede_jobs (claimed_by)
    WHERE state IN ('claimed','executing');

-- Tenant fairness and MCP queries (plans/06).
CREATE INDEX stampede_jobs_tenant_idx
    ON stampede_jobs (tenant_id, state)
    WHERE tenant_id IS NOT NULL;

-- Pruning and the "what failed recently" MCP tool.
CREATE INDEX stampede_jobs_finished_idx
    ON stampede_jobs (finished_at)
    WHERE finished_at IS NOT NULL;

-- `failure_summary` clusters by fingerprint. Without this index that tool is a
-- seq scan over every error string in the table, which is exactly the query an
-- operator runs while the incident is ongoing and the table is at its largest.
CREATE INDEX stampede_jobs_fingerprint_idx
    ON stampede_jobs (error_fingerprint)
    WHERE error_fingerprint IS NOT NULL;

-- Per-destination health, for the breaker and the dashboard.
CREATE INDEX stampede_jobs_destination_idx
    ON stampede_jobs (destination, state)
    WHERE destination IS NOT NULL;

-- The expiry sweep: executing jobs whose wall-clock budget has run out.
-- Partial, because only executing rows can expire.
CREATE INDEX stampede_jobs_expiry_idx
    ON stampede_jobs (claimed_at)
    WHERE state = 'executing';

-- ---------------------------------------------------------------------------
-- Autovacuum
--
-- plans/13: a queue table is the pathological autovacuum case — constant
-- UPDATEs over a small hot working set plus a large finished tail. Defaults
-- (scale_factor 0.2) mean vacuum waits for 20% of the table to be dead, which
-- on a large table is far too late. Numbers are Oban's documented production
-- values.
-- ---------------------------------------------------------------------------
ALTER TABLE stampede_jobs_default SET (
    autovacuum_vacuum_scale_factor  = 0.02,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_cost_limit    = 2000,
    fillfactor                      = 85
);

-- ---------------------------------------------------------------------------
-- Wakeup notification
--
-- plans/12: STATEMENT-level, not per-row. Measured on 18.6: the per-row form
-- that River, procrastinate and underway ship costs +71% on a 50k-row insert;
-- this costs +1.2%. River removed its trigger citing notification storms
-- (CHANGELOG.md:922) — that objection is specific to the per-row form.
--
-- Payload is the bare queue name: Postgres collapses identical
-- (channel, payload) pairs within a transaction, so a 1500-row insert across
-- two queues emits exactly two notifications. That makes it self-throttling and
-- makes the 8000-byte payload cap irrelevant. Graphile's {"r":random()} nonce
-- exists to DEFEAT that collapsing and would break both properties.
--
-- This is why enqueue can stay a plain INSERT from any language (plans/05):
-- the database notifies, not the client.
-- ---------------------------------------------------------------------------
-- The channel is SCHEMA-NAMESPACED. Channel names are global per database, so
-- two Stampede installs in different schemas of one database would wake each
-- other's workers for queues they do not serve. River (CHANGELOG.md:926) and
-- pg-boss (src/plans.ts:841-858) both shipped the bare name first and had to
-- fix it; there is no reason to repeat that.
--
-- Budget: a channel name is capped at 63 bytes, so the schema name has
-- 63 - length('.stampede_insert') = 46 to play with. Verified.
CREATE FUNCTION stampede_notify_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify(current_schema() || '.stampede_insert', queue)
    FROM (SELECT DISTINCT queue FROM inserted
          WHERE state = 'available' AND scheduled_at <= now()) q;
    RETURN NULL;
END;
$$;

-- NOTE: the trigger must exist on the parent AND on every partition. Inserting
-- directly into a partition silently bypasses a parent-only trigger — a real
-- hole in the "enqueue is a plain INSERT" promise (plans/12). Duplicate
-- delivery is free because of the payload collapsing described above.
CREATE TRIGGER stampede_notify_insert_trg
    AFTER INSERT ON stampede_jobs
    REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT EXECUTE FUNCTION stampede_notify_insert();

CREATE TRIGGER stampede_notify_insert_trg
    AFTER INSERT ON stampede_jobs_default
    REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT EXECUTE FUNCTION stampede_notify_insert();

-- ---------------------------------------------------------------------------
-- Queue counters
--
-- plans/13 item 6, and the single most important performance rule in that file:
-- NEVER run an unbounded aggregate over the job table.
--
-- `SELECT count(*) FROM stampede_jobs GROUP BY state` looks harmless and is the
-- documented pg-boss death spiral (src/plans.ts:968-975). A long aggregate holds
-- the xmin horizon open; vacuum cannot reclaim dead tuples below that horizon;
-- the table bloats; the aggregate gets slower; it holds the horizon longer. The
-- queue does not fall over, it degrades — which is worse, because it presents as
-- "mysteriously slow backlog" rather than as an error.
--
-- Anything that wants a count reads THIS table: the dashboard refreshing every
-- second, `queue_status` over MCP, the depth API, every metric.
--
-- Rows are (queue, state) pairs maintained by a statement-level trigger. Small,
-- fixed cardinality: queues x 7 states.
-- ---------------------------------------------------------------------------
CREATE TABLE stampede_queue_counts (
    queue   text     NOT NULL,
    state   text     NOT NULL,
    -- Signed: a counter is allowed to go transiently negative if a row is
    -- deleted by a path the trigger did not see. Clamping it to zero would hide
    -- exactly the drift that stampede_recount_queue exists to repair.
    n       bigint   NOT NULL DEFAULT 0,
    PRIMARY KEY (queue, state)
);

-- Counters drift. Any correction is a bug, but bugs happen, and a counter that
-- cannot be repaired without downtime is worse than one that can.
--
-- THIS is the unbounded aggregate the rest of the system is forbidden from
-- running. It is acceptable here because it is explicit, manual and rare —
-- not because it is cheap. Do not call it on a timer.
CREATE FUNCTION stampede_recount_queue(p_queue text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM stampede_queue_counts WHERE queue = p_queue;
    INSERT INTO stampede_queue_counts (queue, state, n)
    SELECT queue, state, count(*)
      FROM stampede_jobs
     WHERE queue = p_queue
     GROUP BY queue, state;
END;
$$;

-- Maintained by transition deltas, statement-level, using the same transition
-- tables as the notify trigger. Per-row would put a plpgsql call on every row of
-- every bulk insert, which is the +71% figure plans/12 measured.
--
-- Branches on TG_OP because a transition table only EXISTS for the operation it
-- belongs to: an INSERT trigger may declare NEW TABLE and not OLD TABLE, a
-- DELETE trigger the reverse. Referencing the absent one is a create-time error.
CREATE FUNCTION stampede_count_delta() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO stampede_queue_counts (queue, state, n)
        SELECT queue, state, count(*) FROM added GROUP BY queue, state
        ON CONFLICT (queue, state) DO UPDATE SET n = stampede_queue_counts.n + excluded.n;

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO stampede_queue_counts (queue, state, n)
        SELECT queue, state, -count(*) FROM removed GROUP BY queue, state
        ON CONFLICT (queue, state) DO UPDATE SET n = stampede_queue_counts.n + excluded.n;

    ELSE
        -- Both directions netted before they touch the table, so a 10k-row
        -- state change is one UPSERT per (queue, state) rather than 10k. The
        -- HAVING drops rows whose state did not change, which is most of them:
        -- a heartbeat UPDATE moves no job between states and should cost
        -- nothing here.
        WITH delta AS (
            SELECT queue, state, -count(*)::bigint AS d FROM removed GROUP BY queue, state
            UNION ALL
            SELECT queue, state,  count(*)::bigint     FROM added   GROUP BY queue, state
        ),
        net AS (
            SELECT queue, state, sum(d) AS d FROM delta GROUP BY queue, state HAVING sum(d) <> 0
        )
        INSERT INTO stampede_queue_counts (queue, state, n)
        SELECT queue, state, d FROM net
        ON CONFLICT (queue, state) DO UPDATE SET n = stampede_queue_counts.n + excluded.n;
    END IF;

    RETURN NULL;
END;
$$;

-- Attached per PARTITION, not to the parent: a statement-level trigger with
-- transition tables on a partitioned root does not see rows routed to leaves,
-- and a direct INSERT into a partition would bypass a parent-only trigger
-- anyway -- the same hole the notify trigger closes the same way.
CREATE FUNCTION stampede_attach_counters(p_table text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format(
        'CREATE TRIGGER stampede_count_ins_trg AFTER INSERT ON %I'
        '  REFERENCING NEW TABLE AS added'
        '  FOR EACH STATEMENT EXECUTE FUNCTION stampede_count_delta()', p_table);
    EXECUTE format(
        'CREATE TRIGGER stampede_count_upd_trg AFTER UPDATE ON %I'
        '  REFERENCING NEW TABLE AS added OLD TABLE AS removed'
        '  FOR EACH STATEMENT EXECUTE FUNCTION stampede_count_delta()', p_table);
    EXECUTE format(
        'CREATE TRIGGER stampede_count_del_trg AFTER DELETE ON %I'
        '  REFERENCING OLD TABLE AS removed'
        '  FOR EACH STATEMENT EXECUTE FUNCTION stampede_count_delta()', p_table);
END;
$$;

-- BOTH, exactly like the notify trigger, and for the same reason with the
-- opposite failure mode. A statement-level trigger fires for the table the
-- statement NAMED: inserting into the parent fires only the parent's, inserting
-- straight into a partition fires only that partition's. Attach one and the
-- other path is silently uncounted.
--
-- Verified that this does NOT double-count: 5 rows through the parent then 3
-- straight into the partition gave 8, not 13.
DO $$ BEGIN
    PERFORM stampede_attach_counters('stampede_jobs');
    PERFORM stampede_attach_counters('stampede_jobs_default');
END $$;

-- ---------------------------------------------------------------------------
-- Queue provisioning
--
-- Queues are created at runtime, and a partition needs THREE things applied
-- that are not inherited from the parent: storage parameters, autovacuum
-- settings, and the notify trigger. Forgetting any one of them fails silently —
-- the queue works, and then bloats, or stops waking workers.
--
-- So provisioning is a function, not a runbook step. Callable from any
-- language, which keeps it consistent with the polyglot stance in plans/05.
-- ---------------------------------------------------------------------------
CREATE FUNCTION stampede_create_queue(p_queue text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    part_name text := 'stampede_jobs_' || regexp_replace(p_queue, '[^a-zA-Z0-9_]', '_', 'g');
BEGIN
    IF to_regclass(part_name) IS NOT NULL THEN
        RETURN;  -- idempotent: provisioning an existing queue is a no-op
    END IF;

    EXECUTE format(
        'CREATE TABLE %I PARTITION OF stampede_jobs FOR VALUES IN (%L)',
        part_name, p_queue);

    -- Not inherited from the parent. See the note on the parent table.
    EXECUTE format(
        'ALTER TABLE %I SET ('
        '  fillfactor = 85,'
        '  autovacuum_vacuum_scale_factor = 0.02,'
        '  autovacuum_analyze_scale_factor = 0.02,'
        '  autovacuum_vacuum_cost_limit = 2000)',
        part_name);

    -- Direct inserts into a partition bypass a parent-only trigger.
    EXECUTE format(
        'CREATE TRIGGER stampede_notify_insert_trg'
        '  AFTER INSERT ON %I'
        '  REFERENCING NEW TABLE AS inserted'
        '  FOR EACH STATEMENT EXECUTE FUNCTION stampede_notify_insert()',
        part_name);

    -- Counters are per-partition for the same reason the notify trigger is.
    PERFORM stampede_attach_counters(part_name);
END;
$$;

