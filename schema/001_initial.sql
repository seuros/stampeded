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

    scheduled_at    timestamptz NOT NULL DEFAULT now(),

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
    dedup_key       bytea       GENERATED ALWAYS AS (sha256(dedup_scope::bytea)) STORED,

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

    inserted_at     timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,

    -- The PK must contain the partition key. `id` alone is not permitted on a
    -- partitioned table, so uuidv7's ordering is carried by the index below
    -- rather than by the PK.
    PRIMARY KEY (queue, id),

    CONSTRAINT stampede_jobs_state_valid CHECK (
        state IN ('available','claimed','executing','retryable','completed','discarded','cancelled')
    ),
    CONSTRAINT stampede_jobs_failure_kind_valid CHECK (
        failure_kind IS NULL OR failure_kind IN ('raised','timeout','interrupted','cancelled','unrecoverable')
    ),
    CONSTRAINT stampede_jobs_attempt_sane CHECK (attempt >= 0 AND attempt <= max_attempts)
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
CREATE FUNCTION stampede_notify_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('stampede_insert', queue)
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
END;
$$;
