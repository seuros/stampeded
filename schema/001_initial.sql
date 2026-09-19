CREATE TABLE stampeded_workers (
    id              uuid        PRIMARY KEY DEFAULT uuidv7(),
    hostname        text        NOT NULL,
    pid             integer     NOT NULL,
    queues          text[]      NOT NULL DEFAULT '{}',
    metadata        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    started_at      timestamptz NOT NULL DEFAULT now(),
    last_heartbeat  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX stampeded_workers_heartbeat_idx ON stampeded_workers (last_heartbeat);

CREATE TABLE stampeded_jobs (
    id              uuid        NOT NULL DEFAULT uuidv7(),
    queue           text        NOT NULL,
    tenant_id       text,

    task            text        NOT NULL,

    args            jsonb       NOT NULL DEFAULT '{}'::jsonb
                                CHECK (pg_column_size(args) <= 1048576),

    state           text        NOT NULL DEFAULT 'available',
    priority        smallint    NOT NULL DEFAULT 0,

    destination     text,

    scheduled_at    timestamptz NOT NULL DEFAULT now(),

    expire_seconds  integer     NOT NULL DEFAULT 3600 CHECK (expire_seconds > 0),

    heartbeat_at    timestamptz,

    attempt         smallint    NOT NULL DEFAULT 0,

    max_attempts    smallint    NOT NULL DEFAULT 25 CHECK (max_attempts >= 1),

    attempted_by    text[]      NOT NULL DEFAULT '{}',

    interruptions       smallint NOT NULL DEFAULT 0,
    max_interruptions   smallint NOT NULL DEFAULT 1,

    deferrals           smallint NOT NULL DEFAULT 0,
    max_deferrals       smallint NOT NULL DEFAULT 100,

    checkpoint      jsonb CHECK (checkpoint IS NULL OR pg_column_size(checkpoint) <= 65536),
    checkpoint_at   timestamptz,

    checkpoint_seq  integer     NOT NULL DEFAULT 0,

    backoff_base_ms smallint    NOT NULL DEFAULT 1000 CHECK (backoff_base_ms > 0),
    backoff_max_ms  integer     NOT NULL DEFAULT 3600000 CHECK (backoff_max_ms > 0),

    claimed_by      uuid,
    claimed_at      timestamptz,

    dedup_scope     text,

    dedup_period    text        CHECK (dedup_period IN
                                  ('minute','hour','day','week','month')),

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

    failure_kind    text,

    last_error      text        CHECK (length(last_error) <= 4000),

    errors          jsonb       NOT NULL DEFAULT '[]'::jsonb,

    error_fingerprint bytea,

    dead_letter_queue text,

    source_id       uuid,
    source_queue    text,
    source_attempts smallint,

    inserted_at     timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,

    PRIMARY KEY (queue, id),

    CONSTRAINT stampeded_jobs_state_valid CHECK (
        state IN ('available','claimed','executing','retryable','completed','discarded','cancelled')
    ),

    CONSTRAINT stampeded_jobs_failure_kind_valid CHECK (
        failure_kind IS NULL OR failure_kind IN (
            'raised',
            'timed_out',
            'heartbeat_lost',
            'worker_exited',
            'worker_missing',
            'breaker_open',
            'throttled',
            'cancelled',
            'unrecoverable'
        )
    ),
    CONSTRAINT stampeded_jobs_attempt_sane CHECK (attempt >= 0 AND attempt <= max_attempts),
    CONSTRAINT stampeded_jobs_interruptions_sane CHECK (interruptions >= 0),
    CONSTRAINT stampeded_jobs_deferrals_sane CHECK (deferrals >= 0),

    CONSTRAINT stampeded_jobs_dlq_not_self CHECK (
        dead_letter_queue IS NULL OR dead_letter_queue <> queue
    )
)
PARTITION BY LIST (queue);

CREATE TABLE stampeded_jobs_default PARTITION OF stampeded_jobs DEFAULT;

CREATE INDEX stampeded_jobs_claim_idx
    ON stampeded_jobs (queue, priority, scheduled_at)
    WHERE state IN ('available', 'retryable');

CREATE UNIQUE INDEX stampeded_jobs_dedup_idx
    ON stampeded_jobs (queue, dedup_key)
    WHERE dedup_key IS NOT NULL;

CREATE INDEX stampeded_jobs_claimed_idx
    ON stampeded_jobs (claimed_by)
    WHERE state IN ('claimed','executing');

CREATE INDEX stampeded_jobs_tenant_idx
    ON stampeded_jobs (tenant_id, state)
    WHERE tenant_id IS NOT NULL;

CREATE INDEX stampeded_jobs_finished_idx
    ON stampeded_jobs (finished_at)
    WHERE finished_at IS NOT NULL;

CREATE INDEX stampeded_jobs_fingerprint_idx
    ON stampeded_jobs (error_fingerprint)
    WHERE error_fingerprint IS NOT NULL;

CREATE INDEX stampeded_jobs_destination_idx
    ON stampeded_jobs (destination, state)
    WHERE destination IS NOT NULL;

CREATE INDEX stampeded_jobs_expiry_idx
    ON stampeded_jobs (claimed_at)
    WHERE state = 'executing';

ALTER TABLE stampeded_jobs_default SET (
    autovacuum_vacuum_scale_factor  = 0.02,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_cost_limit    = 2000,
    fillfactor                      = 85
);

CREATE FUNCTION stampeded_notify_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify(current_schema() || '.stampeded_insert', queue)
    FROM (SELECT DISTINCT queue FROM inserted
          WHERE state = 'available' AND scheduled_at <= now()) q;
    RETURN NULL;
END;
$$;

CREATE TRIGGER stampeded_notify_insert_trg
    AFTER INSERT ON stampeded_jobs
    REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT EXECUTE FUNCTION stampeded_notify_insert();

CREATE TRIGGER stampeded_notify_insert_trg
    AFTER INSERT ON stampeded_jobs_default
    REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT EXECUTE FUNCTION stampeded_notify_insert();

CREATE TABLE stampeded_queue_counts (
    queue   text     NOT NULL,
    state   text     NOT NULL,

    n       bigint   NOT NULL DEFAULT 0,
    PRIMARY KEY (queue, state)
);

CREATE FUNCTION stampeded_recount_queue(p_queue text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM stampeded_queue_counts WHERE queue = p_queue;
    INSERT INTO stampeded_queue_counts (queue, state, n)
    SELECT queue, state, count(*)
      FROM stampeded_jobs
     WHERE queue = p_queue
     GROUP BY queue, state;
END;
$$;

CREATE FUNCTION stampeded_count_delta() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO stampeded_queue_counts (queue, state, n)
        SELECT queue, state, count(*) FROM added GROUP BY queue, state
        -- Deterministic lock order, same reason as the UPDATE branch below.
        ORDER BY queue, state
        ON CONFLICT (queue, state) DO UPDATE SET n = stampeded_queue_counts.n + excluded.n;

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO stampeded_queue_counts (queue, state, n)
        SELECT queue, state, -count(*) FROM removed GROUP BY queue, state
        -- Deterministic lock order, same reason as the UPDATE branch below.
        ORDER BY queue, state
        ON CONFLICT (queue, state) DO UPDATE SET n = stampeded_queue_counts.n + excluded.n;

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
        INSERT INTO stampeded_queue_counts (queue, state, n)
        SELECT queue, state, d FROM net
        -- ORDER BY IS LOAD-BEARING: it prevents a deadlock.
        --
        -- A transition statement touches several (queue, state) rows at once --
        -- one decrement, one increment -- and without an order, concurrent
        -- transactions lock them in whatever order the CTE happened to emit.
        -- Worker A locks (default, executing) then (default, completed) while
        -- worker B does the reverse, and Postgres kills one of them with
        -- 40P01.
        --
        -- Reproduced: four workers completing 200 jobs concurrently deadlocked
        -- inside stampeded_complete every run. In production that is a worker
        -- failing to report a finished job, which then sits until the reaper
        -- notices -- a silent stall under exactly the load this design is for.
        ORDER BY queue, state
        ON CONFLICT (queue, state) DO UPDATE SET n = stampeded_queue_counts.n + excluded.n;
    END IF;

    RETURN NULL;
END;
$$;

CREATE FUNCTION stampeded_attach_counters(p_table text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format(
        'CREATE TRIGGER stampeded_count_ins_trg AFTER INSERT ON %I'
        '  REFERENCING NEW TABLE AS added'
        '  FOR EACH STATEMENT EXECUTE FUNCTION stampeded_count_delta()', p_table);
    EXECUTE format(
        'CREATE TRIGGER stampeded_count_upd_trg AFTER UPDATE ON %I'
        '  REFERENCING NEW TABLE AS added OLD TABLE AS removed'
        '  FOR EACH STATEMENT EXECUTE FUNCTION stampeded_count_delta()', p_table);
    EXECUTE format(
        'CREATE TRIGGER stampeded_count_del_trg AFTER DELETE ON %I'
        '  REFERENCING OLD TABLE AS removed'
        '  FOR EACH STATEMENT EXECUTE FUNCTION stampeded_count_delta()', p_table);
END;
$$;

DO $$ BEGIN
    PERFORM stampeded_attach_counters('stampeded_jobs');
    PERFORM stampeded_attach_counters('stampeded_jobs_default');
END $$;

CREATE FUNCTION stampeded_create_queue(p_queue text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    part_name text := 'stampeded_jobs_' || regexp_replace(p_queue, '[^a-zA-Z0-9_]', '_', 'g');
BEGIN
    IF to_regclass(part_name) IS NOT NULL THEN
        RETURN;  -- idempotent: provisioning an existing queue is a no-op
    END IF;

    EXECUTE format(
        'CREATE TABLE %I PARTITION OF stampeded_jobs FOR VALUES IN (%L)',
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
        'CREATE TRIGGER stampeded_notify_insert_trg'
        '  AFTER INSERT ON %I'
        '  REFERENCING NEW TABLE AS inserted'
        '  FOR EACH STATEMENT EXECUTE FUNCTION stampeded_notify_insert()',
        part_name);

    -- Counters are per-partition for the same reason the notify trigger is.
    PERFORM stampeded_attach_counters(part_name);
END;
$$;
