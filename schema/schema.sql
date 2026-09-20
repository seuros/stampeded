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

CREATE FUNCTION stampeded_count_truncate() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM stampeded_queue_counts;
    INSERT INTO stampeded_queue_counts (queue, state, n)
    SELECT queue, state, count(*) FROM stampeded_jobs GROUP BY queue, state;
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
    EXECUTE format(
        'CREATE TRIGGER stampeded_count_trunc_trg AFTER TRUNCATE ON %I'
        '  FOR EACH STATEMENT EXECUTE FUNCTION stampeded_count_truncate()', p_table);
END;
$$;

DO $$ BEGIN
    PERFORM stampeded_attach_counters('stampeded_jobs');
    PERFORM stampeded_attach_counters('stampeded_jobs_default');
END $$;

-- Per-queue durability.
--
-- A partition may be UNLOGGED while its parent is permanent, so one queue can
-- trade crash-safety for throughput while its neighbours do not: a metrics or
-- cache-warming queue can be unlogged, a payments queue cannot. An UNLOGGED
-- table is TRUNCATED after an unclean shutdown -- every job in that queue is
-- gone, not merely delayed.
--
-- MEASURED, and the obvious version of this does nothing. One worker, 8
-- threads, against a real 8.7 GB database with synchronous_commit ON:
--
--   logged queue                              792 jobs/sec
--   UNLOGGED queue, logged counters           734 jobs/sec   <-- no gain
--   UNLOGGED queue, UNLOGGED counters       4,565 jobs/sec   <-- 5.8x
--
-- The bottleneck is the COMMIT, not the table. Completing a job also updates
-- stampeded_queue_counts through the counter trigger, so while that table is
-- permanent every commit still flushes WAL and every job still pays an fsync.
-- Unlogging the jobs and leaving the counters is the version that looks right
-- and buys nothing.
--
-- So an unlogged queue is only fast if the counters are unlogged too, which is
-- defensible because they are a derived cache: stampeded_recount_queue()
-- rebuilds them from the jobs table. That is a GLOBAL decision, though -- the
-- counters table is shared -- so it is left to the operator rather than
-- implied by creating one unlogged queue:
--
--   SELECT stampeded_create_queue('metrics', p_unlogged => true);
--   ALTER TABLE stampeded_queue_counts SET UNLOGGED;   -- deliberate, global
CREATE FUNCTION stampeded_create_queue(p_queue text, p_unlogged boolean DEFAULT false)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    part_name text := 'stampeded_jobs_' || regexp_replace(p_queue, '[^a-zA-Z0-9_]', '_', 'g');
BEGIN
    IF to_regclass(part_name) IS NOT NULL THEN
        RETURN;  -- idempotent: provisioning an existing queue is a no-op
    END IF;

    EXECUTE format(
        'CREATE %s TABLE %I PARTITION OF stampeded_jobs FOR VALUES IN (%L)',
        CASE WHEN p_unlogged THEN 'UNLOGGED' ELSE '' END, part_name, p_queue);

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

CREATE FUNCTION stampeded_claim(
    p_queue     text,
    p_worker_id uuid,
    p_limit     integer DEFAULT 1,

    p_now       timestamptz DEFAULT NULL
) RETURNS SETOF stampeded_jobs
LANGUAGE sql AS $$
    WITH candidate AS MATERIALIZED (
        SELECT id
          FROM stampeded_jobs
         WHERE queue        = p_queue
           -- 'retryable' is claimable once its backoff has elapsed. plans/11
           -- catalogues three ways to handle this: a claim predicate
           -- (que/good_job/pg-boss/underway), a separate promotion pass (Oban's
           -- stage_jobs, solid_queue, asynq), or both (River). The predicate is
           -- chosen here for having no moving parts -- a promotion pass is
           -- another process that can fall behind or die.
           --
           -- Omitting 'retryable' here is not a missing feature, it is a silent
           -- poison-job bug: the reaper parks a crashed job in 'retryable', a
           -- claim that only reads 'available' never picks it up again, the
           -- attempt budget never walks down, and max_attempts is never
           -- reached. Found by test, not by reading.
           AND state        IN ('available','retryable')
           AND scheduled_at <= coalesce(p_now, now())
           AND attempt      < max_attempts
         -- Must match stampeded_jobs_claim_idx EXACTLY. `id` is not a
         -- tiebreaker here: adding one reintroduces a sort unless it is also
         -- added to the index, and in the index it costs 8.5x the size by
         -- defeating btree deduplication. See the index comment in 001.
         ORDER BY priority, scheduled_at
         LIMIT p_limit
         FOR UPDATE SKIP LOCKED
    )
    UPDATE stampeded_jobs j
       SET state        = 'executing',
           attempt      = j.attempt + 1,
           -- Seed the job heartbeat at claim so stampeded_expire has a baseline
           -- for a job that never calls stampeded_touch. Left NULL it would fall
           -- back to claimed_at, which is the same thing -- but only by accident,
           -- and the expiry predicate should not depend on that coincidence.
           heartbeat_at = coalesce(p_now, now()),
           claimed_by   = p_worker_id,
           claimed_at   = coalesce(p_now, now()),
           -- Bounded ring buffer (River). Unbounded attempt history is a slow
           -- leak on heavily-retried jobs; keep the last 10.
           attempted_by = array_append(
               CASE WHEN coalesce(array_length(j.attempted_by, 1), 0) >= 10
                    THEN j.attempted_by[coalesce(array_length(j.attempted_by,1),0) - 8 :]
                    ELSE j.attempted_by
               END,
               p_worker_id::text)
      FROM candidate c
     WHERE j.queue = p_queue
       AND j.id    = c.id
    RETURNING j.*;
$$;

CREATE FUNCTION stampeded_complete(
    p_queue     text,
    p_id        uuid,
    p_worker_id uuid,
    p_attempt   smallint
) RETURNS TABLE (was text, now_state text, owned boolean)
LANGUAGE sql AS $$
    UPDATE stampeded_jobs
       SET state       = 'completed',
           finished_at = now(),
           dedup_scope = NULL,
           -- Dead weight once the job is done, on the one table that already
           -- has the bloat problem. Consistent with the fire-and-forget
           -- decision: we do not keep results either.
           checkpoint  = NULL,
           claimed_by  = NULL
     WHERE queue = p_queue
       AND id    = p_id
       AND claimed_by = p_worker_id   -- refuse to complete a job we no longer own
         AND attempt    = p_attempt     -- ...and refuse a PREVIOUS attempt's result
    RETURNING OLD.state, NEW.state, true;
$$;

CREATE FUNCTION stampeded_fingerprint(p_error text) RETURNS bytea
LANGUAGE sql IMMUTABLE AS $$
    SELECT sha256(convert_to(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              lower(coalesce(p_error, '')),
              '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', 'U', 'g'),
            '0x[0-9a-f]+|\m[0-9a-f]{16,}\M', 'X', 'g'),
          '[0-9]+', 'N', 'g'),
        'UTF8')::bytea);
$$;

CREATE FUNCTION stampeded_fail(
    p_queue        text,
    p_id           uuid,
    p_worker_id    uuid,
    p_attempt      smallint,
    p_kind         text,
    p_error        text,

    p_retry_delay_ms bigint DEFAULT NULL,

    p_exit_status  integer DEFAULT NULL
) RETURNS TABLE (was text, now_state text, dead_lettered boolean)
LANGUAGE plpgsql AS $$
DECLARE
    j            stampeded_jobs%ROWTYPE;
    v_was        text;
    v_state      text;
    v_interrupt  boolean;
    v_refund     boolean;
    v_terminal   boolean;
    v_dlq        boolean := false;
BEGIN
    SELECT * INTO j FROM stampeded_jobs
     WHERE queue = p_queue AND id = p_id AND claimed_by = p_worker_id
       AND attempt = p_attempt;
    IF NOT FOUND THEN
        -- Not ours: the reaper already reclaimed it and somebody else owns the
        -- outcome. Writing a failure over another worker's result would be
        -- worse than doing nothing.
        RETURN;
    END IF;

    v_was := j.state;

    -- Did the worker die, rather than the job fail?
    v_interrupt := p_kind IN ('heartbeat_lost', 'worker_exited', 'worker_missing');
    -- Did WE refuse to run it?
    -- Both are US declining to run the job: one because the downstream is
    -- sick, one because the tenant is over its share. Kept as separate kinds
    -- because the operator response differs -- "the API is down" and "this
    -- customer is flooding us" want different people woken up.
    v_refund    := p_kind IN ('breaker_open', 'throttled');

    -- The correctness rule from plans/15: a failure caused by our own
    -- backpressure must not count against the job's retry budget. The attempt
    -- was spent at claim, so declining to run the job has to give it back --
    -- otherwise a downstream outage silently burns every job's budget and a
    -- fleet-wide breaker trip discards the entire backlog. This is Oban's
    -- `snooze` (which decrements) and River's EventKindJobInterrupted (which
    -- "does not consume an attempt or add an attempt error").
    v_terminal := CASE
        -- A deferral is terminal only when the job has been waiting on a
        -- dependency for so long that the dependency is evidently not coming
        -- back. Everything else about the refund still applies: the attempt is
        -- returned, because none of this was the job's doing.
        WHEN v_refund    THEN j.deferrals + 1 > j.max_deferrals
        WHEN p_kind = 'unrecoverable' THEN true
        WHEN v_interrupt THEN j.interruptions + 1 > j.max_interruptions
        ELSE j.attempt >= j.max_attempts
    END;

    v_state := CASE WHEN v_terminal THEN 'discarded' ELSE 'retryable' END;

    UPDATE stampeded_jobs
       SET state        = v_state,
           failure_kind = p_kind,
           attempt      = CASE WHEN v_refund THEN greatest(attempt - 1, 0) ELSE attempt END,
           interruptions = CASE WHEN v_interrupt THEN interruptions + 1 ELSE interruptions END,
           deferrals    = CASE WHEN v_refund THEN deferrals + 1 ELSE deferrals END,
           -- Truncated in SQL, not in client code: que is the only surveyed
           -- implementation that bounds error text database-side, and it is the
           -- only bound that survives a polyglot worker (plans/15).
           last_error   = left(p_error, 4000),
           error_fingerprint = stampeded_fingerprint(p_error),
           -- Ring buffer, newest last, capped at 10. River bounds attempted_by
           -- but appends errors unbounded -- the half that actually leaks,
           -- since error text dwarfs a worker id.
           errors = (
               SELECT coalesce(jsonb_agg(e), '[]'::jsonb) FROM (
                   SELECT e FROM jsonb_array_elements(
                       errors || jsonb_build_object(
                           'at',          now(),
                           'attempt',     j.attempt,
                           'kind',        p_kind,
                           'message',     left(p_error, 1000),
                           'destination', j.destination,
                           'exit_status', p_exit_status
                       )
                   ) e OFFSET greatest(jsonb_array_length(errors) + 1 - 10, 0)
               ) t
           ),
           scheduled_at = CASE
                            WHEN p_retry_delay_ms IS NULL THEN scheduled_at
                            ELSE now() + make_interval(secs => p_retry_delay_ms / 1000.0)
                          END,
           claimed_by   = NULL,
           claimed_at   = NULL,
           heartbeat_at = NULL,
           -- Release the dedup slot only on a terminal outcome; a retryable job
           -- keeps its slot so a duplicate cannot be enqueued while it waits.
           dedup_scope  = CASE WHEN v_terminal THEN NULL ELSE dedup_scope END,
           finished_at  = CASE WHEN v_terminal THEN now() ELSE NULL END
     WHERE queue = p_queue AND id = p_id AND claimed_by = p_worker_id
       AND attempt = p_attempt;

    -- Dead letter, with provenance. The copy carries where it came from and how
    -- hard it tried, because a DLQ row with no history is an alert nobody can
    -- action. Its own dead_letter_queue is left NULL: a dead letter that can
    -- itself dead-letter is a loop.
    IF v_terminal AND j.dead_letter_queue IS NOT NULL THEN
        INSERT INTO stampeded_jobs
            (queue, task, args, tenant_id, priority, destination,
             source_id, source_queue, source_attempts, max_attempts)
        VALUES
            (j.dead_letter_queue, j.task, j.args, j.tenant_id, j.priority, j.destination,
             j.id, j.queue, j.attempt, j.max_attempts);
        v_dlq := true;
    END IF;

    RETURN QUERY SELECT v_was, v_state, v_dlq;
END;
$$;

CREATE FUNCTION stampeded_touch(p_queue text, p_id uuid, p_worker_id uuid, p_attempt smallint)
RETURNS boolean
LANGUAGE sql AS $$
    UPDATE stampeded_jobs SET heartbeat_at = now()
     WHERE queue = p_queue AND id = p_id
       AND claimed_by = p_worker_id AND state = 'executing'
       AND attempt = p_attempt
    RETURNING true;
$$;

CREATE FUNCTION stampeded_checkpoint(
    p_queue     text,
    p_id        uuid,
    p_worker_id uuid,
    p_attempt   smallint,
    p_state     jsonb
) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    -- An eighth of the 64 KB cap: enough runway that a job growing steadily
    -- has thousands of writes left when it is told.
    c_warn_bytes constant int := 8192;
    v_seq        integer;
    v_old_size   integer;
    -- length(::text), NOT pg_column_size.
    --
    -- pg_column_size reports the STORED size, which for a stored jsonb is
    -- TOAST-compressed and for a parameter is not -- so comparing the two
    -- compares compressed against uncompressed. A stable 20 KB checkpoint of
    -- repetitive data compresses to a few hundred bytes on disk and then looks
    -- like it grew 70x on every single write, warning forever about a job
    -- doing nothing wrong. Found by testing the large-but-stable case, which
    -- is the one that distinguishes this heuristic from the size cap.
    --
    -- The text length is the logical size on both sides.
    v_new_size   integer := length(p_state::text);
BEGIN
    UPDATE stampeded_jobs
       SET checkpoint     = p_state,
           checkpoint_at  = now(),
           checkpoint_seq = checkpoint_seq + 1,
           heartbeat_at   = now(),
           interruptions  = 0
     WHERE queue = p_queue
       AND id    = p_id
       AND claimed_by = p_worker_id
       AND attempt = p_attempt
       AND state = 'executing'
    RETURNING checkpoint_seq, length(OLD.checkpoint::text)
         INTO v_seq, v_old_size;

    IF v_seq IS NOT NULL
       AND v_new_size > c_warn_bytes
       AND v_old_size IS NOT NULL
       AND v_new_size > v_old_size
    THEN
        RAISE WARNING
            'checkpoint for job % is % bytes and growing (was %); the 64 KB cap '
            'will discard this job if it continues. Store a cursor ("I have done '
            'N") rather than the set of what was done.',
            p_id, v_new_size, v_old_size;
    END IF;

    RETURN v_seq;
END;
$$;

CREATE FUNCTION stampeded_expire(p_limit integer DEFAULT 100)
RETURNS TABLE (out_id uuid, out_queue text, out_worker uuid)
LANGUAGE plpgsql AS $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT id, queue, claimed_by, attempt, expire_seconds
          FROM stampeded_jobs
         WHERE state = 'executing'
           -- From the last sign of life, not from claim: a job that is touching
           -- is working, however long it has been running.
           AND coalesce(heartbeat_at, claimed_at) < now() - make_interval(secs => expire_seconds)
         LIMIT p_limit
         FOR UPDATE SKIP LOCKED
    LOOP
        PERFORM stampeded_fail(
            r.queue, r.id, r.claimed_by, r.attempt, 'timed_out',
            format('exceeded expire_seconds (%s)', r.expire_seconds), NULL, NULL);
        out_id := r.id; out_queue := r.queue; out_worker := r.claimed_by;
        RETURN NEXT;
    END LOOP;
END;
$$;

CREATE FUNCTION stampeded_reap(
    p_dead_after interval DEFAULT interval '5 minutes'
) RETURNS TABLE (id uuid, queue text, worker_id uuid)
LANGUAGE sql AS $$
    WITH dead AS MATERIALIZED (
        SELECT w.id FROM stampeded_workers w
         WHERE w.last_heartbeat < now() - p_dead_after
    )
    UPDATE stampeded_jobs j
       -- TWO budgets now, and the tighter one usually wins. `attempt` was spent
       -- at claim (the plans/04 poison brake); `interruptions` is the plans/15
       -- brake specific to jobs that KILL their worker, defaulting to one free
       -- one. A job that segfaults every worker it touches hits max_interruptions
       -- after two claims instead of walking 25 workers down first.
       SET state        = CASE WHEN j.interruptions + 1 > j.max_interruptions
                                 OR j.attempt >= j.max_attempts
                               THEN 'discarded' ELSE 'retryable' END,
           -- Named distinctly from a raise. solid_queue's three typed process
           -- errors are the model; conflating them is what makes "why did this
           -- run twice" unanswerable in most implementations.
           failure_kind  = 'worker_missing',
           interruptions = j.interruptions + 1,
           last_error    = 'worker stopped heartbeating; job reclaimed',
           error_fingerprint = stampeded_fingerprint('worker stopped heartbeating; job reclaimed'),
           claimed_by   = NULL,
           claimed_at   = NULL,
           heartbeat_at = NULL,
           finished_at  = CASE WHEN j.interruptions + 1 > j.max_interruptions
                                 OR j.attempt >= j.max_attempts THEN now() END,
           dedup_scope  = CASE WHEN j.interruptions + 1 > j.max_interruptions
                                 OR j.attempt >= j.max_attempts
                               THEN NULL ELSE j.dedup_scope END
      FROM dead d
     WHERE j.claimed_by = d.id
       AND j.state IN ('claimed','executing')
    RETURNING j.id, j.queue, d.id;
$$;

CREATE FUNCTION stampeded_prune(
    p_older_than interval DEFAULT interval '1 hour',
    p_limit      integer  DEFAULT 10000,
    p_failed_older_than interval DEFAULT interval '7 days'
) RETURNS integer
LANGUAGE sql AS $$
    WITH doomed AS MATERIALIZED (
        SELECT queue, id FROM stampeded_jobs
         WHERE finished_at IS NOT NULL
           AND CASE state
                 WHEN 'completed' THEN finished_at < now() - p_older_than
                 WHEN 'discarded' THEN finished_at < now() - p_failed_older_than
                 WHEN 'cancelled' THEN finished_at < now() - p_failed_older_than
                 ELSE false
               END
         ORDER BY finished_at
         LIMIT p_limit
         FOR UPDATE SKIP LOCKED
    ),
    -- COUNTED, not `RETURNING 1`.
    --
    -- A SQL function declared RETURNS integer yields the first column of the
    -- FIRST ROW, so `DELETE ... RETURNING 1` returns 1 whether it deleted one
    -- row or ten thousand (and NULL for none). The caller loops until the
    -- returned count is less than the batch size -- with a constant 1 that
    -- condition is always true, so the loop always exited after one batch and
    -- a large backlog was pruned p_limit rows per TICK instead of per
    -- iteration. The pruner looked correct and quietly did a fraction of the
    -- work.
    deleted AS (
        DELETE FROM stampeded_jobs j
         USING doomed d
         WHERE j.queue = d.queue AND j.id = d.id
        RETURNING 1
    )
    SELECT count(*)::int FROM deleted;
$$;

CREATE FUNCTION stampeded_vacuum_health()
RETURNS TABLE (
    relname       text,
    live_tuples   bigint,
    dead_tuples   bigint,
    dead_ratio    double precision,
    autovac_threshold double precision,
    needs_vacuum  boolean,
    last_autovacuum timestamptz
)
LANGUAGE sql AS $$
    SELECT
        c.relname::text,
        s.n_live_tup,
        s.n_dead_tup,
        CASE WHEN s.n_live_tup > 0
             THEN s.n_dead_tup::float8 / s.n_live_tup
             ELSE 0 END,
        -- The formula autovacuum itself uses, with this table's own settings
        -- where they are set and the cluster defaults where they are not.
        threshold.base + threshold.factor * s.n_live_tup,
        s.n_dead_tup > threshold.base + threshold.factor * s.n_live_tup,
        s.last_autovacuum
      FROM pg_stat_user_tables s
      JOIN pg_class c ON c.oid = s.relid
      CROSS JOIN LATERAL (
          SELECT
            coalesce(
              (SELECT option_value::float8 FROM pg_options_to_table(c.reloptions)
                WHERE option_name = 'autovacuum_vacuum_threshold'),
              current_setting('autovacuum_vacuum_threshold')::float8) AS base,
            coalesce(
              (SELECT option_value::float8 FROM pg_options_to_table(c.reloptions)
                WHERE option_name = 'autovacuum_vacuum_scale_factor'),
              current_setting('autovacuum_vacuum_scale_factor')::float8) AS factor
      ) threshold
     WHERE c.relname LIKE 'stampeded_jobs%'
       -- SCOPED TO THIS SCHEMA. pg_stat_user_tables spans the whole database,
       -- so without this a second Stampeded install -- or, here, a few hundred
       -- leftover test schemas -- reports as this one's partitions. Found by
       -- running it and getting 200 rows for a 2-partition install.
       AND s.schemaname = current_schema()
     ORDER BY s.n_dead_tup DESC;
$$;

CREATE FUNCTION stampeded_index_density()
RETURNS TABLE (
    indexname   text,
    pages       bigint,
    entries     bigint,
    per_page    double precision,
    sparse      boolean
)
LANGUAGE sql AS $$
    SELECT
        c.relname::text,
        c.relpages::bigint,
        c.reltuples::bigint,
        CASE WHEN c.relpages > 0 THEN c.reltuples / c.relpages ELSE 0 END,
        c.relpages >= 128
          AND CASE WHEN c.relpages > 0 THEN c.reltuples / c.relpages ELSE 0 END < 5
      FROM pg_class c
      JOIN pg_index i ON i.indexrelid = c.oid
      JOIN pg_class t ON t.oid = i.indrelid
     WHERE c.relkind = 'i'
       AND t.relname LIKE 'stampeded_%'
       -- Skip REINDEX CONCURRENTLY's leftovers rather than trying to rebuild
       -- them: _ccnew/_ccold are transient artefacts and retrying one is how a
       -- reindexer gets stuck in a loop.
       AND c.relname NOT LIKE '%\_ccnew%'
       AND c.relname NOT LIKE '%\_ccold%'
       -- SCOPED TO THIS SCHEMA. pg_class spans the whole database, so without
       -- this a second Stampeded install's indexes -- or, when this was first
       -- run, a few hundred leftover test schemas -- are reported as ours.
       -- 1167 indexes for a two-partition install is how it was noticed.
       AND t.relnamespace = current_schema()::regnamespace
     ORDER BY 4;
$$;

CREATE FUNCTION stampeded_heartbeat(p_worker_id uuid) RETURNS void
LANGUAGE sql AS $$
    UPDATE stampeded_workers SET last_heartbeat = now() WHERE id = p_worker_id;
$$;

CREATE FUNCTION stampeded_partition_health()
RETURNS TABLE (
    partition      text,
    queue          text,
    has_notify     boolean,
    has_counts     boolean
)
LANGUAGE sql STABLE AS $$
    SELECT c.relname::text,
           -- The bound is `FOR VALUES IN ('name')`; pull the queue name back
           -- out rather than assuming the table is named for its queue.
           --
           -- stampeded_jobs_default is the DEFAULT partition, not a partition
           -- for a queue called "default" -- it catches every queue that has
           -- none of its own. Labelling it "default" would be wrong in the
           -- confusing direction: it would read as one queue among many when
           -- it is the fallback for all of them.
           coalesce(
               substring(pg_get_expr(c.relpartbound, c.oid) from $re$FOR VALUES IN \('(.*)'\)$re$),
               CASE WHEN pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'
                    THEN '(catch-all for queues with no partition)'
                    ELSE c.relname::text END
           ),
           EXISTS (SELECT 1 FROM pg_trigger t
                    WHERE t.tgrelid = c.oid AND NOT t.tgisinternal
                      AND t.tgname = 'stampeded_notify_insert_trg'),
           EXISTS (SELECT 1 FROM pg_trigger t
                    WHERE t.tgrelid = c.oid AND NOT t.tgisinternal
                      AND t.tgname = 'stampeded_count_ins_trg')
      FROM pg_class c
      JOIN pg_inherits i ON i.inhrelid = c.oid
      JOIN pg_class p ON p.oid = i.inhparent
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE p.relname = 'stampeded_jobs'
       AND n.nspname = current_schema()
     ORDER BY 1;
$$;

CREATE TABLE stampeded_schedules (
    key             text        PRIMARY KEY,

    queue           text        NOT NULL,
    task            text        NOT NULL,
    args            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    tenant_id       text,
    priority        smallint    NOT NULL DEFAULT 0,

    interval_secs   bigint      CHECK (interval_secs > 0),
    cron_expr       text,
    CONSTRAINT stampeded_schedules_one_rule CHECK (
        (interval_secs IS NULL) <> (cron_expr IS NULL)
    ),

    next_run_at     timestamptz NOT NULL DEFAULT now(),
    last_run_at     timestamptz,

    dst_policy      text        NOT NULL DEFAULT 'instant'
                                CHECK (dst_policy IN ('instant', 'wall_clock')),
    timezone        text        NOT NULL DEFAULT 'UTC',

    missed_policy   text        NOT NULL DEFAULT 'once'
                                CHECK (missed_policy IN ('once', 'skip')),

    paused          boolean     NOT NULL DEFAULT false,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX stampeded_schedules_due_idx
    ON stampeded_schedules (next_run_at)
    WHERE NOT paused;

CREATE TABLE stampeded_cron_occurrences (
    id                bigserial   PRIMARY KEY,
    cron_key          text        NOT NULL REFERENCES stampeded_schedules(key) ON DELETE CASCADE,

    occurrence_token  text        NOT NULL,

    job_id            uuid,
    enqueued_at       timestamptz NOT NULL DEFAULT now(),

    UNIQUE (cron_key, occurrence_token)
);

CREATE INDEX stampeded_cron_occurrences_enqueued_idx
    ON stampeded_cron_occurrences (enqueued_at);

CREATE FUNCTION stampeded_claim_due_schedules(p_limit integer DEFAULT 100)
RETURNS SETOF stampeded_schedules
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
        SELECT * FROM stampeded_schedules
         WHERE NOT paused AND next_run_at <= now()
         ORDER BY next_run_at
         LIMIT p_limit
         FOR UPDATE SKIP LOCKED;
END;
$$;

CREATE FUNCTION stampeded_fire_schedule(p_key text, p_next_run_at timestamptz)
RETURNS uuid
LANGUAGE plpgsql AS $$
DECLARE
    s        stampeded_schedules%ROWTYPE;
    v_token  text;
    v_job    uuid;
    -- Separate from v_job: the ledger's id is bigserial, the job's is uuid.
    -- Reusing one variable for both is a runtime cast error, not a compile one.
    v_occ    bigint;
BEGIN
    SELECT * INTO s FROM stampeded_schedules WHERE key = p_key;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    v_token := CASE s.dst_policy
        WHEN 'wall_clock' THEN
            to_char(s.next_run_at AT TIME ZONE s.timezone, 'YYYY-MM-DD HH24:MI')
        ELSE
            extract(epoch FROM s.next_run_at)::bigint::text
    END;

    INSERT INTO stampeded_cron_occurrences (cron_key, occurrence_token)
    VALUES (s.key, v_token)
    ON CONFLICT (cron_key, occurrence_token) DO NOTHING
    RETURNING id INTO v_occ;

    IF v_occ IS NULL THEN
        UPDATE stampeded_schedules
           SET next_run_at = p_next_run_at, updated_at = now()
         WHERE key = s.key;
        RETURN NULL;
    END IF;

    INSERT INTO stampeded_jobs (queue, task, args, tenant_id, priority)
    VALUES (s.queue, s.task, s.args, s.tenant_id, s.priority)
    RETURNING id INTO v_job;

    UPDATE stampeded_cron_occurrences
       SET job_id = v_job
     WHERE cron_key = s.key AND occurrence_token = v_token;

    UPDATE stampeded_schedules
       SET next_run_at = p_next_run_at, last_run_at = now(), updated_at = now()
     WHERE key = s.key;

    RETURN v_job;
END;
$$;

CREATE FUNCTION stampeded_prune_occurrences(p_older_than interval DEFAULT interval '30 days')
RETURNS integer
LANGUAGE sql AS $$
    WITH doomed AS (
        SELECT id FROM stampeded_cron_occurrences
         WHERE enqueued_at < now() - p_older_than
         LIMIT 10000
    )
    -- Counted, for the same reason as stampeded_prune: RETURNING 1 through a
    -- RETURNS integer function is a constant, not a count.
    , deleted AS (
        DELETE FROM stampeded_cron_occurrences o USING doomed d
         WHERE o.id = d.id
        RETURNING 1
    )
    SELECT count(*)::int FROM deleted;
$$;
