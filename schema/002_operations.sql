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
    p_worker_id uuid
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
     WHERE queue = p_queue AND id = p_id AND claimed_by = p_worker_id;
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
     WHERE queue = p_queue AND id = p_id AND claimed_by = p_worker_id;

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

CREATE FUNCTION stampeded_touch(p_queue text, p_id uuid, p_worker_id uuid)
RETURNS boolean
LANGUAGE sql AS $$
    UPDATE stampeded_jobs SET heartbeat_at = now()
     WHERE queue = p_queue AND id = p_id
       AND claimed_by = p_worker_id AND state = 'executing'
    RETURNING true;
$$;

CREATE FUNCTION stampeded_checkpoint(
    p_queue     text,
    p_id        uuid,
    p_worker_id uuid,
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
        SELECT id, queue, claimed_by, expire_seconds
          FROM stampeded_jobs
         WHERE state = 'executing'
           -- From the last sign of life, not from claim: a job that is touching
           -- is working, however long it has been running.
           AND coalesce(heartbeat_at, claimed_at) < now() - make_interval(secs => expire_seconds)
         LIMIT p_limit
         FOR UPDATE SKIP LOCKED
    LOOP
        PERFORM stampeded_fail(
            r.queue, r.id, r.claimed_by, 'timed_out',
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
