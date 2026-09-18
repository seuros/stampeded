-- Stampede — operations
--
-- The semantics live here. 001 encodes the decisions; this file encodes the
-- rules that make them correct.
--
-- Everything is a FUNCTION rather than daemon-side SQL, deliberately: one round
-- trip, atomic, and callable from any language with a Postgres driver. That is
-- the polyglot story in plans/05 — a Python worker calls stampede_claim() and
-- gets the same fence, the same attempt accounting and the same transition
-- rules as the Rust daemon, because there is only one implementation.

-- ---------------------------------------------------------------------------
-- CLAIM
--
-- Two things in here are load-bearing and will look like noise to a reader:
--
-- (1) `AS MATERIALIZED` on the CTE. This is Oban's "optimization fence"
--     (basic.ex:119): without it the planner may nested-loop the LIMIT subquery
--     and update MORE ROWS THAN LIMIT, pushing attempt past max_attempts. Note
--     Oban's own comment predates PG12 — since PG12 a plain CTE is INLINED by
--     default, which defeats the fence entirely. On 18 the keyword is required,
--     not optional. An earlier draft of plans/04 claimed RETURNING OLD./NEW.
--     made the fence unnecessary; that was wrong. You still need to pick N rows,
--     which still needs a LIMIT subquery, which still needs the fence.
--
-- (2) `attempt = attempt + 1` HERE, at claim, not at failure. plans/04: half of
--     twelve surveyed implementations have an unbounded poison loop, and this is
--     the difference. A job that segfaults its worker has already spent the
--     attempt, so max_attempts is eventually reached. If the increment moves to
--     the failure path, a worker-killing job is re-claimed forever.
-- ---------------------------------------------------------------------------
CREATE FUNCTION stampede_claim(
    p_queue     text,
    p_worker_id uuid,
    p_limit     integer DEFAULT 1,
    -- Injectable clock (River's `coalesce($now, now())`). Makes scheduling
    -- testable without sleeping.
    p_now       timestamptz DEFAULT NULL
) RETURNS SETOF stampede_jobs
LANGUAGE sql AS $$
    WITH candidate AS MATERIALIZED (
        SELECT id
          FROM stampede_jobs
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
         ORDER BY priority, scheduled_at, id
         LIMIT p_limit
         FOR UPDATE SKIP LOCKED
    )
    UPDATE stampede_jobs j
       SET state        = 'executing',
           attempt      = j.attempt + 1,
           -- Seed the job heartbeat at claim so stampede_expire has a baseline
           -- for a job that never calls stampede_touch. Left NULL it would fall
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

-- ---------------------------------------------------------------------------
-- COMPLETE
--
-- Returns the prior state via PG18 `RETURNING OLD.`. This is where that feature
-- earns its place (plans/03): the caller learns atomically whether it actually
-- owned the transition, so a worker that lost its job to the reaper mid-flight
-- finds out instead of silently completing a job someone else is running.
--
-- `dedup_scope = NULL` releases the uniqueness slot as part of the SAME update
-- (plans/10). Keys can only LEAVE the index, so no transition can ever raise
-- 23505 — River's whole conflict-CTE and its shipped bug are designed out.
-- ---------------------------------------------------------------------------
CREATE FUNCTION stampede_complete(
    p_queue     text,
    p_id        uuid,
    p_worker_id uuid
) RETURNS TABLE (was text, now_state text, owned boolean)
LANGUAGE sql AS $$
    UPDATE stampede_jobs
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

-- ---------------------------------------------------------------------------
-- FAIL
--
-- One entry point for every failure, with the KIND recorded separately from the
-- count. plans/15: only BullMQ and solid_queue distinguish "the worker died"
-- from "the job raised", and it is the thing that makes poison jobs diagnosable.
--
-- Backoff is NOT computed here. chrono-machines owns it (full jitter by
-- default, which not one of thirteen surveyed implementations does) and passes
-- the resulting instant in. Putting the formula in SQL would fork it.
-- ---------------------------------------------------------------------------
-- Error fingerprint: the normalisation that makes failures CLUSTER.
--
-- "connection to 10.0.0.7:5432 timed out after 30012ms" and
-- "connection to 10.0.0.9:5432 timed out after 29998ms" are one incident, but
-- as raw strings they are two. Strip the parts that vary per occurrence —
-- UUIDs, hex blobs, numbers — and hash what survives. Nobody in the survey
-- does this, which is why every `failure_summary` equivalent lists instances
-- instead of causes.
--
-- IMMUTABLE so it can be computed in an index or a generated column later.
CREATE FUNCTION stampede_fingerprint(p_error text) RETURNS bytea
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

-- ---------------------------------------------------------------------------
-- FAIL
--
-- Three outcomes, decided by WHY the job failed rather than only by how many
-- times it has (plans/15).
--
--   breaker_open      -- our backpressure. REFUNDS the attempt.
--   worker-death      -- charges `interruptions`, a separate and much tighter
--                        budget than `attempt`.
--   everything else   -- charges `attempt`, which was already spent at claim.
--
-- On a terminal outcome the job dead-letters if a queue is configured, because
-- pg-boss is right that the DLQ should be a QUEUE: re-drive is then just
-- working it, and every tool that understands jobs already understands it.
-- ---------------------------------------------------------------------------
CREATE FUNCTION stampede_fail(
    p_queue        text,
    p_id           uuid,
    p_worker_id    uuid,
    p_kind         text,
    p_error        text,
    -- A DELAY, not an instant.
    --
    -- Taking an absolute timestamptz computed on the caller makes retry timing
    -- depend on the app server and the database agreeing about the time. A
    -- server 30s ahead delays every retry by 30s; a fleet with mixed skew
    -- behaves inconsistently; and it is invisible in development, where skew is
    -- a few milliseconds. Measured at 3ms locally -- enough to make a retry
    -- scheduled for "now" not ready yet.
    --
    -- The database applies its own clock, so there is nothing to agree about.
    p_retry_delay_ms bigint DEFAULT NULL,
    -- Exit status, when the daemon supervises the worker process and saw it
    -- die. 137 is OOM. plans/15 calls this the cheapest, highest-signal
    -- artefact in the survey, and it is thrown away everywhere else.
    p_exit_status  integer DEFAULT NULL
) RETURNS TABLE (was text, now_state text, dead_lettered boolean)
LANGUAGE plpgsql AS $$
DECLARE
    j            stampede_jobs%ROWTYPE;
    v_was        text;
    v_state      text;
    v_interrupt  boolean;
    v_refund     boolean;
    v_terminal   boolean;
    v_dlq        boolean := false;
BEGIN
    SELECT * INTO j FROM stampede_jobs
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

    UPDATE stampede_jobs
       SET state        = v_state,
           failure_kind = p_kind,
           attempt      = CASE WHEN v_refund THEN greatest(attempt - 1, 0) ELSE attempt END,
           interruptions = CASE WHEN v_interrupt THEN interruptions + 1 ELSE interruptions END,
           deferrals    = CASE WHEN v_refund THEN deferrals + 1 ELSE deferrals END,
           -- Truncated in SQL, not in client code: que is the only surveyed
           -- implementation that bounds error text database-side, and it is the
           -- only bound that survives a polyglot worker (plans/15).
           last_error   = left(p_error, 4000),
           error_fingerprint = stampede_fingerprint(p_error),
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
        INSERT INTO stampede_jobs
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

-- ---------------------------------------------------------------------------
-- TOUCH — a long job proving it is alive
--
-- Layer 3 of the timeout stack (plans/15), pg-boss's touchJobs. The worker
-- heartbeat says the PROCESS is alive; this says this particular JOB is still
-- progressing. Without it a legitimately long job and a wedged one look
-- identical, and the only available response is to guess a timeout.
-- ---------------------------------------------------------------------------
CREATE FUNCTION stampede_touch(p_queue text, p_id uuid, p_worker_id uuid)
RETURNS boolean
LANGUAGE sql AS $$
    UPDATE stampede_jobs SET heartbeat_at = now()
     WHERE queue = p_queue AND id = p_id
       AND claimed_by = p_worker_id AND state = 'executing'
    RETURNING true;
$$;

-- ---------------------------------------------------------------------------
-- CHECKPOINT — save resume state
--
-- plans/17. Returns the new sequence number, or NULL if this worker no longer
-- owns the job.
--
-- THE OWNERSHIP CHECK IS NOT A FORMALITY HERE. The sequence that makes it
-- matter: worker A checkpoints at 50k and its host wedges; the reaper decides A
-- is dead and returns the job; worker B claims it, resumes from 50k and
-- checkpoints at 60k; then A WAKES UP and writes 51k. Without the predicate,
-- A's stale write moves the cursor BACKWARDS and B's next attempt redoes 9,000
-- units of work -- or does them twice, if the job is not perfectly idempotent.
--
-- It also sets heartbeat_at: a job that just saved progress is demonstrably
-- alive, and requiring a separate stampede_touch would be asking the caller to
-- say the same thing twice. Forgetting that second call would let a job which
-- is plainly working get expired out from under itself.
--
-- AND IT RESETS `interruptions`, which is the interesting decision (plans/17
-- §4). The two counters mean different things: `interruptions` exists to stop a
-- job that KILLS WORKERS from walking the fleet one process at a time, and a
-- job that checkpointed at 50k and then died is demonstrably not that job -- it
-- ran, it worked, something else killed it. `attempt` is untouched and still
-- increments at claim, so a job that progresses one unit and then dies forever
-- is still discarded at max_attempts. Progress buys tolerance of
-- INFRASTRUCTURE failure and buys nothing against a job that is simply broken.
-- ---------------------------------------------------------------------------
CREATE FUNCTION stampede_checkpoint(
    p_queue     text,
    p_id        uuid,
    p_worker_id uuid,
    p_state     jsonb
) RETURNS integer
LANGUAGE sql AS $$
    UPDATE stampede_jobs
       SET checkpoint     = p_state,
           checkpoint_at  = now(),
           checkpoint_seq = checkpoint_seq + 1,
           heartbeat_at   = now(),
           interruptions  = 0
     WHERE queue = p_queue
       AND id    = p_id
       AND claimed_by = p_worker_id
       AND state = 'executing'
    RETURNING checkpoint_seq;
$$;

-- ---------------------------------------------------------------------------
-- EXPIRE — the timeout that fires when nobody is alive to fire it
--
-- Layer 2 of the three. The in-process tokio timeout cannot fire if the worker
-- is dead, and the reaper only helps once the worker has stopped heartbeating
-- entirely -- a worker that is alive and healthy but stuck on one job is
-- invisible to both. This catches exactly that.
--
-- Routes through the NORMAL fail path so it consumes a retry, backs off and
-- dead-letters like any other failure. pg-boss's failJobsByTimeout.
-- ---------------------------------------------------------------------------
CREATE FUNCTION stampede_expire(p_limit integer DEFAULT 100)
RETURNS TABLE (out_id uuid, out_queue text, out_worker uuid)
LANGUAGE plpgsql AS $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT id, queue, claimed_by, expire_seconds
          FROM stampede_jobs
         WHERE state = 'executing'
           -- From the last sign of life, not from claim: a job that is touching
           -- is working, however long it has been running.
           AND coalesce(heartbeat_at, claimed_at) < now() - make_interval(secs => expire_seconds)
         LIMIT p_limit
         FOR UPDATE SKIP LOCKED
    LOOP
        PERFORM stampede_fail(
            r.queue, r.id, r.claimed_by, 'timed_out',
            format('exceeded expire_seconds (%s)', r.expire_seconds), NULL, NULL);
        out_id := r.id; out_queue := r.queue; out_worker := r.claimed_by;
        RETURN NEXT;
    END LOOP;
END;
$$;

-- ---------------------------------------------------------------------------
-- REAP — orphan recovery
--
-- plans/04: heartbeat, NOT a time lease. Every other Postgres queue surveyed
-- uses a lease (Oban OSS 60 min, Graphile 4 h, pg-boss 15 min); solid_queue is
-- the exception and the prior art. A lease turns a slow job into duplicate
-- execution because correctness depends on guessing maximum job duration.
--
-- The attempt was already spent at claim, so a job whose worker keeps dying
-- walks its attempt budget down and eventually discards itself. That is the
-- poison-job brake, and it costs nothing here.
-- ---------------------------------------------------------------------------
CREATE FUNCTION stampede_reap(
    p_dead_after interval DEFAULT interval '5 minutes'
) RETURNS TABLE (id uuid, queue text, worker_id uuid)
LANGUAGE sql AS $$
    WITH dead AS MATERIALIZED (
        SELECT w.id FROM stampede_workers w
         WHERE w.last_heartbeat < now() - p_dead_after
    )
    UPDATE stampede_jobs j
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
           error_fingerprint = stampede_fingerprint('worker stopped heartbeating; job reclaimed'),
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

-- ---------------------------------------------------------------------------
-- PRUNE
--
-- plans/13: River's cleaner shape — bounded batch, never an unbounded DELETE.
-- With partitioning by queue there is no state partition to drop, so pruning
-- and autovacuum are the ONLY defence against unbounded growth. A single
-- unbounded DELETE on a large table holds locks and bloats worse than what it
-- reclaims.
--
-- Returns the count so the caller can loop until it returns < p_limit, which is
-- how River avoids both a long transaction and an unfinished sweep.
-- ---------------------------------------------------------------------------
-- TWO retention clocks, not one (pg-boss's shape).
--
-- A completed job is evidence nobody will read; a discarded one is evidence
-- somebody is about to be paged about. Keeping them for the same duration means
-- either drowning in successes or deleting failures before anyone can look at
-- them -- and Oban's 60-second default does exactly the latter, destroying the
-- evidence before the alert has fired.
--
-- Defaults: 1 hour for completed (between Oban's 60s and River's 24h), 7 days
-- for discarded and cancelled (River's number).
CREATE FUNCTION stampede_prune(
    p_older_than interval DEFAULT interval '1 hour',
    p_limit      integer  DEFAULT 10000,
    p_failed_older_than interval DEFAULT interval '7 days'
) RETURNS integer
LANGUAGE sql AS $$
    WITH doomed AS MATERIALIZED (
        SELECT queue, id FROM stampede_jobs
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
        DELETE FROM stampede_jobs j
         USING doomed d
         WHERE j.queue = d.queue AND j.id = d.id
        RETURNING 1
    )
    SELECT count(*)::int FROM deleted;
$$;

-- ---------------------------------------------------------------------------
-- VACUUM HEALTH
--
-- plans/13 item 5: pg-boss's #checkVacuum is the single most valuable
-- operational feature in the survey, because it converts the queue's
-- characteristic death spiral from "mysterious slow backlog" into a named
-- cause. A queue table is the pathological autovacuum case -- constant UPDATEs
-- over a small hot set -- and when autovacuum falls behind, everything simply
-- gets slower with no error anywhere.
--
-- Returns one row per partition: dead tuples, the threshold autovacuum will
-- actually use for THAT table (its own reloptions, not the global defaults --
-- which is the whole reason a naive n_dead_tup alert misfires), and when it
-- last ran.
CREATE FUNCTION stampede_vacuum_health()
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
     WHERE c.relname LIKE 'stampede_jobs%'
       -- SCOPED TO THIS SCHEMA. pg_stat_user_tables spans the whole database,
       -- so without this a second Stampede install -- or, here, a few hundred
       -- leftover test schemas -- reports as this one's partitions. Found by
       -- running it and getting 200 rows for a 2-partition install.
       AND s.schemaname = current_schema()
     ORDER BY s.n_dead_tup DESC;
$$;

-- ---------------------------------------------------------------------------
-- INDEX DENSITY
--
-- plans/13 item 4: reindex on a MEASUREMENT, not on a schedule. River rebuilds
-- everything at midnight; pg-boss computes entries-per-page and only rebuilds
-- what is actually sparse. The heuristic is pg-boss's (minPages 128, and an
-- index is suspect below ~5 entries per page).
--
-- pg-boss calibrated its numbers against v4 UUIDs and notes that random keys
-- defeat btree deduplication. Ours are uuidv7 and time-ordered, so healthy
-- density should be HIGHER and this gate should hold with more margin --
-- inferred, not measured, so treat a positive as a prompt to look rather than
-- as a verdict.
CREATE FUNCTION stampede_index_density()
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
       AND t.relname LIKE 'stampede_%'
       -- Skip REINDEX CONCURRENTLY's leftovers rather than trying to rebuild
       -- them: _ccnew/_ccold are transient artefacts and retrying one is how a
       -- reindexer gets stuck in a loop.
       AND c.relname NOT LIKE '%\_ccnew%'
       AND c.relname NOT LIKE '%\_ccold%'
       -- SCOPED TO THIS SCHEMA. pg_class spans the whole database, so without
       -- this a second Stampede install's indexes -- or, when this was first
       -- run, a few hundred leftover test schemas -- are reported as ours.
       -- 1167 indexes for a two-partition install is how it was noticed.
       AND t.relnamespace = current_schema()::regnamespace
     ORDER BY 4;
$$;

-- ---------------------------------------------------------------------------
-- HEARTBEAT
-- ---------------------------------------------------------------------------
CREATE FUNCTION stampede_heartbeat(p_worker_id uuid) RETURNS void
LANGUAGE sql AS $$
    UPDATE stampede_workers SET last_heartbeat = now() WHERE id = p_worker_id;
$$;
