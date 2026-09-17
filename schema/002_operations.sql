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
    p_retry_delay_ms bigint DEFAULT NULL
) RETURNS TABLE (was text, now_state text)
LANGUAGE sql AS $$
    UPDATE stampede_jobs
       SET state = CASE
                     WHEN p_kind = 'unrecoverable'    THEN 'discarded'
                     WHEN attempt >= max_attempts     THEN 'discarded'
                     ELSE 'retryable'
                   END,
           failure_kind = p_kind,
           -- Truncated in SQL, not in client code: que is the only surveyed
           -- implementation that bounds error text database-side, and it is the
           -- only bound that survives a polyglot worker (plans/15).
           last_error   = left(p_error, 4000),
           scheduled_at = CASE
                            WHEN p_retry_delay_ms IS NULL THEN scheduled_at
                            ELSE now() + make_interval(secs => p_retry_delay_ms / 1000.0)
                          END,
           claimed_by   = NULL,
           claimed_at   = NULL,
           -- Release the dedup slot only on a terminal outcome; a retryable job
           -- keeps its slot so a duplicate cannot be enqueued while it waits.
           dedup_scope  = CASE
                            WHEN p_kind = 'unrecoverable' OR attempt >= max_attempts
                            THEN NULL ELSE dedup_scope
                          END,
           finished_at  = CASE
                            WHEN p_kind = 'unrecoverable' OR attempt >= max_attempts
                            THEN now() ELSE NULL
                          END
     WHERE queue = p_queue
       AND id    = p_id
       AND claimed_by = p_worker_id
    RETURNING OLD.state, NEW.state;
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
       SET state        = CASE WHEN j.attempt >= j.max_attempts
                               THEN 'discarded' ELSE 'retryable' END,
           -- Named distinctly from a raise. solid_queue's three typed process
           -- errors are the model; conflating them is what makes "why did this
           -- run twice" unanswerable in most implementations.
           failure_kind = 'interrupted',
           last_error   = 'worker stopped heartbeating; job reclaimed',
           claimed_by   = NULL,
           claimed_at   = NULL,
           finished_at  = CASE WHEN j.attempt >= j.max_attempts THEN now() END,
           dedup_scope  = CASE WHEN j.attempt >= j.max_attempts
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
CREATE FUNCTION stampede_prune(
    p_older_than interval DEFAULT interval '14 days',
    p_limit      integer  DEFAULT 10000
) RETURNS integer
LANGUAGE sql AS $$
    WITH doomed AS MATERIALIZED (
        SELECT queue, id FROM stampede_jobs
         WHERE state IN ('completed','discarded','cancelled')
           AND finished_at < now() - p_older_than
         ORDER BY finished_at
         LIMIT p_limit
         FOR UPDATE SKIP LOCKED
    )
    DELETE FROM stampede_jobs j
     USING doomed d
     WHERE j.queue = d.queue AND j.id = d.id
    RETURNING 1;
$$;

-- ---------------------------------------------------------------------------
-- HEARTBEAT
-- ---------------------------------------------------------------------------
CREATE FUNCTION stampede_heartbeat(p_worker_id uuid) RETURNS void
LANGUAGE sql AS $$
    UPDATE stampede_workers SET last_heartbeat = now() WHERE id = p_worker_id;
$$;
