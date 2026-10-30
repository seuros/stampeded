-- Schema 7: a queue can be paused. A paused queue's jobs stay where they are
-- and enqueueing still works; stampeded_claim hands out nothing from it, so
-- the pause holds for every runtime and without a daemon. Jobs already
-- running finish.
ALTER TABLE stampeded_queue_policies
    ADD COLUMN IF NOT EXISTS paused boolean NOT NULL DEFAULT false;

-- Whether p_queue is paused; false, not an error, without the policies table,
-- which workers already treat as optional. plpgsql so the table is looked up
-- only once it is known to exist.
CREATE OR REPLACE FUNCTION stampeded_queue_paused(p_queue text)
RETURNS boolean
LANGUAGE plpgsql STABLE AS $$
BEGIN
    IF to_regclass('stampeded_queue_policies') IS NULL THEN
        RETURN false;
    END IF;
    RETURN EXISTS (SELECT 1 FROM stampeded_queue_policies
                    WHERE queue = p_queue AND paused);
END;
$$;

CREATE OR REPLACE FUNCTION stampeded_claim(
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
           -- 'retryable' is claimable once its backoff has elapsed. That can be
           -- handled by a claim predicate, a separate promotion pass, or both.
           -- The predicate is chosen here for having no moving parts -- a
           -- promotion pass is another process that can fall behind or die.
           --
           -- Omitting 'retryable' here is not a missing feature, it is a silent
           -- poison-job bug: the reaper parks a crashed job in 'retryable', a
           -- claim that only reads 'available' never picks it up again, the
           -- attempt budget never walks down, and max_attempts is never
           -- reached. Found by test, not by reading.
           AND state        IN ('available','retryable')
           AND scheduled_at <= coalesce(p_now, now())
           AND attempt      < max_attempts
           -- Reads no job column, so it is a one-time filter: a paused queue
           -- skips the index scan rather than filtering every row it reads.
           AND NOT stampeded_queue_paused(p_queue)
         -- Must match stampeded_jobs_claim_idx EXACTLY. `id` is not a
         -- tiebreaker here: adding one reintroduces a sort unless it is also
         -- added to the index, and in the index it costs 8.5x the size by
         -- defeating btree deduplication. See the claim index note in NOTES.md.
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
           -- Bounded ring buffer. Unbounded attempt history is a slow leak on
           -- heavily-retried jobs; keep the last 10.
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

UPDATE stampeded_schema SET version = 7, changed_at = now();
