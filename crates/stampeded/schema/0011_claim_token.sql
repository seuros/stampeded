-- Stop claiming and drain all workers before applying this migration.
ALTER TABLE stampeded_jobs ADD COLUMN claim_token uuid;

DROP FUNCTION stampeded_complete(text, uuid, uuid, smallint);
DROP FUNCTION stampeded_release(text, uuid, uuid, smallint);
DROP FUNCTION stampeded_touch(text, uuid, uuid, smallint);
DROP FUNCTION stampeded_checkpoint(text, uuid, uuid, smallint, jsonb);
DROP FUNCTION stampeded_fail(
    text, uuid, uuid, smallint, text, text, bigint, integer);

CREATE OR REPLACE FUNCTION stampeded_claim(
    p_queue text, p_worker_id uuid, p_limit integer DEFAULT 1,
    p_now timestamptz DEFAULT NULL
) RETURNS SETOF stampeded_jobs
LANGUAGE sql AS $$
    WITH candidate AS MATERIALIZED (
        SELECT id FROM stampeded_jobs
         WHERE queue = p_queue
           AND state IN ('available', 'retryable')
           AND scheduled_at <= coalesce(p_now, now())
           AND attempt < max_attempts
           AND NOT stampeded_queue_paused(p_queue)
         ORDER BY priority, scheduled_at
         LIMIT p_limit
         FOR UPDATE SKIP LOCKED
    )
    UPDATE stampeded_jobs j
       SET state = 'executing',
           attempt = j.attempt + 1,
           heartbeat_at = coalesce(p_now, now()),
           claimed_by = p_worker_id,
           claimed_at = coalesce(p_now, now()),
           claim_token = uuidv7(),
           attempted_by = array_append(
               CASE WHEN coalesce(array_length(j.attempted_by, 1), 0) >= 10
                    THEN j.attempted_by[array_length(j.attempted_by, 1) - 8:]
                    ELSE j.attempted_by END,
               p_worker_id::text)
      FROM candidate c
     WHERE j.queue = p_queue AND j.id = c.id
    RETURNING j.*;
$$;

CREATE FUNCTION stampeded_complete(
    p_queue text, p_id uuid, p_worker_id uuid, p_claim_token uuid
) RETURNS TABLE (was text, now_state text, owned boolean)
LANGUAGE sql AS $$
    UPDATE stampeded_jobs
       SET state = 'completed', finished_at = now(), dedup_scope = NULL,
           checkpoint = NULL, claimed_by = NULL, claim_token = NULL
     WHERE queue = p_queue AND id = p_id AND claimed_by = p_worker_id
       AND claim_token = p_claim_token AND state = 'executing'
    RETURNING OLD.state, NEW.state, true;
$$;

CREATE FUNCTION stampeded_fail(
    p_queue text, p_id uuid, p_worker_id uuid, p_claim_token uuid,
    p_kind text, p_error text, p_retry_delay_ms bigint DEFAULT NULL,
    p_exit_status integer DEFAULT NULL
) RETURNS TABLE (was text, now_state text, dead_lettered boolean)
LANGUAGE plpgsql AS $$
DECLARE
    j stampeded_jobs%ROWTYPE;
    v_state text;
    v_interrupt boolean;
    v_refund boolean;
    v_terminal boolean;
    v_dlq boolean := false;
BEGIN
    SELECT * INTO j FROM stampeded_jobs
     WHERE queue = p_queue AND id = p_id AND claimed_by = p_worker_id
       AND claim_token = p_claim_token AND state = 'executing'
     FOR UPDATE;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    v_interrupt := p_kind IN ('heartbeat_lost', 'worker_exited',
                              'worker_missing');
    v_refund := p_kind IN ('breaker_open', 'throttled');
    v_terminal := CASE
        WHEN v_refund THEN j.deferrals + 1 > j.max_deferrals
        WHEN p_kind = 'unrecoverable' THEN true
        WHEN v_interrupt THEN j.interruptions + 1 > j.max_interruptions
        ELSE j.attempt >= j.max_attempts
    END;
    v_state := CASE WHEN v_terminal THEN 'discarded' ELSE 'retryable' END;

    UPDATE stampeded_jobs
       SET state = v_state,
           failure_kind = p_kind,
           attempt = CASE WHEN v_refund THEN greatest(attempt - 1, 0)
                          ELSE attempt END,
           interruptions = CASE WHEN v_interrupt THEN interruptions + 1
                                ELSE interruptions END,
           deferrals = CASE WHEN v_refund THEN deferrals + 1 ELSE deferrals END,
           last_error = left(p_error, 4000),
           error_fingerprint = stampeded_fingerprint(p_error),
           errors = (
               SELECT coalesce(jsonb_agg(e), '[]'::jsonb) FROM (
                   SELECT e FROM jsonb_array_elements(
                       errors || jsonb_build_object(
                           'at', now(), 'attempt', j.attempt, 'kind', p_kind,
                           'message', left(p_error, 1000),
                           'destination', j.destination,
                           'exit_status', p_exit_status
                       )
                   ) e OFFSET greatest(jsonb_array_length(errors) + 1 - 10, 0)
               ) t
           ),
           scheduled_at = CASE WHEN p_retry_delay_ms IS NULL THEN scheduled_at
                               ELSE now() + make_interval(
                                   secs => p_retry_delay_ms / 1000.0) END,
           claimed_by = NULL,
           claimed_at = NULL,
           heartbeat_at = NULL,
           claim_token = NULL,
           dedup_scope = CASE WHEN v_terminal THEN NULL ELSE dedup_scope END,
           finished_at = CASE WHEN v_terminal THEN now() ELSE NULL END
     WHERE queue = p_queue AND id = p_id AND claimed_by = p_worker_id
       AND claim_token = p_claim_token AND state = 'executing';
    IF NOT FOUND THEN
        RETURN;
    END IF;

    IF v_terminal AND j.dead_letter_queue IS NOT NULL THEN
        INSERT INTO stampeded_jobs
            (queue, task, args, meta, tenant_id, priority, destination,
             source_id, source_queue, source_attempts, max_attempts)
        VALUES
            (j.dead_letter_queue, j.task, j.args, j.meta, j.tenant_id,
             j.priority, j.destination, j.id, j.queue, j.attempt,
             j.max_attempts);
        v_dlq := true;
    END IF;

    RETURN QUERY SELECT j.state, v_state, v_dlq;
END;
$$;

CREATE FUNCTION stampeded_release(
    p_queue text, p_id uuid, p_worker_id uuid, p_claim_token uuid
) RETURNS TABLE (was text, now_state text, owned boolean)
LANGUAGE sql AS $$
    UPDATE stampeded_jobs
       SET state = CASE WHEN failure_kind IS NULL THEN 'available'
                        ELSE 'retryable' END,
           attempt = greatest(attempt - 1, 0),
           claimed_by = NULL, claimed_at = NULL, heartbeat_at = NULL,
           claim_token = NULL
     WHERE queue = p_queue AND id = p_id AND claimed_by = p_worker_id
       AND claim_token = p_claim_token AND state = 'executing'
    RETURNING OLD.state, NEW.state, true;
$$;

CREATE FUNCTION stampeded_touch(
    p_queue text, p_id uuid, p_worker_id uuid, p_claim_token uuid
) RETURNS boolean
LANGUAGE sql AS $$
    UPDATE stampeded_jobs SET heartbeat_at = now()
     WHERE queue = p_queue AND id = p_id AND claimed_by = p_worker_id
       AND claim_token = p_claim_token AND state = 'executing'
    RETURNING true;
$$;

CREATE FUNCTION stampeded_checkpoint(
    p_queue text, p_id uuid, p_worker_id uuid, p_claim_token uuid,
    p_state jsonb
) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    c_warn_bytes constant integer := 8192;
    v_seq integer;
    v_old_size integer;
    v_new_size integer := length(p_state::text);
BEGIN
    UPDATE stampeded_jobs
       SET checkpoint = p_state, checkpoint_at = now(),
           checkpoint_seq = checkpoint_seq + 1, heartbeat_at = now(),
           interruptions = 0
     WHERE queue = p_queue AND id = p_id AND claimed_by = p_worker_id
       AND claim_token = p_claim_token AND state = 'executing'
    RETURNING checkpoint_seq, length(OLD.checkpoint::text)
         INTO v_seq, v_old_size;

    IF v_seq IS NOT NULL AND v_new_size > c_warn_bytes
       AND v_old_size IS NOT NULL AND v_new_size > v_old_size THEN
        RAISE WARNING USING MESSAGE = format(
            'checkpoint for job %s is %s bytes and growing (was %s);'
            ' the 64 KB cap will discard this job if it continues.'
            ' Store a cursor ("I have done N") rather than the set of'
            ' what was done.', p_id, v_new_size, v_old_size);
    END IF;
    RETURN v_seq;
END;
$$;

CREATE OR REPLACE FUNCTION stampeded_expire(p_limit integer DEFAULT 100)
RETURNS TABLE (out_id uuid, out_queue text, out_worker uuid)
LANGUAGE plpgsql AS $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT id, queue, claimed_by, claim_token, expire_seconds
          FROM stampeded_jobs
         WHERE state = 'executing'
           AND coalesce(heartbeat_at, claimed_at)
               < now() - make_interval(secs => expire_seconds)
         LIMIT p_limit
         FOR UPDATE SKIP LOCKED
    LOOP
        PERFORM stampeded_fail(
            r.queue, r.id, r.claimed_by, r.claim_token, 'timed_out',
            format('exceeded expire_seconds (%s)', r.expire_seconds));
        IF FOUND THEN
            out_id := r.id; out_queue := r.queue; out_worker := r.claimed_by;
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION stampeded_reap(
    p_dead_after interval DEFAULT interval '5 minutes'
) RETURNS TABLE (id uuid, queue text, worker_id uuid)
LANGUAGE sql AS $$
    WITH dead AS MATERIALIZED (
        SELECT w.id FROM stampeded_workers w
         WHERE w.last_heartbeat < now() - p_dead_after
    )
    UPDATE stampeded_jobs j
       SET state = CASE WHEN j.interruptions + 1 > j.max_interruptions
                             OR j.attempt >= j.max_attempts
                        THEN 'discarded' ELSE 'retryable' END,
           failure_kind = 'worker_missing',
           interruptions = j.interruptions + 1,
           last_error = 'worker stopped heartbeating; job reclaimed',
           error_fingerprint = stampeded_fingerprint(
               'worker stopped heartbeating; job reclaimed'),
           claimed_by = NULL, claimed_at = NULL, heartbeat_at = NULL,
           claim_token = NULL,
           finished_at = CASE WHEN j.interruptions + 1 > j.max_interruptions
                                   OR j.attempt >= j.max_attempts
                              THEN now() END,
           dedup_scope = CASE WHEN j.interruptions + 1 > j.max_interruptions
                                   OR j.attempt >= j.max_attempts
                              THEN NULL ELSE j.dedup_scope END
      FROM dead d
     WHERE j.claimed_by = d.id AND j.state IN ('claimed', 'executing')
    RETURNING j.id, j.queue, d.id;
$$;

UPDATE stampeded_schema SET version = 11, changed_at = now();
