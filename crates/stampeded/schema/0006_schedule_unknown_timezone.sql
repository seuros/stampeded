-- Schema 6: a wall-clock occurrence of a schedule whose zone Postgres does not
-- know is keyed in UTC, as the scheduler fires it. Before, the key raised, and
-- the error rolled back the whole tick: one mistyped zone stopped every schedule.
CREATE OR REPLACE FUNCTION stampeded_fire_schedule(p_key text, p_next_run_at timestamptz)
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

    IF s.dst_policy = 'wall_clock' THEN
        BEGIN
            v_token := to_char(s.next_run_at AT TIME ZONE s.timezone, 'YYYY-MM-DD HH24:MI');
        EXCEPTION WHEN invalid_parameter_value THEN
            v_token := to_char(s.next_run_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI');
        END;
    ELSE
        v_token := extract(epoch FROM s.next_run_at)::bigint::text;
    END IF;

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

    INSERT INTO stampeded_jobs (queue, task, args, meta, tenant_id, priority)
    VALUES (s.queue, s.task, s.args, s.meta, s.tenant_id, s.priority)
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

UPDATE stampeded_schema SET version = 6, changed_at = now();
