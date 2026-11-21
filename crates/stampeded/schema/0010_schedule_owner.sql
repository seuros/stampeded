-- Schema 10: a schedule has an owner, the program that declares it.
-- stampeded_sync_schedules makes an owner's rows match its list, never
-- touching another owner's. NULL is a row written before owners, or by hand:
-- unowned, and adopted by the first owner that lists its key.
ALTER TABLE stampeded_schedules
    ADD COLUMN owner text
        CONSTRAINT stampeded_schedules_owner_check CHECK (btrim(owner) <> '');

CREATE INDEX stampeded_schedules_owner_idx
    ON stampeded_schedules (owner)
    WHERE owner IS NOT NULL;

CREATE OR REPLACE FUNCTION stampeded_sync_schedules(p_owner text, p_schedules jsonb)
RETURNS TABLE (schedule_key text, change text)
LANGUAGE plpgsql AS $$
DECLARE
    s        record;
    v_keys   text[];
    v_dup    text;
    v_holder text;
BEGIN
    IF p_owner IS NULL OR btrim(p_owner) = '' THEN
        RAISE EXCEPTION 'stampeded_sync_schedules: an owner is required'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_schedules IS NULL OR jsonb_typeof(p_schedules) <> 'array' THEN
        RAISE EXCEPTION 'stampeded_sync_schedules: schedules must be a JSON array'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    PERFORM pg_advisory_xact_lock(hashtext('stampeded_sync_schedules'), hashtext(p_owner));

    SELECT coalesce(array_agg(e->>'key'), '{}') INTO v_keys
      FROM jsonb_array_elements(p_schedules) e;
    IF EXISTS (SELECT 1 FROM unnest(v_keys) k WHERE k IS NULL OR btrim(k) = '') THEN
        RAISE EXCEPTION 'stampeded_sync_schedules: every schedule needs a key'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    SELECT k INTO v_dup FROM unnest(v_keys) k GROUP BY k HAVING count(*) > 1 LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'stampeded_sync_schedules: schedule % is listed twice', quote_literal(v_dup)
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    FOR s IN
        SELECT * FROM jsonb_to_recordset(p_schedules) AS x(
            key text, queue text, task text, args jsonb, meta jsonb, tenant_id text,
            priority smallint, cron_expr text, interval_secs bigint, timezone text,
            dst_policy text, max_attempts smallint, max_interruptions smallint,
            expire_seconds integer, next_run_at timestamptz)
        ORDER BY x.key
    LOOP
        IF s.queue IS NULL OR s.task IS NULL THEN
            RAISE EXCEPTION 'stampeded_sync_schedules: schedule % needs a queue and a task',
                quote_literal(s.key) USING ERRCODE = 'invalid_parameter_value';
        END IF;
        IF (s.cron_expr IS NULL) = (s.interval_secs IS NULL) THEN
            RAISE EXCEPTION 'stampeded_sync_schedules: schedule % needs exactly one of cron_expr and interval_secs',
                quote_literal(s.key) USING ERRCODE = 'invalid_parameter_value';
        END IF;

        INSERT INTO stampeded_schedules
            (key, owner, queue, task, args, meta, tenant_id, priority, cron_expr, interval_secs,
             timezone, dst_policy, next_run_at)
        VALUES
            (s.key, p_owner, s.queue, s.task, coalesce(s.args, '{}'), coalesce(s.meta, '{}'),
             s.tenant_id, coalesce(s.priority, 0), s.cron_expr, s.interval_secs,
             coalesce(s.timezone, 'UTC'), coalesce(s.dst_policy, 'instant'),
             coalesce(s.next_run_at, now()))
        ON CONFLICT (key) DO NOTHING;

        IF FOUND THEN
            -- New rows keep the column defaults for limits the spec leaves out.
            UPDATE stampeded_schedules t
               SET max_attempts = coalesce(s.max_attempts, t.max_attempts),
                   max_interruptions = coalesce(s.max_interruptions, t.max_interruptions),
                   expire_seconds = coalesce(s.expire_seconds, t.expire_seconds)
             WHERE t.key = s.key;
            schedule_key := s.key;
            change := 'created';
            RETURN NEXT;
            CONTINUE;
        END IF;

        SELECT t.owner INTO v_holder FROM stampeded_schedules t WHERE t.key = s.key FOR UPDATE;
        IF v_holder IS DISTINCT FROM p_owner AND v_holder IS NOT NULL THEN
            RAISE EXCEPTION 'stampeded_sync_schedules: schedule % belongs to owner %, not %',
                quote_literal(s.key), quote_literal(v_holder), quote_literal(p_owner)
                USING ERRCODE = 'SD409',
                      HINT = 'Drop it from that owner''s list, or delete the row, before another owner declares it.';
        END IF;

        -- Unchanged rows are left alone: no write, no change reported. The
        -- next run moves only when the rule does, and an omitted limit keeps
        -- whatever the row holds.
        UPDATE stampeded_schedules t
           SET owner = p_owner, queue = s.queue, task = s.task,
               args = coalesce(s.args, '{}'), meta = coalesce(s.meta, '{}'),
               tenant_id = s.tenant_id, priority = coalesce(s.priority, 0),
               cron_expr = s.cron_expr, interval_secs = s.interval_secs,
               timezone = coalesce(s.timezone, 'UTC'),
               dst_policy = coalesce(s.dst_policy, 'instant'),
               max_attempts = coalesce(s.max_attempts, t.max_attempts),
               max_interruptions = coalesce(s.max_interruptions, t.max_interruptions),
               expire_seconds = coalesce(s.expire_seconds, t.expire_seconds),
               next_run_at = CASE
                   WHEN (t.cron_expr, t.interval_secs, t.timezone, t.dst_policy)
                        IS DISTINCT FROM (s.cron_expr, s.interval_secs,
                                          coalesce(s.timezone, 'UTC'),
                                          coalesce(s.dst_policy, 'instant'))
                   THEN coalesce(s.next_run_at, t.next_run_at)
                   ELSE t.next_run_at
               END,
               updated_at = now()
         WHERE t.key = s.key
           AND (t.owner, t.queue, t.task, t.args, t.meta, t.tenant_id, t.priority,
                t.cron_expr, t.interval_secs, t.timezone, t.dst_policy,
                t.max_attempts, t.max_interruptions, t.expire_seconds)
               IS DISTINCT FROM
               (p_owner, s.queue, s.task, coalesce(s.args, '{}'), coalesce(s.meta, '{}'),
                s.tenant_id, coalesce(s.priority, 0), s.cron_expr, s.interval_secs,
                coalesce(s.timezone, 'UTC'), coalesce(s.dst_policy, 'instant'),
                coalesce(s.max_attempts, t.max_attempts),
                coalesce(s.max_interruptions, t.max_interruptions),
                coalesce(s.expire_seconds, t.expire_seconds));

        IF FOUND THEN
            schedule_key := s.key;
            change := CASE WHEN v_holder IS NULL THEN 'adopted' ELSE 'updated' END;
            RETURN NEXT;
        END IF;
    END LOOP;

    RETURN QUERY
    WITH gone AS (
        DELETE FROM stampeded_schedules t
         WHERE t.owner = p_owner AND t.key <> ALL (v_keys)
        RETURNING t.key
    )
    SELECT gone.key, 'removed'::text FROM gone ORDER BY gone.key;
END;
$$;

UPDATE stampeded_schema SET version = 10, changed_at = now();
