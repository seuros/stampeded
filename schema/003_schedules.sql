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
