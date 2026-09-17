-- Stampede — recurring schedules
--
-- Design from plans/11. Two mechanisms, each doing exactly one job.

-- ---------------------------------------------------------------------------
-- Schedules — the schedule row IS the due row (db-scheduler's model).
--
-- `next_run_at` lives on the row and is claimed with the same
-- FOR UPDATE SKIP LOCKED used for jobs. The claimer enqueues the job and
-- advances next_run_at IN THE SAME TRANSACTION. That buys, for free:
--
--   * no leader election for cron at all — the row lock IS the election;
--   * a missed occurrence runs late, once, because the overdue row is still
--     there when the process comes back;
--   * no drift, because the next time is computed from the previous target
--     rather than from "now";
--   * schedules queryable and mutable at runtime, which Oban, River, good_job
--     and procrastinate cannot offer because their schedules live in config.
-- ---------------------------------------------------------------------------
CREATE TABLE stampede_schedules (
    key             text        PRIMARY KEY,

    queue           text        NOT NULL,
    task            text        NOT NULL,
    args            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    tenant_id       text,
    priority        smallint    NOT NULL DEFAULT 0,

    -- Interval-based for now. A cron expression parser belongs in Rust, not in
    -- PL/pgSQL; when it lands it sets next_run_at the same way.
    interval_secs   bigint      NOT NULL CHECK (interval_secs > 0),

    next_run_at     timestamptz NOT NULL DEFAULT now(),
    last_run_at     timestamptz,

    -- DST policy, per schedule (plans/11).
    --
    -- Every surveyed implementation keys occurrences on a UTC instant, so a
    -- wall-clock time that occurs twice on a fall-back day produces two valid
    -- occurrences and the job runs twice. Making this a column rather than an
    -- algorithm lets each schedule choose:
    --
    --   'instant'    — key on the UTC instant. A 02:30 job runs twice on the
    --                  fall-back day. Correct for "every N hours" work.
    --   'wall_clock' — key on local date+time. A 02:30 job runs ONCE on the
    --                  fall-back day and is skipped on spring-forward. Correct
    --                  for "the daily report at 2:30am".
    dst_policy      text        NOT NULL DEFAULT 'instant'
                                CHECK (dst_policy IN ('instant', 'wall_clock')),
    timezone        text        NOT NULL DEFAULT 'UTC',

    -- What to do about occurrences missed while nothing was running.
    -- 'once' runs the overdue occurrence a single time, however far behind it
    -- is. 'skip' abandons it and waits for the next. Nobody wants 400 backfilled
    -- runs after a long outage, so 'once' is the default and backfill is not
    -- offered at all.
    missed_policy   text        NOT NULL DEFAULT 'once'
                                CHECK (missed_policy IN ('once', 'skip')),

    paused          boolean     NOT NULL DEFAULT false,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- The claim path: due, unpaused schedules, oldest first.
CREATE INDEX stampede_schedules_due_idx
    ON stampede_schedules (next_run_at)
    WHERE NOT paused;

-- ---------------------------------------------------------------------------
-- Occurrence ledger — idempotency backstop and audit trail.
--
-- Deliberately a SEPARATE NARROW TABLE, not a column on stampede_jobs.
-- good_job and solid_queue both put the cron key on the jobs table, and both
-- then had to guarantee cron rows are never pruned. Coupling cron correctness
-- to job retention would make the retention horizon load-bearing for
-- correctness. This table is prunable on its own schedule.
--
-- procrastinate's separate ledger is the precedent.
-- ---------------------------------------------------------------------------
CREATE TABLE stampede_cron_occurrences (
    id                bigserial   PRIMARY KEY,
    cron_key          text        NOT NULL REFERENCES stampede_schedules(key) ON DELETE CASCADE,

    -- The DST policy made concrete. For 'instant' this is the UTC epoch
    -- second; for 'wall_clock' it is the local 'YYYY-MM-DD HH24:MI' string,
    -- which collapses the duplicated hour into one token.
    occurrence_token  text        NOT NULL,

    job_id            uuid,
    enqueued_at       timestamptz NOT NULL DEFAULT now(),

    -- The backstop: two processes racing to enqueue the same occurrence, one
    -- wins and one gets zero rows. Verified on 18.6.
    UNIQUE (cron_key, occurrence_token)
);

CREATE INDEX stampede_cron_occurrences_enqueued_idx
    ON stampede_cron_occurrences (enqueued_at);

-- ---------------------------------------------------------------------------
-- Fire due schedules.
--
-- One statement per due schedule, each in its own transaction, so a schedule
-- whose enqueue fails does not roll back the others.
--
-- Returns the schedules that fired.
-- ---------------------------------------------------------------------------
CREATE FUNCTION stampede_run_due_schedules(p_limit integer DEFAULT 100)
-- OUT params are prefixed because an unprefixed `cron_key` shadows the COLUMN
-- of the same name inside ON CONFLICT, and PL/pgSQL reports it as an ambiguous
-- reference at RUNTIME, not at CREATE FUNCTION time.
RETURNS TABLE (out_cron_key text, out_job_id uuid, out_token text)
LANGUAGE plpgsql AS $$
DECLARE
    s        stampede_schedules%ROWTYPE;
    v_token  text;
    v_job    uuid;
    -- Separate from v_job: the ledger's id is bigserial, the job's is uuid.
    -- Reusing one variable for both is a runtime cast error, not a compile one.
    v_occ    bigint;
    v_next   timestamptz;
BEGIN
    FOR s IN
        SELECT * FROM stampede_schedules
         WHERE NOT paused AND next_run_at <= now()
         ORDER BY next_run_at
         LIMIT p_limit
         -- The row lock IS the leader election. No peers table, no lease.
         FOR UPDATE SKIP LOCKED
    LOOP
        v_token := CASE s.dst_policy
            WHEN 'wall_clock' THEN
                to_char(s.next_run_at AT TIME ZONE s.timezone, 'YYYY-MM-DD HH24:MI')
            ELSE
                extract(epoch FROM s.next_run_at)::bigint::text
        END;

        -- Advance from the PREVIOUS TARGET, not from now(): computing from now
        -- accumulates drift on every late run, which is how "every hour"
        -- silently becomes "every hour and four seconds".
        v_next := s.next_run_at + make_interval(secs => s.interval_secs);

        -- A schedule that fell far behind catches up to the present rather than
        -- replaying every missed slot. 'once' is the whole policy: run the
        -- overdue occurrence a single time, then resume normally.
        IF s.missed_policy = 'skip' OR v_next <= now() THEN
            WHILE v_next <= now() LOOP
                v_next := v_next + make_interval(secs => s.interval_secs);
            END LOOP;
        END IF;

        INSERT INTO stampede_cron_occurrences (cron_key, occurrence_token)
        VALUES (s.key, v_token)
        ON CONFLICT (cron_key, occurrence_token) DO NOTHING
        RETURNING id INTO v_occ;

        IF v_occ IS NULL THEN
            -- Someone already enqueued this occurrence. Still advance, or we
            -- spin on it forever.
            UPDATE stampede_schedules
               SET next_run_at = v_next, updated_at = now()
             WHERE key = s.key;
            CONTINUE;
        END IF;

        INSERT INTO stampede_jobs (queue, task, args, tenant_id, priority)
        VALUES (s.queue, s.task, s.args, s.tenant_id, s.priority)
        RETURNING id INTO v_job;

        UPDATE stampede_cron_occurrences
           SET job_id = v_job
         WHERE cron_key = s.key AND occurrence_token = v_token;

        UPDATE stampede_schedules
           SET next_run_at = v_next, last_run_at = now(), updated_at = now()
         WHERE key = s.key;

        out_cron_key := s.key;
        out_job_id   := v_job;
        out_token    := v_token;
        RETURN NEXT;
    END LOOP;
END;
$$;

-- Prune the ledger on its own horizon, independent of job retention.
CREATE FUNCTION stampede_prune_occurrences(p_older_than interval DEFAULT interval '30 days')
RETURNS integer
LANGUAGE sql AS $$
    WITH doomed AS (
        SELECT id FROM stampede_cron_occurrences
         WHERE enqueued_at < now() - p_older_than
         LIMIT 10000
    )
    DELETE FROM stampede_cron_occurrences o USING doomed d
     WHERE o.id = d.id
    RETURNING 1;
$$;
