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

    -- Exactly one of the two. A cron expression is parsed in Rust (jiff does
    -- the timezone arithmetic -- see crates/stampede-core/src/cron.rs), which is
    -- why the firing loop lives there rather than in PL/pgSQL; a fixed interval
    -- needs no parser at all and is kept for the common "every N seconds" case.
    interval_secs   bigint      CHECK (interval_secs > 0),
    cron_expr       text,
    CONSTRAINT stampede_schedules_one_rule CHECK (
        (interval_secs IS NULL) <> (cron_expr IS NULL)
    ),

    next_run_at     timestamptz NOT NULL DEFAULT now(),
    last_run_at     timestamptz,

    -- DST policy, per schedule (plans/11).
    --
    -- Every surveyed implementation keys occurrences on a UTC instant, so a
    -- wall-clock time that occurs twice on a fall-back day produces two valid
    -- occurrences and the job runs twice. Making this a column rather than an
    -- algorithm lets each schedule choose:
    --
    --   'instant'    — key on the UTC instant. A 01:30 job runs twice on the
    --                  fall-back day, an hour of real time apart. Correct for
    --                  "every N hours" work.
    --   'wall_clock' — key on local date+time, which collapses the repeated
    --                  hour into one token. A 01:30 job runs ONCE on the
    --                  fall-back day; a 02:30 job on a spring-forward day runs
    --                  at 03:30, the instant the gap closes, rather than being
    --                  lost. Correct for "the daily report at 2:30am".
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
-- Firing, in two halves.
--
-- It used to be one PL/pgSQL loop. Cron expressions broke that: computing the
-- next occurrence needs a parser and a timezone database, and PL/pgSQL has
-- neither. Rather than write a cron parser in SQL, the LOOP moved to Rust and
-- the transaction stayed put:
--
--     BEGIN
--       stampede_claim_due_schedules(n)   -- takes the row locks
--       ...Rust computes each next_run_at with jiff...
--       stampede_fire_schedule(key, next) -- once per schedule
--     COMMIT
--
-- The row lock is still the leader election, and it is held across the round
-- trip because it is all one transaction. Nothing about the concurrency
-- argument changed; only who does the arithmetic.
-- ---------------------------------------------------------------------------

-- Take the locks. MUST be called inside a transaction: the locks are what stop
-- two nodes firing the same occurrence, and they last exactly as long as it.
CREATE FUNCTION stampede_claim_due_schedules(p_limit integer DEFAULT 100)
RETURNS SETOF stampede_schedules
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
        SELECT * FROM stampede_schedules
         WHERE NOT paused AND next_run_at <= now()
         ORDER BY next_run_at
         LIMIT p_limit
         FOR UPDATE SKIP LOCKED;
END;
$$;

-- Fire one claimed schedule and advance it. `p_next_run_at` comes from Rust.
--
-- Returns the job id, or NULL if this occurrence was already enqueued by
-- someone else — in which case the schedule is still advanced, or we would spin
-- on it forever.
CREATE FUNCTION stampede_fire_schedule(p_key text, p_next_run_at timestamptz)
RETURNS uuid
LANGUAGE plpgsql AS $$
DECLARE
    s        stampede_schedules%ROWTYPE;
    v_token  text;
    v_job    uuid;
    -- Separate from v_job: the ledger's id is bigserial, the job's is uuid.
    -- Reusing one variable for both is a runtime cast error, not a compile one.
    v_occ    bigint;
BEGIN
    SELECT * INTO s FROM stampede_schedules WHERE key = p_key;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    v_token := CASE s.dst_policy
        WHEN 'wall_clock' THEN
            to_char(s.next_run_at AT TIME ZONE s.timezone, 'YYYY-MM-DD HH24:MI')
        ELSE
            extract(epoch FROM s.next_run_at)::bigint::text
    END;

    INSERT INTO stampede_cron_occurrences (cron_key, occurrence_token)
    VALUES (s.key, v_token)
    ON CONFLICT (cron_key, occurrence_token) DO NOTHING
    RETURNING id INTO v_occ;

    IF v_occ IS NULL THEN
        UPDATE stampede_schedules
           SET next_run_at = p_next_run_at, updated_at = now()
         WHERE key = s.key;
        RETURN NULL;
    END IF;

    INSERT INTO stampede_jobs (queue, task, args, tenant_id, priority)
    VALUES (s.queue, s.task, s.args, s.tenant_id, s.priority)
    RETURNING id INTO v_job;

    UPDATE stampede_cron_occurrences
       SET job_id = v_job
     WHERE cron_key = s.key AND occurrence_token = v_token;

    UPDATE stampede_schedules
       SET next_run_at = p_next_run_at, last_run_at = now(), updated_at = now()
     WHERE key = s.key;

    RETURN v_job;
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
