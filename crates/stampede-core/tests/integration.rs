//! Integration tests against a real Postgres 18.
//!
//! Skipped unless `STAMPEDE_TEST_DATABASE_URL` is set — these assert behaviour
//! that only the database can produce (SKIP LOCKED disjointness, partial unique
//! indexes, the claim fence), so a mock would only prove the mock works.
//!
//!   STAMPEDE_TEST_DATABASE_URL=postgres://ubuntu:ubuntu@localhost/stampede_test \
//!     cargo test --test integration

use chrono::Utc;
use sqlx::{AssertSqlSafe, PgPool};
use stampede_core::{
    DstPolicy, FailureKind, JobState, NewJob, Period, Queue, Wakeup, Worker, WorkerConfig,
};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Each test gets its own Postgres schema.
///
/// Cargo runs integration tests in parallel; sharing one schema means ten tests
/// racing to DROP and CREATE the same tables. A schema per test keeps them
/// parallel AND isolated, which matters here because several assert on absolute
/// counts.
async fn setup() -> Option<Queue> {
    let url = std::env::var("STAMPEDE_TEST_DATABASE_URL").ok()?;

    let schema = format!("t_{}", Uuid::now_v7().simple());
    let admin = PgPool::connect(&url).await.expect("connect");
    // Schema name is UUID-derived, not user input.
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create schema");
    admin.close().await;

    let search_path = schema.clone();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .after_connect(move |conn, _| {
            let search_path = search_path.clone();
            Box::pin(async move {
                sqlx::query(AssertSqlSafe(format!("SET search_path TO {search_path}")))
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .expect("pool");

    stampede_core::schema::migrate(&pool)
        .await
        .expect("migrate");
    Some(Queue::new(pool))
}

/// `setup`, plus the URL a listener needs.
///
/// PgListener opens its OWN connection rather than borrowing from the pool, and
/// that connection does not inherit the pool's search_path. It does not need to:
/// the channel name is fully qualified (`t_abc.stampede_insert`) and the trigger
/// builds the same string from its own `current_schema()`. So these tests still
/// get a private schema and can run in parallel with everything else.
async fn setup_with_url() -> Option<(Queue, String)> {
    let url = std::env::var("STAMPEDE_TEST_DATABASE_URL").ok()?;
    let q = setup().await?;
    q.create_queue("default").await.expect("create queue");
    Some((q, url))
}

macro_rules! q {
    () => {
        match setup().await {
            Some(q) => q,
            None => {
                eprintln!("skipping: STAMPEDE_TEST_DATABASE_URL not set");
                return;
            }
        }
    };
}

#[tokio::test]
async fn enqueue_and_claim_increments_attempt() {
    let q = q!();
    let w = q
        .register_worker("test", 1, &["default".into()])
        .await
        .unwrap();

    q.enqueue(NewJob::new("default", "send"))
        .await
        .unwrap()
        .unwrap();

    let jobs = q.claim("default", w, 10).await.unwrap();
    assert_eq!(jobs.len(), 1);
    // Incremented AT CLAIM, not at failure -- the poison-job brake (plans/04).
    assert_eq!(jobs[0].attempt, 1);
    assert_eq!(jobs[0].claimed_by, Some(w));
    assert_eq!(jobs[0].attempted_by, vec![w.to_string()]);
}

#[tokio::test]
async fn dedup_is_per_queue_and_null_scope_is_unconstrained() {
    let q = q!();

    assert!(
        q.enqueue(NewJob::new("a", "t").unique("k"))
            .await
            .unwrap()
            .is_some()
    );
    // Same scope, same queue: suppressed.
    assert!(
        q.enqueue(NewJob::new("a", "t").unique("k"))
            .await
            .unwrap()
            .is_none()
    );
    // Same scope, different queue: allowed. Uniqueness is per-queue because the
    // partition key must lead the unique index (plans/README §1).
    assert!(
        q.enqueue(NewJob::new("b", "t").unique("k"))
            .await
            .unwrap()
            .is_some()
    );
    // No scope: never deduplicated.
    assert!(q.enqueue(NewJob::new("a", "t")).await.unwrap().is_some());
    assert!(q.enqueue(NewJob::new("a", "t")).await.unwrap().is_some());
}

#[tokio::test]
async fn completing_releases_the_dedup_slot() {
    let q = q!();
    let w = q
        .register_worker("test", 1, &["default".into()])
        .await
        .unwrap();

    q.enqueue(NewJob::new("default", "t").unique("once"))
        .await
        .unwrap()
        .unwrap();
    // Blocked while the first is still live.
    assert!(
        q.enqueue(NewJob::new("default", "t").unique("once"))
            .await
            .unwrap()
            .is_none()
    );

    let job = q.claim("default", w, 1).await.unwrap().pop().unwrap();
    let t = q.complete(&job, w).await.unwrap();
    assert!(t.owned);
    assert_eq!(t.now.as_deref(), Some("completed"));

    // Slot released, so the same scope can be enqueued again.
    assert!(
        q.enqueue(NewJob::new("default", "t").unique("once"))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn concurrent_workers_claim_disjoint_jobs() {
    let q = q!();
    let a = q
        .register_worker("a", 1, &["default".into()])
        .await
        .unwrap();
    let b = q
        .register_worker("b", 2, &["default".into()])
        .await
        .unwrap();

    for _ in 0..10 {
        q.enqueue(NewJob::new("default", "t")).await.unwrap();
    }

    let (ja, jb) = tokio::join!(q.claim("default", a, 5), q.claim("default", b, 5));
    let (ja, jb) = (ja.unwrap(), jb.unwrap());

    assert_eq!(
        ja.len() + jb.len(),
        10,
        "SKIP LOCKED should hand out all 10"
    );
    let ids: std::collections::HashSet<Uuid> = ja.iter().chain(jb.iter()).map(|j| j.id).collect();
    assert_eq!(ids.len(), 10, "a job was handed to two workers");
}

#[tokio::test]
async fn claim_never_exceeds_limit() {
    // The CTE fence (AS MATERIALIZED). Without it the planner may nested-loop
    // the LIMIT subquery and update more rows than asked -- Oban's documented
    // bug, which pushes attempt past max_attempts (plans/04).
    let q = q!();
    let w = q
        .register_worker("test", 1, &["default".into()])
        .await
        .unwrap();
    for _ in 0..50 {
        q.enqueue(NewJob::new("default", "t")).await.unwrap();
    }
    for _ in 0..5 {
        let got = q.claim("default", w, 3).await.unwrap();
        assert!(
            got.len() <= 3,
            "claim returned {} rows for limit 3",
            got.len()
        );
    }
}

#[tokio::test]
async fn retryable_is_reclaimable_so_poison_jobs_terminate() {
    // The bug the schema tests caught: if `retryable` is not claimable, a job
    // whose worker keeps dying never spends its attempt budget and loops
    // forever.
    let q = q!();
    let w = q
        .register_worker("test", 1, &["default".into()])
        .await
        .unwrap();
    q.enqueue(NewJob::new("default", "poison").max_attempts(3))
        .await
        .unwrap();

    for round in 1..=3 {
        let job = q
            .claim("default", w, 1)
            .await
            .unwrap()
            .pop()
            .unwrap_or_else(|| panic!("round {round}: nothing claimable"));
        assert_eq!(job.attempt, round);
        q.fail(
            &job,
            w,
            FailureKind::Raised,
            "boom",
            Some(std::time::Duration::ZERO),
        )
        .await
        .unwrap();
    }

    // Budget spent: discarded, and no longer claimable.
    assert!(q.claim("default", w, 1).await.unwrap().is_empty());
    let depth = q.depth("default").await.unwrap();
    assert!(
        depth
            .iter()
            .all(|(s, _)| s != "available" && s != "retryable")
    );
}

#[tokio::test]
async fn reaper_reclaims_jobs_from_dead_workers_as_worker_missing() {
    let q = q!();
    let w = q
        .register_worker("test", 1, &["default".into()])
        .await
        .unwrap();
    q.enqueue(NewJob::new("default", "t")).await.unwrap();
    let _job = q.claim("default", w, 1).await.unwrap().pop().unwrap();

    // Worker stops heartbeating.
    sqlx::query("UPDATE stampede_workers SET last_heartbeat = now() - interval '1 hour'")
        .execute(q.pool())
        .await
        .unwrap();

    let n = q.reap(std::time::Duration::from_secs(300)).await.unwrap();
    assert_eq!(n, 1);

    // Reclaimed as `worker_missing`: the worker vanished without reporting,
    // which is a different event from the job raising. Only BullMQ and
    // solid_queue make that distinction at all (plans/15), and it is what
    // charges the interruptions budget rather than the attempt budget.
    let (kind, interruptions): (Option<String>, i16) =
        sqlx::query_as("SELECT failure_kind::text, interruptions FROM stampede_jobs")
            .fetch_one(q.pool())
            .await
            .unwrap();
    assert_eq!(kind.as_deref(), Some("worker_missing"));
    assert_eq!(interruptions, 1, "a reclaim charges the interruptions budget");

    // And it is claimable again, so the attempt budget can be spent.
    assert_eq!(q.claim("default", w, 1).await.unwrap().len(), 1);
}

#[tokio::test]
async fn cannot_complete_a_job_we_no_longer_own() {
    let q = q!();
    let a = q
        .register_worker("a", 1, &["default".into()])
        .await
        .unwrap();
    let b = q
        .register_worker("b", 2, &["default".into()])
        .await
        .unwrap();

    q.enqueue(NewJob::new("default", "t")).await.unwrap();
    let job = q.claim("default", a, 1).await.unwrap().pop().unwrap();

    // Someone else takes it (as the reaper would).
    sqlx::query("UPDATE stampede_jobs SET claimed_by = $1")
        .bind(b)
        .execute(q.pool())
        .await
        .unwrap();

    let t = q.complete(&job, a).await.unwrap();
    assert!(
        !t.owned,
        "must refuse to complete a job owned by another worker"
    );
}

#[tokio::test]
async fn prune_removes_only_finished_jobs() {
    let q = q!();
    let w = q
        .register_worker("test", 1, &["default".into()])
        .await
        .unwrap();

    q.enqueue(NewJob::new("default", "done")).await.unwrap();
    q.enqueue(NewJob::new("default", "live")).await.unwrap();

    let job = q.claim("default", w, 1).await.unwrap().pop().unwrap();
    q.complete(&job, w).await.unwrap();

    // Nothing is old enough yet.
    assert_eq!(
        q.prune_batch(
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            100,
            Duration::from_secs(10)
        )
        .await
        .unwrap(),
        0
    );

    sqlx::query("UPDATE stampede_jobs SET finished_at = now() - interval '30 days' WHERE finished_at IS NOT NULL")
        .execute(q.pool())
        .await
        .unwrap();

    assert_eq!(
        q.prune_batch(
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            100,
            Duration::from_secs(10)
        )
        .await
        .unwrap(),
        1
    );
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM stampede_jobs")
        .fetch_one(q.pool())
        .await
        .unwrap();
    assert_eq!(left, 1, "the unfinished job must survive pruning");
}

#[tokio::test]
async fn create_queue_is_idempotent_and_applies_partition_settings() {
    let q = q!();
    q.create_queue("mail").await.unwrap();
    q.create_queue("mail").await.unwrap();

    // Storage and autovacuum settings are NOT inherited from the partitioned
    // parent; a queue provisioned without them silently runs on defaults and
    // bloats (schema/001 note).
    let opts: Option<Vec<String>> =
        sqlx::query_scalar("SELECT reloptions FROM pg_class WHERE relname = 'stampede_jobs_mail'")
            .fetch_one(q.pool())
            .await
            .unwrap();

    let opts = opts.expect("partition has no reloptions");
    assert!(opts.iter().any(|o| o.starts_with("fillfactor=")));
    assert!(
        opts.iter()
            .any(|o| o.contains("autovacuum_vacuum_scale_factor"))
    );
}
// temporary probe appended to the test file

#[tokio::test]
async fn schedules_fire_once_and_do_not_drift() {
    let q = q!();
    q.upsert_schedule(
        "hourly",
        "default",
        "report",
        std::time::Duration::from_secs(3600),
    )
    .await
    .unwrap();

    // Due immediately (next_run_at defaults to now()).
    assert_eq!(q.run_due_schedules(10).await.unwrap(), 1);
    // Not due again for an hour.
    assert_eq!(q.run_due_schedules(10).await.unwrap(), 0);

    let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM stampede_jobs WHERE task='report'")
        .fetch_one(q.pool())
        .await
        .unwrap();
    assert_eq!(jobs, 1);
}

#[tokio::test]
async fn a_schedule_far_behind_catches_up_without_backfilling() {
    // Nobody wants 72 backfilled runs after a three-day outage.
    let q = q!();
    q.upsert_schedule(
        "behind",
        "default",
        "catchup",
        std::time::Duration::from_secs(3600),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE stampede_schedules SET next_run_at = now() - interval '3 days'")
        .execute(q.pool())
        .await
        .unwrap();

    assert_eq!(
        q.run_due_schedules(10).await.unwrap(),
        1,
        "should fire exactly once"
    );

    let future: bool =
        sqlx::query_scalar("SELECT next_run_at > now() FROM stampede_schedules WHERE key='behind'")
            .fetch_one(q.pool())
            .await
            .unwrap();
    assert!(future, "schedule should have caught up to the present");
}

#[tokio::test]
async fn concurrent_schedulers_do_not_double_fire() {
    // No leader election: exclusion is the row lock inside
    // stampede_claim_due_schedules. Two nodes ticking at once must still produce
    // exactly one job.
    let q = q!();
    q.upsert_schedule(
        "shared",
        "default",
        "once",
        std::time::Duration::from_secs(3600),
    )
    .await
    .unwrap();

    let (a, b) = tokio::join!(q.run_due_schedules(10), q.run_due_schedules(10));
    assert_eq!(
        a.unwrap() + b.unwrap(),
        1,
        "the schedule fired more than once"
    );
}

#[tokio::test]
async fn cron_schedules_fire_and_advance_by_the_expression() {
    let q = q!();
    // Every minute, so the first occurrence is at most 60s out and we can make
    // it due by hand rather than sleeping.
    q.upsert_cron_schedule(
        "ticker",
        "default",
        "tick",
        "* * * * *",
        "UTC",
        Some(DstPolicy::Instant),
    )
    .await
    .unwrap();

    // Seeded to the NEXT occurrence, not to now: a schedule must not fire the
    // instant it is created.
    assert_eq!(
        q.run_due_schedules(10).await.unwrap(),
        0,
        "fired on creation"
    );

    sqlx::query("UPDATE stampede_schedules SET next_run_at = now() - interval '1 minute'")
        .execute(q.pool())
        .await
        .unwrap();

    assert_eq!(q.run_due_schedules(10).await.unwrap(), 1);
    assert_eq!(
        q.run_due_schedules(10).await.unwrap(),
        0,
        "fired twice for one occurrence"
    );

    let (expr, secs): (Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT cron_expr, interval_secs FROM stampede_schedules WHERE key='ticker'",
    )
    .fetch_one(q.pool())
    .await
    .unwrap();
    assert_eq!(expr.as_deref(), Some("* * * * *"));
    assert!(secs.is_none(), "cron and interval are mutually exclusive");
}

#[tokio::test]
async fn a_malformed_cron_expression_is_rejected_at_write_time() {
    // Parsing on write means a typo is a failed API call, not a row that warns
    // once a second forever.
    let q = q!();
    assert!(
        q.upsert_cron_schedule(
            "bad",
            "default",
            "t",
            "77 * * * *",
            "UTC",
            Some(DstPolicy::Instant)
        )
        .await
        .is_err()
    );
    assert!(
        q.upsert_cron_schedule(
            "bad",
            "default",
            "t",
            "* * * * *",
            "Mars/Olympus",
            Some(DstPolicy::Instant)
        )
        .await
        .is_err(),
        "an unknown timezone should be rejected too"
    );
}

#[tokio::test]
async fn switching_a_schedule_between_cron_and_interval_clears_the_other_rule() {
    // The CHECK constraint enforces exactly one rule, so an upsert that did not
    // null out the previous one would fail the write.
    let q = q!();
    q.upsert_schedule("flip", "default", "t", std::time::Duration::from_secs(60))
        .await
        .unwrap();
    q.upsert_cron_schedule(
        "flip",
        "default",
        "t",
        "0 3 * * *",
        "UTC",
        Some(DstPolicy::WallClock),
    )
    .await
    .unwrap();
    q.upsert_schedule("flip", "default", "t", std::time::Duration::from_secs(60))
        .await
        .unwrap();

    let (expr, secs): (Option<String>, Option<i64>) =
        sqlx::query_as("SELECT cron_expr, interval_secs FROM stampede_schedules WHERE key='flip'")
            .fetch_one(q.pool())
            .await
            .unwrap();
    assert!(expr.is_none());
    assert_eq!(secs, Some(60));
}

#[tokio::test]
async fn paused_schedules_never_fire() {
    let q = q!();
    q.upsert_schedule(
        "paused",
        "default",
        "nope",
        std::time::Duration::from_secs(1),
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE stampede_schedules SET paused = true, next_run_at = now() - interval '1 hour'",
    )
    .execute(q.pool())
    .await
    .unwrap();

    assert_eq!(q.run_due_schedules(10).await.unwrap(), 0);
}

// ---------------------------------------------------------------- plans/15

#[tokio::test]
async fn our_own_backpressure_refunds_the_attempt() {
    // The correctness rule. `attempt` is spent at claim, so declining to run a
    // job has to give it back -- otherwise a long downstream outage silently
    // burns every queued job's budget and discards the whole backlog.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "calls_api"))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();

    let claimed = q.claim("default", w, 1).await.unwrap();
    assert_eq!(claimed[0].attempt, 1, "claim spends an attempt");

    q.fail(
        &claimed[0],
        w,
        FailureKind::BreakerOpen,
        "circuit open",
        Some(Duration::from_millis(1)),
    )
    .await
    .unwrap();

    let job = q.fetch("default", id).await.unwrap().unwrap();
    assert_eq!(job.attempt, 0, "breaker_open must refund");
    assert_eq!(job.state, JobState::Retryable);
    assert_eq!(job.interruptions, 0, "a refund is not also an interruption");
}

#[tokio::test]
async fn a_raise_does_not_refund() {
    // The other half: if everything refunded, max_attempts would never be
    // reached and the retry budget would be decorative.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "buggy"))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    let claimed = q.claim("default", w, 1).await.unwrap();

    q.fail(
        &claimed[0],
        w,
        FailureKind::Raised,
        "boom",
        Some(Duration::from_millis(1)),
    )
    .await
    .unwrap();

    assert_eq!(q.fetch("default", id).await.unwrap().unwrap().attempt, 1);
}

#[tokio::test]
async fn a_job_that_kills_workers_is_discarded_long_before_max_attempts() {
    // The interruptions brake. With max_attempts 25 and one free interruption,
    // a job that segfaults its worker must stop after the SECOND claim -- not
    // walk 25 workers down one process at a time.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "segfaults"))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();

    let c = q.claim("default", w, 1).await.unwrap();
    q.fail_with_exit(
        &c[0],
        w,
        FailureKind::WorkerExited,
        "killed",
        Some(Duration::from_millis(1)),
        Some(137),
    )
    .await
    .unwrap();
    assert_eq!(
        q.fetch("default", id).await.unwrap().unwrap().state,
        JobState::Retryable
    );

    tokio::time::sleep(Duration::from_millis(20)).await;
    let c = q.claim("default", w, 1).await.unwrap();
    assert_eq!(
        c.len(),
        1,
        "should be claimable again after one interruption"
    );
    q.fail_with_exit(
        &c[0],
        w,
        FailureKind::WorkerExited,
        "killed",
        Some(Duration::from_millis(1)),
        Some(137),
    )
    .await
    .unwrap();

    let job = q.fetch("default", id).await.unwrap().unwrap();
    assert_eq!(
        job.state,
        JobState::Discarded,
        "the interruptions budget, not max_attempts"
    );
    assert_eq!(job.interruptions, 2);
    assert!(
        job.attempt < job.max_attempts,
        "attempt budget was nowhere near spent"
    );

    // The exit status survives, which is what makes an OOM diagnosable.
    let errors = job.errors.as_array().expect("errors is an array");
    assert_eq!(errors.len(), 2);
    assert_eq!(errors[0]["exit_status"], 137);
}

#[tokio::test]
async fn the_error_history_is_bounded() {
    // River bounds attempted_by but appends errors unbounded -- the half that
    // actually leaks, since error text dwarfs a worker id.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "flaky").max_attempts(100))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();

    for i in 0..15 {
        let c = q.claim("default", w, 1).await.unwrap();
        assert_eq!(c.len(), 1, "iteration {i}");
        q.fail(
            &c[0],
            w,
            FailureKind::Raised,
            &format!("failure {i}"),
            Some(Duration::from_millis(1)),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let job = q.fetch("default", id).await.unwrap().unwrap();
    let errors = job.errors.as_array().unwrap();
    assert_eq!(errors.len(), 10, "ring buffer must cap at 10");
    assert_eq!(errors[9]["message"], "failure 14", "newest kept");
    assert_eq!(errors[0]["message"], "failure 5", "oldest dropped");
}

#[tokio::test]
async fn a_terminal_failure_dead_letters_with_provenance() {
    // pg-boss's design: the DLQ is a queue, so re-drive is just working it.
    // The provenance is what makes the resulting row actionable -- a dead
    // letter with no history is an alert nobody can do anything with.
    let q = q!();
    q.create_queue("dead").await.unwrap();

    let id = q
        .enqueue(
            NewJob::new("default", "doomed")
                .max_attempts(1)
                .dead_letter("dead"),
        )
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    let c = q.claim("default", w, 1).await.unwrap();
    q.fail(&c[0], w, FailureKind::Raised, "boom", None)
        .await
        .unwrap();

    assert_eq!(
        q.fetch("default", id).await.unwrap().unwrap().state,
        JobState::Discarded
    );

    let letters = q.claim("dead", w, 10).await.unwrap();
    assert_eq!(
        letters.len(),
        1,
        "the failure should have landed in the DLQ"
    );
    assert_eq!(letters[0].task, "doomed");
    assert_eq!(letters[0].source_queue.as_deref(), Some("default"));
    assert_eq!(letters[0].source_id, Some(id));
    assert_eq!(letters[0].source_attempts, Some(1));
    assert!(
        letters[0].dead_letter_queue.is_none(),
        "a dead letter must not itself dead-letter"
    );
}

#[tokio::test]
async fn a_wedged_job_expires_even_though_its_worker_is_healthy() {
    // The case neither other timeout layer covers: the worker is alive and
    // heartbeating normally, so the reaper sees nothing wrong, but the job
    // itself is stuck.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "wedged").expires_in(Duration::from_secs(1)))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    q.claim("default", w, 1).await.unwrap();

    assert_eq!(q.expire(10).await.unwrap(), 0, "not expired yet");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(q.expire(10).await.unwrap(), 1);

    let job = q.fetch("default", id).await.unwrap().unwrap();
    assert_eq!(job.failure_kind, Some(FailureKind::TimedOut));
    assert_eq!(
        job.state,
        JobState::Retryable,
        "expiry uses the normal fail path"
    );
}

#[tokio::test]
async fn touching_a_long_job_keeps_it_alive() {
    // A legitimately slow job must be distinguishable from a wedged one, or
    // the only available response is to guess a timeout.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "slow").expires_in(Duration::from_secs(1)))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    let c = q.claim("default", w, 1).await.unwrap();

    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            q.touch(&c[0], w).await.unwrap(),
            "touch should succeed while we own it"
        );
        assert_eq!(
            q.expire(10).await.unwrap(),
            0,
            "a touching job must not expire"
        );
    }
    assert_eq!(
        q.fetch("default", id).await.unwrap().unwrap().state,
        JobState::Executing
    );

    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        q.expire(10).await.unwrap(),
        1,
        "stop touching and it expires"
    );
}

// ---------------------------------------------------------------- plans/13

#[tokio::test]
async fn depth_comes_from_counters_and_tracks_transitions() {
    // The counters must follow the job through its whole lifecycle, or the
    // dashboard lies -- which is worse than being slow, since nobody checks a
    // number they believe.
    let q = q!();
    for _ in 0..5 {
        q.enqueue(NewJob::new("default", "x")).await.unwrap();
    }
    assert_eq!(depth_of(&q, "available").await, 5);

    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    let claimed = q.claim("default", w, 2).await.unwrap();
    assert_eq!(depth_of(&q, "available").await, 3);
    assert_eq!(depth_of(&q, "executing").await, 2);

    q.complete(&claimed[0], w).await.unwrap();
    assert_eq!(depth_of(&q, "executing").await, 1);
    assert_eq!(depth_of(&q, "completed").await, 1);
}

#[tokio::test]
async fn recount_repairs_drifted_counters() {
    // Counters drift is a bug, but one that must be repairable without
    // downtime -- otherwise the only fix is the aggregate we banned.
    let q = q!();
    for _ in 0..4 {
        q.enqueue(NewJob::new("default", "x")).await.unwrap();
    }
    sqlx::query("UPDATE stampede_queue_counts SET n = 999 WHERE queue = 'default'")
        .execute(q.pool())
        .await
        .unwrap();
    assert_eq!(depth_of(&q, "available").await, 999);

    q.recount("default").await.unwrap();
    assert_eq!(depth_of(&q, "available").await, 4);
}

async fn depth_of(q: &Queue, state: &str) -> i64 {
    q.depth("default")
        .await
        .unwrap()
        .into_iter()
        .find(|(s, _)| s == state)
        .map(|(_, n)| n)
        .unwrap_or(0)
}

// ---------------------------------------------------------------- plans/12

#[tokio::test]
async fn the_trigger_notifies_and_the_listener_hears_it() {
    // The trigger has to reach a listener that is not the enqueuer. This is
    // the whole polyglot wakeup argument: a Ruby script running a plain INSERT
    // runs none of our code, so if the DATABASE did not notify, nothing would.
    let Some((q, url)) = setup_with_url().await else {
        eprintln!("skipping: STAMPEDE_TEST_DATABASE_URL not set");
        return;
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);

    let wakeup = Wakeup::start(q.pool(), &url, rx).await.unwrap();
    assert!(
        wakeup.is_healthy(),
        "sonar should confirm the path on a direct connection"
    );

    let mut woke = wakeup.subscribe("default");

    // Enqueued with raw SQL, exactly as a non-Rust client would.
    sqlx::query("INSERT INTO stampede_jobs (queue, task) VALUES ('default', 'x')")
        .execute(q.pool())
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), woke.recv())
        .await
        .expect("a plain INSERT must wake a listener");
}

#[tokio::test]
async fn a_bulk_insert_collapses_into_one_notification_per_queue() {
    // Why the trigger is statement-level with a bare queue name as the payload.
    // Postgres collapses identical (channel, payload) pairs within a
    // transaction, so 500 rows is one notification -- self-throttling for free.
    // River tore its trigger out citing notification storms, which is a
    // problem specific to the per-row form it shipped.
    let Some((q, url)) = setup_with_url().await else {
        return;
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let wakeup = Wakeup::start(q.pool(), &url, rx).await.unwrap();
    let mut woke = wakeup.subscribe("default");

    sqlx::query(
        "INSERT INTO stampede_jobs (queue, task) \
         SELECT 'default', 'bulk' FROM generate_series(1, 500)",
    )
    .execute(q.pool())
    .await
    .unwrap();

    tokio::time::timeout(Duration::from_secs(5), woke.recv())
        .await
        .expect("should wake");

    // Drain: if the payload carried a nonce (Graphile's {"r":random()}) there
    // would be 500 of these, which is exactly the storm River complained about.
    let mut extra = 0;
    while tokio::time::timeout(Duration::from_millis(300), woke.recv())
        .await
        .is_ok()
    {
        extra += 1;
        if extra > 5 {
            break;
        }
    }
    assert_eq!(
        extra, 0,
        "500 rows in one transaction must be ONE notification"
    );
}

#[tokio::test]
async fn a_future_dated_job_does_not_wake_anybody() {
    // pg-boss's gate. There is nothing to wake up FOR: the scheduler will
    // promote it when it is due, and waking every worker now would just produce
    // an empty claim.
    let Some((q, url)) = setup_with_url().await else {
        return;
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let wakeup = Wakeup::start(q.pool(), &url, rx).await.unwrap();
    let mut woke = wakeup.subscribe("default");

    q.enqueue(
        NewJob::new("default", "later").scheduled_at(Utc::now() + chrono::Duration::hours(1)),
    )
    .await
    .unwrap();

    let fired = tokio::time::timeout(Duration::from_millis(500), woke.recv()).await;
    assert!(
        fired.is_err(),
        "a job scheduled an hour out must not wake a worker now"
    );
}

#[tokio::test]
async fn failures_are_kept_longer_than_successes() {
    // Two retention clocks. A completed job is evidence nobody will read; a
    // discarded one is evidence somebody is about to be paged about. Oban's
    // 60-second default deletes the failure before the alert fires.
    let q = q!();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();

    let done = q
        .enqueue(NewJob::new("default", "ok"))
        .await
        .unwrap()
        .unwrap();
    let dead = q
        .enqueue(NewJob::new("default", "bad").max_attempts(1))
        .await
        .unwrap()
        .unwrap();

    for c in q.claim("default", w, 2).await.unwrap() {
        if c.id == done {
            q.complete(&c, w).await.unwrap();
        } else {
            q.fail(&c, w, FailureKind::Raised, "boom", None)
                .await
                .unwrap();
        }
    }

    // Retention zero for completed, a week for failed: the success goes, the
    // failure stays.
    let n = q
        .prune_batch(
            Duration::ZERO,
            Duration::from_secs(7 * 24 * 3600),
            100,
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert_eq!(n, 1, "only the completed job should have been pruned");

    assert!(q.fetch("default", done).await.unwrap().is_none());
    assert!(
        q.fetch("default", dead).await.unwrap().is_some(),
        "the discarded job is the evidence; it must outlive the successes"
    );
}

#[tokio::test]
async fn vacuum_health_reports_only_this_schema() {
    // pg_stat_user_tables spans the whole database. Without scoping, a second
    // install -- or another test's schema -- is reported as ours, which is how
    // this was caught: 200 rows for a two-partition install.
    let q = q!();
    let rows = q.vacuum_health().await.unwrap();
    assert!(!rows.is_empty(), "should see our own partitions");
    assert!(
        rows.len() < 10,
        "saw {} tables; the schema filter is not working",
        rows.len()
    );
    assert!(rows.iter().all(|r| r.relname.starts_with("stampede_jobs")));
}

#[tokio::test]
async fn a_dirty_table_is_flagged_for_vacuum() {
    // The detector has to actually fire, or it is decoration. 500 updates
    // against a table with scale_factor 0.02 puts dead tuples well past the
    // threshold autovacuum uses for it.
    let q = q!();
    for _ in 0..500 {
        q.enqueue(NewJob::new("default", "churn")).await.unwrap();
    }
    sqlx::query("UPDATE stampede_jobs SET priority = 1")
        .execute(q.pool())
        .await
        .unwrap();
    sqlx::query("ANALYZE").execute(q.pool()).await.unwrap();

    let rows = q.vacuum_health().await.unwrap();
    let dirty = rows
        .iter()
        .find(|r| r.dead_tuples > 0)
        .expect("dead tuples after 500 updates");
    assert!(
        dirty.needs_vacuum,
        "500 dead against threshold {} should be flagged",
        dirty.autovac_threshold
    );
}

#[tokio::test]
async fn index_density_ignores_reindex_artefacts() {
    // _ccnew/_ccold are transient leftovers of a failed REINDEX CONCURRENTLY.
    // Retrying one is how a reindexer gets stuck in a loop, so they are skipped
    // rather than reported.
    let q = q!();
    let names = q.sparse_indexes().await.unwrap();
    assert!(
        names
            .iter()
            .all(|n| !n.contains("_ccnew") && !n.contains("_ccold"))
    );
}

// ---------------------------------------------------------------- plans/10

#[tokio::test]
async fn a_period_bucket_allows_one_job_per_window() {
    // "One report per hour", not "one report ever". The bucket is computed
    // server-side from inserted_at so enqueuers with skewed clocks still agree
    // on which window they are in -- composing it client-side, which is the
    // obvious alternative, reintroduces exactly that skew.
    let q = q!();

    let first = q
        .enqueue(NewJob::new("default", "report").unique_per("report:42", Period::Hour))
        .await
        .unwrap();
    assert!(first.is_some());

    let second = q
        .enqueue(NewJob::new("default", "report").unique_per("report:42", Period::Hour))
        .await
        .unwrap();
    assert!(
        second.is_none(),
        "same scope, same hour: must be deduplicated"
    );

    // A different scope in the same window is unaffected.
    let other = q
        .enqueue(NewJob::new("default", "report").unique_per("report:43", Period::Hour))
        .await
        .unwrap();
    assert!(other.is_some());
}

#[tokio::test]
async fn different_windows_are_different_keys() {
    // The bucket has to actually participate in the key, or `unique_per` is
    // just `unique` with extra steps. Same scope, different period => different
    // bucket value => both allowed.
    let q = q!();
    assert!(
        q.enqueue(NewJob::new("default", "r").unique_per("k", Period::Hour))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        q.enqueue(NewJob::new("default", "r").unique_per("k", Period::Day))
            .await
            .unwrap()
            .is_some(),
        "a different window is a different key"
    );
}

#[tokio::test]
async fn an_unbucketed_unique_job_is_unique_forever() {
    // No period means no bucket: the key is the scope alone, which is the
    // plain `unique` behaviour and must not change.
    let q = q!();
    assert!(
        q.enqueue(NewJob::new("default", "r").unique("k"))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        q.enqueue(NewJob::new("default", "r").unique("k"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn rescheduling_a_job_does_not_vacate_its_own_bucket() {
    // The trap plans/10 found by experiment: a STORED key bucketed on
    // scheduled_at silently CHANGES when a job is snoozed, so the job vacates
    // its own throttle slot and a duplicate slips in behind it. Bucketing on
    // inserted_at -- which is never updated -- is what prevents that.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "r").unique_per("k", Period::Hour))
        .await
        .unwrap()
        .unwrap();

    let key_before: Vec<u8> =
        sqlx::query_scalar("SELECT dedup_key FROM stampede_jobs WHERE queue='default' AND id=$1")
            .bind(id)
            .fetch_one(q.pool())
            .await
            .unwrap();

    // Snooze it a day: if the bucket followed scheduled_at, the key would move.
    sqlx::query("UPDATE stampede_jobs SET scheduled_at = now() + interval '1 day' WHERE id = $1")
        .bind(id)
        .execute(q.pool())
        .await
        .unwrap();

    let key_after: Vec<u8> =
        sqlx::query_scalar("SELECT dedup_key FROM stampede_jobs WHERE queue='default' AND id=$1")
            .bind(id)
            .fetch_one(q.pool())
            .await
            .unwrap();

    assert_eq!(
        key_before, key_after,
        "rescheduling must not move the dedup key"
    );
    assert!(
        q.enqueue(NewJob::new("default", "r").unique_per("k", Period::Hour))
            .await
            .unwrap()
            .is_none(),
        "the slot must still be held after the reschedule"
    );
}

#[tokio::test]
async fn prune_reports_how_many_it_actually_deleted() {
    // This was a real bug and it hid in plain sight for the whole project.
    //
    // `stampede_prune` is declared RETURNS integer over a `DELETE ... RETURNING
    // 1`, and a SQL function returns the first column of the FIRST ROW -- so it
    // returned 1 whether it deleted one row or ten thousand, and NULL for none.
    // The pruner loops "until the count is less than the batch size", and with
    // a constant 1 that condition is always true: a backlog was pruned one
    // batch per TICK instead of per iteration, and the loop that exists
    // specifically to drain it never ran twice.
    //
    // Nothing failed. The pruner just quietly did a fraction of its job.
    let q = q!();
    let w = q.register_worker("h", 1, &["default".into()]).await.unwrap();

    for _ in 0..7 {
        let id = q.enqueue(NewJob::new("default", "x")).await.unwrap().unwrap();
        let claimed = q.claim("default", w, 1).await.unwrap();
        q.complete(&claimed[0], w).await.unwrap();
        let _ = id;
    }

    // Retention zero: everything finished is eligible.
    let n = q
        .prune_batch(Duration::ZERO, Duration::ZERO, 100, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(n, 7, "must report the real count, not 1");

    let n = q
        .prune_batch(Duration::ZERO, Duration::ZERO, 100, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(n, 0, "nothing left");
}

#[tokio::test]
async fn prune_stops_at_the_batch_limit_so_the_caller_can_loop() {
    // The other half of the same contract: a full batch is how the caller
    // learns there is more to do.
    let q = q!();
    let w = q.register_worker("h", 1, &["default".into()]).await.unwrap();

    for _ in 0..5 {
        q.enqueue(NewJob::new("default", "x")).await.unwrap();
    }
    for c in q.claim("default", w, 5).await.unwrap() {
        q.complete(&c, w).await.unwrap();
    }

    let n = q
        .prune_batch(Duration::ZERO, Duration::ZERO, 2, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(n, 2, "a full batch signals more remaining");

    let mut total = n;
    loop {
        let n = q
            .prune_batch(Duration::ZERO, Duration::ZERO, 2, Duration::from_secs(10))
            .await
            .unwrap();
        total += n;
        if n < 2 {
            break;
        }
    }
    assert_eq!(total, 5, "looping until a short batch must drain everything");
}

#[tokio::test]
async fn a_permanently_open_circuit_eventually_gives_up() {
    // The floor under the refund rule, and a defect that shipped without it.
    //
    // breaker_open and throttled REFUND the attempt, which is what stops a
    // downstream outage discarding the backlog. But if they count toward
    // nothing at all, a permanently open circuit re-claims and re-defers the
    // same job forever: verified at 12 cycles against a max_attempts of 3 with
    // `attempt` still 0. The queue grows and nothing ever reports a failure.
    //
    // So deferrals are counted separately, with a cap generous enough that a
    // real outage never reaches it.
    let q = q!();
    let w = q.register_worker("h", 1, &["default".into()]).await.unwrap();

    let id = q.enqueue(NewJob::new("default", "calls_dead_api")).await.unwrap().unwrap();
    sqlx::query("UPDATE stampede_jobs SET max_deferrals = 3 WHERE id = $1")
        .bind(id)
        .execute(q.pool())
        .await
        .unwrap();

    let mut cycles = 0;
    for _ in 0..20 {
        let claimed = q.claim("default", w, 1).await.unwrap();
        let Some(job) = claimed.into_iter().next() else { break };
        cycles += 1;
        q.fail(&job, w, FailureKind::BreakerOpen, "circuit open", None)
            .await
            .unwrap();
    }

    assert_eq!(cycles, 4, "should defer max_deferrals times, then stop");

    let job = q.fetch("default", id).await.unwrap().unwrap();
    assert_eq!(job.state, JobState::Discarded);
    assert_eq!(job.deferrals, 4);
    // The refund still held the whole way: this job was never charged for a
    // dependency being down, it just stopped waiting.
    assert_eq!(job.attempt, 0, "deferrals must not become a retry budget");
    assert_eq!(
        job.failure_kind,
        Some(FailureKind::BreakerOpen),
        "the recorded cause must still say it was our backpressure"
    );
}

#[tokio::test]
async fn an_ordinary_outage_does_not_exhaust_the_deferral_budget() {
    // The cap must be generous enough to be unreachable in practice. At the
    // capped backoff of an hour, the default of 100 is days of downtime --
    // if this default ever shrinks toward a retry-budget-sized number, the
    // refund rule stops meaning anything.
    let q = q!();
    let id = q.enqueue(NewJob::new("default", "x")).await.unwrap().unwrap();
    let job = q.fetch("default", id).await.unwrap().unwrap();
    assert!(
        job.max_deferrals >= 100,
        "default deferral cap is {}, which is small enough to discard work \
         during a normal outage",
        job.max_deferrals
    );
}

// ---------------------------------------------------------- graceful shutdown

#[tokio::test]
async fn a_graceful_shutdown_lets_jobs_finish_and_charges_nothing() {
    // plans/15: a routine deploy must NOT consume the interruption budget.
    // The drain is what achieves that -- the job reports its own outcome, so
    // nothing is ever recorded as a worker death. This was listed as untested
    // in BOTH implementations; the Ruby side has had a test since the gem
    // landed and this is the Rust one.
    let q = q!();
    let id = q.enqueue(NewJob::new("default", "slow")).await.unwrap().unwrap();

    let started = Arc::new(tokio::sync::Notify::new());
    let ready = started.clone();

    let worker = Worker::new(
        q.clone(),
        WorkerConfig {
            queue: "default".into(),
            poll_interval: Duration::from_millis(50),
            ..Default::default()
        },
    )
    .register("slow", move |_job| {
        let ready = ready.clone();
        async move {
            ready.notify_one();
            tokio::time::sleep(Duration::from_millis(400)).await;
            Ok(())
        }
    });

    let (tx, rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(async move { worker.run(rx).await });

    started.notified().await; // in flight
    let _ = tx.send(true); // deploy

    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("worker should stop")
        .expect("join")
        .expect("run");

    let job = q.fetch("default", id).await.unwrap().unwrap();
    assert_eq!(job.state, JobState::Completed, "the in-flight job must be allowed to finish");
    assert_eq!(job.interruptions, 0, "a graceful shutdown is not a worker death");
    assert_eq!(job.attempt, 1, "and it must not have been retried");
}

#[tokio::test]
async fn a_wedged_job_does_not_block_shutdown_forever() {
    // The other half. An unbounded drain means one stuck job holds the deploy
    // open until the supervisor SIGKILLs the process -- at which point the
    // worker never deregistered and the reaper has to find it the slow way.
    let q = q!();
    q.enqueue(NewJob::new("default", "wedged")).await.unwrap();

    let started = Arc::new(tokio::sync::Notify::new());
    let ready = started.clone();

    let worker = Worker::new(
        q.clone(),
        WorkerConfig {
            queue: "default".into(),
            poll_interval: Duration::from_millis(50),
            shutdown_timeout: Duration::from_millis(300),
            ..Default::default()
        },
    )
    .register("wedged", move |_job| {
        let ready = ready.clone();
        async move {
            ready.notify_one();
            // Longer than any test should wait.
            tokio::time::sleep(Duration::from_secs(120)).await;
            Ok(())
        }
    });

    let (tx, rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(async move { worker.run(rx).await });

    started.notified().await;
    let _ = tx.send(true);

    // Must give up on the wedged job rather than waiting 120s for it.
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("shutdown must be bounded")
        .expect("join")
        .expect("run");

    // And it deregistered, so the reaper can tell the job is orphaned rather
    // than waiting out dead_after on a worker that never said goodbye.
    let workers: i64 = sqlx::query_scalar("SELECT count(*) FROM stampede_workers")
        .fetch_one(q.pool())
        .await
        .unwrap();
    assert_eq!(workers, 0, "a bounded shutdown must still deregister");
}
