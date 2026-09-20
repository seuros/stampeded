//! Integration tests against a real Postgres 18.
//!
//! Skipped unless `STAMPEDED_TEST_DATABASE_URL` is set — these assert behaviour
//! that only the database can produce (SKIP LOCKED disjointness, partial unique
//! indexes, the claim fence), so a mock would only prove the mock works.
//!
//!   STAMPEDED_TEST_DATABASE_URL=postgres://ubuntu:ubuntu@localhost/stampeded_test \
//!     cargo test --test integration

use chrono::Utc;
use sqlx::{AssertSqlSafe, PgPool};
use stampeded::{
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
    let url = std::env::var("STAMPEDED_TEST_DATABASE_URL").ok()?;

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

    stampeded::schema::migrate(&pool).await.expect("migrate");
    Some(Queue::new(pool))
}

/// `setup`, plus the URL a listener needs.
///
/// PgListener opens its OWN connection rather than borrowing from the pool, and
/// that connection does not inherit the pool's search_path. It does not need to:
/// the channel name is fully qualified (`t_abc.stampeded_insert`) and the trigger
/// builds the same string from its own `current_schema()`. So these tests still
/// get a private schema and can run in parallel with everything else.
async fn setup_with_url() -> Option<(Queue, String)> {
    let url = std::env::var("STAMPEDED_TEST_DATABASE_URL").ok()?;
    let q = setup().await?;
    q.create_queue("default").await.expect("create queue");
    Some((q, url))
}

macro_rules! q {
    () => {
        match setup().await {
            Some(q) => q,
            None => {
                // Skipping is a courtesy to someone without a database. It
                // must never be a courtesy to CI: with the variable unset,
                // every test here returns early and `cargo test` reports
                // success, so a renamed variable or a Postgres service
                // container that never came up turns the entire integration
                // suite into a no-op WITHOUT turning anything red. A green
                // build that tested nothing is worse than a red one, because
                // it is believed.
                assert!(
                    std::env::var_os("CI").is_none(),
                    "STAMPEDED_TEST_DATABASE_URL is not set and CI is. Refusing \
                     to skip the integration tests and report success. Check \
                     the Postgres service container."
                );
                eprintln!("skipping: STAMPEDED_TEST_DATABASE_URL not set");
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
    sqlx::query("UPDATE stampeded_workers SET last_heartbeat = now() - interval '1 hour'")
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
        sqlx::query_as("SELECT failure_kind::text, interruptions FROM stampeded_jobs")
            .fetch_one(q.pool())
            .await
            .unwrap();
    assert_eq!(kind.as_deref(), Some("worker_missing"));
    assert_eq!(
        interruptions, 1,
        "a reclaim charges the interruptions budget"
    );

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
    sqlx::query("UPDATE stampeded_jobs SET claimed_by = $1")
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

    sqlx::query("UPDATE stampeded_jobs SET finished_at = now() - interval '30 days' WHERE finished_at IS NOT NULL")
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
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM stampeded_jobs")
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
        sqlx::query_scalar("SELECT reloptions FROM pg_class WHERE relname = 'stampeded_jobs_mail'")
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

    let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM stampeded_jobs WHERE task='report'")
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
    sqlx::query("UPDATE stampeded_schedules SET next_run_at = now() - interval '3 days'")
        .execute(q.pool())
        .await
        .unwrap();

    assert_eq!(
        q.run_due_schedules(10).await.unwrap(),
        1,
        "should fire exactly once"
    );

    let future: bool =
        sqlx::query_scalar("SELECT next_run_at > now() FROM stampeded_schedules WHERE key='behind'")
            .fetch_one(q.pool())
            .await
            .unwrap();
    assert!(future, "schedule should have caught up to the present");
}

#[tokio::test]
async fn concurrent_schedulers_do_not_double_fire() {
    // No leader election: exclusion is the row lock inside
    // stampeded_claim_due_schedules. Two nodes ticking at once must still produce
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

    sqlx::query("UPDATE stampeded_schedules SET next_run_at = now() - interval '1 minute'")
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
        "SELECT cron_expr, interval_secs FROM stampeded_schedules WHERE key='ticker'",
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
        sqlx::query_as("SELECT cron_expr, interval_secs FROM stampeded_schedules WHERE key='flip'")
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
        "UPDATE stampeded_schedules SET paused = true, next_run_at = now() - interval '1 hour'",
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
    sqlx::query("UPDATE stampeded_queue_counts SET n = 999 WHERE queue = 'default'")
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
        // Same reasoning as the q! macro: never silently skip under CI.
        assert!(
            std::env::var_os("CI").is_none(),
            "STAMPEDED_TEST_DATABASE_URL is not set and CI is. Refusing to skip \
             the integration tests and report success."
        );
        eprintln!("skipping: STAMPEDED_TEST_DATABASE_URL not set");
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
    sqlx::query("INSERT INTO stampeded_jobs (queue, task) VALUES ('default', 'x')")
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
        "INSERT INTO stampeded_jobs (queue, task) \
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
    assert!(rows.iter().all(|r| r.relname.starts_with("stampeded_jobs")));
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
    // The UPDATE and the stats flush must happen on the SAME backend.
    //
    // `n_dead_tup` comes from the cumulative statistics system, which each
    // backend accumulates locally and flushes on its own schedule -- ANALYZE
    // does not do it, because ANALYZE updates PLANNER statistics, a different
    // thing that happens to live nearby. So the original version raced: it
    // issued the UPDATE on one pooled connection and read the counters before
    // that backend had flushed, passing or failing on timing. Measured flaky
    // at roughly one run in three.
    //
    // pg_stat_force_next_flush (PG15+) makes it deterministic, but only for
    // the backend that calls it -- hence pinning one connection for both.
    let mut conn = q.pool().acquire().await.unwrap();
    sqlx::query("UPDATE stampeded_jobs SET priority = 1")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(&mut *conn)
        .await
        .unwrap();
    drop(conn);

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
        sqlx::query_scalar("SELECT dedup_key FROM stampeded_jobs WHERE queue='default' AND id=$1")
            .bind(id)
            .fetch_one(q.pool())
            .await
            .unwrap();

    // Snooze it a day: if the bucket followed scheduled_at, the key would move.
    sqlx::query("UPDATE stampeded_jobs SET scheduled_at = now() + interval '1 day' WHERE id = $1")
        .bind(id)
        .execute(q.pool())
        .await
        .unwrap();

    let key_after: Vec<u8> =
        sqlx::query_scalar("SELECT dedup_key FROM stampeded_jobs WHERE queue='default' AND id=$1")
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
    // `stampeded_prune` is declared RETURNS integer over a `DELETE ... RETURNING
    // 1`, and a SQL function returns the first column of the FIRST ROW -- so it
    // returned 1 whether it deleted one row or ten thousand, and NULL for none.
    // The pruner loops "until the count is less than the batch size", and with
    // a constant 1 that condition is always true: a backlog was pruned one
    // batch per TICK instead of per iteration, and the loop that exists
    // specifically to drain it never ran twice.
    //
    // Nothing failed. The pruner just quietly did a fraction of its job.
    let q = q!();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();

    for _ in 0..7 {
        let id = q
            .enqueue(NewJob::new("default", "x"))
            .await
            .unwrap()
            .unwrap();
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
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();

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
    assert_eq!(
        total, 5,
        "looping until a short batch must drain everything"
    );
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
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();

    let id = q
        .enqueue(NewJob::new("default", "calls_dead_api"))
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE stampeded_jobs SET max_deferrals = 3 WHERE id = $1")
        .bind(id)
        .execute(q.pool())
        .await
        .unwrap();

    let mut cycles = 0;
    for _ in 0..20 {
        let claimed = q.claim("default", w, 1).await.unwrap();
        let Some(job) = claimed.into_iter().next() else {
            break;
        };
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
    let id = q
        .enqueue(NewJob::new("default", "x"))
        .await
        .unwrap()
        .unwrap();
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
    let id = q
        .enqueue(NewJob::new("default", "slow"))
        .await
        .unwrap()
        .unwrap();

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
    assert_eq!(
        job.state,
        JobState::Completed,
        "the in-flight job must be allowed to finish"
    );
    assert_eq!(
        job.interruptions, 0,
        "a graceful shutdown is not a worker death"
    );
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
    let workers: i64 = sqlx::query_scalar("SELECT count(*) FROM stampeded_workers")
        .fetch_one(q.pool())
        .await
        .unwrap();
    assert_eq!(workers, 0, "a bounded shutdown must still deregister");
}

// ---------------------------------------------------------------- plans/17

#[tokio::test]
async fn a_job_resumes_from_its_checkpoint_after_its_worker_dies() {
    // A job that died at 50,000 of 100,000 does not start again at row 1.
    // River is the prior art here and its model is richer -- named steps,
    // fast-forwarded on resume (plans/17 §2). This is the single-cursor form.
    let q = q!();
    q.enqueue(NewJob::new("default", "import"))
        .await
        .unwrap()
        .unwrap();

    let w1 = q
        .register_worker("host-a", 1, &["default".into()])
        .await
        .unwrap();
    let first = q.claim("default", w1, 1).await.unwrap();
    assert!(
        first[0].checkpoint.is_none(),
        "nothing to resume from on the first attempt"
    );

    q.checkpoint(&first[0], w1, &serde_json::json!({ "cursor": 50_000 }))
        .await
        .unwrap()
        .expect("we own it");

    // Host wedges; the reaper reclaims.
    sqlx::query("UPDATE stampeded_workers SET last_heartbeat = now() - interval '1 hour'")
        .execute(q.pool())
        .await
        .unwrap();
    assert_eq!(q.reap(Duration::from_secs(300)).await.unwrap(), 1);

    let w2 = q
        .register_worker("host-b", 2, &["default".into()])
        .await
        .unwrap();
    let second = q.claim("default", w2, 1).await.unwrap();

    let state: serde_json::Value = second[0].resume_from().expect("resume state survived");
    assert_eq!(state["cursor"], 50_000, "must resume where it stopped");
    assert_eq!(second[0].attempt, 2);
}

#[tokio::test]
async fn a_stale_worker_cannot_move_the_cursor_backwards() {
    // The sequence that makes the ownership check matter: A checkpoints at 50k
    // and wedges, the reaper reclaims, B resumes and reaches 60k, and THEN A
    // wakes up and writes 51k. Without the check, B's next attempt redoes 9,000
    // units -- or does them twice, if the job is not perfectly idempotent.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "import"))
        .await
        .unwrap()
        .unwrap();

    let a = q
        .register_worker("a", 1, &["default".into()])
        .await
        .unwrap();
    let job_a = q.claim("default", a, 1).await.unwrap().remove(0);
    q.checkpoint(&job_a, a, &serde_json::json!({ "cursor": 50_000 }))
        .await
        .unwrap();

    sqlx::query(
        "UPDATE stampeded_workers SET last_heartbeat = now() - interval '1 hour' WHERE id = $1",
    )
    .bind(a)
    .execute(q.pool())
    .await
    .unwrap();
    q.reap(Duration::from_secs(300)).await.unwrap();

    let b = q
        .register_worker("b", 2, &["default".into()])
        .await
        .unwrap();
    let job_b = q.claim("default", b, 1).await.unwrap().remove(0);
    q.checkpoint(&job_b, b, &serde_json::json!({ "cursor": 60_000 }))
        .await
        .unwrap();

    // A wakes up, still holding its stale handle.
    let stale = q
        .checkpoint(&job_a, a, &serde_json::json!({ "cursor": 51_000 }))
        .await
        .unwrap();
    assert!(
        stale.is_none(),
        "a stale worker must be refused, and must learn it was"
    );

    let now = q.fetch("default", id).await.unwrap().unwrap();
    assert_eq!(
        now.checkpoint.unwrap()["cursor"],
        60_000,
        "the cursor must not go backwards"
    );
}

#[tokio::test]
async fn progress_forgives_infrastructure_but_not_a_broken_job() {
    // The decision in plans/17 §4. `interruptions` exists to stop a job that
    // KILLS WORKERS from walking the fleet; a job that checkpointed and then
    // died is demonstrably not that job. So progress resets interruptions --
    // otherwise a long job on a flaky afternoon is discarded having done 95% of
    // the work and never actually failed.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "import"))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();

    let job = q.claim("default", w, 1).await.unwrap().remove(0);
    q.fail_with_exit(
        &job,
        w,
        FailureKind::WorkerExited,
        "oom",
        Some(Duration::from_millis(1)),
        Some(137),
    )
    .await
    .unwrap();
    assert_eq!(
        q.fetch("default", id).await.unwrap().unwrap().interruptions,
        1
    );

    tokio::time::sleep(Duration::from_millis(20)).await;
    let job = q.claim("default", w, 1).await.unwrap().remove(0);
    q.checkpoint(&job, w, &serde_json::json!({ "cursor": 1 }))
        .await
        .unwrap();

    let after = q.fetch("default", id).await.unwrap().unwrap();
    assert_eq!(
        after.interruptions, 0,
        "demonstrated progress is not a poison job"
    );
    // But the total-work ceiling is untouched, so a job that progresses one
    // unit and then dies forever is still bounded.
    assert_eq!(after.attempt, 2, "attempt must NOT be refunded");
}

#[tokio::test]
async fn a_checkpoint_is_also_a_heartbeat() {
    // A job that just saved progress is demonstrably alive. Requiring a
    // separate touch would be asking the caller to say the same thing twice,
    // and forgetting it would let a job that is plainly working get expired.
    let q = q!();
    q.enqueue(NewJob::new("default", "slow").expires_in(Duration::from_secs(1)))
        .await
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    let job = q.claim("default", w, 1).await.unwrap().remove(0);

    for i in 0..3 {
        tokio::time::sleep(Duration::from_millis(400)).await;
        q.checkpoint(&job, w, &serde_json::json!({ "cursor": i }))
            .await
            .unwrap();
        assert_eq!(
            q.expire(10).await.unwrap(),
            0,
            "a checkpointing job must not expire"
        );
    }
}

#[tokio::test]
async fn the_checkpoint_is_cleared_on_success() {
    // Dead weight otherwise, on the one table that already has the bloat
    // problem -- and inconsistent with not storing results either.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "import"))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    let job = q.claim("default", w, 1).await.unwrap().remove(0);

    q.checkpoint(&job, w, &serde_json::json!({ "cursor": 99 }))
        .await
        .unwrap();
    q.complete(&job, w).await.unwrap();

    assert!(
        q.fetch("default", id)
            .await
            .unwrap()
            .unwrap()
            .checkpoint
            .is_none()
    );
}

#[tokio::test]
async fn an_oversized_checkpoint_is_refused_by_the_database() {
    // Bounded in SQL, not in the client: a Python worker will not run our
    // truncation. Someone will try to store the processed ids rather than a
    // cursor, and the row will be megabytes.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "import"))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    let job = q.claim("default", w, 1).await.unwrap().remove(0);

    let huge = serde_json::json!({ "ids": "x".repeat(70_000) });
    assert!(
        q.checkpoint(&job, w, &huge).await.is_err(),
        "64 KB cap must hold"
    );

    // And the job is untouched.
    assert!(
        q.fetch("default", id)
            .await
            .unwrap()
            .unwrap()
            .checkpoint
            .is_none()
    );
}

#[tokio::test]
async fn a_checkpoint_in_a_transaction_rolls_back_with_its_work() {
    // The hazard, closed. As two separate writes, a crash between the work and
    // the cursor replays that chunk on resume -- silent duplication for a job
    // that is not idempotent. In one transaction the cursor can never claim
    // work that was rolled back.
    //
    // River got here first with ResumableSetStepCursorTx; this exists because
    // reading their implementation showed the plain version could not be made
    // atomic at all.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "import"))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    let job = q.claim("default", w, 1).await.unwrap().remove(0);

    let mut tx = q.pool().begin().await.unwrap();
    q.checkpoint_on(&mut *tx, &job, w, &serde_json::json!({ "cursor": 500 }))
        .await
        .unwrap()
        .expect("we own it");
    tx.rollback().await.unwrap();

    let after = q.fetch("default", id).await.unwrap().unwrap();
    assert!(
        after.checkpoint.is_none(),
        "a rolled-back chunk must not leave a cursor claiming it was done"
    );
    assert_eq!(after.checkpoint_seq, 0);

    // And the committed path does persist.
    let mut tx = q.pool().begin().await.unwrap();
    q.checkpoint_on(&mut *tx, &job, w, &serde_json::json!({ "cursor": 500 }))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let after = q.fetch("default", id).await.unwrap().unwrap();
    assert_eq!(after.checkpoint.unwrap()["cursor"], 500);
}

#[tokio::test]
async fn steps_run_once_and_skip_on_resume() {
    // The Rust half of what the Ruby worker does, so the two first-party
    // workers have the same resume ergonomics rather than one having a step
    // model and the other a bare cursor.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "checkout"))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();

    let charges = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let renders = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // Attempt one: charge and render succeed, then something kills us.
    {
        let job = q.claim("default", w, 1).await.unwrap().remove(0);
        let ctx = q.context_for(&job, w);

        let c = charges.clone();
        ctx.step("charge", async move {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();

        let r = renders.clone();
        let path: String = ctx
            .step("render", async move {
                r.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok("/tmp/receipt.pdf".to_string())
            })
            .await
            .unwrap();
        assert_eq!(path, "/tmp/receipt.pdf");

        q.fail_with_exit(
            &job,
            w,
            FailureKind::WorkerExited,
            "killed",
            Some(Duration::from_millis(1)),
            Some(137),
        )
        .await
        .unwrap();
    }

    // Attempt two: both steps are skipped, and the render still yields its value.
    tokio::time::sleep(Duration::from_millis(30)).await;
    {
        let job = q.claim("default", w, 1).await.unwrap().remove(0);
        let ctx = q.context_for(&job, w);

        let c = charges.clone();
        ctx.step("charge", async move {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();

        let r = renders.clone();
        let path: String = ctx
            .step("render", async move {
                r.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok("SHOULD NOT RUN".to_string())
            })
            .await
            .unwrap();

        assert_eq!(
            path, "/tmp/receipt.pdf",
            "a skipped step must return what it produced"
        );
        q.complete(&job, w).await.unwrap();
    }

    assert_eq!(
        charges.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the card must be charged once"
    );
    assert_eq!(renders.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(
        q.fetch("default", id)
            .await
            .unwrap()
            .unwrap()
            .checkpoint
            .is_none()
    );
}

#[tokio::test]
async fn a_job_checkpointed_by_ruby_resumes_in_rust() {
    // THE POLYGLOT CLAIM, at its most literal. plans/16 says the Ruby worker is
    // "not a client of the Rust daemon, it is a peer" -- and peers have to be
    // able to pick up each other's work.
    //
    // This fixture is not invented: it was captured from Stampeded::Steps by
    // running the Ruby worker against a live database on 2026-09-18, which
    // emitted
    //
    //   {"v": 1, "values": {"render": "/tmp/receipt.pdf"},
    //    "cursors": {"email": 2}, "completed": ["charge", "render"]}
    //
    // The Ruby side has the mirror of this test in steps_test.rb.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "checkout"))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("ruby-worker", 1, &["default".into()])
        .await
        .unwrap();
    let job = q.claim("default", w, 1).await.unwrap().remove(0);

    let written_by_ruby = serde_json::json!({
        "v": 1,
        "completed": ["charge", "render"],
        "values": { "render": "/tmp/receipt.pdf" },
        "cursors": { "email": 2 }
    });
    q.checkpoint(&job, w, &written_by_ruby)
        .await
        .unwrap()
        .unwrap();

    // A Rust worker picks it up.
    let resumed = q.fetch("default", id).await.unwrap().unwrap();
    let ctx = q.context_for(&resumed, w);
    let steps = ctx.steps();

    assert!(
        steps.completed("charge"),
        "Rust must see Ruby's completed steps"
    );
    assert_eq!(steps.value::<String>("render").unwrap(), "/tmp/receipt.pdf");
    assert_eq!(
        steps.cursor::<i64>("email").unwrap(),
        2,
        "and the in-progress cursor"
    );

    // And running the same steps skips them rather than charging twice.
    let mut ran = false;
    ctx.step("charge", async {
        ran = true;
        Ok(())
    })
    .await
    .unwrap();
    assert!(!ran, "a step Ruby completed must not run again in Rust");
}

#[tokio::test]
async fn the_cron_occurrence_ledger_is_pruned() {
    // It was not. `stampeded_prune_occurrences` existed in the schema and was
    // called from nowhere, so the ledger grew one row per firing forever --
    // 525,600 a year for a schedule running every minute. plans/11 flagged it
    // as "narrow, but unbounded"; nothing acted on that.
    let q = q!();

    sqlx::query(
        "INSERT INTO stampeded_schedules (key, queue, task, interval_secs) \
         VALUES ('tick', 'default', 't', 60)",
    )
    .execute(q.pool())
    .await
    .unwrap();

    // A year of firings, aged.
    sqlx::query(
        "INSERT INTO stampeded_cron_occurrences (cron_key, occurrence_token, enqueued_at) \
         SELECT 'tick', g::text, now() - interval '60 days' FROM generate_series(1, 50) g",
    )
    .execute(q.pool())
    .await
    .unwrap();
    // And a few recent ones, which must survive.
    sqlx::query(
        "INSERT INTO stampeded_cron_occurrences (cron_key, occurrence_token) \
         SELECT 'tick', 'recent-' || g FROM generate_series(1, 3) g",
    )
    .execute(q.pool())
    .await
    .unwrap();

    let pruned = q
        .prune_occurrences(Duration::from_secs(30 * 24 * 3600))
        .await
        .unwrap();
    assert_eq!(pruned, 50, "old occurrences should go");

    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM stampeded_cron_occurrences")
        .fetch_one(q.pool())
        .await
        .unwrap();
    assert_eq!(
        left, 3,
        "recent ones are the idempotency backstop and must stay"
    );
}

#[tokio::test]
async fn the_notification_queue_and_clock_skew_are_observable() {
    // plans/12 lists both as untested and notes that NONE of the surveyed
    // implementations export the notification queue usage.
    //
    // The failure it detects is specific: a backend LISTENing from inside a
    // long transaction pins the 8 GB async queue, and once it fills every
    // NOTIFY errors -- which means every INSERT into stampeded_jobs errors,
    // because the notify trigger runs inside it. Enqueue stops working and the
    // cause is nowhere near the symptom.
    let q = q!();

    let usage = q.notification_queue_usage().await.unwrap();
    assert!((0.0..=1.0).contains(&usage), "usage is a fraction: {usage}");

    // Skew against the same machine's database should be tiny. The assertion is
    // loose on purpose -- this is a diagnostic, and a slow CI box should not
    // fail the suite.
    let skew = q.clock_skew().await.unwrap();
    assert!(
        skew.abs() < 60.0,
        "skew against a local database was {skew}s"
    );
}

#[tokio::test]
async fn the_job_listing_is_bounded_by_a_statement_timeout() {
    // The console re-runs every panel once a second for as long as a tab is
    // open, so a slow query is not a slow page -- it is permanent load.
    //
    // Measured: on a 200k-row partition holding 20 discarded jobs, asking for
    // the discarded ones is a parallel seq scan reading 4,634 buffers. No index
    // was added, because one on (queue, state, id) would make every insert and
    // transition pay forever for a dashboard usually nobody is watching. The
    // damage is bounded instead.
    let q = q!();

    // The normal case must stay fast and correct.
    for _ in 0..5 {
        q.enqueue(NewJob::new("default", "x")).await.unwrap();
    }
    let jobs = q
        .recent_jobs("default", Some("available"), 40)
        .await
        .unwrap();
    assert_eq!(jobs.len(), 5);

    // Newest first, so uuidv7 ordering is actually being relied on.
    let ids: Vec<_> = jobs.iter().map(|j| j.id).collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    sorted.reverse();
    assert_eq!(ids, sorted, "the listing must be newest-first");

    // No filter returns everything up to the limit.
    assert_eq!(q.recent_jobs("default", None, 40).await.unwrap().len(), 5);

    // And the timeout is short enough to protect a once-a-second panel.
    assert!(
        stampeded::queue::DASHBOARD_QUERY_TIMEOUT <= Duration::from_secs(5),
        "a dashboard query ceiling longer than a few seconds defeats the point"
    );
}

#[tokio::test]
async fn the_counters_stay_accurate_under_concurrent_transitions() {
    // The counters are load-bearing: depth, the dashboard and every metric read
    // them instead of aggregating, precisely so nothing runs a count(*) over
    // the jobs table. If the trigger drifts under concurrency, every one of
    // those lies -- and lies quietly, which is worse than being slow.
    let q = q!();

    // Enqueue from several connections at once.
    let mut writers = Vec::new();
    for _ in 0..8 {
        let q = q.clone();
        writers.push(tokio::spawn(async move {
            for _ in 0..25 {
                q.enqueue(NewJob::new("default", "churn")).await.unwrap();
            }
        }));
    }
    for w in writers {
        w.await.unwrap();
    }

    let available = depth_of(&q, "available").await;
    assert_eq!(available, 200, "200 concurrent inserts must count as 200");

    // Now move them through the lifecycle from several workers at once.
    let mut workers = Vec::new();
    for i in 0..4 {
        let q = q.clone();
        workers.push(tokio::spawn(async move {
            let w = q
                .register_worker(&format!("h{i}"), i, &["default".into()])
                .await
                .unwrap();
            let mut done = 0;
            loop {
                let jobs = q.claim("default", w, 5).await.unwrap();
                if jobs.is_empty() {
                    break;
                }
                for job in jobs {
                    q.complete(&job, w).await.unwrap();
                    done += 1;
                }
            }
            done
        }));
    }
    let mut completed = 0;
    for w in workers {
        completed += w.await.unwrap();
    }
    assert_eq!(
        completed, 200,
        "every job should have been claimed exactly once"
    );

    // And the counters agree, without anyone aggregating.
    assert_eq!(depth_of(&q, "available").await, 0);
    assert_eq!(depth_of(&q, "executing").await, 0);
    assert_eq!(depth_of(&q, "completed").await, 200);

    // Cross-check against the truth, which is the ONE place an aggregate is
    // allowed: if these disagree, the trigger has drifted.
    let actual: i64 =
        sqlx::query_scalar("SELECT count(*) FROM stampeded_jobs WHERE state = 'completed'")
            .fetch_one(q.pool())
            .await
            .unwrap();
    assert_eq!(actual, 200, "the counter and the table must agree");
}

#[tokio::test]
async fn counters_survive_a_rolled_back_enqueue() {
    // The trigger fires inside the transaction, so a rollback must take the
    // count with it. Otherwise every rolled-back enqueue inflates depth
    // forever and the dashboard slowly drifts away from reality.
    let q = q!();
    q.enqueue(NewJob::new("default", "kept")).await.unwrap();

    let mut tx = q.pool().begin().await.unwrap();
    sqlx::query("INSERT INTO stampeded_jobs (queue, task) VALUES ('default', 'rolled-back')")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    assert_eq!(
        depth_of(&q, "available").await,
        1,
        "the rollback must uncount"
    );
}

#[tokio::test]
async fn a_thundering_herd_of_identical_jobs_enqueues_exactly_one() {
    // The realistic shape: a cache expires and forty web requests all decide to
    // enqueue the same refresh job in the same instant. Uniqueness has to hold
    // under genuine concurrency, not just when called twice in sequence --
    // which is the whole reason it is a UNIQUE INDEX rather than a SELECT
    // followed by an INSERT (plans/10). Oban's advisory-lock model cannot be
    // used here at all, because a polyglot enqueuer does not take the lock.
    let q = q!();

    let mut racers = Vec::new();
    for _ in 0..40 {
        let q = q.clone();
        racers.push(tokio::spawn(async move {
            q.enqueue(NewJob::new("default", "refresh").unique("cache:products"))
                .await
                .unwrap()
        }));
    }

    let mut created = 0;
    for r in racers {
        if r.await.unwrap().is_some() {
            created += 1;
        }
    }

    assert_eq!(created, 1, "exactly one enqueue may win");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM stampeded_jobs")
        .fetch_one(q.pool())
        .await
        .unwrap();
    assert_eq!(rows, 1, "and exactly one row may exist");
}

#[tokio::test]
async fn the_reaper_racing_a_finishing_worker_does_not_double_run_the_job() {
    // The race that makes lease-based queues run jobs twice: the reaper decides
    // a worker is dead at the same moment that worker finishes. Heartbeats make
    // this rare rather than routine (plans/04), but rare is not never, and the
    // outcome must still be exactly one execution.
    //
    // `stampeded_complete` carries `claimed_by = p_worker_id`, so whichever
    // side loses learns it lost instead of writing over the winner.
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "once"))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    let job = q.claim("default", w, 1).await.unwrap().remove(0);

    // Make the worker look dead, then have both act at once.
    sqlx::query("UPDATE stampeded_workers SET last_heartbeat = now() - interval '1 hour'")
        .execute(q.pool())
        .await
        .unwrap();

    let (reap, complete) = tokio::join!(
        {
            let q = q.clone();
            async move { q.reap(Duration::from_secs(300)).await.unwrap() }
        },
        {
            let q = q.clone();
            let job = job.clone();
            async move { q.complete(&job, w).await.unwrap() }
        },
    );

    let final_job = q.fetch("default", id).await.unwrap().unwrap();

    // Exactly one side won, and the loser knows.
    if complete.owned {
        assert_eq!(final_job.state, JobState::Completed, "the worker won");
    } else {
        assert_eq!(reap, 1, "then the reaper must have won");
        assert_eq!(final_job.state, JobState::Retryable);
    }

    // What must never happen: the job reported completed AND queued for retry.
    assert!(
        !(final_job.state == JobState::Completed && final_job.claimed_by.is_some()),
        "a completed job must not still be claimed"
    );
}

#[tokio::test]
async fn concurrent_schedulers_fire_an_occurrence_exactly_once() {
    // Already covered for interval schedules; this is the cron path, where the
    // occurrence ledger is the backstop behind the row lock. Two daemons
    // ticking at the same instant is the normal case, not an edge one -- there
    // is no leader election by design.
    let q = q!();
    q.upsert_cron_schedule("tick", "default", "report", "* * * * *", "UTC", None)
        .await
        .unwrap();
    sqlx::query("UPDATE stampeded_schedules SET next_run_at = now() - interval '1 minute'")
        .execute(q.pool())
        .await
        .unwrap();

    let mut ticks = Vec::new();
    for _ in 0..6 {
        let q = q.clone();
        ticks.push(tokio::spawn(async move {
            q.run_due_schedules(10).await.unwrap()
        }));
    }

    let mut fired = 0;
    for t in ticks {
        fired += t.await.unwrap();
    }

    assert_eq!(fired, 1, "six daemons, one firing");
    let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM stampeded_jobs WHERE task = 'report'")
        .fetch_one(q.pool())
        .await
        .unwrap();
    assert_eq!(jobs, 1);
}

#[tokio::test]
async fn a_job_that_could_never_run_is_refused() {
    // THE BLACK HOLE, found by fuzzing the enqueue endpoint. The claim
    // predicate is `attempt < max_attempts`, so max_attempts = 0 makes a job
    // permanently unclaimable: it sits in `available` forever, counts toward
    // depth, and nothing ever runs it or reports why. The API accepted it
    // happily.
    let q = q!();

    let err = q
        .enqueue(NewJob::new("default", "doomed").max_attempts(0))
        .await;
    assert!(
        err.is_err(),
        "a job that can never be claimed must not be creatable"
    );

    // One is fine: it gets exactly one shot.
    let id = q
        .enqueue(NewJob::new("default", "one_shot").max_attempts(1))
        .await
        .unwrap()
        .unwrap();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    assert_eq!(q.claim("default", w, 5).await.unwrap().len(), 1);
    let _ = id;
}

#[tokio::test]
async fn an_absurdly_large_payload_is_refused() {
    // Postgres TOASTs a huge value out of line so the row stays small -- but
    // `stampeded_claim` returns `stampeded_jobs.*`, which detoasts it on EVERY
    // CLAIM. Someone storing a file here would move megabytes through the
    // hot path this entire schema is shaped around (plans/13).
    let q = q!();

    let huge = serde_json::json!({ "blob": "x".repeat(2_000_000) });
    assert!(
        q.enqueue(NewJob::new("default", "big").args(huge))
            .await
            .is_err(),
        "a 2 MB payload must be refused"
    );

    // The bound is generous: anything a sane caller sends still works.
    let normal = serde_json::json!({
        "user_id": 42,
        "note": "x".repeat(10_000),
    });
    assert!(
        q.enqueue(NewJob::new("default", "ok").args(normal))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn a_schedule_with_a_nonsense_interval_is_refused_not_clamped() {
    // The first version did `.max(1)` to satisfy the CHECK, so asking for
    // `every_secs: 0` silently produced a schedule firing EVERY SECOND -- an
    // extreme value nobody asked for, from input that was plainly a mistake.
    // Found by fuzzing the MCP tool, which then reported "set to every 0s"
    // while storing 1.
    //
    // Silently coercing invalid input into an aggressive default is worse than
    // rejecting it: the caller believes one thing and the system does another.
    let q = q!();

    let err = q
        .upsert_schedule("tick", "default", "t", Duration::from_secs(0))
        .await;
    assert!(err.is_err(), "a zero interval must be refused");

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM stampeded_schedules")
        .fetch_one(q.pool())
        .await
        .unwrap();
    assert_eq!(count, 0, "and nothing may be created");

    // One second is aggressive but legitimate, and still works.
    assert!(
        q.upsert_schedule("tick", "default", "t", Duration::from_secs(1))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn a_schedule_needs_a_key() {
    // An empty key is a valid primary key and a useless one: it is how you
    // pause, remove or find the schedule again.
    let q = q!();

    assert!(
        q.upsert_schedule("", "default", "t", Duration::from_secs(60))
            .await
            .is_err()
    );
    assert!(
        q.upsert_schedule("   ", "default", "t", Duration::from_secs(60))
            .await
            .is_err()
    );
    assert!(
        q.upsert_cron_schedule("", "default", "t", "* * * * *", "UTC", None)
            .await
            .is_err(),
        "the cron path needs the same guard"
    );
}

/// A handler choosing its own retry delay, the Rust half of `RetryIn`.
///
/// Parity with the Ruby worker matters here beyond tidiness: a job moved from
/// one worker to the other must retry on the same schedule, or "the workers
/// are peers" is false in a way users would feel.
///
/// Asserts the delay reached the DATABASE. A delay computed and then dropped
/// looks identical from the handler's side.
#[tokio::test]
async fn a_handler_can_choose_its_own_retry_delay() {
    let q = q!();
    let w = q
        .register_worker("test", 1, &["default".into()])
        .await
        .unwrap();

    let id = q
        .enqueue(NewJob::new("default", "ratelimited").max_attempts(5))
        .await
        .unwrap();
    let jobs = q.claim("default", w, 1).await.unwrap();
    let job = jobs.first().expect("claimed");

    q.fail(
        job,
        w,
        stampeded::FailureKind::Raised,
        "rate limited",
        Some(std::time::Duration::from_secs(3600)),
    )
    .await
    .unwrap();

    let row = q
        .fetch("default", id.expect("enqueued"))
        .await
        .unwrap()
        .expect("job");
    let delay = row.scheduled_at - chrono::Utc::now();

    assert_eq!(row.state.as_str(), "retryable");
    assert_eq!(
        row.attempt, 1,
        "a chosen delay must still charge the attempt"
    );
    assert!(
        delay.num_seconds() > 3000,
        "the chosen delay did not reach the database: {delay:?}"
    );
}

/// The downcast that makes `RetryIn` work must actually fire.
#[test]
fn retry_in_survives_being_boxed_as_a_handler_error() {
    let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(stampeded::RetryIn::secs(42));
    let err = stampeded::Error::Handler(boxed);

    let stampeded::Error::Handler(src) = &err else {
        panic!("wrong variant")
    };
    let found = src
        .downcast_ref::<stampeded::RetryIn>()
        .expect("RetryIn must be recoverable after boxing, or the worker cannot see it");

    assert_eq!(found.0, std::time::Duration::from_secs(42));
}

/// The claim must never plan a sort.
///
/// `stampeded_claim`'s ORDER BY and `stampeded_jobs_claim_idx` have to agree
/// exactly. If they drift -- someone re-adds `id` as a tiebreaker to one but
/// not the other -- Postgres silently inserts a Sort node over the matching
/// rows. Nothing errors, no test fails, the queue stays correct; claiming just
/// gets slower under exactly the load where it matters, and the LIMIT no longer
/// bounds the work because the sort must see every candidate row first.
///
/// Measured when the two disagreed: 0.26ms and 244 buffers, against 0.13ms and
/// 212 when they agree.
#[tokio::test]
async fn the_claim_never_plans_a_sort() {
    let q = q!();

    for i in 0..200 {
        q.enqueue(NewJob::new("default", "t").priority(i % 3))
            .await
            .unwrap();
    }
    sqlx::query("ANALYZE stampeded_jobs_default")
        .execute(q.pool())
        .await
        .ok();

    // The claim's own predicate and ordering, verbatim from schema.sql.
    let plan: (serde_json::Value,) = sqlx::query_as(
        "EXPLAIN (FORMAT JSON) \
         SELECT id FROM stampeded_jobs \
          WHERE queue = 'default' AND state IN ('available','retryable') \
            AND scheduled_at <= now() \
          ORDER BY priority, scheduled_at \
          LIMIT 10 FOR UPDATE SKIP LOCKED",
    )
    .fetch_one(q.pool())
    .await
    .unwrap();

    let rendered = plan.0.to_string();
    assert!(
        !rendered.contains("\"Sort"),
        "the claim planned a sort, so the ORDER BY and stampeded_jobs_claim_idx have \
         drifted apart:\n{rendered}"
    );
}

/// A checkpoint that accumulates instead of advancing must be warned about,
/// and one that is merely large must not be.
///
/// The 64 KB CHECK is a hard stop that fires only when the work is already
/// lost. The misuse it exists for is gradual -- storing the processed ids
/// rather than "I have done N" -- and that has a detectable SHAPE: a cursor
/// advances and stays about the same size, a result set grows on every write.
///
/// The large-but-stable case is the one that earns this test. The first
/// implementation compared `pg_column_size` of the parameter against
/// `pg_column_size` of the stored column -- uncompressed against
/// TOAST-compressed -- so a stable 20 KB checkpoint of repetitive data looked
/// like it grew seventy-fold on every write and warned forever about a job
/// doing nothing wrong.
#[tokio::test]
async fn a_growing_checkpoint_warns_but_a_merely_large_one_does_not() {
    let q = q!();
    let w = q
        .register_worker("test", 1, &["default".into()])
        .await
        .unwrap();
    q.enqueue(NewJob::new("default", "long"))
        .await
        .unwrap()
        .expect("enqueued");
    let jobs = q.claim("default", w, 1).await.unwrap();
    let job = jobs.first().expect("claimed");

    // Helper: the warning is a NOTICE-class message, so observe the effect we
    // can assert on from SQL -- whether the function judged this write to be
    // "large and growing" -- by reproducing its own predicate.
    async fn judge(q: &stampeded::Queue, prev: &str, next: &str) -> bool {
        let (grew,): (bool,) = sqlx::query_as(
            "SELECT length($2::jsonb::text) > 8192 \
                AND length($1::jsonb::text) IS NOT NULL \
                AND length($2::jsonb::text) > length($1::jsonb::text)",
        )
        .bind(prev)
        .bind(next)
        .fetch_one(q.pool())
        .await
        .unwrap();
        grew
    }

    let ids = |n: usize| {
        let v: Vec<usize> = (0..n).collect();
        format!(r#"{{"v":1,"done":{}}}"#, serde_json::to_string(&v).unwrap())
    };
    let cursor = |n: usize| format!(r#"{{"v":1,"cursors":{{"rows":{n}}}}}"#);
    let big_stable = format!(r#"{{"v":1,"blob":"{}"}}"#, "x".repeat(20_000));

    // A cursor advances: never large, so never warned about.
    assert!(!judge(&q, &cursor(10_000), &cursor(20_000)).await);

    // A result set accumulates: once past the threshold it is caught.
    assert!(judge(&q, &ids(2_000), &ids(4_000)).await);

    // Large but stable is NOT misuse -- this is the regression that matters.
    assert!(!judge(&q, &big_stable, &big_stable).await);

    // And the function still works and still returns a sequence.
    let seq = q
        .checkpoint(
            job,
            w,
            &serde_json::from_str::<serde_json::Value>(&cursor(50_000)).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        seq.is_some(),
        "checkpoint must still return its sequence number"
    );
}

/// A checkpoint resets `interruptions` and leaves `attempt` alone.
///
/// This is the plans/17 §4 decision, and it is the one that lets a long job
/// survive a flaky afternoon without becoming immortal:
///
/// - `interruptions` exists to stop a job that KILLS WORKERS from walking the
///   fleet one process at a time. A job that checkpointed and then died is
///   demonstrably not that job -- it ran, it worked, something else killed it.
/// - `attempt` bounds total work regardless, and still increments at claim, so
///   a job that progresses one unit and then dies forever is still discarded
///   at `max_attempts`. The brake holds; it just stops punishing success.
///
/// Untested until now, which meant deleting `interruptions = 0` from
/// stampeded_checkpoint would have failed nothing -- and the symptom would be a
/// long job on flaky infrastructure being discarded while making steady
/// progress, which is the exact outcome this design exists to prevent.
#[tokio::test]
async fn a_checkpoint_forgives_interruptions_but_not_attempts() {
    let q = q!();
    let w = q
        .register_worker("test", 1, &["default".into()])
        .await
        .unwrap();
    let id = q
        .enqueue(NewJob::new("default", "long"))
        .await
        .unwrap()
        .expect("enqueued");

    let jobs = q.claim("default", w, 1).await.unwrap();
    let job = jobs.first().expect("claimed").clone();

    // Something killed the worker once already.
    sqlx::query("UPDATE stampeded_jobs SET interruptions = 1 WHERE id = $1")
        .bind(id)
        .execute(q.pool())
        .await
        .unwrap();

    let before: (i16, i16) =
        sqlx::query_as("SELECT interruptions, attempt FROM stampeded_jobs WHERE id = $1")
            .bind(id)
            .fetch_one(q.pool())
            .await
            .unwrap();
    assert_eq!(
        before,
        (1, 1),
        "precondition: one interruption, one attempt"
    );

    q.checkpoint(
        &job,
        w,
        &serde_json::json!({"v": 1, "cursors": {"rows": 50_000}}),
    )
    .await
    .unwrap()
    .expect("checkpoint accepted");

    let after: (i16, i16) =
        sqlx::query_as("SELECT interruptions, attempt FROM stampeded_jobs WHERE id = $1")
            .bind(id)
            .fetch_one(q.pool())
            .await
            .unwrap();

    assert_eq!(
        after.0, 0,
        "progress must forgive interruptions (plans/17 §4)"
    );
    assert_eq!(
        after.1, before.1,
        "progress must NOT refund an attempt, or a job that checkpoints once \
         and then crashes forever becomes immortal"
    );
}

/// Per-job retry backoff must be settable at enqueue, on both sides.
///
/// The row has carried `backoff_base_ms`/`backoff_max_ms` from the start and
/// NEITHER client could set them, so the documented way to tune retries was
/// unreachable. On the Ruby side that was worse than a missing feature: the
/// error raised when a job declares `retry_on` tells the user to write
/// `stampeded backoff_base_ms: 1_000, backoff_max_ms: 300_000`, and the DSL
/// rejected those very keys.
#[tokio::test]
async fn backoff_can_be_set_when_enqueuing() {
    let q = q!();

    let id = q
        .enqueue(NewJob::new("default", "t").backoff(5_000, 300_000))
        .await
        .unwrap()
        .expect("enqueued");

    let row: (i16, i32) =
        sqlx::query_as("SELECT backoff_base_ms, backoff_max_ms FROM stampeded_jobs WHERE id = $1")
            .bind(id)
            .fetch_one(q.pool())
            .await
            .unwrap();

    assert_eq!(
        row,
        (5_000, 300_000),
        "the backoff policy did not reach the row"
    );

    // base is a smallint; clamping here beats an INSERT that fails with
    // "smallint out of range" and names neither the field nor the limit.
    let id2 = q
        .enqueue(NewJob::new("default", "t2").backoff(90_000, 300_000))
        .await
        .unwrap()
        .expect("enqueued");
    let (base,): (i16,) = sqlx::query_as("SELECT backoff_base_ms FROM stampeded_jobs WHERE id = $1")
        .bind(id2)
        .fetch_one(q.pool())
        .await
        .unwrap();
    assert_eq!(base, i16::MAX, "an over-range base must clamp, not explode");
}

/// A partition created outside `stampeded_create_queue` cannot wake anybody,
/// and that must be detectable.
///
/// The wakeup trigger is `FOR EACH STATEMENT` with a transition table.
/// Postgres does NOT clone statement-level triggers to partitions -- verified
/// on 18.6, a raw `CREATE TABLE ... PARTITION OF` gets zero triggers and an
/// INSERT into it notifies nobody. (Row-level triggers ARE cloned, including
/// to partitions created later, but a row-level wakeup costs +3.5% per row
/// against +1.2% for the statement form, which is why the statement form was
/// chosen.)
///
/// So the hole is structural and cannot be closed by inheritance. What makes
/// it dangerous is that it is SILENT: jobs in that queue are still claimed
/// correctly, just on the poll interval. Nothing fails; one queue is simply
/// slow, for ever.
#[tokio::test]
async fn a_partition_without_the_wakeup_trigger_is_detected() {
    let q = q!();

    // Every partition made the supported way is covered.
    assert!(
        q.partitions_without_wakeup().await.unwrap().is_empty(),
        "stampeded_create_queue must always attach the trigger"
    );

    // The unsupported way.
    sqlx::query(
        "CREATE TABLE stampeded_jobs_silent PARTITION OF stampeded_jobs FOR VALUES IN ('silent')",
    )
    .execute(q.pool())
    .await
    .unwrap();

    let silent = q.partitions_without_wakeup().await.unwrap();

    assert_eq!(silent, vec!["stampeded_jobs_silent".to_string()]);
}

#[tokio::test]
async fn a_stale_execution_cannot_complete_a_job_its_own_worker_has_since_reclaimed() {
    let q = q!();
    let w = q
        .register_worker("test", 1, &["default".into()])
        .await
        .unwrap();

    q.enqueue(NewJob::new("default", "slow")).await.unwrap();

    // Attempt 1. The worker starts running it and then stalls -- long enough to
    // stop heartbeating, but NOT long enough to be declared dead. This is the
    // gap between stampeded_expire (job-level, worker still alive) and
    // stampeded_reap (worker-level, worker gone), and it is where the ABA lives.
    let first = q.claim("default", w, 1).await.unwrap().pop().unwrap();
    assert_eq!(first.attempt, 1);

    sqlx::query(
        "UPDATE stampeded_jobs SET claimed_at = now() - interval '2 hours',
                                   heartbeat_at = NULL WHERE id = $1",
    )
    .bind(first.id)
    .execute(q.pool())
    .await
    .unwrap();

    let expired = q.expire(100).await.unwrap();
    assert_eq!(expired, 1, "the stalled job should have expired");

    // The very same worker -- still alive, still registered, same uuid -- picks
    // the job back up. Nothing prevents this; it is the common case when one
    // worker serves the queue.
    let second = q.claim("default", w, 1).await.unwrap().pop().unwrap();
    assert_eq!(second.id, first.id);
    assert_eq!(second.attempt, 2, "the retry is a new attempt");

    // Now attempt 1 finally returns and reports success. It is a ghost: its work
    // is two hours stale and attempt 2 is running RIGHT NOW. Ownership is fenced
    // on claimed_by alone, and claimed_by is this same worker, so the fence does
    // not see a difference between the two executions.
    let t = q.complete(&first, w).await.unwrap();

    let state: String = sqlx::query_scalar("SELECT state FROM stampeded_jobs WHERE id = $1")
        .bind(first.id)
        .fetch_one(q.pool())
        .await
        .unwrap();

    assert!(
        !t.owned && state == "executing",
        "a stale execution completed a job that had been reclaimed: owned={}, \
         state={state:?}. Attempt 2 is still running, and its result will be \
         thrown away as 'not owned' when it finishes -- the outcome recorded is \
         attempt 1's.",
        t.owned
    );
}

#[tokio::test]
async fn truncating_the_jobs_table_does_not_leave_the_counters_lying() {
    let q = q!();
    q.create_queue("trunc_a").await.unwrap();

    for _ in 0..7 {
        q.enqueue(NewJob::new("trunc_a", "t")).await.unwrap();
    }
    for _ in 0..3 {
        q.enqueue(NewJob::new("default", "t")).await.unwrap();
    }

    let counted = |q: Queue, queue: &'static str| async move {
        sqlx::query_scalar::<_, i64>(
            "SELECT coalesce(sum(n),0)::bigint FROM stampeded_queue_counts WHERE queue = $1",
        )
        .bind(queue)
        .fetch_one(q.pool())
        .await
        .unwrap()
    };

    assert_eq!(counted(q.clone(), "trunc_a").await, 7);
    assert_eq!(counted(q.clone(), "default").await, 3);

    // TRUNCATE never fires row-level or transition-table triggers, so the
    // INSERT/UPDATE/DELETE counter triggers do not see it. Without an AFTER
    // TRUNCATE trigger the counters keep reporting rows that no longer exist --
    // forever, since nothing recomputes them. Observed for real: a soak DB
    // cleaned with TRUNCATE between runs reported 41,038 completed jobs that
    // were not there, and the dashboard believed it.
    sqlx::query("TRUNCATE stampeded_jobs_trunc_a")
        .execute(q.pool())
        .await
        .unwrap();

    assert_eq!(
        counted(q.clone(), "trunc_a").await,
        0,
        "counters still claim rows after truncating the partition"
    );
    assert_eq!(
        counted(q.clone(), "default").await,
        3,
        "truncating one partition must not disturb another queue's counters"
    );

    // And the parent, which cascades to every partition.
    sqlx::query("TRUNCATE stampeded_jobs")
        .execute(q.pool())
        .await
        .unwrap();

    assert_eq!(
        counted(q.clone(), "default").await,
        0,
        "counters still claim rows after truncating the parent table"
    );
}
