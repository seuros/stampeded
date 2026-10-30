//! Integration tests against a real Postgres 18, skipped without a database URL.
//!
//!   STAMPEDED_TEST_DATABASE_URL=postgres://ubuntu:ubuntu@localhost/stampeded_test \
//!     cargo test --test integration

mod common;

use common::{database_url, db_now, pool_on};
use jiff::{SignedDuration, Timestamp};
use sqlx::AssertSqlSafe;
use stampeded::{
    DstPolicy, FailureKind, JobState, NewJob, Period, Queue, Wakeup, Worker, WorkerConfig,
};
use std::assert_matches;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// `common::setup`, against a database whose clock runs `skew` ahead of ours.
async fn setup_with_db_clock_ahead(skew: &str) -> Option<Queue> {
    let q = common::setup().await?;
    let url = database_url()?;
    let schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(q.pool())
        .await
        .expect("schema");
    // Schema name is UUID-derived and the skew a test constant.
    sqlx::query(AssertSqlSafe(format!(
        "CREATE FUNCTION {schema}.now() RETURNS timestamptz LANGUAGE sql STABLE \
         AS $$ SELECT pg_catalog.now() + interval '{skew}' $$"
    )))
    .execute(q.pool())
    .await
    .expect("shadow now()");
    q.pool().close().await;
    // pg_catalog last, so the shadow wins every unqualified now(); column
    // defaults keep the real one.
    Some(Queue::new(
        pool_on(&url, &format!("{schema}, pg_catalog")).await,
    ))
}

/// `common::setup`, plus the URL a listener needs (its channels are schema-qualified).
async fn setup_with_url() -> Option<(Queue, String)> {
    let q = common::setup().await?;
    let url = database_url()?;
    q.create_queue("default").await.expect("create queue");
    Some((q, url))
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
    // Incremented AT CLAIM, not at failure -- the poison-job brake.
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
    // Same scope, other queue: allowed, as the partition key leads the unique index.
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
    // The AS MATERIALIZED fence: without it the planner may update past the LIMIT.
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
    // Were `retryable` not claimable, a failing job would never spend its budget.
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

    // `worker_missing`, not a raise: it charges interruptions, not attempts.
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

    // Partitions do not inherit storage and autovacuum settings from the parent.
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

    let future: bool = sqlx::query_scalar(
        "SELECT next_run_at > now() FROM stampeded_schedules WHERE key='behind'",
    )
    .fetch_one(q.pool())
    .await
    .unwrap();
    assert!(future, "schedule should have caught up to the present");
}

#[tokio::test]
async fn the_scheduler_keeps_the_database_clock_not_ours() {
    // Due means `next_run_at <= now()` on the database clock. Two hours of skew
    // put an hourly boundary between the two clocks every time.
    let q = q!(setup_with_db_clock_ahead("2 hours"));
    let skew = q.clock_skew().await.unwrap();
    assert!(
        skew < -7000.0,
        "the shadowed now() is not in effect: skew {skew}s"
    );

    q.upsert_cron_schedule(
        "api",
        "default",
        "t",
        "0 * * * *",
        "UTC",
        Some(DstPolicy::Instant),
    )
    .await
    .unwrap();
    assert_eq!(
        q.run_due_schedules(10).await.unwrap(),
        0,
        "upsert_cron_schedule seeded the database's past"
    );

    sqlx::query(
        "INSERT INTO stampeded_schedules (key, queue, task, cron_expr) \
         VALUES ('bare', 'default', 't', '0 * * * *')",
    )
    .execute(q.pool())
    .await
    .unwrap();
    assert_eq!(
        q.run_due_schedules(10).await.unwrap(),
        0,
        "the tick seeded a bare cron row into the database's past"
    );

    q.upsert_schedule("hourly", "default", "t", Duration::from_secs(3600))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE stampeded_schedules SET next_run_at = now() - interval '1 hour' \
          WHERE key = 'hourly'",
    )
    .execute(q.pool())
    .await
    .unwrap();
    assert_eq!(q.run_due_schedules(10).await.unwrap(), 1);
    assert_eq!(
        q.run_due_schedules(10).await.unwrap(),
        0,
        "the tick advanced a schedule into the database's past"
    );
}

#[tokio::test]
async fn concurrent_schedulers_do_not_double_fire() {
    // No leader election: the row lock in stampeded_claim_due_schedules excludes.
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
    // Every minute, so it can be made due by hand instead of by sleeping.
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

    // Seeded to the next occurrence: a new schedule must not fire at once.
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
async fn a_cron_row_written_without_a_first_occurrence_is_seeded_not_fired() {
    // Ruby and a bare INSERT cannot parse cron, so next_run_at defaults to now();
    // firing it would run a nightly job at deploy time.
    let q = q!();
    sqlx::query(
        "INSERT INTO stampeded_schedules (key, queue, task, cron_expr) \
         VALUES ('nightly', 'default', 'report', '0 3 * * *')",
    )
    .execute(q.pool())
    .await
    .unwrap();

    assert_eq!(
        q.run_due_schedules(10).await.unwrap(),
        0,
        "fired on creation"
    );

    let (hour, minute, future): (f64, f64, bool) = sqlx::query_as(
        "SELECT extract(hour FROM next_run_at AT TIME ZONE 'UTC')::float8, \
                extract(minute FROM next_run_at AT TIME ZONE 'UTC')::float8, \
                next_run_at > now() \
           FROM stampeded_schedules WHERE key = 'nightly'",
    )
    .fetch_one(q.pool())
    .await
    .unwrap();
    assert!(future, "seeded into the past");
    assert_eq!((hour, minute), (3.0, 0.0), "seeded off the expression");

    // Seeding is once: an operator making it due by hand still fires it.
    sqlx::query("UPDATE stampeded_schedules SET next_run_at = now() WHERE key = 'nightly'")
        .execute(q.pool())
        .await
        .unwrap();
    assert_eq!(q.run_due_schedules(10).await.unwrap(), 1);
}

#[tokio::test]
async fn a_malformed_cron_expression_is_rejected_at_write_time() {
    // A typo is a failed call, not a row that warns once a second forever.
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
    // The CHECK allows exactly one rule, so the upsert must null the other.
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

#[tokio::test]
async fn an_unknown_timezone_fires_in_utc_and_does_not_stop_the_others() {
    let q = q!();
    q.upsert_schedule("good", "default", "tick", Duration::from_secs(60))
        .await
        .unwrap();
    // Written in SQL, as the README shows, so nothing checked the zone on the way in.
    sqlx::query(
        "INSERT INTO stampeded_schedules (key, queue, task, cron_expr, timezone, dst_policy) \
         VALUES ('typo', 'default', 'nightly', '0 3 * * *', 'Africa/Casablnca', 'wall_clock')",
    )
    .execute(q.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE stampeded_schedules SET next_run_at = now() - interval '1 minute'")
        .execute(q.pool())
        .await
        .unwrap();

    // A wall-clock key in a zone Postgres rejects used to roll back the whole tick.
    assert_eq!(q.run_due_schedules(10).await.unwrap(), 2);

    let tasks: Vec<String> = sqlx::query_scalar("SELECT task FROM stampeded_jobs ORDER BY task")
        .fetch_all(q.pool())
        .await
        .unwrap();
    assert_eq!(tasks, ["nightly", "tick"]);
    // Firing in the wrong zone beats never firing: the next run is 03:00 UTC.
    let next: String = sqlx::query_scalar(
        "SELECT to_char(next_run_at AT TIME ZONE 'UTC', 'HH24:MI') \
           FROM stampeded_schedules WHERE key = 'typo'",
    )
    .fetch_one(q.pool())
    .await
    .unwrap();
    assert_eq!(next, "03:00");
}

// ------------------------------------------------------ failure accounting

#[tokio::test]
async fn our_own_backpressure_refunds_the_attempt() {
    // `attempt` is spent at claim, so declining to run a job gives it back, or a
    // downstream outage burns the whole backlog's budget.
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
    // If everything refunded, max_attempts would never be reached.
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
    // The interruptions brake: a worker-killing job stops at the second claim, not the 25th.
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
    // Errors are bounded too: their text dwarfs the worker ids in attempted_by.
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
    // The DLQ is a queue, so re-drive is working it; provenance makes a letter actionable.
    let q = q!();
    q.create_queue("dead").await.unwrap();

    let id = q
        .enqueue(
            NewJob::new("default", "doomed")
                .args(serde_json::json!({ "order": 7 }))
                .meta(serde_json::json!({ "activejob": { "job_class": "DoomedJob" } }))
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
    assert_eq!(letters[0].args, serde_json::json!({ "order": 7 }));
    assert_eq!(
        letters[0].meta,
        serde_json::json!({ "activejob": { "job_class": "DoomedJob" } }),
        "a dead letter must still be runnable by the runtime that enqueued it"
    );
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
    // The worker heartbeats, so the reaper sees nothing; only expiry catches the job.
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
    // A slow job must be distinguishable from a wedged one.
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

// --------------------------------------------------------- operator actions

#[tokio::test]
async fn cancel_takes_only_a_job_that_has_not_started() {
    let q = q!();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    let running = q
        .enqueue(NewJob::new("default", "t"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(q.claim("default", w, 1).await.unwrap()[0].id, running);
    let waiting = q
        .enqueue(NewJob::new("default", "t").unique("acct:1"))
        .await
        .unwrap()
        .unwrap();

    assert!(
        !q.cancel("default", running).await.unwrap(),
        "an executing job is its worker's"
    );
    assert!(q.cancel("default", waiting).await.unwrap());
    assert!(
        !q.cancel("default", waiting).await.unwrap(),
        "already cancelled"
    );
    let job = q.fetch("default", waiting).await.unwrap().unwrap();
    assert_eq!(job.state, JobState::Cancelled);
    assert!(
        q.enqueue(NewJob::new("default", "t").unique("acct:1"))
            .await
            .unwrap()
            .is_some(),
        "cancelling frees the dedup slot"
    );
}

#[tokio::test]
async fn resurrect_runs_a_job_given_up_on_again_and_nothing_else() {
    let q = q!();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    let id = q
        .enqueue(NewJob::new("default", "t").max_attempts(1))
        .await
        .unwrap()
        .unwrap();
    let job = q.claim("default", w, 1).await.unwrap().remove(0);
    q.fail(&job, w, FailureKind::Raised, "boom", None)
        .await
        .unwrap();
    assert_eq!(
        q.fetch("default", id).await.unwrap().unwrap().state,
        JobState::Discarded
    );

    assert!(q.resurrect("default", id).await.unwrap());
    let job = q.fetch("default", id).await.unwrap().unwrap();
    assert_eq!((job.state, job.attempt), (JobState::Available, 0));
    assert!(
        !q.resurrect("default", id).await.unwrap(),
        "an available job is not given up on"
    );
    let job = q.claim("default", w, 1).await.unwrap().remove(0);
    assert_eq!(job.id, id, "claimable again");

    q.complete(&job, w).await.unwrap();
    assert!(
        !q.resurrect("default", id).await.unwrap(),
        "a completed job stays completed"
    );
}

#[tokio::test]
async fn a_resurrected_job_gets_every_budget_back() {
    // Discarded for killing its workers, it must not be discarded again by the next death.
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
    let killed = async || {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let job = q.claim("default", w, 1).await.unwrap().remove(0);
        q.fail_with_exit(
            &job,
            w,
            FailureKind::WorkerExited,
            "killed",
            None,
            Some(137),
        )
        .await
        .unwrap();
        q.fetch("default", id).await.unwrap().unwrap()
    };
    killed().await;
    assert_eq!(killed().await.state, JobState::Discarded);
    // As if it had also been throttled to its limit.
    sqlx::query("UPDATE stampeded_jobs SET deferrals = max_deferrals")
        .execute(q.pool())
        .await
        .unwrap();

    assert!(q.resurrect("default", id).await.unwrap());
    let job = q.fetch("default", id).await.unwrap().unwrap();
    assert_eq!((job.attempt, job.interruptions, job.deferrals), (0, 0, 0));
    assert_eq!(killed().await.state, JobState::Retryable);
}

#[tokio::test]
async fn a_paused_queue_hands_out_nothing_until_resumed() {
    let q = q!();
    q.create_queue("mail").await.unwrap();
    let w = q
        .register_worker("h", 1, &["default".into(), "mail".into()])
        .await
        .unwrap();
    let enqueue = async |queue: &str| q.enqueue(NewJob::new(queue, "t")).await.unwrap().unwrap();
    enqueue("default").await;
    let running = q.claim("default", w, 1).await.unwrap().remove(0);
    enqueue("default").await;
    enqueue("mail").await;

    q.pause_queue("default").await.unwrap();
    q.pause_queue("default").await.unwrap();
    assert_eq!(q.paused_queues().await.unwrap(), ["default"]);
    assert!(
        q.claim("default", w, 10).await.unwrap().is_empty(),
        "the claim itself refuses, daemon or not"
    );
    assert_eq!(q.claim("mail", w, 10).await.unwrap().len(), 1);
    assert!(
        q.complete(&running, w).await.unwrap().owned,
        "a running job finishes"
    );
    enqueue("default").await;
    let snapshots = q.queue_snapshots().await.unwrap();
    assert!(snapshots.iter().any(|s| s.queue == "default" && s.paused));

    q.resume_queue("default").await.unwrap();
    assert_eq!(q.paused_queues().await.unwrap(), Vec::<String>::new());
    assert_eq!(
        q.claim("default", w, 10).await.unwrap().len(),
        2,
        "the backlog waited"
    );
}

// ---------------------------------------------------------- depth counters

#[tokio::test]
async fn depth_comes_from_counters_and_tracks_transitions() {
    // The counters follow the job through its lifecycle, or the dashboard lies.
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
    // Drift is a bug, but one repairable without downtime.
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
        .map_or(0, |(_, n)| n)
}

// -------------------------------------------------- wakeup and maintenance

#[tokio::test]
async fn the_trigger_notifies_and_the_listener_hears_it() {
    // A plain INSERT runs none of our code, so the database itself must notify.
    let Some((q, url)) = setup_with_url().await else {
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
    // Postgres collapses identical (channel, payload) pairs per transaction:
    // hence a statement-level trigger whose payload is the bare queue name.
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

    // A payload with a nonce would make 500 of these.
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
    // Nothing to wake for: the scheduler promotes it when due.
    let Some((q, url)) = setup_with_url().await else {
        return;
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let wakeup = Wakeup::start(q.pool(), &url, rx).await.unwrap();
    let mut woke = wakeup.subscribe("default");

    q.enqueue(
        NewJob::new("default", "later")
            .scheduled_at(Timestamp::now() + SignedDuration::from_hours(1)),
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
async fn timestamps_round_trip_through_jiff_sqlx() {
    // Through jiff-sqlx (nullable columns crate::timestamp): microseconds survive
    // the bind, and NULL decodes to None.
    let q = q!();
    let at: Timestamp = "2030-01-02T03:04:05.123456Z".parse().unwrap();

    let id = q
        .enqueue(NewJob::new("default", "later").scheduled_at(at))
        .await
        .unwrap()
        .expect("enqueued");
    let job = q.fetch("default", id).await.unwrap().expect("job");

    assert_eq!(job.scheduled_at, at, "bind and decode must be lossless");
    assert_eq!(job.finished_at, None, "NULL decodes to None");
    let now = db_now(&q).await;
    assert!(
        job.inserted_at <= now,
        "inserted_at {} is after the database's now, {now}",
        job.inserted_at
    );

    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    q.enqueue(NewJob::new("default", "now")).await.unwrap();
    let claimed = q.claim("default", w, 1).await.unwrap();
    let job = claimed.first().expect("claimed");
    q.complete(job, w).await.unwrap();
    let done = q.fetch("default", job.id).await.unwrap().expect("job");

    assert!(
        done.claimed_at.is_some(),
        "a set nullable column decodes to Some"
    );
    assert!(done.finished_at.is_some_and(|f| f >= done.inserted_at));
}

#[tokio::test]
async fn tenant_load_counts_live_jobs_and_deferrals_per_tenant() {
    // The dashboard's tenant panel: live backlog only, never finished history.
    let q = q!();
    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    for _ in 0..3 {
        q.enqueue(NewJob::new("default", "t").tenant("acme"))
            .await
            .unwrap();
    }
    q.enqueue(NewJob::new("default", "t").tenant("globex"))
        .await
        .unwrap();
    q.enqueue(NewJob::new("default", "untagged")).await.unwrap();

    // Claim every job; then run one acme job, defer one, and finish one.
    let claimed = q.claim("default", w, 10).await.unwrap();
    let acme: Vec<_> = claimed
        .iter()
        .filter(|j| j.tenant_id.as_deref() == Some("acme"))
        .collect();
    let (deferred, finished) = (acme[0], acme[1]);
    q.fail(
        deferred,
        w,
        FailureKind::Throttled,
        "tenant over its share",
        Some(Duration::from_secs(60)),
    )
    .await
    .unwrap();
    q.complete(finished, w).await.unwrap();
    for j in claimed
        .iter()
        .filter(|j| j.id != deferred.id && j.id != finished.id)
    {
        if j.tenant_id.as_deref() != Some("acme") {
            q.fail(
                j,
                w,
                FailureKind::Raised,
                "boom",
                Some(Duration::from_secs(60)),
            )
            .await
            .unwrap();
        }
    }

    let load = q.tenant_load(10).await.unwrap();
    let row = |t: &str| load.iter().find(|r| r.tenant == t).cloned();
    let acme = row("acme").expect("acme has live jobs");
    // One executing, one deferred (waiting), one completed and no longer live.
    assert_eq!((acme.waiting, acme.running, acme.throttled), (1, 1, 1));
    let globex = row("globex").expect("globex has a live job");
    assert_eq!(
        (globex.waiting, globex.running, globex.throttled),
        (1, 0, 0),
        "a raise is not a deferral"
    );
    assert!(
        load.iter().all(|r| !r.tenant.is_empty()),
        "untagged jobs are not a tenant"
    );
    assert_eq!(load.len(), 2);
}

#[tokio::test]
async fn args_and_meta_must_be_json_objects() {
    // A raw INSERT skips NewJob, so the schema alone keeps args and meta objects.
    let q = q!();
    for value in ["null", "[1, 2]", "42", "\"text\""] {
        for (column, table, constraint) in [
            ("args", "stampeded_jobs", "stampeded_jobs_args_object"),
            ("meta", "stampeded_jobs", "stampeded_jobs_meta_object"),
            (
                "args",
                "stampeded_schedules",
                "stampeded_schedules_args_object",
            ),
            (
                "meta",
                "stampeded_schedules",
                "stampeded_schedules_meta_object",
            ),
        ] {
            let sql = if table == "stampeded_jobs" {
                format!(
                    "INSERT INTO stampeded_jobs (queue, task, {column}) VALUES ('default', 't', '{value}'::jsonb)"
                )
            } else {
                format!(
                    "INSERT INTO stampeded_schedules (key, queue, task, {column}, interval_secs) \
                     VALUES ('k', 'default', 't', '{value}'::jsonb, 60)"
                )
            };
            let err = sqlx::query(AssertSqlSafe(sql.clone()))
                .execute(q.pool())
                .await
                .expect_err(&sql);
            let db = err.as_database_error().expect("a database error");
            assert_eq!(db.constraint(), Some(constraint), "{sql}");
        }
    }

    // Omitting both is still "no arguments, no context".
    let (args, meta): (serde_json::Value, serde_json::Value) = sqlx::query_as(
        "INSERT INTO stampeded_jobs (queue, task) VALUES ('default', 't') RETURNING args, meta",
    )
    .fetch_one(q.pool())
    .await
    .unwrap();
    assert_eq!((args, meta), (serde_json::json!({}), serde_json::json!({})));
}

#[tokio::test]
async fn a_schedule_hands_its_args_and_meta_to_every_job_it_fires() {
    let q = q!();
    sqlx::query(
        "INSERT INTO stampeded_schedules (key, queue, task, args, meta, interval_secs, next_run_at) \
         VALUES ('report', 'default', 'report.daily', '{\"region\": \"eu\"}', \
                 '{\"activejob\": {\"job_class\": \"ReportJob\"}}', 3600, now() - interval '1 minute')",
    )
    .execute(q.pool())
    .await
    .unwrap();

    assert_eq!(q.run_due_schedules(10).await.unwrap(), 1);

    let w = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    let jobs = q.claim("default", w, 1).await.unwrap();
    assert_eq!(jobs[0].task, "report.daily");
    assert_eq!(jobs[0].args, serde_json::json!({ "region": "eu" }));
    assert_eq!(
        jobs[0].meta,
        serde_json::json!({ "activejob": { "job_class": "ReportJob" } })
    );
}

#[tokio::test]
async fn failures_are_kept_longer_than_successes() {
    // Two retention clocks: a discarded job is what someone gets paged about.
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

    // Completed retention zero, failed a week.
    let n = q
        .prune_batch(
            Duration::ZERO,
            Duration::from_hours(7 * 24),
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
    // pg_stat_user_tables spans the database; other schemas are not ours.
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
    // 500 updates at scale_factor 0.02 put dead tuples well past the threshold.
    let q = q!();
    for _ in 0..500 {
        q.enqueue(NewJob::new("default", "churn")).await.unwrap();
    }
    // One backend for both: each flushes its own stats, and
    // pg_stat_force_next_flush (PG15+) only flushes the caller's.
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
    // _ccnew/_ccold are failed REINDEX CONCURRENTLY leftovers; retrying one loops.
    let q = q!();
    let names = q.sparse_indexes().await.unwrap();
    assert!(
        names
            .iter()
            .all(|n| !n.contains("_ccnew") && !n.contains("_ccold"))
    );
}

// -------------------------------------- unique windows, pruning, deferrals

#[tokio::test]
async fn a_period_bucket_allows_one_job_per_window() {
    // Bucketed server-side from inserted_at, so enqueuers with skewed clocks agree.
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
    // The bucket is part of the key, or `unique_per` would be `unique`.
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
async fn windows_that_start_together_are_still_different_windows() {
    // 00:30 UTC: the hour and its day both start at midnight, so the key must
    // name the period (it once did not, and this test then only failed in
    // the first hour of a UTC day).
    let q = q!();
    let insert = |period: &'static str| {
        sqlx::query(
            "INSERT INTO stampeded_jobs (queue, task, dedup_scope, dedup_period, inserted_at)
             VALUES ('default', 'r', 'k', $1, '2026-10-24T00:30:00Z')",
        )
        .bind(period)
        .execute(q.pool())
    };
    insert("hour").await.expect("the hour's job");
    insert("day")
        .await
        .expect("a different window is a different key");
    insert("hour")
        .await
        .expect_err("the same window is the same key");
}

#[tokio::test]
async fn an_unbucketed_unique_job_is_unique_forever() {
    // No period, no bucket: the key is the scope alone.
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
    // The key buckets inserted_at, which never changes; bucketing scheduled_at
    // would move it on a snooze and let a duplicate in.
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

    // Snooze it a day.
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
    // The pruner loops until a short batch, so this must be the row count, not
    // the first row a SQL function over `DELETE ... RETURNING 1` would return.
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
    // A full batch tells the caller there is more.
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
    // Refunded deferrals still count toward max_deferrals, or a permanently open
    // circuit re-defers the same job forever.
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
    // Still refunded: a dependency being down never charged the job.
    assert_eq!(job.attempt, 0, "deferrals must not become a retry budget");
    assert_eq!(
        job.failure_kind,
        Some(FailureKind::BreakerOpen),
        "the recorded cause must still say it was our backpressure"
    );
}

#[tokio::test]
async fn an_ordinary_outage_does_not_exhaust_the_deferral_budget() {
    // At the hour-capped backoff, 100 deferrals is days of downtime.
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
    // A deploy must not charge interruptions: the drain lets the job report itself.
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
            queues: stampeded::Queues::one("default"),
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
    // An unbounded drain holds the deploy until SIGKILL, and nothing deregisters.
    let q = q!();
    q.enqueue(NewJob::new("default", "wedged")).await.unwrap();

    let started = Arc::new(tokio::sync::Notify::new());
    let ready = started.clone();

    let worker = Worker::new(
        q.clone(),
        WorkerConfig {
            queues: stampeded::Queues::one("default"),
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

    // Deregistered, so the reaper need not wait out dead_after.
    let workers: i64 = sqlx::query_scalar("SELECT count(*) FROM stampeded_workers")
        .fetch_one(q.pool())
        .await
        .unwrap();
    assert_eq!(workers, 0, "a bounded shutdown must still deregister");
}

// ------------------------------------------------------------- checkpoints

#[tokio::test]
async fn a_job_resumes_from_its_checkpoint_after_its_worker_dies() {
    // A job that died at 50,000 of 100,000 does not start again at row 1.
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
    // A checkpoints and wedges, B resumes and gets further, then A wakes up.
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
    // A job that checkpointed is not a worker-killer, so progress resets interruptions.
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
    // Attempts still bound a job that progresses one unit, then dies forever.
    assert_eq!(after.attempt, 2, "attempt must NOT be refunded");
}

#[tokio::test]
async fn a_checkpoint_is_also_a_heartbeat() {
    // A job that just saved progress is alive; it needs no separate touch.
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
    // Dead weight otherwise, on the table most prone to bloat.
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
    // Bounded in SQL: a worker in another language runs none of our checks.
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
    // In the work's transaction, the cursor never claims work that rolled back.
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
    // The same step model as the Ruby worker's.
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
    // The checkpoint as Ruby's Stampeded::Steps writes it; steps_test.rb mirrors this.
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
    // Otherwise the ledger grows a row per firing forever.
    let q = q!();

    sqlx::query(
        "INSERT INTO stampeded_schedules (key, queue, task, interval_secs) \
         VALUES ('tick', 'default', 't', 60)",
    )
    .execute(q.pool())
    .await
    .unwrap();

    // Aged firings.
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
        .prune_occurrences(Duration::from_hours(30 * 24))
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
    // A LISTEN in a long transaction pins the async queue; once it is full,
    // every NOTIFY, and so every enqueue, errors.
    let q = q!();

    let usage = q.notification_queue_usage().await.unwrap();
    assert!((0.0..=1.0).contains(&usage), "usage is a fraction: {usage}");

    // Loose on purpose: a diagnostic must not fail a slow CI box.
    let skew = q.clock_skew().await.unwrap();
    assert!(
        skew.abs() < 60.0,
        "skew against a local database was {skew}s"
    );
}

#[tokio::test]
async fn the_job_listing_is_bounded_by_a_statement_timeout() {
    // Panels re-run every second, so a slow listing is permanent load; a timeout
    // bounds it rather than an index every insert would pay for.
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

    // Newest first, relying on uuidv7 order.
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
    // Depth, the dashboard and metrics read these counters instead of count(*).
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

    // Cross-check against the table: a mismatch means the trigger drifted.
    let actual: i64 =
        sqlx::query_scalar("SELECT count(*) FROM stampeded_jobs WHERE state = 'completed'")
            .fetch_one(q.pool())
            .await
            .unwrap();
    assert_eq!(actual, 200, "the counter and the table must agree");
}

#[tokio::test]
async fn counters_survive_a_rolled_back_enqueue() {
    // The trigger fires inside the transaction, so a rollback takes the count too.
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
    // Under real concurrency: a unique index, not SELECT-then-INSERT.
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
    // `claimed_by = p_worker_id` in `stampeded_complete` tells the loser it lost.
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
    // The cron path: the occurrence ledger backs up the row lock.
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
    // Claim needs `attempt < max_attempts`, so max_attempts 0 could never run.
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
    // TOAST keeps the row small, but `stampeded_claim` returns `stampeded_jobs.*`
    // and so detoasts the payload on every claim.
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
    // Clamping 0 to 1 would fire every second, which nobody asked for.
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

    // Past i64 seconds: a validation error, not an opaque CHECK violation.
    assert_matches!(
        q.upsert_schedule("tick", "default", "t", Duration::from_secs(u64::MAX))
            .await,
        Err(stampeded::Error::InvalidSchedule(_))
    );

    // One second is aggressive but legitimate, and still works.
    assert!(
        q.upsert_schedule("tick", "default", "t", Duration::from_secs(1))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn a_schedule_needs_a_key() {
    // The key is how a schedule is paused, removed or found again.
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

/// A handler's own retry delay (Ruby's `RetryIn`) reaches the database.
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
    let delay = row.scheduled_at.duration_since(db_now(&q).await);

    assert_eq!(row.state.as_str(), "retryable");
    assert_eq!(
        row.attempt, 1,
        "a chosen delay must still charge the attempt"
    );
    assert!(
        delay.as_secs() > 3000,
        "the chosen delay did not reach the database: {delay:?}"
    );
    assert!(
        delay.as_secs() <= 3600,
        "the chosen delay was not applied as given: {delay:?}"
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

/// The claim never plans a sort: its ORDER BY matches `stampeded_jobs_claim_idx`.
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

    // The claim's own predicate and ordering, verbatim from the schema.
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

/// A checkpoint that accumulates is warned about; one that is merely large is not.
#[tokio::test]
async fn a_growing_checkpoint_warns_but_a_merely_large_one_does_not() {
    // The warning is a NOTICE, so reproduce the function's predicate: text
    // lengths, since pg_column_size of the stored column is compressed.
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

    // Large but stable is not misuse.
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

    assert_eq!(after.0, 0, "progress must forgive interruptions");
    assert_eq!(
        after.1, before.1,
        "progress must NOT refund an attempt, or a job that checkpoints once \
         and then crashes forever becomes immortal"
    );
}

/// Per-job retry backoff is settable at enqueue.
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

    // base is a smallint: clamped, not an INSERT failing "smallint out of range".
    let id2 = q
        .enqueue(NewJob::new("default", "t2").backoff(90_000, 300_000))
        .await
        .unwrap()
        .expect("enqueued");
    let (base,): (i16,) =
        sqlx::query_as("SELECT backoff_base_ms FROM stampeded_jobs WHERE id = $1")
            .bind(id2)
            .fetch_one(q.pool())
            .await
            .unwrap();
    assert_eq!(base, i16::MAX, "an over-range base must clamp, not explode");
}

/// Statement-level triggers are not cloned to partitions, so one made outside
/// `stampeded_create_queue` cannot wake anybody; that must be detectable.
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

    // Attempt 1 stalls: expired (job-level), but its worker is alive, so not reaped.
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

    // The same worker picks it back up, as it does when it alone serves the queue.
    let second = q.claim("default", w, 1).await.unwrap().pop().unwrap();
    assert_eq!(second.id, first.id);
    assert_eq!(second.attempt, 2, "the retry is a new attempt");

    // Attempt 1 returns while attempt 2 runs: same claimed_by, so only the attempt fences it.
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

    // TRUNCATE fires no row or transition-table triggers; an AFTER TRUNCATE one
    // resets the counters.
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

#[tokio::test]
async fn the_planner_holds_a_full_queue_and_rushes_a_late_one() {
    let q = q!();
    sqlx::query(
        "INSERT INTO stampeded_queue_policies (queue, max_running, max_wait_secs) \
         VALUES ('capped', 1, NULL), ('late', NULL, 60), ('calm', NULL, 60)",
    )
    .execute(q.pool())
    .await
    .unwrap();

    // capped: its one slot taken. late: its next job due two minutes ago.
    // calm: a job due just now.
    let w = q.register_worker("h", 1, &["capped".into()]).await.unwrap();
    q.enqueue(NewJob::new("capped", "t")).await.unwrap();
    assert_eq!(q.claim("capped", w, 1).await.unwrap().len(), 1);
    q.enqueue(NewJob::new("late", "t")).await.unwrap();
    q.enqueue(NewJob::new("calm", "t")).await.unwrap();
    sqlx::query("UPDATE stampeded_jobs SET scheduled_at = now() - interval '2 minutes' WHERE queue = 'late'")
        .execute(q.pool())
        .await
        .unwrap();

    let planner = stampeded::Planner::new().unwrap();
    let decisions: Vec<_> = q
        .queue_snapshots()
        .await
        .unwrap()
        .iter()
        .map(|s| (s.queue.clone(), planner.decide(s)))
        .collect();
    assert_eq!(q.record_verdicts(&decisions).await.unwrap(), 3);

    let rows: Vec<(String, String, Option<String>, bool)> = sqlx::query_as(
        "SELECT queue, verdict, reason, decided_at IS NOT NULL \
           FROM stampeded_queue_policies ORDER BY queue",
    )
    .fetch_all(q.pool())
    .await
    .unwrap();
    assert_eq!(rows[0], ("calm".into(), "steady".into(), None, true));
    assert_eq!(
        rows[1],
        (
            "capped".into(),
            "hold".into(),
            Some("1 running, max 1".into()),
            true
        )
    );
    assert_eq!(rows[2].1, "rush", "{:?}", rows[2]);
    assert!(
        rows[2]
            .2
            .as_deref()
            .is_some_and(|r| r.starts_with("next job waited 12")),
        "{:?}",
        rows[2]
    );

    // Unchanged verdicts are not rewritten every tick.
    let again: Vec<_> = decisions
        .into_iter()
        .filter(|(queue, _)| queue != "late")
        .collect();
    assert_eq!(q.record_verdicts(&again).await.unwrap(), 0);
}
