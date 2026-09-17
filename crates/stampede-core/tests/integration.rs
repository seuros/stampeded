//! Integration tests against a real Postgres 18.
//!
//! Skipped unless `STAMPEDE_TEST_DATABASE_URL` is set — these assert behaviour
//! that only the database can produce (SKIP LOCKED disjointness, partial unique
//! indexes, the claim fence), so a mock would only prove the mock works.
//!
//!   STAMPEDE_TEST_DATABASE_URL=postgres://ubuntu:ubuntu@localhost/stampede_test \
//!     cargo test --test integration

use sqlx::{AssertSqlSafe, PgPool};
use stampede_core::{DstPolicy, FailureKind, NewJob, Queue};
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

    stampede_core::schema::migrate(&pool).await.expect("migrate");
    Some(Queue::new(pool))
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
    let w = q.register_worker("test", 1, &["default".into()]).await.unwrap();

    q.enqueue(NewJob::new("default", "send")).await.unwrap().unwrap();

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

    assert!(q.enqueue(NewJob::new("a", "t").unique("k")).await.unwrap().is_some());
    // Same scope, same queue: suppressed.
    assert!(q.enqueue(NewJob::new("a", "t").unique("k")).await.unwrap().is_none());
    // Same scope, different queue: allowed. Uniqueness is per-queue because the
    // partition key must lead the unique index (plans/README §1).
    assert!(q.enqueue(NewJob::new("b", "t").unique("k")).await.unwrap().is_some());
    // No scope: never deduplicated.
    assert!(q.enqueue(NewJob::new("a", "t")).await.unwrap().is_some());
    assert!(q.enqueue(NewJob::new("a", "t")).await.unwrap().is_some());
}

#[tokio::test]
async fn completing_releases_the_dedup_slot() {
    let q = q!();
    let w = q.register_worker("test", 1, &["default".into()]).await.unwrap();

    q.enqueue(NewJob::new("default", "t").unique("once")).await.unwrap().unwrap();
    // Blocked while the first is still live.
    assert!(q.enqueue(NewJob::new("default", "t").unique("once")).await.unwrap().is_none());

    let job = q.claim("default", w, 1).await.unwrap().pop().unwrap();
    let t = q.complete(&job, w).await.unwrap();
    assert!(t.owned);
    assert_eq!(t.now.as_deref(), Some("completed"));

    // Slot released, so the same scope can be enqueued again.
    assert!(q.enqueue(NewJob::new("default", "t").unique("once")).await.unwrap().is_some());
}

#[tokio::test]
async fn concurrent_workers_claim_disjoint_jobs() {
    let q = q!();
    let a = q.register_worker("a", 1, &["default".into()]).await.unwrap();
    let b = q.register_worker("b", 2, &["default".into()]).await.unwrap();

    for _ in 0..10 {
        q.enqueue(NewJob::new("default", "t")).await.unwrap();
    }

    let (ja, jb) = tokio::join!(q.claim("default", a, 5), q.claim("default", b, 5));
    let (ja, jb) = (ja.unwrap(), jb.unwrap());

    assert_eq!(ja.len() + jb.len(), 10, "SKIP LOCKED should hand out all 10");
    let ids: std::collections::HashSet<Uuid> =
        ja.iter().chain(jb.iter()).map(|j| j.id).collect();
    assert_eq!(ids.len(), 10, "a job was handed to two workers");
}

#[tokio::test]
async fn claim_never_exceeds_limit() {
    // The CTE fence (AS MATERIALIZED). Without it the planner may nested-loop
    // the LIMIT subquery and update more rows than asked -- Oban's documented
    // bug, which pushes attempt past max_attempts (plans/04).
    let q = q!();
    let w = q.register_worker("test", 1, &["default".into()]).await.unwrap();
    for _ in 0..50 {
        q.enqueue(NewJob::new("default", "t")).await.unwrap();
    }
    for _ in 0..5 {
        let got = q.claim("default", w, 3).await.unwrap();
        assert!(got.len() <= 3, "claim returned {} rows for limit 3", got.len());
    }
}

#[tokio::test]
async fn retryable_is_reclaimable_so_poison_jobs_terminate() {
    // The bug the schema tests caught: if `retryable` is not claimable, a job
    // whose worker keeps dying never spends its attempt budget and loops
    // forever.
    let q = q!();
    let w = q.register_worker("test", 1, &["default".into()]).await.unwrap();
    q.enqueue(NewJob::new("default", "poison").max_attempts(3)).await.unwrap();

    for round in 1..=3 {
        let job = q.claim("default", w, 1).await.unwrap().pop()
            .unwrap_or_else(|| panic!("round {round}: nothing claimable"));
        assert_eq!(job.attempt, round);
        q.fail(&job, w, FailureKind::Raised, "boom", Some(std::time::Duration::ZERO))
            .await
            .unwrap();
    }

    // Budget spent: discarded, and no longer claimable.
    assert!(q.claim("default", w, 1).await.unwrap().is_empty());
    let depth = q.depth("default").await.unwrap();
    assert!(depth.iter().all(|(s, _)| s != "available" && s != "retryable"));
}

#[tokio::test]
async fn reaper_reclaims_jobs_from_dead_workers_as_interrupted() {
    let q = q!();
    let w = q.register_worker("test", 1, &["default".into()]).await.unwrap();
    q.enqueue(NewJob::new("default", "t")).await.unwrap();
    let _job = q.claim("default", w, 1).await.unwrap().pop().unwrap();

    // Worker stops heartbeating.
    sqlx::query("UPDATE stampede_workers SET last_heartbeat = now() - interval '1 hour'")
        .execute(q.pool())
        .await
        .unwrap();

    let n = q.reap(std::time::Duration::from_secs(300)).await.unwrap();
    assert_eq!(n, 1);

    // Reclaimed as `interrupted`, distinct from a raise -- only BullMQ and
    // solid_queue make this distinction (plans/15).
    let kind: Option<String> =
        sqlx::query_scalar("SELECT failure_kind::text FROM stampede_jobs")
            .fetch_one(q.pool())
            .await
            .unwrap();
    assert_eq!(kind.as_deref(), Some("interrupted"));

    // And it is claimable again, so the attempt budget can be spent.
    assert_eq!(q.claim("default", w, 1).await.unwrap().len(), 1);
}

#[tokio::test]
async fn cannot_complete_a_job_we_no_longer_own() {
    let q = q!();
    let a = q.register_worker("a", 1, &["default".into()]).await.unwrap();
    let b = q.register_worker("b", 2, &["default".into()]).await.unwrap();

    q.enqueue(NewJob::new("default", "t")).await.unwrap();
    let job = q.claim("default", a, 1).await.unwrap().pop().unwrap();

    // Someone else takes it (as the reaper would).
    sqlx::query("UPDATE stampede_jobs SET claimed_by = $1")
        .bind(b)
        .execute(q.pool())
        .await
        .unwrap();

    let t = q.complete(&job, a).await.unwrap();
    assert!(!t.owned, "must refuse to complete a job owned by another worker");
}

#[tokio::test]
async fn prune_removes_only_finished_jobs() {
    let q = q!();
    let w = q.register_worker("test", 1, &["default".into()]).await.unwrap();

    q.enqueue(NewJob::new("default", "done")).await.unwrap();
    q.enqueue(NewJob::new("default", "live")).await.unwrap();

    let job = q.claim("default", w, 1).await.unwrap().pop().unwrap();
    q.complete(&job, w).await.unwrap();

    // Nothing is old enough yet.
    assert_eq!(q.prune(std::time::Duration::from_secs(3600), 100).await.unwrap(), 0);

    sqlx::query("UPDATE stampede_jobs SET finished_at = now() - interval '30 days' WHERE finished_at IS NOT NULL")
        .execute(q.pool())
        .await
        .unwrap();

    assert_eq!(q.prune(std::time::Duration::from_secs(3600), 100).await.unwrap(), 1);
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
    let opts: Option<Vec<String>> = sqlx::query_scalar(
        "SELECT reloptions FROM pg_class WHERE relname = 'stampede_jobs_mail'",
    )
    .fetch_one(q.pool())
    .await
    .unwrap();

    let opts = opts.expect("partition has no reloptions");
    assert!(opts.iter().any(|o| o.starts_with("fillfactor=")));
    assert!(opts.iter().any(|o| o.contains("autovacuum_vacuum_scale_factor")));
}
// temporary probe appended to the test file

#[tokio::test]
async fn schedules_fire_once_and_do_not_drift() {
    let q = q!();
    q.upsert_schedule("hourly", "default", "report", std::time::Duration::from_secs(3600))
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
    q.upsert_schedule("behind", "default", "catchup", std::time::Duration::from_secs(3600))
        .await
        .unwrap();
    sqlx::query("UPDATE stampede_schedules SET next_run_at = now() - interval '3 days'")
        .execute(q.pool())
        .await
        .unwrap();

    assert_eq!(q.run_due_schedules(10).await.unwrap(), 1, "should fire exactly once");

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
    // stampede_run_due_schedules. Two nodes ticking at once must still produce
    // exactly one job.
    let q = q!();
    q.upsert_schedule("shared", "default", "once", std::time::Duration::from_secs(3600))
        .await
        .unwrap();

    let (a, b) = tokio::join!(q.run_due_schedules(10), q.run_due_schedules(10));
    assert_eq!(a.unwrap() + b.unwrap(), 1, "the schedule fired more than once");
}

#[tokio::test]
async fn cron_schedules_fire_and_advance_by_the_expression() {
    let q = q!();
    // Every minute, so the first occurrence is at most 60s out and we can make
    // it due by hand rather than sleeping.
    q.upsert_cron_schedule("ticker", "default", "tick", "* * * * *", "UTC", Some(DstPolicy::Instant))
        .await
        .unwrap();

    // Seeded to the NEXT occurrence, not to now: a schedule must not fire the
    // instant it is created.
    assert_eq!(q.run_due_schedules(10).await.unwrap(), 0, "fired on creation");

    sqlx::query("UPDATE stampede_schedules SET next_run_at = now() - interval '1 minute'")
        .execute(q.pool())
        .await
        .unwrap();

    assert_eq!(q.run_due_schedules(10).await.unwrap(), 1);
    assert_eq!(q.run_due_schedules(10).await.unwrap(), 0, "fired twice for one occurrence");

    let (expr, secs): (Option<String>, Option<i64>) =
        sqlx::query_as("SELECT cron_expr, interval_secs FROM stampede_schedules WHERE key='ticker'")
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
        q.upsert_cron_schedule("bad", "default", "t", "77 * * * *", "UTC", Some(DstPolicy::Instant))
            .await
            .is_err()
    );
    assert!(
        q.upsert_cron_schedule("bad", "default", "t", "* * * * *", "Mars/Olympus", Some(DstPolicy::Instant))
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
    q.upsert_schedule("flip", "default", "t", std::time::Duration::from_secs(60)).await.unwrap();
    q.upsert_cron_schedule("flip", "default", "t", "0 3 * * *", "UTC", Some(DstPolicy::WallClock))
        .await
        .unwrap();
    q.upsert_schedule("flip", "default", "t", std::time::Duration::from_secs(60)).await.unwrap();

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
    q.upsert_schedule("paused", "default", "nope", std::time::Duration::from_secs(1))
        .await
        .unwrap();
    sqlx::query("UPDATE stampede_schedules SET paused = true, next_run_at = now() - interval '1 hour'")
        .execute(q.pool())
        .await
        .unwrap();

    assert_eq!(q.run_due_schedules(10).await.unwrap(), 0);
}
