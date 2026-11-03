//! Real PostgreSQL regressions. Only STAMPEDED_TEST_DATABASE_URL is used, never
//! DATABASE_URL; each test migrates an isolated schema.

use super::*;
use crate::{JobState, NewJob};
use sqlx::{AssertSqlSafe, PgPool};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Notify, mpsc};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("worker regression exceeded five seconds")
}

struct Database {
    queue: Queue,
    admin: PgPool,
    schema: String,
}

impl Database {
    async fn setup() -> Option<Self> {
        let Ok(url) = std::env::var("STAMPEDED_TEST_DATABASE_URL") else {
            assert!(
                std::env::var_os("CI").is_none(),
                "CI must provide STAMPEDED_TEST_DATABASE_URL"
            );
            eprintln!("skipping PostgreSQL regression: STAMPEDED_TEST_DATABASE_URL is unset");
            return None;
        };
        let admin = bounded(PgPool::connect(&url)).await.unwrap();
        let version: String = sqlx::query_scalar("SHOW server_version_num")
            .fetch_one(&admin)
            .await
            .unwrap();
        assert!(version.parse::<u32>().unwrap() >= 180_000);
        let schema = format!("worker_{}", Uuid::now_v7().simple());
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let options: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(3))
            .connect_with(options.options([
                ("search_path", schema.as_str()),
                ("application_name", schema.as_str()),
                ("statement_timeout", "3000"),
            ]))
            .await
            .unwrap();
        bounded(crate::schema::migrate(&pool)).await.unwrap();
        Some(Self {
            queue: Queue::new(pool),
            admin,
            schema,
        })
    }

    async fn close(self) {
        self.queue.pool().close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await
        .unwrap();
        self.admin.close().await;
    }
}

fn config(shutdown_timeout: Duration) -> WorkerConfig {
    WorkerConfig {
        queues: Queues::one("default"),
        poll_interval: Duration::from_millis(5),
        heartbeat_interval: Duration::from_millis(10),
        max_concurrency: 1,
        batch_size: 1,
        tenant_limit: None,
        breaker_failure_threshold: None,
        shutdown_timeout,
        ..WorkerConfig::default()
    }
}

struct Active(Arc<AtomicUsize>);

impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Running {
    ctx: JobContext,
    task: tokio::task::JoinHandle<Result<()>>,
    shutdown: watch::Sender<bool>,
    release: Arc<Notify>,
    active: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
}

impl Running {
    async fn start(queue: &Queue, shutdown_timeout: Duration) -> Self {
        queue
            .enqueue(NewJob::new("default", "wait"))
            .await
            .unwrap()
            .unwrap();
        let release = Arc::new(Notify::new());
        let active = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let (started, mut receive) = mpsc::unbounded_channel();
        let worker = Worker::new(queue.clone(), config(shutdown_timeout)).register("wait", {
            let release = release.clone();
            let active = active.clone();
            let completed = completed.clone();
            move |ctx| {
                let release = release.clone();
                let active = active.clone();
                let completed = completed.clone();
                let started = started.clone();
                async move {
                    active.fetch_add(1, Ordering::SeqCst);
                    let _active = Active(active);
                    started.send(ctx).unwrap();
                    release.notified().await;
                    completed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        });
        let (shutdown, rx) = watch::channel(false);
        let task = tokio::spawn(worker.run(rx));
        let ctx = bounded(receive.recv()).await.unwrap();
        Self {
            ctx,
            task,
            shutdown,
            release,
            active,
            completed,
        }
    }

    async fn stop(&mut self) {
        self.shutdown.send(true).unwrap();
        bounded(&mut self.task).await.unwrap().unwrap();
    }
}

async fn workers(queue: &Queue) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM stampeded_workers")
        .fetch_one(queue.pool())
        .await
        .unwrap()
}

async fn job(queue: &Queue, id: Uuid) -> Job {
    sqlx::query_as("SELECT * FROM stampeded_jobs WHERE id = $1")
        .bind(id)
        .fetch_one(queue.pool())
        .await
        .unwrap()
}

async fn reap_stopped(queue: &Queue, worker: Uuid) {
    // Advance only the worker's age, not the job's one-hour expiry.
    let aged = sqlx::query(
        "UPDATE stampeded_workers SET last_heartbeat = now() - interval '6 minutes'
         WHERE id = $1",
    )
    .bind(worker)
    .execute(queue.pool())
    .await
    .unwrap();
    assert_eq!(aged.rows_affected(), 1, "unsettled worker row was deleted");
    assert_eq!(queue.reap(Duration::from_secs(300)).await.unwrap(), 1);
    assert_eq!(queue.reap(Duration::from_secs(300)).await.unwrap(), 0);
}

#[tokio::test]
async fn drain_timeout_preserves_reaper_recovery_without_waiting_for_job_expiry() {
    let Some(db) = Database::setup().await else {
        return;
    };
    let q = &db.queue;
    let mut running = Running::start(q, Duration::from_millis(40)).await;
    running.stop().await;
    assert_eq!(workers(q).await, 1, "the reaper needs the worker row");
    let before = job(q, running.ctx.id).await;
    assert_eq!(before.state, JobState::Executing);
    assert_eq!(before.attempt, 1);
    assert_eq!(before.interruptions, 0);
    assert_eq!(before.expire_seconds, 3600);
    reap_stopped(q, running.ctx.worker_id()).await;
    let recovered = job(q, running.ctx.id).await;
    assert_eq!(recovered.state, JobState::Retryable);
    assert_eq!(recovered.claimed_by, None);
    assert_eq!(recovered.failure_kind, Some(FailureKind::WorkerMissing));
    assert_eq!(recovered.interruptions, 1);
    assert_eq!(recovered.attempt, 1, "reaping does not refund the claim");
    let replacement = register_worker(q, &Queues::one("default")).await.unwrap();
    let claimed = q.claim("default", replacement, 1).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, running.ctx.id);
    assert_eq!(claimed[0].attempt, 2);
    assert!(q.complete(&claimed[0], replacement).await.unwrap().owned);
    db.close().await;
}

#[tokio::test]
async fn timeout_joins_old_handler_before_reclaim_and_fences_stale_completion() {
    let Some(db) = Database::setup().await else {
        return;
    };
    let q = &db.queue;
    let mut old = Running::start(q, Duration::from_millis(40)).await;
    old.stop().await;
    assert_eq!(old.active.load(Ordering::SeqCst), 0, "handler was detached");
    reap_stopped(q, old.ctx.worker_id()).await;

    let (started, mut receive) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let worker = Worker::new(q.clone(), config(Duration::from_secs(1))).register("wait", {
        let release = release.clone();
        let active = old.active.clone();
        move |ctx| {
            let release = release.clone();
            let active = active.clone();
            let started = started.clone();
            async move {
                assert_eq!(active.load(Ordering::SeqCst), 0, "owners overlap");
                started.send(ctx).unwrap();
                release.notified().await;
                Ok(())
            }
        }
    });
    let (shutdown, rx) = watch::channel(false);
    let task = tokio::spawn(worker.run(rx));
    let replacement = bounded(receive.recv()).await.unwrap();
    assert_eq!(replacement.id, old.ctx.id);
    assert_eq!(replacement.attempt, 2);
    assert_ne!(replacement.worker_id(), old.ctx.worker_id());

    // If the old task survived shutdown this lets it finish while the replacement
    // is still executing. Cancellation must have destroyed that future already.
    old.release.notify_one();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(old.completed.load(Ordering::SeqCst), 0);
    assert!(
        !q.complete(&old.ctx.job, old.ctx.worker_id())
            .await
            .unwrap()
            .owned
    );
    assert!(
        !old.ctx
            .checkpoint(&serde_json::json!({"stale": true}))
            .await
            .unwrap()
    );
    assert!(!old.ctx.touch().await.unwrap());
    assert!(
        !q.fail(
            &old.ctx.job,
            old.ctx.worker_id(),
            FailureKind::Raised,
            "stale failure",
            None,
        )
        .await
        .unwrap()
        .owned
    );
    let still_running = job(q, old.ctx.id).await;
    assert_eq!(still_running.state, JobState::Executing);
    assert_eq!(still_running.claimed_by, Some(replacement.worker_id()));
    assert_eq!(still_running.attempt, 2);
    shutdown.send(true).unwrap();
    release.notify_one();
    bounded(task).await.unwrap().unwrap();
    assert_eq!(job(q, old.ctx.id).await.state, JobState::Completed);
    db.close().await;
}

#[tokio::test]
async fn finite_drain_keeps_heartbeating_and_stops_new_admissions() {
    let Some(db) = Database::setup().await else {
        return;
    };
    let q = &db.queue;
    let mut running = Running::start(q, Duration::from_secs(2)).await;
    running.shutdown.send(true).unwrap();
    let waiting = q
        .enqueue(NewJob::new("default", "wait"))
        .await
        .unwrap()
        .unwrap();
    // Longer than the simulated reaper window: shutdown must not stop heartbeats.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(q.reap(Duration::from_millis(150)).await.unwrap(), 0);
    assert!(
        !running.task.is_finished(),
        "finite work was cancelled early"
    );
    running.release.notify_one();
    bounded(&mut running.task).await.unwrap().unwrap();
    let finished = job(q, running.ctx.id).await;
    assert_eq!(finished.state, JobState::Completed);
    assert_eq!(finished.attempt, 1);
    assert_eq!(finished.interruptions, 0);
    assert_eq!(running.completed.load(Ordering::SeqCst), 1);
    assert_eq!(running.active.load(Ordering::SeqCst), 0);
    assert_eq!(job(q, waiting).await.attempt, 0);
    assert_eq!(workers(q).await, 0, "fully settled workers deregister");
    db.close().await;
}

#[tokio::test]
async fn already_requested_shutdown_never_admits_work() {
    let Some(db) = Database::setup().await else {
        return;
    };
    let q = &db.queue;
    let id = q
        .enqueue(NewJob::new("default", "wait"))
        .await
        .unwrap()
        .unwrap();
    for closed in [false, true] {
        let (shutdown, rx) = watch::channel(!closed);
        if closed {
            drop(shutdown);
        }
        bounded(
            Worker::new(q.clone(), config(Duration::from_millis(40)))
                .register("wait", |_| async { panic!("admitted after shutdown") })
                .run(rx),
        )
        .await
        .unwrap();
        assert_eq!(job(q, id).await.attempt, 0);
        assert_eq!(workers(q).await, 0);
    }
    db.close().await;
}

#[tokio::test]
async fn failed_outcome_report_keeps_a_finished_tasks_claim_reapable() {
    let Some(db) = Database::setup().await else {
        return;
    };
    let q = &db.queue;
    // The handler finishes, but its completion cannot commit. A successful task
    // drain alone is not enough evidence to delete the worker registration.
    sqlx::raw_sql(
        "CREATE FUNCTION reject_completion() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.state = 'completed' THEN
                 RAISE EXCEPTION 'injected outcome reporting failure';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER reject_completion BEFORE UPDATE ON stampeded_jobs
         FOR EACH ROW EXECUTE FUNCTION reject_completion();",
    )
    .execute(q.pool())
    .await
    .unwrap();
    let mut running = Running::start(q, Duration::from_secs(1)).await;
    running.release.notify_one();
    running.stop().await;
    assert_eq!(running.active.load(Ordering::SeqCst), 0);
    assert_eq!(running.completed.load(Ordering::SeqCst), 1);
    assert_eq!(workers(q).await, 1);
    reap_stopped(q, running.ctx.worker_id()).await;
    assert_eq!(job(q, running.ctx.id).await.state, JobState::Retryable);
    db.close().await;
}

#[tokio::test]
async fn panicked_handler_keeps_its_claim_reapable() {
    let Some(db) = Database::setup().await else {
        return;
    };
    let q = &db.queue;
    q.enqueue(NewJob::new("default", "panic")).await.unwrap();
    let (started, mut receive) = mpsc::unbounded_channel();
    let worker =
        Worker::new(q.clone(), config(Duration::from_millis(40))).register("panic", move |ctx| {
            let started = started.clone();
            async move {
                started.send(ctx).unwrap();
                panic!("injected handler panic");
            }
        });
    let (shutdown, rx) = watch::channel(false);
    let task = tokio::spawn(worker.run(rx));
    let ctx = bounded(receive.recv()).await.unwrap();
    shutdown.send(true).unwrap();
    bounded(task).await.unwrap().unwrap();
    assert_eq!(workers(q).await, 1);
    reap_stopped(q, ctx.worker_id()).await;
    assert_eq!(job(q, ctx.id).await.state, JobState::Retryable);
    db.close().await;
}

#[tokio::test]
async fn shutdown_during_claim_does_not_admit_returned_jobs_or_orphan_them() {
    let Some(db) = Database::setup().await else {
        return;
    };
    let q = &db.queue;
    let id = q
        .enqueue(NewJob::new("default", "wait"))
        .await
        .unwrap()
        .unwrap();
    let mut blocker = q.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE stampeded_jobs IN SHARE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let admitted = Arc::new(AtomicUsize::new(0));
    let calls = admitted.clone();
    let worker =
        Worker::new(q.clone(), config(Duration::from_millis(40))).register("wait", move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        });
    let (shutdown, rx) = watch::channel(false);
    let task = tokio::spawn(worker.run(rx));
    bounded(async {
        loop {
            let claim_waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_stat_activity WHERE application_name = $1
                    AND query LIKE '%stampeded_claim(%' AND wait_event_type = 'Lock'
                )",
            )
            .bind(&db.schema)
            .fetch_one(q.pool())
            .await
            .unwrap();
            if claim_waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    shutdown.send(true).unwrap();
    blocker.commit().await.unwrap();
    bounded(task).await.unwrap().unwrap();
    assert_eq!(admitted.load(Ordering::SeqCst), 0);
    let claimed = job(q, id).await;
    assert_eq!(claimed.attempt, 1, "the in-flight claim must settle");
    assert_eq!(workers(q).await, 1, "unstarted claims still need reaping");
    reap_stopped(q, claimed.claimed_by.unwrap()).await;
    assert_eq!(job(q, id).await.state, JobState::Retryable);
    db.close().await;
}
