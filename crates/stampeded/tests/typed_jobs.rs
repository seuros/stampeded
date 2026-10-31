//! Typed jobs (`AsyncJob`) through a real worker, skipped without a database URL.
//!
//!   STAMPEDED_TEST_DATABASE_URL=postgres://ubuntu:ubuntu@localhost/stampeded_test \
//!     cargo test --test typed_jobs

mod common;

use common::db_now;
use jiff::SignedDuration;
use serde::{Deserialize, Serialize};
use stampeded::{
    AsyncJob, FailureKind, Job, JobContext, JobResult, JobState, NewJob, Queue, RetryIn, Worker,
    WorkerConfig,
};
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct Extract {
    name: String,
    version: String,
}

#[derive(Serialize)]
struct Extracted {
    symbols: i64,
}

impl AsyncJob for Extract {
    const TASK: &'static str = "crate.extract";
    type Output = Extracted;

    async fn run(&self, ctx: &JobContext) -> JobResult<Extracted> {
        match self.version.as_str() {
            "0.0.0-broken" => return Err("tarball is not a tarball".into()),
            "0.0.0-later" => return Err(Box::new(RetryIn::secs(3600))),
            _ => {}
        }
        sqlx::query("INSERT INTO extracted (name, version, job_id) VALUES ($1, $2, $3)")
            .bind(&self.name)
            .bind(&self.version)
            .bind(ctx.job.id)
            .execute(ctx.pool())
            .await?;
        Ok(Extracted { symbols: 1 })
    }
}

async fn setup() -> Option<(Queue, watch::Sender<bool>)> {
    let q = common::setup().await?;
    q.create_queue("default").await.expect("create queue");
    sqlx::query("CREATE TABLE extracted (name text, version text, job_id uuid)")
        .execute(q.pool())
        .await
        .expect("handler table");

    let worker = Worker::new(
        q.clone(),
        WorkerConfig {
            queues: stampeded::Queues::one("default"),
            poll_interval: Duration::from_millis(50),
            ..Default::default()
        },
    )
    .register_job::<Extract>();

    let (tx, rx) = watch::channel(false);
    tokio::spawn(worker.run(rx));
    Some((q, tx))
}

async fn settled(q: &Queue, id: Uuid, done: impl Fn(&Job) -> bool + Send + Sync) -> Job {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let job = q.fetch("default", id).await.unwrap().expect("job row");
            if done(&job) {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the worker should have reported an outcome")
}

async fn extract(q: &Queue, version: &str) -> Uuid {
    q.enqueue_job(Extract {
        name: "serde".into(),
        version: version.into(),
    })
    .await
    .unwrap()
    .expect("not deduplicated")
}

fn last_error_contains(job: &Job, needle: &str) -> bool {
    job.last_error
        .as_deref()
        .is_some_and(|e| e.contains(needle))
}

#[tokio::test]
async fn a_typed_job_runs_with_its_payload_and_the_pool() {
    let (q, stop) = q!(setup());
    let id = extract(&q, "1.0.228").await;

    let job = settled(&q, id, |j| j.state.is_terminal()).await;
    assert_eq!(job.state, JobState::Completed, "{:?}", job.last_error);
    assert_eq!(job.task, "crate.extract");
    assert_eq!(job.queue, "default");

    let row: (String, String, Uuid) = sqlx::query_as("SELECT name, version, job_id FROM extracted")
        .fetch_one(q.pool())
        .await
        .expect("the handler wrote through ctx.pool()");
    assert_eq!(row, ("serde".into(), "1.0.228".into(), id));
    let _ = stop.send(true);
}

#[tokio::test]
async fn a_failing_typed_job_is_retried_on_the_workers_backoff() {
    let (q, stop) = q!(setup());
    let id = extract(&q, "0.0.0-broken").await;

    let job = settled(&q, id, |j| j.failure_kind.is_some()).await;
    assert_eq!(job.state, JobState::Retryable, "retried, not discarded");
    assert_eq!(job.failure_kind, Some(FailureKind::Raised));
    assert_eq!(job.attempt, 1);
    assert!(
        last_error_contains(&job, "tarball is not a tarball"),
        "{:?}",
        job.last_error
    );
    assert_eq!(
        job.errors.as_array().map(Vec::len),
        Some(1),
        "{:?}",
        job.errors
    );
    assert!(job.finished_at.is_none(), "a retryable job is not finished");
    let _ = stop.send(true);
}

#[tokio::test]
async fn a_typed_job_can_pick_its_own_retry_delay() {
    let (q, stop) = q!(setup());
    let id = extract(&q, "0.0.0-later").await;

    let job = settled(&q, id, |j| j.failure_kind.is_some()).await;
    assert_eq!(job.state, JobState::Retryable);
    let now = db_now(&q).await;
    let wait = job.scheduled_at.duration_since(now);
    assert!(
        wait > SignedDuration::from_mins(59) && wait <= SignedDuration::from_mins(60),
        "RetryIn(3600s) must set the next attempt, got {wait:?}"
    );
    let _ = stop.send(true);
}

#[tokio::test]
async fn a_payload_that_no_longer_decodes_is_discarded_not_retried() {
    let (q, stop) = q!(setup());
    let id = q
        .enqueue(NewJob::new("default", Extract::TASK).args(serde_json::json!({"crate": "serde"})))
        .await
        .unwrap()
        .unwrap();

    let job = settled(&q, id, |j| j.state.is_terminal()).await;
    assert_eq!(job.state, JobState::Discarded);
    assert_eq!(job.failure_kind, Some(FailureKind::Unrecoverable));
    assert_eq!(job.attempt, 1, "discarded on the first attempt");
    assert!(
        last_error_contains(&job, "missing field"),
        "{:?}",
        job.last_error
    );
    let _ = stop.send(true);
}

#[tokio::test]
async fn new_job_can_be_customised_before_enqueueing() {
    let (q, stop) = q!(setup());
    let payload = Extract {
        name: "serde".into(),
        version: "1.0.228".into(),
    };
    let unique = || payload.new_job().unwrap().unique("serde@1.0.228");
    let first = q.enqueue(unique()).await.unwrap().expect("first");
    assert!(q.enqueue(unique()).await.unwrap().is_none(), "deduplicated");

    let job = settled(&q, first, |j| j.state.is_terminal()).await;
    assert_eq!(job.state, JobState::Completed, "{:?}", job.last_error);
    let _ = stop.send(true);
}
