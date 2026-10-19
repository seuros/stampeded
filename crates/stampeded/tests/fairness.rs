//! The Rust worker's per-tenant fairness, against a real Postgres 18.
//!
//!   STAMPEDED_TEST_DATABASE_URL=postgres://ubuntu:ubuntu@localhost/stampeded_test \
//!     cargo test --test fairness

mod common;

use stampeded::{FailureKind, Job, JobState, NewJob, Queue, TenantLimit, Worker, WorkerConfig};
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

async fn fetch(q: &Queue, id: Uuid) -> Job {
    q.fetch("default", id).await.expect("fetch").expect("row")
}

#[tokio::test]
async fn a_throttled_job_is_released_with_its_attempt_refunded() {
    let q = q!();
    let ids = [
        q.enqueue(NewJob::new("default", "note").tenant("acme"))
            .await
            .unwrap()
            .unwrap(),
        q.enqueue(NewJob::new("default", "note").tenant("acme"))
            .await
            .unwrap()
            .unwrap(),
    ];

    // One job per thousand seconds: the first offered runs, the other is throttled.
    let config = WorkerConfig {
        poll_interval: Duration::from_millis(50),
        tenant_limit: Some(TenantLimit {
            jobs_per_second: 0.001,
            burst: 0.0,
        }),
        ..Default::default()
    };
    let w = Worker::new(q.clone(), config).register("note", |_job| async { Ok(()) });

    let (tx, rx) = watch::channel(false);
    let run = tokio::spawn(w.run(rx));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let (ran, released) = loop {
        let (a, b) = (fetch(&q, ids[0]).await, fetch(&q, ids[1]).await);
        match (a.state, b.state) {
            (JobState::Completed, JobState::Retryable) => break (a, b),
            (JobState::Retryable, JobState::Completed) => break (b, a),
            _ if tokio::time::Instant::now() >= deadline => {
                panic!("not settled in ten seconds: {:?} / {:?}", a.state, b.state)
            }
            _ => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };
    tx.send(true).expect("worker still listening");
    run.await.expect("worker task").expect("worker run");

    assert_eq!(ran.failure_kind, None);
    assert_eq!(released.failure_kind, Some(FailureKind::Throttled));
    assert_eq!(
        released.attempt, 0,
        "throttling is ours, not the job's: the attempt is refunded"
    );
    assert_eq!(released.deferrals, 1);
    assert_eq!(
        released.claimed_by, None,
        "released, not left executing for the reaper"
    );

    // Deferred by the gate's retry-after, so it is not claimable again yet.
    let other = q
        .register_worker("h", 1, &["default".into()])
        .await
        .unwrap();
    assert!(
        q.claim("default", other, 1).await.unwrap().is_empty(),
        "released without its delay"
    );
}
