//! The Rust worker's queue selection and the pull API, against a real Postgres 18.
//!
//!   STAMPEDED_TEST_DATABASE_URL=postgres://ubuntu:ubuntu@localhost/stampeded_test \
//!     cargo test --test parity

#![expect(
    clippy::significant_drop_tightening,
    reason = "tests hold leases and locks for exactly the step they assert on"
)]

mod common;

use stampeded::{JobState, NewJob, PullConfig, Puller, Queue, Queues, Worker, WorkerConfig};
use std::assert_matches;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::watch;

fn worker(queues: Queues) -> WorkerConfig {
    WorkerConfig {
        queues,
        poll_interval: Duration::from_millis(50),
        tenant_limit: None,
        ..Default::default()
    }
}

/// Run `worker` until `done` holds or ten seconds pass, then drain it.
async fn run_until(worker: Worker, done: impl Fn() -> bool) {
    let (tx, rx) = watch::channel(false);
    let run = tokio::spawn(worker.run(rx));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !done() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tx.send(true).expect("worker still listening");
    run.await.expect("worker task").expect("worker run");
    assert!(done(), "the worker did not get there in ten seconds");
}

fn recorder() -> (Arc<Mutex<Vec<String>>>, impl Fn() -> usize + Clone) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let count = {
        let seen = seen.clone();
        move || seen.lock().expect("seen").len()
    };
    (seen, count)
}

async fn state(q: &Queue, queue: &str, id: uuid::Uuid) -> JobState {
    q.fetch(queue, id).await.expect("fetch").expect("row").state
}

async fn policy(q: &Queue, queue: &str, max_running: Option<i32>, verdict: &str) {
    sqlx::query(
        "INSERT INTO stampeded_queue_policies (queue, max_running, verdict, decided_at) \
         VALUES ($1, $2, $3, now())",
    )
    .bind(queue)
    .bind(max_running)
    .bind(verdict)
    .execute(q.pool())
    .await
    .expect("policy");
}

#[tokio::test]
async fn one_worker_serves_several_queues() {
    let q = q!();
    let a = q.enqueue(NewJob::new("a", "note")).await.unwrap().unwrap();
    let b = q.enqueue(NewJob::new("b", "note")).await.unwrap().unwrap();

    let (seen, count) = recorder();
    let w = Worker::new(q.clone(), worker(Queues::parse("a,b").unwrap())).register(
        "note",
        move |job| {
            let seen = seen.clone();
            async move {
                seen.lock().expect("seen").push(job.queue.clone());
                Ok(())
            }
        },
    );
    run_until(w, move || count() == 2).await;

    assert_eq!(state(&q, "a", a).await, JobState::Completed);
    assert_eq!(state(&q, "b", b).await, JobState::Completed);
}

#[tokio::test]
async fn every_queue_means_every_queue_with_work_except_the_excluded() {
    let q = q!();
    let served = q
        .enqueue(NewJob::new("anything", "note"))
        .await
        .unwrap()
        .unwrap();
    let parked = q
        .enqueue(NewJob::new("dlq", "note"))
        .await
        .unwrap()
        .unwrap();

    let (seen, count) = recorder();
    let queues = Queues::parse("*").unwrap().except("dlq").unwrap();
    let w = Worker::new(q.clone(), worker(queues)).register("note", move |job| {
        let seen = seen.clone();
        async move {
            seen.lock().expect("seen").push(job.queue.clone());
            Ok(())
        }
    });
    run_until(w, move || count() == 1).await;

    assert_eq!(state(&q, "anything", served).await, JobState::Completed);
    assert_eq!(state(&q, "dlq", parked).await, JobState::Available);
}

#[tokio::test]
async fn a_paused_queue_waits_with_no_daemon_and_starves_nothing() {
    // No planner runs here, so no verdict: the claim alone keeps `frozen` waiting.
    let q = q!();
    q.pause_queue("frozen").await.unwrap();
    let frozen = q
        .enqueue(NewJob::new("frozen", "note"))
        .await
        .unwrap()
        .unwrap();
    for _ in 0..2 {
        q.enqueue(NewJob::new("live", "note")).await.unwrap();
    }

    let (seen, count) = recorder();
    let make = || {
        let seen = seen.clone();
        Worker::new(q.clone(), worker(Queues::parse("*").unwrap())).register("note", move |job| {
            let seen = seen.clone();
            async move {
                seen.lock().expect("seen").push(job.queue.clone());
                Ok(())
            }
        })
    };
    let after_live = count.clone();
    run_until(make(), move || after_live() == 2).await;
    assert_eq!(state(&q, "frozen", frozen).await, JobState::Available);

    q.resume_queue("frozen").await.unwrap();
    run_until(make(), move || count() == 3).await;
    assert_eq!(state(&q, "frozen", frozen).await, JobState::Completed);
}

#[tokio::test]
async fn a_held_queue_waits_and_a_rushed_one_goes_first() {
    let q = q!();
    policy(&q, "held", None, "hold").await;
    policy(&q, "late", None, "rush").await;
    let held = q
        .enqueue(NewJob::new("held", "note"))
        .await
        .unwrap()
        .unwrap();
    for _ in 0..3 {
        q.enqueue(NewJob::new("bulk", "note")).await.unwrap();
    }
    q.enqueue(NewJob::new("late", "note")).await.unwrap();

    let (seen, count) = recorder();
    let order = seen.clone();
    let config = WorkerConfig {
        max_concurrency: 1,
        batch_size: 1,
        ..worker(Queues::parse("bulk,late,held").unwrap())
    };
    let w = Worker::new(q.clone(), config).register("note", move |job| {
        let seen = seen.clone();
        async move {
            seen.lock().expect("seen").push(job.queue.clone());
            Ok(())
        }
    });
    run_until(w, move || count() == 4).await;

    assert_eq!(
        order.lock().expect("order").first().map(String::as_str),
        Some("late")
    );
    assert_eq!(state(&q, "held", held).await, JobState::Available);
}

#[tokio::test]
async fn max_running_caps_this_process_with_no_daemon_planning() {
    let q = q!();
    // No verdict at all: only the cap, read from the row.
    sqlx::query("INSERT INTO stampeded_queue_policies (queue, max_running) VALUES ('one', 1)")
        .execute(q.pool())
        .await
        .unwrap();
    for _ in 0..4 {
        q.enqueue(NewJob::new("one", "slow")).await.unwrap();
    }

    let running = Arc::new(Mutex::new((0usize, 0usize, 0usize))); // now, peak, done
    let probe = running.clone();
    let config = WorkerConfig {
        max_concurrency: 4,
        ..worker(Queues::one("one"))
    };
    let w = Worker::new(q.clone(), config).register("slow", move |_job| {
        let running = running.clone();
        async move {
            {
                let mut r = running.lock().expect("running");
                r.0 += 1;
                r.1 = r.1.max(r.0);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut r = running.lock().expect("running");
            r.0 -= 1;
            r.2 += 1;
            Ok(())
        }
    });
    let done = probe.clone();
    run_until(w, move || done.lock().expect("done").2 == 4).await;

    assert_eq!(
        probe.lock().expect("peak").1,
        1,
        "ran more than max_running at once"
    );
}

fn puller(queues: Queues) -> PullConfig {
    PullConfig {
        queues,
        poll_interval: Duration::from_millis(20),
        tenant_limit: None,
        ..Default::default()
    }
}

#[tokio::test]
async fn a_pulled_job_is_completed_failed_or_discarded_by_the_caller() {
    let q = q!();
    let done = q
        .enqueue(NewJob::new("default", "t").args(serde_json::json!({ "n": 1 })))
        .await
        .unwrap()
        .unwrap();
    let retried = q
        .enqueue(NewJob::new("default", "t"))
        .await
        .unwrap()
        .unwrap();
    let dead = q
        .enqueue(NewJob::new("default", "t"))
        .await
        .unwrap()
        .unwrap();
    let p = Puller::start(q.clone(), puller(Queues::one("default")))
        .await
        .unwrap();

    let mut leases = Vec::new();
    for _ in 0..3 {
        leases.push(
            p.next(Duration::from_secs(2))
                .await
                .unwrap()
                .expect("a lease"),
        );
    }
    let mut take = |id| {
        let at = leases
            .iter()
            .position(|l: &stampeded::Lease| l.job().id == id)
            .expect("leased");
        leases.remove(at)
    };
    let lease_dead = take(dead);
    let lease_retried = take(retried);
    let lease_done = take(done);

    assert_eq!(lease_done.job().args, serde_json::json!({ "n": 1 }));
    lease_done.complete().await.unwrap();
    lease_retried
        .fail("downstream said no", Some(Duration::from_secs(600)))
        .await
        .unwrap();
    lease_dead.discard("will never work").await.unwrap();

    assert_eq!(state(&q, "default", done).await, JobState::Completed);
    let row = q.fetch("default", retried).await.unwrap().unwrap();
    assert_eq!(row.state, JobState::Retryable);
    assert_eq!(row.last_error.as_deref(), Some("downstream said no"));
    let delay = row.scheduled_at.duration_since(common::db_now(&q).await);
    assert!(
        delay.as_secs() > 590,
        "the 600s retry delay did not reach the database: {delay:?}"
    );
    assert!(
        delay.as_secs() <= 600,
        "the 600s retry delay was not applied as given: {delay:?}"
    );
    let row = q.fetch("default", dead).await.unwrap().unwrap();
    assert_eq!(row.state, JobState::Discarded);

    p.stop(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn a_lease_dropped_without_an_outcome_goes_back_as_an_interruption() {
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "t"))
        .await
        .unwrap()
        .unwrap();
    let p = Puller::start(q.clone(), puller(Queues::one("default")))
        .await
        .unwrap();

    drop(
        p.next(Duration::from_secs(2))
            .await
            .unwrap()
            .expect("a lease"),
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let row = loop {
        let row = q.fetch("default", id).await.unwrap().unwrap();
        if row.state != JobState::Executing || tokio::time::Instant::now() > deadline {
            break row;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(row.state, JobState::Retryable);
    assert_eq!(row.interruptions, 1);
    assert_eq!(
        row.failure_kind.map(stampeded::FailureKind::as_str),
        Some("heartbeat_lost")
    );

    p.stop(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn no_more_leases_than_max_leases_are_out_at_once() {
    let q = q!();
    for _ in 0..2 {
        q.enqueue(NewJob::new("default", "t")).await.unwrap();
    }
    let config = PullConfig {
        max_leases: 1,
        ..puller(Queues::one("default"))
    };
    let p = Puller::start(q.clone(), config).await.unwrap();

    let first = p
        .next(Duration::from_secs(2))
        .await
        .unwrap()
        .expect("a lease");
    assert!(
        p.next(Duration::from_millis(200)).await.unwrap().is_none(),
        "a second lease while one is out"
    );

    first.complete().await.unwrap();
    let second = p
        .next(Duration::from_secs(2))
        .await
        .unwrap()
        .expect("a lease once the first settled");
    second.complete().await.unwrap();

    p.stop(Duration::from_secs(1)).await.unwrap();
}

/// After a stop: (jobs waiting, attempts charged to unfinished jobs, workers registered).
async fn after_stop(q: &Queue) -> (i64, i64, i64) {
    sqlx::query_as(
        "SELECT count(*) FILTER (WHERE state IN ('available', 'retryable')), \
                coalesce(sum(attempt) FILTER (WHERE state <> 'completed'), 0), \
                (SELECT count(*) FROM stampeded_workers) \
           FROM stampeded_jobs",
    )
    .fetch_one(q.pool())
    .await
    .unwrap()
}

#[tokio::test]
async fn stopping_gives_unstarted_jobs_back_and_deregisters() {
    let q = q!();
    for _ in 0..3 {
        q.enqueue(NewJob::new("default", "t")).await.unwrap();
    }
    // One round claims all three; one is handed out, two wait.
    let p = Puller::start(q.clone(), puller(Queues::one("default")))
        .await
        .unwrap();
    let lease = p
        .next(Duration::from_secs(2))
        .await
        .unwrap()
        .expect("a lease");
    lease.complete().await.unwrap();

    p.stop(Duration::from_secs(1)).await.unwrap();

    let (waiting, attempts, workers) = after_stop(&q).await;
    assert_eq!(waiting, 2, "unstarted jobs go back to the queue");
    assert_eq!(attempts, 0, "and keep their attempt");
    assert_eq!(workers, 0);
}

#[tokio::test]
async fn stopping_mid_claim_gives_that_claim_back_and_claims_nothing_more() {
    let q = q!();
    for _ in 0..12 {
        q.enqueue(NewJob::new("default", "t")).await.unwrap();
    }
    let config = PullConfig {
        max_leases: 10,
        batch_size: 5,
        ..puller(Queues::one("default"))
    };
    let p = Arc::new(Puller::start(q.clone(), config).await.unwrap());

    // Claims block here: one in the database, one holding slots, one polling.
    let mut lock = q.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE stampeded_jobs IN EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let holder: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let drain = Duration::from_secs(20);
    let pulling: Vec<_> = (0..3)
        .map(|_| {
            let p = p.clone();
            // Outlives the drain: slots freed only by timing out would make stop wait it out.
            tokio::spawn(async move { p.next(drain * 2).await })
        })
        .collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        // From the pool: a transaction keeps one pg_stat_activity snapshot.
        let blocked: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
              WHERE $1 = ANY(pg_blocking_pids(pid)) AND query LIKE '%stampeded_claim(%'",
        )
        .bind(holder)
        .fetch_one(q.pool())
        .await
        .unwrap();
        if blocked > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no claim reached the database"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let stop = p.stop(drain);
    tokio::pin!(stop);
    // One poll sets `stopping`, then parks on the claimer lock the blocked claim holds.
    let polled = std::future::poll_fn(|cx| Poll::Ready(stop.as_mut().poll(cx))).await;
    assert_matches!(
        polled,
        Poll::Pending,
        "stop finished with a claim still in flight"
    );
    lock.commit().await.unwrap();

    let started = tokio::time::Instant::now();
    stop.await.unwrap();
    let took = started.elapsed();
    for next in pulling {
        assert!(
            next.await.unwrap().unwrap().is_none(),
            "a lease handed out once stopping"
        );
    }
    // The drain's timer starts after `started`, so waiting it out takes at least `drain`.
    assert!(
        took < drain,
        "stop waited out its drain on leases nobody holds: {took:?}"
    );
    let (waiting, attempts, workers) = after_stop(&q).await;
    assert_eq!(
        waiting, 12,
        "the claim that landed after stop started goes back too"
    );
    assert_eq!(attempts, 0, "and keeps its attempt");
    assert_eq!(workers, 0);
    let given_back: i64 =
        sqlx::query_scalar("SELECT count(*) FROM stampeded_jobs WHERE attempted_by <> '{}'")
            .fetch_one(q.pool())
            .await
            .unwrap();
    assert_eq!(
        given_back, 5,
        "only the round in flight was claimed; none started once stopping"
    );
}

#[tokio::test]
async fn a_pulled_job_checkpoints_like_an_in_process_one() {
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "t"))
        .await
        .unwrap()
        .unwrap();
    let p = Puller::start(q.clone(), puller(Queues::one("default")))
        .await
        .unwrap();

    let lease = p
        .next(Duration::from_secs(2))
        .await
        .unwrap()
        .expect("a lease");
    assert!(
        lease
            .context()
            .checkpoint(&serde_json::json!({ "cursor": 50 }))
            .await
            .unwrap()
    );
    lease.complete().await.unwrap();

    let row = q.fetch("default", id).await.unwrap().unwrap();
    assert_eq!(row.checkpoint_seq, 1);

    p.stop(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn the_puller_honours_verdicts_too() {
    let q = q!();
    policy(&q, "held", None, "hold").await;
    q.enqueue(NewJob::new("held", "t")).await.unwrap();
    let p = Puller::start(q.clone(), puller(Queues::one("held")))
        .await
        .unwrap();

    assert!(p.next(Duration::from_millis(300)).await.unwrap().is_none());

    p.stop(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn a_puller_dropped_without_stop_stops_heartbeating() {
    let q = q!();
    let config = PullConfig {
        heartbeat_interval: Duration::from_millis(20),
        ..puller(Queues::one("default"))
    };
    let p = Puller::start(q.clone(), config).await.unwrap();
    let id = p.worker_id();
    drop(p);

    let beat = || async {
        sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
            "SELECT last_heartbeat FROM stampeded_workers WHERE id = $1",
        )
        .bind(id)
        .fetch_one(q.pool())
        .await
        .unwrap()
        .to_jiff()
    };
    tokio::time::sleep(Duration::from_millis(60)).await;
    let before = beat().await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(beat().await, before, "the heartbeat outlived the puller");
}

#[tokio::test]
async fn a_job_given_back_on_stop_is_requeued_not_failed() {
    let q = q!();
    let mut failed = Vec::new();
    for _ in 0..2 {
        failed.push(
            q.enqueue(NewJob::new("default", "t"))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let p = Puller::start(q.clone(), puller(Queues::one("default")))
        .await
        .unwrap();
    for _ in &failed {
        p.next(Duration::from_secs(2))
            .await
            .unwrap()
            .expect("a lease")
            .fail("bad input", Some(Duration::ZERO))
            .await
            .unwrap();
    }
    let mut fresh = Vec::new();
    for _ in 0..2 {
        fresh.push(
            q.enqueue(NewJob::new("default", "t"))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let mut before = std::collections::HashMap::new();
    for &id in failed.iter().chain(&fresh) {
        before.insert(id, q.fetch("default", id).await.unwrap().unwrap());
    }

    // One round claims all four; one is handed out, three wait and are given back.
    let lease = p
        .next(Duration::from_secs(2))
        .await
        .unwrap()
        .expect("a lease");
    let ran = lease.job().id;
    lease.complete().await.unwrap();
    p.stop(Duration::from_secs(1)).await.unwrap();

    let given_back = failed
        .iter()
        .map(|id| (*id, true))
        .chain(fresh.iter().map(|id| (*id, false)))
        .filter(|(id, _)| *id != ran);
    for (id, had_failed) in given_back {
        let was = &before[&id];
        let row = q.fetch("default", id).await.unwrap().unwrap();
        let expected_state = if had_failed {
            JobState::Retryable
        } else {
            JobState::Available
        };
        assert_eq!(row.state, expected_state, "{id}: back to where it was");
        assert_eq!(
            row.failure_kind, was.failure_kind,
            "{id}: stopping is not a failure of the job"
        );
        assert_eq!(row.last_error, was.last_error, "{id}");
        assert_eq!(row.errors, was.errors, "{id}: nothing added to its history");
        assert_eq!(row.error_fingerprint, was.error_fingerprint, "{id}");
        assert_eq!(
            row.attempt, was.attempt,
            "{id}: the claim's attempt refunded"
        );
        assert_eq!(row.deferrals, 0, "{id}: not charged as backpressure");
        assert_eq!(
            row.scheduled_at, was.scheduled_at,
            "{id}: keeps its place in line"
        );
        assert_eq!(row.claimed_by, None, "{id}");
        assert_eq!(row.claimed_at, None, "{id}");
        assert_eq!(row.heartbeat_at, None, "{id}");
    }
}

#[tokio::test]
async fn a_requeued_lease_runs_again_from_its_checkpoint_with_its_attempt_refunded() {
    let q = q!();
    let id = q
        .enqueue(NewJob::new("default", "t"))
        .await
        .unwrap()
        .unwrap();
    let p = Puller::start(q.clone(), puller(Queues::one("default")))
        .await
        .unwrap();

    let lease = p
        .next(Duration::from_secs(2))
        .await
        .unwrap()
        .expect("a lease");
    lease
        .context()
        .checkpoint(&serde_json::json!({ "cursor": 7 }))
        .await
        .unwrap();
    lease.requeue().await.unwrap();

    let row = q.fetch("default", id).await.unwrap().unwrap();
    assert_eq!(row.state, JobState::Available);
    assert_eq!(row.attempt, 0, "a requeue is not an attempt");
    assert_eq!(row.failure_kind, None);
    assert_eq!(row.claimed_by, None);

    let again = p
        .next(Duration::from_secs(2))
        .await
        .unwrap()
        .expect("the same job, again");
    assert_eq!(again.job().id, id);
    assert_eq!(again.job().attempt, 1);
    assert_eq!(
        again.job().checkpoint,
        Some(serde_json::json!({ "cursor": 7 }))
    );
    again.complete().await.unwrap();
    p.stop(Duration::from_secs(1)).await.unwrap();
}
