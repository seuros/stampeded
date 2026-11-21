//! A real worker and puller on PostgreSQL, the host probe replaced by a script.

use super::{DiskReading, DiskVital, FreeSpace, GIB, Probe, Readings, VitalsConfig};
use crate::error::Error;
use crate::job::{JobState, NewJob};
use crate::pull::{PullConfig, Puller};
use crate::queue::Queue;
use crate::queues::Queues;
use crate::test_support::Database;
use crate::worker::{Worker, WorkerConfig};
use std::assert_matches;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc, watch};
use uuid::Uuid;

const HEADROOM: Duration = Duration::from_millis(300);

/// Free space the script reports, in GiB, on a 100 GiB disk; under 10 stops claiming.
#[derive(Clone)]
struct Script(Arc<AtomicU64>);

impl Script {
    fn new(gib: u64) -> Self {
        Self(Arc::new(AtomicU64::new(gib)))
    }

    fn set(&self, gib: u64) {
        self.0.store(gib, Ordering::SeqCst);
    }

    fn config(&self) -> VitalsConfig {
        let free = Arc::clone(&self.0);
        VitalsConfig::default()
            .disk(DiskVital::new("/scripted", FreeSpace::Bytes(10 * GIB)))
            .headroom_for(HEADROOM)
            .probe_interval(Duration::from_millis(25))
            .probe(Probe::custom(move |_| Readings {
                disks: vec![DiskReading::Space {
                    available: free.load(Ordering::SeqCst) * GIB,
                    total: 100 * GIB,
                }],
                thermal: None,
            }))
    }
}

/// The `vitals` key of the only worker row.
async fn row_vitals(queue: &Queue) -> serde_json::Value {
    sqlx::query_scalar("SELECT metadata->'vitals' FROM stampeded_workers")
        .fetch_optional(queue.pool())
        .await
        .unwrap()
        .unwrap_or_default()
}

async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Microseconds since the epoch, on the database's clock.
async fn last_heartbeat(queue: &Queue) -> i64 {
    sqlx::query_scalar(
        "SELECT (extract(epoch FROM last_heartbeat) * 1000000)::int8 FROM stampeded_workers",
    )
    .fetch_one(queue.pool())
    .await
    .unwrap()
}

async fn state(queue: &Queue, id: Uuid) -> JobState {
    queue.fetch("default", id).await.unwrap().unwrap().state
}

#[tokio::test]
async fn a_worker_keeps_its_running_job_and_claims_nothing_new_until_headroom_holds() {
    let Some(db) = Database::setup().await else {
        return;
    };
    let queue = db.queue.clone();
    let script = Script::new(50);

    let held = queue
        .enqueue(NewJob::new("default", "wait"))
        .await
        .unwrap()
        .unwrap();

    let (started_tx, mut started) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let gate = Arc::clone(&release);
    let worker = Worker::new(
        queue.clone(),
        WorkerConfig {
            queues: Queues::one("default"),
            poll_interval: Duration::from_millis(5),
            heartbeat_interval: Duration::from_millis(20),
            max_concurrency: 2,
            tenant_limit: None,
            breaker_failure_threshold: None,
            vitals: Some(script.config()),
            ..WorkerConfig::default()
        },
    )
    .register("wait", move |ctx| {
        let started = started_tx.clone();
        let release = Arc::clone(&gate);
        async move {
            started.send(ctx.job.id).ok();
            release.notified().await;
            Ok(())
        }
    })
    .register("quick", |_| async { Ok(()) });
    let (shutdown, rx) = watch::channel(false);
    let running = tokio::spawn(worker.run(rx));

    assert_eq!(started.recv().await, Some(held));

    script.set(5);
    eventually("the row says claiming paused", async || {
        row_vitals(&queue).await["admitting"] == false
    })
    .await;
    let vitals = row_vitals(&queue).await;
    assert_eq!(vitals["phase"], "warning");
    assert_eq!(vitals["vital"], "disk:/scripted");
    assert!(vitals["since"].is_string(), "{vitals}");

    let waiting = queue
        .enqueue(NewJob::new("default", "quick"))
        .await
        .unwrap()
        .unwrap();
    // The row is written only once no claim is in flight, so from here nothing is claimed.
    // Let the paused worker go round its 5 ms poll loop through three 20 ms heartbeats.
    for beat in 0..3 {
        let before = last_heartbeat(&queue).await;
        eventually(&format!("heartbeat {beat} while paused"), async || {
            last_heartbeat(&queue).await > before
        })
        .await;
        assert_eq!(
            state(&queue, waiting).await,
            JobState::Available,
            "claimed while paused"
        );
    }
    assert_eq!(
        state(&queue, held).await,
        JobState::Executing,
        "the held job was dropped"
    );

    // The held job finishes and reports while claiming is still paused.
    release.notify_one();
    eventually("the held job completes", async || {
        state(&queue, held).await == JobState::Completed
    })
    .await;
    assert_eq!(state(&queue, waiting).await, JobState::Available);

    let cleared = Instant::now();
    script.set(50);
    eventually("the row says stabilizing", async || {
        row_vitals(&queue).await["phase"] == "stabilizing"
    })
    .await;
    eventually("the waiting job runs", async || {
        state(&queue, waiting).await == JobState::Completed
    })
    .await;
    assert!(
        cleared.elapsed() >= HEADROOM,
        "claimed {:?} after the disk cleared, before headroom held {HEADROOM:?}",
        cleared.elapsed()
    );
    // On resume the gate opens before the row is written, so the row may trail the claim.
    eventually("the row says recovered", async || {
        row_vitals(&queue).await["phase"] == "recovered"
    })
    .await;

    shutdown.send(true).unwrap();
    running.await.unwrap().unwrap();
    db.close().await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "the lease is moved into complete(), not held to the end"
)]
async fn a_puller_started_short_claims_nothing_until_headroom_holds() {
    let Some(db) = Database::setup().await else {
        return;
    };
    let queue = db.queue.clone();
    let script = Script::new(5);
    let job = queue
        .enqueue(NewJob::new("default", "t"))
        .await
        .unwrap()
        .unwrap();

    let puller = Puller::start(
        queue.clone(),
        PullConfig {
            queues: Queues::one("default"),
            poll_interval: Duration::from_millis(5),
            tenant_limit: None,
            vitals: Some(script.config()),
            ..PullConfig::default()
        },
    )
    .await
    .unwrap();

    // The first probe ran before start returned: nothing is claimed, not even once.
    assert!(
        puller
            .next(Duration::from_millis(150))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(state(&queue, job).await, JobState::Available);
    let status = puller.vitals().expect("vitals are watched");
    assert!(!status.admitting);
    assert!(
        status
            .reason
            .as_deref()
            .is_some_and(|r| r.contains("under the 10.0 GiB minimum")),
        "{status:?}"
    );

    let cleared = Instant::now();
    script.set(50);
    let lease = puller
        .next(Duration::from_secs(5))
        .await
        .unwrap()
        .expect("claimed once headroom held");
    let elapsed = cleared.elapsed();
    let leased = lease.job().id;
    lease.complete().await.unwrap();
    assert!(elapsed >= HEADROOM, "{elapsed:?}");
    assert_eq!(leased, job);

    puller.stop(Duration::from_secs(1)).await.unwrap();
    db.close().await;
}

#[tokio::test]
async fn an_unusable_vitals_config_fails_before_the_worker_registers() {
    let Some(db) = Database::setup().await else {
        return;
    };
    let config = PullConfig {
        vitals: Some(VitalsConfig::default().disk(DiskVital::new("relative", FreeSpace::Bytes(1)))),
        ..PullConfig::default()
    };
    let started = Puller::start(db.queue.clone(), config).await;
    assert_matches!(started.err(), Some(Error::InvalidConfig(_)));

    let workers: i64 = sqlx::query_scalar("SELECT count(*) FROM stampeded_workers")
        .fetch_one(db.queue.pool())
        .await
        .unwrap();
    assert_eq!(workers, 0);
    db.close().await;
}

#[tokio::test]
async fn a_pause_waits_for_a_claim_already_past_the_gate_before_it_is_reported() {
    let Some(db) = Database::setup().await else {
        return;
    };
    let queue = db.queue.clone();
    let worker = queue
        .register_worker("test", 1, &["default".into()])
        .await
        .unwrap();
    let script = Script::new(50);
    let gate = super::VitalsWatch::start(
        script.config(),
        queue.clone(),
        worker,
        crate::metrics::Metrics::noop(),
    )
    .await
    .unwrap();

    let gate = Arc::new(gate);

    // A claim round in flight: past the check, its claim query not yet returned.
    let in_flight = gate.claim_permit().await.expect("claiming");
    script.set(5);
    eventually("the gate turns paused", async || !gate.admitting()).await;

    // A claim asking now is refused, at once or once it has queued behind the pause.
    let next = tokio::spawn({
        let gate = Arc::clone(&gate);
        async move { gate.claim_permit().await.is_some() }
    });
    // Three probe intervals: the pause is not reported while the round is out.
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            row_vitals(&queue).await["admitting"],
            true,
            "the pause was reported with a claim in flight"
        );
    }

    drop(in_flight);
    assert!(!next.await.unwrap(), "a claim started after the pause");
    eventually("the pause is reported", async || {
        row_vitals(&queue).await["admitting"] == false
    })
    .await;
    drop(gate);
    db.close().await;
}
