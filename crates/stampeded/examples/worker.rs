//! A worker with real handlers.
//!
//!   createdb stampeded_demo
//!   STAMPEDED_DATABASE_URL=postgres://localhost/stampeded_demo \
//!     cargo run -p stampeded --example worker
//!
//! Then enqueue with plain SQL:
//!
//!   INSERT INTO stampeded_jobs (queue, task, args)
//!   VALUES ('default', 'greet', '{"name":"world"}'::jsonb);

use serde::{Deserialize, Serialize};
use stampeded::{AsyncJob, JobContext, JobResult, NewJob, Queue, Worker, WorkerConfig};
use std::time::Duration;
use tokio::sync::watch;

#[derive(Serialize, Deserialize)]
struct Extract {
    name: String,
    version: String,
}

impl AsyncJob for Extract {
    const TASK: &'static str = "crate.extract";
    type Output = u64;

    async fn run(&self, ctx: &JobContext) -> JobResult<u64> {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM stampeded_jobs WHERE task = $1")
            .bind(Self::TASK)
            .fetch_one(ctx.pool())
            .await?;
        println!(
            "extracting {}@{} (attempt {})",
            self.name, self.version, ctx.attempt
        );
        Ok(n.unsigned_abs())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("stampeded=debug,worker=info")
        .init();

    let url = std::env::var("STAMPEDED_DATABASE_URL").map_err(|_| "set STAMPEDED_DATABASE_URL")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    stampeded::schema::migrate(&pool).await?;

    let queue = Queue::new(pool);
    queue.create_queue("default").await?;

    // Seed jobs: one always fails (retries), one is enqueued twice (dedup).
    queue
        .enqueue(NewJob::new("default", "greet").args(serde_json::json!({"name": "world"})))
        .await?;
    queue.enqueue(NewJob::new("default", "slow")).await?;
    queue
        .enqueue(NewJob::new("default", "flaky").max_attempts(3))
        .await?;
    queue
        .enqueue(NewJob::new("default", "greet").unique("only-once"))
        .await?;
    let dup = queue
        .enqueue(NewJob::new("default", "greet").unique("only-once"))
        .await?;
    println!("duplicate enqueue returned {dup:?} (None means deduplicated)");
    queue
        .enqueue_job(Extract {
            name: "serde".into(),
            version: "1.0.228".into(),
        })
        .await?;

    let worker = Worker::new(
        queue,
        WorkerConfig {
            queues: stampeded::Queues::one("default"),
            max_concurrency: 4,
            job_timeout: Duration::from_secs(5),
            ..Default::default()
        },
    )
    .register("greet", |job| async move {
        let name = job
            .args
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("nobody");
        println!("hello, {name}!");
        Ok(())
    })
    .register("slow", |_job| async move {
        // Finishes inside the 5s timeout.
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("slow job done");
        Ok(())
    })
    .register("flaky", |job| async move {
        // Retried with full-jitter backoff, then discarded at max_attempts.
        println!("flaky attempt {}", job.attempt);
        Err("this task always fails".into())
    })
    .register_job::<Extract>();

    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            eprintln!("cannot listen for ctrl-c ({e}); stopping now");
        }
        // Err only once the worker has already stopped.
        let _ = tx.send(true);
    });

    println!("worker running -- ctrl-c to stop");
    worker.run(rx).await?;
    Ok(())
}
