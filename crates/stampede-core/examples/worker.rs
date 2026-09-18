//! A worker with real handlers.
//!
//!   createdb stampede_demo
//!   STAMPEDE_DATABASE_URL=postgres://localhost/stampede_demo \
//!     cargo run -p stampede-core --example worker
//!
//! Then enqueue from anything that speaks Postgres — no client library:
//!
//!   INSERT INTO stampede_jobs (queue, task, args)
//!   VALUES ('default', 'greet', '{"name":"world"}'::jsonb);

use stampede_core::{NewJob, Queue, Worker, WorkerConfig};
use std::time::Duration;
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("stampede_core=debug,worker=info")
        .init();

    let url = std::env::var("STAMPEDE_DATABASE_URL").map_err(|_| "set STAMPEDE_DATABASE_URL")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    stampede_core::schema::migrate(&pool).await?;

    let queue = Queue::new(pool);
    queue.create_queue("default").await?;

    // Seed a few jobs, including one that always fails so the retry path is
    // visible, and a deduplicated one enqueued twice.
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

    let worker = Worker::new(
        queue,
        WorkerConfig {
            queue: "default".into(),
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
        // Fails every time: watch the attempt counter climb with full-jitter
        // backoff between tries, then get discarded at max_attempts.
        println!("flaky attempt {}", job.attempt);
        Err("this task always fails".into())
    });

    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        let _ = tx.send(true);
    });

    println!("worker running — ctrl-c to stop");
    worker.run(rx).await?;
    Ok(())
}
