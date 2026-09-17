//! The Stampede daemon.
//!
//! One binary, roles behind flags (plans/README §4). Run it with every role for
//! a single-node deployment, or with different flags per process to split
//! scheduler from executor later — without shipping a second binary.
//!
//!   stampede --database-url postgres://... --queue default
//!   stampede --no-worker                     # maintenance only
//!   stampede --queue mail --concurrency 50

mod config;

use config::Config;
use stampede_core::{Maintenance, MaintenanceConfig, Queue, Worker, WorkerConfig};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stampede=info,stampede_core=info".into()),
        )
        .init();

    let config = match Config::from_args_and_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}\n\n{}", Config::usage());
            std::process::exit(2);
        }
    };

    let pool = sqlx::postgres::PgPoolOptions::new()
        // Sized against the worker's concurrency, not arbitrarily. underway
        // pins a connection per in-flight job and exhausts a stock
        // max_connections=100 with five processes (plans/14 §5); we hold one
        // only while claiming or reporting, so the pool can be far smaller than
        // the concurrency limit.
        .max_connections(config.pool_size)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&config.database_url)
        .await?;

    if config.migrate {
        info!("applying schema");
        stampede_core::schema::migrate(&pool).await?;
    }

    let queue = Queue::new(pool);

    // Provision the partition up front so the first enqueue does not land in
    // the DEFAULT partition, which has no dedicated autovacuum settings.
    if config.worker {
        queue.create_queue(&config.queue).await?;
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // SIGINT/SIGTERM trigger a graceful drain: stop claiming, let in-flight
    // jobs finish, deregister. Jobs abandoned instead would sit until the
    // reaper's dead_after elapsed.
    let signal_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        info!("shutdown signal received; draining");
        let _ = signal_tx.send(true);
    });

    let mut tasks = Vec::new();

    if config.worker {
        let worker = Worker::new(
            queue.clone(),
            WorkerConfig {
                queue: config.queue.clone(),
                max_concurrency: config.concurrency,
                batch_size: config.batch_size,
                poll_interval: config.poll_interval,
                job_timeout: config.job_timeout,
                ..Default::default()
            },
        );

        info!(
            queue = %config.queue,
            concurrency = config.concurrency,
            "starting worker"
        );
        let rx = shutdown_rx.clone();
        tasks.push(tokio::spawn(async move { worker.run(rx).await }));
    }

    if config.maintenance {
        let m = Maintenance::new(
            queue.clone(),
            MaintenanceConfig {
                dead_after: config.dead_after,
                retain_for: config.retain_for,
                ..Default::default()
            },
        );
        info!("starting maintenance (reaper + pruner)");
        let rx = shutdown_rx.clone();
        tasks.push(tokio::spawn(async move { m.run(rx).await }));
    }

    if tasks.is_empty() {
        eprintln!("error: nothing to do — all roles disabled\n\n{}", Config::usage());
        std::process::exit(2);
    }

    for t in tasks {
        match t.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!(error = %e, "task failed"),
            Err(e) => error!(error = %e, "task panicked"),
        }
    }

    info!("stopped");
    Ok(())
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
