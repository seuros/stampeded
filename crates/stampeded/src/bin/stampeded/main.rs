//! The Stampeded daemon.
//!
//! One binary, roles behind flags (plans/README §4). Run it with every role for
//! a single-node deployment, or with different flags per process to split
//! scheduler from executor later — without shipping a second binary.
//!
//!   stampeded --database-url postgres://... --queue default
//!   stampeded --no-worker                     # maintenance only
//!   stampeded --queue mail --concurrency 50

mod config;

use config::Config;
use stampeded::{
    Maintenance, MaintenanceConfig, Metrics, Queue, TracingRecorder, Wakeup, Worker, WorkerConfig,
};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stampeded=info,stampeded_core=info".into()),
        )
        .init();

    let config = match Config::from_args_and_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}\n\n{}", Config::usage());
            std::process::exit(2);
        }
    };

    // Said BEFORE connecting, because an unreachable server produces no output
    // at all until the acquire timeout elapses -- ten seconds of silence at
    // startup reads as a hang, and the first thing anyone does is kill it.
    info!(
        url = %redact(&config.database_url),
        "connecting"
    );

    let pool = sqlx::postgres::PgPoolOptions::new()
        // Sized against the worker's concurrency, not arbitrarily. underway
        // pins a connection per in-flight job and exhausts a stock
        // max_connections=100 with five processes (plans/14 §5); we hold one
        // only while claiming or reporting, so the pool can be far smaller than
        // the concurrency limit.
        .max_connections(config.pool_size)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&config.database_url)
        .await
        .map_err(|e| explain_connect_failure(&e, &config.database_url))?;

    if config.migrate {
        info!("applying schema");
        stampeded::schema::migrate(&pool).await?;
    }

    let queue = Queue::new(pool.clone());

    // Provision the partition up front so the first enqueue does not land in
    // the DEFAULT partition, which has no dedicated autovacuum settings.
    if config.worker {
        queue.create_queue(&config.queue).await.map_err(|e| {
            // The overwhelmingly likely cause on a first run, and the raw error
            // ("function stampeded_create_queue(text) does not exist") does not
            // say so.
            if e.to_string().contains("stampeded_create_queue") {
                // No interpolation here, so `format!` would be noise.
                Fatal(
                    "the Stampeded schema is not installed in this database.\n\n\
                     Run once with --migrate to create it:\n\n    \
                     stampeded --database-url ... --migrate\n\n\
                     Or apply schema/*.sql by hand. Postgres 18+ is required: the \
                     schema uses uuidv7(), RETURNING OLD./NEW. and generated columns, \
                     with no compatibility shims."
                        .to_string(),
                )
            } else {
                Fatal(format!("could not provision queue {:?}: {e}", config.queue))
            }
        })?;
    }

    // TracingRecorder by default: it emits at debug, so it costs nothing until
    // someone sets RUST_LOG=stampeded::metrics=debug -- at which point they
    // have a working metrics feed with no backend to run. Anyone wanting real
    // telemetry implements Recorder in their own crate, on their own upgrade
    // schedule, which is the point of not depending on a vendor here.
    let metrics = Metrics::new(TracingRecorder);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // ONE listener connection for the whole daemon, fanned out in process to
    // every worker. Never one per worker: with max_connections = 100 that is
    // not affordable, and River's MaxConns 1 hijack is the shape to copy.
    //
    // Failure here is not fatal. A daemon that cannot LISTEN polls instead,
    // which is correct and merely less prompt -- the whole point of keeping the
    // poll floor on unconditionally.
    let wakeup = if config.worker {
        match Wakeup::start(&pool, &config.database_url, shutdown_rx.clone()).await {
            Ok(w) => Some(w),
            Err(e) => {
                warn!(error = %e, "could not start listener; polling only");
                None
            }
        }
    } else {
        None
    };

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
        let worker = match wakeup.clone() {
            Some(w) => worker.with_wakeup(w),
            None => worker,
        }
        .with_metrics(metrics.clone());
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
        let m = m.with_metrics(metrics.clone());
        info!("starting maintenance (reaper, pruner, expirer, health)");
        let rx = shutdown_rx.clone();
        tasks.push(tokio::spawn(async move { m.run(rx).await }));
    }

    if let Some(addr) = config.http.clone() {
        use stampeded::http::api::WakeupStatus;
        let mut api = stampeded::http::ApiState::new(queue.clone()).with_wakeups(match &wakeup {
            Some(w) if w.is_healthy() => WakeupStatus::Live,
            Some(_) => WakeupStatus::PollingOnly,
            // No worker role, so no listener was started and there is nothing
            // here to wake.
            None => WakeupStatus::NotApplicable,
        });

        match &config.dashboard_token {
            Some(t) => {
                api = api.with_dashboard_token(t);
                info!(%addr, "serving dashboard at /dashboard?token=...");
            }
            None => info!(
                "dashboard NOT served: pass --dashboard-token to enable it. \
                 It is off by default because an unauthenticated job dashboard \
                 exposes customer data and a retry button."
            ),
        }

        let rx = shutdown_rx.clone();
        info!(%addr, "starting http api");
        tasks.push(tokio::spawn(async move {
            if let Err(e) = stampeded::http::serve(&addr, api, rx).await {
                error!(error = %e, "http server stopped");
            }
            Ok(())
        }));
    }

    if tasks.is_empty() {
        eprintln!(
            "error: nothing to do — all roles disabled\n\n{}",
            Config::usage()
        );
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

/// A fatal startup error, printed for a human.
///
/// `main` returning `Result` prints the error with DEBUG, so a `String` comes
/// out quoted with literal `\n` escapes — which turns a carefully written
/// multi-line hint into one unreadable line. Debug delegating to Display is
/// the standard fix.
pub(crate) struct Fatal(pub String);

impl std::fmt::Debug for Fatal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::fmt::Display for Fatal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Fatal {}

/// Hide the password when echoing a connection URL.
///
/// It goes into logs, and a log line is the easiest secret to leak by
/// accident — someone pastes a startup error into an issue.
pub(crate) fn redact(url: &str) -> String {
    match (url.find("://"), url.find('@')) {
        (Some(scheme), Some(at)) if at > scheme + 3 => {
            let userinfo = &url[scheme + 3..at];
            let user = userinfo.split(':').next().unwrap_or("");
            format!("{}{}:***{}", &url[..scheme + 3], user, &url[at..])
        }
        _ => url.to_string(),
    }
}

/// Turn a connection failure into something actionable.
///
/// sqlx's Debug output is a full struct dump — accurate, and useless to
/// someone who has just mistyped a database name.
fn explain_connect_failure(e: &sqlx::Error, url: &str) -> Fatal {
    let detail = match e {
        sqlx::Error::Database(db) => db.message().to_string(),
        other => other.to_string(),
    };

    let hint = if detail.contains("does not exist") {
        "\n\nThe database itself is missing. Create it first: createdb <name>"
    } else if detail.contains("password") || detail.contains("authentication") {
        "\n\nCheck the credentials in the URL."
    } else if detail.contains("Connection refused") || detail.contains("connect") {
        "\n\nNothing is listening there. Check the host and port, and that \
         Postgres is running."
    } else {
        ""
    };

    Fatal(format!(
        "could not connect to {}: {detail}{hint}",
        redact(url)
    ))
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
