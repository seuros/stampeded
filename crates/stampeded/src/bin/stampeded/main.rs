//! The Stampeded daemon: one binary, roles behind flags.
//!
//!   stampeded --database-url postgres://... --queue default
//!   stampeded --no-worker                     # maintenance only
//!   stampeded --queue mail --concurrency 50

mod config;

use anyhow::{Context, anyhow};
use config::Config;
use stampeded::{
    Maintenance, MaintenanceConfig, Metrics, Queue, TracingRecorder, Wakeup, Worker, WorkerConfig,
};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stampeded=info".into()),
        )
        .init();

    let config = match Config::from_args_and_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}\n\n{}", Config::usage());
            std::process::exit(2);
        }
    };

    // Logged before connecting: an unanswering host is silent until the acquire timeout.
    info!(
        url = %redact(&config.database_url),
        "connecting"
    );

    let pool = stampeded::connect(
        sqlx::postgres::PgPoolOptions::new()
            // Held only while claiming or reporting, so this can be below concurrency.
            .max_connections(config.pool_size)
            .acquire_timeout(Duration::from_secs(10)),
        &config.database_url,
    )
    .await
    .map_err(|e| explain_connect_failure(&e, &config.database_url))?;

    if config.migrate {
        info!("applying schema");
        stampeded::schema::migrate(&pool)
            .await
            .context("applying the schema")?;
    }

    let queue = Queue::new(pool.clone());

    // Up front, so the first enqueue skips the DEFAULT partition and its untuned autovacuum.
    let provision = match &config.queues {
        stampeded::Queues::Named(names) if config.worker => names.clone(),
        _ => Vec::new(),
    };
    for name in &provision {
        queue.create_queue(name).await.map_err(|e| {
            // Almost always a missing schema, which the raw error does not say.
            if e.to_string().contains("stampeded_create_queue") {
                anyhow!(
                    "the Stampeded schema is not installed in this database.\n\n\
                     Run once with --migrate to create it:\n\n    \
                     stampeded --database-url ... --migrate\n\n\
                     Postgres 18+ is required: the \
                     schema uses uuidv7(), RETURNING OLD./NEW. and generated columns, \
                     with no compatibility shims."
                )
            } else {
                anyhow::Error::new(e).context(format!("could not provision queue {name:?}"))
            }
        })?;
    }

    // Emits at debug: free until RUST_LOG=stampeded::metrics=debug.
    let metrics = Metrics::new(TracingRecorder);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // One LISTEN connection for the whole daemon. Not fatal: without it workers poll.
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

    // SIGINT/SIGTERM drain. Installed here, where a failure is not lost in a detached task.
    let signal = shutdown_signal()?;
    tokio::spawn(async move {
        signal.await;
        info!("shutdown signal received; draining");
        // Err only when every receiver is gone, i.e. everything has stopped.
        let _ = shutdown_tx.send(true);
    });

    let mut tasks = Vec::new();

    if config.worker {
        let worker = Worker::new(
            queue.clone(),
            WorkerConfig {
                queues: config.queues.clone(),
                max_concurrency: config.concurrency,
                batch_size: config.batch_size,
                poll_interval: config.poll_interval,
                job_timeout: config.job_timeout,
                ..Default::default()
            },
        );

        info!(
            queues = %config.queues.label(),
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
        info!("starting maintenance (reaper, pruner, expirer, health, planner)");
        let rx = shutdown_rx.clone();
        tasks.push(tokio::spawn(async move { m.run(rx).await }));
    }

    if let Some(addr) = config.http.clone() {
        use stampeded::http::api::WakeupStatus;
        let mut api = stampeded::http::ApiState::new(queue.clone()).with_wakeups(match &wakeup {
            Some(w) if w.is_healthy() => WakeupStatus::Live,
            Some(_) => WakeupStatus::PollingOnly,
            // No worker role, so no listener.
            None => WakeupStatus::NotApplicable,
        });

        if let Some(t) = &config.dashboard_token {
            api = api.with_dashboard_token(t);
            info!(%addr, "serving dashboard at /dashboard?token=...");
        } else {
            info!(
                "dashboard NOT served: pass --dashboard-token to enable it. \
             It is off by default because an unauthenticated job dashboard \
             exposes customer data and a retry button."
            );
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
            "error: nothing to do -- all roles disabled\n\n{}",
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

/// Hide the passwords when echoing a connection URL: the userinfo's and the query's.
pub(crate) fn redact(url: &str) -> String {
    let Some(start) = url.find("://").map(|i| i + 3) else {
        return url.to_string();
    };
    // As the URL parser reads it: the authority ends at the first `/`, `?` or `#`,
    // and its last `@` ends the userinfo.
    let end = url[start..]
        .find(['/', '?', '#'])
        .map_or(url.len(), |i| start + i);
    let at = match url[start..end].rfind('@') {
        Some(i) => Some(start + i),
        // Not host:port, so a raw `/`, `?` or `#` in the password cut it short.
        None if !is_host_port(&url[start..end]) => Some(url.rfind('@').unwrap_or(url.len())),
        None => None,
    };
    at.map_or_else(
        || redact_query(url),
        |at| {
            let user = url[start..at].split(':').next().unwrap_or("");
            format!("{}{user}:***{}", &url[..start], redact_query(&url[at..]))
        },
    )
}

/// Whether an authority with no `@` reads as `host[:port]`.
fn is_host_port(authority: &str) -> bool {
    let port = match authority.strip_prefix('[') {
        Some(v6) => match v6.split_once(']') {
            Some((_, "")) => return true,
            Some((_, rest)) => rest.strip_prefix(':'),
            None => None,
        },
        None => match authority.split_once(':') {
            Some((_, port)) => Some(port),
            None => return true,
        },
    };
    port.is_some_and(|p| p.bytes().all(|b| b.is_ascii_digit()))
}

/// `rest` with the values of `password` and `sslpassword` in its query masked.
fn redact_query(rest: &str) -> String {
    let Some(q) = rest
        .find(['?', '#'])
        .filter(|&i| rest[i..].starts_with('?'))
    else {
        return rest.to_string();
    };
    let end = rest[q..].find('#').map_or(rest.len(), |i| q + i);
    let query: Vec<String> = rest[q + 1..end]
        .split('&')
        .map(|pair| match pair.split_once('=') {
            Some((key, _)) if key.to_ascii_lowercase().ends_with("password") => {
                format!("{key}=***")
            }
            _ => pair.to_string(),
        })
        .collect();
    format!("{}?{}{}", &rest[..q], query.join("&"), &rest[end..])
}

/// Turn a connection failure into something actionable.
fn explain_connect_failure(e: &sqlx::Error, url: &str) -> anyhow::Error {
    let detail = match e {
        sqlx::Error::Database(db) => db.message().to_string(),
        other => other.to_string(),
    };

    // TLS first, since its message mentions "connect"; a rejected cert is io InvalidData.
    let tls = match e {
        sqlx::Error::Tls(_) => true,
        sqlx::Error::Io(io) => io.kind() == std::io::ErrorKind::InvalidData,
        _ => false,
    };
    // Class 28 first: an unknown role also says "does not exist".
    let login = match e {
        sqlx::Error::Database(db) => db.code().is_some_and(|code| code.starts_with("28")),
        _ => false,
    };
    let hint = if tls {
        "\n\nCheck sslmode and sslrootcert in the URL. verify-ca and verify-full \
         trust Mozilla's roots plus the PEM file sslrootcert names, so a private \
         CA (RDS's included) must be named there."
    } else if login || detail.contains("password") || detail.contains("authentication") {
        "\n\nCheck the credentials in the URL."
    } else if detail.contains("does not exist") {
        "\n\nThe database itself is missing. Create it first: createdb <name>"
    } else if detail.contains("Connection refused") || detail.contains("connect") {
        "\n\nNothing is listening there. Check the host and port, and that \
         Postgres is running."
    } else {
        ""
    };

    anyhow!("could not connect to {}: {detail}{hint}", redact(url))
}

/// Resolves on the first SIGTERM or SIGINT.
#[cfg(unix)]
fn shutdown_signal() -> anyhow::Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).context("installing the SIGTERM handler")?;
    let mut int = signal(SignalKind::interrupt()).context("installing the SIGINT handler")?;
    Ok(async move {
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    })
}

/// Resolves on the first ctrl-c.
#[cfg(not(unix))]
fn shutdown_signal() -> anyhow::Result<impl Future<Output = ()>> {
    Ok(async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            // Resolving here would drain the daemon the moment it started.
            error!(error = %e, "cannot listen for ctrl-c; stop the process another way");
            std::future::pending::<()>().await;
        }
    })
}
