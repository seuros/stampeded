//! `stampeded::connect` and the daemon's startup on it.
//!
//!   STAMPEDED_TEST_DATABASE_URL=postgres://ubuntu:ubuntu@localhost/stampeded_test \
//!     cargo test -p stampeded --test connect

mod common;

use common::database_url;
use sqlx::postgres::PgPoolOptions;
use std::assert_matches;
use std::time::{Duration, Instant};

/// Far below the daemon's 10 s acquire timeout, far above a refused connect.
const PROMPTLY: Duration = Duration::from_secs(5);

#[tokio::test]
async fn a_host_that_never_answers_times_out_saying_so() {
    // MOCKED: not Postgres; never accepts, so the client waits on its SSLRequest.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();

    let error = stampeded::connect(
        PgPoolOptions::new().acquire_timeout(Duration::from_millis(300)),
        &format!("postgres://nobody@127.0.0.1:{port}/none"),
    )
    .await
    .expect_err("nothing answered");

    assert_matches!(&error, sqlx::Error::Io(io) if io.kind() == std::io::ErrorKind::TimedOut);
    assert!(error.to_string().contains("connect timed out"), "{error}");
    drop(listener);
}

#[tokio::test]
async fn a_busy_pool_still_waits_for_a_free_connection() {
    let Some(url) = database_url() else { return };
    let pool = stampeded::connect(
        PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10)),
        &url,
    )
    .await
    .expect("connect");

    let held = pool.acquire().await.expect("the only connection");
    let waiter = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await.map(drop) }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !waiter.is_finished(),
        "a second acquire did not wait: {:?}",
        waiter.await
    );

    drop(held);
    waiter
        .await
        .expect("join")
        .expect("the connection, once it was free");
}

#[cfg(all(feature = "cli", feature = "http"))]
#[test]
fn the_daemon_fails_promptly_and_names_the_fix() {
    use sqlx::ConnectOptions;
    use sqlx::postgres::PgConnectOptions;

    // A loopback port nothing listens on: bound, then released.
    let closed = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("free port")
        .port();
    // (url, what the error must contain: the reason, then any hint)
    let mut cases = vec![
        (
            format!("postgres://nobody@127.0.0.1:{closed}/none"),
            vec!["refused", "Nothing is listening"],
        ),
        (
            "postgres://nobody@stampeded-no-such-host.invalid/none".into(),
            vec!["lookup"],
        ),
    ];
    if let Some(url) = database_url() {
        let good: PgConnectOptions = url.parse().expect("STAMPEDED_TEST_DATABASE_URL");
        let role = "stampeded_no_such_role";
        cases.push((
            good.clone().username(role).to_url_lossy().into(),
            vec![role, "credentials"],
        ));
        let database = "stampeded_no_such_database";
        cases.push((
            good.database(database).to_url_lossy().into(),
            vec![database, "createdb"],
        ));
    }

    let mut wrong = Vec::new();
    for (url, expected) in &cases {
        let started = Instant::now();
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_stampeded"))
            .args(["--database-url", url])
            .env_remove("STAMPEDED_DATABASE_URL")
            .env("RUST_LOG", "off")
            .output()
            .expect("run the daemon");
        let took = started.elapsed();
        let stderr = String::from_utf8_lossy(&out.stderr);
        eprintln!("{url}: {took:?}: {stderr}");
        if out.status.success()
            || took >= PROMPTLY
            || !expected.iter().all(|word| stderr.contains(word))
        {
            wrong.push(format!(
                "{url}: {} after {took:?}, expected {expected:?} in {stderr:?}",
                out.status
            ));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[cfg(all(feature = "cli", feature = "http"))]
#[test]
fn the_daemon_never_prints_a_password() {
    let closed = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("free port")
        .port();
    // As the Ruby gem hands it over, plus a query password sqlx also accepts.
    let url = format!(
        "postgres://app:hun%40ter2@127.0.0.1:{closed}/none?sslmode=prefer&password=ter2hun"
    );
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_stampeded"))
        .env("STAMPEDED_DATABASE_URL", &url)
        .env_remove("RUST_LOG")
        .output()
        .expect("run the daemon");
    let printed = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    assert!(!out.status.success(), "nothing listens there: {printed}");
    assert!(!printed.contains("ter2"), "password printed: {printed}");
    assert!(
        printed.contains(&format!(
            "app:***@127.0.0.1:{closed}/none?sslmode=prefer&password=***"
        )),
        "the connecting line and the error should still name the target: {printed}"
    );
}
