//! Wire-level tests: a real server on a real port, hit with real requests.
//!
//! Skipped unless `STAMPEDE_TEST_DATABASE_URL` is set.
//!
//! These exist because request-shape unit tests cannot see routing. The
//! per-job routes shipped broken — `Path<(String, String)>` binds path
//! parameters by NAME ORDER rather than by position, so `{queue}` and `{id}`
//! arrived swapped and every lookup failed with "id is not a UUID". Nothing
//! below the transport could have caught that.

use sqlx::{AssertSqlSafe, PgPool};
use stampede_core::{NewJob, Queue};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

/// A server on an ephemeral port, with its own schema. Returns the address and
/// a shutdown handle.
async fn serve() -> Option<(String, Queue, watch::Sender<bool>)> {
    let url = std::env::var("STAMPEDE_TEST_DATABASE_URL").ok()?;

    let schema = format!("w_{}", Uuid::now_v7().simple());
    let admin = PgPool::connect(&url).await.expect("connect");
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create schema");
    admin.close().await;

    let sp = schema.clone();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .after_connect(move |conn, _| {
            let sp = sp.clone();
            Box::pin(async move {
                sqlx::query(AssertSqlSafe(format!("SET search_path TO {sp}")))
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .expect("pool");

    stampede_core::schema::migrate(&pool).await.expect("migrate");
    let queue = Queue::new(pool);
    queue.create_queue("default").await.expect("create queue");

    // Port 0 and then ask the OS what it gave us: a fixed port would make these
    // tests fail when run in parallel with anything else.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind probe");
    let addr = probe.local_addr().expect("addr").to_string();
    drop(probe);

    let (tx, rx) = watch::channel(false);
    let q = queue.clone();
    let a = addr.clone();
    tokio::spawn(async move {
        let _ = stampede_http::serve(&a, q, rx).await;
    });

    // Wait for the listener rather than sleeping a fixed amount.
    for _ in 0..100 {
        if TcpStream::connect(&addr).await.is_ok() {
            return Some((addr, queue, tx));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server never came up on {addr}");
}

macro_rules! server {
    () => {
        match serve().await {
            Some(s) => s,
            None => {
                eprintln!("skipping: STAMPEDE_TEST_DATABASE_URL not set");
                return;
            }
        }
    };
}

/// Minimal HTTP/1.1 client. A dependency-free raw request is enough here and
/// avoids pulling a client stack into dev-deps just to read a status line.
async fn request(addr: &str, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(b) = body {
        req.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            b.len()
        ));
    }
    req.push_str("\r\n");
    if let Some(b) = body {
        req.push_str(b);
    }
    s.write_all(req.as_bytes()).await.expect("write");

    let mut raw = Vec::new();
    // Bounded: an SSE stream would otherwise hold this open forever.
    let _ = tokio::time::timeout(Duration::from_secs(10), s.read_to_end(&mut raw)).await;
    let text = String::from_utf8_lossy(&raw).into_owned();

    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("").to_string();
    (status, body)
}

#[tokio::test]
async fn health_answers() {
    let (addr, _q, tx) = server!();
    let (status, body) = request(&addr, "GET", "/health", None).await;
    assert_eq!(status, 200);
    assert!(body.contains("\"ok\""), "{body}");
    let _ = tx.send(true);
}

#[tokio::test]
async fn enqueue_then_read_the_job_back() {
    let (addr, _q, tx) = server!();

    let (status, body) = request(
        &addr,
        "POST",
        "/jobs",
        Some(r#"{"queue":"default","task":"greet","args":{"who":"wire"}}"#),
    )
    .await;
    assert_eq!(status, 201, "{body}");

    let v: serde_json::Value = serde_json::from_str(&body).expect("json");
    let id = v["id"].as_str().expect("id").to_string();

    // The route that shipped broken: queue and id must not be transposed.
    let (status, body) = request(&addr, "GET", &format!("/queues/default/jobs/{id}"), None).await;
    assert_eq!(status, 200, "{body}");

    let job: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(job["task"], "greet");
    assert_eq!(job["state"], "available");
    assert_eq!(job["args"]["who"], "wire");
    assert_eq!(job["id"], id, "returned a different job than requested");

    let _ = tx.send(true);
}

#[tokio::test]
async fn unknown_job_is_404_and_a_bad_uuid_is_400() {
    let (addr, _q, tx) = server!();

    let missing = Uuid::now_v7();
    let (status, _) = request(&addr, "GET", &format!("/queues/default/jobs/{missing}"), None).await;
    assert_eq!(status, 404);

    let (status, _) = request(&addr, "GET", "/queues/default/jobs/not-a-uuid", None).await;
    assert_eq!(status, 400, "a malformed id is the caller's error, not a 500");

    let _ = tx.send(true);
}

#[tokio::test]
async fn the_event_stream_ends_once_the_job_is_terminal() {
    // A client should be able to read to EOF rather than deciding for itself
    // when to stop. If the stream stayed open on a finished job this read would
    // hit the 10s timeout instead of returning promptly.
    let (addr, q, tx) = server!();

    let id = q
        .enqueue(NewJob::new("default", "greet"))
        .await
        .expect("enqueue")
        .expect("id");
    let worker = q
        .register_worker("wire-test", 1, &["default".into()])
        .await
        .expect("register");
    let claimed = q.claim("default", worker, 1).await.expect("claim");
    assert_eq!(claimed.len(), 1);
    q.complete(&claimed[0], worker).await.expect("complete");

    let started = std::time::Instant::now();
    let (status, body) = request(
        &addr,
        "GET",
        &format!("/queues/default/jobs/{id}/events"),
        None,
    )
    .await;

    assert_eq!(status, 200);
    assert!(started.elapsed() < Duration::from_secs(5), "stream did not close");
    assert!(body.contains("event: state"), "no state event: {body}");
    assert!(body.contains("completed"), "terminal state not reported: {body}");

    let _ = tx.send(true);
}

#[tokio::test]
async fn depth_reports_by_state() {
    let (addr, q, tx) = server!();
    for _ in 0..3 {
        q.enqueue(NewJob::new("default", "x")).await.expect("enqueue");
    }
    let (status, body) = request(&addr, "GET", "/queues/default/depth", None).await;
    assert_eq!(status, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(v["depth"]["available"], 3);
    let _ = tx.send(true);
}
