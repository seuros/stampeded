//! Wire-level tests: a real server on a real port, hit with real requests.
//!
//! Skipped unless `STAMPEDED_TEST_DATABASE_URL` is set.
//!
//! These exist because request-shape unit tests cannot see routing. The
//! per-job routes shipped broken — `Path<(String, String)>` binds path
//! parameters by NAME ORDER rather than by position, so `{queue}` and `{id}`
//! arrived swapped and every lookup failed with "id is not a UUID". Nothing
//! below the transport could have caught that.

use sqlx::{AssertSqlSafe, PgPool};
use stampeded::{NewJob, Queue};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

/// A server on an ephemeral port, with its own schema. Returns the address and
/// a shutdown handle.
async fn serve() -> Option<(String, Queue, watch::Sender<bool>)> {
    let url = std::env::var("STAMPEDED_TEST_DATABASE_URL").ok()?;

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

    stampeded::schema::migrate(&pool).await.expect("migrate");
    let queue = Queue::new(pool);
    queue.create_queue("default").await.expect("create queue");
    queue.create_queue("dead").await.expect("create dlq");

    // Port 0 and then ask the OS what it gave us: a fixed port would make these
    // tests fail when run in parallel with anything else.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind probe");
    let addr = probe.local_addr().expect("addr").to_string();
    drop(probe);

    let (tx, rx) = watch::channel(false);
    let q = queue.clone();
    let a = addr.clone();
    tokio::spawn(async move {
        let state = stampeded::http::ApiState::new(q).with_dashboard_token("s3cret");
        let _ = stampeded::http::serve(&a, state, rx).await;
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
                // Skipping is a courtesy to someone without a database. It
                // must never be a courtesy to CI: with the variable unset,
                // every test here returns early and `cargo test` reports
                // success, so a renamed variable or a Postgres service
                // container that never came up turns the entire integration
                // suite into a no-op WITHOUT turning anything red. A green
                // build that tested nothing is worse than a red one, because
                // it is believed.
                assert!(
                    std::env::var_os("CI").is_none(),
                    "STAMPEDED_TEST_DATABASE_URL is not set and CI is. Refusing \
                     to skip the integration tests and report success. Check \
                     the Postgres service container."
                );
                eprintln!("skipping: STAMPEDED_TEST_DATABASE_URL not set");
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
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b)
        .unwrap_or("")
        .to_string();
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
    let (status, _) = request(
        &addr,
        "GET",
        &format!("/queues/default/jobs/{missing}"),
        None,
    )
    .await;
    assert_eq!(status, 404);

    let (status, _) = request(&addr, "GET", "/queues/default/jobs/not-a-uuid", None).await;
    assert_eq!(
        status, 400,
        "a malformed id is the caller's error, not a 500"
    );

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
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "stream did not close"
    );
    assert!(body.contains("event: state"), "no state event: {body}");
    assert!(
        body.contains("completed"),
        "terminal state not reported: {body}"
    );

    let _ = tx.send(true);
}

#[tokio::test]
async fn depth_reports_by_state() {
    let (addr, q, tx) = server!();
    for _ in 0..3 {
        q.enqueue(NewJob::new("default", "x"))
            .await
            .expect("enqueue");
    }
    let (status, body) = request(&addr, "GET", "/queues/default/depth", None).await;
    assert_eq!(status, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(v["depth"]["available"], 3);
    let _ = tx.send(true);
}

#[tokio::test]
async fn the_dashboard_is_not_served_without_a_token() {
    // plans/06 asks for secure-by-default and names the precedent: an opt-in
    // auth setting meant a job dashboard sat open on staging and production
    // because nobody opted in. A job dashboard exposes customer data in job
    // args AND offers a retry button.
    let (addr, _q, tx) = server!();

    let (status, _) = request(&addr, "GET", "/dashboard", None).await;
    assert_eq!(status, 404, "no token presented");

    let (status, _) = request(&addr, "GET", "/dashboard?token=wrong", None).await;
    assert_eq!(status, 404, "wrong token");

    // 404 rather than 401: an unauthenticated caller should not learn that a
    // dashboard exists here at all.
    let (status, body) = request(&addr, "GET", "/dashboard?token=s3cret", None).await;
    assert_eq!(status, 200);
    assert!(
        body.contains("Stampeded"),
        "the real dashboard should render"
    );

    let _ = tx.send(true);
}

#[tokio::test]
async fn the_dashboard_stream_is_gated_too() {
    // Gating the page but not the stream would be worse than gating neither,
    // because it would look protected.
    let (addr, _q, tx) = server!();

    let (status, _) = request(&addr, "GET", "/dashboard/stream", None).await;
    assert_eq!(
        status, 404,
        "the SSE stream must be gated as well as the page"
    );

    let _ = tx.send(true);
}

#[tokio::test]
async fn the_token_is_not_reflected_unescaped_into_the_page() {
    // The token is echoed into the stream URL so the browser can authenticate
    // its EventSource. Anything echoed into a page is an XSS sink if it is not
    // escaped -- and this one is attacker-controlled by construction, since
    // they choose what to put in the query string.
    let (addr, _q, tx) = server!();

    let (_status, body) = request(
        &addr,
        "GET",
        "/dashboard?token=%27%3E%3Cscript%3Ealert(1)%3C/script%3E",
        None,
    )
    .await;
    assert!(!body.contains("<script>alert(1)"), "reflected XSS: {body}");

    let _ = tx.send(true);
}

#[tokio::test]
async fn enqueue_accepts_every_field_the_job_row_has() {
    // The API silently dropped destination, dead_letter_queue and
    // expire_seconds after they were added to NewJob: the request struct did
    // not have them, serde ignored the unknown keys, and the job was created
    // WITHOUT them. No error, no warning -- an HTTP caller simply could not
    // reach half the features, and the end-to-end dead-letter check is what
    // caught it.
    let (addr, q, tx) = server!();

    let (status, body) = request(
        &addr,
        "POST",
        "/jobs",
        Some(
            r#"{"queue":"default","task":"sync","destination":"api.vendor.com",
                "dead_letter_queue":"dead","expire_seconds":42,
                "unique":"k","unique_period":"hour","priority":3}"#,
        ),
    )
    .await;
    assert_eq!(status, 201, "{body}");

    let id: uuid::Uuid = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    let job = q.fetch("default", id).await.unwrap().expect("job exists");
    assert_eq!(job.destination.as_deref(), Some("api.vendor.com"));
    assert_eq!(job.dead_letter_queue.as_deref(), Some("dead"));
    assert_eq!(job.expire_seconds, 42);
    assert_eq!(job.dedup_period.as_deref(), Some("hour"));
    assert_eq!(job.priority, 3);

    let _ = tx.send(true);
}

#[tokio::test]
async fn a_bad_unique_period_is_rejected_rather_than_ignored() {
    // Silently ignoring it would mean the caller believes they have an hourly
    // limit and they have a permanent one.
    let (addr, _q, tx) = server!();
    let (status, body) = request(
        &addr,
        "POST",
        "/jobs",
        Some(r#"{"queue":"default","task":"x","unique":"k","unique_period":"fortnight"}"#),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body.contains("fortnight"),
        "the error should name the bad value: {body}"
    );

    let _ = tx.send(true);
}

/// Whatever the dashboard's assets advertise must actually resolve.
///
/// datastar.js ends with `//# sourceMappingURL=datastar.js.map`, so any
/// browser with devtools open requests the map. Serving the script from the
/// binary without the map turned that into a 404 in the console of the person
/// debugging the dashboard — noise that sends an operator looking for a
/// problem that is not there.
///
/// This asserts the relationship rather than the two routes: if the embedded
/// script ever stops advertising a map, or advertises a different name, the
/// test follows it.
#[tokio::test]
async fn the_dashboard_assets_resolve_including_whatever_they_advertise() {
    let (addr, _q, tx) = server!();

    let (status, js) = request(&addr, "GET", "/dashboard/datastar.js", None).await;
    assert_eq!(status, 200, "the dashboard cannot load Datastar");
    assert!(js.len() > 1000, "datastar.js looks truncated: {} bytes", js.len());

    // Follow what the script itself asks for.
    if let Some(line) = js.lines().rev().find(|l| l.contains("sourceMappingURL=")) {
        let name = line.rsplit("sourceMappingURL=").next().unwrap().trim();
        let (status, map) = request(&addr, "GET", &format!("/dashboard/{name}"), None).await;
        assert_eq!(status, 200, "datastar.js advertises {name}, which 404s");
        assert!(
            map.trim_start().starts_with('{'),
            "the source map should be JSON"
        );
    }

    let _ = tx.send(true);
}

#[tokio::test]
#[cfg(feature = "game")]
async fn the_game_world_assets_fetch_nothing_from_the_network() {
    let (addr, _q, tx) = server!();

    // Verify the game page itself loads
    let (status, _html) = request(&addr, "GET", "/game?token=s3cret", None).await;
    assert_eq!(status, 200, "the game page cannot load");

    // Verify Three.js module has no external imports
    let (status, three_js) = request(&addr, "GET", "/game/three.module.js", None).await;
    assert_eq!(status, 200, "three.module.js must be served from the binary");
    assert!(three_js.len() > 100_000, "three.module.js looks truncated: {} bytes", three_js.len());
    assert!(
        !three_js.contains("http://") && !three_js.contains("https://") && !three_js.contains("//cdn"),
        "three.module.js must not fetch from the network"
    );

    // Verify GLTFLoader has no external imports
    let (status, gltf_loader) = request(&addr, "GET", "/game/gltf-loader.js", None).await;
    assert_eq!(status, 200, "gltf-loader.js must be served from the binary");
    assert!(gltf_loader.len() > 10_000, "gltf-loader.js looks truncated: {} bytes", gltf_loader.len());
    assert!(
        !gltf_loader.contains("http://") && !gltf_loader.contains("https://"),
        "gltf-loader.js must not fetch from the network"
    );

    // Verify models are served and are binary (not HTML error pages)
    let models = vec![
        "box-small.glb",
        "box-large.glb",
        "conveyor-long.glb",
        "crane.glb",
        "hopper-square.glb",
        "machine.glb",
    ];
    for model in models {
        let (status, body) = request(&addr, "GET", &format!("/game/models/{}", model), None).await;
        assert_eq!(status, 200, "model {} must be served from the binary", model);
        // GLB files start with magic bytes 0x67 0x6C 0x54 0x46 ('glTF' in ASCII)
        let bytes = body.as_bytes();
        assert!(
            bytes.len() > 100 && bytes.starts_with(b"glTF"),
            "model {} looks wrong: starts with {:?}",
            model,
            &bytes[..bytes.len().min(20)]
        );
    }

    let _ = tx.send(true);
}
