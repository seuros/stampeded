//! Wire-level tests: a real server on a real port, since unit tests cannot see
//! routing. Skipped unless `STAMPEDED_TEST_DATABASE_URL` is set.

use sqlx::{AssertSqlSafe, PgPool};
use stampeded::{NewJob, Queue};
use std::fmt::Write as _;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use uuid::Uuid;

/// A server on an ephemeral port and its own schema: (address, queue, shutdown).
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

    // Port 0: a fixed port would clash with tests running in parallel.
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
                // Never under CI: a suite that skips everything reports green.
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

#[cfg(feature = "game")]
fn has_extension(name: &str, ext: &str) -> bool {
    std::path::Path::new(name)
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case(ext))
}

/// Minimal HTTP/1.1 client, so dev-deps need no client stack.
async fn request(addr: &str, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(b) = body {
        let _ = write!(
            req,
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            b.len()
        );
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
        .map_or("", |(_, b)| b)
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

    // Queue and id must not be transposed.
    let (status, body) = request(&addr, "GET", &format!("/queues/default/jobs/{id}"), None).await;
    assert_eq!(status, 200, "{body}");

    let job: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(job["task"], "greet");
    assert_eq!(job["state"], "available");
    assert_eq!(job["args"]["who"], "wire");
    assert_eq!(job["id"], id, "returned a different job than requested");

    // Omitted and null args are both stored as `{}`: the schema refuses JSON null.
    for body in [
        r#"{"queue":"default","task":"bare"}"#,
        r#"{"queue":"default","task":"bare","args":null}"#,
    ] {
        let (status, created) = request(&addr, "POST", "/jobs", Some(body)).await;
        assert_eq!(status, 201, "{created}");
        let v: serde_json::Value = serde_json::from_str(&created).expect("json");
        let id = v["id"].as_str().expect("id");
        let (_, read) = request(&addr, "GET", &format!("/queues/default/jobs/{id}"), None).await;
        let job: serde_json::Value = serde_json::from_str(&read).expect("json");
        assert_eq!(job["args"], serde_json::json!({}), "request: {body}");
    }

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
    // A client reads to EOF; a stream left open would hit the 10s timeout.
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
async fn every_dashboard_page_and_stream_needs_the_token() {
    // Secure by default: the dashboard shows job args and offers a retry button,
    // and a gated page with an open stream would only look protected.
    let (addr, _q, tx) = server!();

    let mut gated = vec!["/dashboard", "/dashboard/stream"];
    if cfg!(feature = "game") {
        gated.extend(["/game", "/game/stream"]);
    }
    for path in gated {
        for query in [
            "",
            "?token=",
            "?token=wrong",
            "?token=s3cre",
            "?token=s3cret2",
        ] {
            // 404, not 401: an unauthenticated caller must not learn a dashboard exists.
            let (status, _) = request(&addr, "GET", &format!("{path}{query}"), None).await;
            assert_eq!(status, 404, "{path}{query}");
        }
    }

    let (status, body) = request(&addr, "GET", "/dashboard?token=s3cret", None).await;
    assert_eq!(status, 200);
    assert!(
        body.contains("Stampeded"),
        "the real dashboard should render"
    );

    let _ = tx.send(true);
}

#[tokio::test]
async fn the_token_is_not_reflected_unescaped_into_the_page() {
    // The token is echoed into the EventSource URL, and the caller chooses it.
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
    // serde ignores unknown keys, so a field the request struct lacks is dropped silently.
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
    // Ignoring it would turn the caller's periodic limit into a permanent one.
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

/// The dashboard's assets resolve, including the source map datastar.js advertises.
#[tokio::test]
async fn the_dashboard_assets_resolve_including_whatever_they_advertise() {
    let (addr, _q, tx) = server!();

    let (status, js) = request(&addr, "GET", "/dashboard/datastar.js", None).await;
    assert_eq!(status, 200, "the dashboard cannot load Datastar");
    assert!(
        js.len() > 1000,
        "datastar.js looks truncated: {} bytes",
        js.len()
    );

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

    let (status, _html) = request(&addr, "GET", "/game?token=s3cret", None).await;
    assert_eq!(status, 200, "the game page cannot load");

    let (status, three_js) = request(&addr, "GET", "/game/three.module.min.js", None).await;
    assert_eq!(
        status, 200,
        "three.module.js must be served from the binary"
    );
    assert!(
        three_js.len() > 100_000,
        "three.module.min.js looks truncated: {} bytes",
        three_js.len()
    );
    assert_no_remote_fetch("three.module.min.js", &three_js);

    // The shell is useless without the core it re-exports from.
    let (status, core) = request(&addr, "GET", "/game/three.core.min.js", None).await;
    assert_eq!(
        status, 200,
        "three.core.min.js must be served: the module imports it"
    );
    assert!(
        core.len() > 100_000,
        "three.core.min.js looks truncated: {} bytes",
        core.len()
    );
    assert_no_remote_fetch("three.core.min.js", &core);

    // Under /game/loaders/ so its '../utils/...' imports resolve to /game/utils/.
    let (status, gltf_loader) = request(&addr, "GET", "/game/loaders/gltf-loader.js", None).await;
    assert_eq!(status, 200, "gltf-loader.js must be served from the binary");
    assert!(
        gltf_loader.len() > 10_000,
        "gltf-loader.js looks truncated: {} bytes",
        gltf_loader.len()
    );
    assert_no_remote_fetch("gltf-loader.js", &gltf_loader);

    // The addon utils it imports: a 404 here kills the loader.
    for util in ["BufferGeometryUtils.js", "SkeletonUtils.js"] {
        let (status, body) = request(&addr, "GET", &format!("/game/utils/{util}"), None).await;
        assert_eq!(status, 200, "{util} must be served: GLTFLoader imports it");
        assert!(
            body.len() > 5_000,
            "{util} looks truncated: {} bytes",
            body.len()
        );
        assert_no_remote_fetch(util, &body);
    }

    // Models must be glTF, not HTML error pages.
    let models = vec![
        "box-small.glb",
        "box-large.glb",
        "conveyor-long.glb",
        "crane.glb",
        "hopper-square.glb",
        "machine.glb",
    ];
    for model in models {
        let (status, body) = request(&addr, "GET", &format!("/game/models/{model}"), None).await;
        assert_eq!(status, 200, "model {model} must be served from the binary");
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
/// Assert a served script's code, not its comments (which cite URLs), fetches nothing.
#[cfg(feature = "game")]
fn assert_no_remote_fetch(what: &str, src: &str) {
    // Whitespace collapsed: minified three writes `from"./three.core.min.js"`.
    let code: String = strip_js_comments(src)
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();

    for pat in [
        "from\"http",
        "from'http",
        "from\"//",
        "from'//",
        "import(\"http",
        "import('http",
        "fetch(\"http",
        "fetch('http",
        "importScripts(\"http",
        "importScripts('http",
    ] {
        assert!(
            !code.contains(pat),
            "{what} fetches from the network: found {pat:?} outside a comment"
        );
    }
}
/// Remove // and /* */ comments, leaving string literals intact.
#[cfg(feature = "game")]
fn strip_js_comments(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let (mut i, mut quote) = (0usize, None::<u8>);

    while i < b.len() {
        let c = b[i];
        if let Some(q) = quote {
            if c == b'\\' {
                i += 2;
                continue;
            }
            if c == q {
                quote = None;
            }
            out.push(c as char);
            i += 1;
        } else if c == b'"' || c == b'\'' || c == b'`' {
            quote = Some(c);
            out.push(c as char);
            i += 1;
        } else if c == b'/' && i + 1 < b.len() && b[i + 1] == b'/' {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if c == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i += 2;
        } else {
            out.push(c as char);
            i += 1;
        }
    }
    out
}

/// The event name the server sends must be the one the browser listens for.
#[test]
#[cfg(feature = "game")]
fn the_game_page_listens_for_the_event_the_server_actually_sends() {
    let src = include_str!("../src/http/game.rs");

    // What the server sends.
    let sent = src
        .split("try_with_event(\"")
        .nth(1)
        .and_then(|r| r.split('"').next())
        .map(str::to_string);

    match sent {
        Some(name) => {
            assert!(
                src.contains(&format!("addEventListener(\"{name}\"")),
                "the server sends a named SSE event {name:?} but the page never \
                 registers addEventListener(\"{name}\"). A named event does NOT \
                 trigger onmessage, so the browser receives every frame and \
                 discards it."
            );
        }
        None => {
            // Unnamed events are type "message", which onmessage handles.
            assert!(
                src.contains("eventSource.onmessage"),
                "the server sends unnamed SSE events but the page does not \
                 define onmessage"
            );
        }
    }
}

/// The page loads models, and every model it names is embedded.
#[test]
#[cfg(feature = "game")]
fn the_models_in_the_binary_are_actually_used_by_the_scene() {
    let raw = include_str!("../src/http/game.rs");

    // Line-based, not strip_js_comments: apostrophes in the Rust prose desync
    // its quote tracking, and a comment there mentions "loader.load()".
    let calls_loader = raw.lines().any(|l| {
        let t = l.trim_start();
        !t.starts_with("//") && t.contains("loader.load(")
    });

    assert!(
        calls_loader,
        "the page never calls loader.load(), so the embedded .glb models are \
         dead weight and the scene is primitives"
    );

    // Every model the page asks for at runtime must be embedded, or it 404s.
    let embedded: Vec<&str> = raw
        .split("include_bytes!(\"../../assets/models/")
        .skip(1)
        .filter_map(|r| r.split('"').next())
        .collect();
    assert!(!embedded.is_empty(), "no models embedded at all");

    for referenced in raw
        .split("MODEL_FILES")
        .nth(1)
        .unwrap_or("")
        .split("};")
        .next()
        .unwrap_or("")
        .split('"')
        .filter(|t| has_extension(t, "glb"))
    {
        assert!(
            embedded.contains(&referenced),
            "the scene asks for {referenced:?} but it is not embedded -- it \
             will 404 at runtime. Embedded: {embedded:?}"
        );
    }
}

/// Every relative import in every served JS asset resolves to a route.
#[test]
#[cfg(feature = "game")]
fn every_import_in_the_served_assets_resolves_to_a_route() {
    let routes = include_str!("../src/http.rs");
    // Every embedded .js; the guard below keeps this list complete.
    let assets: [(&str, &str); 5] = [
        (
            "/game/three.module.min.js",
            include_str!("../assets/three.module.min.js"),
        ),
        (
            "/game/three.core.min.js",
            include_str!("../assets/three.core.min.js"),
        ),
        (
            "/game/loaders/gltf-loader.js",
            include_str!("../assets/gltf-loader.js"),
        ),
        (
            "/game/utils/BufferGeometryUtils.js",
            include_str!("../assets/utils/BufferGeometryUtils.js"),
        ),
        (
            "/game/utils/SkeletonUtils.js",
            include_str!("../assets/utils/SkeletonUtils.js"),
        ),
    ];

    // include_bytes! of .js only: a bare ".js\")" also matches routes and the importmap.
    let embedded_js = include_str!("../src/http/game.rs")
        .split("include_bytes!(\"../../assets/")
        .skip(1)
        .filter(|r| r.split('"').next().is_some_and(|f| has_extension(f, "js")))
        .count();
    assert_eq!(
        assets.len(),
        embedded_js,
        "the binary embeds {embedded_js} .js assets but this test checks {}. \
         Add the new one here, or its imports go unverified -- which is exactly \
         how three.core.min.js came to be imported but never served.",
        assets.len()
    );

    for (served_at, src) in assets {
        let base = served_at.rsplit_once('/').unwrap().0;

        // Specifiers, not lines: minified three is one line. Only relative ones
        // resolve against the serving path; bare ones are the importmap's.
        for (i, _) in src.match_indices("from") {
            let rest = src[i + 4..].trim_start();
            let Some(quote) = rest.chars().next().filter(|c| *c == '"' || *c == '\'') else {
                continue;
            };
            let Some(spec) = rest[1..].split(quote).next() else {
                continue;
            };
            if !(spec.starts_with("./") || spec.starts_with("../")) || spec.len() > 200 {
                continue;
            }

            let mut resolved = base.to_string();
            for seg in spec.split('/') {
                match seg {
                    "." => {}
                    ".." => resolved = resolved.rsplit_once('/').unwrap().0.to_string(),
                    other => {
                        resolved.push('/');
                        resolved.push_str(other);
                    }
                }
            }

            assert!(
                routes.contains(&format!("\"{resolved}\"")),
                "{served_at} imports {spec:?}, which resolves to {resolved} -- \
                 no route serves that, so it 404s and the module never loads"
            );
        }
    }
}

/// Every external resource (a texture URI) an embedded model references is served.
#[test]
#[cfg(feature = "game")]
fn every_external_resource_a_model_references_is_served() {
    let routes = include_str!("../src/http.rs");
    let models: [(&str, &[u8]); 3] = [
        (
            "box-small.glb",
            include_bytes!("../assets/models/box-small.glb"),
        ),
        (
            "conveyor-long.glb",
            include_bytes!("../assets/models/conveyor-long.glb"),
        ),
        ("crane.glb", include_bytes!("../assets/models/crane.glb")),
    ];

    for (name, bytes) in models {
        // glTF-Binary: 12-byte header, then length-prefixed chunks. The first
        // chunk is JSON; its `uri` fields are the external references.
        assert_eq!(&bytes[0..4], b"glTF", "{name} is not a GLB");
        let json_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
        let json = std::str::from_utf8(&bytes[20..20 + json_len]).expect("GLB JSON chunk");

        for (i, _) in json.match_indices("\"uri\"") {
            let rest = &json[i + 5..];
            let Some(start) = rest.find('"') else {
                continue;
            };
            let Some(uri) = rest[start + 1..].split('"').next() else {
                continue;
            };

            // Data URIs are self-contained; only file references can 404.
            if uri.starts_with("data:") {
                continue;
            }

            let resolved = format!("/game/models/{uri}");
            assert!(
                routes.contains(&format!("\"{resolved}\"")),
                "{name} references {uri:?}, which resolves to {resolved} -- no \
                 route serves it, so the model loads but renders untextured"
            );
        }
    }
}

/// Every state summed into the denominator must also be drawable.
#[test]
#[cfg(feature = "game")]
fn every_state_in_the_denominator_can_be_drawn() {
    let src = include_str!("../src/http/game.rs");

    let field_names = |frag: &str| -> Vec<String> {
        frag.match_indices("queue.")
            .map(|(i, _)| {
                frag[i + 6..]
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect()
            })
            .collect()
    };

    let total = src
        .split("const total = ")
        .nth(1)
        .and_then(|r| r.split(';').next())
        .expect("the total expression");
    let states = src
        .split("const states = [")
        .nth(1)
        .and_then(|r| r.split("];").next())
        .expect("the states array");

    for state in field_names(total) {
        assert!(
            field_names(states).contains(&state),
            "queue.{state} is summed into `total` but is not in the states \
             array, so it dilutes every other state's share of the boxes and \
             is never drawn itself"
        );
    }
}
