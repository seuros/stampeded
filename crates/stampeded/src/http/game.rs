//! 3D factory-floor view of the queue: belts are queues, boxes are sampled jobs.
//!
//! Read-only and token-gated like the dashboard. three.js and Datastar come from
//! rama-x-threejs under [`THREE`]; the scene (`assets/game.js`) and its models
//! are embedded here, and the world reaches the scene as a Datastar signal.

use super::ApiState;
use rama::futures::stream;
use rama::http::StatusCode;
use rama::http::header;
use rama::http::service::web::extract::State;
use rama::http::service::web::response::{Html, IntoResponse, Script, Sse};
use rama::http::sse::server::KeepAlive;
use rama_x_threejs::{ImportMap, SceneSignals};
use serde::Serialize;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// The game world as JSON state, streamed live.
#[derive(Debug, Serialize, Clone)]
pub struct GameWorld {
    /// Queue conveyors: name, state counts, positions.
    pub queues: Vec<QueueConveyor>,
    /// Active workers: where they are, what they're doing.
    pub workers: Vec<WorkerEntity>,
    /// Destination systems: health, backlog.
    pub destinations: Vec<DestinationEntity>,
    /// Timestamp of this world state (milliseconds since epoch).
    pub timestamp_ms: u64,
}

#[derive(Debug, Serialize, Clone)]
pub struct QueueConveyor {
    /// Queue name.
    pub name: String,
    /// Number of jobs in each state (representative boxes rendered).
    pub available: i64,
    pub executing: i64,
    pub completed: i64,
    pub retryable: i64,
    pub discarded: i64,
    pub cancelled: i64,
    /// Position on the factory floor (x, y, z).
    pub position: [f32; 3],
    /// Jobs completing per second, from the `completed` delta between builds.
    pub throughput_per_sec: f64,
    /// Oldest waiting job's age in seconds, if any.
    pub oldest_age_secs: Option<i32>,
}

#[derive(Debug, Serialize, Clone)]
pub struct WorkerEntity {
    /// Worker ID.
    pub id: String,
    /// `hostname:pid`, the way an operator identifies a process.
    pub name: String,
    /// What queue this worker claims from.
    pub queue: String,
    /// Is this worker still heartbeating?
    pub alive: bool,
    /// Last heartbeat timestamp (seconds ago).
    pub last_heartbeat_secs_ago: i32,
    /// Position on the floor.
    pub position: [f32; 3],
}

#[derive(Debug, Serialize, Clone)]
pub struct DestinationEntity {
    /// Destination name (external service).
    pub name: String,
    /// Number of jobs currently waiting for this destination.
    pub waiting: i64,
    /// Number of recent failures.
    pub failed_recently: i64,
    /// Inferred breaker status: open, failing or closed.
    pub breaker_state: String,
    /// Position on the floor.
    pub position: [f32; 3],
}

const INDEX: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Stampeded - 3D Game World</title>
<style>
  body {
    margin: 0;
    overflow: hidden;
    background: #0d0e14;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }
  #canvas {
    display: block;
    width: 100vw;
    height: 100vh;
  }
  #hud {
    position: absolute;
    top: 1rem;
    left: 1rem;
    color: #00f0ff;
    font-size: 12px;
    background: rgba(13, 14, 20, 0.9);
    padding: 1rem;
    border: 1px solid #00f0ff;
    max-width: 300px;
    max-height: 40vh;
    overflow-y: auto;
  }
  #hud h2 {
    margin: 0 0 0.5rem 0;
    font-size: 14px;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #00f0ff;
  }
  #hud h3 {
    margin: 0.5rem 0 0.25rem 0;
    font-size: 11px;
    color: #ff2a6d;
  }
  #hud div {
    margin: 0.25rem 0;
    font-size: 11px;
    color: #b2b7c7;
  }
  #stats {
    position: absolute;
    bottom: 1rem;
    right: 1rem;
    color: #05ffa1;
    font-size: 11px;
    background: rgba(13, 14, 20, 0.9);
    padding: 0.75rem;
    border: 1px solid #05ffa1;
  }
</style>
</head>
<body data-init="@get('/game/stream?token=' + encodeURIComponent(new URLSearchParams(location.search).get('token') ?? ''))">
<canvas id="canvas" data-three:world="/game/game.js"></canvas>
<div id="hud">
  <h2>Factory Floor</h2>
  <div id="queue-info">Connecting...</div>
</div>
<div id="stats">
  <div>FPS: <span id="fps">--</span></div>
  <div id="world-stats">Initializing...</div>
</div>
__IMPORTMAP__
<!-- The hook brings Datastar, which opens the stream, and loads the scene
     named by data-three:world. -->
<script type="module">import "rama-x-threejs"</script>
</body>
</html>"#;

/// Where `with_three` mounts three.js and Datastar for the game page. Public,
/// like the scene and its models: the page and the stream are the gated part.
pub const THREE: &str = "/game/three";

/// INDEX with its import map filled in.
static PAGE: LazyLock<String> =
    LazyLock::new(|| INDEX.replace("__IMPORTMAP__", &ImportMap::new(THREE).to_string()));

pub async fn index() -> impl IntoResponse {
    Html(PAGE.as_str())
}

/// A single world feed shared by every viewer, polled while anyone watches.
fn world_feed(state: &ApiState) -> watch::Receiver<Arc<GameWorld>> {
    // Keep the sender, not a receiver, so send() can report that nobody is watching.
    static FEED: OnceLock<(watch::Sender<Arc<GameWorld>>, AtomicBool)> = OnceLock::new();

    let (tx, running) = FEED.get_or_init(|| {
        let (tx, _rx) = watch::channel(Arc::new(GameWorld {
            queues: Vec::new(),
            workers: Vec::new(),
            destinations: Vec::new(),
            timestamp_ms: 0,
        }));
        (tx, AtomicBool::new(false))
    });

    // Subscribe before the spawn check so a new builder never sees zero receivers.
    let rx = tx.subscribe();

    if running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        let tx = tx.clone();
        let state = state.clone();

        tokio::spawn(async move {
            let mut last: HashMap<String, (i64, Instant, f64)> = HashMap::new();

            loop {
                let mut world = build_world_state(&state).await;
                let now = Instant::now();

                for q in &mut world.queues {
                    if let Some((prev_completed, at, prev_rate)) = last.get(&q.name) {
                        let secs = now.duration_since(*at).as_secs_f64();
                        let delta = q.completed - prev_completed;

                        // A falling count means the pruner ran: carry the last rate.
                        q.throughput_per_sec = if delta >= 0 && secs > 0.0 {
                            delta as f64 / secs
                        } else {
                            *prev_rate
                        };
                    }
                    last.insert(q.name.clone(), (q.completed, now, q.throughput_per_sec));
                }

                if tx.send(Arc::new(world)).is_err() {
                    // Nobody is watching. Re-check after releasing the flag: a
                    // viewer may have subscribed between the send and the store.
                    running.store(false, Ordering::Release);
                    if tx.receiver_count() > 0
                        && running
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                    {
                        continue;
                    }
                    break;
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }

    rx
}

pub async fn stream(State(state): State<ApiState>) -> impl IntoResponse {
    // Subscribe to the shared feed; do not start a private poll loop.
    let ticks = stream::unfold(
        (world_feed(&state), true),
        move |(mut rx, first)| async move {
            // timestamp_ms == 0 is the empty placeholder; wait for a real world.
            if !first || rx.borrow_and_update().timestamp_ms == 0 {
                // Err: the feed's builder is gone, so the stream is over.
                rx.changed().await.ok()?;
            }

            let world = rx.borrow().clone();
            let event = SceneSignals::new("world", &*world)
                .try_into_datastar_event()
                .inspect_err(|e| tracing::warn!(error = %e, "game world did not serialize"))
                .ok()?;
            Some((Ok::<_, Infallible>(event), (rx, false)))
        },
    );

    // Sse sets Cache-Control: no-cache itself.
    Sse::new(ticks).with_keep_alive(KeepAlive::new())
}

type QueueState = (i64, i64, i64, i64, i64, i64, Option<i32>);

/// Run a read under the dashboard's statement_timeout, in a transaction for SET LOCAL.
async fn timed_read<T>(state: &ApiState, sql: &'static str) -> Vec<T>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin,
{
    match try_timed_read(state, sql).await {
        Ok(rows) => rows,
        Err(e) => {
            // Logged: an empty world must not hide a broken query.
            tracing::warn!(error = %e, "game world query failed; rendering it empty");
            Vec::new()
        }
    }
}

async fn try_timed_read<T>(state: &ApiState, sql: &'static str) -> crate::Result<Vec<T>>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin,
{
    let mut tx = state
        .queue
        .begin_timed(crate::queue::DASHBOARD_QUERY_TIMEOUT)
        .await?;
    let rows = sqlx::query_as::<_, T>(sql).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(rows)
}

async fn build_world_state(state: &ApiState) -> GameWorld {
    let queue_rows: Vec<(String, String, i64)> = timed_read(
        state,
        "SELECT queue, state, n FROM stampeded_queue_counts WHERE n <> 0 ORDER BY queue, state",
    )
    .await;

    let ages = state
        .queue
        .oldest_available_age()
        .await
        .inspect_err(|e| tracing::warn!(error = %e, "game world age query failed"))
        .unwrap_or_default();

    let mut queue_map: std::collections::HashMap<String, QueueState> =
        std::collections::HashMap::default();
    for (queue, st, n) in queue_rows {
        let entry = queue_map.entry(queue).or_insert((0, 0, 0, 0, 0, 0, None));
        match st.as_str() {
            "available" => entry.0 = n,
            "executing" => entry.1 = n,
            "completed" => entry.2 = n,
            "retryable" => entry.3 = n,
            "discarded" => entry.4 = n,
            "cancelled" => entry.5 = n,
            _ => {}
        }
    }

    for (queue_name, age) in &ages {
        if let Some(entry) = queue_map.get_mut(queue_name)
            && (entry.0 > 0 || entry.3 > 0)
        {
            // Whole seconds; float-to-int `as` saturates past i32::MAX.
            #[expect(clippy::cast_possible_truncation)]
            let secs = *age as i32;
            entry.6 = Some(secs);
        }
    }

    let mut queues = Vec::new();
    let mut x_offset = 0.0;
    for (
        queue_name,
        (available, executing, completed, retryable, discarded, cancelled, oldest_age),
    ) in queue_map
    {
        queues.push(QueueConveyor {
            name: queue_name,
            available,
            executing,
            completed,
            retryable,
            discarded,
            cancelled,
            position: [x_offset, 0.0, 0.0],
            throughput_per_sec: 0.0,
            oldest_age_secs: oldest_age,
        });
        x_offset += 15.0;
    }

    let worker_rows: Vec<(String, String, String, i32)> = timed_read(
        state,
        "SELECT id::text, hostname || ':' || pid, queues::text,
                EXTRACT(EPOCH FROM (NOW() - last_heartbeat))::int AS secs_ago
           FROM stampeded_workers
          ORDER BY last_heartbeat DESC
          LIMIT 20",
    )
    .await;

    let mut workers = Vec::new();
    let mut w_offset = 0.0;
    for (id, name, queue_str, secs_ago) in worker_rows {
        // A Postgres text array, e.g. "{default,mail}".
        let queue = queue_str
            .trim_matches(|c| c == '{' || c == '}')
            .split(',')
            .next()
            .unwrap_or("default")
            .to_string();

        workers.push(WorkerEntity {
            id,
            name,
            queue,
            alive: secs_ago < 300, // Alive if heartbeat < 5 minutes ago
            last_heartbeat_secs_ago: secs_ago,
            position: [w_offset, 2.0, 5.0],
        });
        w_offset += 2.0;
    }

    let dest_rows = state
        .queue
        .destination_health(Duration::from_secs(3600))
        .await
        .inspect_err(|e| tracing::warn!(error = %e, "game world destination query failed"))
        .unwrap_or_default();

    let mut destinations = Vec::new();
    let mut d_offset = 0.0;
    for row in dest_rows {
        destinations.push(DestinationEntity {
            name: row.destination.unwrap_or_else(|| "unknown".to_string()),
            waiting: row.waiting,
            failed_recently: row.failed,
            breaker_state: if row.refused > 0 {
                "open".to_string()
            } else if row.failed > 0 {
                "failing".to_string()
            } else {
                "closed".to_string()
            },
            position: [d_offset, 1.0, -10.0],
        });
        d_offset += 3.0;
    }

    GameWorld {
        queues,
        workers,
        destinations,
        timestamp_ms: u64::try_from(jiff::Timestamp::now().as_millisecond()).unwrap_or(0),
    }
}

/// The scene module the canvas names in `data-three:world`.
pub async fn scene() -> impl IntoResponse {
    Script(include_str!("../../assets/game.js"))
}

/// A static asset served whole, with its content type.
fn asset(content_type: &'static str, bytes: &'static [u8]) -> rama::http::Response {
    ([(header::CONTENT_TYPE, content_type)], bytes).into_response()
}

/// Serve the Factory Kit's texture atlas, which every GLB references externally.
///
/// Each Kenney kit has its own colormap; only Factory Kit's has the right colours.
pub async fn model_texture() -> impl IntoResponse {
    asset(
        "image/png",
        include_bytes!("../../assets/models/Textures/colormap.png"),
    )
}

/// Embedded 3D models, keyed by the filename `model()` is asked for.
const MODELS: &[(&str, &[u8])] = &[
    (
        "box-small.glb",
        include_bytes!("../../assets/models/box-small.glb"),
    ),
    (
        "box-large.glb",
        include_bytes!("../../assets/models/box-large.glb"),
    ),
    (
        "conveyor-long.glb",
        include_bytes!("../../assets/models/conveyor-long.glb"),
    ),
    (
        "conveyor-corner.glb",
        include_bytes!("../../assets/models/conveyor-corner.glb"),
    ),
    ("crane.glb", include_bytes!("../../assets/models/crane.glb")),
    (
        "robot-arm-a.glb",
        include_bytes!("../../assets/models/robot-arm-a.glb"),
    ),
    (
        "hopper-square.glb",
        include_bytes!("../../assets/models/hopper-square.glb"),
    ),
    (
        "hopper-high-square.glb",
        include_bytes!("../../assets/models/hopper-high-square.glb"),
    ),
    (
        "machine.glb",
        include_bytes!("../../assets/models/machine.glb"),
    ),
    (
        "pipe-large-long.glb",
        include_bytes!("../../assets/models/pipe-large-long.glb"),
    ),
    (
        "structure-tall.glb",
        include_bytes!("../../assets/models/structure-tall.glb"),
    ),
    ("floor.glb", include_bytes!("../../assets/models/floor.glb")),
    ("cog-a.glb", include_bytes!("../../assets/models/cog-a.glb")),
    ("door.glb", include_bytes!("../../assets/models/door.glb")),
];

/// Serve a 3D model file from embedded assets.
pub fn model(model_name: &str) -> rama::http::Response {
    match MODELS.iter().find(|(name, _)| *name == model_name) {
        Some((_, bytes)) => asset("model/gltf-binary", bytes),
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

#[cfg(test)]
mod tests;
