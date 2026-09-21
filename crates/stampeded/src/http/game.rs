//! 3D game world visualization of the job queue state.
//!
//! The metaphor: a factory floor where queues are conveyor belts, jobs are boxes
//! moving through states, workers are cranes/robots picking up boxes, and
//! destinations are loading docks.
//!
//! The world is read-only and token-gated identically to the dashboard.
//! All assets are embedded in the binary behind the `game` feature flag.
//!
//! Scale handling: A real queue can hold 200k+ jobs. We render 8-12 representative
//! boxes per queue, sampled proportionally by state, with actual counts shown
//! as labels. Workers and destinations are rendered as actual entities (not
//! many of those).

use super::ApiState;
use rama::http::StatusCode;
use rama::http::header;
use rama::http::service::web::extract::{Query, State};
use rama::http::service::web::response::{Html, IntoResponse};
use rama::futures::stream;
use rama::http::sse::Event;
use rama::http::sse::server::KeepAlive;
use rama::http::service::web::response::Sse;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::time::Duration;

#[derive(Debug, Deserialize, Default)]
pub struct GameParams {
    #[serde(default)]
    pub token: Option<String>,
}

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
    /// Oldest waiting job's age in seconds, if any.
    pub oldest_age_secs: Option<i32>,
}

#[derive(Debug, Serialize, Clone)]
pub struct WorkerEntity {
    /// Worker ID or name.
    pub id: String,
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
    /// Circuit breaker status (open, closed, half-open).
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
<body>
<canvas id="canvas"></canvas>
<div id="hud">
  <h2>Factory Floor</h2>
  <div id="queue-info">Connecting...</div>
</div>
<div id="stats">
  <div>FPS: <span id="fps">--</span></div>
  <div id="world-stats">Initializing...</div>
</div>

<script type="importmap">
{
  "imports": {
    "three": "/game/three.module.js",
    "gltf": "/game/loaders/gltf-loader.js"
  }
}
</script>

<script type="module">
import * as THREE from "three";
import { GLTFLoader } from "gltf";

// The Kenney models were embedded in the binary and routed, and then nothing
// ever asked for them: not one loader.load() call anywhere, and the scene was
// built entirely from BoxGeometry and ConeGeometry. 1.6 MB of assets shipped
// as decoration. These four are the ones the world actually uses.
const MODEL_FILES = {
  box: "box-small.glb",
  conveyor: "conveyor-long.glb",
  crane: "crane.glb",
  hopper: "hopper-square.glb",
};
const MODELS = {};

// Normalise each model to a known size. Kenney's pieces do not share a scale,
// and the layout below is written in world units, not model units.
function fitTo(obj, targetSize) {
  const bounds = new THREE.Box3().setFromObject(obj);
  const size = new THREE.Vector3();
  bounds.getSize(size);
  const largest = Math.max(size.x, size.y, size.z);
  if (largest > 0) obj.scale.setScalar(targetSize / largest);
  return obj;
}

const TARGET_SIZE = { box: 0.8, conveyor: 10, crane: 2.5, hopper: 2 };

(function loadModels() {
  const loader = new GLTFLoader();
  for (const [key, file] of Object.entries(MODEL_FILES)) {
    loader.load(
      `/game/models/${file}`,
      (gltf) => {
        const root = gltf.scene;
        root.traverse((n) => {
          if (n.isMesh) { n.castShadow = true; n.receiveShadow = true; }
        });
        MODELS[key] = fitTo(root, TARGET_SIZE[key]);
      },
      undefined,
      // A missing model must not take the page down. updateScene rebuilds the
      // world every tick, so the primitive fallback just keeps being used.
      (err) => console.warn(`model ${file} failed to load; using primitive`, err),
    );
  }
})();

// A clone of a loaded model, or null if it has not arrived (or failed).
function modelClone(key) {
  const m = MODELS[key];
  return m ? m.clone(true) : null;
}

// Recolour a clone without touching the shared source material.
function tint(obj, color) {
  obj.traverse((n) => {
    if (n.isMesh && n.material) {
      n.material = n.material.clone();
      n.material.color = new THREE.Color(color);
    }
  });
  return obj;
}

const canvas = document.getElementById("canvas");
const scene = new THREE.Scene();
const camera = new THREE.PerspectiveCamera(75, window.innerWidth / window.innerHeight, 0.1, 1000);
const renderer = new THREE.WebGLRenderer({ canvas, antialias: true });

renderer.setSize(window.innerWidth, window.innerHeight);
renderer.setClearColor(0x0d0e14);
scene.background = new THREE.Color(0x0d0e14);

// Lighting
const ambientLight = new THREE.AmbientLight(0xffffff, 0.6);
scene.add(ambientLight);
const directionalLight = new THREE.DirectionalLight(0xffffff, 0.7);
directionalLight.position.set(15, 25, 15);
directionalLight.castShadow = true;
scene.add(directionalLight);

camera.position.set(0, 18, 35);
camera.lookAt(0, 0, 0);

let worldData = null;
let fps = 0;
let frameCount = 0;
let lastTime = performance.now();
let sceneObjects = { queues: [], workers: [], destinations: [] };

// Create floor
const floorGeometry = new THREE.PlaneGeometry(120, 120);
const floorMaterial = new THREE.MeshStandardMaterial({ color: 0x181a25 });
const floor = new THREE.Mesh(floorGeometry, floorMaterial);
floor.rotation.x = -Math.PI / 2;
floor.receiveShadow = true;
scene.add(floor);

// Helper: create a colored box (representing a job)
function createJobBox(state) {
  const colors = {
    available: 0x4db8ff,    // blue
    executing: 0x00f0ff,    // cyan
    retryable: 0xfcee0a,    // yellow
    discarded: 0xff2a6d,    // magenta
    cancelled: 0x808080,    // gray
    completed: 0x05ffa1,    // green
  };
  const color = colors[state] || 0xb2b7c7;
  const model = modelClone("box");
  if (model) return tint(model, color);

  const geo = new THREE.BoxGeometry(0.8, 0.8, 0.8);
  const mat = new THREE.MeshStandardMaterial({ color });
  const box = new THREE.Mesh(geo, mat);
  box.castShadow = true;
  box.receiveShadow = true;
  return box;
}

// Helper: create a conveyor belt visual
function createConveyorBelt(length = 8) {
  const model = modelClone("conveyor");
  if (model) return fitTo(model, length);

  const geo = new THREE.BoxGeometry(length, 0.4, 1.5);
  const mat = new THREE.MeshStandardMaterial({ color: 0x333333 });
  const belt = new THREE.Mesh(geo, mat);
  belt.castShadow = true;
  belt.receiveShadow = true;
  return belt;
}

// Helper: create a simple crane (worker)
function createCrane() {
  const model = modelClone("crane");
  if (model) return model;

  const group = new THREE.Group();

  // Base
  const baseGeo = new THREE.BoxGeometry(1, 0.3, 1);
  const baseMat = new THREE.MeshStandardMaterial({ color: 0xffcc00 });
  const base = new THREE.Mesh(baseGeo, baseMat);
  base.receiveShadow = true;
  group.add(base);

  // Arm
  const armGeo = new THREE.BoxGeometry(0.2, 2, 0.2);
  const armMat = new THREE.MeshStandardMaterial({ color: 0xff9900 });
  const arm = new THREE.Mesh(armGeo, armMat);
  arm.position.z = -0.4;
  arm.position.y = 1.2;
  arm.castShadow = true;
  arm.receiveShadow = true;
  group.add(arm);

  return group;
}

// Helper: create a hopper (destination)
function createHopper() {
  const model = modelClone("hopper");
  if (model) return model;

  const group = new THREE.Group();

  // Main body
  const bodyGeo = new THREE.ConeGeometry(1, 2, 8);
  const bodyMat = new THREE.MeshStandardMaterial({ color: 0x444444 });
  const body = new THREE.Mesh(bodyGeo, bodyMat);
  body.castShadow = true;
  body.receiveShadow = true;
  group.add(body);

  return group;
}

// Update scene from world data
function updateScene(world) {
  if (!world.queues) return;

  // Clear old queue visualizations
  sceneObjects.queues.forEach(obj => scene.remove(obj));
  sceneObjects.queues = [];

  // Create conveyor belts and job boxes
  let queueIndex = 0;
  for (const queue of world.queues) {
    const x = -30 + queueIndex * 15;
    const z = 0;

    // Add conveyor belt
    const belt = createConveyorBelt(10);
    belt.position.set(x, 0.3, z);
    scene.add(belt);
    sceneObjects.queues.push(belt);

    // Calculate total jobs and add representative boxes
    const total = queue.available + queue.executing + queue.retryable + queue.discarded + queue.cancelled + queue.completed;
    if (total === 0) {
      queueIndex++;
      continue;
    }

    // Show up to 8 representative jobs
    const boxesToShow = Math.min(8, Math.ceil(total / Math.max(1, total / 8)));
    const states = [
      { name: 'available', count: queue.available },
      { name: 'executing', count: queue.executing },
      { name: 'retryable', count: queue.retryable },
      { name: 'discarded', count: queue.discarded },
      { name: 'cancelled', count: queue.cancelled },
    ];

    let boxIndex = 0;
    for (const state of states) {
      if (state.count === 0) continue;
      const boxesForState = Math.ceil((state.count / total) * boxesToShow);
      for (let i = 0; i < boxesForState && boxIndex < boxesToShow; i++) {
        const box = createJobBox(state.name);
        box.position.set(
          x - 4 + (boxIndex % 4) * 2,
          1.5 + Math.sin(boxIndex) * 0.2,
          z - 1 + Math.floor(boxIndex / 4) * 1.5
        );
        scene.add(box);
        sceneObjects.queues.push(box);
        boxIndex++;
      }
    }

    // Add queue label (as a small colored indicator)
    const labelGeo = new THREE.SphereGeometry(0.3, 8, 8);
    const labelMat = new THREE.MeshStandardMaterial({ color: 0x00f0ff });
    const label = new THREE.Mesh(labelGeo, labelMat);
    label.position.set(x, 2.5, z + 2);
    scene.add(label);
    sceneObjects.queues.push(label);

    queueIndex++;
  }

  // Clear and recreate workers
  sceneObjects.workers.forEach(obj => scene.remove(obj));
  sceneObjects.workers = [];
  for (let i = 0; i < (world.workers?.length || 0); i++) {
    const w = world.workers[i];
    const crane = createCrane();
    crane.position.set(w.position[0], w.position[1], w.position[2]);
    scene.add(crane);
    sceneObjects.workers.push(crane);
  }

  // Clear and recreate destinations
  sceneObjects.destinations.forEach(obj => scene.remove(obj));
  sceneObjects.destinations = [];
  for (let i = 0; i < (world.destinations?.length || 0); i++) {
    const d = world.destinations[i];
    const hopper = createHopper();
    hopper.position.set(d.position[0], d.position[1], d.position[2]);
    scene.add(hopper);
    sceneObjects.destinations.push(hopper);
  }
}

// Connect to game stream
const token = new URLSearchParams(window.location.search).get("token") || "";
const eventSource = new EventSource(`/game/stream?token=${encodeURIComponent(token)}`);

// addEventListener, NOT onmessage. The server NAMES this event "world"
// (try_with_event), and onmessage fires ONLY for unnamed events -- the
// default "message" type. With onmessage the stream connects, the server
// polls the database every 500ms forever, and not one frame reaches the
// scene: the page sits at "Initializing..." looking like a slow query.
eventSource.addEventListener("world", (event) => {
  try {
    worldData = JSON.parse(event.data);
    updateScene(worldData);
    updateHUD(worldData);
  } catch (e) {
    console.error("Failed to parse world data:", e);
  }
});

eventSource.onerror = () => {
  document.getElementById("queue-info").textContent = "Connection lost";
  eventSource.close();
};

function updateHUD(world) {
  if (!world.queues) return;

  let html = "<h3>Queues</h3>";
  for (const queue of world.queues) {
    const total = queue.available + queue.executing + queue.completed + queue.retryable + queue.discarded + queue.cancelled;
    html += `<div><strong>${queue.name}</strong>: ${total} jobs`;
    if (queue.available > 0) html += ` <span style="color: #4db8ff">${queue.available}a</span>`;
    if (queue.executing > 0) html += ` <span style="color: #00f0ff">${queue.executing}e</span>`;
    if (queue.retryable > 0) html += ` <span style="color: #fcee0a">${queue.retryable}r</span>`;
    if (queue.discarded > 0) html += ` <span style="color: #ff2a6d">${queue.discarded}d</span>`;
    html += "</div>";
  }

  if (world.workers && world.workers.length > 0) {
    html += `<h3>Workers (${world.workers.length})</h3>`;
    for (const w of world.workers.slice(0, 3)) {
      const status = w.alive ? `<span style="color: #05ffa1">✓</span>` : `<span style="color: #ff2a6d">✗</span>`;
      html += `<div>${status} ${w.id}</div>`;
    }
  }

  if (world.destinations && world.destinations.length > 0) {
    html += `<h3>Destinations</h3>`;
    for (const d of world.destinations.slice(0, 3)) {
      html += `<div>${d.name}: ${d.waiting}</div>`;
    }
  }

  document.getElementById("queue-info").innerHTML = html;
  document.getElementById("world-stats").textContent =
    `Queues: ${world.queues.length} | Workers: ${world.workers?.length || 0} | Dest: ${world.destinations?.length || 0}`;
}

function animate() {
  requestAnimationFrame(animate);

  frameCount++;
  const now = performance.now();
  if (now - lastTime >= 1000) {
    fps = frameCount;
    frameCount = 0;
    lastTime = now;
    document.getElementById("fps").textContent = fps;
  }

  // Gentle rotation for visual interest
  sceneObjects.queues.forEach(obj => {
    if (obj.rotation) obj.rotation.y += 0.0001;
  });

  renderer.render(scene, camera);
}

animate();

window.addEventListener("resize", () => {
  camera.aspect = window.innerWidth / window.innerHeight;
  camera.updateProjectionMatrix();
  renderer.setSize(window.innerWidth, window.innerHeight);
});
</script>
</body>
</html>"#;

pub async fn index(State(state): State<ApiState>, Query(p): Query<GameParams>) -> impl IntoResponse {
    if !state.dashboard_authorised(p.token.as_deref()) {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }
    Html(INDEX).into_response()
}

pub async fn stream(State(state): State<ApiState>, Query(p): Query<GameParams>) -> impl IntoResponse {
    if !state.dashboard_authorised(p.token.as_deref()) {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }

    // Stream JSON world state over SSE
    let ticks = stream::unfold(
        (state, true),
        move |(state, first)| async move {
            if !first {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            let world = build_world_state(&state).await;
            let json_str = serde_json::to_string(&world).ok()?;

            // Create SSE event with JSON data
            let event = Event::new()
                .try_with_event("world")
                .ok()?
                .with_data(json_str);
            Some((Ok::<_, Infallible>(event), (state, false)))
        },
    );

    (
        [(header::CACHE_CONTROL, "no-cache")],
        Sse::new(ticks).with_keep_alive(KeepAlive::new()),
    )
        .into_response()
}

type QueueState = (i64, i64, i64, i64, i64, i64, Option<i32>);

/// Run a read on a connection carrying the dashboard's statement_timeout.
///
/// Every /game/stream connection re-runs these every 500ms, so an unbounded
/// read here is worse than on the dashboard: a slow query does not just stall
/// one page load, it pins a pool connection per viewer and keeps doing it.
/// SET LOCAL needs an explicit transaction to mean anything -- see
/// Queue::recent_jobs, where issuing it on an autocommit connection made it a
/// silent no-op.
async fn timed_read<T>(state: &ApiState, sql: &'static str) -> Vec<T>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin,
{
    let Ok(mut tx) = state.queue.pool().begin().await else {
        return Vec::new();
    };
    if sqlx::query(sqlx::AssertSqlSafe(format!(
        "SET LOCAL statement_timeout = {}",
        crate::queue::DASHBOARD_QUERY_TIMEOUT.as_millis()
    )))
    .execute(&mut *tx)
    .await
    .is_err()
    {
        return Vec::new();
    }
    let rows = match sqlx::query_as::<_, T>(sql).fetch_all(&mut *tx).await {
        Ok(rows) => rows,
        Err(e) => {
            // NOT silent. The worker query referenced heartbeat_at and `alive`,
            // neither of which exists on stampeded_workers, so it failed on
            // every single tick -- and unwrap_or_default rendered that as an
            // empty worker list. The world looked idle instead of broken.
            tracing::warn!(error = %e, "game world query failed; rendering it empty");
            Vec::new()
        }
    };
    let _ = tx.commit().await;
    rows
}

async fn build_world_state(state: &ApiState) -> GameWorld {
    // Fetch queue depth information
    let queue_rows: Vec<(String, String, i64)> = timed_read(
        state,
        "SELECT queue, state, n FROM stampeded_queue_counts WHERE n <> 0 ORDER BY queue, state",
    )
    .await;

    let ages = state.queue.oldest_available_age().await.unwrap_or_default();

    // Group by queue
    let mut queue_map: std::collections::HashMap<String, QueueState> = Default::default();
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

    // Add age for waiting states
    for (queue_name, age) in &ages {
        if let Some(entry) = queue_map.get_mut(queue_name)
            && (entry.0 > 0 || entry.3 > 0)
        {
            // Has available or retryable jobs
            entry.6 = Some(*age as i32);
        }
    }

    // Build conveyor entities
    let mut queues = Vec::new();
    let mut x_offset = 0.0;
    for (queue_name, (available, executing, completed, retryable, discarded, cancelled, oldest_age)) in queue_map {
        queues.push(QueueConveyor {
            name: queue_name,
            available,
            executing,
            completed,
            retryable,
            discarded,
            cancelled,
            position: [x_offset, 0.0, 0.0],
            oldest_age_secs: oldest_age,
        });
        x_offset += 15.0;
    }

    // Fetch worker information
    let worker_rows: Vec<(String, String, i32)> = timed_read(
        state,
        // last_heartbeat, NOT heartbeat_at, and there is no `alive` column on
        // stampeded_workers -- see the table definition. The original query
        // referenced all three, so it errored on every tick and unwrap_or_default
        // turned that into an empty worker list. The world never showed a single
        // worker and looked merely idle.
        "SELECT id::text, queues::text,
                EXTRACT(EPOCH FROM (NOW() - last_heartbeat))::int AS secs_ago
           FROM stampeded_workers
          ORDER BY last_heartbeat DESC
          LIMIT 20",
    )
    .await;

    let mut workers = Vec::new();
    let mut w_offset = 0.0;
    for (id, queue_str, secs_ago) in worker_rows {
        // Parse queue string (PostgreSQL text array format like "{default,mail}")
        let queue = queue_str
            .trim_matches(|c| c == '{' || c == '}')
            .split(',')
            .next()
            .unwrap_or("default")
            .to_string();

        workers.push(WorkerEntity {
            id,
            queue,
            alive: secs_ago < 300, // Alive if heartbeat < 5 minutes ago
            last_heartbeat_secs_ago: secs_ago,
            position: [w_offset, 2.0, 5.0],
        });
        w_offset += 2.0;
    }

    // Fetch destination health information
    let dest_rows = state
        .queue
        .destination_health(Duration::from_secs(3600))
        .await
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
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
    }
}

/// Serve Three.js library from embedded bytes.
pub async fn three_module() -> impl IntoResponse {
    const THREE_JS: &[u8] = include_bytes!("../../assets/three.module.js");

    (
        [(header::CONTENT_TYPE, "application/javascript")],
        THREE_JS,
    )
        .into_response()
}

/// Serve GLTFLoader from embedded bytes.
pub async fn gltf_loader() -> impl IntoResponse {
    const GLTF_LOADER: &[u8] = include_bytes!("../../assets/gltf-loader.js");

    (
        [(header::CONTENT_TYPE, "application/javascript")],
        GLTF_LOADER,
    )
        .into_response()
}

/// Serve GLTFLoader's addon dependencies from embedded bytes.
///
/// GLTFLoader does `import ... from '../utils/BufferGeometryUtils.js'`. Served
/// from /game/gltf-loader.js that resolved to /utils/BufferGeometryUtils.js,
/// which nothing served -- a 404, so the module never evaluated, the loader
/// never existed, and every model silently fell back to a primitive. Serving
/// the loader from /game/loaders/ instead makes ../utils/ land on /game/utils/,
/// so three's own relative imports resolve untouched. Do not flatten these
/// paths without re-reading the import statements.
pub async fn buffer_geometry_utils() -> impl IntoResponse {
    const SRC: &[u8] = include_bytes!("../../assets/utils/BufferGeometryUtils.js");

    ([(header::CONTENT_TYPE, "application/javascript")], SRC).into_response()
}

/// See buffer_geometry_utils: GLTFLoader imports `clone` from this.
pub async fn skeleton_utils() -> impl IntoResponse {
    const SRC: &[u8] = include_bytes!("../../assets/utils/SkeletonUtils.js");

    ([(header::CONTENT_TYPE, "application/javascript")], SRC).into_response()
}

/// Serve a 3D model file from embedded assets.
pub async fn model(model_name: String) -> impl IntoResponse {
    match model_name.as_str() {
        "box-small.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/box-small.glb").as_ref(),
        )
            .into_response(),
        "box-large.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/box-large.glb").as_ref(),
        )
            .into_response(),
        "conveyor-long.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/conveyor-long.glb").as_ref(),
        )
            .into_response(),
        "conveyor-corner.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/conveyor-corner.glb").as_ref(),
        )
            .into_response(),
        "crane.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/crane.glb").as_ref(),
        )
            .into_response(),
        "robot-arm-a.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/robot-arm-a.glb").as_ref(),
        )
            .into_response(),
        "hopper-square.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/hopper-square.glb").as_ref(),
        )
            .into_response(),
        "hopper-high-square.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/hopper-high-square.glb").as_ref(),
        )
            .into_response(),
        "machine.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/machine.glb").as_ref(),
        )
            .into_response(),
        "pipe-large-long.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/pipe-large-long.glb").as_ref(),
        )
            .into_response(),
        "structure-tall.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/structure-tall.glb").as_ref(),
        )
            .into_response(),
        "floor.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/floor.glb").as_ref(),
        )
            .into_response(),
        "cog-a.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/cog-a.glb").as_ref(),
        )
            .into_response(),
        "door.glb" => (
            [(header::CONTENT_TYPE, "model/gltf-binary")],
            include_bytes!("../../assets/models/door.glb").as_ref(),
        )
            .into_response(),
        _ => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_game_page_loads_no_external_assets() {
        let html = INDEX;

        // Check what the browser would FETCH, not every string that looks like a URL.
        for attr in ["src=\"", "href=\""] {
            for (i, _) in html.match_indices(attr) {
                let value: String = html[i + attr.len()..]
                    .chars()
                    .take_while(|c| *c != '"')
                    .collect();
                let external = value.starts_with("http://")
                    || value.starts_with("https://")
                    || value.starts_with("//");
                assert!(
                    !external,
                    "the game page fetches {value:?} from the network; it must serve its own assets"
                );
            }
        }

        // The Three.js loader must be served from the binary
        assert!(
            html.contains("/game/three.module.js"),
            "the game page does not load Three.js"
        );

        // The GLTF loader must be served from the binary
        assert!(
            html.contains("/game/loaders/gltf-loader.js"),
            "the game page does not load GLTFLoader"
        );

        // The game stream endpoint must exist
        assert!(
            html.contains("/game/stream"),
            "the game page does not connect to the game stream"
        );
    }

    #[test]
    fn game_world_serializes_to_json() {
        let world = GameWorld {
            queues: vec![QueueConveyor {
                name: "default".to_string(),
                available: 10,
                executing: 5,
                completed: 100,
                retryable: 2,
                discarded: 1,
                cancelled: 0,
                position: [0.0, 0.0, 0.0],
                oldest_age_secs: Some(30),
            }],
            workers: vec![WorkerEntity {
                id: "worker-1".to_string(),
                queue: "default".to_string(),
                alive: true,
                last_heartbeat_secs_ago: 5,
                position: [1.0, 1.0, 1.0],
            }],
            destinations: vec![DestinationEntity {
                name: "api.example.com".to_string(),
                waiting: 3,
                failed_recently: 0,
                breaker_state: "closed".to_string(),
                position: [5.0, 1.0, -5.0],
            }],
            timestamp_ms: 1234567890,
        };

        let json = serde_json::to_string(&world).expect("failed to serialize");
        assert!(json.contains("default"));
        assert!(json.contains("worker-1"));
        assert!(json.contains("api.example.com"));
        assert!(json.contains("1234567890"));
    }
}
