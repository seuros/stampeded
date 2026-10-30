//! 3D factory-floor view of the queue: belts are queues, boxes are sampled jobs.
//!
//! Read-only and token-gated like the dashboard; assets are embedded.

use super::ApiState;
use rama::futures::stream;
use rama::http::StatusCode;
use rama::http::header;
use rama::http::service::web::extract::State;
use rama::http::service::web::response::Sse;
use rama::http::service::web::response::{Html, IntoResponse};
use rama::http::sse::Event;
use rama::http::sse::server::KeepAlive;
use serde::Serialize;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
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
    "three": "/game/three.module.min.js",
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
  // Four models each reference Textures/colormap.png, and three.js ships with
  // its loader cache DISABLED, so the same texture was fetched four times --
  // once per loader.load(). FileLoader consults this cache, so enabling it
  // collapses them into one request. Set before any load starts.
  THREE.Cache.enabled = true;

  const loader = new GLTFLoader();
  for (const [key, file] of Object.entries(MODEL_FILES)) {
    loader.load(
      `/game/models/${file}`,
      (gltf) => {
        const root = gltf.scene;
        root.traverse((n) => {
          if (!n.isMesh) return;
          n.castShadow = true;
          n.receiveShadow = true;
          // The palette's dark slates read as black; let the texture light itself.
          const m = n.material;
          if (m && m.map) {
            m.emissiveMap = m.map;
            m.emissive = new THREE.Color(0xffffff);
            m.emissiveIntensity = 0.45;
          }
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

// One glowing material per state colour, shared: a fresh one per tick leaks GPU memory.
const STATE_MATERIALS = new Map();
function stateMaterial(color) {
  let mat = STATE_MATERIALS.get(color);
  if (!mat) {
    mat = new THREE.MeshStandardMaterial({
      color, emissive: color, emissiveIntensity: 0.35, roughness: 0.55,
    });
    STATE_MATERIALS.set(color, mat);
  }
  return mat;
}

// Recolour a clone without touching the shared source material.
function tint(obj, color) {
  obj.traverse((n) => {
    if (n.isMesh) n.material = stateMaterial(color);
  });
  return obj;
}

const canvas = document.getElementById("canvas");
const scene = new THREE.Scene();
const camera = new THREE.PerspectiveCamera(50, window.innerWidth / window.innerHeight, 0.1, 1000);
const renderer = new THREE.WebGLRenderer({ canvas, antialias: true });

renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
renderer.setSize(window.innerWidth, window.innerHeight);
// Physical light units since three r155; tone mapping keeps the brighter rig from clipping.
renderer.toneMapping = THREE.NeutralToneMapping;
renderer.toneMappingExposure = 1.2;
renderer.shadowMap.enabled = true;
renderer.shadowMap.type = THREE.PCFSoftShadowMap;

const SKY = 0x161a2b;
renderer.setClearColor(SKY);
scene.background = new THREE.Color(SKY);
scene.fog = new THREE.Fog(SKY, 80, 180);

// Sky/ground fill, a shadow-casting key light, and a cyan rim to lift silhouettes.
scene.add(new THREE.HemisphereLight(0xdfe7ff, 0x2a2f45, 2.2));
const sun = new THREE.DirectionalLight(0xffffff, 3.0);
sun.position.set(20, 35, 25);
sun.castShadow = true;
sun.shadow.mapSize.set(2048, 2048);
Object.assign(sun.shadow.camera, { left: -70, right: 70, top: 45, bottom: -45, near: 1, far: 140 });
sun.shadow.camera.updateProjectionMatrix();
scene.add(sun);
const rim = new THREE.DirectionalLight(0x00f0ff, 1.2);
rim.position.set(-25, 15, -30);
scene.add(rim);

// Orbit camera: drag to turn, wheel to zoom; frames every belt until touched.
const view = { yaw: 0, pitch: 0.6, dist: 50, auto: true };
const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
function placeCamera() {
  const flat = Math.cos(view.pitch) * view.dist;
  camera.position.set(Math.sin(view.yaw) * flat, Math.sin(view.pitch) * view.dist, Math.cos(view.yaw) * flat);
  camera.lookAt(0, 1, 0);
}
function frameRow(width) {
  if (!view.auto) return;
  const halfH = Math.atan(Math.tan(THREE.MathUtils.degToRad(camera.fov / 2)) * camera.aspect);
  view.dist = clamp((width / 2) / Math.tan(halfH) * 1.15, 20, 160);
  placeCamera();
}
let drag = null;
canvas.addEventListener("pointerdown", (e) => {
  drag = { x: e.clientX, y: e.clientY };
  canvas.setPointerCapture(e.pointerId);
});
canvas.addEventListener("pointermove", (e) => {
  if (!drag) return;
  view.yaw -= (e.clientX - drag.x) * 0.005;
  view.pitch = clamp(view.pitch + (e.clientY - drag.y) * 0.005, 0.12, 1.45);
  drag = { x: e.clientX, y: e.clientY };
  view.auto = false;
  placeCamera();
});
canvas.addEventListener("pointerup", () => { drag = null; });
canvas.addEventListener("wheel", (e) => {
  e.preventDefault();
  view.dist = clamp(view.dist * (1 + e.deltaY * 0.001), 8, 200);
  view.auto = false;
  placeCamera();
}, { passive: false });
placeCamera();

let worldData = null;
let fps = 0;
let frameCount = 0;
let lastTime = performance.now();
let sceneObjects = { queues: [], workers: [], destinations: [], belt: [] };

// Belt geometry, shared by the layout and the animation so they cannot drift.
const BELT_LENGTH = 10;
const BELT_SPACING = 15;
const rowWidth = (n) => Math.max(n - 1, 0) * BELT_SPACING + 14;

// Primitive fallbacks, shared: removing a mesh does not free its buffers.
const PRIM = {
  box: new THREE.BoxGeometry(0.8, 0.8, 0.8),
  belt: new THREE.BoxGeometry(BELT_LENGTH, 0.4, 1.5),
  beltMat: new THREE.MeshStandardMaterial({ color: 0x3a3f55 }),
  craneBase: new THREE.BoxGeometry(1, 0.3, 1),
  craneBaseMat: new THREE.MeshStandardMaterial({ color: 0xffcc00 }),
  craneArm: new THREE.BoxGeometry(0.2, 2, 0.2),
  craneArmMat: new THREE.MeshStandardMaterial({ color: 0xff9900 }),
  hopper: new THREE.ConeGeometry(1, 2, 8),
  hopperMat: new THREE.MeshStandardMaterial({ color: 0x5a6078 }),
};

// One label sprite per queue, redrawn on change, disposed with the queue.
const LABEL_W = 512;
const LABEL_H = 128;
const labels = new Map();
function beltLabel(name, detail) {
  let label = labels.get(name);
  if (!label) {
    const canvas = document.createElement("canvas");
    canvas.width = LABEL_W;
    canvas.height = LABEL_H;
    const texture = new THREE.CanvasTexture(canvas);
    texture.colorSpace = THREE.SRGBColorSpace;
    const material = new THREE.SpriteMaterial({
      map: texture, transparent: true, depthWrite: false, toneMapped: false,
    });
    const sprite = new THREE.Sprite(material);
    sprite.scale.set(8, 2, 1);
    scene.add(sprite);
    label = { canvas, texture, material, sprite, detail: null };
    labels.set(name, label);
  }
  if (label.detail !== detail) {
    const ctx = label.canvas.getContext("2d");
    ctx.clearRect(0, 0, LABEL_W, LABEL_H);
    ctx.fillStyle = "rgba(13, 14, 20, 0.8)";
    ctx.fillRect(0, 0, LABEL_W, LABEL_H);
    ctx.strokeStyle = "rgba(0, 240, 255, 0.9)";
    ctx.lineWidth = 4;
    ctx.strokeRect(2, 2, LABEL_W - 4, LABEL_H - 4);
    ctx.fillStyle = "rgb(0, 240, 255)";
    ctx.font = "bold 46px ui-monospace, Menlo, monospace";
    ctx.fillText(name, 22, 56);
    // Shrink the detail line until it fits.
    ctx.fillStyle = "rgb(178, 183, 199)";
    let size = 30;
    do {
      ctx.font = `${size}px ui-monospace, Menlo, monospace`;
      size -= 2;
    } while (ctx.measureText(detail).width > LABEL_W - 44 && size >= 16);
    ctx.fillText(detail, 22, 104);
    label.texture.needsUpdate = true;
    label.detail = detail;
  }
  return label.sprite;
}
function dropStaleLabels(live) {
  for (const [name, label] of labels) {
    if (live.has(name)) continue;
    scene.remove(label.sprite);
    label.texture.dispose();
    label.material.dispose();
    labels.delete(name);
  }
}

// Create floor
const floorGeometry = new THREE.PlaneGeometry(120, 120);
const floorMaterial = new THREE.MeshStandardMaterial({ color: 0x262b40, roughness: 0.9 });
const floor = new THREE.Mesh(floorGeometry, floorMaterial);
floor.rotation.x = -Math.PI / 2;
floor.receiveShadow = true;
scene.add(floor);
// A faint grid gives the floor depth.
const grid = new THREE.GridHelper(120, 60, 0x00f0ff, 0x3a4160);
grid.material.transparent = true;
grid.material.opacity = 0.25;
grid.position.y = 0.01;
scene.add(grid);

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

  const box = new THREE.Mesh(PRIM.box, stateMaterial(color));
  box.castShadow = true;
  box.receiveShadow = true;
  return box;
}

// Helper: create a conveyor belt visual
function createConveyorBelt(length = 8) {
  const model = modelClone("conveyor");
  if (model) return fitTo(model, length);

  const belt = new THREE.Mesh(PRIM.belt, PRIM.beltMat);
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
  const base = new THREE.Mesh(PRIM.craneBase, PRIM.craneBaseMat);
  base.receiveShadow = true;
  group.add(base);

  // Arm
  const arm = new THREE.Mesh(PRIM.craneArm, PRIM.craneArmMat);
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
  const body = new THREE.Mesh(PRIM.hopper, PRIM.hopperMat);
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
  sceneObjects.belt = [];

  dropStaleLabels(new Set(world.queues.map((q) => q.name)));

  // Belts and job boxes, centred on the origin.
  const first = -((world.queues.length - 1) * BELT_SPACING) / 2;
  frameRow(rowWidth(world.queues.length));
  let queueIndex = 0;
  for (const queue of world.queues) {
    const x = first + queueIndex * BELT_SPACING;
    const z = 0;

    // Add conveyor belt
    const belt = createConveyorBelt(10);
    belt.position.set(x, 0.3, z);
    scene.add(belt);
    sceneObjects.queues.push(belt);

    // The label sprite persists across rebuilds, so it is only repositioned.
    const rate = queue.throughput_per_sec || 0;
    beltLabel(
      queue.name,
      `${queue.available} waiting · ${queue.executing} running · ${rate.toFixed(1)}/s`,
    ).position.set(x, 3.4, z);

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
      // completed was in `total` but not here: it inflated the denominator,
      // diluting every other state's share of the eight boxes, and was never
      // drawn itself. A queue that had finished its work rendered as a bare
      // conveyor -- soak, 7,544 completed, zero boxes.
      { name: 'completed', count: queue.completed },
    ];

    let boxIndex = 0;
    for (const state of states) {
      if (state.count === 0) continue;
      const boxesForState = Math.ceil((state.count / total) * boxesToShow);
      for (let i = 0; i < boxesForState && boxIndex < boxesToShow; i++) {
        const box = createJobBox(state.name);
        box.position.set(x, 1.0, z - 0.4 + (boxIndex % 2) * 0.8);

        // Position along the belt is a pure function of the clock, NOT an
        // accumulated offset: updateScene rebuilds every box twice a second, so
        // anything stateful would snap back to the start on each tick and the
        // belt would stutter in place.
        //
        // Speed is the MEASURED completion rate. A still belt means nothing is
        // finishing, which is true and worth being able to see at a glance.
        box.userData = {
          baseX: x,
          speed: Math.min(queue.throughput_per_sec || 0, 40) * 0.08,
          phase: (boxIndex / Math.max(1, boxesToShow)) * BELT_LENGTH,
        };

        scene.add(box);
        sceneObjects.queues.push(box);
        sceneObjects.belt.push(box);
        boxIndex++;
      }
    }

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
      html += `<div>${status} ${w.name || w.id}</div>`;
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

  // Run the belt. Each box's position comes from the clock, so a rebuild
  // mid-travel does not jump it back to the start.
  const t = now / 1000;
  for (const box of sceneObjects.belt) {
    const d = box.userData;
    const travelled = t * d.speed + d.phase;
    const along = ((travelled % BELT_LENGTH) + BELT_LENGTH) % BELT_LENGTH;
    box.position.x = d.baseX - BELT_LENGTH / 2 + along;
    box.rotation.y = d.speed > 0 ? travelled * 0.5 : 0;
  }

  renderer.render(scene, camera);
}

animate();

window.addEventListener("resize", () => {
  camera.aspect = window.innerWidth / window.innerHeight;
  camera.updateProjectionMatrix();
  renderer.setSize(window.innerWidth, window.innerHeight);
  if (worldData?.queues) frameRow(rowWidth(worldData.queues.length));
});
</script>
</body>
</html>"#;

pub async fn index() -> impl IntoResponse {
    Html(INDEX)
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
            let json_str = serde_json::to_string(&*world)
                .inspect_err(|e| tracing::warn!(error = %e, "game world did not serialize"))
                .ok()?;

            let event = Event::new()
                .try_with_event("world")
                .ok()?
                .with_data(json_str);
            Some((Ok::<_, Infallible>(event), (rx, false)))
        },
    );

    (
        [(header::CACHE_CONTROL, "no-cache")],
        Sse::new(ticks).with_keep_alive(KeepAlive::new()),
    )
        .into_response()
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
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
    }
}

/// A static asset served whole, with its content type.
fn asset(content_type: &'static str, bytes: &'static [u8]) -> rama::http::Response {
    ([(header::CONTENT_TYPE, content_type)], bytes).into_response()
}

/// Serve Three.js library from embedded bytes.
pub async fn three_module() -> impl IntoResponse {
    asset(
        "application/javascript",
        include_bytes!("../../assets/three.module.min.js"),
    )
}

/// Serve GLTFLoader from embedded bytes.
pub async fn gltf_loader() -> impl IntoResponse {
    asset(
        "application/javascript",
        include_bytes!("../../assets/gltf-loader.js"),
    )
}

/// Serve three's core module, which three.module.min.js re-exports by this exact name.
pub async fn three_core() -> impl IntoResponse {
    asset(
        "application/javascript",
        include_bytes!("../../assets/three.core.min.js"),
    )
}

/// Serve GLTFLoader's addon dependencies from embedded bytes.
///
/// GLTFLoader imports `../utils/`, so these must stay under /game/utils/.
pub async fn buffer_geometry_utils() -> impl IntoResponse {
    asset(
        "application/javascript",
        include_bytes!("../../assets/utils/BufferGeometryUtils.js"),
    )
}

/// See buffer_geometry_utils: GLTFLoader imports `clone` from this.
pub async fn skeleton_utils() -> impl IntoResponse {
    asset(
        "application/javascript",
        include_bytes!("../../assets/utils/SkeletonUtils.js"),
    )
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
