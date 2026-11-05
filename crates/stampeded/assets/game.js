// The factory floor. three.js comes from rama-x-threejs through the page's
// import map; the models sit next to this module, so they resolve against it.
import * as THREE from "three";
import { GLTFLoader } from "three/addons/loaders/GLTFLoader.js";

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
      new URL(`./models/${file}`, import.meta.url).href,
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

// The canvas carries data-three:world="/game/game.js": the hook imported this
// module for it, so the scene above is already built on it. Each world the
// server sends (SceneSignals::new("world", ..)) arrives here decoded.
export function mount(_canvas, world) {
  if (world) update(world);
}

export function update(world) {
  worldData = world;
  updateScene(worldData);
  updateHUD(worldData);
}

// Datastar retries a dropped stream on its own; say so until the next world
// repaints the HUD.
document.addEventListener("datastar-fetch", (event) => {
  const type = event.detail?.type;
  if (type === "error" || type === "retrying" || type === "retries-failed") {
    document.getElementById("queue-info").textContent = "Connection lost";
  }
});

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
