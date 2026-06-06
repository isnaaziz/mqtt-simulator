import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { CSS2DRenderer, CSS2DObject } from 'three/addons/renderers/CSS2DRenderer.js';
import { fmt } from './fmt.js';

let _applyState = null;
let _latest = { tags: {}, breakers: {}, energize: {} };

export function init3D(mountEl, onCBClick) {
  const fallback = document.getElementById("sceneFallback");
  try {
    _setup(mountEl, fallback, onCBClick);
  } catch (err) {
    console.error("3D init error:", err);
    if (fallback) fallback.innerHTML = 'Scene 3D gagal dimuat.<br/><span style="font-size:11px">Cek koneksi internet (three.js dimuat dari CDN). Panel CB & grid di bawah tetap berfungsi.</span>';
  }
}

export function update3D(state) {
  _latest = state;
  if (_applyState) _applyState();
}

function _setup(mount, fallback, onCBClick) {
  const W = () => mount.clientWidth;
  const H = () => mount.clientHeight;

  const scene = new THREE.Scene();
  // No scene.background → canvas stays transparent, glass CSS shows through
  scene.fog = new THREE.FogExp2(0xF8FAFC, 0.015);

  const camera = new THREE.PerspectiveCamera(46, W() / H(), 0.1, 200);
  camera.position.set(9, 8.5, 17);

  const renderer = new THREE.WebGLRenderer({
    antialias: true,
    alpha: true,
    powerPreference: "high-performance",
  });
  renderer.setSize(W(), H());
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 1.5));
  renderer.setClearColor(0x000000, 0);
  renderer.shadowMap.enabled = true;
  renderer.shadowMap.type = THREE.PCFSoftShadowMap;
  mount.appendChild(renderer.domElement);
  if (fallback) fallback.style.display = "none";

  const labelRenderer = new CSS2DRenderer();
  labelRenderer.setSize(W(), H());
  labelRenderer.domElement.style.cssText = "position:absolute;top:0;left:0;pointer-events:none";
  mount.appendChild(labelRenderer.domElement);

  const controls = new OrbitControls(camera, renderer.domElement);
  controls.target.set(-0.5, 2, 0);
  controls.enableDamping = true;
  controls.dampingFactor = 0.08;
  controls.minDistance = 8;
  controls.maxDistance = 38;
  controls.maxPolarAngle = Math.PI / 2.05;
  controls.rotateSpeed = 0.65;
  controls.zoomSpeed = 0.8;

  // Studio-quality 3-point lighting for a premium industrial render
  const ambient = new THREE.AmbientLight(0xF0F4FF, 0.55);
  scene.add(ambient);

  const sun = new THREE.DirectionalLight(0xFFFCF5, 2.2);
  sun.position.set(14, 24, 12);
  sun.castShadow = true;
  sun.shadow.mapSize.width = 1024;
  sun.shadow.mapSize.height = 1024;
  sun.shadow.camera.near = 1;
  sun.shadow.camera.far = 60;
  sun.shadow.camera.left = -22;
  sun.shadow.camera.right = 22;
  sun.shadow.camera.top = 20;
  sun.shadow.camera.bottom = -20;
  sun.shadow.bias = -0.0015;
  sun.shadow.normalBias = 0.02;
  scene.add(sun);

  const fill = new THREE.DirectionalLight(0xC5D8F5, 0.9);
  fill.position.set(-14, 10, -10);
  scene.add(fill);

  const rim = new THREE.DirectionalLight(0xE8F0FF, 0.4);
  rim.position.set(0, 2, -18);
  scene.add(rim);

  // Polished concrete floor — premium industrial look
  const floor = new THREE.Mesh(
    new THREE.PlaneGeometry(80, 60, 1, 1),
    new THREE.MeshStandardMaterial({
      color: 0xF1F5F9,
      roughness: 0.55,
      metalness: 0.05,
      transparent: true,
      opacity: 0.90,
    })
  );
  floor.rotation.x = -Math.PI / 2;
  floor.position.y = -0.01;
  floor.receiveShadow = true;
  scene.add(floor);

  const grid = new THREE.GridHelper(60, 40, 0xBDCAD9, 0xDDE4EE);
  grid.position.y = 0.002;
  grid.material.transparent = true;
  grid.material.opacity = 0.55;
  scene.add(grid);

  const Y_BUS = 3.2;
  const Y_BYP = 4.7;
  const NODE = { GRID: -11, INC: -5.5, UPS: 0, LOAD: 6.5 };
  const COL = {
    busOn:     0x4A68E9, // Softer corporate blue
    busBypass: 0x00E5FF, // Vibrant cyan-blue for bypass
    busOff:    0xBDD1E0, // Light ice-blue/grey for inactive pipes
    gray:      0xE2E8F0, // Clean light grey/white for inactive cabinets
    closed:    0x4A68E9, // Softer blue for closed breakers
    open:      0x94A3B8, // Sleek grey for open breakers
    housing:   0xFFFFFF, // Pure white for breaker housings
    lever:     0x4A68E9, // Softer blue for levers
    // Base cabinet colors (white/light-grey glossy towers)
    incomer:   0xF8FAFC, 
    ups:       0xF8FAFC, 
    load:      0xF8FAFC, 
  };

  function makePipe(a, b, r, color) {
    const dirv = new THREE.Vector3().subVectors(b, a);
    const len = dirv.length();
    const geo = new THREE.CylinderGeometry(r, r, len, 14, 1);
    const mat = new THREE.MeshStandardMaterial({
      color,
      roughness: 0.08,
      metalness: 0.12,
      transparent: true,
      opacity: 0.85,
      emissive: new THREE.Color(0x000000),
      emissiveIntensity: 0
    });
    const mesh = new THREE.Mesh(geo, mat);
    mesh.castShadow = true;
    mesh.receiveShadow = true;
    mesh.position.copy(a).add(dirv.clone().multiplyScalar(0.5));
    mesh.quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), dirv.clone().normalize());
    scene.add(mesh);
    return mat;
  }

  const cabinets = {};
  function makeCabinet(key, x, color, title) {
    const g = new THREE.Group();
    g.position.set(x, 0, 0);
    const body = new THREE.Mesh(
      new THREE.BoxGeometry(2.4, 2.6, 1.9),
      new THREE.MeshStandardMaterial({
        color: color,
        roughness: 0.15,
        metalness: 0.05,
        emissive: new THREE.Color(0x000000),
        emissiveIntensity: 0
      })
    );
    body.position.y = 1.3;
    body.castShadow = true;
    body.receiveShadow = true;
    g.add(body);
    const cap = new THREE.Mesh(
      new THREE.BoxGeometry(2.5, 0.12, 2.0),
      new THREE.MeshStandardMaterial({ color: 0x4A68E9, roughness: 0.15, metalness: 0.20 })
    );
    cap.position.y = 2.62;
    cap.castShadow = true;
    cap.receiveShadow = true;
    g.add(cap);
    makePipe(new THREE.Vector3(x, 2.6, 0), new THREE.Vector3(x, Y_BUS, 0), 0.06, COL.busOff);
    const div = document.createElement("div");
    div.className = "lbl3d";
    div.innerHTML = `<div class="t">${title}</div><div class="v" data-vals>—</div>`;
    const lbl = new CSS2DObject(div);
    lbl.position.set(0, 3.5, 0);
    g.add(lbl);

    // Pre-compute a brightened emissive color (50% lerp toward white)
    const _ec = new THREE.Color(color).lerp(new THREE.Color(0xFFFFFF), 0.5);
    const emissiveHex = _ec.getHex();

    scene.add(g);
    cabinets[key] = { body, baseColor: color, emissiveHex, valsEl: div.querySelector("[data-vals]") };
  }
  makeCabinet("INC", NODE.INC, COL.incomer, "INC1");
  makeCabinet("UPS", NODE.UPS, COL.ups, "UPS");
  makeCabinet("LOAD", NODE.LOAD, COL.load, "LOAD");

  (() => {
    const g = new THREE.Group();
    g.position.set(NODE.GRID, 0, 0);
    const pole = new THREE.Mesh(
      new THREE.CylinderGeometry(0.12, 0.16, Y_BUS, 10),
      new THREE.MeshStandardMaterial({ color: 0x7a8898, roughness: .6 })
    );
    pole.position.y = Y_BUS / 2;

    g.add(pole);
    const ball = new THREE.Mesh(
      new THREE.SphereGeometry(0.28, 14, 14),
      new THREE.MeshStandardMaterial({ color: COL.busOn, emissive: COL.busOn, emissiveIntensity: .55 })
    );
    ball.position.y = Y_BUS;

    g.add(ball);
    const div = document.createElement("div");
    div.className = "lbl3d";
    div.innerHTML = `<div class="t">GRID</div><div class="v">INCOMING</div>`;
    const lbl = new CSS2DObject(div);
    lbl.position.set(0, Y_BUS + 0.7, 0);
    g.add(lbl);
    scene.add(g);
  })();

  const segGI = makePipe(new THREE.Vector3(NODE.GRID, Y_BUS, 0), new THREE.Vector3(NODE.INC, Y_BUS, 0), 0.07, COL.busOff);
  const segIU = makePipe(new THREE.Vector3(NODE.INC, Y_BUS, 0), new THREE.Vector3(NODE.UPS, Y_BUS, 0), 0.07, COL.busOff);
  const segUL = makePipe(new THREE.Vector3(NODE.UPS, Y_BUS, 0), new THREE.Vector3(NODE.LOAD, Y_BUS, 0), 0.07, COL.busOff);
  const segBPL = makePipe(new THREE.Vector3(NODE.INC, Y_BUS, 0), new THREE.Vector3(NODE.INC, Y_BYP, 0), 0.06, COL.busOff);
  const segBPR = makePipe(new THREE.Vector3(NODE.LOAD, Y_BUS, 0), new THREE.Vector3(NODE.LOAD, Y_BYP, 0), 0.06, COL.busOff);
  const segBP = makePipe(new THREE.Vector3(NODE.INC, Y_BYP, 0), new THREE.Vector3(NODE.LOAD, Y_BYP, 0), 0.07, COL.busOff);

  const breakerMeshes = {};
  function makeBreaker(name, x, y) {
    const g = new THREE.Group();
    g.position.set(x, y, 0);
    const housing = new THREE.Mesh(
      new THREE.BoxGeometry(0.7, 0.95, 0.7),
      new THREE.MeshStandardMaterial({ color: COL.housing, roughness: 0.35, metalness: 0.55 })
    );
    housing.userData.cb = name;
    housing.castShadow = true;
    housing.receiveShadow = true;
    g.add(housing);
    const ind = new THREE.Mesh(
      new THREE.SphereGeometry(0.16, 12, 12),
      new THREE.MeshStandardMaterial({ color: COL.closed, emissive: COL.closed, emissiveIntensity: .65 })
    );
    ind.position.y = 0.66;
    ind.userData.cb = name;

    g.add(ind);
    const lpivot = new THREE.Group();
    lpivot.position.set(0, 0.45, 0.38);
    const lever = new THREE.Mesh(
      new THREE.BoxGeometry(0.1, 0.55, 0.1),
      new THREE.MeshStandardMaterial({ color: COL.lever, roughness: .4 })
    );
    lever.position.y = 0.27;
    lever.userData.cb = name;

    lpivot.add(lever);
    g.add(lpivot);
    const div = document.createElement("div");
    div.className = "cb3d";
    div.innerHTML = `<div class="n">${name}</div><div class="s closed" data-st>CLOSED</div>`;
    const lbl = new CSS2DObject(div);
    lbl.position.set(0, -0.75, 0);
    g.add(lbl);
    scene.add(g);
    breakerMeshes[name] = { group: g, ind, lpivot, stEl: div.querySelector("[data-st]") };
  }
  makeBreaker("CB_INC", (NODE.GRID + NODE.INC) / 2, Y_BUS);
  makeBreaker("CB_UPS", (NODE.INC + NODE.UPS) / 2, Y_BUS);
  makeBreaker("CB_LOAD", (NODE.UPS + NODE.LOAD) / 2, Y_BUS);
  makeBreaker("CB_BYPASS", (NODE.INC + NODE.LOAD) / 2, Y_BYP);

  // Flow particles
  const flowMatGreen = new THREE.MeshStandardMaterial({
    color: COL.busOn, emissive: COL.busOn, emissiveIntensity: 0.9,
  });
  const flowMatOrange = new THREE.MeshStandardMaterial({
    color: COL.busBypass, emissive: COL.busBypass, emissiveIntensity: 0.9,
  });
  const segments = [];
  function makeFlowSeg(x1, x2, y, flowMat, energizedFn) {
    const from = new THREE.Vector3(x1, y, 0);
    const to = new THREE.Vector3(x2, y, 0);
    const spheres = [];
    for (let i = 0; i < 5; i++) {
      const s = new THREE.Mesh(new THREE.SphereGeometry(0.09, 8, 8), flowMat);
      s.visible = false;
      scene.add(s);
      spheres.push({ mesh: s, phase: i / 5 });
    }
    segments.push({ from, to, spheres, energizedFn });
  }
  const cl = (n) => _latest.breakers[n] && _latest.breakers[n].state === "closed";
  makeFlowSeg(NODE.GRID, NODE.INC, Y_BUS, flowMatGreen, () => _latest.energize.inc);
  makeFlowSeg(NODE.INC, NODE.UPS, Y_BUS, flowMatGreen, () => _latest.energize.ups);
  makeFlowSeg(NODE.UPS, NODE.LOAD, Y_BUS, flowMatGreen, () => _latest.energize.ups && cl("CB_LOAD"));
  makeFlowSeg(NODE.INC, NODE.LOAD, Y_BYP, flowMatOrange, () => _latest.energize.inc && cl("CB_BYPASS") && cl("CB_LOAD"));

  // Raycaster for CB click
  const raycaster = new THREE.Raycaster();
  const pointer = new THREE.Vector2();
  let downPos = null;
  renderer.domElement.addEventListener("pointerdown", (e) => { downPos = { x: e.clientX, y: e.clientY }; });
  renderer.domElement.addEventListener("pointerup", (e) => {
    if (!downPos) return;
    const moved = Math.hypot(e.clientX - downPos.x, e.clientY - downPos.y);
    downPos = null;
    if (moved > 6) return;
    const rect = renderer.domElement.getBoundingClientRect();
    pointer.x = ((e.clientX - rect.left) / rect.width) * 2 - 1;
    pointer.y = -((e.clientY - rect.top) / rect.height) * 2 + 1;
    raycaster.setFromCamera(pointer, camera);
    for (const h of raycaster.intersectObjects(scene.children, true)) {
      const cb = h.object.userData && h.object.userData.cb;
      if (cb) { onCBClick(cb); break; }
    }
  });

  const getv = (n) => (_latest.tags[n] ? _latest.tags[n].value : null);
  const tint = (mat, on, activeColor = COL.busOn) => {
    mat.color.setHex(on ? activeColor : COL.busOff);
    mat.emissive.setHex(on ? activeColor : 0x000000);
    mat.emissiveIntensity = on ? 0.9 : 0;
    mat.opacity = on ? 0.95 : 0.75;
  };

  _applyState = () => {
    for (const name in breakerMeshes) {
      const b = _latest.breakers[name];
      const m = breakerMeshes[name];
      const closed = b && b.state === "closed";
      const c = closed ? COL.closed : COL.open;
      m.ind.material.color.setHex(c);
      m.ind.material.emissive.setHex(c);
      m.stEl.textContent = closed ? "CLOSED" : "OPEN";
      m.stEl.className = "s " + (closed ? "closed" : "open");
      m.lpivot.rotation.x = closed ? 0 : -1.0;
    }
    const e = _latest.energize;
    tint(segGI, e.inc, COL.busOn);
    tint(segIU, e.ups, COL.busOn);
    tint(segUL, e.ups && cl("CB_LOAD"), COL.busOn);
    tint(segBPL, e.inc, COL.busBypass);
    tint(segBP, e.inc && cl("CB_BYPASS"), COL.busBypass);
    tint(segBPR, e.inc && cl("CB_BYPASS") && cl("CB_LOAD"), COL.busBypass);

    const setCab = (key, on, activeColor = COL.busOn) => {
      const c = cabinets[key]; if (!c) return;
      c.body.material.color.setHex(on ? 0xF8FAFC : 0xD2DBE4); // Glossy white when active, cool light gray-blue when de-energized
      c.body.material.emissive.setHex(on ? activeColor : 0x000000);
      c.body.material.emissiveIntensity = on ? 0.55 : 0; // High-tech glowing slot effect
    };
    const isBypassActive = e.inc && cl("CB_BYPASS");
    const loadColor = isBypassActive ? COL.busBypass : COL.busOn;

    setCab("INC",  e.inc, COL.busOn);
    setCab("UPS",  e.ups, COL.busOn);
    setCab("LOAD", e.load, loadColor);
    if (cabinets.INC) cabinets.INC.valsEl.innerHTML = `${fmt(getv("INC1_VL12"), "V")} V · ${fmt(getv("INC1_IL1"), "A")} A`;
    if (cabinets.UPS) cabinets.UPS.valsEl.innerHTML = `${fmt(getv("UPS_P_LoadTotal"), "%")}% load · ${fmt(getv("UPS_SOC_Battery"), "%")}% SOC`;
    if (cabinets.LOAD) cabinets.LOAD.valsEl.innerHTML = `${fmt(getv("LOAD_VL1N"), "V")} V · ${fmt(getv("LOAD_Freq"), "Hz")} Hz`;
  };

  window.addEventListener("resize", () => {
    camera.aspect = W() / H();
    camera.updateProjectionMatrix();
    renderer.setSize(W(), H());
    labelRenderer.setSize(W(), H());
  });

  const FLOW_SPEED = 0.32; // units per second
  const tmp = new THREE.Vector3();
  let lastTime = performance.now();

  const fps = 30;
  const interval = 1000 / fps;
  let lastRenderTime = 0;

  (function animate(timestamp) {
    requestAnimationFrame(animate);

    if (document.hidden) return; // skip rendering if tab is hidden

    const elapsed = timestamp - lastRenderTime;
    if (elapsed < interval) return;

    lastRenderTime = timestamp - (elapsed % interval);

    const now = performance.now();
    const dt = Math.min((now - lastTime) / 1000, 0.05);
    lastTime = now;

    controls.update();

    for (const seg of segments) {
      const on = !!seg.energizedFn();
      for (const sp of seg.spheres) {
        sp.mesh.visible = on;
        if (!on) continue;
        sp.phase = (sp.phase + FLOW_SPEED * dt) % 1;
        tmp.copy(seg.from).lerp(seg.to, sp.phase);
        sp.mesh.position.copy(tmp);
      }
    }

    renderer.render(scene, camera);
    labelRenderer.render(scene, camera);
  })(0);
}
