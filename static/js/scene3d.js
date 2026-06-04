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
  scene.background = new THREE.Color(0xeef2f8);
  scene.fog = new THREE.Fog(0xeef2f8, 28, 60);

  const camera = new THREE.PerspectiveCamera(46, W() / H(), 0.1, 200);
  camera.position.set(9, 8.5, 17);

  const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
  renderer.setSize(W(), H());
  renderer.setPixelRatio(Math.min(devicePixelRatio, 2));
  mount.appendChild(renderer.domElement);
  if (fallback) fallback.style.display = "none";

  const labelRenderer = new CSS2DRenderer();
  labelRenderer.setSize(W(), H());
  labelRenderer.domElement.style.cssText = "position:absolute;top:0;left:0;pointer-events:none";
  mount.appendChild(labelRenderer.domElement);

  const controls = new OrbitControls(camera, renderer.domElement);
  controls.target.set(-0.5, 2, 0);
  controls.enableDamping = true;
  controls.minDistance = 8;
  controls.maxDistance = 38;
  controls.maxPolarAngle = Math.PI / 2.05;

  scene.add(new THREE.HemisphereLight(0xffffff, 0xc4cedd, 1.05));
  const dir = new THREE.DirectionalLight(0xffffff, 1.1);
  dir.position.set(10, 18, 12);
  scene.add(dir);

  const floor = new THREE.Mesh(
    new THREE.PlaneGeometry(60, 40),
    new THREE.MeshStandardMaterial({ color: 0xf4f7fb, roughness: 1 })
  );
  floor.rotation.x = -Math.PI / 2;
  floor.position.y = -0.01;
  scene.add(floor);
  scene.add(new THREE.GridHelper(60, 60, 0xcfd8e6, 0xe5ebf3));

  const Y_BUS = 3.2;
  const Y_BYP = 4.7;
  const NODE = { GRID: -11, INC: -5.5, UPS: 0, LOAD: 6.5 };
  const COL = {
    busOn: 0x16a34a, busOff: 0xc2ccd9, gray: 0xb4bdca,
    closed: 0x16a34a, open: 0xdc2626, housing: 0x55657a, lever: 0x2b3543,
    accent: 0x2563eb, teal: 0x0d9488, purple: 0x7c3aed,
  };

  function makePipe(a, b, r, color) {
    const dirv = new THREE.Vector3().subVectors(b, a);
    const len = dirv.length();
    const geo = new THREE.CylinderGeometry(r, r, len, 16);
    const mat = new THREE.MeshStandardMaterial({ color, roughness: .5, metalness: .3 });
    const mesh = new THREE.Mesh(geo, mat);
    mesh.position.copy(a).add(dirv.multiplyScalar(0.5));
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
      new THREE.MeshStandardMaterial({ color, roughness: .55, metalness: .25, emissive: 0x000000 })
    );
    body.position.y = 1.3;
    g.add(body);
    const cap = new THREE.Mesh(
      new THREE.BoxGeometry(2.5, 0.12, 2.0),
      new THREE.MeshStandardMaterial({ color: 0x33414f, roughness: .6 })
    );
    cap.position.y = 2.62;
    g.add(cap);
    makePipe(new THREE.Vector3(x, 2.6, 0), new THREE.Vector3(x, Y_BUS, 0), 0.06, COL.busOff);
    const div = document.createElement("div");
    div.className = "lbl3d";
    div.innerHTML = `<div class="t">${title}</div><div class="v" data-vals>—</div>`;
    const lbl = new CSS2DObject(div);
    lbl.position.set(0, 3.5, 0);
    g.add(lbl);
    scene.add(g);
    cabinets[key] = { body, baseColor: color, valsEl: div.querySelector("[data-vals]") };
  }
  makeCabinet("INC", NODE.INC, COL.purple, "INC1");
  makeCabinet("UPS", NODE.UPS, COL.teal, "UPS");
  makeCabinet("LOAD", NODE.LOAD, COL.accent, "LOAD");

  (() => {
    const g = new THREE.Group();
    g.position.set(NODE.GRID, 0, 0);
    const pole = new THREE.Mesh(
      new THREE.CylinderGeometry(0.12, 0.16, Y_BUS, 12),
      new THREE.MeshStandardMaterial({ color: 0x8a96a6, roughness: .6 })
    );
    pole.position.y = Y_BUS / 2;
    g.add(pole);
    const ball = new THREE.Mesh(
      new THREE.SphereGeometry(0.28, 20, 20),
      new THREE.MeshStandardMaterial({ color: COL.busOn, emissive: COL.busOn, emissiveIntensity: .5 })
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
  makePipe(new THREE.Vector3(NODE.INC, Y_BUS, 0), new THREE.Vector3(NODE.INC, Y_BYP, 0), 0.06, COL.busOff);
  makePipe(new THREE.Vector3(NODE.LOAD, Y_BUS, 0), new THREE.Vector3(NODE.LOAD, Y_BYP, 0), 0.06, COL.busOff);
  const segBP = makePipe(new THREE.Vector3(NODE.INC, Y_BYP, 0), new THREE.Vector3(NODE.LOAD, Y_BYP, 0), 0.07, COL.busOff);

  const breakerMeshes = {};
  function makeBreaker(name, x, y) {
    const g = new THREE.Group();
    g.position.set(x, y, 0);
    const housing = new THREE.Mesh(
      new THREE.BoxGeometry(0.7, 0.95, 0.7),
      new THREE.MeshStandardMaterial({ color: COL.housing, roughness: .5, metalness: .4 })
    );
    housing.userData.cb = name;
    g.add(housing);
    const ind = new THREE.Mesh(
      new THREE.SphereGeometry(0.16, 18, 18),
      new THREE.MeshStandardMaterial({ color: COL.closed, emissive: COL.closed, emissiveIntensity: .6 })
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

  const flowMat = new THREE.MeshStandardMaterial({ color: 0x22c55e, emissive: 0x22c55e, emissiveIntensity: .9 });
  const segments = [];
  function makeFlowSeg(x1, x2, y, energizedFn) {
    const from = new THREE.Vector3(x1, y, 0);
    const to = new THREE.Vector3(x2, y, 0);
    const spheres = [];
    for (let i = 0; i < 4; i++) {
      const s = new THREE.Mesh(new THREE.SphereGeometry(0.1, 12, 12), flowMat);
      s.visible = false;
      scene.add(s);
      spheres.push({ mesh: s, phase: i / 4 });
    }
    segments.push({ from, to, spheres, energizedFn });
  }
  const cl = (n) => _latest.breakers[n] && _latest.breakers[n].state === "closed";
  makeFlowSeg(NODE.GRID, NODE.INC, Y_BUS, () => _latest.energize.inc);
  makeFlowSeg(NODE.INC, NODE.UPS, Y_BUS, () => _latest.energize.ups);
  makeFlowSeg(NODE.UPS, NODE.LOAD, Y_BUS, () => _latest.energize.ups && cl("CB_LOAD"));
  makeFlowSeg(NODE.INC, NODE.LOAD, Y_BYP, () => _latest.energize.inc && cl("CB_BYPASS") && cl("CB_LOAD"));

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
  const tint = (mat, on) => mat.color.setHex(on ? COL.busOn : COL.busOff);

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
    tint(segGI, e.inc);
    tint(segIU, e.ups);
    tint(segUL, e.ups && cl("CB_LOAD"));
    tint(segBP, e.inc && cl("CB_BYPASS"));
    const setCab = (key, on) => {
      const c = cabinets[key]; if (!c) return;
      c.body.material.color.setHex(on ? c.baseColor : COL.gray);
      c.body.material.emissive.setHex(on ? c.baseColor : 0x000000);
      c.body.material.emissiveIntensity = on ? 0.12 : 0;
    };
    setCab("INC", e.inc);
    setCab("UPS", e.ups);
    setCab("LOAD", e.load);
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

  const tmp = new THREE.Vector3();
  (function animate() {
    requestAnimationFrame(animate);
    controls.update();
    for (const seg of segments) {
      const on = !!seg.energizedFn();
      for (const sp of seg.spheres) {
        sp.mesh.visible = on;
        if (!on) continue;
        sp.phase += 0.006;
        if (sp.phase > 1) sp.phase -= 1;
        tmp.copy(seg.from).lerp(seg.to, sp.phase);
        sp.mesh.position.copy(tmp);
      }
    }
    renderer.render(scene, camera);
    labelRenderer.render(scene, camera);
  })();
}
