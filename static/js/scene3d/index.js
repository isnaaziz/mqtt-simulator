import * as THREE from 'three';
import { CSS2DObject } from 'three/addons/renderers/CSS2DRenderer.js';
import { setupScene } from './setup.js';
import { COL, NODE, Y_BUS, Y_BYP } from './config.js';
import { makePipe, makeCabinet, makeBreaker, makeFlowSeg, tint } from './helpers.js';
import { fmt } from '../fmt.js';

let _applyState = null;
let _latest = { tags: {}, breakers: {}, energize: {} };

export function init3D(mountEl, onCBClick) {
  const fallback = document.getElementById("sceneFallback");
  try {
    _setup(mountEl, fallback, onCBClick);
  } catch (err) {
    console.error("3D init error:", err);
    if (fallback) {
      fallback.innerHTML = 'Scene 3D gagal dimuat.<br/><span style="font-size:11px">Cek log konsol browser. Panel CB & sirkuit di bawah tetap berfungsi.</span>';
    }
  }
}

export function update3D(state) {
  _latest = state;
  if (_applyState) _applyState();
}

function _setup(mount, fallback, onCBClick) {
  const { scene, camera, renderer, labelRenderer, controls, W, H } = setupScene(mount, fallback);

  const cabinets = {};
  makeCabinet(scene, cabinets, "INC", NODE.INC, COL.incomer, "INC1");
  makeCabinet(scene, cabinets, "UPS", NODE.UPS, COL.ups, "UPS");
  makeCabinet(scene, cabinets, "LOAD", NODE.LOAD, COL.load, "LOAD");

  const pole = new THREE.Mesh(
    new THREE.CylinderGeometry(0.12, 0.16, Y_BUS, 10),
    new THREE.MeshStandardMaterial({ color: 0x7a8898, roughness: .6 })
  );
  pole.position.y = Y_BUS / 2;

  const ball = new THREE.Mesh(
    new THREE.SphereGeometry(0.28, 14, 14),
    new THREE.MeshStandardMaterial({ color: COL.busOn, emissive: COL.busOn, emissiveIntensity: .55 })
  );
  ball.position.y = Y_BUS;

  const gridGroup = new THREE.Group();
  gridGroup.position.set(NODE.GRID, 0, 0);
  gridGroup.add(pole);
  gridGroup.add(ball);

  const div = document.createElement("div");
  div.className = "lbl3d";
  div.innerHTML = `<div class="t">GRID</div><div class="v">INCOMING</div>`;
  const lbl = new CSS2DObject(div);
  lbl.position.set(0, Y_BUS + 0.7, 0);
  gridGroup.add(lbl);
  scene.add(gridGroup);

  const segGI = makePipe(scene, new THREE.Vector3(NODE.GRID, Y_BUS, 0), new THREE.Vector3(NODE.INC, Y_BUS, 0), 0.07, COL.busOff);
  const segIU = makePipe(scene, new THREE.Vector3(NODE.INC, Y_BUS, 0), new THREE.Vector3(NODE.UPS, Y_BUS, 0), 0.07, COL.busOff);
  const segUL = makePipe(scene, new THREE.Vector3(NODE.UPS, Y_BUS, 0), new THREE.Vector3(NODE.LOAD, Y_BUS, 0), 0.07, COL.busOff);
  const segBPL = makePipe(scene, new THREE.Vector3(NODE.INC, Y_BUS, 0), new THREE.Vector3(NODE.INC, Y_BYP, 0), 0.06, COL.busOff);
  const segBPR = makePipe(scene, new THREE.Vector3(NODE.LOAD, Y_BUS, 0), new THREE.Vector3(NODE.LOAD, Y_BYP, 0), 0.06, COL.busOff);
  const segBP = makePipe(scene, new THREE.Vector3(NODE.INC, Y_BYP, 0), new THREE.Vector3(NODE.LOAD, Y_BYP, 0), 0.07, COL.busOff);

  const breakerMeshes = {};
  makeBreaker(scene, breakerMeshes, "CB_INC", (NODE.GRID + NODE.INC) / 2, Y_BUS);
  makeBreaker(scene, breakerMeshes, "CB_UPS", (NODE.INC + NODE.UPS) / 2, Y_BUS);
  makeBreaker(scene, breakerMeshes, "CB_LOAD", (NODE.UPS + NODE.LOAD) / 2, Y_BUS);
  makeBreaker(scene, breakerMeshes, "CB_BYPASS", (NODE.INC + NODE.LOAD) / 2, Y_BYP);

  const flowMatGreen = new THREE.MeshStandardMaterial({
    color: COL.busOn, emissive: COL.busOn, emissiveIntensity: 0.9,
  });
  const flowMatOrange = new THREE.MeshStandardMaterial({
    color: COL.busBypass, emissive: COL.busBypass, emissiveIntensity: 0.9,
  });

  const segments = [];
  const cl = (n) => _latest.breakers[n] && _latest.breakers[n].state === "closed";
  makeFlowSeg(scene, segments, NODE.GRID, NODE.INC, Y_BUS, flowMatGreen, () => _latest.energize.inc);
  makeFlowSeg(scene, segments, NODE.INC, NODE.UPS, Y_BUS, flowMatGreen, () => _latest.energize.ups);
  makeFlowSeg(scene, segments, NODE.UPS, NODE.LOAD, Y_BUS, flowMatGreen, () => _latest.energize.ups && cl("CB_LOAD"));
  makeFlowSeg(scene, segments, NODE.INC, NODE.LOAD, Y_BYP, flowMatOrange, () => _latest.energize.inc && cl("CB_BYPASS") && cl("CB_LOAD"));

  const raycaster = new THREE.Raycaster();
  const pointer = new THREE.Vector2();
  let downPos = null;

  renderer.domElement.addEventListener("pointerdown", (e) => {
    downPos = { x: e.clientX, y: e.clientY };
  });

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
      if (cb) {
        onCBClick(cb);
        break;
      }
    }
  });

  const getv = (n) => (_latest.tags[n] ? _latest.tags[n].value : null);

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
      const c = cabinets[key];
      if (!c) return;
      c.body.material.color.setHex(on ? 0xF8FAFC : 0xD2DBE4);
      c.body.material.emissive.setHex(on ? activeColor : 0x000000);
      c.body.material.emissiveIntensity = on ? 0.55 : 0;
    };
    const isBypassActive = e.inc && cl("CB_BYPASS");
    const loadColor = isBypassActive ? COL.busBypass : COL.busOn;

    setCab("INC", e.inc, COL.busOn);
    setCab("UPS", e.ups, COL.busOn);
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

  const FLOW_SPEED = 0.32;
  const tmp = new THREE.Vector3();
  let lastTime = performance.now();

  const fps = 30;
  const interval = 1000 / fps;
  let lastRenderTime = 0;

  (function animate(timestamp) {
    requestAnimationFrame(animate);

    if (document.hidden) return;

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
