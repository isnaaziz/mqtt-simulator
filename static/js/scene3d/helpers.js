import * as THREE from 'three';
import { CSS2DObject } from 'three/addons/renderers/CSS2DRenderer.js';
import { COL, Y_BUS } from './config.js';

export function makePipe(scene, a, b, r, color) {
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

export function makeCabinet(scene, cabinets, key, x, color, title) {
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
  makePipe(scene, new THREE.Vector3(x, 2.6, 0), new THREE.Vector3(x, Y_BUS, 0), 0.06, COL.busOff);
  const div = document.createElement("div");
  div.className = "lbl3d";
  div.innerHTML = `<div class="t">${title}</div><div class="v" data-vals>—</div>`;
  const lbl = new CSS2DObject(div);
  lbl.position.set(0, 3.5, 0);
  g.add(lbl);

  const _ec = new THREE.Color(color).lerp(new THREE.Color(0xFFFFFF), 0.5);
  const emissiveHex = _ec.getHex();

  scene.add(g);
  cabinets[key] = { body, baseColor: color, emissiveHex, valsEl: div.querySelector("[data-vals]") };
}

export function makeBreaker(scene, breakerMeshes, name, x, y) {
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

export function makeFlowSeg(scene, segments, x1, x2, y, flowMat, energizedFn) {
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

export function tint(mat, on, activeColor) {
  mat.color.setHex(on ? activeColor : COL.busOff);
  mat.emissive.setHex(on ? activeColor : 0x000000);
  mat.emissiveIntensity = on ? 0.9 : 0;
  mat.opacity = on ? 0.95 : 0.75;
}
