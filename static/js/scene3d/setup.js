import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { CSS2DRenderer } from 'three/addons/renderers/CSS2DRenderer.js';

export function setupScene(mount, fallback) {
  const W = () => mount.clientWidth;
  const H = () => mount.clientHeight;

  const scene = new THREE.Scene();
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

  return { scene, camera, renderer, labelRenderer, controls, W, H };
}
