import { connect, send } from './ws.js';
import { tags, breakers, computeEnergize } from './state.js';
import { initUI, buildGrid, buildCB, updateTags, updateCB } from './ui.js';
import { initDrawer, openDrawer, getActiveTag, refreshDrawer } from './drawer.js';
import { init3D, update3D } from './scene3d.js';

const $ = (id) => document.getElementById(id);

function setConn(on) {
  const el = $("conn");
  el.className = "conn " + (on ? "on" : "off");
  $("connTxt").textContent = on ? "HMI Online" : "HMI Offline";
}

function setBroker(on) {
  const el = $("broker");
  el.className = "conn " + (on ? "on" : "off");
  $("brokerTxt").textContent = on ? "Broker Online" : "Broker Offline";
}

function onUpdate(msg) {
  $("rtuName").textContent = msg.rtu || "UPS HMI · ONE-LINE";
  $("prefix").textContent = "TOPIC PREFIX · " + (msg.prefix || "—");
  if (msg.ts) $("ts").textContent = new Date(msg.ts).toLocaleTimeString("id-ID");
  setBroker(!!msg.mqtt);

  if (msg.tags) {
    const prev = Object.assign({}, tags);
    buildGrid(msg.tags);
    for (const t of msg.tags) tags[t.name] = t;
    updateTags(msg.tags, prev);
    if (getActiveTag()) refreshDrawer();
  }

  if (msg.breakers) {
    buildCB(msg.breakers);
    updateCB(msg.breakers);
    for (const b of msg.breakers) breakers[b.name] = b;
  }

  update3D({ tags, breakers, energize: computeEnergize() });
}

$("resetAll").onclick = () => send({ action: "reset_all" });

initUI(
  (name) => openDrawer(name),
  (name, state) => send({ action: "cb_set", tag: name, state })
);

initDrawer(tags);

init3D(
  $("scene3d"),
  (name) => send({ action: "cb_toggle", tag: name })
);

connect(onUpdate, setConn);
