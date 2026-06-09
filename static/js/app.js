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
  $("rtuName").textContent = msg.rtu || "UPS CyberTwin Console";
  $("prefix").textContent = "TOPIC PREFIX · " + (msg.prefix || "—");
  if (msg.ts) $("ts").textContent = new Date(msg.ts).toLocaleTimeString("id-ID");
  setBroker(!!msg.mqtt);

  if (msg.breakers) {
    for (const b of msg.breakers) {
      breakers[b.name] = b;
    }
    buildCB(msg.breakers);
    updateCB(msg.breakers);
  }

  if (msg.tags) {
    for (const t of msg.tags) {
      tags[t.name] = t;
    }
    const prev = Object.assign({}, tags);
    buildGrid(msg.tags);
    updateTags(msg.tags, prev);
    if (getActiveTag()) refreshDrawer();


  }

  update3D({ tags, breakers, energize: computeEnergize() });
}

$("autoAll").onclick = () => {
  send({ action: "auto_all" });
};
$("manualAll").onclick = () => {
  send({ action: "manual_all" });
};

initUI(
  (name) => openDrawer(name),
  (name, state) => {
    send({ action: "cb_set", tag: name, state });
  }
);

initDrawer(tags);

init3D(
  $("scene3d"),
  (name) => {
    send({ action: "cb_toggle", tag: name });
  }
);

// Modal elements
const btnAddData = $("btnAddData");
const modalScrim = $("modalScrim");
const addDataModal = $("addDataModal");
const modalClose = $("modalClose");

const tabTelemetry = $("tabTelemetry");
const tabBreaker = $("tabBreaker");
const formTelemetry = $("formTelemetry");
const formBreaker = $("formBreaker");

// Open Modal
if (btnAddData) {
  btnAddData.onclick = () => {
    modalScrim.classList.add("show");
    addDataModal.classList.add("show");
  };
}

// Close Modal helper
function closeModal() {
  modalScrim.classList.remove("show");
  addDataModal.classList.remove("show");
  formTelemetry.reset();
  formBreaker.reset();
}

if (modalClose) modalClose.onclick = closeModal;
if (modalScrim) modalScrim.onclick = closeModal;

// Switch Tabs
if (tabTelemetry && tabBreaker) {
  tabTelemetry.onclick = () => {
    tabTelemetry.classList.add("active");
    tabBreaker.classList.remove("active");
    formTelemetry.classList.add("active");
    formBreaker.classList.remove("active");
  };

  tabBreaker.onclick = () => {
    tabBreaker.classList.add("active");
    tabTelemetry.classList.remove("active");
    formBreaker.classList.add("active");
    formTelemetry.classList.remove("active");
  };
}

// Form Telemetry Submit
if (formTelemetry) {
  formTelemetry.onsubmit = (e) => {
    e.preventDefault();
    const tag = $("tagName").value.trim();
    const category = $("tagCategory").value.trim().toUpperCase();
    const unit = $("tagUnit").value.trim();
    const base = parseFloat($("tagBase").value);
    const variance = parseFloat($("tagVariance").value);
    const cum = $("tagCum").checked;

    if (!tag || !category) return;

    send({
      action: "add_tag",
      tag,
      category,
      unit,
      base,
      variance,
      cum
    });

    closeModal();
  };
}

// Form Breaker Submit
if (formBreaker) {
  formBreaker.onsubmit = (e) => {
    e.preventDefault();
    const tag = $("cbName").value.trim();
    const label = $("cbLabel").value.trim();
    const category = $("cbCategory").value.trim().toUpperCase();
    const state = $("cbState").value;

    if (!tag || !label || !category) return;

    send({
      action: "add_breaker",
      tag,
      label,
      category,
      state
    });

    closeModal();
  };
}

connect(onUpdate, setConn);
