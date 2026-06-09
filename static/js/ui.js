import { fmt } from './fmt.js';

const _refs = {};
const _cbRefs = {};
let _builtTags = false;
let _builtCB = false;
let _onCardClick = null;
let _onCBAction = null;

export function initUI(onCardClick, onCBAction) {
  _onCardClick = onCardClick;
  _onCBAction = onCBAction;
}

export function buildGrid(list) {
  const currentCount = Object.keys(_refs).length;
  if (_builtTags && list.length === currentCount) return;
  _builtTags = false;
  for (const k in _refs) delete _refs[k];
  const board = document.getElementById("board");
  board.innerHTML = "";
  const groups = [], idx = {};
  for (const t of list) {
    if (!(t.category in idx)) { idx[t.category] = groups.length; groups.push({ cat: t.category, items: [] }); }
    groups[idx[t.category]].items.push(t);
  }
  const palette = { LOAD: "#2563EB", INC: "#7C3AED", UPS: "#0284C7" };
  for (const g of groups) {
    const sec = document.createElement("section");
    sec.className = "group";
    const accent = palette[g.cat] || "#2563eb";
    sec.innerHTML = `<div class="sec-title"><span class="bar" style="background:${accent}"></span><h2>${g.cat}</h2><span class="count">${g.items.length} tags</span></div>`;
    const grid = document.createElement("div");
    grid.className = "grid";
    for (const t of g.items) {
      const card = document.createElement("div");
      card.className = "card" + (t.mode === "manual" ? " manual" : "");
      card.innerHTML = `
        <div class="card-left">
          <div class="badge">${t.mode === "manual" ? "MAN" : "AUTO"}</div>
          <div class="name">${t.name}</div>
          <div class="read"><span class="val">${fmt(t.value, t.unit)}</span><span class="unit">${t.unit || ""}</span></div>
        </div>
        <div class="card-rack">
          <div class="card-rack-bar"></div>
          <div class="card-rack-bar"></div>
          <div class="card-rack-bar"></div>
          <div class="card-rack-bar"></div>
          <div class="card-rack-bar"></div>
          <div class="card-rack-bar"></div>
        </div>`;
      card.onclick = () => _onCardClick && _onCardClick(t.name);
      grid.appendChild(card);
      _refs[t.name] = {
        card,
        val: card.querySelector(".val"),
        badge: card.querySelector(".badge"),
        bars: Array.from(card.querySelectorAll(".card-rack-bar")),
        category: t.category
      };
    }
    sec.appendChild(grid);
    board.appendChild(sec);
  }
  document.getElementById("tagCount").textContent = list.length + " tags";
  _builtTags = true;
}

export function buildCB(list) {
  const currentCount = Object.keys(_cbRefs).length;
  if (_builtCB && list.length === currentCount) return;
  _builtCB = false;
  for (const k in _cbRefs) delete _cbRefs[k];
  const panel = document.getElementById("cbPanel");
  panel.innerHTML = "";
  for (const b of list) {
    const card = document.createElement("div");
    card.className = "cb-card " + b.state;
    card.innerHTML = `
      <div class="label">${b.label || b.name}</div>
      <div class="tag">${b.category}/${b.name}</div>
      <div class="cb-state"><i></i><span class="st-txt">${b.state.toUpperCase()}</span></div>
      <div class="cb-control-row">
        <span class="cb-control-label">Control Switch</span>
        <button class="cb-toggle" id="toggle-${b.name}" aria-label="Toggle Breaker">
          <span class="cb-toggle-thumb"></span>
        </button>
      </div>`;
    const toggleBtn = card.querySelector(".cb-toggle");
    toggleBtn.onclick = () => {
      const isClosed = toggleBtn.classList.contains("active");
      const nextState = isClosed ? "open" : "closed";
      _onCBAction && _onCBAction(b.name, nextState);
    };
    panel.appendChild(card);
    _cbRefs[b.name] = { card, txt: card.querySelector(".st-txt"), toggleBtn };
  }
  document.getElementById("cbCount").textContent = list.length + " breakers";
  _builtCB = true;
}

export function updateTags(list, prevSnapshot) {
  let manual = 0;
  for (const t of list) {
    if (t.mode === "manual") manual++;
    const r = _refs[t.name];
    if (!r) continue;
    const prev = prevSnapshot[t.name];
    r.val.textContent = fmt(t.value, t.unit);
    if (prev && Math.abs(prev.value - t.value) > 1e-9) {
      r.val.classList.add("flash");
      setTimeout(() => r.val.classList.remove("flash"), 220);
    }
    const isMan = t.mode === "manual";
    r.card.classList.toggle("manual", isMan);
    r.badge.textContent = isMan ? "MAN" : "AUTO";

    // Calculate percentage fill (0 - 100)
    let pct = 50;
    const minVal = typeof t.min === 'number' ? t.min : (t.base - t.variance);
    const maxVal = typeof t.max === 'number' ? t.max : (t.base + t.variance);
    const range = maxVal - minVal;
    if (range > 0) {
      pct = ((t.value - minVal) / range) * 100;
    } else if (t.base > 0) {
      pct = (t.value / t.base) * 100;
    }
    if (t.cum) {
      pct = (t.value % 10) * 10;
    }
    pct = Math.max(0, Math.min(100, pct));

    // Fill the 6 bars (column-reverse ensures filling bottom-to-top)
    const activeCount = Math.round((pct / 100) * 6);
    const palette = { LOAD: "#2563EB", INC: "#7C3AED", UPS: "#0284C7" };
    const activeColor = t.mode === "manual" ? "var(--warning)" : (palette[r.category] || "var(--accent)");

    r.bars.forEach((bar, idx) => {
      if (idx < activeCount) {
        bar.style.background = activeColor;
        bar.style.boxShadow = "none";
      } else {
        bar.style.background = "#E2E8F0";
        bar.style.boxShadow = "none";
      }
    });
  }
  document.getElementById("manCount").textContent = manual;

  const autoAllBtn = document.getElementById("autoAll");
  const manualAllBtn = document.getElementById("manualAll");
  if (autoAllBtn && manualAllBtn) {
    autoAllBtn.classList.toggle("active", manual === 0);
    manualAllBtn.classList.toggle("active", list.length > 0 && manual === list.length);
  }

  // Update KPI Row
  const getVal = (name) => {
    const t = list.find(x => x.name === name);
    return t ? t.value : 0;
  };
  const vIncVolts = getVal("INC1_VL1N");
  const vIncAmps = getVal("INC1_IL1");
  const vLoadTotal = getVal("UPS_P_LoadTotal");
  const vSoc = getVal("UPS_SOC_Battery");
  const vBatVolts = getVal("UPS_V_Battery");
  const vTemp = getVal("UPS_Temp");

  const kpiGrid = document.getElementById("kpiGrid");
  const kpiGridCard = document.getElementById("kpiGridCard");
  if (kpiGrid && kpiGridCard) {
    kpiGrid.textContent = `${fmt(vIncVolts, "V")} V / ${fmt(vIncAmps, "A")} A`;
    kpiGridCard.classList.toggle("active", vIncVolts > 50);
  }

  const kpiLoad = document.getElementById("kpiLoad");
  const kpiLoadCard = document.getElementById("kpiLoadCard");
  if (kpiLoad && kpiLoadCard) {
    kpiLoad.textContent = `${fmt(vLoadTotal, "%")}%`;
    kpiLoadCard.classList.toggle("active", vLoadTotal > 1);
    kpiLoadCard.classList.toggle("warning", vLoadTotal > 80);
  }

  const kpiBattery = document.getElementById("kpiBattery");
  const kpiBatteryCard = document.getElementById("kpiBatteryCard");
  if (kpiBattery && kpiBatteryCard) {
    kpiBattery.textContent = `${fmt(vSoc, "%")}% (${fmt(vBatVolts, "V")} V)`;
    kpiBatteryCard.classList.toggle("active", vSoc > 90);
    kpiBatteryCard.classList.toggle("warning", vSoc < 50);
  }

  const kpiTemp = document.getElementById("kpiTemp");
  const kpiTempCard = document.getElementById("kpiTempCard");
  if (kpiTemp && kpiTempCard) {
    kpiTemp.textContent = `${fmt(vTemp, "degC")}°C`;
    kpiTempCard.classList.toggle("warning", vTemp > 45);
  }
}

export function updateCB(list) {
  for (const b of list) {
    const r = _cbRefs[b.name];
    if (!r) continue;
    r.card.className = "cb-card " + b.state;
    r.txt.textContent = b.state.toUpperCase();
    r.toggleBtn.classList.toggle("active", b.state === "closed");
  }
}
