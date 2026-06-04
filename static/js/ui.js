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
  if (_builtTags) return;
  const board = document.getElementById("board");
  board.innerHTML = "";
  const groups = [], idx = {};
  for (const t of list) {
    if (!(t.category in idx)) { idx[t.category] = groups.length; groups.push({ cat: t.category, items: [] }); }
    groups[idx[t.category]].items.push(t);
  }
  const palette = { LOAD: "#3b82f6", INC: "#8b5cf6", UPS: "#06b6d4" };
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
  if (_builtCB) return;
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
    if (t.variance > 0) {
      pct = ((t.value - (t.base - t.variance)) / (t.variance * 2)) * 100;
    } else if (t.base > 0) {
      pct = (t.value / t.base) * 100;
    }
    if (t.cum) {
      pct = (t.value % 10) * 10;
    }
    pct = Math.max(0, Math.min(100, pct));

    // Fill the 6 bars (column-reverse ensures filling bottom-to-top)
    const activeCount = Math.round((pct / 100) * 6);
    const palette = { LOAD: "#3b82f6", INC: "#8b5cf6", UPS: "#06b6d4" };
    const activeColor = t.mode === "manual" ? "var(--manual)" : (palette[r.category] || "var(--accent)");
    const activeGlow = t.mode === "manual" ? "0 0 4px var(--manual)" : `0 0 4px ${activeColor}`;

    r.bars.forEach((bar, idx) => {
      if (idx < activeCount) {
        bar.style.background = activeColor;
        bar.style.boxShadow = activeGlow;
      } else {
        bar.style.background = "rgba(0, 0, 0, 0.08)";
        bar.style.boxShadow = "none";
      }
    });
  }
  document.getElementById("manCount").textContent = manual;
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
