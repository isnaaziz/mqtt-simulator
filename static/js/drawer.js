import { fmt } from './fmt.js';
import { send } from './ws.js';

let _activeTag = null;
let _tagsRef = null;

const $ = (id) => document.getElementById(id);

export function initDrawer(tagsRef) {
  _tagsRef = tagsRef;

  $("dClose").onclick = closeDrawer;
  $("scrim").onclick = closeDrawer;
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeDrawer(); });

  $("btnAuto").onclick = () => { if (_activeTag) send({ action: "set_mode", tag: _activeTag, mode: "auto" }); };
  $("btnManual").onclick = () => { if (_activeTag) send({ action: "set_mode", tag: _activeTag, mode: "manual" }); };

  $("slider").oninput = () => { $("setpoint").value = Number(parseFloat($("slider").value).toFixed(4)); };
  $("setpoint").oninput = () => { const v = parseFloat($("setpoint").value); if (!isNaN(v)) $("slider").value = v; };

  $("stepUp").onclick = () => _step(1);
  $("stepDown").onclick = () => _step(-1);
  $("apply").onclick = () => {
    if (!_activeTag) return;
    const v = parseFloat($("setpoint").value);
    if (!isNaN(v)) send({ action: "set_value", tag: _activeTag, value: v });
  };

  $("applySimLimits").onclick = () => {
    if (!_activeTag) return;
    const minVal = parseFloat($("simMin").value);
    const maxVal = parseFloat($("simMax").value);
    if (!isNaN(minVal) && !isNaN(maxVal) && maxVal > minVal) {
      send({ action: "update_limits", tag: _activeTag, min: minVal, max: maxVal });
    }
  };
}

export function getActiveTag() { return _activeTag; }

export function openDrawer(name) {
  _activeTag = name;
  const t = _tagsRef && _tagsRef[name];
  if (!t) return;

  $("dCat").textContent = t.category;
  $("dName").textContent = t.name;
  $("dUnit").textContent = t.unit || "(tanpa satuan)";

  const [min, max] = _rangeFor(t);
  $("slider").min = min;
  $("slider").max = max;
  $("slider").step = (max - min) / 200 || 0.01;
  $("rMin").textContent = fmt(min, t.unit);
  $("rMax").textContent = fmt(max, t.unit);

  const seed = t.mode === "manual" ? t.manual : t.value;
  $("setpoint").value = Number(seed.toFixed(4));
  $("slider").value = seed;

  const minVal = typeof t.min === 'number' ? t.min : (t.base - t.variance);
  const maxVal = typeof t.max === 'number' ? t.max : (t.base + t.variance);
  $("simMin").value = Number(minVal.toFixed(4));
  $("simMax").value = Number(maxVal.toFixed(4));

  _syncMode(t.mode);
  $("drawer").classList.add("show");
  $("scrim").classList.add("show");
}

export function closeDrawer() {
  _activeTag = null;
  $("drawer").classList.remove("show");
  $("scrim").classList.remove("show");
}

export function refreshDrawer() {
  if (!_activeTag) return;
  const t = _tagsRef && _tagsRef[_activeTag];
  if (!t) return;
  $("dLive").textContent = fmt(t.value, t.unit);
  _syncMode(t.mode);

  const [min, max] = _rangeFor(t);
  $("slider").min = min;
  $("slider").max = max;
  $("slider").step = (max - min) / 200 || 0.01;
  $("rMin").textContent = fmt(min, t.unit);
  $("rMax").textContent = fmt(max, t.unit);

  if (document.activeElement !== $("setpoint") && t.mode === "auto") {
    $("setpoint").value = Number(t.value.toFixed(4));
    $("slider").value = t.value;
  }
}

function _step(dir) {
  const t = _tagsRef && _tagsRef[_activeTag];
  if (!t) return;
  const inc = t.variance > 0 ? t.variance / 5 : 1;
  const next = ((parseFloat($("setpoint").value) || 0) + dir * inc);
  $("setpoint").value = Number(next.toFixed(4));
  $("slider").value = next;
}

function _rangeFor(t) {
  const minVal = typeof t.min === 'number' ? t.min : (t.base - t.variance);
  const maxVal = typeof t.max === 'number' ? t.max : (t.base + t.variance);
  const range = maxVal - minVal;
  if (range > 0) {
    let min = minVal - range * 1.5;
    let max = maxVal + range * 1.5;
    if (minVal >= 0 && min < 0) min = 0;
    return [min, max];
  }
  const span = Math.max(Math.abs(t.base) * 0.5, 100);
  return [Math.max(0, t.base - span), t.base + span];
}

function _syncMode(mode) {
  const man = mode === "manual";
  $("btnAuto").className = man ? "" : "on-auto";
  $("btnManual").className = man ? "on-manual" : "";
  $("ctl").classList.toggle("active", man);
}
