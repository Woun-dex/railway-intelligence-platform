// Mapbox token resolution — the token is NEVER hard-coded here. Priority:
//   1) ?token=pk... URL param   2) localStorage 'mapboxToken'
//   3) the backend /graph/config endpoint, which serves the MAPBOX_TOKEN env var
//      (set it in your shell or a gitignored .env). Restrict the pk.* token by URL.
let MAPBOX_TOKEN =
  new URLSearchParams(location.search).get("token") ||
  localStorage.getItem("mapboxToken") || "";
const $ = (id) => document.getElementById(id);
const tip = $("tip");
function showTip(x, y, html) { tip.innerHTML = html; tip.style.display = "block"; tip.style.left = x + 12 + "px"; tip.style.top = y + 12 + "px"; }
function hideTip() { tip.style.display = "none"; }
function hms(s) { const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = Math.floor(s % 60);
  return String(h).padStart(2, "0") + ":" + String(m).padStart(2, "0") + ":" + String(sec).padStart(2, "0"); }
function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }

let DATA, ORDER, SEL = -1, ACTIVE_LINES = new Set();
let MAT_MODE = "A"; const SPECTRAL = {};
let map = null, MAP_READY = false, CASCADE = null, JOURNEY = null, popup = null;
let GEOM = null;
function nowLocalSec() { const d = new Date(); return d.getHours() * 3600 + d.getMinutes() * 60 + d.getSeconds(); }

// ---- terminal boot log + live telemetry (real, not decorative) ----
function bootLog(msg, kind) {
  const el = $("cascStats"); if (!el || el.querySelector(".casc-stats")) return; // don't clobber a cascade result
  const col = kind === "ok" ? "#3fb950" : kind === "act" ? "#58a6ff" : kind === "err" ? "#f85149" : "";
  const ts = hms(nowLocalSec());
  const tag = kind === "ok" ? "[INFO]" : kind === "act" ? "[ACTION]" : kind === "err" ? "[ERR]" : "[LOG]";
  el.insertAdjacentHTML("beforeend", `<span style="color:${col || ""}" class="${col ? "" : "muted"}">${tag} ${ts} — ${msg}</span><br/>`);
  el.scrollTop = el.scrollHeight;
}
function setUplink(ok, ms) {
  const dot = $("uplinkDot"), txt = $("uplinkText"), lat = $("latency");
  if (txt) txt.textContent = ok ? "Uplink stable" : "Uplink lost";
  if (dot) dot.style.background = ok ? "var(--green)" : "#f85149";
  if (lat && ms != null) lat.textContent = Math.round(ms) + "ms";
}

bootLog("connecting to graph-engine…");
const _t0 = performance.now();
fetch("graph/matrix")
  .then((r) => { if (!r.ok) throw new Error("topology not loaded yet (HTTP " + r.status + ")"); setUplink(true, performance.now() - _t0); return r.json(); })
  .then(init)
  .catch((e) => { $("summary").textContent = e.message; setUplink(false); bootLog(e.message, "err"); });

async function init(d) {
  DATA = d; ORDER = d.order;
  // Resolve the Mapbox token from the backend env if not provided via URL/localStorage.
  if (!MAPBOX_TOKEN) { try { MAPBOX_TOKEN = (await fetch("graph/config").then((r) => r.json())).mapboxToken || ""; } catch (e) { /* map shows a token hint */ } }
  $("summary").textContent = `${d.stationCount} stations · ${d.edges.length} edges · version ${d.graphVersion}`;
  bootLog(`topology loaded — ${d.stationCount} stations, ${d.edges.length} edges`, "ok");
  const lineSet = new Set(); d.stations.forEach((s) => s.lines.forEach((l) => lineSet.add(l)));
  const lines = [...lineSet].sort(); lines.forEach((l) => ACTIVE_LINES.add(l));
  const palette = ["#58a6ff","#f0c674","#7ee787","#ff7b72","#d2a8ff","#79c0ff","#ffa657","#56d4dd",
                   "#e3b341","#a5d6ff","#f778ba","#3fb950","#bc8cff","#f0883e","#8b949e","#da3633"];
  const lc = {}; lines.forEach((l, i) => (lc[l] = palette[i % palette.length])); window.LINE_COLORS = lc;
  const lcDiv = $("lines"); lcDiv.innerHTML = "";
  lines.forEach((l) => {
    const c = document.createElement("span"); c.className = "chip";
    c.innerHTML = `<span class="swatch" style="background:${lc[l]}"></span>${l}`;
    c.onclick = () => {
      if (ACTIVE_LINES.has(l)) { ACTIVE_LINES.delete(l); c.classList.add("off"); }
      else { ACTIVE_LINES.add(l); c.classList.remove("off"); }
      applyLineFilter(); drawMatrix(); updateNetworkLoad();
    };
    lcDiv.appendChild(c);
  });

  setupSearchDrop("fromSearch", "fromDrop", "from", d.stations);
  setupSearchDrop("toSearch", "toDrop", "to", d.stations);
  setupSearchDrop("cascSearch", "cascDrop", "cascFrom", d.stations);
  setupSearchDrop("boardSearch", "boardDrop", "boardStation", d.stations, (s) => loadBoard(s.index));
  setInterval(tickClock, 1000); tickClock();
  if (d.stations.length > 9) { $("to").value = d.stations[9].index; $("toSearch").value = d.stations[9].name; }
  $("go").onclick = runPlan;
  $("cascGo").onclick = () => runCascade(+$("cascFrom").value);
  $("cascClear").onclick = clearCascade;
  $("pccApply").onclick = applyPcc;
  $("pccReset").onclick = resetPcc;
  document.querySelectorAll("#matModes button").forEach((b) => {
    b.onclick = () => { document.querySelectorAll("#matModes button").forEach((x) => x.classList.remove("on")); b.classList.add("on"); setMatMode(b.dataset.m); };
  });

  const mc = $("matrix");
  if (d.stationCount > 150) { const cell = Math.max(1, Math.floor(560 / d.stationCount)); mc.width = Math.min(cell * d.stationCount, 560); mc.height = mc.width; }
  buildAdjacency(d); drawMatrix(); initMap(d); loadHubs();
  updateNetworkLoad(); bootLog("select a station to inject a delay", "act");
  setInterval(pingLatency, 8000);
}

// NETWORK LOAD = fraction of the network currently engaged.
//   • during a cascade → stations hit / total   (operational impact)
//   • otherwise        → active stations under the line filter / total
function updateNetworkLoad() {
  if (!DATA) return;
  let pct, label;
  if (CASCADE) { pct = 100 * (CASCADE.affected.length + 1) / DATA.stationCount; label = "CASCADE IMPACT"; }
  else {
    const active = DATA.stations.filter(stationActive).length;
    pct = 100 * active / DATA.stationCount; label = "NETWORK LOAD";
  }
  const fill = $("slFill"), val = $("slValue"), title = $("slTitle");
  if (fill) fill.style.width = clamp(pct, 0, 100).toFixed(1) + "%";
  if (val) val.textContent = pct.toFixed(1) + "%";
  if (title) title.textContent = label;
}

// real RTT to the backend — drives the footer latency + uplink dot
function pingLatency() {
  const t = performance.now();
  fetch("graph/config", { cache: "no-store" })
    .then((r) => { if (!r.ok) throw 0; setUplink(true, performance.now() - t); })
    .catch(() => setUplink(false));
}

// ---- search dropdown ----
function setupSearchDrop(inputId, dropId, hiddenId, stations, onPick) {
  const input = $(inputId), drop = $(dropId), hidden = $(hiddenId); let hl = -1;
  function render(q) {
    q = q.toLowerCase();
    const m = q ? stations.filter((s) => s.name.toLowerCase().includes(q)).slice(0, 30) : stations.slice(0, 30);
    drop.innerHTML = ""; hl = -1;
    m.forEach((s) => {
      const div = document.createElement("div"); div.className = "opt"; div.textContent = s.name;
      div.onclick = () => { hidden.value = s.index; input.value = s.name; drop.style.display = "none"; if (onPick) onPick(s); };
      drop.appendChild(div);
    });
    drop.style.display = m.length ? "block" : "none";
  }
  input.addEventListener("focus", () => render(input.value));
  input.addEventListener("input", () => render(input.value));
  input.addEventListener("keydown", (e) => {
    const o = drop.querySelectorAll(".opt");
    if (e.key === "ArrowDown") { e.preventDefault(); hl = Math.min(hl + 1, o.length - 1); u(o); }
    else if (e.key === "ArrowUp") { e.preventDefault(); hl = Math.max(hl - 1, 0); u(o); }
    else if (e.key === "Enter" && hl >= 0 && o[hl]) { e.preventDefault(); o[hl].click(); }
    else if (e.key === "Escape") { drop.style.display = "none"; }
  });
  function u(o) { o.forEach((x, i) => x.classList.toggle("hl", i === hl)); }
  document.addEventListener("click", (e) => { if (!e.target.closest(".search-wrap")) drop.style.display = "none"; });
}

let ADJ;
function buildAdjacency(d) { ADJ = new Map(); d.edges.forEach(([a, b, w]) => ADJ.set(a + "," + b, w)); }

// =====================================================================
//  MATRIX (mathematical operators) — canvas
// =====================================================================
const mc = $("matrix"), mctx = mc.getContext("2d");
function setMatMode(m) {
  MAT_MODE = m;
  if (m === "A" || SPECTRAL[m]) { drawMatrix(); return; }
  $("mathEq").textContent = "loading " + m + " …";
  fetch("graph/spectral?form=" + m).then((r) => r.json()).then((sm) => {
    const map = new Map(); let maxAbs = 0;
    for (let k = 0; k < sm.vals.length; k++) { const v = sm.vals[k]; map.set(sm.rows[k] + "," + sm.cols[k], v); const a = Math.abs(v); if (a > maxAbs) maxAbs = a; }
    SPECTRAL[m] = { map, lambdaMax: sm.lambdaMax, nnz: sm.vals.length, maxAbs, n: sm.n }; drawMatrix();
  }).catch(() => { $("mathEq").textContent = "spectral fetch failed"; });
}
function divColor(v, maxAbs) { if (!v) return "#0a0d12"; const t = clamp(v / maxAbs, -1, 1), mag = Math.pow(Math.abs(t), 0.6); return `hsl(${v < 0 ? 210 : 8},85%,${8 + 52 * mag}%)`; }
function seqColor(t) { return `hsl(190,70%,${70 - 48 * t}%)`; }
function stationActive(s) { return s.lines.length === 0 || s.lines.some((l) => ACTIVE_LINES.has(l)); }
function currentCell(i, j) {
  if (MAT_MODE === "A") { const w = ADJ.get(i + "," + j); return w === undefined ? null : w; }
  const S = SPECTRAL[MAT_MODE]; if (!S) return null; const v = S.map.get(i + "," + j); return v === undefined ? null : v;
}
function drawMatrix() {
  const n = DATA.stationCount, size = mc.width, cell = size / n;
  mctx.clearRect(0, 0, size, size); mctx.fillStyle = "#0a0d12"; mctx.fillRect(0, 0, size, size);
  let painter, lo, hi;
  if (MAT_MODE === "A") {
    let mn = 1e9, mx = 0; ADJ.forEach((w) => { if (w < mn) mn = w; if (w > mx) mx = w; });
    painter = (v) => seqColor(mx > mn ? (v - mn) / (mx - mn) : 0); lo = (mn / 60).toFixed(1) + "m"; hi = (mx / 60).toFixed(1) + "m";
  } else { const S = SPECTRAL[MAT_MODE]; if (!S) return; painter = (v) => divColor(v, S.maxAbs); lo = "−" + S.maxAbs.toFixed(2); hi = "+" + S.maxAbs.toFixed(2); }
  for (let r = 0; r < n; r++) for (let c = 0; c < n; c++) {
    const i = ORDER[r], j = ORDER[c], v = currentCell(i, j); if (v === null) continue;
    const act = stationActive(DATA.stations[i]) && stationActive(DATA.stations[j]);
    mctx.fillStyle = act ? painter(v) : "#1b222b"; mctx.fillRect(c * cell, r * cell, Math.ceil(cell), Math.ceil(cell));
  }
  if (SEL >= 0) { const dp = ORDER.indexOf(SEL); if (dp >= 0) { mctx.fillStyle = "rgba(240,198,116,.22)"; mctx.fillRect(0, dp * cell, size, Math.ceil(cell)); mctx.fillRect(dp * cell, 0, Math.ceil(cell), size); } }
  drawColorbar(); updateMathBox(lo, hi);
}
function drawColorbar() { const cb = $("cbar"), x = cb.getContext("2d"), W = cb.width, H = cb.height; for (let px = 0; px < W; px++) { const t = px / (W - 1); x.fillStyle = MAT_MODE === "A" ? seqColor(t) : divColor(t * 2 - 1, 1); x.fillRect(px, 0, 1, H); } }
function updateMathBox(lo, hi) {
  const meta = {
    A: ["A — weighted adjacency, aᵢⱼ = segment running time (s)", "Directed timetable graph. Block-diagonal structure = commercial lines."],
    adjacency: ["Â = D̃<sup>−½</sup>(W+I)D̃<sup>−½</sup> — renormalized GCN propagation", "Self-loops added, symmetric-normalized; one GCN layer’s operator."],
    laplacian: ["L = I − D<sup>−½</sup>W D<sup>−½</sup> — symmetric normalized Laplacian", "Diag = +1; off-diag ≤ 0. spec(L) ⊂ [0, 2]."],
    scaled: ["L̃ = (2/λ<sub>max</sub>)·L − I — Chebyshev-rescaled Laplacian (STGCN)", "spec(L̃) ⊂ [−1, 1] → Tₖ(L̃)=2L̃·Tₖ₋₁−Tₖ₋₂ stable."],
  }[MAT_MODE];
  $("mathEq").innerHTML = meta[0]; const n = DATA.stationCount; let nnz, lam = "—", range;
  if (MAT_MODE === "A") { nnz = ADJ.size; range = lo + " … " + hi; }
  else { const S = SPECTRAL[MAT_MODE]; nnz = S.nnz; lam = S.lambdaMax > 0 ? S.lambdaMax.toFixed(4) : "—"; range = "[" + lo + ", " + hi + "]"; }
  $("mathStats").innerHTML =
    `<span class="stat">n × n: <b>${n} × ${n}</b></span><span class="stat">nnz: <b>${nnz.toLocaleString()}</b></span>` +
    `<span class="stat">density: <b>${(100 * nnz / (n * n)).toFixed(3)}%</b></span><span class="stat">λ<sub>max</sub>(L): <b>${lam}</b></span>` +
    `<span class="stat">range: <b>${range}</b></span><span class="stat">symmetry: <b>${MAT_MODE === "A" ? "directed" : "symmetric"}</b></span>` +
    `<div class="muted" style="margin-top:6px;">${meta[1]}</div>`;
  $("cbarLo").textContent = lo; $("cbarHi").textContent = hi;
}
mc.addEventListener("mousemove", (ev) => {
  const n = DATA.stationCount, b = mc.getBoundingClientRect(), cellCSS = b.width / n;
  const c = Math.floor((ev.clientX - b.left) / cellCSS), r = Math.floor((ev.clientY - b.top) / cellCSS);
  if (r < 0 || c < 0 || r >= n || c >= n) { hideTip(); return; }
  const i = ORDER[r], j = ORDER[c], v = currentCell(i, j); if (v === null) { hideTip(); return; }
  const val = MAT_MODE === "A" ? `run ${v}s (${(v / 60).toFixed(1)} min)` : `${MAT_MODE === "scaled" ? "L̃" : MAT_MODE === "laplacian" ? "L" : "Â"}ᵢⱼ = ${v.toFixed(4)}`;
  showTip(ev.clientX, ev.clientY, `<b>${DATA.stations[i].name}</b> → <b>${DATA.stations[j].name}</b><br/>${val}`);
});
mc.addEventListener("mouseleave", hideTip);

// =====================================================================
//  MAP (Mapbox GL)
// =====================================================================
function coord(idx) { const s = DATA.stations[idx]; return [s.lon, s.lat]; }
function legColor(stops) {
  const a = DATA.stations[stops[0]], b = DATA.stations[stops[stops.length - 1]];
  const common = a.lines.find((l) => b.lines.includes(l));
  return common ? window.LINE_COLORS[common] : "#9aa4b2";
}
function legLine(stops) {
  const a = DATA.stations[stops[0]], b = DATA.stations[stops[stops.length - 1]];
  return a.lines.find((l) => b.lines.includes(l)) || "—";
}
function stationsGeoJSON(d) {
  let prMax = 0; d.stations.forEach((s) => (prMax = Math.max(prMax, s.pagerank)));
  return { type: "FeatureCollection", features: d.stations.map((s) => ({
    type: "Feature", geometry: { type: "Point", coordinates: [s.lon, s.lat] },
    properties: { index: s.index, name: s.name, line0: s.lines[0] || "—", lines: s.lines.join(", ") || "—",
      color: s.lines.length ? (window.LINE_COLORS[s.lines[0]] || "#58a6ff") : "#8b949e",
      r: 2 + 4 * Math.sqrt(s.pagerank / (prMax || 1)), hub: s.pagerank } })) };
}
function edgesGeoJSON(d) {
  return { type: "FeatureCollection", features: d.edges.map(([a, b]) => {
    const sa = d.stations[a], sb = d.stations[b];
    const common = sa.lines.find((l) => sb.lines.includes(l));
    const g = GEOM && GEOM.get(a + "," + b);
    return { type: "Feature", geometry: { type: "LineString", coordinates: g || [[sa.lon, sa.lat], [sb.lon, sb.lat]] },
      properties: { line0: common || sa.lines[0] || "—", color: common ? window.LINE_COLORS[common] : "#475569" } }; }) };
}
function empty() { return { type: "FeatureCollection", features: [] }; }

// real track curvature from shapes.txt — overlay it onto the base network once loaded
function loadGeometry(d) {
  fetch("graph/geometry").then((r) => r.json()).then((res) => {
    if (!res.segments || !res.segments.length) return;
    GEOM = new Map(); res.segments.forEach((s) => GEOM.set(s.from + "," + s.to, s.path));
    if (MAP_READY) { map.getSource("edges").setData(edgesGeoJSON(d)); }
    if (JOURNEY) drawJourney(JOURNEY); // redraw with curves if a journey is shown
  }).catch(() => {});
}

function initMap(d) {
  if (!MAPBOX_TOKEN || MAPBOX_TOKEN.indexOf("pk.") !== 0) { $("map").innerHTML = '<div class="mapwarn">No valid Mapbox token. Append ?token=pk... to the URL.</div>'; return; }
  mapboxgl.accessToken = MAPBOX_TOKEN;
  let la1 = 90, la2 = -90, lo1 = 180, lo2 = -180;
  d.stations.forEach((s) => { la1 = Math.min(la1, s.lat); la2 = Math.max(la2, s.lat); lo1 = Math.min(lo1, s.lon); lo2 = Math.max(lo2, s.lon); });
  map = new mapboxgl.Map({ container: "map", style: "mapbox://styles/mapbox/dark-v11", center: [(lo1 + lo2) / 2, (la1 + la2) / 2], zoom: 9 });
  map.addControl(new mapboxgl.NavigationControl({ showCompass: false }), "top-right");
  map.on("error", (e) => { if (e && e.error && /access token|401/i.test(e.error.message || "")) $("map").innerHTML = '<div class="mapwarn">Mapbox token rejected (401). Check the token / its URL restrictions.</div>'; });

  map.on("load", () => {
    map.addSource("edges", { type: "geojson", data: edgesGeoJSON(d) });
    map.addSource("stations", { type: "geojson", data: stationsGeoJSON(d) });
    map.addSource("cascade", { type: "geojson", data: empty() });
    map.addSource("journey", { type: "geojson", data: empty() });
    map.addSource("journey-pts", { type: "geojson", data: empty() });

    // subtle outer glow for depth
    map.addLayer({ id: "edges-glow-outer", type: "line", source: "edges",
      paint: { "line-color": ["get", "color"], "line-width": 6, "line-blur": 5, "line-opacity": 0.08 } });
    // tighter inner glow
    map.addLayer({ id: "edges-glow-inner", type: "line", source: "edges",
      paint: { "line-color": ["get", "color"], "line-width": 2.5, "line-blur": 1.5, "line-opacity": 0.25 } });
    // solid core line
    map.addLayer({ id: "edges", type: "line", source: "edges",
      paint: { "line-color": ["get", "color"], "line-width": 1.2, "line-opacity": 0.7 } });

    map.addLayer({ id: "cascade-glow", type: "circle", source: "cascade",
      paint: { "circle-radius": ["interpolate", ["linear"], ["get", "frac"], 0, 10, 1, 34],
        "circle-color": ["interpolate", ["linear"], ["get", "frac"], 0, "#3fb950", 0.5, "#f0c674", 1, "#f85149"],
        "circle-blur": 1, "circle-opacity": 0.35 } });

    // RAPTOR journey (under stations so dots stay visible)
    map.addLayer({ id: "journey-glow", type: "line", source: "journey",
      paint: { "line-color": ["get", "color"], "line-width": 12, "line-blur": 6, "line-opacity": 0.35 },
      layout: { "line-cap": "round", "line-join": "round" } });
    map.addLayer({ id: "journey-line", type: "line", source: "journey",
      paint: { "line-color": ["get", "color"], "line-width": 5,
        "line-opacity": 0.95, "line-dasharray": ["case", ["==", ["get", "kind"], "WALK"], ["literal", [1, 1.6]], ["literal", [1, 0]]] },
      layout: { "line-cap": "round", "line-join": "round" } });

    map.addLayer({ id: "stations", type: "circle", source: "stations",
      paint: { "circle-radius": ["get", "r"], "circle-color": ["get", "color"],
        "circle-stroke-width": 0.5, "circle-stroke-color": "#0d1117", "circle-opacity": 0.85 } });

    map.addLayer({ id: "cascade", type: "circle", source: "cascade",
      paint: { "circle-radius": ["interpolate", ["linear"], ["get", "frac"], 0, 4, 1, 11],
        "circle-color": ["interpolate", ["linear"], ["get", "frac"], 0, "#3fb950", 0.5, "#f0c674", 1, "#f85149"],
        "circle-stroke-width": ["case", ["get", "missed"], 2, 0], "circle-stroke-color": "#fff" } });
    map.addLayer({ id: "cascade-src", type: "circle", source: "cascade", filter: ["==", ["get", "src"], true],
      paint: { "circle-radius": 9, "circle-color": "#ffffff", "circle-stroke-width": 2, "circle-stroke-color": "#ff7b72" } });

    // journey endpoint markers (board / transfer / alight) on top
    map.addLayer({ id: "journey-pts", type: "circle", source: "journey-pts",
      paint: { "circle-radius": 6, "circle-color": ["get", "color"], "circle-stroke-width": 2, "circle-stroke-color": "#0d1117" } });

    MAP_READY = true; applyLineFilter(); loadGeometry(d);
    map.fitBounds([[lo1, la1], [lo2, la2]], { padding: 30, duration: 0 });

    popup = new mapboxgl.Popup({ closeButton: false, closeOnClick: false });
    map.on("mousemove", "stations", (e) => {
      map.getCanvas().style.cursor = "pointer"; const f = e.features[0], p = f.properties;
      let extra = "";
      if (CASCADE) { const a = CASCADE.byIndex.get(+p.index);
        extra = a ? `<br/><span style="color:#f0c674">δ=${(a.propagatedDelaySec / 60).toFixed(1)}min · slack −${(a.absorbedSlackSec / 60).toFixed(1)}min · hop ${a.hop}${a.missedConnection ? " · ✖ missed" : ""}</span>` : '<br/><span class="muted">not in cascade</span>'; }
      popup.setLngLat(f.geometry.coordinates).setHTML(`<b>${p.name}</b><br/>lines: ${p.lines}<br/>hub: ${(+p.hub).toFixed(4)}${extra}`).addTo(map);
    });
    map.on("mouseleave", "stations", () => { map.getCanvas().style.cursor = ""; popup.remove(); });
    map.on("click", "stations", (e) => {
      const idx = +e.features[0].properties.index, nm = e.features[0].properties.name; SEL = idx; drawMatrix();
      $("cascFrom").value = idx; $("cascSearch").value = nm; runCascade(idx);
      $("boardStation").value = idx; $("boardSearch").value = nm; loadBoard(idx);
    });
  });
}
function applyLineFilter() {
  if (!MAP_READY) return; const arr = [...ACTIVE_LINES];
  const fil = ["any", ["==", ["get", "line0"], "—"], ["in", ["get", "line0"], ["literal", arr]]];
  map.setFilter("stations", fil); map.setFilter("edges", fil); map.setFilter("edges-glow-inner", fil); map.setFilter("edges-glow-outer", fil);
}

// =====================================================================
//  CASCADE — /graph/propagate then animate on the map
// =====================================================================
function clearCascade() { CASCADE = null; if (MAP_READY) map.getSource("cascade").setData(empty()); $("cascStats").innerHTML = '<span class="muted">cleared.</span>'; updateNetworkLoad(); }
function cascadeFeatures(reveal) {
  const C = CASCADE, f = [];
  f.push({ type: "Feature", geometry: { type: "Point", coordinates: coord(C.source) }, properties: { src: true, frac: 1, missed: false } });
  C.affected.forEach((a) => { if (a.hop > reveal) return; f.push({ type: "Feature", geometry: { type: "Point", coordinates: coord(a.stationIndex) }, properties: { src: false, frac: a.propagatedDelaySec / C.maxDelay, missed: !!a.missedConnection } }); });
  return { type: "FeatureCollection", features: f };
}
function runCascade(from) {
  if (isNaN(from)) { $("cascStats").innerHTML = '<span class="muted">select a station.</span>'; return; }
  const mins = +$("cascDelay").value, delay = Math.round(mins * 60);
  $("cascStats").textContent = "propagating…";
  fetch(`graph/propagate?from=${from}&delay=${delay}`).then((r) => r.json()).then((res) => {
    if (!res.length) { $("cascStats").innerHTML = '<span class="muted">no downstream propagation (slack absorbed it immediately).</span>'; clearCascade(); return; }
    let maxDelay = 0, maxHop = 0, slackSum = 0, missed = 0; const byIndex = new Map();
    res.forEach((a) => { byIndex.set(a.stationIndex, a); maxDelay = Math.max(maxDelay, a.propagatedDelaySec); maxHop = Math.max(maxHop, a.hop); slackSum += a.absorbedSlackSec; if (a.missedConnection) missed++; });
    CASCADE = { byIndex, affected: res, source: from, maxDelay: maxDelay || 1, maxHop, start: performance.now() };
    updateNetworkLoad();
    const name = DATA.stations[from].name;
    $("cascStats").innerHTML =
      `<div class="casc-stats">` +
      `<span class="k">source</span><span class="v">${name}</span>` +
      `<span class="k">primary δ₀</span><span class="v">${(delay / 60).toFixed(0)} min</span>` +
      `<span class="k">stations hit</span><span class="v">${res.length} / ${DATA.stationCount}</span>` +
      `<span class="k">max hop depth</span><span class="v">${maxHop}</span>` +
      `<span class="k">peak residual δ</span><span class="v">${(maxDelay / 60).toFixed(1)} min</span>` +
      `<span class="k">slack absorbed Σ</span><span class="v">${(slackSum / 60).toFixed(0)} min</span>` +
      `<span class="k">missed connections</span><span class="v"><span class="pill" style="background:${missed ? "#da3633" : "#238636"}">${missed}</span></span></div>` +
      `<div class="cbar"><span>0</span><canvas id="dbar" width="160" height="10"></canvas><span>${(maxDelay / 60).toFixed(0)}m</span></div>`;
    const db = $("dbar"); if (db) { const x = db.getContext("2d"); for (let px = 0; px < db.width; px++) { const t = px / (db.width - 1); x.fillStyle = t < 0.5 ? `hsl(${140 - 80 * t * 2},80%,55%)` : `hsl(${60 - 60 * (t - 0.5) * 2},85%,55%)`; x.fillRect(px, 0, 1, db.height); } }
    animateCascade();
  }).catch(() => { $("cascStats").textContent = "propagation failed"; });
}
function animateCascade() {
  if (!CASCADE || !MAP_READY) return;
  const dur = 2400, t = clamp((performance.now() - CASCADE.start) / dur, 0, 1), reveal = t * (CASCADE.maxHop + 1);
  map.getSource("cascade").setData(cascadeFeatures(reveal));
  if (t < 1) requestAnimationFrame(animateCascade);
}

// =====================================================================
//  RAPTOR — frontier (/plan) + drawn journey (/journey)
// =====================================================================
// Dim the base network to grey when a journey is shown so the route pops
function dimNetwork() {
  if (!MAP_READY) return;
  map.setPaintProperty("stations", "circle-color", "#3a3f47");
  map.setPaintProperty("stations", "circle-opacity", 0.35);
  map.setPaintProperty("edges", "line-color", "#2a2f38");
  map.setPaintProperty("edges", "line-opacity", 0.25);
  map.setPaintProperty("edges-glow-inner", "line-color", "#2a2f38");
  map.setPaintProperty("edges-glow-inner", "line-opacity", 0.08);
  map.setPaintProperty("edges-glow-outer", "line-opacity", 0);
}
// Restore vivid colors when journey is cleared
function restoreNetwork() {
  if (!MAP_READY) return;
  map.setPaintProperty("stations", "circle-color", ["get", "color"]);
  map.setPaintProperty("stations", "circle-opacity", 0.85);
  map.setPaintProperty("edges", "line-color", ["get", "color"]);
  map.setPaintProperty("edges", "line-opacity", 0.7);
  map.setPaintProperty("edges-glow-inner", "line-color", ["get", "color"]);
  map.setPaintProperty("edges-glow-inner", "line-opacity", 0.25);
  map.setPaintProperty("edges-glow-outer", "line-opacity", 0.08);
}
function clearJourney() { JOURNEY = null; if (MAP_READY) { map.getSource("journey").setData(empty()); map.getSource("journey-pts").setData(empty()); restoreNetwork(); } }
function drawJourney(path) {
  if (!MAP_READY || !path.reachable || !path.legs.length) { clearJourney(); return; }
  JOURNEY = path;
  const legFeats = path.legs.map((leg) => ({
    type: "Feature",
    geometry: { type: "LineString", coordinates: (leg.shape && leg.shape.length) ? leg.shape : leg.stops.map(coord) },
    properties: { kind: leg.kind, color: leg.kind === "WALK" ? "#c9d1d9" : legColor(leg.stops) },
  }));
  // endpoint markers: board (green) → transfers (amber) → alight (red)
  const pts = [];
  const first = path.legs[0].stops[0];
  pts.push({ type: "Feature", geometry: { type: "Point", coordinates: coord(first) }, properties: { color: "#3fb950" } }); // board
  for (let i = 0; i < path.legs.length - 1; i++) {
    const junction = path.legs[i].stops[path.legs[i].stops.length - 1];
    pts.push({ type: "Feature", geometry: { type: "Point", coordinates: coord(junction) }, properties: { color: "#f0c674" } }); // transfer
  }
  const last = path.legs[path.legs.length - 1].stops[path.legs[path.legs.length - 1].stops.length - 1];
  pts.push({ type: "Feature", geometry: { type: "Point", coordinates: coord(last) }, properties: { color: "#f85149" } }); // alight

  // Dim the base network so the journey route stands out
  dimNetwork();

  map.getSource("journey").setData({ type: "FeatureCollection", features: legFeats });
  map.getSource("journey-pts").setData({ type: "FeatureCollection", features: pts });

  // frame the journey
  let la1 = 90, la2 = -90, lo1 = 180, lo2 = -180;
  legFeats.forEach((f) => f.geometry.coordinates.forEach(([lo, la]) => { la1 = Math.min(la1, la); la2 = Math.max(la2, la); lo1 = Math.min(lo1, lo); lo2 = Math.max(lo2, lo); }));
  map.fitBounds([[lo1, la1], [lo2, la2]], { padding: 80, maxZoom: 13, duration: 900 });
}
function renderLegList(path) {
  if (!path.reachable) { return '<span class="muted">unreachable within the round bound.</span>'; }
  const rows = path.legs.map((leg) => {
    const from = DATA.stations[leg.stops[0]].name, to = DATA.stations[leg.stops[leg.stops.length - 1]].name;
    if (leg.kind === "WALK") return `<div class="row"><span class="muted">🚶 transfer</span> ${from} → ${to}</div>`;
    const ln = legLine(leg.stops), col = window.LINE_COLORS[ln] || "#9aa4b2";
    return `<div class="row"><span class="swatch" style="background:${col}"></span><b>${ln}</b> ${from} → ${to}
      <span class="muted">(${hms(leg.depSec)}–${hms(leg.arrSec)}, ${leg.stops.length} stops)</span></div>`;
  }).join("");
  return `<div class="row"><b>arrive ${hms(path.arrivalSec)}</b> · ${path.transfers} transfer${path.transfers === 1 ? "" : "s"} · ${((path.arrivalSec - path.departureSec) / 60).toFixed(0)} min</div>` + rows;
}
function runPlan() {
  const from = +$("from").value, to = +$("to").value;
  if (isNaN(from) || isNaN(to) || from === to) { $("plan").innerHTML = '<span class="muted">Select different origin and destination.</span>'; return; }
  const [h, m] = $("dep").value.split(":").map(Number), dep = h * 3600 + m * 60, el = $("plan");
  el.textContent = "planning…";
  // 1) Pareto frontier, 2) concrete path drawn on the map.
  fetch(`graph/plan?from=${from}&to=${to}&departure=${dep}`).then((r) => r.json()).then((p) => {
    const frontier = (p.frontier || []).map((f) => `<div class="row"><b>${f.transfers}</b> transfer${f.transfers === 1 ? "" : "s"} → arrive <b>${hms(f.arrivalSec)}</b> <span class="muted">(${((f.arrivalSec - dep) / 60).toFixed(0)} min)</span></div>`).join("");
    return fetch(`graph/journey?from=${from}&to=${to}&departure=${dep}`).then((r) => r.json()).then((path) => {
      drawJourney(path);
      el.innerHTML = `<div style="margin:4px 0;color:#8b949e">Earliest-arrival journey</div>${renderLegList(path)}` +
        (frontier ? `<div style="margin:8px 0 2px;color:#8b949e">Pareto frontier (arrival vs transfers)</div>${frontier}` : "");
    });
  }).catch(() => { el.textContent = "plan failed"; });
}

function loadHubs() {
  fetch("graph/stats").then((r) => r.json()).then((s) => {
    renderHubs(s.topHubs);
    if (s.topHubs.length && !$("cascFrom").value) { const top = DATA.stations.find((x) => x.name === s.topHubs[0].name); if (top) { $("cascFrom").value = top.index; $("cascSearch").value = top.name; } }
  }).catch(() => {});
  fetch("graph/hub/pcc").then((r) => r.json()).then(showPccState).catch(() => {});
}
function renderHubs(hubs) { $("hubs").innerHTML = hubs.map((h, i) => `${i + 1}. ${h.name} <span class="muted">${h.pagerank.toFixed(4)}</span>`).join("<br/>"); }

// ---- PCC control: operational overrides drive a hub-ranking recompute ----
function showPccState(res) {
  const lo = res.lineOverrides || {}, so = res.stationOverrides || {};
  const parts = [...Object.entries(lo).map(([k, v]) => `line ${k} ×${v}`),
                 ...Object.entries(so).map(([k, v]) => `${DATA.stations[k] ? DATA.stations[k].name : "#" + k} ×${v}`)];
  $("pccState").textContent = parts.length ? "active overrides: " + parts.join(" · ") : "no overrides";
}
function onPccResult(res) {
  if (res.topHubs) renderHubs(res.topHubs);
  showPccState(res);
  // reflect the new ranking on the map (dot sizes are ∝ PageRank)
  fetch("graph/matrix").then((r) => r.json()).then((d) => { DATA.stations = d.stations; if (MAP_READY) map.getSource("stations").setData(stationsGeoJSON(d)); }).catch(() => {});
}
function applyPcc() {
  const target = $("pccTarget").value, id = $("pccId").value.trim(), weight = parseFloat($("pccWeight").value);
  if (!id || isNaN(weight)) { $("pccState").textContent = "enter a line code / station index and a weight."; return; }
  $("pccState").textContent = "applying & recomputing…";
  fetch("graph/hub/pcc", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ target, id, weight, reason: "viz" }) })
    .then((r) => r.json()).then(onPccResult).catch(() => { $("pccState").textContent = "PCC apply failed."; });
}
function resetPcc() {
  fetch("graph/hub/pcc", { method: "DELETE" }).then((r) => r.json()).then(onPccResult).catch(() => {});
}

// =====================================================================
//  DEPARTURE BOARD — SSE off the rail.display Kafka topic, per line ("canal")
//  The display SUBSCRIBES to the event stream; it never polls a REST endpoint.
// =====================================================================
let BOARD_ES = null, BOARD_STATION = -1, BOARD_MSGS = new Map(), BOARD_SINCE = 0;
function fmtClockMs(ms) { const d = new Date(ms); return String(d.getHours()).padStart(2, "0") + ":" + String(d.getMinutes()).padStart(2, "0"); }
function loadBoard(idx) {
  if (isNaN(idx) || idx === BOARD_STATION) return;
  BOARD_STATION = idx; BOARD_MSGS = new Map(); BOARD_SINCE = 0;
  if (BOARD_ES) { BOARD_ES.close(); BOARD_ES = null; }
  $("board").innerHTML = `<span class="muted">subscribing to rail.display for ${DATA.stations[idx].name}…</span>`;
  BOARD_ES = new EventSource(`graph/display/stream?station=${idx}`);
  BOARD_ES.addEventListener("departure", (ev) => {
    const m = JSON.parse(ev.data); if (m.station !== BOARD_STATION) return;
    BOARD_MSGS.set(m.line + "|" + m.depMs, m); BOARD_SINCE = Date.now(); renderBoard();
  });
  BOARD_ES.onerror = () => { if (!BOARD_MSGS.size) $("board").innerHTML = '<span class="muted">no rail.display feed (is Kafka running?). Stream will populate once events arrive.</span>'; };
}
function renderBoard() {
  const el = $("board"); if (BOARD_STATION < 0) return;
  const now = Date.now();
  const byLine = new Map();
  BOARD_MSGS.forEach((m) => {
    if (m.depMs < now - 60000) return; // prune departures more than a minute past
    if (!byLine.has(m.line)) byLine.set(m.line, []);
    byLine.get(m.line).push(m);
  });
  const lines = [...byLine.keys()].sort();
  if (!lines.length) { el.innerHTML = `<span class="muted">${DATA.stations[BOARD_STATION].name}: awaiting departures on rail.display…</span>`; return; }
  el.innerHTML = `<div class="muted" style="margin-bottom:6px;">${DATA.stations[BOARD_STATION].name} · ${lines.length} canal${lines.length === 1 ? "" : "s"} · live from rail.display</div>` +
    lines.map((line) => {
      const col = (window.LINE_COLORS && window.LINE_COLORS[line]) || "#58a6ff";
      const deps = byLine.get(line).sort((a, b) => a.depMs - b.depMs).slice(0, 5);
      const rows = deps.map((d) => {
        const eta = Math.round((d.depMs - now) / 1000);
        const etaTxt = eta <= 0 ? "now" : eta < 60 ? "<1 min" : Math.round(eta / 60) + " min";
        const cls = eta <= 0 ? "now" : eta < 120 ? "soon" : "";
        return `<div class="dep"><span class="dep-time">${fmtClockMs(d.depMs)}</span><span class="dep-dest">→ ${d.destination}</span><span class="dep-eta ${cls}">${etaTxt}</span></div>`;
      }).join("");
      return `<div class="board-line"><div class="bl-head"><span class="bl-badge" style="background:${col}">${line}</span><span class="muted">${deps.length} upcoming</span></div>${rows}</div>`;
    }).join("");
}
function tickClock() {
  const c = $("boardClock"); if (c) c.textContent = hms(nowLocalSec());
  if (BOARD_STATION >= 0 && BOARD_MSGS.size) renderBoard(); // live countdown + prune
}
