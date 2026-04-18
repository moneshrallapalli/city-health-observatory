/* City Health Observatory — editorial UI over the cloned REST backend.
 * Endpoints wired:
 *   GET  /state         — live district snapshot (hero + leaderboard)
 *   GET  /info          — histogram + stats
 *   GET  /performance   — endpoint latency + pipeline stats
 *   GET  /history       — event feed
 *   POST /add           — submit score (with catastrophic flag)
 *   POST /remove        — remove district
 *   GET  /leaderboard   — HTML view (linked, not embedded)
 *   GET  /openapi.yaml  — spec (linked)
 */

const POLL_MS = 5000;
const INFO_POLL_MS = 4000;
const PERF_POLL_MS = 5000;
const HISTORY_POLL_MS = 3000;
const HISTORY_LIMIT = 2000;        // max rows per /history call (API cap)
const HISTORY_MAX_PAGES = 5;       // pagination safety · 5 × 2000 = 10k events
const SPARK_LEN = 36;

// ─── theme-aware colour scale ──────────────────────────────────────────────
// Stops live as CSS variables (--health-0/50/100) so the same gradient shifts
// with the edition. We cache-parse them once per theme change to keep paint hot.
const THEME_COLOR_CACHE = { theme: null, stops: null, rule: null, ink: null, ink2: null, ink3: null, paper3: null, bands: null };

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}
function parseRgb(str) {
  const m = String(str).match(/-?\d+(\.\d+)?/g);
  return m && m.length >= 3 ? [+m[0], +m[1], +m[2]] : [0, 0, 0];
}
function refreshThemeCache() {
  const theme = document.documentElement.dataset.theme || "light";
  THEME_COLOR_CACHE.theme = theme;
  THEME_COLOR_CACHE.stops = [
    [0,   parseRgb(cssVar("--health-0"))],
    [50,  parseRgb(cssVar("--health-50"))],
    [100, parseRgb(cssVar("--health-100"))],
  ];
  THEME_COLOR_CACHE.rule   = cssVar("--rule");
  THEME_COLOR_CACHE.ink    = cssVar("--ink");
  THEME_COLOR_CACHE.ink2   = cssVar("--ink-2");
  THEME_COLOR_CACHE.ink3   = cssVar("--ink-3");
  THEME_COLOR_CACHE.paper3 = cssVar("--paper-3");
  THEME_COLOR_CACHE.bands  = {
    critical: `rgb(${THEME_COLOR_CACHE.stops[0][1].join(",")})`,
    watch:    `rgb(${THEME_COLOR_CACHE.stops[1][1].join(",")})`,
    healthy:  `rgb(${THEME_COLOR_CACHE.stops[2][1].join(",")})`,
  };
}

function healthColor(score) {
  if (THEME_COLOR_CACHE.stops == null
   || THEME_COLOR_CACHE.theme !== (document.documentElement.dataset.theme || "light")) {
    refreshThemeCache();
  }
  if (score == null || Number.isNaN(score)) return THEME_COLOR_CACHE.rule || "#c8c0ad";
  const stops = THEME_COLOR_CACHE.stops;
  let lo = stops[0], hi = stops[stops.length - 1];
  for (let i = 0; i < stops.length - 1; i++) {
    if (score >= stops[i][0] && score <= stops[i + 1][0]) { lo = stops[i]; hi = stops[i + 1]; break; }
  }
  const t = (score - lo[0]) / (hi[0] - lo[0] || 1);
  const c = [0, 1, 2].map(k => Math.round(lo[1][k] + (hi[1][k] - lo[1][k]) * t));
  return `rgb(${c[0]}, ${c[1]}, ${c[2]})`;
}

// ─── http ──────────────────────────────────────────────────────────────────
async function fetchJSON(url, opts) {
  const r = await fetch(url, {
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    ...opts,
  });
  const text = await r.text();
  let body;
  try { body = text ? JSON.parse(text) : null; } catch { body = { raw: text }; }
  if (!r.ok) {
    const detail = body && body.detail ? body.detail : body;
    const msg = typeof detail === "string" ? detail : (detail && detail.raw) || JSON.stringify(detail);
    throw new Error(`${r.status} ${msg}`);
  }
  return body;
}

// ─── app state ─────────────────────────────────────────────────────────────
const state = {
  districts: [],                // latest /state snapshot (full objects)
  history: {},                  // district_id -> [displayed scores] for sparkline
  prev:    {},                  // district_id -> previously displayed score (1dp)
  aggHistory: [],               // last N displayed average UHS values (1dp)
  prevAvg: null,                // previously displayed hero avg (1dp) — baseline for delta
  rowEls: {},                   // district_id -> row element
  latestEvents: [],             // last /history payload
  mode: "—",
  online: false,
  stateInFlight: false,         // guard against overlapping /state polls

  // feed filter state — all client-side, applied to `latestEvents` on paint
  feedFilter: "all",            // all | manual | pipeline | catastrophic
  feedWindowSec: 3600,          // 0 = all, otherwise rolling window in seconds
  feedCustomTime: false,        // when true, use feedFromTs/feedToTs instead of the window
  feedSearch: "",               // free-text match against district_id
  feedDistricts: new Set(),     // multi-select chips; empty = any district
  feedScoreMin: null,           // inclusive
  feedScoreMax: null,           // inclusive
  feedFromTs: null,             // ms epoch · only used when feedCustomTime
  feedToTs:   null,             // ms epoch · only used when feedCustomTime
  feedSort:   "newest",         // newest | oldest | highest | lowest
  feedHitCap: false,            // true when pagination stopped on MAX_PAGES (older events may be missing)

  // leaderboard pagination — renders all ranked districts but only shows a page at a time.
  // pageSize is tuned small enough that pagination is visible even when only a handful of
  // districts are on the board (e.g. before the simulator has warmed up every seed).
  lbPage: 1,
  lbPageSize: 6,
  lbLastRanked: [],             // memo of last ranked list for page-change renders

  overrideTimers: {},           // district_id -> setTimeout id that triggers pollState at TTL expiry
};

function pushHistory(arr, val) {
  arr.push(val);
  if (arr.length > SPARK_LEN) arr.shift();
}

function sparkPath(values, w, h, pad = 2) {
  if (values.length < 2) return "";
  const min = Math.min(...values), max = Math.max(...values);
  const range = max - min || 1;
  return values.map((v, i) => {
    const x = pad + (i / (values.length - 1)) * (w - pad * 2);
    const y = pad + (1 - (v - min) / range) * (h - pad * 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
}

function parseSubmittedAt(s) {
  if (!s) return NaN;
  let iso = String(s).trim().replace(" ", "T");
  iso = iso.replace(/([+-]\d{2})(\d{2})?$/, (_, h, mm) => `${h}:${mm || "00"}`);
  return new Date(iso).getTime();
}

function fmtClock(d = new Date()) {
  const p = n => n.toString().padStart(2, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function round1(x) {
  return Number.isFinite(x) ? Math.round(x * 10) / 10 : x;
}

const REDUCED_MOTION = typeof window !== "undefined"
  && window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

// Smoothly animate an element's numeric text from its last-shown value to `to`.
// State is kept on the element (dataset.val + rafId) so repeated calls cancel
// the in-flight tween and continue from wherever the previous frame landed —
// no snap-backs between polls.
function tweenNumber(el, to, { duration = 900, decimals = 1 } = {}) {
  if (!el || !Number.isFinite(to)) return;
  const prev = Number.parseFloat(el.dataset.val);
  const from = Number.isFinite(prev) ? prev : to;
  if (el.dataset.rafId) cancelAnimationFrame(+el.dataset.rafId);
  el.dataset.val = String(to);
  if (REDUCED_MOTION || from === to) {
    el.textContent = to.toFixed(decimals);
    return;
  }
  const t0 = performance.now();
  const ease = t => 1 - Math.pow(1 - t, 3);   // easeOutCubic
  const step = (now) => {
    const t = Math.min(1, (now - t0) / duration);
    const v = from + (to - from) * ease(t);
    el.textContent = v.toFixed(decimals);
    if (t < 1) el.dataset.rafId = String(requestAnimationFrame(step));
    else delete el.dataset.rafId;
  };
  el.dataset.rafId = String(requestAnimationFrame(step));
}

function setDeltaText(el, delta) {
  if (!el) return;
  if (el.textContent === delta.text && el.dataset.cls === (delta.cls || "")) return;
  el.textContent = delta.text;
  el.dataset.cls = delta.cls || "";
  el.classList.remove("up", "down", "delta-updated");
  if (delta.cls) el.classList.add(delta.cls);
  // Restart a short fade-in so the value change is perceptible without flashing.
  void el.offsetWidth;
  el.classList.add("delta-updated");
}

function fmtDelta(prev, now) {
  if (prev == null || now == null) return { text: "—", cls: "" };
  // Round the delta itself so what we show is exactly (shown_now - shown_prev)
  // at 1 decimal place — no float dust, and the sign tracks what the user sees.
  const d = round1(round1(now) - round1(prev));
  if (d === 0) return { text: "—", cls: "" };
  const sign = d > 0 ? "▲" : "▼";
  const cls  = d > 0 ? "up" : "down";
  return { text: `${sign}  ${Math.abs(d).toFixed(1)}`, cls };
}

function escapeHtml(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

// ─── leaderboard render (case2 style, driven by /state) ────────────────────
function upsertRow(d) {
  if (state.rowEls[d]) return state.rowEls[d];
  const list = document.getElementById("district-list");
  const row = document.createElement("div");
  row.className = "district-row fade-in";
  const delay = Math.min(Object.keys(state.rowEls).length * 40 + 100, 500);
  row.style.setProperty("--delay", `${delay}ms`);
  row.style.opacity = "1";
  row.dataset.district = d;
  row.setAttribute("role", "listitem");
  row.innerHTML = `
    <div class="rank">—</div>
    <div class="name">
      <span class="name-line">
        <span class="name-text"></span>
        <span class="type-badge override hidden" data-role="override-badge">Override</span>
        <span class="type-badge catastrophic hidden" data-role="catastrophic-badge">Catastrophic</span>
      </span>
      <small class="cohort">awaiting first reading</small>
      <div class="metrics" data-role="metrics"></div>
    </div>
    <svg class="spark" viewBox="0 0 120 28" preserveAspectRatio="none">
      <polyline points=""/>
    </svg>
    <div class="delta">—</div>
    <div class="health-bar"><span></span></div>
    <div class="score-num">--</div>
    <button class="remove-btn" title="Remove from leaderboard" aria-label="Remove ${d}">×</button>
    <span class="override-bar" data-role="override-bar" aria-hidden="true"></span>`;
  row.querySelector(".name-text").textContent = d;
  row.querySelector(".remove-btn").addEventListener("click", (e) => {
    e.stopPropagation();
    removeDistrict(d, /*fromRow*/ true);
  });
  list.appendChild(row);
  state.rowEls[d] = row;
  return row;
}

function reorderRows(entries) {
  const list = document.getElementById("district-list");
  entries.forEach((e) => {
    const row = state.rowEls[e.district_id];
    if (row && row.parentNode === list) list.appendChild(row);
  });
}

function pruneRows(alive) {
  for (const [d, row] of Object.entries(state.rowEls)) {
    if (!alive.has(d)) {
      // Short fade-out so the 30s override expiry reads as a deliberate
      // "spotlight is over" rather than an abrupt pop. 320ms matches the
      // .evicting CSS transition.
      if (row.classList.contains("evicting")) continue;
      row.classList.add("evicting");
      const el = row;
      delete state.rowEls[d];
      delete state.history[d];
      delete state.prev[d];
      setTimeout(() => { el.remove(); }, 320);
    }
  }
}

function cohortLabel(i, n) {
  if (!n) return "awaiting first reading";
  if (i < Math.ceil(n / 4))          return "top quartile";
  if (i < Math.ceil(3 * n / 4))      return "mid pack";
  return "bottom quartile";
}

function paintLeaderboard(districts) {
  const list = document.getElementById("district-list");
  const ranked = districts
    .filter(d => d.score != null && Number.isFinite(d.score))
    .sort((a, b) => b.score - a.score);

  if (!ranked.length) {
    list.innerHTML = `<div class="district-row empty">No districts on the board yet. Submit a score or start the pipeline.</div>`;
    state.rowEls = {};
    return;
  }

  // clear empty-state div if present
  if (list.querySelector(".district-row.empty")) {
    list.innerHTML = "";
    state.rowEls = {};
  }

  const alive = new Set(ranked.map(d => d.district_id));
  pruneRows(alive);

  ranked.forEach((d, i) => {
    const row = upsertRow(d.district_id);
    const s = round1(d.score);

    if (!state.history[d.district_id]) state.history[d.district_id] = [];
    pushHistory(state.history[d.district_id], s);

    // Delta = change vs the score last shown to this user. That guarantees
    // on-screen arithmetic holds: shown_prev + delta == shown_now (at 1 dp).
    // If nothing changed this poll, fmtDelta returns "—" (neutral).
    const prev = state.prev[d.district_id];
    const delta = fmtDelta(prev, s);
    const changed = prev != null && round1(s - prev) !== 0;
    state.prev[d.district_id] = s;
    const dropped = prev != null && prev - s > 12;

    row.querySelector(".rank").textContent = (i + 1).toString().padStart(2, "0");
    tweenNumber(row.querySelector(".score-num"), s, { duration: 1200, decimals: 1 });

    const bar = row.querySelector(".health-bar > span");
    bar.style.width = `${Math.max(2, Math.min(100, s))}%`;
    bar.style.background = healthColor(s);
    // No flash pulse — the CSS width/background transition carries the change.

    setDeltaText(row.querySelector(".delta"), delta);

    row.querySelector(".spark polyline").setAttribute("points",
      sparkPath(state.history[d.district_id], 120, 28));

    row.querySelector(".cohort").textContent = cohortLabel(i, ranked.length);

    const comps = d.components || {};
    const mEl = row.querySelector('[data-role="metrics"]');
    const keys = ["traffic", "aqi", "power", "water", "noise"];
    const hasAny = keys.some(k => comps[k] != null && Number.isFinite(comps[k]));
    if (hasAny) {
      mEl.innerHTML = keys
        .filter(k => comps[k] != null && Number.isFinite(comps[k]))
        .map(k => {
          const v = comps[k];
          return `<span class="m" style="--c:${healthColor(v)}">
            <i>${k}</i><b>${v.toFixed(0)}</b></span>`;
        }).join("");
    } else {
      mEl.innerHTML = "";
    }

    const overrideBadge = row.querySelector('[data-role="override-badge"]');
    const overrideBar = row.querySelector('[data-role="override-bar"]');
    const catBadge = row.querySelector('[data-role="catastrophic-badge"]');
    if (d.override_active) {
      overrideBadge.classList.remove("hidden");
      row.classList.add("has-override");
      const remaining = Number(d.override_remaining_seconds);
      const safeRemaining = Number.isFinite(remaining) && remaining > 0 ? remaining : 0;
      // Stash the wall-clock expiry on the badge so the 1s ticker can update
      // the countdown + bar in between 5s polls without re-fetching /state.
      const expiryAt = Date.now() + safeRemaining * 1000;
      const total = Math.max(Number(overrideBadge.dataset.total || 0), safeRemaining, 30);
      overrideBadge.dataset.expiryAt = String(expiryAt);
      overrideBadge.dataset.total = String(total);
      overrideBadge.textContent = safeRemaining > 0 ? `Override ${safeRemaining}s` : "Override";
      if (overrideBar) {
        overrideBar.dataset.expiryAt = String(expiryAt);
        overrideBar.dataset.total = String(total);
        overrideBar.style.transform = `scaleX(${Math.max(0, Math.min(1, safeRemaining / total))})`;
      }
    } else {
      overrideBadge.classList.add("hidden");
      row.classList.remove("has-override");
      delete overrideBadge.dataset.expiryAt;
      delete overrideBadge.dataset.total;
      if (overrideBar) {
        overrideBar.style.transform = "scaleX(0)";
        delete overrideBar.dataset.expiryAt;
        delete overrideBar.dataset.total;
      }
    }
    if (d.catastrophic) {
      catBadge.classList.remove("hidden");
      row.classList.add("catastrophic");
    } else {
      catBadge.classList.add("hidden");
      row.classList.remove("catastrophic");
    }

    if (dropped) {
      row.classList.remove("flash"); void row.offsetWidth; row.classList.add("flash");
    }
  });
  reorderRows(ranked);
  state.lbLastRanked = ranked;
  applyLeaderboardPagination(ranked);
}

// Hide rows outside the current page and update the prev/next/indicator UI.
// Runs on every paint (silent) and on explicit page change (setLeaderboardPage
// adds a one-shot `page-turn` class so only user navigation animates).
function applyLeaderboardPagination(ranked) {
  const pageSize = state.lbPageSize;
  const totalPages = Math.max(1, Math.ceil(ranked.length / pageSize));
  if (state.lbPage > totalPages) state.lbPage = totalPages;
  if (state.lbPage < 1) state.lbPage = 1;
  const start = (state.lbPage - 1) * pageSize;
  const end = Math.min(start + pageSize, ranked.length);

  let visibleIdx = 0;
  ranked.forEach((d, i) => {
    const row = state.rowEls[d.district_id];
    if (!row) return;
    const inPage = i >= start && i < end;
    row.classList.toggle("off-page", !inPage);
    if (inPage) {
      row.style.setProperty("--stagger", `${visibleIdx * 22}ms`);
      visibleIdx++;
    }
  });

  const ctrl = document.getElementById("lb-pagination");
  if (!ctrl) return;
  if (ranked.length <= pageSize) {
    ctrl.classList.add("hidden");
    return;
  }
  ctrl.classList.remove("hidden");
  document.getElementById("lb-prev").disabled = state.lbPage <= 1;
  document.getElementById("lb-next").disabled = state.lbPage >= totalPages;
  document.getElementById("lb-page").textContent = `Page ${state.lbPage} of ${totalPages}`;
  document.getElementById("lb-range").textContent = ranked.length
    ? `showing ${start + 1}–${end} of ${ranked.length}`
    : "";
}

// A district with an active override carries Redis-TTL seconds remaining. The
// 5s /state poll cadence means a 30s spotlight can visually linger up to 35s;
// schedule a one-shot poll right as the TTL fires so eviction shows up at 30s.
function scheduleOverrideExpiryPolls(districts) {
  const live = new Set();
  for (const d of districts) {
    if (!d.override_active) continue;
    const remaining = Number(d.override_remaining_seconds);
    if (!Number.isFinite(remaining) || remaining <= 0) continue;
    live.add(d.district_id);
    if (state.overrideTimers[d.district_id]) continue;
    // +400ms so the backend has a beat to expire the Redis key before we poll.
    state.overrideTimers[d.district_id] = setTimeout(() => {
      delete state.overrideTimers[d.district_id];
      pollState();
    }, remaining * 1000 + 400);
  }
  for (const did of Object.keys(state.overrideTimers)) {
    if (!live.has(did)) {
      clearTimeout(state.overrideTimers[did]);
      delete state.overrideTimers[did];
    }
  }
}

// Per-second visual decay for active-override rows. /state polls every 5s,
// so without this the countdown badge would tick in jerky 5-second jumps;
// here we recompute remaining-seconds from the stashed wall-clock expiry
// and scale the depleting bar smoothly under the row.
function tickOverrideCountdowns() {
  const now = Date.now();
  document.querySelectorAll('[data-role="override-badge"]').forEach((badge) => {
    if (badge.classList.contains("hidden")) return;
    const expiryAt = Number(badge.dataset.expiryAt);
    const total = Number(badge.dataset.total) || 30;
    if (!Number.isFinite(expiryAt)) return;
    const remainingMs = Math.max(0, expiryAt - now);
    const remainingSec = Math.ceil(remainingMs / 1000);
    badge.textContent = remainingSec > 0 ? `Override ${remainingSec}s` : "Override";
    const row = badge.closest(".district-row");
    const bar = row && row.querySelector('[data-role="override-bar"]');
    if (bar) {
      const ratio = Math.max(0, Math.min(1, remainingMs / (total * 1000)));
      bar.style.transform = `scaleX(${ratio})`;
    }
  });
}

function setLeaderboardPage(p) {
  const ranked = state.lbLastRanked || [];
  const totalPages = Math.max(1, Math.ceil(ranked.length / state.lbPageSize));
  const next = Math.min(Math.max(1, p), totalPages);
  if (next === state.lbPage) return;
  state.lbPage = next;
  const list = document.getElementById("district-list");
  // One-shot animation class — stripped after the keyframe completes so the
  // next paint driven by pollState doesn't re-animate every 5s.
  list.classList.remove("page-turn");
  void list.offsetWidth;
  list.classList.add("page-turn");
  applyLeaderboardPagination(ranked);
  setTimeout(() => list.classList.remove("page-turn"), 420);
}

// ─── hero ──────────────────────────────────────────────────────────────────
function paintHero(districts) {
  const scores = districts.map(d => d.score).filter(Number.isFinite);
  if (!scores.length) {
    document.getElementById("hero-score").textContent = "--";
    document.getElementById("hero-delta").textContent = "Δ —";
    return;
  }
  const avgRaw = scores.reduce((a, b) => a + b, 0) / scores.length;
  const avg = round1(avgRaw);                       // the value we will DISPLAY

  // Delta is computed STRICTLY from the last value we displayed to the user,
  // so screen-math always checks out: shown_prev + delta == shown_now (at 1 dp).
  const prevAvg = state.prevAvg;
  const d = fmtDelta(prevAvg, avg);
  state.prevAvg = avg;
  document.getElementById("hero-score").textContent = avg.toFixed(1);
  document.getElementById("hero-delta").textContent = `Δ ${d.text}`;

  const trend = prevAvg == null ? 0 : avg - prevAvg;
  let status = "stable";
  if (trend < -1.5) status = "degrading";
  else if (trend > 1.5) status = "improving";
  const pill = document.getElementById("hero-status-text");
  pill.textContent = status;
  pill.classList.remove("degrading", "improving");
  if (status !== "stable") pill.classList.add(status);

  pushHistory(state.aggHistory, avg);

  // override summary — show any active override
  const active = districts.filter(x => x.override_active);
  const overrideEl = document.getElementById("hero-override");
  if (active.length) {
    const names = active.slice(0, 2).map(x => x.district_id).join(", ");
    const extra = active.length > 2 ? ` +${active.length - 2}` : "";
    overrideEl.textContent = `override active · ${names}${extra}`;
  } else {
    overrideEl.textContent = "no override active";
  }

  // sparkline
  const svg = document.getElementById("hero-sparkline");
  const w = 600, h = 100, pad = 4;
  const pts = sparkPath(state.aggHistory, w, h, pad);
  const series = state.aggHistory;
  const min = Math.min(...series), max = Math.max(...series);
  const range = max - min || 1;
  const refY = pad + (1 - (80 - min) / range) * (h - pad * 2);
  svg.innerHTML = `
    <line class="ref" x1="0" x2="${w}" y1="${refY.toFixed(1)}" y2="${refY.toFixed(1)}"/>
    <polyline points="${pts}"/>`;
}

// ─── /info → detailed distribution panel ──────────────────────────────────
function fmt(v, d = 1) {
  return v == null || !Number.isFinite(v) ? "—" : Number(v).toFixed(d);
}

function paintInfo(info) {
  refreshThemeCache();  // keep SVG strokes in sync with the current edition
  const has = info && (info.count ?? 0) > 0;

  // ── Headline ──
  const head = document.getElementById("dist-headline");
  if (has) {
    const n = info.count;
    head.innerHTML = `
      <span class="dist-n">${n}</span>
      <span class="dist-n-lbl">district${n === 1 ? "" : "s"}</span>
      <span class="dist-head-sep">·</span>
      <span>mean <b>${fmt(info.mean, 1)}</b></span>
      <span class="dist-head-sep">·</span>
      <span>σ <b>${fmt(info.std_dev, 2)}</b></span>
      <span class="dist-head-sep">·</span>
      <span>range <b>${fmt(info.min, 0)}–${fmt(info.max, 0)}</b></span>`;
  } else {
    head.innerHTML = `<span class="dist-empty">no scores yet — pipeline populating…</span>`;
  }

  // ── Histogram ──
  const svg = document.getElementById("histogram");
  const yMaxEl = document.getElementById("dist-y-max");
  const bins = (info && info.histogram) || [];
  if (!bins.length) {
    svg.innerHTML = "";
    yMaxEl.textContent = "—";
  } else {
    const maxCount = Math.max(...bins.map(b => b.count), 1);
    yMaxEl.textContent = maxCount;
    const w = 320, h = 160, gap = 2, padB = 6;
    const bw = (w - gap * (bins.length - 1)) / bins.length;
    const mean = info.mean, median = info.median;
    const xFromScore = s => {
      const lo = bins[0].bin_start;
      const hi = bins[bins.length - 1].bin_end;
      return ((s - lo) / (hi - lo || 1)) * w;
    };
    refreshThemeCache();
    const C = THEME_COLOR_CACHE;
    const ticks = [0.25, 0.5, 0.75].map(f =>
      `<line x1="0" x2="${w}" y1="${(h - padB) * (1 - f)}" y2="${(h - padB) * (1 - f)}"
             stroke="${C.rule}" stroke-dasharray="2,3" stroke-width="1"/>`
    ).join("");
    const bars = bins.map((b, i) => {
      const bh = (b.count / maxCount) * (h - padB - 2);
      const x = i * (bw + gap);
      const y = h - bh - padB;
      const mid = (b.bin_start + b.bin_end) / 2;
      return `<rect x="${x}" y="${y}" width="${bw}" height="${bh}" rx="1" fill="${healthColor(mid)}">
        <title>${b.bin_start.toFixed(1)}–${b.bin_end.toFixed(1)} · ${b.count}</title></rect>`;
    }).join("");
    const meanMarker = Number.isFinite(mean) ? `
      <line x1="${xFromScore(mean)}" x2="${xFromScore(mean)}" y1="2" y2="${h - padB}"
            stroke="${C.ink}" stroke-width="1.5" stroke-dasharray="3,2" opacity=".7"/>
      <text x="${xFromScore(mean) + 3}" y="12" font-family="JetBrains Mono" font-size="9"
            fill="${C.ink}">μ ${fmt(mean, 1)}</text>` : "";
    const medianMarker = Number.isFinite(median) ? `
      <line x1="${xFromScore(median)}" x2="${xFromScore(median)}" y1="2" y2="${h - padB}"
            stroke="${C.ink3}" stroke-width="1"/>` : "";
    svg.innerHTML = ticks + bars + medianMarker + meanMarker;
  }

  // ── KPIs ──
  const kpis = [
    ["mean",     fmt(info?.mean, 1)],
    ["median",   fmt(info?.median, 1)],
    ["std dev",  fmt(info?.std_dev, 2)],
    ["iqr",      fmt(info?.iqr, 2)],
    ["skewness", fmt(info?.skewness, 2)],
    ["p10 / p90", `${fmt(info?.p10, 1)} / ${fmt(info?.p90, 1)}`],
  ];
  document.getElementById("dist-kpis").innerHTML = kpis.map(
    ([k, v]) => `<div class="kpi"><span class="kpi-lbl">${escapeHtml(k)}</span><span class="kpi-val">${escapeHtml(String(v))}</span></div>`
  ).join("");

  // ── Box plot (min p10 q1 median q3 p90 max) ──
  const box = document.getElementById("boxplot");
  const legend = document.getElementById("boxplot-legend");
  if (has && info.min != null && info.max != null) {
    const w = 320, h = 54, padX = 10, padY = 8;
    const lo = 0, hi = 100;
    const xAt = s => padX + ((s - lo) / (hi - lo)) * (w - padX * 2);
    const yMid = h / 2;
    const boxTop = yMid - 9, boxH = 18;
    const [xMin, xP10, xQ1, xMed, xQ3, xP90, xMax] =
      [info.min, info.p10, info.q1, info.median, info.q3, info.p90, info.max].map(xAt);
    const CB = THEME_COLOR_CACHE;
    box.innerHTML = `
      <line x1="${padX}" x2="${w - padX}" y1="${yMid}" y2="${yMid}" stroke="${CB.rule}" stroke-width="1"/>
      <line x1="${xMin}" x2="${xMax}" y1="${yMid}" y2="${yMid}" stroke="${CB.ink2}" stroke-width="1.5"/>
      <line x1="${xMin}" x2="${xMin}" y1="${yMid - 5}" y2="${yMid + 5}" stroke="${CB.ink2}" stroke-width="1.5"/>
      <line x1="${xMax}" x2="${xMax}" y1="${yMid - 5}" y2="${yMid + 5}" stroke="${CB.ink2}" stroke-width="1.5"/>
      <line x1="${xP10}" x2="${xP90}" y1="${yMid}" y2="${yMid}" stroke="${CB.ink}" stroke-width="2"/>
      <rect x="${xQ1}" y="${boxTop}" width="${Math.max(1, xQ3 - xQ1)}" height="${boxH}"
            fill="${healthColor(info.median)}" fill-opacity=".35" stroke="${CB.ink}" stroke-width="1"/>
      <line x1="${xMed}" x2="${xMed}" y1="${boxTop}" y2="${boxTop + boxH}" stroke="${CB.ink}" stroke-width="2"/>`;
    legend.innerHTML = [
      ["min", info.min], ["p10", info.p10], ["q1", info.q1],
      ["median", info.median], ["q3", info.q3], ["p90", info.p90], ["max", info.max],
    ].map(([k, v]) => `<span><i>${k}</i><b>${fmt(v, 1)}</b></span>`).join("");
  } else {
    box.innerHTML = ""; legend.innerHTML = "";
  }

  // ── Health bands (from live state for real-time counts) ──
  const scores = state.districts.map(d => d.score).filter(Number.isFinite);
  const CC = THEME_COLOR_CACHE;
  const bands = [
    { key: "critical", label: "Critical",     lo: 0,   hi: 40,  color: CC.bands?.critical || healthColor(20) },
    { key: "watch",    label: "Watch",        lo: 40,  hi: 70,  color: CC.bands?.watch    || healthColor(55) },
    { key: "healthy",  label: "Healthy",      lo: 70,  hi: 100, color: CC.bands?.healthy  || healthColor(85) },
  ];
  const total = scores.length || 1;
  const counts = bands.map(b => scores.filter(s => s >= b.lo && s < (b.hi === 100 ? 101 : b.hi)).length);
  document.getElementById("dist-bands").innerHTML = bands.map((b, i) => {
    const n = counts[i];
    const pct = (n / total) * 100;
    return `<div class="band">
      <div class="band-head"><span class="band-dot" style="background:${b.color}"></span>
        <span class="band-lbl">${b.label}</span>
        <span class="band-range">${b.lo}–${b.hi}</span>
        <span class="band-n">${n}</span>
      </div>
      <div class="band-track"><span style="width:${pct}%;background:${b.color}"></span></div>
    </div>`;
  }).join("");

  // ── Interpretation line ──
  const interp = document.getElementById("dist-interp");
  if (has) {
    const sk = info.skewness;
    let skText = "symmetric";
    if (Number.isFinite(sk)) {
      if (sk > 0.5) skText = "right-skewed · outliers above";
      else if (sk < -0.5) skText = "left-skewed · outliers below";
      else if (sk > 0.1) skText = "slight right tilt";
      else if (sk < -0.1) skText = "slight left tilt";
    }
    const iqrText = Number.isFinite(info.iqr)
      ? (info.iqr < 5 ? "tight spread (IQR &lt; 5)" :
         info.iqr > 20 ? "wide spread (IQR &gt; 20)" : "moderate spread")
      : "—";
    interp.innerHTML = `shape: <b>${skText}</b> · <b>${iqrText}</b>`;
  } else {
    interp.innerHTML = "";
  }
}

// ─── /performance → latency table + pipeline stats ─────────────────────────
function paintPerf(perf) {
  const tbody = document.querySelector("#perf-table tbody");
  const byEndpoint = perf.by_endpoint || {};
  const rows = Object.entries(byEndpoint)
    .sort(([, a], [, b]) => b - a)
    .slice(0, 12)
    .map(([route, avg]) => `<tr>
      <td>${escapeHtml(route)}</td>
      <td>${Number(avg).toFixed(2)}</td>
    </tr>`).join("");
  tbody.innerHTML = rows || `<tr><td colspan="2" style="color:var(--ink-3);text-align:center;padding:1rem 0">no samples yet</td></tr>`;

  const foot = document.getElementById("perf-foot");
  const overall = perf.overall_avg_ms;
  const parts = [
    `overall <b>${overall != null ? Number(overall).toFixed(2) + " ms" : "—"}</b>`,
    `samples <b>${perf.sample_count ?? 0}</b>`,
    `mode <b>${escapeHtml(perf.mode || "—")}</b>`,
  ];
  foot.innerHTML = parts.join(" · ");

  const pipeEl = document.getElementById("pipe-stats");
  const pipe = perf.pipeline;
  if (pipe && typeof pipe === "object" && Object.keys(pipe).length) {
    pipeEl.classList.remove("hidden");
    pipeEl.innerHTML = Object.entries(pipe).map(([k, v]) => {
      const disp = typeof v === "number"
        ? (Number.isInteger(v) ? v.toLocaleString() : Number(v).toFixed(3))
        : typeof v === "boolean" ? (v ? "true" : "false")
        : typeof v === "object" ? JSON.stringify(v) : String(v);
      return `<span class="pk">${escapeHtml(k)}</span><span class="pv">${escapeHtml(disp)}</span>`;
    }).join("");
  } else {
    pipeEl.classList.add("hidden");
    pipeEl.innerHTML = "";
  }

  // Pipeline control UI: reflect processing_enabled on the toggle + status pill.
  const enabled = pipe && pipe.processing_enabled !== false;
  const toggleBtn = document.getElementById("pipe-toggle");
  const stateEl = document.getElementById("pipe-state");
  if (toggleBtn) {
    toggleBtn.textContent = enabled ? "Pause processing" : "Start processing";
    toggleBtn.setAttribute("aria-pressed", enabled ? "true" : "false");
  }
  if (stateEl) {
    stateEl.textContent = enabled ? "running" : "paused";
    stateEl.className = "pipe-state " + (enabled ? "running" : "paused");
  }
}

// ─── /history → event feed ────────────────────────────────────────────────
function classifyEvent(e) {
  const meta = e.meta || {};
  if (meta.catastrophic === true) return "catastrophic";
  if (e.source === "manual")      return "manual";
  return e.source || "pipeline";
}

function matchesKind(e) {
  const f = state.feedFilter;
  if (f === "all") return true;
  const kind = classifyEvent(e);
  if (f === "manual")        return kind === "manual" || kind === "catastrophic";
  if (f === "pipeline")      return kind !== "manual" && kind !== "catastrophic";
  if (f === "catastrophic")  return kind === "catastrophic";
  return true;
}

function matchesSearch(e) {
  if (!state.feedSearch) return true;
  const q = state.feedSearch.toLowerCase();
  const hay = `${e.district_id || ""} ${e.user || ""}`.toLowerCase();
  return hay.includes(q);
}

function matchesDistrict(e) {
  if (state.feedDistricts.size === 0) return true;
  const d = e.district_id || e.user || "";
  return state.feedDistricts.has(d);
}

function matchesScore(e) {
  const s = Number(e.score);
  if (!Number.isFinite(s)) return true;  // don't hide blank-score events on numeric filter
  if (state.feedScoreMin != null && s < state.feedScoreMin) return false;
  if (state.feedScoreMax != null && s > state.feedScoreMax) return false;
  return true;
}

function matchesTime(e) {
  const ts = parseSubmittedAt(e.submitted_at);
  if (!Number.isFinite(ts)) return true;
  if (state.feedCustomTime) {
    if (state.feedFromTs != null && ts < state.feedFromTs) return false;
    if (state.feedToTs   != null && ts > state.feedToTs)   return false;
    return true;
  }
  if (!state.feedWindowSec) return true;
  const age = (Date.now() - ts) / 1000;
  return age <= state.feedWindowSec;
}

function sortEvents(arr) {
  const s = state.feedSort;
  const byTime = (a, b) => (parseSubmittedAt(b.submitted_at) || 0) - (parseSubmittedAt(a.submitted_at) || 0);
  if (s === "newest") return arr.sort(byTime);
  if (s === "oldest") return arr.sort((a, b) => -byTime(a, b));
  if (s === "highest") return arr.sort((a, b) => (Number(b.score) || 0) - (Number(a.score) || 0));
  if (s === "lowest")  return arr.sort((a, b) => (Number(a.score) || 0) - (Number(b.score) || 0));
  return arr;
}

function paintFeed() {
  const feedEl = document.getElementById("feed-list");
  const total = state.latestEvents.length;

  const filtered = state.latestEvents.filter(e =>
    matchesKind(e) && matchesSearch(e) && matchesDistrict(e) && matchesScore(e) && matchesTime(e)
  );
  const rows = sortEvents(filtered.slice()).slice(0, 60);

  paintFeedSummary(filtered.length, total);
  paintFeedCardSub(total);

  if (!rows.length) {
    feedEl.innerHTML = `<li class="empty">No events match these filters</li>`;
    return;
  }

  feedEl.innerHTML = rows.map(e => {
    const tms = parseSubmittedAt(e.submitted_at);
    const t = Number.isFinite(tms)
      ? new Date(tms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false })
      : String(e.submitted_at || "");
    const score = e.score != null ? Number(e.score).toFixed(1) : "—";
    const kind = classifyEvent(e);
    const district = e.district_id || e.user || "—";
    return `<li>
      <span class="ts">${escapeHtml(t)}</span>
      <span class="tag ${escapeHtml(kind)}">${escapeHtml(kind)}</span>
      <span class="what">${escapeHtml(district)}</span>
      <span class="score">${score}</span>
    </li>`;
  }).join("");
}

// ─── feed summary (count + active-filter chips + reset) ──────────────────
function fmtTimeLabel(ts) {
  if (!Number.isFinite(ts)) return "—";
  const d = new Date(ts);
  const p = n => String(n).padStart(2, "0");
  return `${d.getMonth() + 1}/${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

function fmtWindowLabel(sec) {
  if (!sec) return "all time";
  if (sec < 3600)   return `last ${Math.round(sec / 60)}m`;
  if (sec < 86400)  return `last ${Math.round(sec / 3600)}h`;
  return `last ${Math.round(sec / 86400)}d`;
}

function isFeedFiltered() {
  return state.feedFilter !== "all"
      || state.feedSearch !== ""
      || state.feedDistricts.size > 0
      || state.feedScoreMin != null
      || state.feedScoreMax != null
      || state.feedCustomTime
      || state.feedSort !== "newest"
      || state.feedWindowSec !== 3600;
}

function activeFilterChips() {
  const chips = [];
  if (state.feedFilter !== "all") {
    chips.push({ key: "kind", label: "kind", value: state.feedFilter });
  }
  if (state.feedSearch) {
    chips.push({ key: "search", label: "search", value: `"${state.feedSearch}"` });
  }
  for (const d of state.feedDistricts) {
    chips.push({ key: `district:${d}`, label: "district", value: d });
  }
  if (state.feedScoreMin != null || state.feedScoreMax != null) {
    const lo = state.feedScoreMin != null ? state.feedScoreMin : 0;
    const hi = state.feedScoreMax != null ? state.feedScoreMax : 100;
    chips.push({ key: "score", label: "score", value: `${lo} – ${hi}` });
  }
  if (state.feedCustomTime) {
    const lo = state.feedFromTs != null ? fmtTimeLabel(state.feedFromTs) : "…";
    const hi = state.feedToTs   != null ? fmtTimeLabel(state.feedToTs)   : "now";
    chips.push({ key: "time", label: "time", value: `${lo} → ${hi}` });
  } else if (state.feedWindowSec !== 3600) {
    chips.push({ key: "window", label: "window", value: fmtWindowLabel(state.feedWindowSec) });
  }
  if (state.feedSort !== "newest") {
    chips.push({ key: "sort", label: "sort", value: state.feedSort });
  }
  return chips;
}

function paintFeedCardSub(total) {
  const el = document.getElementById("feed-card-sub");
  if (!el) return;
  let scope;
  if (state.feedCustomTime) {
    const lo = state.feedFromTs != null ? fmtTimeLabel(state.feedFromTs) : "…";
    const hi = state.feedToTs   != null ? fmtTimeLabel(state.feedToTs)   : "now";
    scope = `${lo} → ${hi}`;
  } else if (state.feedDistricts.size === 1) {
    scope = `${[...state.feedDistricts][0]} · ${fmtWindowLabel(state.feedWindowSec)}`;
  } else {
    scope = fmtWindowLabel(state.feedWindowSec);
  }
  const cap = state.feedHitCap ? " · cap reached · narrow the range to see older events" : "";
  el.textContent = `GET /history · ${scope} · ${total} events${cap}`;
}

function paintFeedSummary(matched, total) {
  const el       = document.getElementById("feed-summary");
  const countEl  = document.getElementById("feed-count");
  const chipsEl  = document.getElementById("feed-active");
  const filtered = isFeedFiltered();

  if (!filtered) { el.hidden = true; return; }
  el.hidden = false;
  const capped = state.feedHitCap ? " <i class='feed-cap'>· cap</i>" : "";
  countEl.innerHTML = `Showing <b>${matched}</b> of <b>${total}</b> events${capped}`;

  const chips = activeFilterChips();
  chipsEl.innerHTML = chips.map(c => `
    <span class="feed-active-chip" data-key="${escapeHtml(c.key)}">
      <i>${escapeHtml(c.label)}:</i><b>${escapeHtml(c.value)}</b>
      <button type="button" aria-label="Remove filter">×</button>
    </span>
  `).join("");
}

function removeActiveFilter(key) {
  if (key === "kind")         setFeedKind("all");
  else if (key === "search")  setFeedSearch("");
  else if (key === "score")   { state.feedScoreMin = null; state.feedScoreMax = null; syncFeedInputs(); paintFeed(); }
  else if (key === "sort")    { state.feedSort = "newest"; syncFeedInputs(); paintFeed(); }
  // time / window / district toggles change what we ask the server for — refetch:
  else if (key === "time")    { state.feedCustomTime = false; state.feedFromTs = null; state.feedToTs = null; syncFeedInputs(); pollHistory(); }
  else if (key === "window")  { state.feedWindowSec = 3600; syncFeedInputs(); pollHistory(); }
  else if (key.startsWith("district:")) {
    const d = key.slice("district:".length);
    const prev = state.feedDistricts.size;
    state.feedDistricts.delete(d);
    renderDistrictChips();
    const size = state.feedDistricts.size;
    const crossedServerBoundary = prev !== size && (prev <= 1 || size <= 1);
    if (crossedServerBoundary) pollHistory();
    else paintFeed();
  }
}

function setFeedKind(kind) {
  state.feedFilter = kind;
  document.querySelectorAll(".tab").forEach(b => {
    b.classList.toggle("active", b.dataset.filter === kind);
  });
  paintFeed();
}

function setFeedSearch(q) {
  state.feedSearch = q;
  const input = document.getElementById("feed-search");
  const clear = document.getElementById("feed-search-clear");
  if (input.value !== q) input.value = q;
  if (clear) clear.hidden = !q;
  paintFeed();
}

function resetFeedFilters() {
  state.feedFilter      = "all";
  state.feedSearch      = "";
  state.feedDistricts   = new Set();
  state.feedScoreMin    = null;
  state.feedScoreMax    = null;
  state.feedCustomTime  = false;
  state.feedFromTs      = null;
  state.feedToTs        = null;
  state.feedSort        = "newest";
  state.feedWindowSec   = 3600;
  syncFeedInputs();
  renderDistrictChips();
  document.querySelectorAll(".tab").forEach(b => {
    b.classList.toggle("active", b.dataset.filter === "all");
  });
  // Reset may have cleared a server-side filter (custom range / single district) →
  // refetch so the feed is sourced from the default 1h window.
  pollHistory();
}

function renderDistrictChips() {
  const host = document.getElementById("feed-district-chips");
  if (!host) return;
  host.innerHTML = [...state.feedDistricts].map(d => `
    <span class="adv-chip" data-district="${escapeHtml(d)}">
      ${escapeHtml(d)}
      <button type="button" aria-label="Remove ${escapeHtml(d)}">×</button>
    </span>
  `).join("");
}

function toDatetimeLocalValue(ts) {
  if (ts == null || !Number.isFinite(ts)) return "";
  const d = new Date(ts);
  const p = n => String(n).padStart(2, "0");
  // Local time in the format required by <input type="datetime-local">
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}`;
}

function syncFeedInputs() {
  const win = document.getElementById("feed-window");
  const sort = document.getElementById("feed-sort");
  const smin = document.getElementById("feed-score-min");
  const smax = document.getElementById("feed-score-max");
  const from = document.getElementById("feed-from");
  const to   = document.getElementById("feed-to");
  const customRow = document.getElementById("feed-custom-time");

  if (win) {
    win.value = state.feedCustomTime ? "custom" : String(state.feedWindowSec);
  }
  if (sort) sort.value = state.feedSort;
  if (smin) smin.value = state.feedScoreMin != null ? String(state.feedScoreMin) : "";
  if (smax) smax.value = state.feedScoreMax != null ? String(state.feedScoreMax) : "";
  if (customRow) customRow.hidden = !state.feedCustomTime;
  if (from) from.value = toDatetimeLocalValue(state.feedFromTs);
  if (to)   to.value   = toDatetimeLocalValue(state.feedToTs);
}

// ─── datalist for district inputs ─────────────────────────────────────────
function refreshDatalist(districts) {
  const dl = document.getElementById("districts-dl");
  const seen = new Set();
  const opts = [];
  for (const d of districts) {
    const name = d.district_id;
    if (name && !seen.has(name)) { seen.add(name); opts.push(name); }
  }
  dl.innerHTML = opts.map(n => `<option value="${escapeHtml(n)}"></option>`).join("");
}

// ─── toasts + hints ────────────────────────────────────────────────────────
let toastTimer;
function toast(msg, kind = "") {
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.classList.remove("err", "ok");
  if (kind) el.classList.add(kind);
  el.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.remove("show"), 3400);
}

function setHint(id, msg, kind = "") {
  const el = document.getElementById(id);
  el.textContent = msg;
  el.classList.remove("ok", "err");
  if (kind) el.classList.add(kind);
  if (msg) setTimeout(() => {
    if (el.textContent === msg) { el.textContent = ""; el.classList.remove("ok", "err"); }
  }, 4500);
}

function flashRow(district) {
  const row = state.rowEls[district];
  if (!row) return;
  // If the target sits on another page, flip to that page first so the
  // highlight + scroll lands on a visible row instead of a `display:none` one.
  if (row.classList.contains("off-page")) {
    const ranked = state.lbLastRanked || [];
    const idx = ranked.findIndex(r => r.district_id === district);
    if (idx >= 0) {
      const page = Math.floor(idx / state.lbPageSize) + 1;
      setLeaderboardPage(page);
    }
  }
  row.classList.remove("highlighted"); void row.offsetWidth;
  row.classList.add("highlighted");
  row.scrollIntoView({ behavior: "smooth", block: "center" });
  setTimeout(() => row.classList.remove("highlighted"), 6000);
}

// ─── actions ──────────────────────────────────────────────────────────────
async function submitScore(district, score, catastrophic) {
  return fetchJSON("/add", {
    method: "POST",
    body: JSON.stringify({ district, user: district, score, catastrophic }),
  });
}

async function submitSensor(district, sensor, value) {
  return fetchJSON("/sensor", {
    method: "POST",
    body: JSON.stringify({ district, sensor, value }),
  });
}

async function removeDistrict(district, fromRow = false) {
  if (!district) return;
  if (!confirm(`Remove "${district}" from the leaderboard?`)) return;
  try {
    await fetchJSON("/remove", {
      method: "POST",
      body: JSON.stringify({ district, id: district }),
    });
    toast(`Removed ${district}`, "ok");
    if (fromRow) setHint("remove-hint", "", "");
    else setHint("remove-hint", `removed ${district}`, "ok");
    await Promise.allSettled([pollState(), pollHistory()]);
  } catch (e) {
    toast(`Remove failed: ${e.message}`, "err");
    if (!fromRow) setHint("remove-hint", e.message, "err");
  }
}

const QUICK_PUSH = {
  "broad-ripple-healthy":  { district: "Broad Ripple",    score: 88, catastrophic: false },
  "fountain-square-watch": { district: "Fountain Square", score: 66, catastrophic: false },
  "haughville-warn":       { district: "Haughville",      score: 42, catastrophic: false },
  "downtown-crit":         { district: "Downtown",        score: 18, catastrophic: true  },
};

const SENSOR_KEYS = ["traffic", "aqi", "power", "water", "noise"];

function wireForms() {
  document.getElementById("submit-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const f = ev.target;
    const district = f.district.value.trim();
    if (!district) {
      setHint("submit-hint", "district required", "err");
      return;
    }

    const readings = SENSOR_KEYS
      .map(k => ({ k, raw: f[k].value.trim() }))
      .filter(x => x.raw !== "")
      .map(x => ({ k: x.k, v: parseFloat(x.raw) }))
      .filter(x => Number.isFinite(x.v));

    const scoreRaw = f.score ? f.score.value.trim() : "";
    const hasScore = scoreRaw !== "";
    const score = hasScore ? parseFloat(scoreRaw) : null;
    const catastrophic = f.catastrophic ? f.catastrophic.checked : false;

    if (!readings.length && !hasScore) {
      setHint("submit-hint", "fill at least one sensor or score", "err");
      return;
    }

    const btn = f.querySelector("button.primary");
    btn.disabled = true;
    try {
      const tasks = readings.map(r => submitSensor(district, r.k, r.v));
      if (hasScore) tasks.push(submitScore(district, score, catastrophic));
      const results = await Promise.allSettled(tasks);
      const failures = results.filter(r => r.status === "rejected");

      const parts = [];
      if (readings.length) parts.push(`${readings.length} sensor${readings.length > 1 ? "s" : ""}`);
      if (hasScore) parts.push(`score ${score.toFixed(1)}${catastrophic ? " cat" : ""}`);
      const summary = `${district} · ${parts.join(" · ")}`;

      if (failures.length) {
        const first = failures[0].reason?.message || "error";
        setHint("submit-hint", `${results.length - failures.length}/${results.length} sent — ${first}`, "err");
        toast(`Partial: ${summary} (${failures.length} failed)`, "err");
      } else {
        setHint("submit-hint", `pushed ${parts.join(" · ")}`, "ok");
        toast(summary, "ok");
        SENSOR_KEYS.forEach(k => { if (f[k]) f[k].value = ""; });
        if (hasScore && f.score) f.score.value = "";
      }
      await Promise.allSettled([pollState(), pollHistory(), pollInfo()]);
      flashRow(district);
    } catch (e) {
      setHint("submit-hint", e.message, "err");
      toast(`Submit failed: ${e.message}`, "err");
    } finally {
      btn.disabled = false;
    }
  });

  // Live "known vs new place" hint next to the district input. We don't need a
  // separate form — /add accepts any district and the 30s override + eviction
  // logic already handles "new places" returning to baseline after TTL.
  const submitForm = document.getElementById("submit-form");
  const districtInput = submitForm.querySelector('input[name="district"]');
  const districtHint = document.getElementById("submit-district-hint");
  const refreshDistrictHint = () => {
    if (!districtHint) return;
    const v = districtInput.value.trim();
    districtHint.classList.remove("hint-known", "hint-new");
    if (!v) { districtHint.textContent = ""; return; }
    const known = state.districts.some(d => d.district_id === v);
    if (known) {
      districtHint.textContent = "· on the board";
      districtHint.classList.add("hint-known");
    } else {
      districtHint.textContent = "· new place · 30s spotlight";
      districtHint.classList.add("hint-new");
    }
  };
  districtInput.addEventListener("input", refreshDistrictHint);
  // Re-evaluate after polls so the hint flips once a newly-added place joins the board.
  state._refreshDistrictHint = refreshDistrictHint;

  document.getElementById("remove-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const f = ev.target;
    const district = f.district.value.trim();
    if (!district) {
      setHint("remove-hint", "district required", "err");
      return;
    }
    await removeDistrict(district, false);
    f.district.value = "";
  });

  // quick-push chips (inside the remove card) — use /add with a preset
  document.querySelectorAll(".chip[data-preset]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const p = QUICK_PUSH[btn.dataset.preset];
      if (!p) return;
      btn.disabled = true;
      try {
        const res = await submitScore(p.district, p.score, p.catastrophic);
        const tag = p.catastrophic ? " [CATASTROPHIC]" : "";
        toast(`${p.district} → ${res.score.toFixed(1)}${tag}`, "ok");
        await Promise.allSettled([pollState(), pollHistory()]);
        flashRow(p.district);
      } catch (e) {
        toast(`Preset failed: ${e.message}`, "err");
      } finally {
        btn.disabled = false;
      }
    });
  });

  wireFeedFilters();
  wireLeaderboardPagination();
}

function wireLeaderboardPagination() {
  const prev = document.getElementById("lb-prev");
  const next = document.getElementById("lb-next");
  if (!prev || !next) return;
  prev.addEventListener("click", () => setLeaderboardPage(state.lbPage - 1));
  next.addEventListener("click", () => setLeaderboardPage(state.lbPage + 1));
  // Keyboard: ← / → when focus isn't in a form field.
  document.addEventListener("keydown", (e) => {
    const tag = (document.activeElement && document.activeElement.tagName) || "";
    if (/^(INPUT|TEXTAREA|SELECT)$/.test(tag)) return;
    if (e.key === "ArrowLeft")  setLeaderboardPage(state.lbPage - 1);
    if (e.key === "ArrowRight") setLeaderboardPage(state.lbPage + 1);
  });
}

// ─── feed filter wiring ───────────────────────────────────────────────────
function wireFeedFilters() {
  // kind pills
  document.querySelectorAll(".tab[data-filter]").forEach(btn => {
    btn.addEventListener("click", () => setFeedKind(btn.dataset.filter));
  });

  // time window dropdown — the "custom" option toggles the date-range row
  const winEl = document.getElementById("feed-window");
  winEl.addEventListener("change", (e) => {
    const v = e.target.value;
    if (v === "custom") {
      state.feedCustomTime = true;
      // Seed the range to the last hour if empty, so the pickers have something.
      if (state.feedFromTs == null) state.feedFromTs = Date.now() - 3600 * 1000;
      if (state.feedToTs   == null) state.feedToTs   = Date.now();
      // Also expand the advanced panel so the inputs are visible.
      setMoreOpen(true);
    } else {
      state.feedCustomTime = false;
      state.feedWindowSec  = parseInt(v, 10) || 0;
    }
    syncFeedInputs();
    // Time range is a server-side filter (to pull genuine older data, not
    // just re-slice the last 2000 events) → refetch, don't just repaint.
    pollHistory();
  });

  // search with debounce + inline clear
  const searchEl = document.getElementById("feed-search");
  const searchClearEl = document.getElementById("feed-search-clear");
  let searchTimer;
  searchEl.addEventListener("input", (e) => {
    clearTimeout(searchTimer);
    const v = e.target.value.trim();
    searchTimer = setTimeout(() => setFeedSearch(v), 120);
    if (searchClearEl) searchClearEl.hidden = !v;
  });
  if (searchClearEl) searchClearEl.addEventListener("click", () => setFeedSearch(""));

  // more-filters toggle
  const moreBtn  = document.getElementById("feed-more-toggle");
  const advPanel = document.getElementById("feed-advanced");
  moreBtn.addEventListener("click", () => setMoreOpen(advPanel.hasAttribute("hidden")));
  function setMoreOpen(open) {
    if (open) advPanel.removeAttribute("hidden");
    else advPanel.setAttribute("hidden", "");
    moreBtn.setAttribute("aria-expanded", open ? "true" : "false");
  }
  // expose to outer scope so the window-change handler can expand on "custom"
  wireFeedFilters._setMoreOpen = setMoreOpen;

  // districts multi-select: chip add on Enter / on datalist pick
  const addInput = document.getElementById("feed-district-add");
  const chipsHost = document.getElementById("feed-district-chips");
  // When exactly one district is selected, the request is server-scoped (the
  // API supports ?district=) — so we refetch. Multi-district is filtered
  // client-side, no refetch needed. This helper decides which to do.
  function afterDistrictChange(prevSize) {
    renderDistrictChips();
    const size = state.feedDistricts.size;
    const crossedServerBoundary = prevSize !== size && (prevSize <= 1 || size <= 1);
    if (crossedServerBoundary) pollHistory();
    else paintFeed();
  }
  function tryAddDistrict(raw) {
    const v = String(raw || "").trim();
    if (!v || state.feedDistricts.has(v)) return;
    const prev = state.feedDistricts.size;
    state.feedDistricts.add(v);
    addInput.value = "";
    afterDistrictChange(prev);
  }
  addInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") { e.preventDefault(); tryAddDistrict(addInput.value); }
  });
  // Picking from the datalist fires 'change' (not Enter).
  addInput.addEventListener("change", () => tryAddDistrict(addInput.value));
  chipsHost.addEventListener("click", (e) => {
    const btn = e.target.closest("button");
    if (!btn) return;
    const chip = btn.closest(".adv-chip");
    const d = chip && chip.dataset.district;
    if (!d) return;
    const prev = state.feedDistricts.size;
    state.feedDistricts.delete(d);
    afterDistrictChange(prev);
  });

  // score range
  const sMin = document.getElementById("feed-score-min");
  const sMax = document.getElementById("feed-score-max");
  const readNum = (el) => {
    const v = el.value.trim();
    if (v === "") return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };
  sMin.addEventListener("input", () => { state.feedScoreMin = readNum(sMin); paintFeed(); });
  sMax.addEventListener("input", () => { state.feedScoreMax = readNum(sMax); paintFeed(); });

  // custom time range · server-side filter → debounce then refetch
  const fromEl = document.getElementById("feed-from");
  const toEl   = document.getElementById("feed-to");
  const readDt = (el) => {
    const v = el.value;
    if (!v) return null;
    const t = new Date(v).getTime();
    return Number.isFinite(t) ? t : null;
  };
  let timeTimer;
  const onTimeChange = () => {
    state.feedFromTs = readDt(fromEl);
    state.feedToTs   = readDt(toEl);
    // Typing into from/to implicitly enables custom-time mode so the range
    // actually filters — otherwise buildHistoryQuery/matchesTime fall back
    // to the window preset and ignore the datetime inputs entirely.
    const hasRange = state.feedFromTs != null || state.feedToTs != null;
    state.feedCustomTime = hasRange;
    const winEl2 = document.getElementById("feed-window");
    if (winEl2) winEl2.value = hasRange ? "custom" : String(state.feedWindowSec);
    clearTimeout(timeTimer);
    timeTimer = setTimeout(pollHistory, 250);
  };
  fromEl.addEventListener("input", onTimeChange);
  toEl.addEventListener  ("input", onTimeChange);

  // sort
  document.getElementById("feed-sort").addEventListener("change", (e) => {
    state.feedSort = e.target.value;
    paintFeed();
  });

  // active-filter chips (delegated)
  document.getElementById("feed-active").addEventListener("click", (e) => {
    const btn = e.target.closest("button");
    if (!btn) return;
    const chip = btn.closest(".feed-active-chip");
    if (chip && chip.dataset.key) removeActiveFilter(chip.dataset.key);
  });

  // reset
  document.getElementById("feed-reset").addEventListener("click", () => {
    resetFeedFilters();
    wireFeedFilters._setMoreOpen && wireFeedFilters._setMoreOpen(false);
  });
}

// ─── pollers ──────────────────────────────────────────────────────────────
function setOnline(online, modeNote) {
  state.online = online;
  const pill = document.getElementById("live-pill");
  pill.classList.toggle("offline", !online);
  pill.innerHTML = `<span class="live-dot"></span>${online ? "LIVE" : "OFFLINE"}`;
  if (modeNote) {
    document.getElementById("mode-label").textContent = `mode: ${modeNote}`;
  }
}

async function pollState() {
  // Serialize concurrent callers (interval + submit + remove + chips). Without
  // this guard, two overlapping fetches can push out-of-order avgs, making the
  // on-screen delta disagree with the value it was computed from.
  if (state.stateInFlight) return;
  state.stateInFlight = true;
  try {
    const data = await fetchJSON("/state");
    const districts = Array.isArray(data.districts) ? data.districts : [];
    state.districts = districts;
    state.mode = data.mode || "—";
    setOnline(true, state.mode);
    document.getElementById("district-count").textContent =
      `${districts.filter(d => d.score != null).length} entries`;
    paintHero(districts);
    paintLeaderboard(districts);
    refreshDatalist(districts);
    scheduleOverrideExpiryPolls(districts);
    if (typeof state._refreshDistrictHint === "function") state._refreshDistrictHint();
  } catch (e) {
    setOnline(false, "unreachable");
    console.warn("/state failed:", e.message);
  } finally {
    state.stateInFlight = false;
  }
}

async function pollInfo() {
  try {
    const info = await fetchJSON("/info");
    paintInfo(info);
  } catch (e) {
    console.warn("/info failed:", e.message);
  }
}

async function pollPerf() {
  try {
    const perf = await fetchJSON("/performance");
    paintPerf(perf);
  } catch (e) {
    console.warn("/performance failed:", e.message);
  }
}

// Build the base /history params from current filter state. The backend
// filters natively on `district`, `from`, `to` — we push those server-side
// so a narrow filter pulls genuine older data instead of re-slicing whatever
// happened to be in the last N events. Client-side-only filters (kind, free
// search, score range, multi-district, sort) are applied after the response.
function buildHistoryParams() {
  const p = new URLSearchParams();
  p.set("limit", String(HISTORY_LIMIT));
  if (state.feedCustomTime) {
    if (state.feedFromTs != null) p.set("from", new Date(state.feedFromTs).toISOString());
    if (state.feedToTs   != null) p.set("to",   new Date(state.feedToTs).toISOString());
  } else if (state.feedWindowSec && state.feedWindowSec >= 3600) {
    const from = new Date(Date.now() - state.feedWindowSec * 1000).toISOString();
    p.set("from", from);
  }
  if (state.feedDistricts.size === 1) {
    p.set("district", [...state.feedDistricts][0]);
  }
  return p;
}

// Floor of the effective time window (ms). Pagination stops walking further
// back once it crosses this boundary — no point fetching events we'd discard.
function effectiveFromMs() {
  if (state.feedCustomTime) return state.feedFromTs;
  if (state.feedWindowSec && state.feedWindowSec >= 3600) {
    return Date.now() - state.feedWindowSec * 1000;
  }
  return null;
}

// Walk /history backwards by seeding `to` with the oldest event from each
// page. Stops on: (a) a page smaller than HISTORY_LIMIT (natural end);
// (b) the effective `from` boundary; (c) MAX_PAGES safety cap.
// The API orders DESC, so items[length-1] is the oldest of that page.
async function fetchHistoryPaginated() {
  const base = buildHistoryParams();
  const fromBound = effectiveFromMs();
  const all = [];
  let toCursor = null;   // null on first page (use base params as-is)
  let pages = 0;
  let lastPage = [];

  for (pages = 0; pages < HISTORY_MAX_PAGES; pages++) {
    const p = new URLSearchParams(base);
    if (toCursor != null) p.set("to", new Date(toCursor).toISOString());
    const data = await fetchJSON(`/history?${p.toString()}`);
    lastPage = Array.isArray(data.items) ? data.items : [];
    if (!lastPage.length) break;
    all.push(...lastPage);
    if (lastPage.length < HISTORY_LIMIT) break;     // natural end (no more rows)

    const oldest = lastPage[lastPage.length - 1];
    const oldestMs = parseSubmittedAt(oldest.submitted_at);
    if (!Number.isFinite(oldestMs)) break;
    if (fromBound != null && oldestMs <= fromBound) break;

    // Seed next page. Subtract 1ms so the cursor boundary event isn't refetched.
    toCursor = oldestMs - 1;
  }

  // We "hit the cap" only when we exhausted MAX_PAGES AND the final page was
  // still full — meaning there are definitely more older events we didn't pull.
  const hitCap = pages >= HISTORY_MAX_PAGES && lastPage.length >= HISTORY_LIMIT;
  return { items: all, hitCap };
}

async function pollHistory() {
  try {
    const { items, hitCap } = await fetchHistoryPaginated();
    state.latestEvents = items;
    state.feedHitCap   = hitCap;
    paintFeed();
  } catch (e) {
    console.warn("/history failed:", e.message);
  }
}

// ─── theme edition (light / dark) ────────────────────────────────────────
// The pre-hydration script in <head> already applied the theme before first
// paint. Here we: mount the toggle, slide the rail under the active word,
// persist explicit choices, and re-run the SVG paints so histogram/boxplot
// stroke colours adopt the new edition.
function slideThemeRail(btn, theme) {
  const active = btn.querySelector(`.tt-opt[data-mode="${theme}"]`);
  const rail   = btn.querySelector(".tt-rail");
  if (!active || !rail) return;
  const btnRect = btn.getBoundingClientRect();
  const r = active.getBoundingClientRect();
  rail.style.left  = `${r.left - btnRect.left}px`;
  rail.style.width = `${r.width}px`;
}

function applyTheme(theme, { persist = false } = {}) {
  const next = theme === "dark" ? "dark" : "light";
  const prev = document.documentElement.dataset.theme || "light";
  document.documentElement.dataset.theme = next;

  const meta = document.getElementById("meta-theme-color");
  if (meta) meta.content = next === "dark" ? "#14130f" : "#f1ede4";

  const btn = document.getElementById("theme-toggle");
  if (btn) {
    btn.setAttribute("aria-checked", next === "dark" ? "true" : "false");
    btn.querySelectorAll(".tt-opt").forEach(el => {
      el.dataset.active = String(el.dataset.mode === next);
    });
    slideThemeRail(btn, next);
  }

  if (persist) {
    try { localStorage.setItem("cho-theme", next); } catch {}
  }

  if (prev !== next) {
    // Tokenise cached stroke/fill values then repaint SVG surfaces so they
    // adopt the new edition on the same frame the palette transitions.
    refreshThemeCache();
    if (state.districts && state.districts.length) paintLeaderboard(state.districts);
    pollInfo();
  }
}

function initTheme() {
  const saved = (() => { try { return localStorage.getItem("cho-theme"); } catch { return null; } })();
  const mq = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;
  const current = document.documentElement.dataset.theme
    || saved
    || (mq && mq.matches ? "dark" : "light");

  // Mount toggle state (rail + aria). applyTheme with persist=false so we
  // don't overwrite localStorage on first load.
  applyTheme(current, { persist: false });

  const btn = document.getElementById("theme-toggle");
  if (btn) {
    btn.addEventListener("click", () => {
      const now = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
      applyTheme(now, { persist: true });
    });
    // Keep the rail aligned as the masthead flexes.
    window.addEventListener("resize", () => {
      slideThemeRail(btn, document.documentElement.dataset.theme || "light");
    });
    // Fonts load async — recompute once they land so the rail isn't mis-sized.
    if (document.fonts && document.fonts.ready) {
      document.fonts.ready.then(() =>
        slideThemeRail(btn, document.documentElement.dataset.theme || "light"));
    }
  }

  // Follow system changes only when the user hasn't explicitly chosen.
  if (mq) {
    const handler = (e) => {
      let savedNow = null;
      try { savedNow = localStorage.getItem("cho-theme"); } catch {}
      if (!savedNow) applyTheme(e.matches ? "dark" : "light", { persist: false });
    };
    if (mq.addEventListener) mq.addEventListener("change", handler);
    else if (mq.addListener) mq.addListener(handler);
  }
}

// ─── pipeline control buttons ─────────────────────────────────────────────
function wirePipeControls() {
  const toggle = document.getElementById("pipe-toggle");
  const reset  = document.getElementById("pipe-reset");
  if (!toggle || !reset) return;

  const call = async (path, btn) => {
    btn.disabled = true;
    try {
      const res = await fetch(path, { method: "POST" });
      if (!res.ok) throw new Error(`${res.status}`);
    } catch (e) {
      console.warn(`${path} failed:`, e.message);
    } finally {
      btn.disabled = false;
      pollPerf();
    }
  };

  toggle.addEventListener("click", () => {
    const wasRunning = toggle.getAttribute("aria-pressed") === "true";
    call(wasRunning ? "/pipeline/pause" : "/pipeline/start", toggle);
  });
  reset.addEventListener("click", () => call("/pipeline/reset", reset));
}

// ─── boot ─────────────────────────────────────────────────────────────────
(function init() {
  setInterval(() => {
    document.getElementById("clock").textContent = fmtClock();
  }, 1000);
  document.getElementById("clock").textContent = fmtClock();

  initTheme();
  wireForms();
  wirePipeControls();

  // initial fetches in parallel
  Promise.allSettled([pollState(), pollInfo(), pollPerf(), pollHistory()]);

  setInterval(pollState,   POLL_MS);
  setInterval(pollInfo,    INFO_POLL_MS);
  setInterval(pollPerf,    PERF_POLL_MS);
  setInterval(pollHistory, HISTORY_POLL_MS);
  setInterval(tickOverrideCountdowns, 1000);
})();
