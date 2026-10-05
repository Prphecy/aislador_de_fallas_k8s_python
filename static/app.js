"use strict";

const SEVERITY_ICON = { "CRÍTICA": "🔴", "ALTA": "🟠", "MEDIA": "🟡", "BAJA": "🔵", "INFO": "🟢" };
const SEVERITY_LABEL_ES = {
  "CRÍTICA": "CRÍTICA", "ALTA": "ALTA", "MEDIA": "MEDIA", "BAJA": "BAJA", "INFO": "SALUDABLE",
};

const el = (id) => document.getElementById(id);

const namespaceInput = el("namespaceInput");
const namespaceList = el("namespaceList");
const podInput = el("podInput");
const podList = el("podList");
const contextInput = el("contextInput");
const connectBtn = el("connectBtn");
const refreshBtn = el("refreshBtn");
const statusPill = el("statusPill");
const statusText = el("statusText");
const banner = el("banner");

const hero = el("hero");
const heroEmpty = el("heroEmpty");
const heroContent = el("heroContent");
const heroIcon = el("heroIcon");
const heroSeverity = el("heroSeverity");
const heroPod = el("heroPod");
const heroMeta = el("heroMeta");
const heroClock = el("heroClock");

const findingsEl = el("findings");
const inferencePanel = el("inferencePanel");
const inferenceTree = el("inferenceTree");
const podInfoPanel = el("podInfoPanel");
const podInfoGrid = el("podInfoGrid");
const containersPanel = el("containersPanel");
const containersBody = document.querySelector("#containersTable tbody");
const eventsPanel = el("eventsPanel");
const eventsBody = document.querySelector("#eventsTable tbody");
const timelinePanel = el("timelinePanel");
const timelineList = el("timelineList");

let socket = null;
let manualClose = false;
let lastFindingKey = null;   // para no re-animar el historial en cada heartbeat
const MAX_TIMELINE = 25;

// ── Carga de namespaces / pods para los <datalist> ─────────────────────
async function loadNamespaces() {
  try {
    const res = await fetch("/api/namespaces");
    if (!res.ok) return;
    const data = await res.json();
    namespaceList.innerHTML = "";
    for (const ns of data.namespaces) {
      const opt = document.createElement("option");
      opt.value = ns;
      namespaceList.appendChild(opt);
    }
  } catch { /* el clúster puede no estar disponible todavía; no es fatal */ }
}

async function loadPods() {
  const namespace = namespaceInput.value.trim() || "default";
  try {
    const res = await fetch(`/api/pods?namespace=${encodeURIComponent(namespace)}`);
    if (!res.ok) return;
    const data = await res.json();
    podList.innerHTML = "";
    for (const p of data.pods) {
      const opt = document.createElement("option");
      opt.value = p.name;
      opt.label = p.phase;
      podList.appendChild(opt);
    }
  } catch { /* igual, no bloquea: el usuario puede escribir el nombre a mano */ }
}

refreshBtn.addEventListener("click", () => { loadNamespaces(); loadPods(); });
namespaceInput.addEventListener("change", loadPods);
window.addEventListener("DOMContentLoaded", () => { loadNamespaces(); loadPods(); });

// ── Conexión / desconexión ──────────────────────────────────────────────
el("controlsForm").addEventListener("submit", (ev) => {
  ev.preventDefault();
  if (socket) {
    disconnect();
  } else {
    connect();
  }
});

function connect() {
  const pod = podInput.value.trim();
  if (!pod) return;
  const namespace = namespaceInput.value.trim() || "default";
  const context = contextInput.value.trim();

  resetPanels();
  manualClose = false;
  setStatus("connecting", "Conectando…");

  const proto = location.protocol === "https:" ? "wss" : "ws";
  const params = new URLSearchParams({ pod, namespace });
  if (context) params.set("context", context);
  socket = new WebSocket(`${proto}://${location.host}/ws/watch?${params.toString()}`);

  socket.addEventListener("message", (ev) => handleMessage(JSON.parse(ev.data)));
  socket.addEventListener("close", onSocketClosed);
  socket.addEventListener("error", () => { /* el evento "close" llega después y maneja el aviso */ });

  connectBtn.textContent = "Desconectar";
  connectBtn.dataset.connected = "true";
}

function disconnect() {
  manualClose = true;
  if (socket) socket.close();
  socket = null;
  setStatus("offline", "Desconectado");
  connectBtn.textContent = "Conectar";
  connectBtn.dataset.connected = "false";
}

function onSocketClosed() {
  socket = null;
  connectBtn.textContent = "Conectar";
  connectBtn.dataset.connected = "false";
  if (!manualClose) setStatus("error", "Conexión perdida");
}

function setStatus(state, text) {
  statusPill.dataset.state = state;
  statusText.textContent = text;
}

// ── Mensajes entrantes ───────────────────────────────────────────────────
function handleMessage(msg) {
  if (msg.type === "report") {
    setStatus("live", "En vivo");
    hideBanner();
    renderReport(msg);
  } else if (msg.type === "heartbeat") {
    setStatus("live", "En vivo");
    heroClock.textContent = formatTime(msg.timestamp);
  } else if (msg.type === "not_found") {
    setStatus("live", "Esperando al Pod…");
    showBanner(msg.message + " Si lo acabas de aplicar, aparecerá solo.", "warn");
  } else if (msg.type === "error") {
    setStatus("error", "Error");
    showBanner(msg.message, "error");
  }
}

function showBanner(message, tone) {
  banner.textContent = message;
  banner.dataset.tone = tone === "error" ? "error" : "warn";
  banner.hidden = false;
}
function hideBanner() { banner.hidden = true; }

// ── Render: hero ─────────────────────────────────────────────────────────
function renderReport(msg) {
  const { pod, result, timestamp, context } = msg;
  const worst = result.worst_severity;

  heroEmpty.hidden = true;
  heroContent.hidden = false;
  hero.dataset.severity = worst || "";
  heroIcon.textContent = SEVERITY_ICON[worst] || "⚪";
  heroSeverity.textContent = SEVERITY_LABEL_ES[worst] || worst || "—";
  heroPod.textContent = `${pod.namespace}/${pod.name}`;
  heroMeta.textContent = `Fase ${pod.phase} · nodo ${pod.node || "—"} · contexto ${context || "—"}`;
  heroClock.textContent = formatTime(timestamp);

  renderFindings(result.findings);
  renderInference(result);
  renderPodInfo(pod);
  renderContainers(pod.containers);
  renderEvents(pod.events);
  pushTimeline(pod, result, timestamp);
}

function formatTime(iso) {
  try { return new Date(iso).toLocaleTimeString("es-CO", { hour12: false }); }
  catch { return iso; }
}

// ── Render: tarjetas de diagnóstico ────────────────────────────────────
function renderFindings(findings) {
  findingsEl.innerHTML = "";
  findings.forEach((f, i) => findingsEl.appendChild(buildFindingCard(f, i === 0)));
}

function buildFindingCard(f, primary) {
  const card = document.createElement("article");
  card.className = "finding-card" + (primary ? " is-primary" : "");
  card.dataset.severity = f.severity;

  const head = document.createElement("div");
  head.className = "finding-head";
  head.innerHTML = `
    <span class="finding-icon" aria-hidden="true">${SEVERITY_ICON[f.severity] || "⚪"}</span>
    <div class="finding-head-text">
      <div class="finding-kicker">${primary ? "Diagnóstico" : "Hallazgo adicional"}${f.container ? " · " + escapeHtml(f.container) : ""}</div>
      <div class="finding-category">${escapeHtml(f.category)}</div>
    </div>
    <div class="finding-rule">${escapeHtml(f.rule_id)} · ${escapeHtml(f.key)}</div>
  `;

  const body = document.createElement("div");
  body.className = "finding-body";

  const causeBlock = document.createElement("div");
  causeBlock.innerHTML = `<div class="finding-row-label">Causa raíz</div><p class="finding-cause"></p>`;
  causeBlock.querySelector("p").textContent = f.cause;
  body.appendChild(causeBlock);

  const stepsBlock = document.createElement("div");
  stepsBlock.innerHTML = `<div class="finding-row-label">Remediación</div>`;
  const stepsList = document.createElement("ol");
  stepsList.className = "steps";
  for (const step of f.steps) {
    const li = document.createElement("li");
    const p = document.createElement("span");
    p.textContent = step.description;
    li.appendChild(p);
    if (step.command) {
      const cmd = document.createElement("button");
      cmd.type = "button";
      cmd.className = "cmd";
      cmd.textContent = step.command;
      cmd.title = "Clic para copiar";
      cmd.addEventListener("click", () => copyCommand(cmd));
      li.appendChild(cmd);
    }
    stepsList.appendChild(li);
  }
  stepsBlock.appendChild(stepsList);
  body.appendChild(stepsBlock);

  if (f.evidence && f.evidence.length) {
    const evBlock = document.createElement("div");
    evBlock.innerHTML = `<div class="finding-row-label">Evidencia</div>`;
    const evList = document.createElement("ul");
    evList.className = "evidence";
    for (const line of f.evidence) {
      const li = document.createElement("li");
      if (line.startsWith("log ▸")) li.classList.add("is-log");
      li.textContent = line;
      evList.appendChild(li);
    }
    evBlock.appendChild(evList);
    body.appendChild(evBlock);
  }

  card.appendChild(head);
  card.appendChild(body);
  return card;
}

function copyCommand(button) {
  const text = button.textContent;
  navigator.clipboard?.writeText(text).then(() => {
    button.classList.add("copied");
    setTimeout(() => button.classList.remove("copied"), 1500);
  }).catch(() => {});
}

// ── Render: árbol de inferencia ────────────────────────────────────────
function renderInference(result) {
  inferencePanel.hidden = false;
  inferenceTree.innerHTML = "";

  const factsLine = Object.entries(result.fact_counts).map(([k, v]) => `${k}×${v}`).join(", ");
  inferenceTree.appendChild(treeItem(`Hechos declarados: ${factsLine}`, "is-fact"));

  for (const s of result.symptoms) {
    const label = `${s.rule} → síntoma ${s.kind}` + (s.container ? ` @ ${s.container}` : "");
    inferenceTree.appendChild(treeItem(label, "is-symptom"));
  }
  for (const f of result.findings) {
    inferenceTree.appendChild(treeItem(`${f.rule_id} → diagnóstico ${f.key} (${f.severity})`, "is-finding"));
  }
}

function treeItem(text, cls) {
  const li = document.createElement("li");
  li.className = cls;
  li.textContent = text;
  return li;
}

// ── Render: info del Pod ────────────────────────────────────────────────
function renderPodInfo(pod) {
  podInfoPanel.hidden = false;
  podInfoGrid.innerHTML = "";
  const rows = [
    ["Namespace", pod.namespace], ["Fase", pod.phase], ["Nodo", pod.node || "—"],
    ["QoS", pod.qos || "—"], ["Motivo", pod.reason || "—"],
  ];
  for (const [k, v] of rows) {
    const dt = document.createElement("dt"); dt.textContent = k;
    const dd = document.createElement("dd"); dd.textContent = v;
    podInfoGrid.appendChild(dt); podInfoGrid.appendChild(dd);
  }
}

// ── Render: tabla de contenedores ───────────────────────────────────────
function renderContainers(containers) {
  containersPanel.hidden = false;
  containersBody.innerHTML = "";
  for (const c of containers) {
    const tr = document.createElement("tr");
    const last = c.last_reason ? `${c.last_reason} (${c.last_exit_code})` : "—";
    tr.innerHTML = `
      <td class="is-mono">${escapeHtml(c.name)}${c.init ? " (init)" : ""}</td>
      <td>${escapeHtml(c.display_state)}</td>
      <td class="${c.problem ? "tag-problem" : ""}">${escapeHtml(c.reason || "—")}</td>
      <td class="${c.restarts >= 3 ? "tag-warn" : ""}">${c.restarts}</td>
      <td class="${c.ready ? "tag-ok" : "tag-problem"}">${c.ready ? "✔" : "✘"}</td>
      <td class="is-mono cell-dim">${escapeHtml(c.mem_limit || "—")}</td>
    `;
    containersBody.appendChild(tr);
  }
}

// ── Render: tabla de eventos ────────────────────────────────────────────
function renderEvents(events) {
  eventsPanel.hidden = events.length === 0;
  eventsBody.innerHTML = "";
  for (const e of events) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="${e.type === "Warning" ? "tag-warn" : "cell-dim"}">${escapeHtml(e.type)}</td>
      <td>${escapeHtml(e.reason)}</td>
      <td>${escapeHtml(e.message)}</td>
      <td class="cell-dim">${e.count}</td>
    `;
    eventsBody.appendChild(tr);
  }
}

// ── Historial en vivo ────────────────────────────────────────────────────
function pushTimeline(pod, result, timestamp) {
  const key = result.findings.map((f) => f.rule_id).sort().join("+");
  if (key === lastFindingKey) return;   // mismo diagnóstico: no duplicar en el historial
  lastFindingKey = key;

  timelinePanel.hidden = false;
  const empty = timelineList.querySelector(".timeline-empty");
  if (empty) empty.remove();

  const li = document.createElement("li");
  const worst = result.worst_severity;
  const summary = result.findings.map((f) => f.category).join(" · ");
  li.innerHTML = `
    <span class="timeline-dot" data-severity="${worst}" aria-hidden="true"></span>
    <span><span class="timeline-time">${formatTime(timestamp)}</span>${escapeHtml(summary)}</span>
  `;
  timelineList.insertBefore(li, timelineList.firstChild);
  while (timelineList.children.length > MAX_TIMELINE) {
    timelineList.removeChild(timelineList.lastChild);
  }
}

// ── Utilidades ───────────────────────────────────────────────────────────
function resetPanels() {
  heroEmpty.hidden = true;
  heroContent.hidden = false;
  hero.dataset.severity = "";
  heroIcon.textContent = "⏳";
  heroSeverity.textContent = "";
  heroPod.textContent = `${namespaceInput.value.trim() || "default"}/${podInput.value.trim()}`;
  heroMeta.textContent = "Esperando la primera lectura…";
  heroClock.textContent = "—";
  hideBanner();
  findingsEl.innerHTML = "";
  inferencePanel.hidden = true;
  podInfoPanel.hidden = true;
  containersPanel.hidden = true;
  eventsPanel.hidden = true;
  lastFindingKey = null;
  timelineList.innerHTML = '<li class="timeline-empty">Aún sin cambios de estado.</li>';
  timelinePanel.hidden = true;
}

function escapeHtml(str) {
  return String(str ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}
