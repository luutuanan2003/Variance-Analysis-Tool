// =================== CONFIG ===================
const API_BASE = ""; // empty = relative to current host

// =================== DOM HELPERS ===================
function el(id) { return document.getElementById(id); }
function setPill(node, text, state) {
  if (!node) return;
  node.textContent = text;
  node.className = "pill";
  if (state) node.classList.add(state);
}
function appendLog(msg) {
  const div = el("response"); if (!div) return;
  div.textContent += (msg.endsWith("\n") ? msg : msg + "\n");
  div.scrollTop = div.scrollHeight;
}
function clearLog() { const div = el("response"); if (div) div.textContent = ""; }
function addIfPresent(fd, key, value) {
  if (value !== undefined && value !== null && String(value).trim() !== "") fd.append(key, value);
}
function renderLatestFiles(files) {
  const list = el("files"); if (!list) return;
  list.innerHTML = "";
  if (!files || files.length === 0) {
    const li = document.createElement("li");
    li.textContent = "No files yet.";
    list.appendChild(li);
    return;
  }
  files.forEach((name) => {
    const li = document.createElement("li");
    const a = document.createElement("a");
    a.href = `${API_BASE}/download/${encodeURIComponent(name)}`;
    a.textContent = name;
    a.download = name;
    li.appendChild(a);

    const dl = document.createElement("a");
    dl.href = a.href;
    dl.textContent = "download";
    li.appendChild(dl);

    list.appendChild(li);
  });
}

// Keep track of current run outputs // NEW
let latestRunFiles = [];

// =================== HEALTH ===================
async function checkHealth() {
  const pill = el("healthPill");
  try {
    const r = await fetch(`${API_BASE}/health`, { cache: "no-store" });
    if (!r.ok) throw new Error(`${r.status}`);
    const j = await r.json();
    if (j.status === "ok") {
      setPill(pill, "health: ok", "ok");
    } else {
      const c = j.checks || {};
      const msgs = [];
      if (c.frontend && c.frontend.index_exists === false) msgs.push("no index.html");
      const s = c.storage || {};
      ["input_writable","output_writable","archive_writable","logic_writable"].forEach(k=>{
        if (s[k] === false) msgs.push(`${k} false`);
      });
      if (s.input_exists === false) msgs.push("input missing");
      if (s.output_exists === false) msgs.push("output missing");
      if (s.archive_exists === false) msgs.push("archive missing");
      if (s.logic_exists === false) msgs.push("logic missing");
      if (c.config && c.config.loaded === false) msgs.push("config not loaded");
      setPill(pill, `health: degraded${msgs.length ? " (" + msgs.join(", ") + ")" : ""}`, "warn");
      console.warn("Health details:", j);
    }
  } catch (e) {
    setPill(pill, "health: down", "err");
    console.error("Health check failed:", e);
  }
}

// =================== OUTPUT LIST (ALL) ===================
async function refreshOutputs() {
  try {
    const r = await fetch(`${API_BASE}/outputs`, { cache: "no-store" });
    if (!r.ok) throw new Error(`${r.status}`);
    const j = await r.json();
    renderLatestFiles(j.files || []);
    const title = el("outputsTitle"); if (title) title.textContent = "Outputs (all)";
  } catch (e) {
    appendLog(`Failed to list outputs: ${e.message}`);
  }
}

// =================== PROCESS ===================
async function runProcess() {
  clearLog();
  const btn = el("runBtn");
  btn.disabled = true;
  setPill(el("liveStatus"), "uploading…", "warn");

  try {
    const excels = el("excel").files;
    if (!excels || excels.length === 0) {
      appendLog("Please choose at least one .xlsx file.");
      setPill(el("liveStatus"), "idle", "");
      return;
    }

    const fd = new FormData();
    for (const f of excels) fd.append("excel_files", f);
    const mapping = el("mapping").files?.[0];
    if (mapping) fd.append("mapping_file", mapping);

    addIfPresent(fd, "materiality_vnd", el("materiality_vnd").value);
    addIfPresent(fd, "archive_processed", el("archive_processed").value);
    addIfPresent(fd, "recurring_pct_threshold", el("recurring_pct_threshold").value);
    addIfPresent(fd, "revenue_opex_pct_threshold", el("revenue_opex_pct_threshold").value);
    addIfPresent(fd, "bs_pct_threshold", el("bs_pct_threshold").value);
    addIfPresent(fd, "recurring_code_prefixes", el("recurring_code_prefixes").value);
    addIfPresent(fd, "min_trend_periods", el("min_trend_periods").value);
    addIfPresent(fd, "base_dir", el("base_dir").value);

    appendLog("POST /process …");
    const resp = await fetch(`${API_BASE}/process`, { method: "POST", body: fd });

    setPill(el("liveStatus"), "processing…", "warn");

    const text = await resp.text();
    let json; try { json = JSON.parse(text); } catch { json = { raw: text }; }

    if (!resp.ok) {
      appendLog(`Server error (${resp.status}):\n` + JSON.stringify(json, null, 2));
      setPill(el("liveStatus"), "failed", "err");
    } else {
      appendLog("Success:\n" + JSON.stringify(json, null, 2));
      setPill(el("liveStatus"), "done", "ok");

      // >>> Show ONLY this run's outputs  // NEW/CHANGED
      latestRunFiles = Array.isArray(json.generated_files) ? json.generated_files : [];
      renderLatestFiles(latestRunFiles);
      const title = el("outputsTitle"); if (title) title.textContent = "Outputs (this run)";
      // (we no longer call refreshOutputs() here)
    }
  } catch (e) {
    appendLog("Fetch failed: " + e.message);
    setPill(el("liveStatus"), "failed", "err");
  } finally {
    btn.disabled = false;
  }
}

// =================== INIT ===================
function bindEvents() {
  const runBtn = el("runBtn");
  const refreshHealthBtn = el("refreshHealth");
  const showAllBtn = el("showAllBtn"); // NEW

  if (runBtn) runBtn.addEventListener("click", runProcess);
  if (refreshHealthBtn) refreshHealthBtn.addEventListener("click", checkHealth);
  if (showAllBtn) showAllBtn.addEventListener("click", refreshOutputs); // NEW

  // Optional: add a small "Clear log" button under Response box
  const responseBox = el("response");
  if (responseBox && !document.getElementById("clearLogBtn")) {
    const btn = document.createElement("button");
    btn.id = "clearLogBtn";
    btn.className = "btn";
    btn.style.marginTop = "8px";
    btn.textContent = "Clear log";
    btn.addEventListener("click", clearLog);
    responseBox.parentElement.appendChild(btn);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  bindEvents();
  checkHealth();
  // Default: show nothing until a run happens // NEW/CHANGED
  renderLatestFiles([]);
  const title = el("outputsTitle"); if (title) title.textContent = "Outputs (this run)";
});
