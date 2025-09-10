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

    // Build FormData with all config fields, including gm_drop_threshold_pct
    const fd = new FormData();
    for (const f of document.getElementById("excel").files) fd.append("excel_files", f);
    const mapping = document.getElementById("mapping").files[0];
    if (mapping) fd.append("mapping_file", mapping);

    fd.append("materiality_vnd", document.getElementById("materiality_vnd").value);
    fd.append("recurring_pct_threshold", document.getElementById("recurring_pct_threshold").value);
    fd.append("revenue_opex_pct_threshold", document.getElementById("revenue_opex_pct_threshold").value);
    fd.append("bs_pct_threshold", document.getElementById("bs_pct_threshold").value);
    fd.append("recurring_code_prefixes", document.getElementById("recurring_code_prefixes").value);
    fd.append("min_trend_periods", document.getElementById("min_trend_periods").value);
    fd.append("gm_drop_threshold_pct", document.getElementById("gm_drop_threshold_pct").value); // 👈 NEW

    appendLog("POST /process …");
    const resp = await fetch(`/process`, { method: "POST", body: fd });

    if (!resp.ok) {
      const text = await resp.text();
      setPill(el("liveStatus"), "failed", "err");
      appendLog(`Server error (${resp.status}):\n${text}`);
      return;
    }

    setPill(el("liveStatus"), "processing…", "warn");

    // Expect ONE Excel workbook (.xlsx)
    const blob = await resp.blob();

    // Overwrite the file list with ONE fresh link
    const url = URL.createObjectURL(blob);
    const list = el("files");
    list.innerHTML = "";
    const li = document.createElement("li");
    const a = document.createElement("a");
    a.href = url;
    a.download = "variance_output.xlsx";
    a.textContent = "variance_output.xlsx";
    li.appendChild(a);
    list.appendChild(li);

    const title = el("outputsTitle");
    if (title) title.textContent = "Download (this run)";

    appendLog("Success: Excel ready to download.");
    setPill(el("liveStatus"), "done", "ok");
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

  if (runBtn) runBtn.addEventListener("click", runProcess);
  if (refreshHealthBtn) refreshHealthBtn.addEventListener("click", checkHealth);

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
  renderLatestFiles([]);  // empty at start
  const title = el("outputsTitle"); 
  if (title) title.textContent = "Outputs";
});

