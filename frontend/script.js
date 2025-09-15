// =================== CONFIG ===================
const API_BASE = ""; // empty = relative to current host

// =================== DOM HELPERS ===================
function el(id) { return document.getElementById(id); }
function currentLang() { return document.documentElement.getAttribute("lang") || "en"; }
function t(key) {
  const d = (window.dict && dict[currentLang()]) || dict.en;
  return (d && d[key]) || key;
}
function setPill(node, textKey, state) {
  if (!node) return;
  node.textContent = t(textKey);
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
  if (value !== undefined && value !== null && String(value).trim() !== "") {
    fd.append(key, value);
  }
}

// =================== HEALTH ===================
async function checkHealth() {
  const pill = el("healthPill");
  try {
    const r = await fetch(`${API_BASE}/health`, { cache: "no-store" });
    if (!r.ok) throw new Error(`${r.status}`);
    const j = await r.json();
    if (j.status === "ok") {
      setPill(pill, "healthOk", "ok");
    } else {
      setPill(pill, "healthDegraded", "warn");
      console.warn("Health details:", j);
    }
  } catch (e) {
    setPill(pill, "healthDown", "err");
    console.error("Health check failed:", e);
  }
}

// =================== PROCESS ===================
async function runProcess() {
  clearLog();
  const btn = el("runBtn");
  btn.disabled = true;
  setPill(el("liveStatus"), "uploading", "warn");

  try {
    const excels = el("excel").files;
    if (!excels || excels.length === 0) {
      appendLog("Please choose at least one .xlsx file.");
      setPill(el("liveStatus"), "idle", "");
      btn.disabled = false;
      return;
    }

    // Build FormData with all config fields
    const fd = new FormData();
    for (const f of excels) fd.append("excel_files", f);
    const mapping = el("mapping").files[0];
    if (mapping) fd.append("mapping_file", mapping);

    addIfPresent(fd, "materiality_vnd", el("materiality_vnd").value);
    addIfPresent(fd, "recurring_pct_threshold", el("recurring_pct_threshold").value);
    addIfPresent(fd, "revenue_opex_pct_threshold", el("revenue_opex_pct_threshold").value);
    addIfPresent(fd, "bs_pct_threshold", el("bs_pct_threshold").value);
    addIfPresent(fd, "recurring_code_prefixes", el("recurring_code_prefixes").value);
    addIfPresent(fd, "min_trend_periods", el("min_trend_periods").value);
    addIfPresent(fd, "gm_drop_threshold_pct", el("gm_drop_threshold_pct").value);
    addIfPresent(fd, "dep_pct_only_prefixes", el("dep_pct_only_prefixes").value);
    addIfPresent(fd, "customer_column_hints", el("customer_column_hints").value);

    appendLog("POST /process …");
    const resp = await fetch(`${API_BASE}/process`, { method: "POST", body: fd });

    if (!resp.ok) {
      const text = await resp.text();
      setPill(el("liveStatus"), "failed", "err");
      appendLog(`Server error (${resp.status}):\n${text}`);
      btn.disabled = false;
      return;
    }

    setPill(el("liveStatus"), "processing", "warn");

    // Expect ONE Excel workbook (.xlsx)
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);

    // Overwrite the file list with ONE fresh link
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
    if (title) title.textContent = t("downloadThisRun");

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

  // Optional: "Clear log" button under Response box
  const responseBox = el("response");
  if (responseBox && !el("clearLogBtn")) {
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
  const list = el("files");
  if (list) {
    const li = document.createElement("li");
    li.textContent = t("noFilesYet");
    list.appendChild(li);
  }
  const title = el("outputsTitle");
  if (title) title.textContent = t("downloadThisRun");
});
