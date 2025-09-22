// =================== CONFIG ===================
const API_BASE = ""; // empty = relative to current host

// =================== DOM HELPERS ===================
function el(id) { return document.getElementById(id); }
function currentLang() { return document.documentElement.getAttribute("lang") || "en"; }
function t(key) {
  const d = (window.dict && window.dict[currentLang()]) || (window.dict && window.dict.en) || {};
  return (d && d[key]) || key;
}
function setPill(node, textKey, state) {
  if (!node) return;
  node.textContent = t(textKey);
  node.className = "pill";
  node.classList.remove("ok", "warn", "err");
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

// Accept "0.05", "5%", or "5"→0.05
function normPct(val) {
  if (val == null) return "";
  const s = String(val).trim();
  if (s === "") return "";
  if (s.endsWith("%")) {
    const num = parseFloat(s.replace("%",""));
    return isNaN(num) ? "" : String(num / 100);
  }
  const num = parseFloat(s);
  if (isNaN(num)) return "";
  return (num > 1 && num <= 100) ? String(num / 100) : String(num);
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
      appendLog(t("noExcelSelected"));
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

    // Percent-like fields normalized
    addIfPresent(fd, "recurring_pct_threshold", normPct(el("recurring_pct_threshold").value));
    addIfPresent(fd, "revenue_opex_pct_threshold", normPct(el("revenue_opex_pct_threshold").value));
    addIfPresent(fd, "bs_pct_threshold", normPct(el("bs_pct_threshold").value));
    addIfPresent(fd, "gm_drop_threshold_pct", normPct(el("gm_drop_threshold_pct").value));

    // CSV / text / numeric fields
    addIfPresent(fd, "recurring_code_prefixes", el("recurring_code_prefixes").value);
    addIfPresent(fd, "min_trend_periods", el("min_trend_periods").value);
    addIfPresent(fd, "dep_pct_only_prefixes", el("dep_pct_only_prefixes").value);

    appendLog("POST /process …");
    const resp = await fetch(`${API_BASE}/process`, { method: "POST", body: fd });

    if (!resp.ok) {
      let text = "";
      try { text = await resp.text(); } catch (_) {}
      setPill(el("liveStatus"), "failed", "err");
      appendLog(`Server error (${resp.status}):\n${text || "Unknown error"}`);
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

    // Automatically run revenue analysis on the first uploaded file
    await runRevenueAnalysisAutomatically(excels[0]);

    setPill(el("liveStatus"), "done", "ok");
  } catch (e) {
    appendLog("Fetch failed: " + e.message);
    setPill(el("liveStatus"), "failed", "err");
  } finally {
    btn.disabled = false;
  }
}

// =================== REVENUE ANALYSIS ===================
async function runRevenueAnalysisAutomatically(file) {
  if (!file) return;

  const resultsDiv = el("revenueResults");
  const contentDiv = el("revenueContent");

  try {
    appendLog("Running revenue impact analysis...");

    const formData = new FormData();
    formData.append("excel_file", file);

    const response = await fetch("/analyze-revenue", {
      method: "POST",
      body: formData
    });

    const result = await response.json();

    if (result.error) {
      throw new Error(result.error);
    }

    // Display results
    displayRevenueAnalysis(result);

    if (resultsDiv) {
      resultsDiv.style.display = "block";
    }

    appendLog("Revenue analysis completed successfully.");

  } catch (error) {
    console.error("Revenue analysis error:", error);

    if (contentDiv) {
      contentDiv.innerHTML = `<div class="error">Revenue analysis failed: ${error.message}</div>`;
    }

    if (resultsDiv) {
      resultsDiv.style.display = "block";
    }

    appendLog(`Revenue analysis failed: ${error.message}`);
  }
}

// =================== INIT ===================
function injectClearLogButton() {
  const responseBox = el("response");
  if (responseBox && !el("clearLogBtn")) {
    const btn = document.createElement("button");
    btn.id = "clearLogBtn";
    btn.className = "btn";
    btn.style.marginTop = "8px";
    btn.setAttribute("data-i18n", "clearLog");
    btn.textContent = t("clearLog");
    btn.addEventListener("click", clearLog);
    responseBox.parentElement.appendChild(btn);
  }
}

function seedNoFilesPlaceholder() {
  const list = el("files");
  if (!list) return;
  if (list.children.length === 0) {
    const li = document.createElement("li");
    li.setAttribute("data-i18n", "noFilesYet");
    li.textContent = t("noFilesYet");
    list.appendChild(li);
  }
}

function bindEvents() {
  const runBtn = el("runBtn");

  if (runBtn) runBtn.addEventListener("click", runProcess);

  injectClearLogButton();
}


function displayRevenueAnalysis(data) {
  const contentDiv = el("revenueContent");
  if (!contentDiv) return;

  let html = "";

  // Summary section
  if (data.summary) {
    html += `
      <div class="analysis-section">
        <h4>📊 Summary</h4>
        <div class="summary-stats">
          <div class="stat">
            <label>Subsidiary:</label>
            <span>${data.subsidiary || 'N/A'}</span>
          </div>
          <div class="stat">
            <label>Total Revenue Accounts:</label>
            <span>${data.summary.total_accounts || 0}</span>
          </div>
          <div class="stat">
            <label>Latest Total Revenue:</label>
            <span>${formatVND(data.summary.total_revenue_latest || 0)}</span>
          </div>
          <div class="stat">
            <label>Highest Variance Account:</label>
            <span>${data.summary.highest_variance_account || 'N/A'}</span>
          </div>
        </div>
      </div>
    `;
  }

  // Total revenue trend
  if (data.total_revenue_changes && data.total_revenue_changes.length > 0) {
    html += `
      <div class="analysis-section">
        <h4>📈 Total Revenue Trend (511*)</h4>
        <div class="revenue-changes">
    `;

    data.total_revenue_changes.forEach(change => {
      const changeClass = change.change >= 0 ? 'positive' : 'negative';
      html += `
        <div class="change-item ${changeClass}">
          <div class="period">${change.from} → ${change.to}</div>
          <div class="values">
            ${formatVND(change.prev_value)} → ${formatVND(change.curr_value)}
          </div>
          <div class="change">
            ${change.change >= 0 ? '+' : ''}${formatVND(change.change)}
            (${change.pct_change >= 0 ? '+' : ''}${change.pct_change.toFixed(1)}%)
          </div>
        </div>
      `;
    });

    html += `
        </div>
      </div>
    `;
  }

  // Account-level analysis with customer impacts
  if (data.account_analysis && data.account_analysis.length > 0) {
    html += `
      <div class="analysis-section">
        <h4>🏦 Revenue by Account & Customer Impact</h4>
    `;

    data.account_analysis.forEach(account => {
      if (account.biggest_change && Math.abs(account.biggest_change.change) > 1000000) {
        html += `
          <div class="account-analysis">
            <h5>${account.account}</h5>
            <div class="biggest-change">
              <strong>Biggest Change:</strong> ${account.biggest_change.from} → ${account.biggest_change.to}
              <br>
              <span class="${account.biggest_change.change >= 0 ? 'positive' : 'negative'}">
                ${account.biggest_change.change >= 0 ? '+' : ''}${formatVND(account.biggest_change.change)}
                (${account.biggest_change.pct_change >= 0 ? '+' : ''}${account.biggest_change.pct_change.toFixed(1)}%)
              </span>
            </div>
        `;

        if (account.customer_impacts && account.customer_impacts.length > 0) {
          html += `
            <div class="customer-impacts">
              <strong>Top Customer Impacts:</strong>
              <ul>
          `;

          account.customer_impacts.forEach(impact => {
            const impactClass = impact.change >= 0 ? 'positive' : 'negative';
            html += `
              <li class="${impactClass}">
                <strong>${impact.entity}:</strong>
                ${impact.change >= 0 ? '+' : ''}${formatVND(impact.change)}
                (${impact.pct_change >= 0 ? '+' : ''}${impact.pct_change.toFixed(1)}%)
                <br>
                <small>${formatVND(impact.prev_val)} → ${formatVND(impact.curr_val)}</small>
              </li>
            `;
          });

          html += `
              </ul>
            </div>
          `;
        }

        html += `</div>`;
      }
    });

    html += `</div>`;
  }

  // Risk periods
  if (data.risk_periods && data.risk_periods.length > 0) {
    html += `
      <div class="analysis-section">
        <h4>⚠️ Risk Periods</h4>
        <div class="risk-periods">
    `;

    data.risk_periods.forEach(risk => {
      const riskClass = risk.risk_level.toLowerCase();
      html += `
        <div class="risk-item ${riskClass}">
          <div class="risk-badge">${risk.risk_level}</div>
          <div class="risk-details">
            <strong>${risk.period}</strong><br>
            ${risk.description}
          </div>
        </div>
      `;
    });

    html += `
        </div>
      </div>
    `;
  }

  contentDiv.innerHTML = html;
}

function formatVND(amount) {
  if (typeof amount !== 'number') return 'N/A';
  return new Intl.NumberFormat('vi-VN').format(Math.round(amount)) + ' VND';
}

document.addEventListener("DOMContentLoaded", () => {
  bindEvents();
  seedNoFilesPlaceholder();

  // Keep outputs header localized (it has data-i18n already)
  const title = el("outputsHdr");
  if (title) title.textContent = t("downloadThisRun");
});
