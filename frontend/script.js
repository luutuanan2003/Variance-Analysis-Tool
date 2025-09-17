// AI-only variance analysis script
const API_BASE = "";

function el(id) { return document.getElementById(id); }

function appendLog(msg) {
  const div = el("response");
  if (!div) return;
  div.textContent += (msg.endsWith("\n") ? msg : msg + "\n");
  div.scrollTop = div.scrollHeight;
}

function clearLog() {
  const div = el("response");
  if (div) div.textContent = "";
}

function updateStatus(status, text) {
  const statusEl = el("liveStatus");
  if (!statusEl) return;

  statusEl.className = `pill ${status}`;
  statusEl.textContent = text;
}

function showProcessingStatus(show) {
  const statusDiv = el("processingStatus");
  if (statusDiv) {
    statusDiv.style.display = show ? "block" : "none";
  }
}

function addProcessingStep(step, status = "pending") {
  const stepsDiv = el("statusSteps");
  if (!stepsDiv) return;

  const stepDiv = document.createElement("div");
  stepDiv.className = `status-step ${status}`;
  stepDiv.innerHTML = `
    <span class="step-icon">${status === "completed" ? "✅" : status === "processing" ? "🔄" : "⏳"}</span>
    <span class="step-text">${step}</span>
  `;
  stepDiv.id = `step-${stepsDiv.children.length}`;
  stepsDiv.appendChild(stepDiv);

  return stepDiv.id;
}

function updateProcessingStep(stepId, status, text) {
  const stepDiv = el(stepId);
  if (!stepDiv) return;

  stepDiv.className = `status-step ${status}`;
  const icon = stepDiv.querySelector(".step-icon");
  const textEl = stepDiv.querySelector(".step-text");

  if (icon) {
    icon.textContent = status === "completed" ? "✅" : status === "processing" ? "🔄" : status === "error" ? "❌" : "⏳";
  }
  if (textEl && text) {
    textEl.textContent = text;
  }
}

async function processFiles() {
  const runBtn = el("runBtn");
  const excelInput = el("excel");

  if (!excelInput?.files?.length) {
    alert("Please select at least one Excel file");
    return;
  }

  // Disable button and show loading
  runBtn.disabled = true;
  runBtn.classList.add("loading");
  updateStatus("processing", "Starting...");
  showProcessingStatus(true);
  clearLog();

  // Clear previous steps
  const stepsDiv = el("statusSteps");
  if (stepsDiv) stepsDiv.innerHTML = "";

  let sessionId = null;

  try {
    // Create form data
    const fd = new FormData();
    for (const file of excelInput.files) {
      fd.append("excel_files", file);
    }

    appendLog("🚀 Starting AI-powered variance analysis...");

    // Start analysis and get session ID
    const resp = await fetch(`${API_BASE}/start_analysis`, { method: "POST", body: fd });

    if (!resp.ok) {
      throw new Error(`Server error (${resp.status}): ${await resp.text()}`);
    }

    const data = await resp.json();
    sessionId = data.session_id;

    appendLog(`📡 Analysis session started: ${sessionId}`);
    appendLog("📡 Streaming live logs from backend...");

    // Start streaming logs
    const eventSource = new EventSource(`${API_BASE}/logs/${sessionId}`);

    eventSource.onopen = function(event) {
      console.log("SSE connection opened:", event);
      appendLog("📡 Connected to live log stream");
    };

    eventSource.onmessage = function(event) {
      try {
        console.log("SSE message received:", event.data);
        const data = JSON.parse(event.data);

        switch(data.type) {
          case 'log':
            appendLog(data.message);
            break;

          case 'complete':
            appendLog("🎉 Analysis completed successfully!");
            updateStatus("success", "Complete!");

            // Show download buttons
            showDownloadOptions(sessionId);
            eventSource.close();
            break;

          case 'error':
            appendLog(`❌ Error: ${data.message}`);
            updateStatus("error", "Error");
            eventSource.close();
            break;

          case 'heartbeat':
            console.log("Heartbeat received");
            // Keep connection alive, no action needed
            break;

          default:
            console.log("Unknown message type:", data.type);
        }
      } catch (err) {
        console.error("Error parsing SSE message:", err, "Raw data:", event.data);
        appendLog(`⚠️ Error parsing message: ${event.data}`);
      }
    };

    eventSource.onerror = function(event) {
      console.error("SSE connection error:", event);
      console.log("EventSource readyState:", eventSource.readyState);

      if (eventSource.readyState === EventSource.CLOSED) {
        appendLog("❌ Connection closed by server");
      } else if (eventSource.readyState === EventSource.CONNECTING) {
        appendLog("🔄 Reconnecting to server...");
      } else {
        appendLog("❌ Connection to server lost");
        updateStatus("error", "Connection Error");
        eventSource.close();
      }
    };

  } catch (error) {
    console.error("Processing failed:", error);
    updateStatus("error", "Error");
    appendLog(`❌ Error: ${error.message}`);
  } finally {
    // Re-enable button
    runBtn.disabled = false;
    runBtn.classList.remove("loading");
  }
}

async function showDownloadOptions(sessionId) {
  try {
    appendLog("📥 Setting up download options...");

    // Show download section with main result
    const downloadSection = el("downloadSection");
    if (downloadSection) {
      downloadSection.style.display = "block";

      // Create main download button
      const mainDownloadBtn = document.createElement("button");
      mainDownloadBtn.className = "btn primary";
      mainDownloadBtn.innerHTML = "📊 Download Main Analysis";
      mainDownloadBtn.style.marginRight = "10px";
      mainDownloadBtn.onclick = () => downloadMainResult(sessionId);

      downloadSection.appendChild(mainDownloadBtn);
    }

    // Show debug files
    await showDebugFiles(sessionId);

  } catch (error) {
    console.error("Failed to show download options:", error);
    appendLog(`⚠️ Download options not available: ${error.message}`);
  }
}

async function downloadMainResult(sessionId) {
  try {
    appendLog(`📊 Downloading main analysis result...`);
    const resp = await fetch(`${API_BASE}/download/${sessionId}`);
    if (!resp.ok) {
      throw new Error(`Failed to download: ${resp.status}`);
    }

    const blob = await resp.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `ai_variance_analysis_${sessionId}.xlsx`;
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url);
    document.body.removeChild(a);

    appendLog(`✅ Main analysis downloaded successfully`);
  } catch (error) {
    console.error("Download failed:", error);
    appendLog(`❌ Failed to download main result: ${error.message}`);
  }
}

async function showDebugFiles(sessionId) {
  try {
    appendLog("📄 Fetching debug pipeline files...");
    const resp = await fetch(`${API_BASE}/debug/list/${sessionId}`);
    if (!resp.ok) {
      throw new Error(`Failed to fetch debug files: ${resp.status}`);
    }

    const data = await resp.json();
    if (data.files && data.files.length > 0) {
      appendLog(`📄 Debug files available: ${data.files.length} files`);

      // Show debug section in UI
      const debugSection = document.createElement("div");
      debugSection.className = "download-section";
      debugSection.style.marginTop = "20px";
      debugSection.innerHTML = `
        <h3>📄 Debug Pipeline Files</h3>
        <p>Download detailed processing pipeline data:</p>
        <div id="debugFilesList"></div>
      `;

      // Add to main content
      const downloadSection = el("downloadSection");
      if (downloadSection && downloadSection.parentNode) {
        downloadSection.parentNode.insertBefore(debugSection, downloadSection.nextSibling);
      }

      // Add individual debug file links
      const debugList = debugSection.querySelector("#debugFilesList");
      for (const file of data.files) {
        const fileDiv = document.createElement("div");
        fileDiv.style.marginBottom = "8px";
        fileDiv.innerHTML = `
          <button class="btn" onclick="downloadDebugFile('${file.key}', '${file.name}')" style="margin-right: 10px;">
            📊 ${file.name}
          </button>
          <span style="color: #666; font-size: 12px;">(${(file.size / 1024).toFixed(1)} KB)</span>
        `;
        debugList.appendChild(fileDiv);
      }

      appendLog("📄 Debug files ready for download (see below)");
    }
  } catch (error) {
    console.error("Failed to show debug files:", error);
    appendLog(`⚠️ Debug files not available: ${error.message}`);
  }
}

async function downloadDebugFile(fileKey, fileName) {
  try {
    appendLog(`📄 Downloading ${fileName}...`);
    const resp = await fetch(`${API_BASE}/debug/${fileKey}`);
    if (!resp.ok) {
      throw new Error(`Failed to download: ${resp.status}`);
    }

    const blob = await resp.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url);
    document.body.removeChild(a);

    appendLog(`✅ ${fileName} downloaded successfully`);
  } catch (error) {
    console.error("Download failed:", error);
    appendLog(`❌ Failed to download ${fileName}: ${error.message}`);
  }
}

// Event listeners
document.addEventListener("DOMContentLoaded", () => {
  const runBtn = el("runBtn");
  if (runBtn) {
    runBtn.addEventListener("click", processFiles);
  }
});