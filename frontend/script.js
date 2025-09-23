// Additional JavaScript functionality for the merged variance analysis tool
console.log("Merged Variance Analysis Tool script loaded");

// =================== COMPREHENSIVE REVENUE ANALYSIS ===================

async function runComprehensiveRevenueAnalysis(file) {
  console.log("🔍 runComprehensiveRevenueAnalysis called with file:", file?.name || "no file");

  if (!file) {
    console.error("❌ No file provided to runComprehensiveRevenueAnalysis");
    return;
  }

  const resultsSection = document.getElementById("revenueResults");
  const contentDiv = document.getElementById("revenueContent");

  console.log("🔍 Found DOM elements:", {
    resultsSection: !!resultsSection,
    contentDiv: !!contentDiv
  });

  try {
    console.log("🔍 Running comprehensive revenue analysis...");

    const formData = new FormData();
    formData.append("excel_file", file);

    console.log("📤 Sending request to /analyze-comprehensive-revenue...");
    const response = await fetch("/analyze-comprehensive-revenue", {
      method: "POST",
      body: formData
    });

    console.log("📨 Revenue analysis response:", response.status, response.ok);
    const result = await response.json();
    console.log("📊 Revenue analysis result:", result);

    if (result.error) {
      throw new Error(result.error);
    }

    // Display comprehensive results
    console.log("🎨 Calling displayComprehensiveRevenueAnalysis...");
    displayComprehensiveRevenueAnalysis(result);

    if (resultsSection) {
      resultsSection.style.display = "block";
      console.log("✅ Revenue results section made visible");
    } else {
      console.error("❌ Revenue results section not found!");
    }

    console.log("✅ Comprehensive revenue analysis completed successfully.");

  } catch (error) {
    console.error("❌ Comprehensive revenue analysis error:", error);

    if (contentDiv) {
      contentDiv.innerHTML = `<div class="error">Revenue analysis failed: ${error.message}</div>`;
      console.log("❌ Error message displayed in content div");
    } else {
      console.error("❌ Content div not found for error display");
    }

    if (resultsSection) {
      resultsSection.style.display = "block";
      console.log("⚠️ Revenue results section made visible to show error");
    } else {
      console.error("❌ Revenue results section not found for error display!");
    }
  }
}

function displayComprehensiveRevenueAnalysis(data) {
  console.log("🎨 displayComprehensiveRevenueAnalysis called with data:", data);

  const contentDiv = document.getElementById("revenueContent");
  console.log("🔍 Content div found:", !!contentDiv);

  if (!contentDiv) {
    console.error("❌ revenueContent div not found!");
    return;
  }

  let html = "";
  console.log("🔧 Starting HTML generation...");

  // Summary section
  if (data.summary) {
    html += `
      <div class="analysis-section">
        <h4>📊 Executive Summary</h4>
        <div class="summary-stats">
          <div class="stat">
            <label>Subsidiary:</label>
            <span>${data.subsidiary || 'N/A'}</span>
          </div>
          <div class="stat">
            <label>Months Analyzed:</label>
            <span>${data.months_analyzed ? data.months_analyzed.length : 0} months</span>
          </div>
          <div class="stat">
            <label>Revenue Accounts:</label>
            <span>${data.summary.total_accounts || 0}</span>
          </div>
          <div class="stat">
            <label>Latest Total Revenue:</label>
            <span>${formatVND(data.summary.total_revenue_latest || 0)}</span>
          </div>
          <div class="stat">
            <label>Latest Gross Margin:</label>
            <span>${data.summary.gross_margin_latest ? data.summary.gross_margin_latest.toFixed(1) + '%' : 'N/A'}</span>
          </div>
        </div>
      </div>
    `;
  }

  // Risk Assessment (show first for visibility)
  if (data.risk_assessment && data.risk_assessment.length > 0) {
    html += `
      <div class="analysis-section">
        <h4>⚠️ Risk Assessment</h4>
        <div class="risk-periods">
    `;

    data.risk_assessment.forEach(risk => {
      const riskClass = risk.risk_level.toLowerCase();
      html += `
        <div class="risk-item ${riskClass}">
          <div class="risk-badge">${risk.risk_level}</div>
          <div class="risk-details">
            <strong>${risk.period}</strong><br>
            <span class="risk-type">${risk.type}</span><br>
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

  // Total revenue trend
  if (data.total_revenue_analysis && data.total_revenue_analysis.changes && data.total_revenue_analysis.changes.length > 0) {
    html += `
      <div class="analysis-section">
        <h4>📈 Total Revenue Trend (511*)</h4>
        <div class="revenue-changes">
    `;

    data.total_revenue_analysis.changes.forEach(change => {
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

  // Revenue by Account with Customer Impact Analysis
  if (data.revenue_by_account) {
    html += `
      <div class="analysis-section">
        <h4>🏦 Revenue by Account & Customer Impact</h4>
    `;

    Object.entries(data.revenue_by_account).forEach(([accountName, accountData]) => {
      if (accountData.biggest_change && Math.abs(accountData.biggest_change.change) > 1000000) {
        html += `
          <div class="account-analysis">
            <h5>${accountName}</h5>
            <div class="biggest-change">
              <strong>Biggest Change:</strong> ${accountData.biggest_change.from} → ${accountData.biggest_change.to}
              <br>
              <span class="${accountData.biggest_change.change >= 0 ? 'positive' : 'negative'}">
                ${accountData.biggest_change.change >= 0 ? '+' : ''}${formatVND(accountData.biggest_change.change)}
                (${accountData.biggest_change.pct_change >= 0 ? '+' : ''}${accountData.biggest_change.pct_change.toFixed(1)}%)
              </span>
            </div>
        `;

        if (accountData.customer_impacts && accountData.customer_impacts.length > 0) {
          html += `
            <div class="customer-impacts">
              <strong>Top Customer Impacts:</strong>
              <ul>
          `;

          accountData.customer_impacts.forEach(impact => {
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

  // Gross Margin Analysis
  if (data.gross_margin_analysis && data.gross_margin_analysis.trend && data.gross_margin_analysis.trend.length > 0) {
    html += `
      <div class="analysis-section">
        <h4>💰 Gross Margin Analysis</h4>
        <div class="margin-trend">
    `;

    data.gross_margin_analysis.trend.forEach((marginData, index) => {
      const prevMargin = index > 0 ? data.gross_margin_analysis.trend[index - 1].gross_margin_pct : null;
      let changeIndicator = '';
      if (prevMargin !== null) {
        const change = marginData.gross_margin_pct - prevMargin;
        const changeClass = change >= 0 ? 'positive' : 'negative';
        changeIndicator = `<span class="margin-change ${changeClass}">(${change >= 0 ? '+' : ''}${change.toFixed(1)}%)</span>`;
      }

      html += `
        <div class="margin-item">
          <div class="margin-month">${marginData.month}</div>
          <div class="margin-values">
            <div class="margin-pct">GM: ${marginData.gross_margin_pct.toFixed(1)}% ${changeIndicator}</div>
            <div class="margin-breakdown">
              Revenue: ${formatVND(marginData.revenue)}<br>
              Cost: ${formatVND(marginData.cost)}
            </div>
          </div>
        </div>
      `;
    });

    html += `
        </div>
      </div>
    `;
  }

  // Utility Analysis
  if (data.utility_analysis) {
    if (data.utility_analysis.available && data.utility_analysis.margins) {
      html += `
        <div class="analysis-section">
          <h4>⚡ Utility Revenue vs Cost Analysis</h4>
          <div class="utility-margins">
      `;

      data.utility_analysis.margins.forEach(margin => {
        const marginClass = margin.margin_pct >= 0 ? 'positive' : 'negative';
        html += `
          <div class="utility-item">
            <div class="utility-month">${margin.month}</div>
            <div class="utility-values">
              <div class="utility-margin ${marginClass}">Margin: ${margin.margin_pct.toFixed(1)}%</div>
              <div class="utility-breakdown">
                Revenue: ${formatVND(margin.revenue)}<br>
                Cost: ${formatVND(margin.cost)}
              </div>
            </div>
          </div>
        `;
      });

      html += `
          </div>
        </div>
      `;
    } else {
      html += `
        <div class="analysis-section">
          <h4>⚡ Utility Analysis</h4>
          <p class="no-data">Utility accounts not found in the data.</p>
        </div>
      `;
    }
  }

  console.log("📝 Generated HTML length:", html.length);
  console.log("🎨 Setting innerHTML...");
  contentDiv.innerHTML = html;
  console.log("✅ HTML set successfully!");
}

function formatVND(amount) {
  if (typeof amount !== 'number') return 'N/A';
  return new Intl.NumberFormat('vi-VN').format(Math.round(amount)) + ' VND';
}

// Enhanced Python analysis integration
function enhancePythonAnalysisWithRevenueAnalysis() {
  console.log("🔧 Setting up revenue analysis enhancement...");

  // Override the existing Python analysis function BEFORE it gets called
  const originalInitFunction = window.initializePythonAnalysis;

  // Store the enhanced version as a global function
  window.enhancedInitializePythonAnalysis = function() {
    console.log("🔧 Initializing ENHANCED Python analysis (custom version)...");

    // Get the Python run button
    const runBtn = document.getElementById("pythonRunBtn");
    const statusSpan = document.getElementById("pythonLiveStatus");
    const responseDiv = document.getElementById("pythonResponse");

    console.log("🔍 Found elements:", {
      runBtn: !!runBtn,
      statusSpan: !!statusSpan,
      responseDiv: !!responseDiv
    });

    if (runBtn) {
      console.log("✅ Found Python run button, replacing with enhanced functionality");

      // Remove ALL existing listeners by cloning the node
      const newRunBtn = runBtn.cloneNode(true);
      runBtn.parentNode.replaceChild(newRunBtn, runBtn);

      // Set up our enhanced click handler
      newRunBtn.addEventListener("click", async function(event) {
        event.preventDefault();
        console.log("🚀 ENHANCED Python analysis triggered!");

        const excelInput = document.getElementById("python-excel");
        if (!excelInput?.files?.length) {
          alert("Please select at least one Excel file");
          return;
        }

        console.log("📁 Files selected:", excelInput.files.length);

        newRunBtn.disabled = true;
        newRunBtn.textContent = "Processing...";
        statusSpan.textContent = "Processing…";
        responseDiv.innerHTML = "<p>🔄 Processing files...</p>";

        try {
          const fd = new FormData();

          // Add Excel files
          for (const file of excelInput.files) {
            fd.append("excel_files", file);
            console.log("📎 Added file:", file.name);
          }

          // Add mapping file if selected
          const mappingInput = document.getElementById("mapping");
          if (mappingInput?.files?.length > 0) {
            fd.append("mapping_file", mappingInput.files[0]);
            console.log("📎 Added mapping file:", mappingInput.files[0].name);
          }

          // Add configuration parameters
          const configFields = [
            'materiality_vnd', 'recurring_pct_threshold', 'revenue_opex_pct_threshold',
            'bs_pct_threshold', 'recurring_code_prefixes', 'min_trend_periods',
            'gm_drop_threshold_pct', 'dep_pct_only_prefixes'
          ];

          configFields.forEach(field => {
            const input = document.getElementById(field);
            if (input && input.value) {
              fd.append(field, input.value);
              console.log(`⚙️ Config ${field}:`, input.value);
            }
          });

          console.log("📤 Sending Python analysis request to /process...");
          const response = await fetch('/process', { method: "POST", body: fd });
          console.log("📨 Response received:", response.status, response.ok);

          if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = "variance_analysis_python.xlsx";
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);

            responseDiv.innerHTML = "<p>✅ Processing completed! File downloaded.</p>";
            statusSpan.textContent = "Done";

            // Now run comprehensive revenue analysis
            console.log("🔍 Starting comprehensive revenue analysis...");
            responseDiv.innerHTML += "<p>🔍 Running comprehensive revenue analysis...</p>";

            try {
              console.log("🔍 Calling runComprehensiveRevenueAnalysis with first file...");
              await runComprehensiveRevenueAnalysis(excelInput.files[0]);
              console.log("✅ Revenue analysis completed successfully!");
              responseDiv.innerHTML += "<p>✅ Revenue analysis completed!</p>";
            } catch (revenueError) {
              console.error("❌ Revenue analysis error:", revenueError);
              responseDiv.innerHTML += `<p>⚠️ Revenue analysis failed: ${revenueError.message}</p>`;
            }

          } else {
            const errorData = await response.json();
            console.error("❌ Python analysis failed:", errorData.error);
            responseDiv.innerHTML = `<p>❌ Error: ${errorData.error}</p>`;
            statusSpan.textContent = "Failed";
          }
        } catch (error) {
          console.error("❌ Python analysis error:", error);
          responseDiv.innerHTML = `<p>❌ Network error: ${error.message}</p>`;
          statusSpan.textContent = "Failed";
        } finally {
          newRunBtn.disabled = false;
          newRunBtn.textContent = "Process";
        }
      });

      console.log("✅ Enhanced event listener attached to new button");
    } else {
      console.error("❌ Python run button not found!");
    }
  };

  // Replace the original function
  window.initializePythonAnalysis = window.enhancedInitializePythonAnalysis;

  console.log("✅ Revenue analysis enhancement setup complete - initializePythonAnalysis replaced");
}

// Initialize when DOM is ready
document.addEventListener("DOMContentLoaded", () => {
  console.log("🔧 DOM loaded, setting up revenue analysis...");
  enhancePythonAnalysisWithRevenueAnalysis();

  // Also try to call it after a delay in case the original scripts load later
  setTimeout(() => {
    console.log("🔧 Delayed setup of revenue analysis...");
    enhancePythonAnalysisWithRevenueAnalysis();
  }, 1000);
});