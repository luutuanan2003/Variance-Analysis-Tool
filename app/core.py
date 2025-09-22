from __future__ import annotations

import io
import re
import warnings
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill, Font
from openpyxl.utils.dataframe import dataframe_to_rows

from .llm_analyzer import LLMFinancialAnalyzer

warnings.filterwarnings("ignore")  # Avoid noisy pandas dtype warnings in logs

# -----------------------------------------------------------------------------
# AI-ONLY VARIANCE ANALYSIS CORE MODULE
# -----------------------------------------------------------------------------
# This module provides the core functionality for AI-driven financial variance analysis.
# It processes Excel files containing Balance Sheet and P&L data, then uses LLM-based
# analysis to automatically detect anomalies and provide detailed explanations.
#
# === AUTONOMOUS AI APPROACH ===
# This system completely replaces traditional rule-based variance analysis with
# intelligent AI that can:
#
# 1. MATERIALITY DETERMINATION:
#    - Automatically calculates appropriate thresholds based on company size
#    - Small companies (<50B VND): 50M VND threshold
#    - Medium companies (50B-500B VND): 200M VND threshold
#    - Large companies (>500B VND): 1B VND threshold
#    - Explains reasoning for each threshold choice
#
# 2. INTELLIGENT ACCOUNT PRIORITIZATION:
#    - Revenue (511*): Highest priority - any unusual patterns flagged
#    - Utilities (627*, 641*): Operational expense monitoring
#    - Interest (515*, 635*): Financial health indicators
#    - Adapts % thresholds based on account volatility and business nature
#
# 3. BUSINESS CONTEXT UNDERSTANDING:
#    - Provides detailed explanations for every anomaly
#    - Considers Vietnamese business practices and accounting standards
#    - Suggests specific investigation steps and likely causes
#    - Cross-references account relationships (e.g., revenue vs utilities scaling)
#
# === WORKFLOW ===
# Upload Excel → AI Analyzes → Download Results
# No manual configuration, thresholds, or rules needed
#
# Vietnamese Chart of Accounts (VN CoA) Support:
# - 511*: Revenue accounts (primary focus)
# - 627*: Utilities expenses
# - 641*: Utilities expenses (alternative classification)
# - 515*: Financial income
# - 635*: Financial expenses
# - 632*: Cost of goods sold (COGS)
# - 217*: Accumulated depreciation
# - 214*: Accumulated depreciation (construction in progress)

# -----------------------------------------------------------------------------
# Configuration & Constants (AI-Only Mode)
# -----------------------------------------------------------------------------

DEFAULT_CONFIG: dict = {
    # Enable LLM-based anomaly detection (always True for AI-only mode)
    "use_llm_analysis": True,
    # LLM model to use for analysis (compatibility with existing code, actual model determined by .env)
    "llm_model": "gpt-4o",
}

MONTHS = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]

# Patterns to recognize header row content that carries a period like "As of Feb-2024" etc.
BS_PAT = re.compile(r'^\s*as\s*of\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[\.\-\s]*(\d{2,4})\s*$', re.I)
PL_PAT = re.compile(r'^\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[\.\-\s]*(\d{2,4})\s*$', re.I)

# -----------------------------------------------------------------------------
# Helpers: find optional “customer” column 
# -----------------------------------------------------------------------------
def find_customer_column(df: pd.DataFrame, CONFIG: dict) -> Optional[str]:
    """Try to find a customer dimension column in a cleaned PL frame.
    Heuristic: any column whose name contains one of the configured hints.
    """
    if df is None or df.empty:
        return None
    cols = [str(c).strip() for c in df.columns]
    hints = [h.lower() for h in CONFIG.get("customer_column_hints", [])]
    for c in cols:
        lc = c.lower()
        if any(h in lc for h in hints):
            return c
    return None


# -----------------------------------------------------------------------------
# Helpers: period parsing / ordering
# -----------------------------------------------------------------------------
def normalize_period_label(label: object) -> str:
    """Normalize various month-year formats into standardized 'Mon YYYY' format.

    This function handles multiple date formats commonly found in Vietnamese
    financial reports and Excel files, including:
    - \"As of Jan-2024\", \"Tính đến Feb 2024\"
    - \"01/2024\", \"2024/01\"
    - \"Jan 2024\", \"January 2024\"
    - Mixed Vietnamese and English date prefixes

    Used by AI analysis to ensure consistent period identification across
    different file formats and naming conventions.
    """
    if label is None:
        return ""
    s = str(label).strip()
    if s == "":
        return ""
    try:
        s_clean = re.sub(r'^\s*(as\s*of|tinh\s*den|tính\s*đến|den\s*ngay|đến\s*ngày)\s*', '', s, flags=re.I)

        m = re.search(r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[^\w]?[\s\-\.]*([12]\d{3}|\d{2})\b',
                      s_clean, flags=re.I)
        if m:
            mon, yr = m.group(1), m.group(2)
            yr = int(yr)
            yr = yr + 2000 if yr < 100 else yr
            return f"{mon.title()} {yr}"

        m = re.search(r'\b(1[0-2]|0?[1-9])[./\-](\d{4})\b', s_clean)
        if m:
            mon = int(m.group(1)); yr = int(m.group(2))
            return f"{MONTHS[mon-1].title()} {yr}"

        m = re.search(r'\b(\d{4})[./\-](1[0-2]|0?[1-9])\b', s_clean)
        if m:
            yr = int(m.group(1)); mon = int(m.group(2))
            return f"{MONTHS[mon-1].title()} {yr}"

        m_year = re.search(r'(20\d{2}|19\d{2})', s_clean)
        m_mon  = re.search(r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b', s_clean, flags=re.I)
        if m_year and m_mon:
            yr  = int(m_year.group(1))
            mon = m_mon.group(0)
            return f"{mon.title()} {yr}"
    except Exception:
        pass
    return s

def month_key(label: object) -> tuple[int, int]:
    """Return (year, month) for sorting. Unknown -> (9999, 99)."""
    n = normalize_period_label(label)
    m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+(\d{4})', n, re.I)
    if not m:
        return (9999, 99)
    y = int(m.group(2))
    mi = MONTHS.index(m.group(1).lower()) + 1
    return (y, mi)

# -----------------------------------------------------------------------------
# Excel reading / header detection / cleaning (IN-MEMORY)
# -----------------------------------------------------------------------------

def detect_header_row(xl_bytes: bytes, sheet: str) -> int:
    """Heuristically find the header row by scanning first ~40 rows for 'Financial row'."""
    try:
        probe = pd.read_excel(io.BytesIO(xl_bytes), sheet_name=sheet, header=None, nrows=40)
        for i in range(len(probe)):
            row_values = probe.iloc[i].astype(str).str.strip().str.lower()
            if any("financial row" in v for v in row_values):
                return i
    except Exception:
        pass
    return 0  # fallback to first row


def normalize_financial_col(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the main descriptor column is exactly 'Financial row'."""
    for c in df.columns:
        if str(c).strip().lower() == "financial row":
            return df.rename(columns={c: "Financial row"})
    # Otherwise assume first column is the descriptor
    return df.rename(columns={df.columns[0]: "Financial row"})

# It makes sure your DataFrame has the right column names (months/periods) and tells you which columns are months, 
# so you can analyze your data more easily.
def promote_row8(df: pd.DataFrame, mode: str, sub: str) -> tuple[pd.DataFrame, list[str]]:
    """Use the first data row as headers when period info is there; normalize month columns."""
    # Note: mode and sub parameters kept for interface consistency but not used
    _ = mode, sub  # Explicitly mark as unused
    if len(df) < 1:
        return df, []
    row0 = df.iloc[0]
    new_cols: list[str] = []
    for c in df.columns:
        v = str(row0.get(c, "")).strip()
        if BS_PAT.match(v) or PL_PAT.match(v):
            new_cols.append(normalize_period_label(v))
        else:
            new_cols.append(str(c))
    df = df.copy()
    df.columns = new_cols
    df = df.iloc[1:].reset_index(drop=True)

    month_cols: list[str] = []
    for c in df.columns:
        normalized = normalize_period_label(c)
        if re.match(r'^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+\d{4}$', normalized, re.I):
            month_cols.append(c)
    month_cols = sorted(month_cols, key=month_key)
    return df, month_cols

# It cleans up messy financial data so every row has a usable account code and name, and filters out non-data rows.
# This makes it much easier to analyze and aggregate financial information.
def fill_down_assign(df: pd.DataFrame) -> pd.DataFrame:
    """Extract account code / name from descriptor and forward-fill."""
    print(f"      🔄 Starting account code extraction and assignment...")
    print(f"         • Input rows: {len(df)}")

    ser = df["Financial row"].astype(str)

    code_extract = ser.str.extract(r'(\d{4,})', expand=False)
    name_extract = ser.str.replace(r'.*?(\d{4,})\s*[-:]*\s*', '', regex=True).str.strip()

    row_has_code     = code_extract.notna()
    is_total_word    = ser.str.strip().str.lower().str.startswith(("total","subtotal","cộng","tong","tổng"))
    is_total_with_code = is_total_word & row_has_code
    is_section       = ser.str.match(r'^\s*([IVX]+\.|[A-Z]\.)\s')
    is_empty         = ser.str.strip().eq("")

    # Log what we're finding
    print(f"         • Rows with account codes: {row_has_code.sum()}")
    print(f"         • Section headers to remove: {is_section.sum()}")
    print(f"         • Empty rows to remove: {is_empty.sum()}")
    print(f"         • Total rows: {is_total_word.sum()}")

    df["Account Code"] = code_extract.ffill()
    df["Account Name"] = name_extract.where(row_has_code).ffill()
    df["RowHadOwnCode"] = row_has_code
    df["IsTotal"] = is_total_with_code

    # Only discard section headers (like "I.", "A.") and completely empty rows
    # Keep all rows that have account codes (either original or forward-filled)
    keep_mask = ~(is_section | is_empty)
    df_after_basic_filter = df[keep_mask].copy()
    print(f"         • After removing headers/empty: {len(df_after_basic_filter)} rows")

    # Only filter out rows that still don't have account codes after forward-filling
    # This preserves data rows that belong to accounts
    df_final = df_after_basic_filter[df_after_basic_filter["Account Code"].notna()].copy()
    print(f"         • After removing unassigned: {len(df_final)} rows")
    print(f"      ✅ Account processing completed: {len(df)} → {len(df_final)} rows")

    return df_final

# It ensures that all your month columns contain clean, numeric data, even if the original 
# Excel file had messy formatting (like "1,000", "(200)", or extra spaces).
# This makes it much easier to do calculations and analysis on your financial data.
def coerce_numeric(df: pd.DataFrame, month_cols: list[str]) -> pd.DataFrame:
    """Coerce month columns to numeric."""
    out = df.copy()
    for c in month_cols:
        if c in out.columns:
            series = out[c].astype(str)
            series = (
                series
                .str.replace("\u00a0","", regex=False)    # nbsp
                .str.replace(",","", regex=False)
                .str.replace(r"\((.*)\)", r"-\1", regex=True)  # (100) -> -100
                .str.replace(r"[^0-9\.\-]", "", regex=True)
            )
            out[c] = pd.to_numeric(series, errors="coerce").fillna(0.0)
    return out

# It helps you get a clean summary of your financial data, with one row per account code and the correct totals for each month.
# This makes it much easier to analyze, report, or compare financial results.
def aggregate_totals(df: pd.DataFrame, month_cols: list[str]) -> pd.DataFrame:
    """Aggregate to a single row per account code; prefer explicit 'total' lines when present."""
    print(f"      🔄 Starting account aggregation...")
    print(f"         • Input detail rows: {len(df)}")

    if df.empty:
        return pd.DataFrame(columns=["Account Code","Account Name"] + month_cols)

    # Count unique account codes
    unique_accounts = df["Account Code"].nunique()
    print(f"         • Unique account codes found: {unique_accounts}")

    nm_src   = df[df["RowHadOwnCode"]] if "RowHadOwnCode" in df.columns else df
    name_map = (
        nm_src.dropna(subset=["Account Code"])[["Account Code","Account Name"]]
             .drop_duplicates("Account Code")
             .set_index("Account Code")["Account Name"]
    )

    totals_df = df[df.get("IsTotal", False)]
    codes_with_total = set(totals_df["Account Code"].dropna().astype(str).unique())
    print(f"         • Accounts with explicit totals: {len(codes_with_total)}")

    cols = ["Account Code"] + [c for c in month_cols if c in df.columns]
    parts = []

    if not totals_df.empty:
        total_agg = totals_df[cols].groupby("Account Code", as_index=False).sum()
        parts.append(total_agg)
        print(f"         • Aggregated from explicit totals: {len(total_agg)} accounts")

    no_total_df = df[~df["Account Code"].astype(str).isin(codes_with_total)]
    if not no_total_df.empty:
        detail_agg = no_total_df[cols].groupby("Account Code", as_index=False).sum()
        parts.append(detail_agg)
        print(f"         • Aggregated from detail rows: {len(detail_agg)} accounts")
        print(f"         • Detail rows aggregated: {len(no_total_df)} → {len(detail_agg)}")

    agg = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=cols)
    agg["Account Name"] = agg["Account Code"].map(name_map).fillna("")

    print(f"      ✅ Aggregation completed: {len(df)} detail rows → {len(agg)} final accounts")

    # Show some example account codes for reference
    if len(agg) > 0:
        sample_accounts = agg["Account Code"].head(5).tolist()
        print(f"         • Sample accounts: {', '.join(map(str, sample_accounts))}")

    return agg[["Account Code","Account Name"] + [c for c in month_cols if c in agg.columns]]

# -----------------------------------------------------------------------------
# MoM + trend signals
# -----------------------------------------------------------------------------

# It computes month-over-month changes and simple trend signals for your financial data.
# This helps you quickly see how each account is changing over time and identify any unusual trends.
def compute_mom_with_trends(df: pd.DataFrame, month_cols: list[str], CONFIG: dict) -> pd.DataFrame:
    """Compute MoM deltas and a simple rolling average signal."""
    if len(month_cols) < 2:
        return pd.DataFrame(columns=[
            "Account Code","Account Name","Prior","Current","Delta","Pct Change","Period",
            "Trend_3M_Avg","Trend_Deviation"
        ])

    out: list[pd.DataFrame] = []
    for i in range(1, len(month_cols)):
        cur, prev = month_cols[i], month_cols[i-1]
        if cur not in df.columns or prev not in df.columns:
            continue

        tmp = df[["Account Code","Account Name", prev, cur]].copy()
        tmp = tmp.rename(columns={prev: "Prior", cur: "Current"})
        tmp["Delta"] = tmp["Current"] - tmp["Prior"]
        tmp["Pct Change"] = np.where(tmp["Prior"] == 0, np.nan, tmp["Delta"] / tmp["Prior"])
        tmp["Period"] = normalize_period_label(cur)

        # simple trend window using preceding up-to-5 periods (require min_trend_periods)
        if i >= CONFIG["min_trend_periods"]:
            start_idx = max(0, i - 5)
            trend_cols = month_cols[start_idx:i]
            if len(trend_cols) >= CONFIG["min_trend_periods"]:
                trend_data = df[trend_cols]
                tmp["Trend_3M_Avg"] = trend_data.mean(axis=1)
                tmp["Trend_Deviation"] = tmp["Current"] - tmp["Trend_3M_Avg"]
            else:
                tmp["Trend_3M_Avg"] = np.nan
                tmp["Trend_Deviation"] = np.nan
        else:
            tmp["Trend_3M_Avg"] = np.nan
            tmp["Trend_Deviation"] = np.nan

        out.append(tmp)

    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=[
        "Account Code","Account Name","Prior","Current","Delta","Pct Change","Period",
        "Trend_3M_Avg","Trend_Deviation"
    ])

# -----------------------------------------------------------------------------
# AI-Powered Anomaly Detection
# -----------------------------------------------------------------------------
# This section contains the core AI-based anomaly detection functionality.
# Instead of using manual rules and thresholds, it leverages LLM analysis to:
# - Automatically determine materiality thresholds based on company size
# - Focus analysis on critical account types (Revenue, Utilities, Interest)
# - Provide detailed business reasoning for each detected anomaly
# - Apply Vietnamese accounting standards and business context

# Purpose:
# This function performs AI-driven anomaly detection on financial data for a single subsidiary.
# It replaces traditional rule-based approaches with intelligent LLM analysis that can:
# - Understand business context and company scale
# - Provide detailed explanations for each anomaly
# - Focus on the most critical account types
# - Adapt thresholds based on account nature and volatility
#
# The AI automatically prioritizes:
# 1. Revenue accounts (511*) - highest priority, any unusual patterns flagged
# 2. Utilities accounts (627*, 641*) - operational expense monitoring
# 3. Interest accounts (515*, 635*) - financial health indicators
#
# Returns:
# A DataFrame with standardized anomaly records containing AI explanations,
# materiality reasoning, and specific business recommendations.
def build_anoms_raw_excel(
    sub: str,
    excel_bytes: bytes,
    filename: str,
    CONFIG: dict,
) -> pd.DataFrame:
    """AI-only anomaly detection using complete raw Excel file.

    This function bypasses all data cleaning and processing, sending the complete
    raw Excel file directly to the AI for analysis. The AI will focus specifically
    on the 'BS Breakdown' and 'PL Breakdown' sheets while having access to the
    complete file context.

    Args:
        sub: Subsidiary/company name for analysis
        excel_bytes: Complete raw Excel file as bytes
        filename: Original filename for context
        CONFIG: Configuration dict containing LLM model settings

    Returns:
        DataFrame with anomaly records containing AI analysis and explanations
    """
    anomalies: list[dict] = []

    print(f"\n🧠 Starting RAW EXCEL AI analysis for '{sub}'...")
    try:
        llm_analyzer = LLMFinancialAnalyzer(CONFIG.get("llm_model", "gpt-4o"))
        print(f"✅ AI analyzer initialized with model: {CONFIG.get('llm_model', 'gpt-4o')}")

        print(f"\n🔍 Running AI analysis on complete raw Excel file...")
        llm_anomalies = llm_analyzer.analyze_raw_excel_file(excel_bytes, filename, sub, CONFIG)
        print(f"✅ Raw Excel AI analysis completed, processing {len(llm_anomalies)} results")

        print(f"\n📋 Converting {len(llm_anomalies)} AI results to report format...")
        for idx, anom in enumerate(llm_anomalies, 1):
            print(f"   • Processing anomaly {idx}: Account {anom.get('account_code', 'Unknown')}")
            anomalies.append({
                "Subsidiary": anom["subsidiary"],
                "Account": anom["account_code"],
                "Period": "Current",
                "Pct Change": anom["change_percent"],
                "Abs Change (VND)": int(anom["change_amount"]),
                "Trigger(s)": anom["rule_name"],
                "Suggested likely cause": anom["details"],
                "Status": "Raw Excel AI Analysis",
                "Notes": anom["details"],
            })
        print(f"✅ Successfully converted all AI results to report format")

        print(f"\n✅ Raw Excel AI anomaly detection completed for '{sub}' - returning {len(anomalies)} records")
        return pd.DataFrame(anomalies)

    except Exception as e:
        print(f"\n❌ Raw Excel AI analysis failed for '{sub}': {e}")
        error_record = pd.DataFrame([{
            "Subsidiary": sub,
            "Account": "RAW_EXCEL_AI_ERROR",
            "Period": "N/A",
            "Pct Change": 0,
            "Abs Change (VND)": 0,
            "Trigger(s)": "Raw Excel AI Analysis Failed",
            "Suggested likely cause": f"Raw Excel AI error: {str(e)[:100]}...",
            "Status": "Error",
            "Notes": "Check if OpenAI is running and model is available",
        }])
        print(f"⚠️  Returning error record to continue processing other files")
        return error_record

def build_anoms(
    sub: str,
    bs_data: pd.DataFrame, bs_cols: list[str],
    pl_data: pd.DataFrame, pl_cols: list[str],
    CONFIG: dict,
) -> pd.DataFrame:
    """AI-only anomaly detection for financial data.

    This function uses LLM-based analysis to automatically detect financial anomalies
    without requiring manual configuration. The AI determines appropriate materiality
    thresholds and focuses on critical Vietnamese Chart of Accounts categories.

    Args:
        sub: Subsidiary/company name for analysis
        bs_data: Balance Sheet DataFrame with account codes and monthly data
        bs_cols: List of month column names in Balance Sheet
        pl_data: Profit & Loss DataFrame with account codes and monthly data
        pl_cols: List of month column names in P&L
        CONFIG: Configuration dict containing LLM model settings

    Returns:
        DataFrame with anomaly records containing AI analysis and explanations

    AI Analysis Features:
        - Autonomous materiality determination (50M/200M/1B VND thresholds)
        - Revenue-first approach (511* accounts get highest priority)
        - Utilities correlation analysis (627*, 641* vs business activity)
        - Interest rate context (515*, 635* with market conditions)
        - Vietnamese business context and accounting standards
        - Detailed explanations for every decision and threshold
    """
    # Note: bs_cols and pl_cols kept for interface consistency but not used by AI
    _ = bs_cols, pl_cols  # AI analyzes DataFrame structure directly
    anomalies: list[dict] = []

    # === AI-POWERED ANALYSIS PIPELINE ===
    # Step 1: Initialize LLM analyzer with configured model
    # Step 2: Pass financial data to AI for autonomous analysis
    # Step 3: AI determines materiality, focuses on key accounts, provides explanations
    # Step 4: Convert AI results to standardized anomaly format

    print(f"\n🧠 Initializing AI analyzer for '{sub}'...")
    try:
        llm_analyzer = LLMFinancialAnalyzer(CONFIG.get("llm_model", "gpt-4o"))
        print(f"✅ AI analyzer initialized with model: {CONFIG.get('llm_model', 'gpt-4o')}")

        print(f"\n🔍 Running AI analysis on financial data...")
        llm_anomalies = llm_analyzer.analyze_financial_data(bs_data, pl_data, sub, CONFIG)
        print(f"✅ AI analysis completed, processing {len(llm_anomalies)} results")

        # Transform AI analysis results into standardized anomaly report format
        # Each anomaly includes:
        # - Account identification and classification
        # - AI-determined materiality threshold reasoning
        # - Detailed business context explanation
        # - Specific investigation recommendations

        print(f"\n📋 Converting {len(llm_anomalies)} AI results to report format...")
        for idx, anom in enumerate(llm_anomalies, 1):
            print(f"   • Processing anomaly {idx}: Account {anom.get('account_code', 'Unknown')}")
            anomalies.append({
                "Subsidiary": anom["subsidiary"],
                "Account": anom["account_code"],
                "Period": "Current",  # LLM doesn't specify period
                "Pct Change": anom["change_percent"],
                "Abs Change (VND)": int(anom["change_amount"]),
                "Trigger(s)": anom["rule_name"],
                "Suggested likely cause": anom["details"],  # Use the clean explanation
                "Status": "AI Analysis",
                "Notes": anom["details"],  # Keep same for compatibility
            })
        print(f"✅ Successfully converted all AI results to report format")

        print(f"\n✅ AI anomaly detection completed for '{sub}' - returning {len(anomalies)} records")
        return pd.DataFrame(anomalies)

    except Exception as e:
        print(f"\n❌ AI analysis failed for '{sub}': {e}")
        print(f"🔧 Troubleshooting suggestions:")
        print(f"   • Check OpenAI API key is valid in .env file")
        print(f"   • Verify OpenAI service status at https://status.openai.com/")
        print(f"   • Check model name: '{CONFIG.get('llm_model', 'gpt-4o')}'")

        # Graceful error handling: return informative error record instead of failing
        # This ensures the analysis continues for other subsidiaries even if AI fails
        error_record = pd.DataFrame([{
            "Subsidiary": sub,
            "Account": "AI_ERROR",
            "Period": "N/A",
            "Pct Change": 0,
            "Abs Change (VND)": 0,
            "Trigger(s)": "AI Analysis Failed",
            "Suggested likely cause": f"AI model error: {str(e)[:100]}...",
            "Status": "Error",
            "Notes": "Check if OpenAI is running and model is available",
        }])
        print(f"⚠️  Returning error record to continue processing other files")
        return error_record


# -----------------------------------------------------------------------------
# File-level processing (IN-MEMORY)
# -----------------------------------------------------------------------------

def process_financial_tab_from_bytes(
    xl_bytes: bytes,
    sheet_name: str,
    mode: str,
    subsidiary: str,
) -> tuple[pd.DataFrame, list[str], bytes]:
    """Load and clean one sheet ('BS Breakdown' or 'PL Breakdown') from in-memory bytes."""
    print(f"\n   🔧 Processing '{sheet_name}' sheet for '{subsidiary}'...")

    # Create debug workbook to save processing stages
    debug_wb = Workbook()
    debug_wb.remove(debug_wb.active)  # Remove default sheet
    stage_count = 1

    print(f"   📊 Detecting header row...")
    header_row = detect_header_row(xl_bytes, sheet_name)
    print(f"   ✅ Header row detected at line {header_row}")

    print(f"   📥 Reading Excel data...")
    df_raw = pd.read_excel(io.BytesIO(xl_bytes), sheet_name=sheet_name, header=header_row, dtype=str)
    print(f"   ✅ Read {len(df_raw)} rows, {len(df_raw.columns)} columns")

    # Stage 1: Raw data after header detection
    ws1 = debug_wb.create_sheet(f"Stage{stage_count}_Raw_Data")
    for r in dataframe_to_rows(df_raw, index=False, header=True):
        ws1.append(r)
    stage_count += 1

    print(f"   🔧 Normalizing financial columns...")
    df_normalized = normalize_financial_col(df_raw)

    # Stage 2: After column normalization
    ws2 = debug_wb.create_sheet(f"Stage{stage_count}_Normalized_Cols")
    for r in dataframe_to_rows(df_normalized, index=False, header=True):
        ws2.append(r)
    stage_count += 1

    print(f"   📅 Extracting period information...")
    df_periods, month_cols = promote_row8(df_normalized, mode, subsidiary)
    print(f"   ✅ Found {len(month_cols)} month columns: {', '.join(month_cols[:3])}{'...' if len(month_cols) > 3 else ''}")

    # Stage 3: After period extraction
    ws3 = debug_wb.create_sheet(f"Stage{stage_count}_Period_Extracted")
    for r in dataframe_to_rows(df_periods, index=False, header=True):
        ws3.append(r)
    stage_count += 1

    print(f"   🏷️ Processing account codes and names...")
    df_accounts = fill_down_assign(df_periods)

    # Stage 4: After account processing (this is where big reduction happens)
    ws4 = debug_wb.create_sheet(f"Stage{stage_count}_Account_Processed")
    for r in dataframe_to_rows(df_accounts, index=False, header=True):
        ws4.append(r)
    stage_count += 1

    print(f"   🔢 Converting to numeric values...")
    df_numeric = coerce_numeric(df_accounts, month_cols)

    # Stage 5: After numeric conversion
    ws5 = debug_wb.create_sheet(f"Stage{stage_count}_Numeric_Converted")
    for r in dataframe_to_rows(df_numeric, index=False, header=True):
        ws5.append(r)
    stage_count += 1

    print(f"   📋 Selecting final columns...")
    keep_cols = ["Account Code","Account Name","RowHadOwnCode","IsTotal"] + [c for c in month_cols if c in df_numeric.columns]
    df_selected = df_numeric[keep_cols]

    # Stage 6: After column selection
    ws6 = debug_wb.create_sheet(f"Stage{stage_count}_Columns_Selected")
    for r in dataframe_to_rows(df_selected, index=False, header=True):
        ws6.append(r)
    stage_count += 1

    print(f"   📊 Aggregating account totals...")
    totals = aggregate_totals(df_selected, month_cols)
    print(f"   ✅ Final aggregated data: {len(totals)} accounts, {len(month_cols)} periods")

    # Stage 7: Final aggregated data
    ws7 = debug_wb.create_sheet(f"Stage{stage_count}_Final_Aggregated")
    for r in dataframe_to_rows(totals, index=False, header=True):
        ws7.append(r)

    # Save debug workbook to memory for download
    debug_filename = f"data_pipeline_debug_{sheet_name.replace(' ', '_')}_{subsidiary.replace(' ', '_')[:20]}.xlsx"
    debug_bio = io.BytesIO()
    try:
        debug_wb.save(debug_bio)
        debug_bytes = debug_bio.getvalue()
        print(f"   📄 Debug pipeline prepared for download: {debug_filename}")
        print(f"       • Debug file size: {len(debug_bytes):,} bytes ({len(debug_bytes)/1024:.1f} KB)")
    except Exception as e:
        print(f"   ⚠️  Could not prepare debug file: {e}")
        debug_bytes = b""

    return totals, month_cols, debug_bytes


def extract_subsidiary_name_from_bytes(xl_bytes: bytes, fallback_filename: str) -> str:
    """Try to find a name on A2 of BS/PL sheets like 'Subsidiary: XYZ'. Fallback to filename stem."""
    print(f"   🔍 Checking Excel sheets for subsidiary name...")
    try:
        wb = load_workbook(io.BytesIO(xl_bytes), read_only=True, data_only=True)
        for sheet_name in ["BS Breakdown", "PL Breakdown"]:
            if sheet_name in wb.sheetnames:
                print(f"   📋 Checking '{sheet_name}' sheet cell A2...")
                sheet = wb[sheet_name]
                cell_value = sheet["A2"].value
                if isinstance(cell_value, str) and ":" in cell_value:
                    extracted_name = cell_value.split(":")[-1].strip()
                    print(f"   ✅ Found subsidiary name in sheet: '{extracted_name}'")
                    wb.close()
                    return extracted_name
        wb.close()
        print(f"   ⚠️  No subsidiary name found in Excel sheets")
    except Exception as e:
        print(f"   ⚠️  Error reading Excel for subsidiary name: {e}")

    # fallback: filename before first underscore or dot
    print(f"   📁 Using filename as fallback: '{fallback_filename}'")
    stem = fallback_filename.rsplit("/", 1)[-1]
    stem = stem.split("\\")[-1]
    stem = stem.split(".")[0]
    final_name = stem.split("_")[0] if "_" in stem else stem
    print(f"   ✅ Extracted name from filename: '{final_name}'")
    return final_name

# -----------------------------------------------------------------------------
# Excel formatting (IN-MEMORY, works on a worksheet not a saved file)
# -----------------------------------------------------------------------------

def apply_excel_formatting_ws(ws, anomaly_df: pd.DataFrame, CONFIG: dict) -> None:
    """Apply visual formatting to highlight AI analysis results.

    This function applies consistent visual formatting to the Excel output:
    - Blue header row with bold text for column titles
    - Light green fill for all AI-generated anomaly records
    - Professional appearance for business reporting

    The formatting is designed to:
    - Clearly distinguish AI-generated content
    - Maintain readability for financial analysis
    - Provide visual consistency across reports
    """
    # Note: anomaly_df and CONFIG kept for interface consistency but not used in simple formatting
    _ = anomaly_df, CONFIG  # Simple visual highlighting doesn't need data analysis
    try:
        # === COLOR SCHEME DEFINITION ===
        # Light blue for headers (professional business look)
        header_fill = PatternFill(start_color="CCE5FF", end_color="CCE5FF", fill_type="solid")
        # Light green for AI analysis content (indicates AI-generated)
        ai_fill = PatternFill(start_color="E8F5E8", end_color="E8F5E8", fill_type="solid")

        # === HEADER ROW FORMATTING ===
        # Apply blue background and bold font to column headers
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = Font(bold=True)

        # === AI CONTENT HIGHLIGHTING ===
        # Apply light green background to all AI analysis rows
        # This visually identifies content generated by AI analysis
        for row_idx in range(2, ws.max_row + 1):
            for col_idx in range(1, ws.max_column + 1):
                ws.cell(row=row_idx, column=col_idx).fill = ai_fill
    except Exception:
        # Formatting failures should never break the analysis pipeline
        # Continue processing even if Excel formatting encounters errors
        pass

# -----------------------------------------------------------------------------
# MAIN ORCHESTRATION FUNCTION (AI-ONLY MODE)
# -----------------------------------------------------------------------------
# This is the primary entry point for AI-driven variance analysis processing.
#
# Process Overview:
# 1. Accepts multiple Excel files containing financial data
# 2. Extracts and cleans Balance Sheet and P&L data for each subsidiary
# 3. Runs AI-powered anomaly detection (no manual rules)
# 4. Combines all AI analysis results into a single formatted Excel report
# 5. Applies visual formatting to highlight AI findings
#
# Key Features:
# - Completely autonomous - no manual configuration required
# - AI determines all materiality thresholds based on company size
# - Focuses on Vietnamese Chart of Accounts (511*, 627*, 641*, 515*, 635*)
# - Provides detailed business explanations for every anomaly
# - Processes multiple subsidiaries in a single batch
# - Returns formatted Excel file ready for download
#
# File Processing:
# - Expects Excel files with "BS Breakdown" and "PL Breakdown" sheets
# - Gracefully handles missing sheets (continues processing other data)
# - Extracts subsidiary names from file structure or content
# - Maintains data integrity through in-memory processing
def process_all(
    files: list[tuple[str, bytes]],
    CONFIG: dict = DEFAULT_CONFIG,
    progress_callback=None
) -> tuple[bytes, list[tuple[str, bytes]]]:
    print(f"\n🚀 ===== STARTING AI VARIANCE ANALYSIS PROCESSING =====\n")
    print(f"📥 Processing {len(files)} Excel file(s) for AI analysis")
    print(f"🤖 LLM Model: {CONFIG.get('llm_model', 'gpt-4o')}")
    print(f"🔧 AI-Only Mode: {CONFIG.get('use_llm_analysis', True)}")

    # === EXCEL WORKBOOK INITIALIZATION ===
    print(f"\n📊 Initializing Excel workbook for results...")
    wb = Workbook()
    ws = wb.active
    ws.title = "Anomalies Summary"
    all_anoms: list[pd.DataFrame] = []
    debug_files: list[tuple[str, bytes]] = []  # Store debug files for download
    print(f"✅ Excel workbook initialized successfully")

    # === MULTI-FILE PROCESSING LOOP ===
    print(f"\n🔄 Starting processing loop for {len(files)} file(s)...\n")

    for file_idx, (fname, xl_bytes) in enumerate(files, 1):
        # Calculate progress range for this file (30% to 80% of total)
        file_start = 30 + ((file_idx - 1) * 50 // len(files))
        file_end = 30 + (file_idx * 50 // len(files))

        if progress_callback:
            progress_callback(file_start, f"Processing file {file_idx}/{len(files)}: {fname}")

        print(f"\n📁 ===== PROCESSING FILE {file_idx}/{len(files)} =====\n")
        print(f"📄 File: {fname}")
        print(f"📏 File Size: {len(xl_bytes):,} bytes ({len(xl_bytes)/1024:.1f} KB)")

        if progress_callback:
            progress_callback(file_start + 2, f"Extracting subsidiary name from {fname}")

        print(f"\n🏢 Extracting subsidiary name...")
        sub = extract_subsidiary_name_from_bytes(xl_bytes, fname)
        print(f"✅ Subsidiary: '{sub}'")

        if progress_callback:
            progress_callback(file_start + 5, f"Validating Excel sheets for {sub}")

        print(f"\n🔍 Validating required Excel sheets...")
        try:
            # Just check that the required sheets exist, don't process them
            wb_check = load_workbook(io.BytesIO(xl_bytes), read_only=True)
            if "BS Breakdown" not in wb_check.sheetnames:
                raise ValueError(f"Required sheet 'BS Breakdown' not found in '{fname}'")
            if "PL Breakdown" not in wb_check.sheetnames:
                raise ValueError(f"Required sheet 'PL Breakdown' not found in '{fname}'")
            wb_check.close()
            print(f"✅ Required sheets validated: 'BS Breakdown' and 'PL Breakdown' found")
        except Exception as e:
            print(f"❌ Excel validation failed: {e}")
            raise ValueError(f"Cannot read Excel file '{fname}': {e}") from e

        # === RAW EXCEL AI ANALYSIS ===
        if progress_callback:
            progress_callback(file_start + 10, f"Starting AI analysis for {sub}")

        print(f"\n🤖 Starting RAW EXCEL AI analysis for '{sub}'...")
        print(f"📄 Passing complete raw Excel file to AI (no data cleaning)")
        print(f"🎯 AI will focus on 'BS Breakdown' and 'PL Breakdown' sheets")
        anoms = build_anoms_raw_excel(sub, xl_bytes, fname, CONFIG)

        if progress_callback:
            progress_callback(file_end - 5, f"AI analysis complete for {sub}")

        if anoms is not None and not anoms.empty:
            print(f"✅ AI analysis completed successfully")
            print(f"   • Anomalies detected: {len(anoms)}")
            if len(anoms) > 0:
                ai_status_count = anoms['Status'].value_counts().to_dict()
                for status, count in ai_status_count.items():
                    print(f"   • {status}: {count}")
            all_anoms.append(anoms)
        else:
            print(f"⚠️  No anomalies detected or AI analysis returned empty result")

        print(f"\n✅ File '{fname}' processing completed\n")

    # === CONSOLIDATION & EXCEL GENERATION ===
    print(f"\n📊 ===== CONSOLIDATING RESULTS =====\n")
    print(f"📈 Processed {len(files)} file(s) successfully")

    if all_anoms:
        print(f"🔗 Consolidating {len(all_anoms)} result set(s)...")
        anom_df = pd.concat(all_anoms, ignore_index=True)
        print(f"✅ Consolidation completed")
        print(f"   • Total anomalies: {len(anom_df)}")

        # Summary by subsidiary
        if len(anom_df) > 0:
            sub_summary = anom_df['Subsidiary'].value_counts()
            print(f"\n📋 Anomaly summary by subsidiary:")
            for sub, count in sub_summary.items():
                print(f"   • {sub}: {count} anomalies")

            status_summary = anom_df['Status'].value_counts()
            print(f"\n🔍 Analysis status summary:")
            for status, count in status_summary.items():
                print(f"   • {status}: {count}")
    else:
        print(f"⚠️  No anomalies detected across all files")
        anom_df = pd.DataFrame(columns=[
            "Subsidiary","Account","Period","Pct Change","Abs Change (VND)",
            "Trigger(s)","Suggested likely cause","Status","Notes"
        ])

    # === WRITE TO WORKSHEET ===
    print(f"\n📝 Writing results to Excel worksheet...")
    row_count = 0
    for r in dataframe_to_rows(anom_df, index=False, header=True):
        ws.append(r)
        row_count += 1
    print(f"✅ Written {row_count} rows to worksheet (including header)")

    # === VISUAL FORMATTING ===
    print(f"\n🎨 Applying visual formatting to Excel output...")
    apply_excel_formatting_ws(ws, anom_df, CONFIG)
    print(f"✅ Excel formatting applied successfully")

    # === RETURN BYTES ===
    print(f"\n💾 Generating final Excel file...")
    bio = io.BytesIO()
    wb.save(bio)
    final_size = len(bio.getvalue())
    print(f"✅ Excel file generated successfully")
    print(f"   • Output size: {final_size:,} bytes ({final_size/1024:.1f} KB)")

    print(f"\n📊 Debug Files Summary:")
    print(f"   • Debug files created: {len(debug_files)}")
    for debug_name, debug_bytes in debug_files:
        print(f"     - {debug_name}: {len(debug_bytes):,} bytes ({len(debug_bytes)/1024:.1f} KB)")

    print(f"\n🎉 ===== AI VARIANCE ANALYSIS COMPLETED =====\n")
    return bio.getvalue(), debug_files
