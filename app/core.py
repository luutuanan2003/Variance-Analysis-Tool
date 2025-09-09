# app/core.py
"""
Core data-processing logic for the Variance Analysis Tool (stateless, in-memory).

This module:
- Normalizes messy period labels (e.g., "As of Mar-24" -> "Mar 2024")
- Reads BS / PL tabs from uploaded Excel files (from in-memory bytes)
- Cleans and aggregates rows to account-level series
- Computes month-over-month deltas + simple trend signals
- Applies anomaly rules (materiality + % thresholds + correlation breaks)
- Builds ONE consolidated Excel workbook in memory and returns its bytes
"""

from __future__ import annotations

import io
import re
import datetime as dt
import warnings
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill, Font
from openpyxl.utils.dataframe import dataframe_to_rows

warnings.filterwarnings("ignore")  # Avoid noisy pandas dtype warnings in logs

# -----------------------------------------------------------------------------
# Defaults & constants (NO base_dir, NO archive flags)
# -----------------------------------------------------------------------------

DEFAULT_CONFIG: dict = {
    "materiality_vnd": 1_000_000_000,      # absolute VND change threshold
    "recurring_pct_threshold": 0.05,       # 5% for recurring P/L accounts
    "revenue_opex_pct_threshold": 0.10,    # 10% for revenue/opex accounts
    "bs_pct_threshold": 0.05,              # 5% for balance sheet
    "recurring_code_prefixes": ["6321", "635", "515"],
    "min_trend_periods": 3,
}

MONTHS = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]

# Patterns to recognize header row content that carries a period like "As of Feb-2024" etc.
BS_PAT = re.compile(r'^\s*as\s*of\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[\.\-\s]*(\d{2,4})\s*$', re.I)
PL_PAT = re.compile(r'^\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[\.\-\s]*(\d{2,4})\s*$', re.I)

# -----------------------------------------------------------------------------
# Helpers: period parsing / ordering
# -----------------------------------------------------------------------------

def normalize_period_label(label: object) -> str:
    """Turn many month-year formats into 'Mon YYYY'."""
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


def promote_row8(df: pd.DataFrame, mode: str, sub: str) -> tuple[pd.DataFrame, list[str]]:
    """Use the first data row as headers when period info is there; normalize month columns."""
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


def fill_down_assign(df: pd.DataFrame) -> pd.DataFrame:
    """Extract account code / name from descriptor and forward-fill."""
    ser = df["Financial row"].astype(str)

    code_extract = ser.str.extract(r'(\d{4,})', expand=False)
    name_extract = ser.str.replace(r'.*?(\d{4,})\s*[-:]*\s*', '', regex=True).str.strip()

    row_has_code     = code_extract.notna()
    is_total_word    = ser.str.strip().str.lower().str.startswith(("total","subtotal","cộng","tong","tổng"))
    is_total_with_code = is_total_word & row_has_code
    is_section       = ser.str.match(r'^\s*([IVX]+\.|[A-Z]\.)\s')
    is_empty         = ser.str.strip().eq("")

    df["Account Code"] = code_extract.ffill()
    df["Account Name"] = name_extract.where(row_has_code).ffill()
    df["RowHadOwnCode"] = row_has_code
    df["IsTotal"] = is_total_with_code

    keep_mask = ~(is_section | is_empty)
    df = df[keep_mask & df["Account Code"].notna()].copy()
    return df


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


def aggregate_totals(df: pd.DataFrame, month_cols: list[str]) -> pd.DataFrame:
    """Aggregate to a single row per account code; prefer explicit 'total' lines when present."""
    if df.empty:
        return pd.DataFrame(columns=["Account Code","Account Name"] + month_cols)

    nm_src   = df[df["RowHadOwnCode"]] if "RowHadOwnCode" in df.columns else df
    name_map = (
        nm_src.dropna(subset=["Account Code"])[["Account Code","Account Name"]]
             .drop_duplicates("Account Code")
             .set_index("Account Code")["Account Name"]
    )

    totals_df = df[df.get("IsTotal", False)]
    codes_with_total = set(totals_df["Account Code"].dropna().astype(str).unique())

    cols = ["Account Code"] + [c for c in month_cols if c in df.columns]
    parts = []
    if not totals_df.empty:
        parts.append(totals_df[cols].groupby("Account Code", as_index=False).sum())

    no_total_df = df[~df["Account Code"].astype(str).isin(codes_with_total)]
    if not no_total_df.empty:
        parts.append(no_total_df[cols].groupby("Account Code", as_index=False).sum())

    agg = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=cols)
    agg["Account Name"] = agg["Account Code"].map(name_map).fillna("")
    return agg[["Account Code","Account Name"] + [c for c in month_cols if c in agg.columns]]

# -----------------------------------------------------------------------------
# MoM + trend signals
# -----------------------------------------------------------------------------

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
# Rules & anomaly builders
# -----------------------------------------------------------------------------

def classify_pl_account(code: object, CONFIG: dict) -> str:
    """Return 'Recurring' if code starts with any configured prefix; else 'Revenue/OPEX'."""
    code_str = str(code)
    return "Recurring" if any(code_str.startswith(p) for p in CONFIG["recurring_code_prefixes"]) else "Revenue/OPEX"


def get_threshold_cause(statement: str, code: object, CONFIG: dict) -> str:
    """Human-friendly cause suggestion based on statement type and code classification."""
    if statement == "BS":
        return "Balance changed materially — check reclass/missing offset."
    return ("Recurring moved — check accruals/timing."
            if classify_pl_account(code, CONFIG) == "Recurring"
            else "Revenue/OPEX moved — check billing/cut-off.")


def match_codes(series: pd.Series, pattern_str: str | float | int | None) -> pd.Series:
    """Return boolean mask for 'code patterns' like '111*,112*|515'."""
    if pd.isna(pattern_str) or pattern_str == "":
        return pd.Series(False, index=series.index)
    patterns = [p.strip() for p in str(pattern_str).split("|") if p.strip()]
    mask = pd.Series(False, index=series.index)
    for pattern in patterns:
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            mask |= series.astype(str).str.startswith(prefix)
        else:
            mask |= (series.astype(str) == pattern)
    return mask


def build_corr_anoms(
    sub: str,
    combined: pd.DataFrame,
    corr_rules: pd.DataFrame,
    periods: list[str],
    materiality: int,
) -> list[dict]:
    """Correlation-rule anomalies; flags when left/right deltas break expected direction."""
    items: list[dict] = []

    cols = {c.lower(): c for c in corr_rules.columns}
    def pick(opts: list[str]) -> str | None:
        for n in opts:
            if n in cols:
                return cols[n]
        return None

    left_col  = pick(["left_patterns","left_pattern","left_patter","left"])
    right_col = pick(["right_patterns","right_pattern","right_patter","right"])
    cause_col = pick(["cause_message","cause","message","notes"])
    type_col  = pick(["relation_type","type","direction"])

    for _, rule in corr_rules.iterrows():
        lp = str(rule[left_col]) if left_col else ""
        rp = str(rule[right_col]) if right_col else ""
        cause = str(rule[cause_col]) if cause_col else "Correlation mismatch"
        rel = str(rule.get(type_col, "directional")).strip().lower() if type_col else "directional"
        inverse = rel in ("inverse","opposite","neg","negative")
        if not lp or not rp:
            continue

        for per in periods:
            mom = combined[combined["Norm_Period"] == per]
            if mom.empty:
                continue

            l = mom[match_codes(mom["Account Code"], lp)]["Delta"].sum()
            r = mom[match_codes(mom["Account Code"], rp)]["Delta"].sum()

            ok = ((l > 0 and r < 0) or (l < 0 and r > 0)) if inverse else ((l > 0 and r > 0) or (l < 0 and r < 0))
            if abs(l) >= materiality and not ok:
                items.append({
                    "Subsidiary": sub,
                    "Account": f"{lp} ↔ {rp}",
                    "Period": per,
                    "Pct Change": "",
                    "Abs Change (VND)": int(l),
                    "Trigger(s)": "Correlation break",
                    "Suggested likely cause": cause,
                    "Status": "Needs Review",
                    "Notes": f"Left Δ={int(l):,}, Right Δ={int(r):,}, relation={'inverse' if inverse else 'directional'}",
                })
    return items


def build_anoms(
    sub: str,
    bs_data: pd.DataFrame, bs_cols: list[str],
    pl_data: pd.DataFrame, pl_cols: list[str],
    corr_rules: pd.DataFrame, season_rules: pd.DataFrame,
    CONFIG: dict,
) -> pd.DataFrame:
    """Apply all anomaly rules to one subsidiary's cleaned BS/PL frames."""
    anomalies: list[dict] = []
    materiality = CONFIG["materiality_vnd"]

    bs_mom = compute_mom_with_trends(bs_data, bs_cols, CONFIG)
    pl_mom = compute_mom_with_trends(pl_data, pl_cols, CONFIG)

    # Balance Sheet rule
    for _, row in bs_mom.iterrows():
        abs_delta = abs(row["Delta"])
        pct_change = row["Pct Change"]
        if (abs_delta >= materiality and pd.notna(pct_change) and abs(pct_change) > CONFIG["bs_pct_threshold"]):
            anomalies.append({
                "Subsidiary": sub,
                "Account": f'{row["Account Code"]}-{row["Account Name"]}',
                "Period": row["Period"],
                "Pct Change": round(pct_change * 100, 2),
                "Abs Change (VND)": int(row["Delta"]),
                "Trigger(s)": "BS >5% & ≥1B",
                "Suggested likely cause": get_threshold_cause("BS", row["Account Code"], CONFIG),
                "Status": "Needs Review",
                "Notes": "",
            })

    # P/L rules split: Recurring vs Revenue/OPEX
    for _, row in pl_mom.iterrows():
        abs_delta = abs(row["Delta"])
        pct_change = row["Pct Change"]
        account_class = classify_pl_account(row["Account Code"], CONFIG)
        trigger = ""

        if account_class == "Recurring":
            if (abs_delta >= materiality and pd.notna(pct_change) and abs(pct_change) > CONFIG["recurring_pct_threshold"]):
                trigger = "Recurring >5% & ≥1B"
        else:
            if ((pd.notna(pct_change) and abs(pct_change) > CONFIG["revenue_opex_pct_threshold"]) or abs_delta >= materiality):
                trigger = "Revenue/OPEX >10% or ≥1B"

        if trigger:
            anomalies.append({
                "Subsidiary": sub,
                "Account": f'{row["Account Code"]}-{row["Account Name"]}',
                "Period": row["Period"],
                "Pct Change": round(pct_change * 100, 2) if pd.notna(pct_change) else "",
                "Abs Change (VND)": int(row["Delta"]),
                "Trigger(s)": trigger,
                "Suggested likely cause": get_threshold_cause("PL", row["Account Code"], CONFIG),
                "Status": "Needs Review",
                "Notes": "",
            })

    # Correlation rules (optional)
    combined = pd.concat([
        bs_mom[["Account Code","Period","Delta"]],
        pl_mom[["Account Code","Period","Delta"]],
    ], ignore_index=True)
    combined["Norm_Period"] = combined["Period"].astype(str).map(normalize_period_label)
    periods = sorted(set(combined["Norm_Period"]), key=month_key)
    anomalies.extend(build_corr_anoms(sub, combined, corr_rules, periods, materiality))

    return pd.DataFrame(anomalies)

# -----------------------------------------------------------------------------
# File-level processing (IN-MEMORY)
# -----------------------------------------------------------------------------

def process_financial_tab_from_bytes(
    xl_bytes: bytes,
    sheet_name: str,
    mode: str,
    subsidiary: str,
) -> tuple[pd.DataFrame, list[str]]:
    """Load and clean one sheet ('BS Breakdown' or 'PL Breakdown') from in-memory bytes."""
    header_row = detect_header_row(xl_bytes, sheet_name)
    df = pd.read_excel(io.BytesIO(xl_bytes), sheet_name=sheet_name, header=header_row, dtype=str)
    df = normalize_financial_col(df)
    df, month_cols = promote_row8(df, mode, subsidiary)
    df = fill_down_assign(df)
    df = coerce_numeric(df, month_cols)
    keep_cols = ["Account Code","Account Name","RowHadOwnCode","IsTotal"] + [c for c in month_cols if c in df.columns]
    df = df[keep_cols]
    totals = aggregate_totals(df, month_cols)
    return totals, month_cols


def extract_subsidiary_name_from_bytes(xl_bytes: bytes, fallback_filename: str) -> str:
    """Try to find a name on A2 of BS/PL sheets like 'Subsidiary: XYZ'. Fallback to filename stem."""
    try:
        wb = load_workbook(io.BytesIO(xl_bytes), read_only=True, data_only=True)
        for sheet_name in ["BS Breakdown", "PL Breakdown"]:
            if sheet_name in wb.sheetnames:
                sheet = wb[sheet_name]
                cell_value = sheet["A2"].value
                if isinstance(cell_value, str) and ":" in cell_value:
                    wb.close()
                    return cell_value.split(":")[-1].strip()
        wb.close()
    except Exception:
        pass
    # fallback: filename before first underscore or dot
    stem = fallback_filename.rsplit("/", 1)[-1]
    stem = stem.split("\\")[-1]
    stem = stem.split(".")[0]
    return stem.split("_")[0] if "_" in stem else stem

# -----------------------------------------------------------------------------
# Excel formatting (IN-MEMORY, works on a worksheet not a saved file)
# -----------------------------------------------------------------------------

def apply_excel_formatting_ws(ws, anomaly_df: pd.DataFrame, CONFIG: dict) -> None:
    """Apply simple conditional fills directly on the 'Anomalies Summary' worksheet."""
    try:
        critical_fill = PatternFill(start_color="FFCCCC", end_color="FFCCCC", fill_type="solid")
        warning_fill  = PatternFill(start_color="FFFF99", end_color="FFFF99", fill_type="solid")
        header_fill   = PatternFill(start_color="CCE5FF", end_color="CCE5FF", fill_type="solid")

        # Header
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = Font(bold=True)

        # Find indexes for columns we care about
        headers = [c.value for c in ws[1]]
        try:
            abs_idx = headers.index("Abs Change (VND)") + 1
            trig_idx = headers.index("Trigger(s)") + 1
        except ValueError:
            # If columns not found, skip formatting
            return

        # Rows
        for row_idx in range(2, ws.max_row + 1):
            try:
                abs_change = ws.cell(row=row_idx, column=abs_idx).value or 0
                trigger    = str(ws.cell(row=row_idx, column=trig_idx).value or "")
                fill = None
                if abs_change >= DEFAULT_CONFIG["materiality_vnd"] * 5:
                    fill = critical_fill
                elif "Correlation break" in trigger or abs_change >= DEFAULT_CONFIG["materiality_vnd"] * 2:
                    fill = warning_fill
                if fill:
                    for col_idx in range(1, len(headers) + 1):
                        ws.cell(row=row_idx, column=col_idx).fill = fill
            except Exception:
                continue
    except Exception:
        # Formatting should never break the pipeline
        pass

# -----------------------------------------------------------------------------
# Orchestration (IN-MEMORY): Excel in -> ONE Excel bytes out
# -----------------------------------------------------------------------------

def process_all(
    *,
    excel_blobs: List[Tuple[str, bytes]],
    mapping_blob: Optional[Tuple[str, bytes]] = None,
    materiality_vnd: Optional[float] = None,
    recurring_pct_threshold: Optional[float] = None,
    revenue_opex_pct_threshold: Optional[float] = None,
    bs_pct_threshold: Optional[float] = None,
    recurring_code_prefixes: Optional[str] = None,
    min_trend_periods: Optional[int] = None,
) -> bytes:
    """
    Read all uploaded Excels (in-memory), run rules, produce ONE workbook with:
      - 'Anomalies Summary' (all subsidiaries)
      - Optional per-subsidiary BS/PL cleaned sheets (best-effort if present)
    Return workbook bytes. NO filesystem interaction.
    """
    # Build runtime config overrides safely
    CONFIG = DEFAULT_CONFIG.copy()
    if materiality_vnd is not None:
        CONFIG["materiality_vnd"] = float(materiality_vnd)
    if recurring_pct_threshold is not None:
        CONFIG["recurring_pct_threshold"] = float(recurring_pct_threshold)
    if revenue_opex_pct_threshold is not None:
        CONFIG["revenue_opex_pct_threshold"] = float(revenue_opex_pct_threshold)
    if bs_pct_threshold is not None:
        CONFIG["bs_pct_threshold"] = float(bs_pct_threshold)
    if min_trend_periods is not None:
        CONFIG["min_trend_periods"] = int(min_trend_periods)
    if recurring_code_prefixes:
        # allow "6321,635,515" or "6321|635|515"
        parts = [p.strip() for p in str(recurring_code_prefixes).replace("|", ",").split(",") if p.strip()]
        if parts:
            CONFIG["recurring_code_prefixes"] = parts

    # Optional rules from mapping (if provided) — best-effort read
    corr_rules = pd.DataFrame()
    season_rules = pd.DataFrame()
    if mapping_blob:
        try:
            xls = pd.ExcelFile(io.BytesIO(mapping_blob[1]))
            if "Correlation Rules" in xls.sheet_names:
                corr_rules = pd.read_excel(xls, sheet_name="Correlation Rules")
            if "Seasonality Rules" in xls.sheet_names:
                season_rules = pd.read_excel(xls, sheet_name="Seasonality Rules")
        except Exception:
            pass

    all_anoms: list[pd.DataFrame] = []
    per_subsidiary_outputs: list[tuple[str, pd.DataFrame, pd.DataFrame]] = []

    for fname, fbytes in excel_blobs:
        sub = extract_subsidiary_name_from_bytes(fbytes, fname)

        # Try to read BS/PL breakdowns (skip gracefully if sheets missing)
        bs_df, bs_cols = pd.DataFrame(), []
        pl_df, pl_cols = pd.DataFrame(), []
        try:
            bs_df, bs_cols = process_financial_tab_from_bytes(fbytes, "BS Breakdown", mode="BS", subsidiary=sub)
        except Exception:
            pass
        try:
            pl_df, pl_cols = process_financial_tab_from_bytes(fbytes, "PL Breakdown", mode="PL", subsidiary=sub)
        except Exception:
            pass

        # If both empty, attempt to read the first sheet as a generic table (fallback)
        if bs_df.empty and pl_df.empty:
            try:
                tmp = pd.read_excel(io.BytesIO(fbytes))
                tmp = tmp if isinstance(tmp, pd.DataFrame) else pd.DataFrame(tmp)
                # Create a simple MoM-like diff if we can detect 2+ numeric columns
                num_cols = tmp.select_dtypes(include=[np.number]).columns.tolist()
                if len(num_cols) >= 2:
                    pl_df = tmp.copy()
                    pl_cols = num_cols[:2]
            except Exception:
                pass

        # Build anomalies
        anoms = build_anoms(sub, bs_df, bs_cols, pl_df, pl_cols, corr_rules, season_rules, CONFIG)
        if not anoms.empty:
            all_anoms.append(anoms)

        per_subsidiary_outputs.append((sub, bs_df, pl_df))

    # ------------------------
    # Build the output workbook
    # ------------------------
    wb = Workbook()
    # a) Anomalies Summary
    if all_anoms:
        summary = pd.concat(all_anoms, ignore_index=True)
    else:
        summary = pd.DataFrame(columns=[
            "Subsidiary","Account","Period","Pct Change","Abs Change (VND)",
            "Trigger(s)","Suggested likely cause","Status","Notes"
        ])
    ws_summary = wb.active
    ws_summary.title = "Anomalies Summary"
    for r in dataframe_to_rows(summary, index=False, header=True):
        ws_summary.append(r)
    apply_excel_formatting_ws(ws_summary, summary, CONFIG)

    # b) Per-subsidiary cleaned sheets (BS / PL)
    for sub, bs_df, pl_df in per_subsidiary_outputs:
        if not bs_df.empty:
            ws = wb.create_sheet(title=f"{sub[:22]}_BS")  # Excel sheet name limit
            for r in dataframe_to_rows(bs_df, index=False, header=True):
                ws.append(r)
        if not pl_df.empty:
            ws = wb.create_sheet(title=f"{sub[:22]}_PL")
            for r in dataframe_to_rows(pl_df, index=False, header=True):
                ws.append(r)

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()
