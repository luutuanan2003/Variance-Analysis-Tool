# app/main.py
from __future__ import annotations

import io
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .core import process_all, DEFAULT_CONFIG, analyze_revenue_impact_from_bytes  # process_all returns a single .xlsx as bytes

# ---------------------------------------------------------------------
# App initialization
# ---------------------------------------------------------------------
app = FastAPI(title="Variance Analysis Tool API", version="1.0.0")

# ---------------------------------------------------------------------
# CORS (relax for prototype; tighten for prod)
# ---------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # change to your domain(s) in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------
# Frontend: serve index.html at "/" and static assets at "/frontend"
# ---------------------------------------------------------------------
FRONTEND_DIR = Path("frontend").resolve()
app.mount("/frontend", StaticFiles(directory=str(FRONTEND_DIR), html=False), name="frontend")

@app.get("/", response_class=HTMLResponse)
def serve_index():
    index = FRONTEND_DIR / "index.html"
    if not index.exists():
        return HTMLResponse("<h1>frontend/index.html not found</h1>", status_code=500)
    return HTMLResponse(index.read_text(encoding="utf-8"))

@app.get("/test", response_class=HTMLResponse)
def serve_test():
    test_file = Path("test.html")
    if not test_file.exists():
        return HTMLResponse("<h1>test.html not found</h1>", status_code=500)
    return HTMLResponse(test_file.read_text(encoding="utf-8"))

# ---------------------------------------------------------------------
# Health (simple)
# ---------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "version": getattr(app, "version", "unknown")}

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _split_list_string(s: Optional[str]) -> Optional[list[str]]:
    if s is None:
        return None
    raw = s.replace("|", ",")
    vals = [v.strip() for v in raw.split(",") if v.strip()]
    return vals or None

def _load_mapping_rules(mapping_bytes: bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Best-effort loader:
      - Tries to find a sheet for correlation rules by common names
      - Tries to find a sheet for seasonality rules by common names
      - If not found, returns empty DataFrames
    """
    try:
        xls = pd.ExcelFile(io.BytesIO(mapping_bytes))
        sheets_lower = {name.lower(): name for name in xls.sheet_names}

        # Heuristics for correlation rules
        corr_candidates = [
            "correlation", "correlations", "corr", "mapping", "rules", "correlation_rules"
        ]
        corr_name = next((sheets_lower[n] for n in corr_candidates if n in sheets_lower), None)

        # Heuristics for seasonality rules
        season_candidates = [
            "seasonality", "season", "season_rules", "seasonality_rules"
        ]
        season_name = next((sheets_lower[n] for n in season_candidates if n in sheets_lower), None)

        corr_df = pd.read_excel(xls, sheet_name=corr_name) if corr_name else pd.DataFrame()
        season_df = pd.read_excel(xls, sheet_name=season_name) if season_name else pd.DataFrame()
        return corr_df, season_df
    except Exception:
        return pd.DataFrame(), pd.DataFrame()

def _build_config_overrides(
    materiality_vnd: Optional[float],
    recurring_pct_threshold: Optional[float],
    revenue_opex_pct_threshold: Optional[float],
    bs_pct_threshold: Optional[float],
    recurring_code_prefixes: Optional[str],
    min_trend_periods: Optional[int],
    gm_drop_threshold_pct: Optional[float],
    dep_pct_only_prefixes: Optional[str],
    customer_column_hints: Optional[str],
) -> dict:
    cfg = {**DEFAULT_CONFIG}

    if materiality_vnd is not None:
        cfg["materiality_vnd"] = float(materiality_vnd)
    if recurring_pct_threshold is not None:
        cfg["recurring_pct_threshold"] = float(recurring_pct_threshold)
    if revenue_opex_pct_threshold is not None:
        cfg["revenue_opex_pct_threshold"] = float(revenue_opex_pct_threshold)
    if bs_pct_threshold is not None:
        cfg["bs_pct_threshold"] = float(bs_pct_threshold)
    if min_trend_periods is not None:
        cfg["min_trend_periods"] = int(min_trend_periods)
    if gm_drop_threshold_pct is not None:
        cfg["gm_drop_threshold_pct"] = float(gm_drop_threshold_pct)

    rc = _split_list_string(recurring_code_prefixes)
    if rc is not None:
        cfg["recurring_code_prefixes"] = rc

    dep = _split_list_string(dep_pct_only_prefixes)
    if dep is not None:
        cfg["dep_pct_only_prefixes"] = dep

    cust = _split_list_string(customer_column_hints)
    if cust is not None:
        cfg["customer_column_hints"] = cust

    return cfg

# ---------------------------------------------------------------------
# Main processing endpoint (Excel in -> Excel out). No disk writes.
# ---------------------------------------------------------------------
@app.post("/process")
async def process(
    excel_files: List[UploadFile] = File(...),
    mapping_file: Optional[UploadFile] = File(None),

    materiality_vnd: Optional[float] = Form(None),
    recurring_pct_threshold: Optional[float] = Form(None),
    revenue_opex_pct_threshold: Optional[float] = Form(None),
    bs_pct_threshold: Optional[float] = Form(None),
    recurring_code_prefixes: Optional[str] = Form(None),
    min_trend_periods: Optional[int] = Form(None),
    gm_drop_threshold_pct: Optional[float] = Form(None),   # NEW

    dep_pct_only_prefixes: Optional[str] = Form(None),      # NEW
    customer_column_hints: Optional[str] = Form(None),      # NEW
):
    try:
        # Read uploads fully into memory (no saving)
        files: List[Tuple[str, bytes]] = [
            (f.filename or "input.xlsx", await f.read())
            for f in excel_files
        ]

        # Optional mapping workbook -> correlation/seasonality DataFrames
        corr_rules: Optional[pd.DataFrame] = None
        season_rules: Optional[pd.DataFrame] = None
        if mapping_file is not None:
            mapping_bytes = await mapping_file.read()
            cdf, sdf = _load_mapping_rules(mapping_bytes)
            corr_rules = cdf if not cdf.empty else None
            season_rules = sdf if not sdf.empty else None

        # Build CONFIG overrides
        CONFIG = _build_config_overrides(
            materiality_vnd,
            recurring_pct_threshold,
            revenue_opex_pct_threshold,
            bs_pct_threshold,
            recurring_code_prefixes,
            min_trend_periods,
            gm_drop_threshold_pct,
            dep_pct_only_prefixes,
            customer_column_hints,
        )

        # Produce ONE Excel workbook (bytes)
        xlsx_bytes: bytes = process_all(
            files=files,
            corr_rules=corr_rules,
            season_rules=season_rules,
            CONFIG=CONFIG,
        )

        return StreamingResponse(
            iter([xlsx_bytes]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="variance_output.xlsx"'}
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ---------------------------------------------------------------------
# Revenue Impact Analysis endpoint
# ---------------------------------------------------------------------
@app.post("/analyze-revenue")
async def analyze_revenue(
    excel_file: UploadFile = File(...),
):
    """
    Analyze revenue impact from a single Excel file
    Returns JSON with detailed revenue analysis
    """
    try:
        # Read the uploaded file
        file_bytes = await excel_file.read()
        filename = excel_file.filename or "input.xlsx"

        # Run revenue impact analysis
        analysis_result = analyze_revenue_impact_from_bytes(file_bytes, filename)

        return JSONResponse(content=analysis_result)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
