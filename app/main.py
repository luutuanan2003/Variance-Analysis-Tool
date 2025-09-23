# app/main.py
from __future__ import annotations

import io
import json
import sys
import threading
import queue
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional
from contextlib import redirect_stdout, redirect_stderr

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .core import process_all, DEFAULT_CONFIG, analyze_revenue_impact_from_bytes, analyze_comprehensive_revenue_impact_from_bytes, analyze_comprehensive_revenue_impact_ai

# ---------------------------------------------------------------------
# App initialization
# ---------------------------------------------------------------------
app = FastAPI(title="Variance Analysis Tool API", version="1.0.0")

# Global storage for debug files (in-memory, for simplicity)
debug_files_store: dict[str, tuple[str, bytes]] = {}

# Global storage for streaming logs (for AI analysis)
log_streams: dict[str, queue.Queue] = {}

class LogCapture:
    """Custom class to capture all print output and stream it to frontend."""
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.queue = queue.Queue()
        log_streams[session_id] = self.queue

    def write(self, message: str):
        if message.strip():  # Only send non-empty messages
            self.queue.put(message.strip())
        # Also write to original stdout for server logs
        sys.__stdout__.write(message)

    def flush(self):
        sys.__stdout__.flush()

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

# ---------------------------------------------------------------------
# Health (simple)
# ---------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "version": getattr(app, "version", "unknown")}

# ---------------------------------------------------------------------
# Helpers for Python analysis
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
# Python Analysis endpoint (Excel in -> Excel out). No disk writes.
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
    gm_drop_threshold_pct: Optional[float] = Form(None),

    dep_pct_only_prefixes: Optional[str] = Form(None),
    customer_column_hints: Optional[str] = Form(None),
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

        # Build CONFIG overrides for Python analysis
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

        # Disable LLM analysis for Python mode
        CONFIG["use_llm_analysis"] = False

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
# AI Analysis endpoint (streaming with progress)
# ---------------------------------------------------------------------
@app.post("/start_analysis")
async def start_analysis(
    excel_files: List[UploadFile] = File(...),
):
    """Start AI analysis and return session ID for log streaming."""
    try:
        # Create session ID
        session_id = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]  # Include milliseconds

        # Read uploads fully into memory
        files: List[Tuple[str, bytes]] = [
            (f.filename or "input.xlsx", await f.read())
            for f in excel_files
        ]

        # Start processing in background thread
        def run_analysis():
            log_capture = LogCapture(session_id)
            try:
                # Test that log capture works
                log_capture.write("🔧 Log capture initialized successfully")

                def progress_update(percentage, message):
                    log_capture.queue.put(f"__PROGRESS__{percentage}__{message}")

                with redirect_stdout(log_capture), redirect_stderr(log_capture):
                    progress_update(10, "Starting AI variance analysis...")
                    print("🤖 Starting AI-only variance analysis...")

                    progress_update(15, "Loading Excel files...")
                    print(f"📤 Loaded {len(files)} Excel files for AI analysis")

                    progress_update(20, "Configuring AI analysis settings...")
                    # Use AI-only configuration (no user input needed)
                    CONFIG = {**DEFAULT_CONFIG}
                    CONFIG["use_llm_analysis"] = True  # Force AI mode
                    CONFIG["llm_model"] = "gpt-4o"

                    progress_update(25, "AI determining thresholds and focus areas...")
                    print("🧠 AI will autonomously determine all thresholds and focus areas")

                    progress_update(30, "Beginning AI analysis of financial data...")
                    # Process with AI-only mode
                    xlsx_bytes, debug_files = process_all(
                        files=files,
                        CONFIG=CONFIG,
                        progress_callback=progress_update
                    )

                    progress_update(85, "AI analysis complete, storing results...")
                    # Store results for download
                    for debug_name, debug_bytes in debug_files:
                        file_key = f"{session_id}_{debug_name}"
                        debug_files_store[file_key] = (debug_name, debug_bytes)

                    progress_update(90, "Preparing main analysis file...")
                    # Store main result
                    main_file_key = f"{session_id}_main_result"
                    debug_files_store[main_file_key] = (f"ai_variance_analysis_{session_id}.xlsx", xlsx_bytes)

                    progress_update(95, "Finalizing results...")
                    print("✅ AI analysis complete!")
                    if debug_files:
                        print(f"📄 Debug files ready for download")

                    progress_update(100, "Analysis complete - ready for download!")
                    # Signal completion
                    log_capture.queue.put("__ANALYSIS_COMPLETE__")

            except Exception as e:
                print(f"❌ AI processing error: {e}")
                log_capture.queue.put(f"__ERROR__{str(e)}")

        # Start background thread
        thread = threading.Thread(target=run_analysis)
        thread.daemon = True
        thread.start()

        return {"session_id": session_id, "status": "started"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start analysis: {str(e)}")

@app.get("/logs/{session_id}")
async def stream_logs(session_id: str):
    """Stream logs for a session using Server-Sent Events."""
    def generate():
        if session_id not in log_streams:
            yield f"data: {json.dumps({'error': 'Session not found'})}\n\n"
            return

        log_queue = log_streams[session_id]

        # Send initial connection confirmation
        yield f"data: {json.dumps({'type': 'log', 'message': '📡 SSE connection established'})}\n\n"

        while True:
            try:
                # Wait for new log message (with timeout)
                message = log_queue.get(timeout=1)

                if message == "__ANALYSIS_COMPLETE__":
                    yield f"data: {json.dumps({'type': 'complete', 'message': 'Analysis completed successfully'})}\n\n"
                    break
                elif message.startswith("__ERROR__"):
                    error_msg = message[9:]  # Remove __ERROR__ prefix
                    yield f"data: {json.dumps({'type': 'error', 'message': error_msg})}\n\n"
                    break
                elif message.startswith("__PROGRESS__"):
                    # Parse progress message: __PROGRESS__<percentage>__<message>
                    parts = message.split("__")
                    if len(parts) >= 4:
                        try:
                            percentage = int(parts[2])
                            progress_msg = parts[3]
                            yield f"data: {json.dumps({'type': 'progress', 'percentage': percentage, 'message': progress_msg})}\n\n"
                        except ValueError:
                            # If parsing fails, treat as regular log
                            yield f"data: {json.dumps({'type': 'log', 'message': message})}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'log', 'message': message})}\n\n"
                else:
                    yield f"data: {json.dumps({'type': 'log', 'message': message})}\n\n"

            except queue.Empty:
                # Send heartbeat to keep connection alive
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
                continue

        # Cleanup
        if session_id in log_streams:
            del log_streams[session_id]

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "*",
        }
    )

# ---------------------------------------------------------------------
# Revenue Impact Analysis endpoint (from Python tool)
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

# ---------------------------------------------------------------------
# Comprehensive Revenue Impact Analysis endpoint
# ---------------------------------------------------------------------
@app.post("/analyze-comprehensive-revenue")
async def analyze_comprehensive_revenue(
    excel_file: UploadFile = File(...),
):
    """
    Comprehensive Revenue Impact Analysis
    Answers specific questions:
    1. If revenue increases (511*), which specific revenue accounts drive the increase?
    2. Which customers/entities drive the revenue changes for each account?
    3. Gross margin analysis: (Revenue - Cost)/Revenue and risk identification
    4. Utility revenue vs cost pairing analysis
    """
    try:
        # Read the uploaded file
        file_bytes = await excel_file.read()
        filename = excel_file.filename or "input.xlsx"

        # Run comprehensive revenue impact analysis
        analysis_result = analyze_comprehensive_revenue_impact_from_bytes(file_bytes, filename)

        return JSONResponse(content=analysis_result)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ---------------------------------------------------------------------
# Download endpoints (for AI analysis)
# ---------------------------------------------------------------------
@app.get("/download/{session_id}")
async def download_main_result(session_id: str):
    """Download the main analysis result."""
    main_file_key = f"{session_id}_main_result"
    if main_file_key not in debug_files_store:
        raise HTTPException(status_code=404, detail=f"Main result for session '{session_id}' not found")

    filename, file_bytes = debug_files_store[main_file_key]

    return StreamingResponse(
        io.BytesIO(file_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.get("/debug/{file_key}")
async def download_debug_file(file_key: str):
    """Download a debug pipeline file by its key."""
    if file_key not in debug_files_store:
        raise HTTPException(status_code=404, detail=f"Debug file '{file_key}' not found")

    original_name, file_bytes = debug_files_store[file_key]

    return StreamingResponse(
        io.BytesIO(file_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={original_name}"}
    )

@app.get("/debug/list/{session_id}")
async def list_debug_files(session_id: str):
    """List all debug files for a session."""
    session_files = []
    for key, (name, file_bytes) in debug_files_store.items():
        if key.startswith(session_id + "_"):
            session_files.append({
                "key": key,
                "name": name,
                "size": len(file_bytes),
                "download_url": f"/debug/{key}"
            })

    return {"session_id": session_id, "files": session_files}

# ---------------------------------------------------------------------
# AI Comprehensive Revenue Impact Analysis endpoint
# ---------------------------------------------------------------------
@app.post("/analyze-comprehensive-revenue-ai")
async def analyze_comprehensive_revenue_ai_endpoint(
    excel_file: UploadFile = File(...),
):
    """
    AI-Powered Comprehensive Revenue Impact Analysis
    Uses enhanced AI prompts to provide detailed analysis matching core.py functionality:
    1. Total revenue trend analysis (511* accounts)
    2. Individual revenue account breakdowns with entity impacts
    3. SG&A 641* account analysis with entity-level variances
    4. SG&A 642* account analysis with entity-level variances
    5. Combined SG&A ratio analysis (% of revenue)
    6. Entity-level impact identification for all material changes
    """
    try:
        # Read the uploaded file
        file_bytes = await excel_file.read()
        filename = excel_file.filename or "input.xlsx"

        # Extract subsidiary name from filename for AI analysis
        subsidiary = filename.replace('.xlsx', '').replace('.xls', '').replace('_', ' ').title()

        # Run AI comprehensive revenue impact analysis
        print(f"\n🤖 Starting AI comprehensive revenue analysis for {filename}")
        analysis_result = analyze_comprehensive_revenue_impact_ai(file_bytes, filename, subsidiary)

        print(f"✅ AI comprehensive revenue analysis completed")
        return JSONResponse(content=analysis_result)

    except Exception as e:
        print(f"❌ AI comprehensive revenue analysis failed: {str(e)}")
        return JSONResponse(status_code=500, content={"error": str(e)})