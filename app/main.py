# app/main.py

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from typing import List, Optional
from pathlib import Path
import shutil

# Project modules
from .models import ConfigOverrides, ProcessResult
from .core import process_all, DEFAULT_CONFIG

# ---------------------------------------------------------------------
# App initialization
# ---------------------------------------------------------------------
app = FastAPI(title="Variance Analysis Tool API", version="1.0.0")

# ---------------------------------------------------------------------
# CORS (relax for prototype; tighten for prod)
# ---------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # e.g., ["https://your-domain.com"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------
# Frontend: serve index.html at "/" and static assets at "/assets"
# ---------------------------------------------------------------------
FRONTEND_DIR = Path("frontend").resolve()

# Serve frontend/ as /frontend
app.mount("/frontend", StaticFiles(directory=str(FRONTEND_DIR), html=False), name="frontend")

# Root serves the SPA/HTML
@app.get("/", response_class=HTMLResponse)
def serve_index():
    index = FRONTEND_DIR / "index.html"
    if not index.exists():
        return HTMLResponse("<h1>frontend/index.html not found</h1>", status_code=500)
    return HTMLResponse(index.read_text(encoding="utf-8"))

# ---------------------------------------------------------------------
# Directory setup for processing
# ---------------------------------------------------------------------
BASE_DIR = Path(DEFAULT_CONFIG["base_dir"]).resolve()
IN_DIR = BASE_DIR / "input"
OUT_DIR = BASE_DIR / "output"
ARC_DIR = BASE_DIR / "archive"
LOGIC_DIR = BASE_DIR / "logic"
for d in (IN_DIR, OUT_DIR, ARC_DIR, LOGIC_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------
def _is_writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        test = path / ".write_test"
        with test.open("w") as f:
            f.write("ok")
        test.unlink(missing_ok=True)
        return True
    except Exception:
        return False

@app.get("/health")
def health():
    """
    Returns a structured health report so the frontend can show
    granular status instead of just 'ok'/'down'.
    """
    checks = {
        "app": "ok",
        "version": app.version if hasattr(app, "version") else "unknown",
        "frontend": {
            "index_exists": False,
        },
        "storage": {
            "base_dir": str(BASE_DIR),
            "input_exists": IN_DIR.exists(),
            "output_exists": OUT_DIR.exists(),
            "archive_exists": ARC_DIR.exists(),
            "logic_exists": LOGIC_DIR.exists(),
            "input_writable": _is_writable(IN_DIR),
            "output_writable": _is_writable(OUT_DIR),
            "archive_writable": _is_writable(ARC_DIR),
            "logic_writable": _is_writable(LOGIC_DIR),
        },
        "config": {
            "loaded": False,
            "base_dir": DEFAULT_CONFIG.get("base_dir", None),
            "materiality_vnd": DEFAULT_CONFIG.get("materiality_vnd", None),
        },
        "mapping": {
            "active_path": str(LOGIC_DIR / "Mapping_ACTIVE.xlsx"),
            "exists": (LOGIC_DIR / "Mapping_ACTIVE.xlsx").exists(),
        },
    }

    # Frontend index
    index = FRONTEND_DIR / "index.html"
    checks["frontend"]["index_exists"] = index.exists()

    # Config sanity
    # replace the config sanity block in /health with this:
    try:
        # Only check that it's a dict; don't force JSON serialization
        checks["config"]["loaded"] = isinstance(DEFAULT_CONFIG, dict)
        # Optional: show a JSON-serializable view for debugging
        def _safe(v):
            try:
                json.dumps(v)
                return v
            except Exception:
                return str(v)
        checks["config"]["preview"] = {k: _safe(v) for k, v in DEFAULT_CONFIG.items()}
    except Exception:
        checks["config"]["loaded"] = False


    # Overall status
    ok = (
        checks["frontend"]["index_exists"]
        and all([
            checks["storage"]["input_exists"],
            checks["storage"]["output_exists"],
            checks["storage"]["archive_exists"],
            checks["storage"]["logic_exists"],
            checks["storage"]["input_writable"],
            checks["storage"]["output_writable"],
            checks["storage"]["archive_writable"],
            checks["storage"]["logic_writable"],
            checks["config"]["loaded"],
        ])
    )

    status = "ok" if ok else "degraded"
    return JSONResponse({"status": status, "checks": checks})

# ---------------------------------------------------------------------
# Quick single-file processing (for Postman/testing)
# ---------------------------------------------------------------------
@app.post("/process-basic")
async def process_file(file: UploadFile = File(...)):
    try:
        input_path = IN_DIR / file.filename
        with input_path.open("wb") as f:
            f.write(await file.read())

        overrides = DEFAULT_CONFIG.copy()
        overrides["base_dir"] = str(BASE_DIR)

        result = process_all(overrides)
        return JSONResponse({
            "status": "success",
            "total_anomalies": result["total_anomalies"],
            "generated_files": result["generated_files"],
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# ---------------------------------------------------------------------
# Upload Mapping_ACTIVE.xlsx
# ---------------------------------------------------------------------
@app.post("/upload-mapping")
async def upload_mapping(file: UploadFile = File(...)):
    dest = LOGIC_DIR / "Mapping_ACTIVE.xlsx"
    with dest.open("wb") as f:
        shutil.copyfileobj(file.file, f)
    return {"message": "Mapping uploaded", "path": str(dest)}

# ---------------------------------------------------------------------
# Main processing endpoint (multiple files + overrides)
# ---------------------------------------------------------------------
@app.post("/process", response_model=ProcessResult)
async def process_endpoint(
    excel_files: List[UploadFile] = File(..., description="One or more *.xlsx files"),
    mapping_file: Optional[UploadFile] = File(None),
    base_dir: Optional[str] = Form(None),
    materiality_vnd: Optional[int] = Form(None),
    recurring_pct_threshold: Optional[float] = Form(None),
    revenue_opex_pct_threshold: Optional[float] = Form(None),
    bs_pct_threshold: Optional[float] = Form(None),
    archive_processed: Optional[bool] = Form(None),
    recurring_code_prefixes: Optional[str] = Form(None),  # comma-separated
    min_trend_periods: Optional[int] = Form(None),
):
    # Save mapping if provided
    if mapping_file is not None:
        dest = LOGIC_DIR / "Mapping_ACTIVE.xlsx"
        with dest.open("wb") as f:
            shutil.copyfileobj(mapping_file.file, f)

    # Save Excel files
    for uf in excel_files:
        if not uf.filename.lower().endswith(".xlsx"):
            raise HTTPException(status_code=400, detail=f"Invalid file type: {uf.filename}")
        dest = IN_DIR / uf.filename
        with dest.open("wb") as f:
            shutil.copyfileobj(uf.file, f)

    # Build overrides
    overrides: dict = {}
    if base_dir is not None:
        overrides["base_dir"] = base_dir
    if materiality_vnd is not None:
        overrides["materiality_vnd"] = materiality_vnd
    if recurring_pct_threshold is not None:
        overrides["recurring_pct_threshold"] = recurring_pct_threshold
    if revenue_opex_pct_threshold is not None:
        overrides["revenue_opex_pct_threshold"] = revenue_opex_pct_threshold
    if bs_pct_threshold is not None:
        overrides["bs_pct_threshold"] = bs_pct_threshold
    if archive_processed is not None:
        overrides["archive_processed"] = archive_processed
    if recurring_code_prefixes is not None:
        lst = [s.strip() for s in recurring_code_prefixes.split(",") if s.strip()]
        if lst:
            overrides["recurring_code_prefixes"] = lst
    if min_trend_periods is not None:
        overrides["min_trend_periods"] = min_trend_periods

    try:
        result = process_all(overrides)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return ProcessResult(
        message="Processing complete",
        total_anomalies=result["total_anomalies"],
        per_subsidiary=result["per_subsidiary"],
        generated_files=result["generated_files"],
    )

# ---------------------------------------------------------------------
# Outputs list + download
# ---------------------------------------------------------------------
@app.get("/outputs")
def list_outputs():
    files = sorted([p.name for p in OUT_DIR.glob("*.xlsx")])
    return {"files": files}

@app.get("/download/{filename}")
def download_file(filename: str):
    file_path = OUT_DIR / filename
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(str(file_path), filename=filename)
