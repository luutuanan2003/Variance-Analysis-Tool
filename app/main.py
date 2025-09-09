# app/main.py

from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from typing import List, Optional, Tuple
from pathlib import Path

from .core import process_all  # must return a single .xlsx as bytes

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

# ---------------------------------------------------------------------
# Health (simple, no filesystem checks)
# ---------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "version": app.version if hasattr(app, "version") else "unknown"}

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
):
    try:
        # Read uploads fully into memory (no saving)
        excel_blobs: List[Tuple[str, bytes]] = [
            (f.filename or "input.xlsx", await f.read())
            for f in excel_files
        ]
        mapping_blob: Optional[Tuple[str, bytes]] = None
        if mapping_file is not None:
            mapping_blob = (mapping_file.filename or "mapping.xlsx", await mapping_file.read())

        # Your core logic must return ONE Excel workbook as bytes
        xlsx_bytes: bytes = process_all(
            excel_blobs=excel_blobs,
            mapping_blob=mapping_blob,
            materiality_vnd=materiality_vnd,
            recurring_pct_threshold=recurring_pct_threshold,
            revenue_opex_pct_threshold=revenue_opex_pct_threshold,
            bs_pct_threshold=bs_pct_threshold,
            recurring_code_prefixes=recurring_code_prefixes,
            min_trend_periods=min_trend_periods,
        )

        return StreamingResponse(
            iter([xlsx_bytes]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="variance_output.xlsx"'}
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

