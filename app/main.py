# app/main.py
from __future__ import annotations

import io
import json
import sys
import threading
import queue
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
from contextlib import redirect_stdout, redirect_stderr

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .core import process_all, DEFAULT_CONFIG  # process_all returns analysis + debug files

# ---------------------------------------------------------------------
# App initialization
# ---------------------------------------------------------------------
app = FastAPI(title="Variance Analysis Tool API", version="1.0.0")

# Global storage for debug files (in-memory, for simplicity)
# In production, you might want to use Redis or a file storage service
debug_files_store: dict[str, tuple[str, bytes]] = {}

# Global storage for streaming logs
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
# Main processing endpoint (Excel in -> Excel out). No disk writes.
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

                with redirect_stdout(log_capture), redirect_stderr(log_capture):
                    print("🤖 Starting AI-only variance analysis...")
                    print(f"📤 Loaded {len(files)} Excel files for AI analysis")

                    # Use AI-only configuration (no user input needed)
                    CONFIG = {**DEFAULT_CONFIG}
                    CONFIG["use_llm_analysis"] = True  # Force AI mode
                    CONFIG["llm_model"] = "llama3.1"

                    print("🧠 AI will autonomously determine all thresholds and focus areas")

                    # Process with AI-only mode
                    xlsx_bytes, debug_files = process_all(
                        files=files,
                        CONFIG=CONFIG,
                    )

                    # Store results for download
                    for debug_name, debug_bytes in debug_files:
                        file_key = f"{session_id}_{debug_name}"
                        debug_files_store[file_key] = (debug_name, debug_bytes)

                    # Store main result
                    main_file_key = f"{session_id}_main_result"
                    debug_files_store[main_file_key] = (f"ai_variance_analysis_{session_id}.xlsx", xlsx_bytes)

                    print("✅ AI analysis complete!")
                    if debug_files:
                        print(f"📄 Debug files ready for download")

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
# Debug file download endpoints
# ---------------------------------------------------------------------

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


