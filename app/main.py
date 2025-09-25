# app/main.py
"""
FastAPI application with clean architecture.

This is the restructured version following FastAPI best practices:
- Proper separation of concerns
- Dependency injection
- Error handling
- Configuration management
- Service layer architecture
"""

from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.exceptions import RequestValidationError

from .core.config import get_settings
from .core.exceptions import (
    AnalysisError, analysis_error_handler, validation_error_handler,
    http_error_handler, general_error_handler
)
from .api import health, analysis
from .services.analysis_service import analysis_service

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    settings = get_settings()
    print(f"🚀 Starting {settings.app_name} v{settings.app_version}")

    # Setup periodic cleanup for old sessions
    import asyncio
    from datetime import timedelta

    async def cleanup_sessions():
        """Periodic cleanup of old sessions."""
        while True:
            try:
                analysis_service.cleanup_old_sessions(
                    max_age_minutes=settings.session_timeout_minutes
                )
                await asyncio.sleep(300)  # Run every 5 minutes
            except Exception as e:
                print(f"Session cleanup error: {e}")
                await asyncio.sleep(60)  # Retry after 1 minute

    # Start cleanup task
    cleanup_task = asyncio.create_task(cleanup_sessions())

    yield

    # Shutdown
    print("🛑 Shutting down application...")
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass

def create_application() -> FastAPI:
    """Create and configure FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        debug=settings.debug,
        lifespan=lifespan,
        docs_url="/docs" if settings.debug else None,
        redoc_url="/redoc" if settings.debug else None,
    )

    # Configure CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=settings.cors_methods,
        allow_headers=settings.cors_headers,
    )

    # Register exception handlers
    app.add_exception_handler(AnalysisError, analysis_error_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.add_exception_handler(Exception, general_error_handler)

    # Include routers
    app.include_router(health.router)
    app.include_router(analysis.router)

    # Serve frontend static files
    frontend_dir = Path("frontend").resolve()
    if frontend_dir.exists():
        app.mount("/frontend", StaticFiles(directory=str(frontend_dir), html=False), name="frontend")

        @app.get("/", response_class=HTMLResponse)
        async def serve_index():
            """Serve the frontend index.html."""
            index_file = frontend_dir / "index.html"
            if not index_file.exists():
                return HTMLResponse(
                    "<h1>Frontend not found</h1><p>Place your frontend files in the 'frontend' directory.</p>",
                    status_code=404
                )
            return HTMLResponse(index_file.read_text(encoding="utf-8"))
    else:
        @app.get("/", response_class=HTMLResponse)
        async def serve_index():
            """Serve a simple API info page."""
            return HTMLResponse(f"""
                <html>
                    <head><title>{settings.app_name}</title></head>
                    <body>
                        <h1>{settings.app_name}</h1>
                        <p>Version: {settings.app_version}</p>
                        <p>API Documentation: <a href="/docs">/docs</a></p>
                        <p>Alternative Documentation: <a href="/redoc">/redoc</a></p>
                        <p>Health Check: <a href="/health">/health</a></p>
                    </body>
                </html>
            """)

    return app

# Create the application
app = create_application()

# For backward compatibility, add some legacy routes
from .analysis.revenue_analysis import (
    analyze_revenue_impact_from_bytes,
    analyze_comprehensive_revenue_impact_from_bytes,
    analyze_comprehensive_revenue_impact_ai
)

@app.post("/analyze-revenue")
async def analyze_revenue_legacy(excel_file):
    """Legacy revenue analysis endpoint."""
    # This can redirect to the new endpoint or maintain compatibility
    from .api.analysis import analyze_revenue_variance
    return await analyze_revenue_variance(excel_file)

# Add backward compatibility for /process endpoint
from fastapi import UploadFile, File, Form
from typing import List, Optional

@app.post("/process")
async def process_legacy(
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
    """Legacy /process endpoint - redirects to /api/process."""
    from .api.analysis import process_python_analysis
    from .core.config import get_settings
    return await process_python_analysis(
        excel_files=excel_files,
        mapping_file=mapping_file,
        materiality_vnd=materiality_vnd,
        recurring_pct_threshold=recurring_pct_threshold,
        revenue_opex_pct_threshold=revenue_opex_pct_threshold,
        bs_pct_threshold=bs_pct_threshold,
        recurring_code_prefixes=recurring_code_prefixes,
        min_trend_periods=min_trend_periods,
        gm_drop_threshold_pct=gm_drop_threshold_pct,
        dep_pct_only_prefixes=dep_pct_only_prefixes,
        customer_column_hints=customer_column_hints,
        settings=get_settings()
    )

if __name__ == "__main__":
    import uvicorn
    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level="info"
    )