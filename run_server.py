#!/usr/bin/env python3
"""
Startup script for the Variance Analysis Tool API.

This script provides an easy way to start the server with proper configuration.
"""

import uvicorn
from app.core.config import get_settings
from app.utils.logging_config import setup_logging, get_logger

def main():
    """Start the FastAPI server."""
    settings = get_settings()

    # Setup logging before starting
    log_level = "DEBUG" if settings.debug else "INFO"
    setup_logging(level=log_level, log_file="logs/server.log")
    logger = get_logger(__name__)

    logger.info(f"🚀 Starting {settings.app_name} v{settings.app_version}")
    logger.info(f"📊 Debug mode: {'ON' if settings.debug else 'OFF'}")
    logger.info(f"🤖 AI model: {settings.llm_model}")
    logger.info(f"📁 Max file size: {settings.max_file_size / (1024*1024):.0f}MB")
    logger.info(f"⏱️  Session timeout: {settings.session_timeout_minutes} minutes")
    logger.info("🌐 Server will be available at:")
    logger.info("   • Main API: http://localhost:8000")
    logger.info("   • Health Check: http://localhost:8000/health")
    logger.info("   • API Docs: http://localhost:8000/docs")
    logger.info("   • ReDoc: http://localhost:8000/redoc")

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level="info" if not settings.debug else "debug",
        access_log=True
    )

if __name__ == "__main__":
    main()