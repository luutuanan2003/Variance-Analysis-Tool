#!/usr/bin/env python3
"""
Startup script for the Variance Analysis Tool API.

This script provides an easy way to start the server with proper configuration.
"""

import uvicorn
from app.core.config import get_settings

def main():
    """Start the FastAPI server."""
    settings = get_settings()

    print(f"🚀 Starting {settings.app_name} v{settings.app_version}")
    print(f"📊 Debug mode: {'ON' if settings.debug else 'OFF'}")
    print(f"🤖 AI model: {settings.llm_model}")
    print(f"📁 Max file size: {settings.max_file_size / (1024*1024):.0f}MB")
    print(f"⏱️  Session timeout: {settings.session_timeout_minutes} minutes")
    print()
    print("🌐 Server will be available at:")
    print("   • Main API: http://localhost:8000")
    print("   • Health Check: http://localhost:8000/health")
    print("   • API Docs: http://localhost:8000/docs")
    print("   • ReDoc: http://localhost:8000/redoc")
    print()

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