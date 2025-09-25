# app/api/health.py
"""Health check endpoints."""

from fastapi import APIRouter, Depends
from ..models.analysis import HealthResponse
from ..core.config import get_settings, Settings

router = APIRouter(tags=["health"])

@router.get("/health", response_model=HealthResponse)
async def health_check(settings: Settings = Depends(get_settings)):
    """Health check endpoint."""
    return HealthResponse(
        status="ok",
        version=settings.app_version
    )