# app/core/exceptions.py
"""Custom exceptions and error handlers."""

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from typing import Union

class AnalysisError(Exception):
    """Custom exception for analysis errors."""
    def __init__(self, message: str, details: str = None):
        self.message = message
        self.details = details
        super().__init__(self.message)

class FileProcessingError(AnalysisError):
    """Exception for file processing errors."""
    pass

class ConfigurationError(AnalysisError):
    """Exception for configuration errors."""
    pass

class SessionError(AnalysisError):
    """Exception for session-related errors."""
    pass

# Error handlers
async def analysis_error_handler(request: Request, exc: AnalysisError) -> JSONResponse:
    """Handle custom analysis errors."""
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "error": exc.message,
            "details": exc.details,
            "type": exc.__class__.__name__
        }
    )

async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Handle validation errors."""
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "Validation error",
            "details": exc.errors(),
            "type": "ValidationError"
        }
    )

async def http_error_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Handle HTTP errors."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "type": "HTTPException"
        }
    )

async def general_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle general exceptions."""
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "details": str(exc),
            "type": exc.__class__.__name__
        }
    )