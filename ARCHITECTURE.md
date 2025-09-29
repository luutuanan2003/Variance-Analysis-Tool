# FastAPI Application Architecture

## Overview

The Variance Analysis Tool has been restructured to follow FastAPI best practices with clean architecture principles, comprehensive error handling, structured logging, and unified configuration management. This provides better maintainability, testability, scalability, and production readiness.

## Directory Structure

```
app/
├── main.py                     # ✨ Application entry point & middleware stack
├── api/                        # 🛣️  API route handlers
│   ├── health.py              # Health check endpoints with config monitoring
│   └── analysis.py            # Analysis endpoints with validation
├── core/                       # 🏛️  Core application components
│   ├── config.py              # Legacy configuration (backward compatibility)
│   ├── unified_config.py      # 🔧 Unified Pydantic v2 configuration system
│   ├── dependencies.py        # Dependency injection
│   └── exceptions.py          # 🛡️ Custom exceptions & error handlers
├── models/                     # 📋 Pydantic models
│   └── analysis.py            # Request/response models with validation
├── services/                   # 🔧 Business logic layer
│   ├── analysis_service.py    # Analysis business logic with session management
│   └── processing_service.py  # Core processing orchestration
├── middleware/                 # 🛡️ Request/response middleware
│   ├── __init__.py            # Middleware exports
│   ├── validation_middleware.py # Request validation & security headers
│   └── config_middleware.py   # Configuration health monitoring
├── utils/                      # 🛠️  Utility functions
│   ├── helpers.py             # Helper functions
│   ├── log_capture.py         # Log streaming utilities
│   ├── logging_config.py      # 📝 Structured logging configuration
│   ├── file_validation.py     # 🔒 File security validation
│   ├── input_sanitization.py  # 🛡️ Input sanitization & XSS protection
│   └── data_recovery.py       # 🔄 Automatic data recovery for malformed files
├── analysis/                   # 📊 Financial analysis modules
│   ├── revenue_analysis.py    # Revenue variance analysis
│   ├── revenue_variance_excel.py # Excel output formatting
│   ├── anomaly_detection.py   # Anomaly detection
│   ├── accounting_rules.py    # Accounting rule engine
│   └── llm_analyzer.py        # AI-powered analysis
└── data/                       # 💾 Data processing modules
    ├── data_utils.py           # Data utilities and helpers
    └── excel_processing.py     # Excel file processing
```

## Architecture Layers

### 1. **API Layer** (`app/api/`)
- **Purpose**: Handle HTTP requests and responses with comprehensive validation
- **Responsibilities**:
  - Request validation with Pydantic models
  - Response formatting with proper error handling
  - Route definitions with OpenAPI documentation
  - Input sanitization and security validation
  - Health monitoring with configuration status

### 2. **Service Layer** (`app/services/`)
- **Purpose**: Business logic and orchestration with session management
- **Responsibilities**:
  - File processing coordination with progress tracking
  - Session management with automatic cleanup
  - Analysis orchestration with error recovery
  - Data transformation with validation
  - Logging integration for audit trails

### 3. **Core Layer** (`app/core/`)
- **Purpose**: Application foundation with unified configuration
- **Responsibilities**:
  - **Unified Configuration System** (Pydantic v2 with validation)
  - Dependency injection with proper lifecycle management
  - **Custom Exception Handling** with user-friendly messages
  - Application lifecycle with health monitoring
  - Backward compatibility with legacy configuration

### 4. **Middleware Layer** (`app/middleware/`)
- **Purpose**: Request/response processing and security
- **Responsibilities**:
  - **Request Validation** with size limits and rate limiting
  - **Security Headers** (CORS, CSP, HSTS)
  - **Configuration Monitoring** with health checks
  - Request logging and metrics collection
  - Input sanitization and XSS protection

### 5. **Models Layer** (`app/models/`)
- **Purpose**: Data validation and serialization with comprehensive schemas
- **Responsibilities**:
  - Request/response schemas with validation rules
  - Type safety with Pydantic v2
  - Error response models with structured messages
  - Configuration models with environment variable support

### 6. **Utils Layer** (`app/utils/`)
- **Purpose**: Shared utilities with security and reliability features
- **Responsibilities**:
  - **Structured Logging** with rotation and formatting
  - **File Security Validation** with magic number detection
  - **Input Sanitization** against injection attacks
  - **Data Recovery** for malformed Excel files
  - Helper functions with error handling

### 7. **Analysis Layer** (`app/analysis/`)
- **Purpose**: Financial analysis algorithms with robust error handling
- **Responsibilities**:
  - Revenue variance analysis with threshold validation
  - Anomaly detection with configurable sensitivity
  - Accounting rule engines with audit trails
  - AI-powered analysis with retry logic and timeout handling

### 8. **Data Layer** (`app/data/`)
- **Purpose**: Data processing with validation and recovery
- **Responsibilities**:
  - Excel file processing with structure validation
  - Data cleaning with automatic recovery
  - Data utilities with comprehensive error handling
  - Format validation and security checks

## Key Features

### ✅ **Clean Architecture**
- Separation of concerns with clear layer boundaries
- Dependency inversion with service interfaces
- Single responsibility principle throughout

### ✅ **Unified Configuration Management** 🆕
- **Pydantic v2 Settings** with type validation and coercion
- **Environment Variable Support** with `VARIANCE_` prefix and nested delimiters
- **Organized Sections**: app, file_processing, ai_analysis, security, core_analysis, revenue_analysis
- **Runtime Validation** with helpful error messages and range checking
- **Documentation** with comprehensive examples and migration guide
- **Backward Compatibility** - existing code continues to work unchanged

### ✅ **Comprehensive Error Handling** 🆕
- **Custom Exception Classes** with business-friendly error messages
- **Global Error Handlers** with structured responses and error codes
- **User-Friendly Messages** with suggestions and troubleshooting steps
- **Security-Aware** error responses without sensitive information exposure
- **Logging Integration** with error tracking and audit trails

### ✅ **Structured Logging System** 🆕
- **Centralized Configuration** with level management and file rotation
- **Structured Output** with JSON formatting for production
- **Context Preservation** with request IDs and session tracking
- **Performance Monitoring** with execution time tracking
- **Security Logging** with audit trails for sensitive operations

### ✅ **Security & Validation** 🆕
- **File Security Validation** with magic number detection and structure validation
- **Input Sanitization** against SQL injection, XSS, and path traversal
- **Request Validation Middleware** with size limits and rate limiting
- **Security Headers** including CORS, CSP, and HSTS
- **Session Management** with timeout and cleanup

### ✅ **Data Recovery & Resilience** 🆕
- **Automatic Data Recovery** for malformed Excel files
- **Column Name Fixing** with fuzzy matching and standardization
- **Header Detection** with multiple strategy fallbacks
- **Numeric Data Cleaning** with format normalization
- **Graceful Degradation** when data issues are encountered

### ✅ **Production Readiness**
- **Health Monitoring** with configuration validation and service status
- **Middleware Stack** with proper ordering and error handling
- **Session Cleanup** with automatic old session removal
- **Performance Metrics** with request timing and resource monitoring
- **Environment Configuration** with development and production profiles

### ✅ **API Documentation**
- Automatic OpenAPI generation with comprehensive schemas
- Interactive documentation at `/docs` and `/redoc`
- Type-safe endpoints with validation examples

## API Endpoints

### **Health Check**
- `GET /health` - Basic application health status
- `GET /health/config` - Configuration health with validation status
- `GET /health/detailed` - Comprehensive health with service status

### **Analysis Endpoints**
- `POST /api/process` - Python-based analysis with comprehensive validation
- `POST /api/start-analysis` - Start AI analysis with session management
- `GET /api/logs/{session_id}` - Stream analysis logs with real-time updates
- `POST /api/analyze-revenue-variance` - Revenue variance analysis
- `GET /api/download/{session_id}` - Download results with security validation
- `GET /api/debug/{file_key}` - Download debug files (debug mode only)
- `GET /api/debug/list/{session_id}` - List debug files with metadata

### **Legacy Compatibility**
- `POST /analyze-revenue` - Legacy revenue analysis (redirects to new endpoint)
- `POST /process` - Legacy process endpoint with parameter compatibility
- `POST /start_analysis` - Legacy AI analysis endpoint
- `GET /logs/{session_id}` - Legacy log streaming
- `GET /download/{session_id}` - Legacy download endpoint

## Configuration System

### **Unified Configuration** (`app/core/unified_config.py`)

The application uses a **unified Pydantic v2 configuration system** that consolidates all settings into organized sections with validation:

```python
class UnifiedConfig(BaseSettings):
    # Configuration sections
    app: ApplicationConfig                    # Application metadata and CORS
    file_processing: FileProcessingConfig     # File upload and validation
    ai_analysis: AIAnalysisConfig            # AI-powered analysis settings
    security: SecurityConfig                 # Session and rate limiting
    core_analysis: CoreAnalysisConfig        # Financial analysis parameters
    revenue_analysis: RevenueAnalysisConfig  # Revenue-specific thresholds
    excel_processing: ExcelProcessingConfig  # Excel file processing
    data_processing: DataProcessingConfig    # Data processing constants
```

### **Environment Variables**

All settings can be overridden using environment variables with the `VARIANCE_` prefix:

```bash
# Application settings
VARIANCE_APP__DEBUG=true
VARIANCE_APP__LOG_LEVEL=DEBUG

# File processing
VARIANCE_FILE_PROCESSING__MAX_FILE_SIZE=104857600  # 100MB

# AI Analysis
VARIANCE_AI_ANALYSIS__LLM_MODEL=gpt-4o
VARIANCE_AI_ANALYSIS__USE_LLM_ANALYSIS=true

# Security
VARIANCE_SECURITY__SESSION_TIMEOUT_MINUTES=120
VARIANCE_SECURITY__RATE_LIMIT_REQUESTS_PER_MINUTE=200

# Analysis thresholds
VARIANCE_CORE_ANALYSIS__MATERIALITY_VND=2000000000
VARIANCE_REVENUE_ANALYSIS__REVENUE_CHANGE_THRESHOLD_VND=5000000
```

### **Configuration Validation**

The system provides comprehensive validation:

- **Type Validation**: Automatic type coercion and validation
- **Range Validation**: Min/max values for numeric settings
- **Format Validation**: Email, URL, and enum validation
- **Cross-Section Validation**: Consistency checks across configuration sections
- **Runtime Health Checks**: Continuous monitoring of configuration health

### **Backward Compatibility**

The unified system maintains **100% backward compatibility**:

```python
# Old way (still works)
from app.core.config import get_settings, get_analysis_config
settings = get_settings()
analysis_config = get_analysis_config()

# New way (recommended)
from app.core.unified_config import get_unified_config
config = get_unified_config()
# Access structured configuration
debug_mode = config.app.debug
max_file_size = config.file_processing.max_file_size
# Or get legacy format
legacy_config = config.to_legacy_dict()
```

## Error Handling & Validation

### **Custom Exception Classes**
```python
class AnalysisError(Exception):
    """Base exception for analysis errors with user-friendly messages."""

class FileProcessingError(AnalysisError):
    """File processing and validation errors."""

class ValidationError(AnalysisError):
    """Data validation and sanitization errors."""

class DataQualityError(AnalysisError):
    """Data quality and recovery errors."""
```

### **Security Validation**
- **File Type Validation**: Magic number detection for Excel files
- **Content Scanning**: Structure validation and malicious content detection
- **Input Sanitization**: Protection against SQL injection, XSS, and path traversal
- **Request Validation**: Size limits, rate limiting, and CORS enforcement

### **Data Recovery**
- **Automatic Recovery**: Malformed Excel file repair and standardization
- **Column Mapping**: Fuzzy matching for column name variations
- **Data Cleaning**: Numeric format normalization and missing value handling
- **Graceful Degradation**: Partial processing when data issues are encountered

## Middleware Stack

The application uses a comprehensive middleware stack (applied in order):

1. **SecurityHeadersMiddleware** - CORS, CSP, HSTS headers
2. **ConfigValidationMiddleware** - Configuration health monitoring
3. **ValidationMiddleware** - Request validation and rate limiting
4. **RequestLoggingMiddleware** - Request/response logging (debug mode)
5. **ConfigMonitoringMiddleware** - Configuration usage monitoring (debug mode)

## Running the Application

### Development
```bash
# Set debug mode and AI analysis
export VARIANCE_APP__DEBUG=true
export VARIANCE_AI_ANALYSIS__USE_LLM_ANALYSIS=true

# Run with reload
python -m app.main
# or
uvicorn app.main:app --reload
```

### Production
```bash
# Production configuration
export VARIANCE_APP__DEBUG=false
export VARIANCE_APP__LOG_LEVEL=INFO
export VARIANCE_SECURITY__SESSION_TIMEOUT_MINUTES=60
export VARIANCE_FILE_PROCESSING__MAX_FILE_SIZE=104857600

# Run production server
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

### Docker
```dockerfile
# Environment variables in Dockerfile
ENV VARIANCE_APP__DEBUG=false
ENV VARIANCE_APP__LOG_LEVEL=INFO
ENV VARIANCE_FILE_PROCESSING__MAX_FILE_SIZE=104857600
ENV VARIANCE_SECURITY__SESSION_TIMEOUT_MINUTES=60
```

## Benefits of New Architecture

### 🚀 **Improved Maintainability**
- Clear separation of concerns with well-defined layers
- Unified configuration system with comprehensive documentation
- Structured logging for easier debugging and monitoring
- Comprehensive error handling with user-friendly messages

### 🧪 **Better Testability**
- Dependency injection enables comprehensive mocking
- Service layer can be tested independently with minimal setup
- Clear interfaces between layers facilitate unit testing
- Configuration validation ensures test environment consistency

### 📈 **Enhanced Scalability**
- Services can be extracted to microservices without code changes
- Configuration-driven behavior enables easy environment scaling
- Session management with automatic cleanup prevents memory leaks
- Middleware stack enables horizontal scaling with load balancers

### 🔒 **Production Security**
- Comprehensive input validation and sanitization
- File security validation with malicious content detection
- Rate limiting and request size validation
- Security headers and CORS enforcement
- Audit logging for security monitoring

### 🛡️ **Reliability & Resilience**
- Automatic data recovery for malformed input files
- Graceful error handling with user-friendly messages
- Health monitoring with configuration validation
- Session cleanup and resource management
- Comprehensive logging for troubleshooting

### 📝 **Operational Excellence**
- Structured logging with rotation and performance metrics
- Configuration health monitoring with real-time validation
- Comprehensive API documentation with interactive examples
- Environment-based configuration with validation
- Backward compatibility ensuring zero-downtime deployments

## Migration Guide

The restructured application maintains **100% backward compatibility**:

### **For Existing Users**
1. **All endpoints** continue to work without changes
2. **Configuration** can use existing environment variables
3. **Frontend integration** works without modifications
4. **Analysis results** maintain the same format and structure

### **For Developers**

**Configuration**:
```python
# Migrate from old configuration
# OLD:
from app.core.config import get_settings
settings = get_settings()

# NEW (recommended):
from app.core.unified_config import get_unified_config
config = get_unified_config()
```

**Error Handling**:
```python
# Use custom exceptions for better error messages
from app.core.exceptions import FileProcessingError, ValidationError

try:
    process_file(file)
except FileProcessingError as e:
    # Automatic user-friendly error message with suggestions
    return {"error": str(e), "suggestions": e.suggestions}
```

**Logging**:
```python
# Use structured logging
from app.utils.logging_config import get_logger
logger = get_logger(__name__)

logger.info("Processing started", extra={"session_id": session_id, "file_count": len(files)})
```

### **For Operations**

**Environment Variables**:
```bash
# OLD style (still works):
export VARIANCE_DEBUG=true

# NEW style (recommended):
export VARIANCE_APP__DEBUG=true
export VARIANCE_FILE_PROCESSING__MAX_FILE_SIZE=104857600
```

**Health Monitoring**:
```bash
# Check application health
curl http://localhost:8000/health

# Check configuration health
curl http://localhost:8000/health/config

# Check detailed service status
curl http://localhost:8000/health/detailed
```

## Future Enhancements

The new architecture enables:

### **Infrastructure**
- **Database integration** via service layer with connection pooling
- **Caching layer** with Redis for session and analysis result caching
- **Message queues** for asynchronous analysis processing
- **Microservices** extraction with service interfaces

### **Security**
- **Authentication/authorization** via JWT middleware and role-based access
- **API rate limiting** with distributed rate limiting across instances
- **Audit logging** with centralized log aggregation and monitoring
- **Encryption** for sensitive data at rest and in transit

### **Monitoring & Observability**
- **Metrics collection** with Prometheus integration
- **Distributed tracing** with OpenTelemetry
- **Performance monitoring** with APM integration
- **Error tracking** with Sentry or similar platforms

### **Development**
- **Automated testing** with pytest and comprehensive test coverage
- **CI/CD pipelines** with automated testing and deployment
- **Documentation generation** with automated API documentation updates
- **Development tooling** with linting, formatting, and type checking

This comprehensive restructure provides a solid, production-ready foundation for future growth while maintaining all existing functionality and ensuring a smooth migration path.