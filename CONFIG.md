# Variance Analysis Tool - Configuration Guide

This document provides comprehensive guidance on configuring the Variance Analysis Tool API.

## Table of Contents

1. [Configuration System Overview](#configuration-system-overview)
2. [Environment Variables](#environment-variables)
3. [Configuration Sections](#configuration-sections)
4. [Examples](#examples)
5. [Migration Guide](#migration-guide)
6. [Troubleshooting](#troubleshooting)

## Configuration System Overview

The Variance Analysis Tool uses a **unified configuration system** built with Pydantic that provides:

- ✅ **Type validation** and automatic coercion
- ✅ **Environment variable support** with nested configuration
- ✅ **Clear documentation** for all settings
- ✅ **Backward compatibility** with legacy configuration
- ✅ **Runtime validation** with helpful error messages

### Key Features

- **Validation**: All configuration values are validated for type and range
- **Environment Variables**: Override any setting using environment variables
- **Documentation**: Every setting includes description and valid ranges
- **Sections**: Configuration is organized into logical sections
- **Backward Compatibility**: Existing code continues to work unchanged

## Environment Variables

### Naming Convention

Environment variables use the prefix `VARIANCE_` followed by the section and setting name:

```
VARIANCE_<SECTION>__<SETTING>=value
```

**Examples:**
```bash
# Application settings
VARIANCE_APP__DEBUG=true
VARIANCE_APP__LOG_LEVEL=DEBUG

# AI Analysis settings
VARIANCE_AI_ANALYSIS__LLM_MODEL=gpt-4
VARIANCE_AI_ANALYSIS__USE_LLM_ANALYSIS=true

# File processing settings
VARIANCE_FILE_PROCESSING__MAX_FILE_SIZE=209715200  # 200MB

# Security settings
VARIANCE_SECURITY__SESSION_TIMEOUT_MINUTES=120
VARIANCE_SECURITY__RATE_LIMIT_REQUESTS_PER_MINUTE=200
```

### Common Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `VARIANCE_APP__DEBUG` | `false` | Enable debug mode |
| `VARIANCE_APP__LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| `VARIANCE_AI_ANALYSIS__LLM_MODEL` | `gpt-4o` | AI model for analysis |
| `VARIANCE_AI_ANALYSIS__USE_LLM_ANALYSIS` | `false` | Enable AI-powered analysis |
| `VARIANCE_FILE_PROCESSING__MAX_FILE_SIZE` | `104857600` | Max file size in bytes (100MB) |
| `VARIANCE_SECURITY__SESSION_TIMEOUT_MINUTES` | `60` | Session timeout in minutes |
| `VARIANCE_CORE_ANALYSIS__MATERIALITY_VND` | `1000000000` | Materiality threshold in VND |

## Configuration Sections

### 1. Application Configuration (`app`)

Basic application settings:

```python
# Environment variables:
VARIANCE_APP__DEBUG=true
VARIANCE_APP__LOG_LEVEL=DEBUG
VARIANCE_APP__LOG_FILE=logs/custom.log

# Programmatic access:
from app.core.unified_config import get_unified_config
config = get_unified_config()
print(config.app.debug)  # True
```

**Settings:**
- `app_name`: Application name
- `app_version`: Application version
- `debug`: Enable debug mode
- `log_level`: Logging level
- `log_file`: Log file path
- `cors_origins`: CORS allowed origins
- `cors_methods`: CORS allowed methods
- `cors_headers`: CORS allowed headers

### 2. File Processing Configuration (`file_processing`)

File upload and validation settings:

```python
# Environment variables:
VARIANCE_FILE_PROCESSING__MAX_FILE_SIZE=209715200  # 200MB
VARIANCE_FILE_PROCESSING__MAX_FILES_PER_REQUEST=5

# Settings:
- max_file_size: Maximum file size in bytes (1KB - 1GB)
- allowed_file_extensions: [".xlsx", ".xls"]
- max_files_per_request: Maximum files per upload (1-100)
- required_sheets: ["BS Breakdown", "PL Breakdown"]
```

### 3. AI Analysis Configuration (`ai_analysis`)

AI-powered analysis settings:

```python
# Environment variables:
VARIANCE_AI_ANALYSIS__LLM_MODEL=gpt-4
VARIANCE_AI_ANALYSIS__USE_LLM_ANALYSIS=true
VARIANCE_AI_ANALYSIS__AI_TIMEOUT_SECONDS=600

# Settings:
- use_llm_analysis: Enable AI analysis
- llm_model: AI model name
- enable_ai_analysis: Global AI feature flag
- max_ai_retries: Max retries for AI calls (1-10)
- ai_timeout_seconds: AI call timeout (30-3600)
```

### 4. Security Configuration (`security`)

Security and session management:

```python
# Environment variables:
VARIANCE_SECURITY__SESSION_TIMEOUT_MINUTES=120
VARIANCE_SECURITY__MAX_CONCURRENT_SESSIONS=20
VARIANCE_SECURITY__RATE_LIMIT_REQUESTS_PER_MINUTE=200

# Settings:
- session_timeout_minutes: Session timeout (1-1440)
- max_concurrent_sessions: Max concurrent sessions (1-1000)
- rate_limit_requests_per_minute: Rate limit per IP (1-10000)
- rate_limit_window_minutes: Rate limit window (1-60)
```

### 5. Core Analysis Configuration (`core_analysis`)

Financial analysis parameters:

```python
# Environment variables:
VARIANCE_CORE_ANALYSIS__MATERIALITY_VND=2000000000
VARIANCE_CORE_ANALYSIS__RECURRING_PCT_THRESHOLD=0.03
VARIANCE_CORE_ANALYSIS__MIN_TREND_PERIODS=5

# Settings:
- materiality_vnd: Materiality threshold in VND (0-1e15)
- recurring_pct_threshold: Recurring P/L threshold (0-1)
- revenue_opex_pct_threshold: Revenue/OPEX threshold (0-1)
- bs_pct_threshold: Balance sheet threshold (0-1)
- min_trend_periods: Minimum periods for trends (1-120)
- gm_drop_threshold_pct: Gross margin drop threshold (0-1)
```

### 6. Revenue Analysis Configuration (`revenue_analysis`)

Revenue-specific analysis thresholds:

```python
# Environment variables:
VARIANCE_REVENUE_ANALYSIS__REVENUE_CHANGE_THRESHOLD_VND=2000000
VARIANCE_REVENUE_ANALYSIS__MONTHS_TO_ANALYZE=12

# Settings:
- revenue_change_threshold_vnd: Revenue change threshold (0-1e12)
- revenue_entity_threshold_vnd: Entity revenue threshold (0-1e12)
- cogs_change_threshold_vnd: COGS change threshold (0-1e12)
- sga_change_threshold_vnd: SG&A change threshold (0-1e12)
- months_to_analyze: Analysis period in months (1-120)
- top_entity_impacts: Top entities to show (1-50)
```

## Examples

### Development Environment

Create a `.env` file:

```bash
# Development configuration
VARIANCE_APP__DEBUG=true
VARIANCE_APP__LOG_LEVEL=DEBUG

# AI Analysis
VARIANCE_AI_ANALYSIS__USE_LLM_ANALYSIS=true
VARIANCE_AI_ANALYSIS__LLM_MODEL=gpt-4o

# File Processing (smaller limits for dev)
VARIANCE_FILE_PROCESSING__MAX_FILE_SIZE=52428800  # 50MB

# Security (relaxed for dev)
VARIANCE_SECURITY__SESSION_TIMEOUT_MINUTES=240
VARIANCE_SECURITY__RATE_LIMIT_REQUESTS_PER_MINUTE=1000
```

### Production Environment

```bash
# Production configuration
VARIANCE_APP__DEBUG=false
VARIANCE_APP__LOG_LEVEL=INFO
VARIANCE_APP__LOG_FILE=/var/log/variance/app.log

# AI Analysis
VARIANCE_AI_ANALYSIS__USE_LLM_ANALYSIS=true
VARIANCE_AI_ANALYSIS__LLM_MODEL=gpt-4o
VARIANCE_AI_ANALYSIS__AI_TIMEOUT_SECONDS=300

# File Processing (strict limits)
VARIANCE_FILE_PROCESSING__MAX_FILE_SIZE=104857600  # 100MB
VARIANCE_FILE_PROCESSING__MAX_FILES_PER_REQUEST=5

# Security (strict)
VARIANCE_SECURITY__SESSION_TIMEOUT_MINUTES=60
VARIANCE_SECURITY__MAX_CONCURRENT_SESSIONS=10
VARIANCE_SECURITY__RATE_LIMIT_REQUESTS_PER_MINUTE=100

# Analysis (production thresholds)
VARIANCE_CORE_ANALYSIS__MATERIALITY_VND=5000000000
VARIANCE_REVENUE_ANALYSIS__REVENUE_CHANGE_THRESHOLD_VND=5000000
```

### Docker Environment

```dockerfile
# Dockerfile
ENV VARIANCE_APP__DEBUG=false
ENV VARIANCE_APP__LOG_LEVEL=INFO
ENV VARIANCE_FILE_PROCESSING__MAX_FILE_SIZE=104857600
ENV VARIANCE_SECURITY__SESSION_TIMEOUT_MINUTES=60
ENV VARIANCE_AI_ANALYSIS__LLM_MODEL=gpt-4o
```

### Programmatic Configuration

```python
from app.core.unified_config import get_unified_config

# Get configuration
config = get_unified_config()

# Access settings
print(f"Debug mode: {config.app.debug}")
print(f"Max file size: {config.file_processing.max_file_size}")
print(f"AI model: {config.ai_analysis.llm_model}")

# Get legacy format for existing code
legacy_config = config.to_legacy_dict()
materiality = legacy_config["materiality_vnd"]
```

## Migration Guide

### From Legacy Configuration

**Old way** (deprecated):
```python
from app.core.config import get_settings, get_analysis_config

settings = get_settings()
analysis_config = get_analysis_config()
```

**New way** (recommended):
```python
from app.core.unified_config import get_unified_config

config = get_unified_config()
# Access structured configuration
debug_mode = config.app.debug
max_file_size = config.file_processing.max_file_size

# Or get legacy format for existing code
legacy_config = config.to_legacy_dict()
```

### Backward Compatibility

The system maintains **100% backward compatibility**:

- Existing `get_settings()` and `get_analysis_config()` functions work unchanged
- All environment variables from the old system continue to work
- Legacy configuration dictionary format is preserved

## Configuration Validation

The system automatically validates all configuration:

### Type Validation
```python
# ✅ Valid
VARIANCE_FILE_PROCESSING__MAX_FILE_SIZE=104857600

# ❌ Invalid - will raise validation error
VARIANCE_FILE_PROCESSING__MAX_FILE_SIZE=not_a_number
```

### Range Validation
```python
# ✅ Valid
VARIANCE_SECURITY__SESSION_TIMEOUT_MINUTES=60

# ❌ Invalid - exceeds maximum
VARIANCE_SECURITY__SESSION_TIMEOUT_MINUTES=2000  # Max is 1440 (24 hours)
```

### Format Validation
```python
# ✅ Valid
VARIANCE_APP__LOG_LEVEL=INFO

# ❌ Invalid - not a valid log level
VARIANCE_APP__LOG_LEVEL=INVALID_LEVEL
```

## Troubleshooting

### Common Issues

**1. Configuration Validation Errors**
```
pydantic.ValidationError: validation error for UnifiedConfig
```
**Solution**: Check the error message for the specific field and valid range.

**2. Environment Variable Not Applied**
```python
# Check if environment variable is correctly named
import os
print(os.getenv("VARIANCE_APP__DEBUG"))  # Should print your value
```

**3. Legacy Code Not Working**
```python
# Ensure you're using the compatibility functions
from app.core.config import get_settings  # Not unified_config
```

**4. File Path Issues**
```bash
# Ensure log directories exist
mkdir -p logs
VARIANCE_APP__LOG_FILE=logs/app.log
```

### Debug Configuration

Enable debug mode to see configuration details:

```bash
VARIANCE_APP__DEBUG=true
VARIANCE_APP__LOG_LEVEL=DEBUG
```

Check configuration health:
```
GET /health  # Returns configuration status
```

### Getting Help

1. **Check the logs** - All configuration issues are logged
2. **Validate environment variables** - Use the naming convention exactly
3. **Check value ranges** - Each setting has documented valid ranges
4. **Test with defaults** - Remove custom environment variables to test defaults

## Configuration Reference

### Complete Environment Variable List

```bash
# Application
VARIANCE_APP__DEBUG=false
VARIANCE_APP__LOG_LEVEL=INFO
VARIANCE_APP__LOG_FILE=logs/variance_analysis.log

# File Processing
VARIANCE_FILE_PROCESSING__MAX_FILE_SIZE=104857600
VARIANCE_FILE_PROCESSING__MAX_FILES_PER_REQUEST=10

# AI Analysis
VARIANCE_AI_ANALYSIS__USE_LLM_ANALYSIS=false
VARIANCE_AI_ANALYSIS__LLM_MODEL=gpt-4o
VARIANCE_AI_ANALYSIS__ENABLE_AI_ANALYSIS=true
VARIANCE_AI_ANALYSIS__MAX_AI_RETRIES=3
VARIANCE_AI_ANALYSIS__AI_TIMEOUT_SECONDS=300

# Security
VARIANCE_SECURITY__SESSION_TIMEOUT_MINUTES=60
VARIANCE_SECURITY__MAX_CONCURRENT_SESSIONS=10
VARIANCE_SECURITY__RATE_LIMIT_REQUESTS_PER_MINUTE=100

# Core Analysis
VARIANCE_CORE_ANALYSIS__MATERIALITY_VND=1000000000
VARIANCE_CORE_ANALYSIS__RECURRING_PCT_THRESHOLD=0.05
VARIANCE_CORE_ANALYSIS__REVENUE_OPEX_PCT_THRESHOLD=0.10
VARIANCE_CORE_ANALYSIS__BS_PCT_THRESHOLD=0.05
VARIANCE_CORE_ANALYSIS__MIN_TREND_PERIODS=3
VARIANCE_CORE_ANALYSIS__GM_DROP_THRESHOLD_PCT=0.01

# Revenue Analysis
VARIANCE_REVENUE_ANALYSIS__REVENUE_CHANGE_THRESHOLD_VND=1000000
VARIANCE_REVENUE_ANALYSIS__REVENUE_ENTITY_THRESHOLD_VND=100000
VARIANCE_REVENUE_ANALYSIS__MONTHS_TO_ANALYZE=8
VARIANCE_REVENUE_ANALYSIS__TOP_ENTITY_IMPACTS=5
```

For more details, see the [API Documentation](/docs) or contact support.