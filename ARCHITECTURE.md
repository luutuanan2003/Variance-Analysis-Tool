# FastAPI Application Architecture

## Overview

The Variance Analysis Tool has been restructured to follow FastAPI best practices with clean architecture principles. This provides better maintainability, testability, and scalability.

## Directory Structure

```
app/
├── main.py                    # ✨ Application entry point
├── api/                       # 🛣️  API route handlers
│   ├── health.py             # Health check endpoints
│   └── analysis.py           # Analysis endpoints
├── core/                      # 🏛️  Core application components
│   ├── config.py             # Configuration management
│   ├── dependencies.py       # Dependency injection
│   └── exceptions.py         # Custom exceptions & error handlers
├── models/                    # 📋 Pydantic models
│   └── analysis.py           # Request/response models
├── services/                  # 🔧 Business logic layer
│   ├── analysis_service.py   # Analysis business logic
│   └── processing_service.py  # Core processing orchestration
├── utils/                     # 🛠️  Utility functions
│   ├── helpers.py            # Helper functions
│   └── log_capture.py        # Log streaming utilities
├── analysis/                  # 📊 Financial analysis modules
│   ├── revenue_analysis.py   # Revenue variance analysis
│   ├── revenue_variance_excel.py # Excel output formatting
│   ├── anomaly_detection.py  # Anomaly detection
│   ├── accounting_rules.py   # Accounting rule engine
│   └── llm_analyzer.py       # AI-powered analysis
└── data/                      # 💾 Data processing modules
    ├── data_utils.py          # Data utilities and helpers
    └── excel_processing.py    # Excel file processing
```

## Architecture Layers

### 1. **API Layer** (`app/api/`)
- **Purpose**: Handle HTTP requests and responses
- **Responsibilities**:
  - Request validation
  - Response formatting
  - Route definitions
  - Swagger documentation

### 2. **Service Layer** (`app/services/`)
- **Purpose**: Business logic and orchestration
- **Responsibilities**:
  - File processing coordination
  - Session management
  - Analysis orchestration
  - Data transformation

### 3. **Core Layer** (`app/core/`)
- **Purpose**: Application foundation
- **Responsibilities**:
  - Configuration management
  - Dependency injection
  - Error handling
  - Application lifecycle

### 4. **Models Layer** (`app/models/`)
- **Purpose**: Data validation and serialization
- **Responsibilities**:
  - Request/response schemas
  - Data validation
  - Type safety

### 5. **Utils Layer** (`app/utils/`)
- **Purpose**: Shared utilities
- **Responsibilities**:
  - Helper functions
  - Common utilities
  - Logging utilities

### 6. **Analysis Layer** (`app/analysis/`)
- **Purpose**: Financial analysis algorithms
- **Responsibilities**:
  - Revenue variance analysis
  - Anomaly detection
  - Accounting rule engines
  - AI-powered analysis

### 7. **Data Layer** (`app/data/`)
- **Purpose**: Data processing and transformation
- **Responsibilities**:
  - Excel file processing
  - Data cleaning and validation
  - Data utilities and helpers

## Key Features

### ✅ **Clean Architecture**
- Separation of concerns
- Dependency inversion
- Single responsibility principle

### ✅ **Configuration Management**
- Environment-based configuration
- Centralized settings
- Easy configuration override

### ✅ **Error Handling**
- Custom exception classes
- Global error handlers
- Proper HTTP status codes

### ✅ **Dependency Injection**
- Service lifetime management
- Easy testing and mocking
- Loose coupling

### ✅ **API Documentation**
- Automatic OpenAPI generation
- Interactive documentation at `/docs`
- Type-safe endpoints

## API Endpoints

### **Health Check**
- `GET /health` - Application health status

### **Analysis Endpoints**
- `POST /api/process` - Python-based analysis
- `POST /api/start-analysis` - Start AI analysis
- `GET /api/logs/{session_id}` - Stream analysis logs
- `POST /api/analyze-revenue-variance` - Revenue variance analysis
- `GET /api/download/{session_id}` - Download results
- `GET /api/debug/{file_key}` - Download debug files
- `GET /api/debug/list/{session_id}` - List debug files

### **Legacy Compatibility**
- `POST /analyze-revenue` - Legacy revenue analysis (redirects to new endpoint)

## Configuration

### Environment Variables
- `VARIANCE_DEBUG` - Enable debug mode
- `VARIANCE_LLM_MODEL` - AI model selection

### Settings (`app/core/config.py`)
```python
class Settings:
    app_name: str = "Variance Analysis Tool API"
    app_version: str = "2.0.0"
    max_file_size: int = 100 * 1024 * 1024  # 100MB
    session_timeout_minutes: int = 60
    # ... more settings
```

## Running the Application

### Development
```bash
python -m app.main
# or
uvicorn app.main:app --reload
```

### Production
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Benefits of New Architecture

### 🚀 **Improved Maintainability**
- Clear separation of concerns
- Easy to locate and modify code
- Consistent structure

### 🧪 **Better Testability**
- Dependency injection enables mocking
- Service layer can be tested independently
- Clear interfaces between layers

### 📈 **Enhanced Scalability**
- Services can be extracted to microservices
- Easy to add new features
- Configuration-driven behavior

### 🔒 **Better Error Handling**
- Centralized error management
- Consistent error responses
- Proper logging and monitoring

### 📝 **Automatic Documentation**
- OpenAPI/Swagger generation
- Type-safe API contracts
- Interactive documentation

## Migration Guide

The restructured application maintains **100% backward compatibility**:

1. **Existing endpoints** continue to work
2. **Legacy main.py** is preserved as `main_legacy.py`
3. **All analysis functions** remain unchanged
4. **Frontend integration** works without changes

### For Developers

1. **New features**: Add to appropriate service layer
2. **API changes**: Update models and route handlers
3. **Configuration**: Use the centralized config system
4. **Error handling**: Use custom exception classes

### For Operations

1. **Deployment**: Same commands, better structure
2. **Monitoring**: Enhanced error tracking and logging
3. **Configuration**: Environment-based settings
4. **Scaling**: Service-oriented architecture

## Future Enhancements

The new architecture enables:

- **Database integration** (via service layer)
- **Authentication/authorization** (via dependencies)
- **Caching** (via service layer)
- **Rate limiting** (via middleware)
- **Microservices** (extract services)
- **Event-driven architecture** (via service events)

This restructure provides a solid foundation for future growth while maintaining all existing functionality.