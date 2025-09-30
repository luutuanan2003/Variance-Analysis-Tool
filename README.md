# Merged Variance Analysis Tool

A comprehensive financial variance analysis tool that combines both traditional Python rule-based analysis and AI-powered analysis in a single application.

## Features

### 🔧 Python Analysis Tab
- Manual configuration with detailed parameter controls
- Traditional rule-based anomaly detection
- Support for correlation/seasonality mapping files
- **Comprehensive Revenue Analysis** with multiple analysis sheets:
  - **Months Analysis**: Consolidated view of all analyzed months with revenue variance, legacy analysis, and account breakdowns (511, 632, 641, 642)
  - **Month to Month Analysis**: Focused analysis of the last two months based on Excel file row 4 date (e.g., "End of May 2025" → analyzes Apr-May)
- **Cleaned Data Sheets**: Automatically generated BS and PL cleaned sheets for each subsidiary
- Fully customizable thresholds and rules

### 🤖 AI Analysis Tab
- Automatic AI-powered anomaly detection
- Smart materiality threshold determination
- Focus on critical Vietnamese Chart of Accounts
- Detailed business context explanations
- No manual configuration required
- **Cleaned Data Sheets**: Automatically generated BS and PL cleaned sheets in AI mode as well

## Quick Start

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure AI Analysis (Optional)**
   ```bash
   # Copy the example environment file
   cp .env.example .env

   # Edit .env and add your OpenAI API key
   OPENAI_API_KEY=your_api_key_here
   ```

3. **Run the Application**
   ```bash
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

4. **Access the Tool**
   Open your browser to: http://localhost:8000

## Usage

### Python Analysis
1. Select the "Python Analysis" tab
2. Upload Excel files with "BS Breakdown" and "PL Breakdown" sheets
3. Configure analysis parameters (materiality, thresholds, etc.)
4. Optionally upload mapping rules file
5. Click "Process" to run analysis
6. Download the resulting Excel report

### AI Analysis
1. Select the "AI Analysis" tab
2. Upload Excel files with "BS Breakdown" and "PL Breakdown" sheets
3. Click "🚀 Analyze with AI"
4. Watch the progress as AI analyzes your data
5. Download the AI-generated analysis report

## Output Sheets

### Python Analysis Output
The analysis generates an Excel file with the following sheets:

1. **Anomalies Summary**: Main anomaly detection results with status tracking
2. **[Subsidiary]_BS_Cleaned**: Cleaned Balance Sheet data for each subsidiary
3. **[Subsidiary]_PL_Cleaned**: Cleaned Profit & Loss data for each subsidiary
4. **Months Analysis**: Consolidated analysis sheet containing:
   - Revenue Variance Analysis (month-over-month changes)
   - Legacy Revenue Analysis (historical trends)
   - 511 Accounts Analysis (Revenue accounts)
   - 632 Accounts Analysis (COGS - Raw Materials)
   - 641 Accounts Analysis (Personnel expenses)
   - 642 Accounts Analysis (Material expenses)
5. **Month to Month Analysis**: Focused two-month comparison based on row 4 date in source file

### AI Analysis Output
Similar structure with AI-generated insights:
1. **Anomalies Summary**: AI-detected anomalies with business context
2. **[Subsidiary]_BS_Cleaned**: Cleaned Balance Sheet data
3. **[Subsidiary]_PL_Cleaned**: Cleaned Profit & Loss data

## File Structure

```
Variance-Analysis-Tool/
├── app/
│   ├── main.py                    # FastAPI application entry point
│   ├── api/                       # API route handlers
│   │   ├── health.py             # Health check endpoints
│   │   └── analysis.py           # Analysis endpoints
│   ├── core/                      # Core application components
│   │   ├── unified_config.py     # Unified configuration system
│   │   ├── config.py             # Legacy configuration
│   │   ├── dependencies.py       # Dependency injection
│   │   └── exceptions.py         # Custom exceptions
│   ├── services/                  # Business logic layer
│   │   ├── processing_service.py # Core processing orchestration
│   │   └── analysis_service.py   # Analysis business logic
│   ├── analysis/                  # Financial analysis modules
│   │   ├── revenue_analysis.py   # Revenue variance analysis
│   │   ├── anomaly_detection.py  # Anomaly detection
│   │   └── llm_analyzer.py       # AI-powered analysis
│   ├── data/                      # Data processing modules
│   │   ├── excel_processing.py   # Excel file processing
│   │   └── data_utils.py         # Data utilities
│   ├── utils/                     # Utility functions
│   │   ├── logging_config.py     # Structured logging
│   │   ├── file_validation.py    # File security validation
│   │   └── input_sanitization.py # Input sanitization
│   └── middleware/                # Request/response middleware
│       ├── validation_middleware.py
│       └── config_middleware.py
├── frontend/
│   ├── index.html                 # Web interface
│   ├── styles.css                 # Styling
│   └── script.js                  # JavaScript logic
├── requirements.txt               # Python dependencies
├── .env.example                   # Environment configuration template
├── README.md                      # This file
├── ARCHITECTURE.md                # Architecture documentation
└── CONFIG.md                      # Configuration guide
```

## API Endpoints

### Main Endpoints
- `GET /` - Web interface
- `POST /api/process` - Python analysis endpoint with comprehensive validation
- `POST /api/start-analysis` - Start AI analysis with session management
- `GET /api/logs/{session_id}` - Stream AI analysis progress logs
- `GET /api/download/{session_id}` - Download analysis results
- `POST /api/analyze-revenue-variance` - Revenue variance analysis

### Health & Monitoring
- `GET /health` - Basic application health status
- `GET /health/config` - Configuration health validation
- `GET /health/detailed` - Comprehensive service status

### Legacy Endpoints (backward compatibility)
- `POST /process` - Legacy Python analysis endpoint
- `POST /start_analysis` - Legacy AI analysis endpoint
- `GET /logs/{session_id}` - Legacy log streaming
- `GET /download/{session_id}` - Legacy download endpoint

## Configuration

### Python Analysis Parameters
- **Materiality (VND)**: Absolute change threshold
- **Recurring %**: Threshold for recurring P/L accounts
- **Revenue/OPEX %**: Threshold for revenue/operating expense accounts
- **Balance Sheet %**: Threshold for BS balances
- **Code Prefixes**: Define recurring account types
- **Trend Periods**: Minimum periods for trend analysis

### AI Analysis
- Automatically determines all thresholds
- Focuses on Vietnamese Chart of Accounts (511*, 627*, 641*, 515*, 635*)
- Provides detailed business explanations

## Troubleshooting

### AI Analysis Issues
- Ensure OpenAI API key is set in `.env` file
- Check OpenAI service status at https://status.openai.com/
- Verify the model name in configuration

### File Upload Issues
- Ensure Excel files contain "BS Breakdown" and "PL Breakdown" sheets
- Check that files are valid .xlsx format
- Verify file size is reasonable (< 50MB recommended)

## Development

To extend or modify the tool:

1. **Backend API**: Modify `app/api/analysis.py` for endpoints
2. **Business Logic**: Update `app/services/processing_service.py` for core processing
3. **Analysis Logic**: Customize `app/analysis/` modules:
   - `revenue_analysis.py` - Revenue variance analysis
   - `anomaly_detection.py` - Anomaly detection rules
   - `llm_analyzer.py` - AI-powered analysis
4. **Data Processing**: Modify `app/data/excel_processing.py` for Excel handling
5. **Frontend**: Update `frontend/index.html` and `frontend/styles.css`
6. **Configuration**: Adjust `app/core/unified_config.py` for new settings

### Key Implementation Details

#### Month to Month Analysis
The system automatically detects the analysis period from row 4 of the Excel file:
- Looks for patterns like "End of May 2025" or "From Jan 2025 to May 2025"
- Extracts the target month (e.g., "May")
- Analyzes that month and the previous month (e.g., Apr-May)
- Filters all analysis data to only include these two months

Located in: `app/data/excel_processing.py:_add_month_to_month_analysis_to_sheet()`

#### Consolidated Months Analysis
Combines six separate analysis sections into one comprehensive sheet:
1. Revenue Variance Analysis
2. Legacy Revenue Analysis
3. 511 Accounts (Revenue)
4. 632 Accounts (COGS - Raw Materials)
5. 641 Accounts (Personnel Expenses)
6. 642 Accounts (Material Expenses)

Located in: `app/data/excel_processing.py:_add_consolidated_months_analysis_to_sheet()`

## Documentation

- **ARCHITECTURE.md**: Detailed architecture documentation and design patterns
- **CONFIG.md**: Comprehensive configuration guide with all environment variables
- **README.md**: This file - quick start and overview

## License

This tool is provided as-is for financial analysis purposes.