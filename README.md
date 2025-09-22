# Merged Variance Analysis Tool

A comprehensive financial variance analysis tool that combines both traditional Python rule-based analysis and AI-powered analysis in a single application.

## Features

### 🔧 Python Analysis Tab
- Manual configuration with detailed parameter controls
- Traditional rule-based anomaly detection
- Support for correlation/seasonality mapping files
- Revenue impact analysis
- Fully customizable thresholds and rules

### 🤖 AI Analysis Tab
- Automatic AI-powered anomaly detection
- Smart materiality threshold determination
- Focus on critical Vietnamese Chart of Accounts
- Detailed business context explanations
- No manual configuration required

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

## File Structure

```
Merged-Variance-Analysis-Tool/
├── app/
│   ├── __init__.py
│   ├── main.py          # FastAPI application
│   ├── core.py          # Core analysis logic
│   └── llm_analyzer.py  # AI analysis module
├── frontend/
│   ├── index.html       # Web interface
│   ├── styles.css       # Styling
│   ├── script.js        # Additional JS
│   └── assets/          # Static assets
├── requirements.txt     # Python dependencies
├── .env.example         # Environment configuration template
└── README.md           # This file
```

## API Endpoints

- `GET /` - Web interface
- `POST /process` - Python analysis endpoint
- `POST /start_analysis` - AI analysis endpoint
- `GET /logs/{session_id}` - Stream AI analysis progress
- `GET /download/{session_id}` - Download AI analysis results
- `POST /analyze-revenue` - Revenue impact analysis

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

1. **Backend**: Modify `app/main.py` and `app/core.py`
2. **Frontend**: Update `frontend/index.html` and `frontend/styles.css`
3. **AI Logic**: Customize `app/llm_analyzer.py`

## License

This tool is provided as-is for financial analysis purposes.