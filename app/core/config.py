# app/core/config.py
"""Application configuration management."""

from typing import List, Optional
from dataclasses import dataclass
from functools import lru_cache
import os

@dataclass
class Settings:
    """Application settings with environment variable support."""

    # API Settings
    app_name: str = "Variance Analysis Tool API"
    app_version: str = "2.0.0"
    debug: bool = False

    # CORS Settings
    cors_origins: List[str] = None
    cors_methods: List[str] = None
    cors_headers: List[str] = None

    # File Processing Settings
    max_file_size: int = 100 * 1024 * 1024  # 100MB
    allowed_file_extensions: List[str] = None

    # Analysis Settings
    default_months_to_analyze: int = 8
    revenue_change_threshold_vnd: float = 1_000_000
    revenue_entity_threshold_vnd: float = 100_000

    # AI Settings
    llm_model: str = "gpt-4o"
    enable_ai_analysis: bool = True

    # Session Management
    session_timeout_minutes: int = 60
    max_concurrent_sessions: int = 10

    def __post_init__(self):
        """Initialize default values and read from environment."""
        if self.cors_origins is None:
            self.cors_origins = ["*"]
        if self.cors_methods is None:
            self.cors_methods = ["*"]
        if self.cors_headers is None:
            self.cors_headers = ["*"]
        if self.allowed_file_extensions is None:
            self.allowed_file_extensions = [".xlsx", ".xls"]

        # Read from environment variables
        self.debug = os.getenv("VARIANCE_DEBUG", "false").lower() == "true"
        self.llm_model = os.getenv("VARIANCE_LLM_MODEL", self.llm_model)

class AnalysisConfig:
    """Configuration for financial analysis parameters."""

    # Revenue Analysis Thresholds
    REVENUE_ANALYSIS = {
        "revenue_change_threshold_vnd": 1_000_000,
        "revenue_entity_threshold_vnd": 100_000,
        "revenue_account_prefixes": ["511"],

        "cogs_change_threshold_vnd": 500_000,
        "cogs_entity_threshold_vnd": 50_000,
        "cogs_account_prefixes": ["632"],

        "sga_change_threshold_vnd": 500_000,
        "sga_entity_threshold_vnd": 50_000,
        "sga_641_account_prefixes": ["641"],
        "sga_642_account_prefixes": ["642"],

        "gross_margin_change_threshold_pct": 1.0,
        "high_gross_margin_risk_threshold_pct": -2.0,
        "sga_ratio_change_threshold_pct": 2.0,
        "high_sga_ratio_threshold_pct": 3.0,
        "revenue_pct_change_risk_threshold": 5.0,
        "high_revenue_pct_change_threshold": 20.0,

        "months_to_analyze": 8,
        "top_entity_impacts": 5,
        "lookback_periods": 10,
    }

    # Excel Processing Constants
    EXCEL_PROCESSING = {
        "max_sheet_name_length": 31,
        "header_scan_rows": 40,
        "data_row_offset": 2,
        "account_code_min_digits": 4,
        "progress_milestones": {
            "start": 10,
            "load": 15,
            "config": 20,
            "ai_thresholds": 25,
            "analysis_start": 30,
            "analysis_complete": 85,
            "storage": 90,
            "finalize": 95,
            "complete": 100
        }
    }

    # Core Analysis Configuration
    DEFAULT_CONFIG = {
        "materiality_vnd": 1_000_000_000,
        "recurring_pct_threshold": 0.05,
        "revenue_opex_pct_threshold": 0.10,
        "bs_pct_threshold": 0.05,
        "recurring_code_prefixes": ["6321", "635", "515"],
        "min_trend_periods": 3,
        "gm_drop_threshold_pct": 0.01,
        "dep_pct_only_prefixes": ["217", "632"],
        "customer_column_hints": [
            "customer", "khách", "khach", "client", "buyer", "entity",
            "company", "subsidiary", "parent company", "bwid", "vc1", "vc2", "vc3", "logistics"
        ],

        # Include all analysis configurations
        **REVENUE_ANALYSIS,
        **EXCEL_PROCESSING,

        # Additional settings
        "use_llm_analysis": False,
        "llm_model": "gpt-4o",
        "year_range": ["2024", "2025", "2026", "2027", "2028", "2029", "2030"],
        "trend_window_max": 5,
        "zero_division_replacement": 0.0,
        "numeric_fill_value": 0.0,
        "percentage_multiplier": 100.0,
    }

@lru_cache()
def get_settings() -> Settings:
    """Get cached application settings."""
    settings = Settings()
    settings.__post_init__()
    return settings

@lru_cache()
def get_analysis_config() -> dict:
    """Get cached analysis configuration."""
    return AnalysisConfig.DEFAULT_CONFIG.copy()